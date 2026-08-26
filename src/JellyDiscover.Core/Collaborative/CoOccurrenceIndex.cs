namespace JellyDiscover.Core.Collaborative;

public sealed record CoOccurrenceOptions
{
    /// <summary>
    /// Below this many contributing users the signal is noise, and the index reports
    /// itself inactive rather than producing confident nonsense.
    ///
    /// Co-occurrence needs density. A four-person household generates almost none, which
    /// is why this is a bonus term that switches on when the data supports it rather than
    /// a pillar of the ranking.
    /// </summary>
    public int MinimumUsers { get; init; } = 8;

    /// <summary>Ignore items almost nobody has watched; their neighbours are accidents.</summary>
    public int MinimumItemPlays { get; init; } = 2;

    /// <summary>
    /// Shrinkage. A pair seen by 2 users out of 2 has cosine 1.0 but means nothing; this
    /// damps a similarity by its support, so confidence has to be earned.
    /// </summary>
    public double SupportShrinkage { get; init; } = 5.0;

    /// <summary>Neighbours retained per item. Keeps the index small and scoring cheap.</summary>
    public int NeighboursPerItem { get; init; } = 40;
}

/// <summary>
/// Item-item collaborative filtering from watch co-occurrence.
///
/// "People who watched this also watched that" — a different signal from content
/// similarity, and often a better one, because it captures things no metadata explains.
/// Content similarity knows Arrival and Annihilation share vocabulary; only co-occurrence
/// knows the people who loved one loved the other.
///
/// The data was already there: JellyDiscover 1.x walked every user's played items to
/// compute a popularity count and then threw the pairings away.
/// </summary>
public sealed class CoOccurrenceIndex
{
    private readonly Dictionary<string, Dictionary<string, double>> _neighbours;

    private CoOccurrenceIndex(
        Dictionary<string, Dictionary<string, double>> neighbours,
        bool isActive,
        int userCount,
        int itemCount)
    {
        _neighbours = neighbours;
        IsActive = isActive;
        UserCount = userCount;
        ItemCount = itemCount;
    }

    /// <summary>False when the server has too little data for this to mean anything.</summary>
    public bool IsActive { get; }

    public int UserCount { get; }

    public int ItemCount { get; }

    public static CoOccurrenceIndex Empty { get; } = new([], false, 0, 0);

    /// <summary>
    /// Builds from each user's set of positively-interacted item ids.
    ///
    /// Only positives are used. An abandoned item tells us about that user's taste, not
    /// about which items belong together.
    /// </summary>
    public static CoOccurrenceIndex Build(
        IReadOnlyCollection<IReadOnlyCollection<string>> usersLikedItems,
        CoOccurrenceOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(usersLikedItems);
        var o = options ?? new CoOccurrenceOptions();

        var contributing = usersLikedItems.Where(set => set.Count >= 2).ToArray();
        if (contributing.Length < o.MinimumUsers)
        {
            return new CoOccurrenceIndex([], false, contributing.Length, 0);
        }

        // How many users played each item.
        var itemPlays = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var set in contributing)
        {
            foreach (var id in set.Distinct(StringComparer.Ordinal))
            {
                itemPlays[id] = itemPlays.TryGetValue(id, out var c) ? c + 1 : 1;
            }
        }

        var eligible = itemPlays
            .Where(kv => kv.Value >= o.MinimumItemPlays)
            .Select(kv => kv.Key)
            .ToHashSet(StringComparer.Ordinal);

        if (eligible.Count < 2)
        {
            return new CoOccurrenceIndex([], false, contributing.Length, eligible.Count);
        }

        // Pair counts. Quadratic in a user's history, which is fine: histories are small
        // and this runs once per full refresh, not once per user per item.
        var pairs = new Dictionary<(string, string), int>();
        foreach (var set in contributing)
        {
            var items = set.Where(eligible.Contains).Distinct(StringComparer.Ordinal)
                .OrderBy(id => id, StringComparer.Ordinal)
                .ToArray();

            for (var i = 0; i < items.Length; i++)
            {
                for (var j = i + 1; j < items.Length; j++)
                {
                    var key = (items[i], items[j]);
                    pairs[key] = pairs.TryGetValue(key, out var c) ? c + 1 : 1;
                }
            }
        }

        var neighbours = new Dictionary<string, Dictionary<string, double>>(StringComparer.Ordinal);

        foreach (var ((left, right), together) in pairs)
        {
            // Cosine over the user sets, shrunk by how much support the pair actually has.
            var raw = together / Math.Sqrt((double)itemPlays[left] * itemPlays[right]);
            var shrunk = raw * (together / (together + o.SupportShrinkage));
            if (shrunk <= 0)
            {
                continue;
            }

            Add(neighbours, left, right, shrunk);
            Add(neighbours, right, left, shrunk);
        }

        // Trim to the strongest neighbours per item.
        foreach (var key in neighbours.Keys.ToArray())
        {
            if (neighbours[key].Count <= o.NeighboursPerItem)
            {
                continue;
            }

            neighbours[key] = neighbours[key]
                .OrderByDescending(kv => kv.Value)
                .Take(o.NeighboursPerItem)
                .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);
        }

        return new CoOccurrenceIndex(neighbours, true, contributing.Length, eligible.Count);
    }

    private static void Add(
        Dictionary<string, Dictionary<string, double>> into,
        string from,
        string to,
        double score)
    {
        if (!into.TryGetValue(from, out var bucket))
        {
            bucket = new Dictionary<string, double>(StringComparer.Ordinal);
            into[from] = bucket;
        }

        bucket[to] = score;
    }

    /// <summary>Similarity between two items, 0 when unknown.</summary>
    public double Similarity(string left, string right)
    {
        if (!IsActive || !_neighbours.TryGetValue(left, out var bucket))
        {
            return 0;
        }

        return bucket.TryGetValue(right, out var score) ? score : 0;
    }

    /// <summary>
    /// How strongly this candidate co-occurs with what the user already likes.
    /// Uses the single strongest link rather than an average: one very strong "people who
    /// loved X loved this" beats a diffuse relationship with many things.
    /// </summary>
    public double AffinityTo(string candidateId, IEnumerable<string> likedItemIds)
    {
        if (!IsActive)
        {
            return 0;
        }

        double best = 0;
        foreach (var liked in likedItemIds)
        {
            var score = Similarity(candidateId, liked);
            if (score > best)
            {
                best = score;
            }
        }

        return best;
    }
}

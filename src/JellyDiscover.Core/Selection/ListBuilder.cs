using JellyDiscover.Core.Models;

namespace JellyDiscover.Core.Selection;

public sealed record ListOptions
{
    /// <summary>
    /// MMR trade-off. 1.0 is pure relevance (25 variations of the same film);
    /// lower values buy variety at the cost of a little predicted relevance.
    /// </summary>
    public double Lambda { get; init; } = 0.72;

    /// <summary>
    /// How hard to push the list toward the user's own genre distribution. If they watch
    /// 60% comedy the list should look roughly like that, rather than whatever the ranker
    /// happened to score highest.
    /// </summary>
    public double CalibrationStrength { get; init; } = 0.35;

    public int Count { get; init; } = 25;
}

public sealed record ScoredCandidate(CatalogItem Item, double Score, CatalogItem? NearestLiked)
{
    public IReadOnlyList<FeatureContribution> Why { get; init; } = Array.Empty<FeatureContribution>();
}

/// <summary>
/// Turns a ranked set into a *list*.
///
/// Sorting by score and taking the top N produces a monotonous list. JellyDiscover 1.x
/// tried to address that by adding `random.uniform(0, diversity)` to every score, which is
/// not diversity — it is noise that degrades the ranking at random and cannot distinguish
/// "these two are near-duplicates" from "these two are different".
///
/// Maximal Marginal Relevance does the real thing: at each step pick the item that
/// maximises relevance minus similarity to what has already been picked. Calibration then
/// nudges the genre mix toward the user's actual distribution.
/// </summary>
public static class ListBuilder
{
    public static IReadOnlyList<ScoredCandidate> Build(
        IReadOnlyList<ScoredCandidate> candidates,
        TasteProfileGenres targetDistribution,
        Func<CatalogItem, CatalogItem, double> similarity,
        ListOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(candidates);
        ArgumentNullException.ThrowIfNull(similarity);
        var o = options ?? new ListOptions();

        if (candidates.Count == 0 || o.Count <= 0)
        {
            return Array.Empty<ScoredCandidate>();
        }

        var pool = candidates.OrderByDescending(c => c.Score).ToList();
        var selected = new List<ScoredCandidate>(Math.Min(o.Count, pool.Count));
        var selectedGenreCounts = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);

        while (selected.Count < o.Count && pool.Count > 0)
        {
            var bestIndex = 0;
            var bestValue = double.NegativeInfinity;

            for (var i = 0; i < pool.Count; i++)
            {
                var candidate = pool[i];

                // Redundancy: closeness to the most similar already-selected item.
                double maxSim = 0;
                foreach (var chosen in selected)
                {
                    var sim = similarity(candidate.Item, chosen.Item);
                    if (sim > maxSim)
                    {
                        maxSim = sim;
                    }
                }

                var mmr = (o.Lambda * candidate.Score) - ((1 - o.Lambda) * maxSim);

                var calibration = CalibrationPenalty(
                    candidate.Item,
                    selectedGenreCounts,
                    selected.Count,
                    targetDistribution);

                var value = mmr - (o.CalibrationStrength * calibration);

                if (value > bestValue)
                {
                    bestValue = value;
                    bestIndex = i;
                }
            }

            var picked = pool[bestIndex];
            pool.RemoveAt(bestIndex);
            selected.Add(picked);

            foreach (var genre in picked.Item.Genres)
            {
                selectedGenreCounts[genre] =
                    selectedGenreCounts.TryGetValue(genre, out var c) ? c + 1 : 1;
            }
        }

        return selected;
    }

    /// <summary>
    /// How much adding this item would push the list away from the user's genre mix.
    /// Zero when the genre is under-represented so far, growing as it is over-served.
    /// </summary>
    private static double CalibrationPenalty(
        CatalogItem candidate,
        IReadOnlyDictionary<string, double> selectedCounts,
        int selectedTotal,
        TasteProfileGenres target)
    {
        if (selectedTotal == 0 || candidate.Genres.Count == 0 || target.IsEmpty)
        {
            return 0;
        }

        double penalty = 0;
        foreach (var genre in candidate.Genres)
        {
            var current = selectedCounts.TryGetValue(genre, out var c) ? c : 0;
            var currentShare = current / (double)selectedTotal;
            var targetShare = target.ShareOf(genre);

            // Only penalise over-representation; under-representation is what we want.
            var excess = currentShare - targetShare;
            if (excess > 0)
            {
                penalty += excess;
            }
        }

        return penalty / candidate.Genres.Count;
    }
}

/// <summary>Thin wrapper so the calibration target is explicit at the call site.</summary>
public readonly record struct TasteProfileGenres(IReadOnlyDictionary<string, double> Distribution)
{
    public bool IsEmpty => Distribution is null || Distribution.Count == 0;

    public double ShareOf(string genre)
    {
        if (Distribution is null)
        {
            return 0;
        }

        return Distribution.TryGetValue(genre, out var share) ? share : 0;
    }
}

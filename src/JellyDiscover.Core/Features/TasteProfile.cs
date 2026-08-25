using JellyDiscover.Core.Models;

namespace JellyDiscover.Core.Features;

/// <summary>
/// What we know about one user's taste.
///
/// Note what this is NOT: a single averaged "vibe vector". JellyDiscover 1.x mean-pooled
/// the user's history into one centroid, which for anyone with more than one taste lands
/// between their tastes and therefore represents none of them. Liked items are kept
/// individually so a candidate can be scored against each of them.
/// </summary>
public sealed class TasteProfile
{
    public required IReadOnlyList<WeightedItem> Liked { get; init; }

    public required IReadOnlyList<WeightedItem> Disliked { get; init; }

    public required IReadOnlyDictionary<string, double> GenreAffinity { get; init; }

    /// <summary>Affinity for genre *pairs* — "sci-fi horror" as a concept in its own
    /// right, not the coincidence of liking sci-fi and liking horror.</summary>
    public required IReadOnlyDictionary<string, double> GenreBlendAffinity { get; init; }

    public required IReadOnlyDictionary<string, double> TagAffinity { get; init; }

    public required IReadOnlyDictionary<string, double> DirectorAffinity { get; init; }

    public required IReadOnlyDictionary<string, double> ActorAffinity { get; init; }

    public required IReadOnlySet<string> Collections { get; init; }

    /// <summary>Share of the user's positive history per genre. Drives list calibration.</summary>
    public required IReadOnlyDictionary<string, double> GenreDistribution { get; init; }

    public double? MeanYear { get; init; }

    public double? MeanRuntimeMinutes { get; init; }

    public int PositiveCount => Liked.Count;

    public int NegativeCount => Disliked.Count;

    /// <summary>
    /// Below this much history we cannot fit a personal model and fall back to the
    /// server-wide prior. Deliberately low: a handful of signals still beats nothing.
    /// </summary>
    public bool HasEnoughHistory => Liked.Count >= 5;

    public static TasteProfile Empty { get; } = new()
    {
        Liked = Array.Empty<WeightedItem>(),
        Disliked = Array.Empty<WeightedItem>(),
        GenreAffinity = new Dictionary<string, double>(),
        GenreBlendAffinity = new Dictionary<string, double>(),
        TagAffinity = new Dictionary<string, double>(),
        DirectorAffinity = new Dictionary<string, double>(),
        ActorAffinity = new Dictionary<string, double>(),
        Collections = new HashSet<string>(),
        GenreDistribution = new Dictionary<string, double>(),
    };
}

public readonly record struct WeightedItem(CatalogItem Item, double Weight);

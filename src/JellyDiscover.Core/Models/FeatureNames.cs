namespace JellyDiscover.Core.Models;

/// <summary>
/// The feature vocabulary. Names are part of the contract: the ranker learns a weight per
/// feature and we surface the largest contributions as the reason for a recommendation,
/// so these must stay stable and human-meaningful.
/// </summary>
public static class FeatureNames
{
    public const string SimilarityTop1 = "similarity.top1";
    public const string SimilarityTop3 = "similarity.top3";
    public const string SimilarityNegative = "similarity.negative";
    public const string GenreAffinity = "genre.affinity";
    public const string GenreBlendAffinity = "genre.blend";
    public const string TagAffinity = "tag.affinity";
    public const string DirectorAffinity = "person.director";
    public const string ActorAffinity = "person.actor";
    public const string CollectionContinuation = "collection.continuation";
    public const string CollaborativeAffinity = "collaborative.affinity";
    public const string CommunityRating = "rating.community";
    public const string EraAffinity = "era.affinity";
    public const string RuntimeFit = "runtime.fit";
    public const string ServerPopularity = "popularity.server";
    public const string ExternalWatchlist = "external.watchlist";
    public const string ExternalRequested = "external.requested";
    public const string ExternalTrending = "external.trending";
    public const string Bias = "bias";

    /// <summary>Index order of the feature vector. Do not reorder without bumping the
    /// stored model version — persisted weights are positional.</summary>
    public static readonly string[] All =
    [
        SimilarityTop1,
        SimilarityTop3,
        SimilarityNegative,
        GenreAffinity,
        GenreBlendAffinity,
        TagAffinity,
        DirectorAffinity,
        ActorAffinity,
        CollectionContinuation,
        CollaborativeAffinity,
        CommunityRating,
        EraAffinity,
        RuntimeFit,
        ServerPopularity,
        ExternalWatchlist,
        ExternalRequested,
        ExternalTrending,
        Bias,
    ];

    public static int Count => All.Length;

    public static int IndexOf(string name)
    {
        var i = Array.IndexOf(All, name);
        if (i < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(name), name, "Unknown feature.");
        }

        return i;
    }
}

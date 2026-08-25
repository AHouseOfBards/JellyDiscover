namespace JellyDiscover.Core.Models;

/// <summary>One recommended item, with the reasoning that produced it.</summary>
public sealed record Recommendation
{
    public required CatalogItem Item { get; init; }

    /// <summary>Model probability that this user would engage with the item (0..1).</summary>
    public required double Score { get; init; }

    /// <summary>The user's own item that most drove this match — the "Because you watched X"
    /// label, which only exists because we score against individual items instead of a centroid.</summary>
    public CatalogItem? BecauseOf { get; init; }

    /// <summary>Top contributing features, largest absolute contribution first.</summary>
    public IReadOnlyList<FeatureContribution> Why { get; init; } = Array.Empty<FeatureContribution>();

    public string Explain()
    {
        if (BecauseOf is not null)
        {
            return $"Because you watched {BecauseOf.Name}";
        }

        var top = Why.FirstOrDefault();
        return top is null ? "Popular on this server" : top.Describe();
    }
}

public sealed record FeatureContribution(string Feature, double Value, double Weight)
{
    public double Contribution => Value * Weight;

    public string Describe() => Feature switch
    {
        FeatureNames.GenreAffinity => "Matches genres you watch",
        FeatureNames.GenreBlendAffinity => "Matches a genre blend you seek out",
        FeatureNames.TagAffinity => "Shares themes with your history",
        FeatureNames.DirectorAffinity => "From a director you follow",
        FeatureNames.ActorAffinity => "Features actors you watch",
        FeatureNames.CollectionContinuation => "Continues a collection you started",
        FeatureNames.CommunityRating => "Highly rated",
        FeatureNames.ServerPopularity => "Popular on this server",
        FeatureNames.EraAffinity => "From an era you favour",
        FeatureNames.RuntimeFit => "About the length you usually finish",
        FeatureNames.ExternalWatchlist => "On your watchlist",
        FeatureNames.ExternalRequested => "You requested this",
        FeatureNames.ExternalTrending => "Trending now",
        _ => "Similar to what you watch",
    };
}

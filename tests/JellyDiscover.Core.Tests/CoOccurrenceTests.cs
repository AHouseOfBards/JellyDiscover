using JellyDiscover.Core.Collaborative;
using Xunit;

namespace JellyDiscover.Core.Tests;

/// <summary>
/// "People who watched this also watched that" — a signal no metadata explains, built from
/// data 1.x already walked every user's history to collect and then discarded.
/// </summary>
public sealed class CoOccurrenceTests
{
    private static IReadOnlyCollection<IReadOnlyCollection<string>> Households(
        int count,
        params string[][] patterns)
    {
        var users = new List<IReadOnlyCollection<string>>();
        for (var i = 0; i < count; i++)
        {
            users.Add(patterns[i % patterns.Length]);
        }

        return users;
    }

    [Fact]
    public void SmallServer_ReportsItselfInactive()
    {
        // Four users cannot support co-occurrence. Saying so is better than inventing
        // confident relationships from three overlaps.
        var index = CoOccurrenceIndex.Build(Households(4, ["a", "b"], ["a", "c"]));

        Assert.False(index.IsActive);
        Assert.Equal(0, index.Similarity("a", "b"));
        Assert.Equal(0, index.AffinityTo("a", ["b", "c"]));
    }

    [Fact]
    public void LargeServer_LearnsPairsThatTravelTogether()
    {
        var index = CoOccurrenceIndex.Build(Households(
            20,
            ["arrival", "annihilation", "filler1"],
            ["arrival", "annihilation", "filler2"],
            ["comedy1", "comedy2", "filler3"]));

        Assert.True(index.IsActive);

        var together = index.Similarity("arrival", "annihilation");
        var apart = index.Similarity("arrival", "comedy1");

        Assert.True(together > apart, $"co-watched {together:F3} should beat unrelated {apart:F3}");
        Assert.True(together > 0);
    }

    [Fact]
    public void Similarity_IsSymmetric()
    {
        var index = CoOccurrenceIndex.Build(Households(
            20, ["x", "y"], ["x", "y", "z"], ["p", "q"]));

        Assert.Equal(index.Similarity("x", "y"), index.Similarity("y", "x"), 10);
    }

    /// <summary>
    /// A pair seen by two users has cosine 1.0 and means nothing. Shrinkage is what stops
    /// a coincidence outranking a genuinely popular pairing.
    /// </summary>
    [Fact]
    public void ThinSupport_IsDampedBelowWellSupportedPairs()
    {
        var users = new List<IReadOnlyCollection<string>>();

        // 18 users establish a strong, well-supported pairing.
        for (var i = 0; i < 18; i++)
        {
            users.Add(["popular1", "popular2"]);
        }

        // Two users happen to share an obscure pairing, and nothing else.
        users.Add(["rare1", "rare2"]);
        users.Add(["rare1", "rare2"]);

        var index = CoOccurrenceIndex.Build(users);

        var strong = index.Similarity("popular1", "popular2");
        var coincidence = index.Similarity("rare1", "rare2");

        Assert.True(
            strong > coincidence,
            $"well-supported {strong:F3} must beat a 2-user coincidence {coincidence:F3}");
    }

    [Fact]
    public void RarelyPlayedItems_AreExcluded()
    {
        var users = new List<IReadOnlyCollection<string>>();
        for (var i = 0; i < 15; i++)
        {
            users.Add(["common1", "common2"]);
        }

        users.Add(["common1", "seen-once"]);

        var index = CoOccurrenceIndex.Build(users);

        Assert.Equal(0, index.Similarity("common1", "seen-once"));
    }

    [Fact]
    public void AffinityTo_UsesTheStrongestLink()
    {
        var index = CoOccurrenceIndex.Build(Households(
            20,
            ["target", "strong", "filler1"],
            ["target", "strong", "filler2"],
            ["weak", "filler3"]));

        var viaStrong = index.AffinityTo("target", ["strong"]);
        var viaBoth = index.AffinityTo("target", ["weak", "strong"]);

        // Adding an unrelated liked item must not dilute the result the way a mean would.
        Assert.Equal(viaStrong, viaBoth, 10);
    }

    [Fact]
    public void UnknownItems_ScoreZeroRatherThanThrowing()
    {
        var index = CoOccurrenceIndex.Build(Households(20, ["a", "b"], ["a", "c"]));

        Assert.Equal(0, index.Similarity("nonexistent", "a"));
        Assert.Equal(0, index.AffinityTo("nonexistent", ["a", "b"]));
    }

    [Fact]
    public void EmptyInput_IsHandled()
    {
        var index = CoOccurrenceIndex.Build(Array.Empty<IReadOnlyCollection<string>>());

        Assert.False(index.IsActive);
        Assert.Equal(0, index.UserCount);
    }
}

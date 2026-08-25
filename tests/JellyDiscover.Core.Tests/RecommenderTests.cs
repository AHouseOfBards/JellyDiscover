using JellyDiscover.Core.Features;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Pipeline;
using JellyDiscover.Core.Ranking;
using JellyDiscover.Core.Text;
using Xunit;

namespace JellyDiscover.Core.Tests;

public sealed class RecommenderTests
{
    /// <summary>
    /// The defect that motivated the rewrite. A user who likes horror AND romantic comedy
    /// gets a mean-pooled centroid sitting between the two, which corresponds to nothing —
    /// so 1.x served them neither. Scoring against liked items individually must surface
    /// both clusters.
    /// </summary>
    [Fact]
    public void MultiModalTaste_SurfacesBothClusters_NotTheAverageOfThem()
    {
        var library = Fake.Library();
        var history = new List<Interaction>();
        for (var i = 0; i < 5; i++)
        {
            history.Add(Fake.Liked($"horror{i}", Sentiment.StrongPositive));
            history.Add(Fake.Liked($"romcom{i}", Sentiment.StrongPositive));
        }

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = history,
            CountPerKind = 10,
        });

        var names = result.All.Select(r => r.Item.Id).ToArray();
        var horror = names.Count(n => n.StartsWith("horror", StringComparison.Ordinal));
        var romcom = names.Count(n => n.StartsWith("romcom", StringComparison.Ordinal));

        Assert.True(horror > 0, $"expected some horror, got: {string.Join(", ", names)}");
        Assert.True(romcom > 0, $"expected some rom-com, got: {string.Join(", ", names)}");

        // And the bland middle should not have crowded them out.
        var filler = names.Count(n => n.StartsWith("filler", StringComparison.Ordinal));
        Assert.True(
            horror + romcom > filler,
            $"clusters ({horror}+{romcom}) should beat filler ({filler}): {string.Join(", ", names)}");
    }

    /// <summary>
    /// Abandonment is the only real negative signal a media server produces, and 1.x
    /// downloaded it on every run without ever reading it.
    ///
    /// Two users with identical positive history, differing only in whether they enjoyed
    /// or abandoned the same three horror films. The one who bailed should see fewer
    /// horror recommendations.
    /// </summary>
    [Fact]
    public void AbandonedItems_DemoteTheirNeighbours()
    {
        var library = Fake.Library();

        var enjoyed = new List<Interaction>();
        var bailed = new List<Interaction>();
        for (var i = 0; i < 6; i++)
        {
            enjoyed.Add(Fake.Liked($"scifi{i}", Sentiment.StrongPositive));
            bailed.Add(Fake.Liked($"scifi{i}", Sentiment.StrongPositive));
        }

        // The only difference between these two users.
        for (var i = 0; i < 3; i++)
        {
            enjoyed.Add(Fake.Liked($"horror{i}", Sentiment.StrongPositive));
            bailed.Add(Fake.Abandoned($"horror{i}"));
        }

        var recommender = new Recommender();
        var likedHorror = recommender.Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = enjoyed,
            CountPerKind = 12,
        });
        var quitHorror = recommender.Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = bailed,
            CountPerKind = 12,
        });

        var withLike = CountPrefix(likedHorror, "horror");
        var withBail = CountPrefix(quitHorror, "horror");

        Assert.True(
            withBail < withLike,
            $"abandoning horror should yield less horror than enjoying it: {withLike} -> {withBail}");
    }

    /// <summary>
    /// The mechanism itself, isolated from list construction: an unseen item similar to
    /// something the user abandoned must score lower than it would otherwise.
    /// </summary>
    [Fact]
    public void NegativeSimilarity_LowersAnItemScore()
    {
        var library = Fake.Library();
        var byId = library.ToDictionary(i => i.Id, StringComparer.Ordinal);
        var index = Bm25Index.Build(library);
        var extractor = new FeatureExtractor(index);

        var positives = Enumerable.Range(0, 6)
            .Select(i => Fake.Liked($"scifi{i}", Sentiment.StrongPositive))
            .ToList();

        var withoutDislike = TasteProfileBuilder.Build(positives, byId);

        var withDislike = TasteProfileBuilder.Build(
            positives.Concat(Enumerable.Range(0, 3).Select(i => Fake.Abandoned($"horror{i}"))),
            byId);

        var target = byId["horror9"];
        var model = RankerWeights.ColdStartPrior();

        var scoreClean = LogisticRanker.Score(model, extractor.Extract(target, withoutDislike).Values);
        var scoreAfterBail = LogisticRanker.Score(model, extractor.Extract(target, withDislike).Values);

        Assert.True(
            scoreAfterBail < scoreClean,
            $"similarity to an abandoned item must reduce score: {scoreClean:F4} -> {scoreAfterBail:F4}");
    }

    [Fact]
    public void AlreadyWatchedAndExcludedItems_AreNeverRecommended()
    {
        var library = Fake.Library();
        var history = Enumerable.Range(0, 6)
            .Select(i => Fake.Liked($"scifi{i}"))
            .ToList();

        var excluded = new HashSet<string>(StringComparer.Ordinal) { "scifi9", "horror1" };

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = history,
            ExcludedItemIds = excluded,
            CountPerKind = 25,
        });

        var ids = result.All.Select(r => r.Item.Id).ToHashSet(StringComparer.Ordinal);

        foreach (var watched in history)
        {
            Assert.DoesNotContain(watched.ItemId, ids);
        }

        Assert.DoesNotContain("scifi9", ids);
        Assert.DoesNotContain("horror1", ids);
    }

    /// <summary>
    /// The whole library is eligible. 1.x hardcoded Limit=600 with no sort and no paging,
    /// so most of a real library was permanently invisible.
    /// </summary>
    [Fact]
    public void EveryUnseenItem_IsConsidered()
    {
        var library = Fake.Library();
        var history = Enumerable.Range(0, 4).Select(i => Fake.Liked($"scifi{i}")).ToList();

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = history,
            CountPerKind = 5,
        });

        Assert.Equal(library.Count, result.CatalogueSize);
        Assert.Equal(library.Count - history.Count, result.CandidatesConsidered);
    }

    [Fact]
    public void Recommendations_CarryAnExplanation()
    {
        var library = Fake.Library();
        var history = Enumerable.Range(0, 6)
            .Select(i => Fake.Liked($"scifi{i}", Sentiment.StrongPositive))
            .ToList();

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = history,
            CountPerKind = 5,
        });

        var top = result.All.First();
        Assert.False(string.IsNullOrWhiteSpace(top.Explain()));

        // "Because you watched X" only exists because we keep liked items individually.
        var anyBecause = result.All.Any(r => r.BecauseOf is not null);
        Assert.True(anyBecause, "at least one recommendation should name the item that drove it");
    }

    [Fact]
    public void EachKind_GetsItsOwnList()
    {
        var library = Fake.Library();
        library.Add(Fake.Item("album1", "Blue Hour", MediaKind.MusicAlbum, genres: ["Jazz"]));
        library.Add(Fake.Item("album2", "Night Bus", MediaKind.MusicAlbum, genres: ["Jazz"]));
        library.Add(Fake.Item("show1", "The Long Wait", MediaKind.Series, genres: ["Drama"]));

        var history = Enumerable.Range(0, 4).Select(i => Fake.Liked($"scifi{i}")).ToList();

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = history,
            CountPerKind = 5,
        });

        Assert.True(result.ByKind.ContainsKey(MediaKind.Movie));
        Assert.True(result.ByKind.ContainsKey(MediaKind.MusicAlbum));
        Assert.All(
            result.ByKind,
            kv => Assert.All(kv.Value, r => Assert.Equal(kv.Key, r.Item.Kind)));
    }

    [Fact]
    public void EmptyHistory_StillProducesAList()
    {
        var library = Fake.Library();

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = Array.Empty<Interaction>(),
            CountPerKind = 10,
        });

        Assert.NotEmpty(result.All);
        Assert.False(result.ModelWasFitted);
    }

    [Fact]
    public void EmptyCatalogue_ReturnsNothingRatherThanThrowing()
    {
        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = Array.Empty<CatalogItem>(),
            History = Array.Empty<Interaction>(),
        });

        Assert.Empty(result.All);
        Assert.Equal(0, result.CatalogueSize);
    }

    private static int CountPrefix(RecommendationResult result, string prefix)
        => result.All.Count(r => r.Item.Id.StartsWith(prefix, StringComparison.Ordinal));
}

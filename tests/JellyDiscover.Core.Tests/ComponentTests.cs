using JellyDiscover.Core.Models;
using JellyDiscover.Core.Ranking;
using JellyDiscover.Core.Signals;
using JellyDiscover.Core.Text;
using Xunit;

namespace JellyDiscover.Core.Tests;

public sealed class SignalClassifierTests
{
    private static readonly DateTimeOffset Now = new(2026, 8, 25, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Rewatch_IsTheStrongestPositive()
    {
        var result = SignalClassifier.Classify(
            "x",
            new PlaybackFacts { Played = true, PlayCount = 3, LastPlayed = Now.AddDays(-5) },
            Now);

        Assert.NotNull(result);
        Assert.Equal(Sentiment.StrongPositive, result!.Sentiment);
    }

    [Fact]
    public void FavoriteOrHighRating_IsStrongPositive()
    {
        var fav = SignalClassifier.Classify("a", new PlaybackFacts { IsFavorite = true }, Now);
        var rated = SignalClassifier.Classify("b", new PlaybackFacts { UserRating = 9 }, Now);

        Assert.Equal(Sentiment.StrongPositive, fav!.Sentiment);
        Assert.Equal(Sentiment.StrongPositive, rated!.Sentiment);
    }

    [Fact]
    public void LowExplicitRating_BeatsImplicitPositives()
    {
        // They watched the whole thing and then rated it 2. Believe the rating.
        var result = SignalClassifier.Classify(
            "x",
            new PlaybackFacts { Played = true, PlayCount = 1, UserRating = 2 },
            Now);

        Assert.Equal(Sentiment.StrongNegative, result!.Sentiment);
    }

    [Fact]
    public void StartedAndAbandonedLongAgo_IsStrongNegative()
    {
        var result = SignalClassifier.Classify(
            "x",
            new PlaybackFacts { PlayedPercentage = 8, LastPlayed = Now.AddDays(-90) },
            Now);

        Assert.Equal(Sentiment.StrongNegative, result!.Sentiment);
    }

    [Fact]
    public void StartedRecently_IsNotYetJudged()
    {
        // Still plausibly mid-watch. No label is better than a wrong one.
        var result = SignalClassifier.Classify(
            "x",
            new PlaybackFacts { PlayedPercentage = 8, LastPlayed = Now.AddDays(-2) },
            Now);

        Assert.Null(result);
    }

    [Fact]
    public void NeverStartedButNewToTheLibrary_ProducesNoSignal()
    {
        var result = SignalClassifier.Classify(
            "x",
            new PlaybackFacts { AddedToLibrary = Now.AddDays(-3) },
            Now);

        Assert.Null(result);
    }

    [Fact]
    public void NeverStartedAndLongAvailable_IsWeakNegative()
    {
        var result = SignalClassifier.Classify(
            "x",
            new PlaybackFacts { AddedToLibrary = Now.AddDays(-200) },
            Now);

        Assert.Equal(Sentiment.WeakNegative, result!.Sentiment);
        Assert.True(result.Confidence < 0.5, "weak negatives must not carry full confidence");
    }

    /// <summary>
    /// 1.x wrapped its date handling in a catch-all that returned a fixed constant, so
    /// recency weighting silently became a no-op and no test could observe it. This must
    /// actually vary with time.
    /// </summary>
    [Fact]
    public void RecencyWeight_ActuallyDecays()
    {
        var fresh = SignalClassifier.RecencyWeight(Now.AddDays(-1), Now, 240);
        var mid = SignalClassifier.RecencyWeight(Now.AddDays(-240), Now, 240);
        var ancient = SignalClassifier.RecencyWeight(Now.AddDays(-2000), Now, 240);

        Assert.True(fresh > mid, $"{fresh} should exceed {mid}");
        Assert.True(mid > ancient, $"{mid} should exceed {ancient}");
        Assert.InRange(mid, 0.49, 0.51);
        Assert.True(ancient >= 0.15, "decay is floored, never zero");
    }
}

public sealed class Bm25IndexTests
{
    /// <summary>
    /// The genre vocabulary comes from the library, not a hardcoded list. "Science Fiction"
    /// is a term here because the catalogue contains it — 1.x hardcoded 22 IMDb genres and
    /// silently produced near-empty vectors on TMDB-sourced metadata.
    /// </summary>
    [Fact]
    public void Vocabulary_IsLearnedFromTheCatalogue()
    {
        var index = Bm25Index.Build(Fake.Library());

        Assert.True(index.Knows(Tokenizer.Field("genre", "Science Fiction")));
        Assert.False(index.Knows(Tokenizer.Field("genre", "Sci-Fi")));
        Assert.True(index.VocabularySize > 0);
    }

    [Fact]
    public void StructuredValues_StayAtomic()
    {
        Assert.Equal("genre:science_fiction", Tokenizer.Field("genre", "Science Fiction"));
        Assert.Equal("director:christopher_nolan", Tokenizer.Field("director", "Christopher Nolan"));

        // Diacritics normalise, so the same person does not split into two terms.
        Assert.Equal(Tokenizer.Field("actor", "Amelie"), Tokenizer.Field("actor", "Amélie"));
    }

    [Fact]
    public void SameClusterItems_AreMoreSimilarThanCrossClusterOnes()
    {
        var index = Bm25Index.Build(Fake.Library());

        var within = index.Similarity("horror0", "horror7");
        var across = index.Similarity("horror0", "romcom7");

        Assert.True(within > across, $"within-cluster {within:F3} should beat across {across:F3}");
    }

    [Fact]
    public void EmptyCatalogue_DoesNotThrow()
    {
        var index = Bm25Index.Build(Array.Empty<CatalogItem>());

        Assert.Equal(0, index.DocumentCount);
        Assert.Equal(0, index.Similarity("a", "b"));
    }
}

public sealed class LogisticRankerTests
{
    /// <summary>
    /// The point of learning weights instead of shipping sliders: a user who only cares
    /// about one signal should end up with a model that reflects that.
    /// </summary>
    [Fact]
    public void Fit_LearnsWhichFeatureMatters()
    {
        var directorIndex = FeatureNames.IndexOf(FeatureNames.DirectorAffinity);
        var ratingIndex = FeatureNames.IndexOf(FeatureNames.CommunityRating);
        var biasIndex = FeatureNames.IndexOf(FeatureNames.Bias);

        var samples = new List<TrainingSample>();
        for (var i = 0; i < 60; i++)
        {
            // Label depends entirely on director affinity; rating is pure noise.
            var likesDirector = i % 2 == 0;
            var x = new double[FeatureNames.Count];
            x[directorIndex] = likesDirector ? 1.0 : 0.0;
            x[ratingIndex] = (i % 5) / 5.0;
            x[biasIndex] = 1.0;
            samples.Add(new TrainingSample(x, likesDirector ? 1.0 : 0.0));
        }

        var prior = RankerWeights.ColdStartPrior();
        var fitted = LogisticRanker.Fit(samples, prior);

        Assert.True(
            fitted.Values[directorIndex] > prior.Values[directorIndex],
            "the decisive feature should gain weight");

        var withDirector = new double[FeatureNames.Count];
        withDirector[directorIndex] = 1.0;
        withDirector[biasIndex] = 1.0;

        var without = new double[FeatureNames.Count];
        without[biasIndex] = 1.0;

        Assert.True(
            LogisticRanker.Score(fitted, withDirector) > LogisticRanker.Score(fitted, without),
            "the fitted model should separate the classes");
    }

    [Fact]
    public void Fit_WithOnlyOneClass_KeepsThePrior()
    {
        var prior = RankerWeights.ColdStartPrior();
        var samples = Enumerable.Range(0, 20)
            .Select(_ =>
            {
                var x = new double[FeatureNames.Count];
                x[FeatureNames.IndexOf(FeatureNames.Bias)] = 1;
                return new TrainingSample(x, 1.0);
            })
            .ToList();

        var fitted = LogisticRanker.Fit(samples, prior);

        Assert.Equal(prior.Values, fitted.Values);
    }

    [Fact]
    public void Score_IsAProbability()
    {
        var w = RankerWeights.ColdStartPrior();
        var x = new double[FeatureNames.Count];
        Array.Fill(x, 1.0);

        var score = LogisticRanker.Score(w, x);

        Assert.InRange(score, 0.0, 1.0);
    }

    [Fact]
    public void Score_HandlesExtremeInputsWithoutOverflow()
    {
        var w = new RankerWeights { Values = Enumerable.Repeat(1e6, FeatureNames.Count).ToArray() };
        var big = Enumerable.Repeat(1e6, FeatureNames.Count).ToArray();
        var smallNegative = Enumerable.Repeat(-1e6, FeatureNames.Count).ToArray();

        Assert.InRange(LogisticRanker.Score(w, big), 0.0, 1.0);
        Assert.InRange(LogisticRanker.Score(w, smallNegative), 0.0, 1.0);
    }

    [Fact]
    public void Explain_RanksContributionsByImpact()
    {
        var w = RankerWeights.ColdStartPrior();
        var x = new double[FeatureNames.Count];
        x[FeatureNames.IndexOf(FeatureNames.SimilarityTop1)] = 1.0;
        x[FeatureNames.IndexOf(FeatureNames.RuntimeFit)] = 0.1;
        x[FeatureNames.IndexOf(FeatureNames.Bias)] = 1.0;

        var explained = LogisticRanker.Explain(w, x);

        Assert.NotEmpty(explained);
        Assert.Equal(FeatureNames.SimilarityTop1, explained[0].Feature);

        // Bias is a model intercept, not a reason a human would accept.
        Assert.DoesNotContain(explained, c => c.Feature == FeatureNames.Bias);
    }
}

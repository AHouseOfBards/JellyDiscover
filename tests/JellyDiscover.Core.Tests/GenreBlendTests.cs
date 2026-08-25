using JellyDiscover.Core.Features;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Pipeline;
using JellyDiscover.Core.Ranking;
using JellyDiscover.Core.Text;
using Xunit;

namespace JellyDiscover.Core.Tests;

/// <summary>
/// Hybrids are their own genre. Alien is not "a sci-fi film that also has horror in it" to
/// anyone deciding what to watch, and Central Intelligence is not a slightly funny action
/// film. A recommender that treats genres as independent labels gets both cases wrong.
/// </summary>
public sealed class GenreBlendTests
{
    private static List<CatalogItem> BlendLibrary()
    {
        var items = new List<CatalogItem>();

        // The blend the user actually likes. Enough of them that the list could be
        // filled with blends alone -- otherwise the test measures fixture size, not ranking.
        for (var i = 0; i < 14; i++)
        {
            items.Add(Fake.Item(
                $"scifihorror{i}",
                $"Derelict {i}",
                genres: ["Science Fiction", "Horror"],
                tags: ["creature", "spacecraft", "isolation"],
                director: "Ridley Vance",
                year: 1979 + i,
                overview: "The crew of a deep space freighter is hunted by something that came aboard."));
        }

        // Pure sci-fi: shares one genre, none of the dread.
        for (var i = 0; i < 6; i++)
        {
            items.Add(Fake.Item(
                $"puresci{i}",
                $"Contact Protocol {i}",
                genres: ["Science Fiction"],
                tags: ["first contact", "linguistics", "diplomacy"],
                director: "Corin Alba",
                year: 2010 + i,
                overview: "A linguist works to translate a message from an orbiting craft."));
        }

        // Pure horror: shares the other genre.
        for (var i = 0; i < 6; i++)
        {
            items.Add(Fake.Item(
                $"purehorror{i}",
                $"The Old House {i}",
                genres: ["Horror"],
                tags: ["haunted house", "possession", "family curse"],
                director: "Mira Kell",
                year: 2010 + i,
                overview: "A family discovers their new home has a violent history."));
        }

        return items;
    }

    /// <summary>
    /// A user who watches sci-fi horror should get more sci-fi horror than either pure
    /// genre — even though, counted independently, they have maximal affinity for both
    /// "Science Fiction" and "Horror" and so a naive model rates all three equally.
    /// </summary>
    [Fact]
    public void SciFiHorrorFan_GetsBlends_NotJustEitherComponentGenre()
    {
        var library = BlendLibrary();
        var history = Enumerable.Range(0, 4)
            .Select(i => Fake.Liked($"scifihorror{i}", Sentiment.StrongPositive))
            .ToList();

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = library,
            History = history,
            CountPerKind = 6,
        });

        var ids = result.All.Select(r => r.Item.Id).ToArray();
        var blends = ids.Count(i => i.StartsWith("scifihorror", StringComparison.Ordinal));
        var pureSci = ids.Count(i => i.StartsWith("puresci", StringComparison.Ordinal));
        var pureHorror = ids.Count(i => i.StartsWith("purehorror", StringComparison.Ordinal));

        // The strongest match must be a blend: that is the ranking working.
        Assert.StartsWith("scifihorror", ids[0], StringComparison.Ordinal);

        // And blends should outnumber either component genre. Note this is deliberately
        // not "all blends" -- MMR is meant to leave room for adjacent exploration.
        Assert.True(
            blends > pureSci && blends > pureHorror,
            $"blend should lead: blend={blends} pureSci={pureSci} pureHorror={pureHorror} [{string.Join(", ", ids)}]");
    }

    /// <summary>The blend term must be a real, distinct concept in the vocabulary.</summary>
    [Fact]
    public void BlendTerms_AreCanonicalAndOrderIndependent()
    {
        var one = DocumentBuilder.BlendTerms(["Science Fiction", "Horror"]).ToArray();
        var other = DocumentBuilder.BlendTerms(["Horror", "Science Fiction"]).ToArray();

        Assert.Equal(one, other);
        Assert.Single(one);
        Assert.Equal("blend:horror_science_fiction", one[0]);

        // Action + Comedy is likewise its own thing.
        var buddy = DocumentBuilder.BlendTerms(["Comedy", "Action"]).ToArray();
        Assert.Equal("blend:action_comedy", buddy[0]);
    }

    [Fact]
    public void SingleGenreItems_ProduceNoBlend()
        => Assert.Empty(DocumentBuilder.BlendTerms(["Drama"]));

    [Fact]
    public void ThreeGenres_ProduceAllPairs()
    {
        var terms = DocumentBuilder.BlendTerms(["Action", "Comedy", "Crime"]).ToArray();

        Assert.Equal(3, terms.Length);
        Assert.Contains("blend:action_comedy", terms);
        Assert.Contains("blend:action_crime", terms);
        Assert.Contains("blend:comedy_crime", terms);
    }

    /// <summary>
    /// The second defect: a plain mean over genres penalised hybrids. Central Intelligence
    /// (Action, Comedy) must not lose to a pure action film for a user who likes both.
    /// </summary>
    [Fact]
    public void ActionComedy_IsNotPenalisedForHavingTwoGenres()
    {
        var library = new List<CatalogItem>();

        for (var i = 0; i < 5; i++)
        {
            library.Add(Fake.Item(
                $"buddy{i}",
                $"Partners in Crime {i}",
                genres: ["Action", "Comedy"],
                tags: ["buddy cop", "mismatched partners"],
                overview: "A mismatched pair are forced to work together and bicker their way to victory."));
        }

        for (var i = 0; i < 5; i++)
        {
            library.Add(Fake.Item(
                $"straightaction{i}",
                $"Hard Target {i}",
                genres: ["Action"],
                tags: ["revenge", "mercenary"],
                overview: "A soldier hunts the people who betrayed his unit."));
        }

        var byId = library.ToDictionary(i => i.Id, StringComparer.Ordinal);
        var index = Bm25Index.Build(library);
        var extractor = new FeatureExtractor(index);

        // This user likes action-comedies specifically.
        var history = Enumerable.Range(0, 3)
            .Select(i => Fake.Liked($"buddy{i}", Sentiment.StrongPositive))
            .ToList();
        var profile = TasteProfileBuilder.Build(history, byId);
        var model = RankerWeights.ColdStartPrior();

        var blendScore = LogisticRanker.Score(
            model, extractor.Extract(byId["buddy4"], profile).Values);
        var pureScore = LogisticRanker.Score(
            model, extractor.Extract(byId["straightaction4"], profile).Values);

        Assert.True(
            blendScore > pureScore,
            $"the action-comedy should beat the pure action film: {blendScore:F4} vs {pureScore:F4}");
    }

    /// <summary>
    /// Regression guard for the dilution bug specifically: the genre-affinity feature
    /// alone must not rank a two-genre match below a one-genre match.
    /// </summary>
    [Fact]
    public void GenreAffinityFeature_DoesNotDiluteWithGenreCount()
    {
        var library = new List<CatalogItem>
        {
            Fake.Item("liked", "Liked Comedy", genres: ["Comedy"]),
            Fake.Item("likedAction", "Liked Action", genres: ["Action"]),
            Fake.Item("both", "Action Comedy", genres: ["Action", "Comedy"]),
            Fake.Item("onlyComedy", "Pure Comedy", genres: ["Comedy"]),
        };

        var byId = library.ToDictionary(i => i.Id, StringComparer.Ordinal);
        var index = Bm25Index.Build(library);
        var extractor = new FeatureExtractor(index);

        var profile = TasteProfileBuilder.Build(
            [Fake.Liked("liked"), Fake.Liked("likedAction")],
            byId);

        var genreIndex = FeatureNames.IndexOf(FeatureNames.GenreAffinity);
        var both = extractor.Extract(byId["both"], profile).Values[genreIndex];
        var single = extractor.Extract(byId["onlyComedy"], profile).Values[genreIndex];

        Assert.True(
            both >= single,
            $"matching two liked genres must not score below matching one: {both:F4} vs {single:F4}");
    }
}

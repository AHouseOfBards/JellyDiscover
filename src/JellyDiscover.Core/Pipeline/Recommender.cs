using JellyDiscover.Core.Collaborative;
using JellyDiscover.Core.Features;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Ranking;
using JellyDiscover.Core.Selection;
using JellyDiscover.Core.Text;

namespace JellyDiscover.Core.Pipeline;

public sealed record RecommendationRequest
{
    /// <summary>Everything the user could be shown. Not a pre-truncated page — the whole
    /// eligible catalogue for the kinds being generated.</summary>
    public required IReadOnlyList<CatalogItem> Catalogue { get; init; }

    public required IReadOnlyList<Interaction> History { get; init; }

    /// <summary>Items never to recommend (admin blocklist, already-in-library, etc).</summary>
    public IReadOnlySet<string> ExcludedItemIds { get; init; } = new HashSet<string>();

    public ExternalSignals External { get; init; } = ExternalSignals.None;

    /// <summary>
    /// Server-wide co-watch signal, built once per refresh and shared across users.
    /// Null or inactive on servers too small to support it.
    /// </summary>
    public CoOccurrenceIndex? CoOccurrence { get; init; }

    /// <summary>Weights from the previous run, so a user's model improves over time
    /// instead of being refit from scratch on every pass.</summary>
    public RankerWeights? ExistingModel { get; init; }

    /// <summary>Server-wide weights, used as the prior for users with thin history.</summary>
    public RankerWeights? ServerPrior { get; init; }

    public int CountPerKind { get; init; } = 25;

    public int RetrievalDepth { get; init; } = 500;

    public ListOptions List { get; init; } = new();

    public LogisticOptions Learning { get; init; } = new();
}

public sealed record RecommendationResult
{
    public required IReadOnlyDictionary<MediaKind, IReadOnlyList<Recommendation>> ByKind { get; init; }

    public required RankerWeights Model { get; init; }

    public required int CandidatesConsidered { get; init; }

    public required int CatalogueSize { get; init; }

    public required bool ModelWasFitted { get; init; }

    public IReadOnlyList<Recommendation> All
        => ByKind.Values.SelectMany(v => v).OrderByDescending(r => r.Score).ToArray();
}

/// <summary>
/// The whole recommendation pipeline, host-free.
///
/// retrieve (whole catalogue) -> rank (learned) -> diversify (MMR) -> calibrate -> explain
/// </summary>
public sealed class Recommender
{
    public RecommendationResult Recommend(RecommendationRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var catalogue = request.Catalogue;
        var byId = catalogue.ToDictionary(i => i.Id, StringComparer.Ordinal);

        // ---- Item representation. IDF comes from this server's actual library. --------
        var index = Bm25Index.Build(catalogue);
        var maxPlays = catalogue.Count == 0 ? 1 : catalogue.Max(i => i.ServerPlayCount);
        var extractor = new FeatureExtractor(index, maxPlays, request.CoOccurrence);

        var profile = TasteProfileBuilder.Build(request.History, byId);

        // ---- Learn this user's weights from their own labelled history. ---------------
        var prior = request.ExistingModel ?? request.ServerPrior ?? RankerWeights.ColdStartPrior();
        var samples = BuildTrainingSet(request.History, byId, profile, extractor, request.External);

        var fitted = samples.Count > 0;
        var model = fitted
            ? LogisticRanker.Fit(samples, prior, request.Learning)
            : prior;

        // ---- Score every eligible candidate. No 600-item ceiling. ---------------------
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var interaction in request.History)
        {
            seen.Add(interaction.ItemId);
        }

        var scoredByKind = new Dictionary<MediaKind, List<ScoredCandidate>>();

        foreach (var item in catalogue)
        {
            if (seen.Contains(item.Id) || request.ExcludedItemIds.Contains(item.Id))
            {
                continue;
            }

            var features = extractor.Extract(item, profile, request.External);
            var score = LogisticRanker.Score(model, features.Values);

            if (!scoredByKind.TryGetValue(item.Kind, out var bucket))
            {
                bucket = new List<ScoredCandidate>();
                scoredByKind[item.Kind] = bucket;
            }

            bucket.Add(new ScoredCandidate(item, score, features.NearestLiked)
            {
                Why = LogisticRanker.Explain(model, features.Values),
            });
        }

        // ---- Build a list per kind: MMR + calibration, then explain. ------------------
        var target = new TasteProfileGenres(profile.GenreDistribution);
        var listOptions = request.List with { Count = request.CountPerKind };
        var result = new Dictionary<MediaKind, IReadOnlyList<Recommendation>>();
        var considered = 0;

        foreach (var (kind, bucket) in scoredByKind)
        {
            considered += bucket.Count;

            // Retrieval: keep the strongest N before the O(n*k) diversity pass.
            var shortlist = bucket
                .OrderByDescending(c => c.Score)
                .Take(request.RetrievalDepth)
                .ToArray();

            var chosen = ListBuilder.Build(
                shortlist,
                target,
                (a, b) => index.Similarity(a.Id, b.Id),
                listOptions);

            result[kind] = chosen
                .Select(c => new Recommendation
                {
                    Item = c.Item,
                    Score = c.Score,
                    BecauseOf = c.NearestLiked,
                    Why = c.Why,
                })
                .ToArray();
        }

        return new RecommendationResult
        {
            ByKind = result,
            Model = model,
            CandidatesConsidered = considered,
            CatalogueSize = catalogue.Count,
            ModelWasFitted = fitted,
        };
    }

    /// <summary>
    /// Turns the user's labelled history into training rows.
    ///
    /// Each historical item is featurised against the profile *excluding itself*, which is
    /// what stops the model simply learning "similarity to yourself is 1.0".
    /// </summary>
    private static List<TrainingSample> BuildTrainingSet(
        IReadOnlyList<Interaction> history,
        IReadOnlyDictionary<string, CatalogItem> byId,
        TasteProfile profile,
        FeatureExtractor extractor,
        ExternalSignals external)
    {
        var samples = new List<TrainingSample>(history.Count);

        foreach (var interaction in history)
        {
            if (!byId.TryGetValue(interaction.ItemId, out var item))
            {
                continue;
            }

            var features = extractor.Extract(item, profile, external);
            var label = interaction.IsPositive ? 1.0 : 0.0;
            var weight = interaction.Sentiment switch
            {
                Sentiment.StrongPositive => 1.5,
                Sentiment.Positive => 1.0,
                Sentiment.StrongNegative => 1.5,
                Sentiment.WeakNegative => 0.4,
                _ => 1.0,
            } * Math.Clamp(interaction.Confidence, 0.05, 1.0);

            samples.Add(new TrainingSample(features.Values, label, weight));
        }

        return samples;
    }
}

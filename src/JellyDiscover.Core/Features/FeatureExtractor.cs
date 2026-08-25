using JellyDiscover.Core.Models;
using JellyDiscover.Core.Text;

namespace JellyDiscover.Core.Features;

/// <summary>External signals that are not derivable from the catalogue itself.</summary>
public sealed record ExternalSignals
{
    public IReadOnlySet<string> WatchlistItemIds { get; init; } = new HashSet<string>();

    public IReadOnlySet<string> RequestedItemIds { get; init; } = new HashSet<string>();

    public IReadOnlySet<string> TrendingItemIds { get; init; } = new HashSet<string>();

    public static ExternalSignals None { get; } = new();
}

/// <summary>The features for one candidate, plus the liked item that best explains it.</summary>
public sealed record ExtractedFeatures(double[] Values, CatalogItem? NearestLiked);

/// <summary>
/// Turns (candidate, profile) into the fixed-length feature vector the ranker consumes.
///
/// The two similarity features are computed against the user's liked items *individually*
/// and then reduced, rather than against an average of them. That is what preserves
/// multi-modal taste and what makes "Because you watched X" expressible at all.
/// </summary>
public sealed class FeatureExtractor
{
    private readonly Bm25Index _index;
    private readonly int _maxServerPlays;

    public FeatureExtractor(Bm25Index index, int maxServerPlays = 1)
    {
        ArgumentNullException.ThrowIfNull(index);
        _index = index;
        _maxServerPlays = Math.Max(1, maxServerPlays);
    }

    public ExtractedFeatures Extract(
        CatalogItem candidate,
        TasteProfile profile,
        ExternalSignals? external = null)
    {
        ArgumentNullException.ThrowIfNull(candidate);
        ArgumentNullException.ThrowIfNull(profile);
        var ext = external ?? ExternalSignals.None;

        var x = new double[FeatureNames.Count];
        var candidateVector = _index.Vector(candidate.Id);

        // --- Similarity against individual liked items -----------------------------
        double best = 0;
        CatalogItem? bestItem = null;
        Span<double> topThree = stackalloc double[3];
        topThree.Clear();

        foreach (var (item, weight) in profile.Liked)
        {
            if (string.Equals(item.Id, candidate.Id, StringComparison.Ordinal))
            {
                continue;
            }

            var sim = candidateVector.Cosine(_index.Vector(item.Id)) * weight;
            if (sim <= 0)
            {
                continue;
            }

            if (sim > best)
            {
                best = sim;
                bestItem = item;
            }

            InsertTop(topThree, sim);
        }

        var top3Mean = Mean(topThree);

        // --- Similarity to things they abandoned or disliked ------------------------
        double worst = 0;
        foreach (var (item, weight) in profile.Disliked)
        {
            var sim = candidateVector.Cosine(_index.Vector(item.Id)) * weight;
            if (sim > worst)
            {
                worst = sim;
            }
        }

        x[FeatureNames.IndexOf(FeatureNames.SimilarityTop1)] = Clamp01(best);
        x[FeatureNames.IndexOf(FeatureNames.SimilarityTop3)] = Clamp01(top3Mean);
        x[FeatureNames.IndexOf(FeatureNames.SimilarityNegative)] = Clamp01(worst);

        // --- Structured affinities ---------------------------------------------------
        x[FeatureNames.IndexOf(FeatureNames.GenreAffinity)] =
            Clamp01(SoftAffinity(candidate.Genres, profile.GenreAffinity));
        x[FeatureNames.IndexOf(FeatureNames.GenreBlendAffinity)] =
            Clamp01(MaxAffinity(
                DocumentBuilder.BlendTerms(candidate.Genres),
                profile.GenreBlendAffinity));
        x[FeatureNames.IndexOf(FeatureNames.TagAffinity)] =
            Clamp01(MeanAffinity(candidate.Tags, profile.TagAffinity));
        x[FeatureNames.IndexOf(FeatureNames.DirectorAffinity)] =
            Clamp01(MaxAffinity(Names(candidate, PersonRole.Director), profile.DirectorAffinity));
        x[FeatureNames.IndexOf(FeatureNames.ActorAffinity)] =
            Clamp01(MeanAffinity(Names(candidate, PersonRole.Actor), profile.ActorAffinity));

        x[FeatureNames.IndexOf(FeatureNames.CollectionContinuation)] =
            !string.IsNullOrWhiteSpace(candidate.CollectionName)
            && profile.Collections.Contains(candidate.CollectionName)
                ? 1.0
                : 0.0;

        // --- Item priors ---------------------------------------------------------------
        x[FeatureNames.IndexOf(FeatureNames.CommunityRating)] =
            Clamp01((candidate.CommunityRating ?? 0) / 10.0);

        x[FeatureNames.IndexOf(FeatureNames.EraAffinity)] = EraFit(candidate, profile);
        x[FeatureNames.IndexOf(FeatureNames.RuntimeFit)] = RuntimeFit(candidate, profile);

        x[FeatureNames.IndexOf(FeatureNames.ServerPopularity)] =
            Clamp01((double)candidate.ServerPlayCount / _maxServerPlays);

        // --- External signals -------------------------------------------------------------
        x[FeatureNames.IndexOf(FeatureNames.ExternalWatchlist)] =
            ext.WatchlistItemIds.Contains(candidate.Id) ? 1 : 0;
        x[FeatureNames.IndexOf(FeatureNames.ExternalRequested)] =
            ext.RequestedItemIds.Contains(candidate.Id) ? 1 : 0;
        x[FeatureNames.IndexOf(FeatureNames.ExternalTrending)] =
            ext.TrendingItemIds.Contains(candidate.Id) ? 1 : 0;

        x[FeatureNames.IndexOf(FeatureNames.Bias)] = 1.0;

        return new ExtractedFeatures(x, bestItem);
    }

    private static IEnumerable<string> Names(CatalogItem item, PersonRole role)
        => item.PeopleOf(role);

    private static double MeanAffinity(
        IEnumerable<string> keys,
        IReadOnlyDictionary<string, double> affinity)
    {
        double sum = 0;
        var count = 0;
        foreach (var key in keys)
        {
            count++;
            if (affinity.TryGetValue(key, out var v))
            {
                sum += v;
            }
        }

        return count == 0 ? 0 : sum / count;
    }

    /// <summary>
    /// Length-normalised affinity: sum over sqrt(count).
    ///
    /// A plain mean systematically penalises hybrids — an Action/Comedy scores
    /// (1.0 + 0.2) / 2 = 0.6 for a comedy lover, losing to a pure comedy at 1.0, even
    /// though it genuinely is a comedy. A plain sum overcorrects, rewarding any film that
    /// lists many genres. Dividing by sqrt(n) sits between the two: matching a second
    /// liked genre helps, and an unliked extra genre dilutes only mildly.
    /// </summary>
    private static double SoftAffinity(
        IReadOnlyList<string> keys,
        IReadOnlyDictionary<string, double> affinity)
    {
        if (keys.Count == 0)
        {
            return 0;
        }

        double sum = 0;
        foreach (var key in keys)
        {
            if (affinity.TryGetValue(key, out var v))
            {
                sum += v;
            }
        }

        return sum / Math.Sqrt(keys.Count);
    }

    private static double MaxAffinity(
        IEnumerable<string> keys,
        IReadOnlyDictionary<string, double> affinity)
    {
        double max = 0;
        foreach (var key in keys)
        {
            if (affinity.TryGetValue(key, out var v) && v > max)
            {
                max = v;
            }
        }

        return max;
    }

    /// <summary>1.0 when the release decade matches the user's centre of mass, decaying over ~25 years.</summary>
    private static double EraFit(CatalogItem candidate, TasteProfile profile)
    {
        if (profile.MeanYear is not { } mean || candidate.ProductionYear is not { } year)
        {
            return 0.5;
        }

        var distance = Math.Abs(year - mean);
        return Clamp01(1.0 - (distance / 25.0));
    }

    /// <summary>Penalises candidates far from the runtime the user actually finishes.</summary>
    private static double RuntimeFit(CatalogItem candidate, TasteProfile profile)
    {
        if (profile.MeanRuntimeMinutes is not { } mean
            || mean <= 0
            || candidate.Runtime is not { } runtime
            || runtime <= TimeSpan.Zero)
        {
            return 0.5;
        }

        var ratio = runtime.TotalMinutes / mean;
        // 1.0 at parity, falling away symmetrically in log space.
        var penalty = Math.Abs(Math.Log(ratio));
        return Clamp01(1.0 - (penalty / 1.5));
    }

    private static void InsertTop(Span<double> top, double value)
    {
        for (var i = 0; i < top.Length; i++)
        {
            if (value > top[i])
            {
                for (var j = top.Length - 1; j > i; j--)
                {
                    top[j] = top[j - 1];
                }

                top[i] = value;
                return;
            }
        }
    }

    private static double Mean(ReadOnlySpan<double> values)
    {
        double sum = 0;
        var used = 0;
        foreach (var v in values)
        {
            if (v > 0)
            {
                sum += v;
                used++;
            }
        }

        return used == 0 ? 0 : sum / used;
    }

    private static double Clamp01(double v)
        => double.IsNaN(v) ? 0 : Math.Clamp(v, 0, 1);
}

using JellyDiscover.Core.Models;
using JellyDiscover.Core.Text;

namespace JellyDiscover.Core.Features;

public static class TasteProfileBuilder
{
    /// <summary>How many liked items to keep for per-item similarity scoring.</summary>
    public const int MaxLikedRetained = 200;

    public const int MaxDislikedRetained = 100;

    public static TasteProfile Build(
        IEnumerable<Interaction> interactions,
        IReadOnlyDictionary<string, CatalogItem> catalogue)
    {
        ArgumentNullException.ThrowIfNull(interactions);
        ArgumentNullException.ThrowIfNull(catalogue);

        var liked = new List<WeightedItem>();
        var disliked = new List<WeightedItem>();

        var genres = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        var blends = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        var tags = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        var directors = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        var actors = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        var collections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var genreCounts = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);

        double yearSum = 0, yearWeight = 0;
        double runtimeSum = 0, runtimeWeight = 0;

        foreach (var interaction in interactions)
        {
            if (!catalogue.TryGetValue(interaction.ItemId, out var item))
            {
                continue;
            }

            // StrongPositive counts double; a rewatch really is worth two viewings.
            var magnitude = interaction.Sentiment switch
            {
                Sentiment.StrongPositive => 2.0,
                Sentiment.Positive => 1.0,
                Sentiment.WeakNegative => 0.4,
                Sentiment.StrongNegative => 1.0,
                _ => 0.0,
            };

            var weight = magnitude * Math.Clamp(interaction.Confidence, 0, 1);
            if (weight <= 0)
            {
                continue;
            }

            if (interaction.IsNegative)
            {
                disliked.Add(new WeightedItem(item, weight));
                continue;
            }

            liked.Add(new WeightedItem(item, weight));

            foreach (var g in item.Genres)
            {
                Accumulate(genres, g, weight);
                Accumulate(genreCounts, g, weight);
            }

            foreach (var blend in DocumentBuilder.BlendTerms(item.Genres))
            {
                Accumulate(blends, blend, weight);
            }

            foreach (var t in item.Tags)
            {
                Accumulate(tags, t, weight);
            }

            foreach (var p in item.People)
            {
                switch (p.Role)
                {
                    case PersonRole.Director:
                        Accumulate(directors, p.Name, weight);
                        break;
                    case PersonRole.AlbumArtist:
                        Accumulate(directors, p.Name, weight);
                        break;
                    case PersonRole.Actor:
                        Accumulate(actors, p.Name, weight);
                        break;
                    default:
                        break;
                }
            }

            if (!string.IsNullOrWhiteSpace(item.CollectionName))
            {
                collections.Add(item.CollectionName);
            }

            if (item.ProductionYear is { } year and > 1800)
            {
                yearSum += year * weight;
                yearWeight += weight;
            }

            if (item.Runtime is { } runtime && runtime > TimeSpan.Zero)
            {
                runtimeSum += runtime.TotalMinutes * weight;
                runtimeWeight += weight;
            }
        }

        // Keep the strongest signals; scoring cost is linear in this list.
        liked.Sort(static (a, b) => b.Weight.CompareTo(a.Weight));
        disliked.Sort(static (a, b) => b.Weight.CompareTo(a.Weight));

        return new TasteProfile
        {
            Liked = liked.Take(MaxLikedRetained).ToArray(),
            Disliked = disliked.Take(MaxDislikedRetained).ToArray(),
            GenreAffinity = NormalizeToMax(genres),
            GenreBlendAffinity = NormalizeToMax(blends),
            TagAffinity = NormalizeToMax(tags),
            DirectorAffinity = NormalizeToMax(directors),
            ActorAffinity = NormalizeToMax(actors),
            Collections = collections,
            GenreDistribution = NormalizeToSum(genreCounts),
            MeanYear = yearWeight > 0 ? yearSum / yearWeight : null,
            MeanRuntimeMinutes = runtimeWeight > 0 ? runtimeSum / runtimeWeight : null,
        };
    }

    private static void Accumulate(Dictionary<string, double> into, string key, double weight)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        into[key] = into.TryGetValue(key, out var existing) ? existing + weight : weight;
    }

    /// <summary>Scales so the strongest affinity is 1.0 — features stay comparable across users.</summary>
    private static Dictionary<string, double> NormalizeToMax(Dictionary<string, double> source)
    {
        if (source.Count == 0)
        {
            return source;
        }

        var max = source.Values.Max();
        if (max <= 0)
        {
            return source;
        }

        var result = new Dictionary<string, double>(source.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in source)
        {
            result[k] = v / max;
        }

        return result;
    }

    /// <summary>Scales to a probability distribution — used for list calibration.</summary>
    private static Dictionary<string, double> NormalizeToSum(Dictionary<string, double> source)
    {
        if (source.Count == 0)
        {
            return source;
        }

        var total = source.Values.Sum();
        if (total <= 0)
        {
            return source;
        }

        var result = new Dictionary<string, double>(source.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in source)
        {
            result[k] = v / total;
        }

        return result;
    }
}

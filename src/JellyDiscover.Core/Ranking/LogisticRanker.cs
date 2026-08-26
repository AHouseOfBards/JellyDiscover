using JellyDiscover.Core.Models;

namespace JellyDiscover.Core.Ranking;

public sealed record TrainingSample(double[] Features, double Label, double Weight = 1.0);

public sealed record LogisticOptions
{
    public int Epochs { get; init; } = 300;

    public double LearningRate { get; init; } = 0.35;

    /// <summary>L2 penalty. With a few hundred samples and sixteen features this matters —
    /// it is what stops a user with narrow history getting an over-confident model.</summary>
    public double L2 { get; init; } = 0.02;

    /// <summary>Blend the fitted weights toward the server-wide prior by this much.
    /// Cheap shrinkage: a user with thin history stays close to what works generally.</summary>
    public double PriorBlend { get; init; } = 0.25;
}

/// <summary>
/// Persisted model weights. Positional against <see cref="FeatureNames.All"/> — bump
/// <see cref="Version"/> if that array ever changes order.
/// </summary>
public sealed record RankerWeights
{
    // v2: added genre.blend and collaborative.affinity to the feature vector.
    public const int CurrentVersion = 2;

    public required double[] Values { get; init; }

    public int Version { get; init; } = CurrentVersion;

    public static RankerWeights Zero()
        => new() { Values = new double[FeatureNames.Count] };

    /// <summary>
    /// Hand-set starting point used when a user has no history at all. These are priors,
    /// not tuning knobs — the model moves away from them as soon as there is any signal,
    /// which is the whole reason the bias sliders are gone.
    /// </summary>
    public static RankerWeights ColdStartPrior()
    {
        var w = new double[FeatureNames.Count];
        w[FeatureNames.IndexOf(FeatureNames.SimilarityTop1)] = 2.2;
        w[FeatureNames.IndexOf(FeatureNames.SimilarityTop3)] = 1.4;
        w[FeatureNames.IndexOf(FeatureNames.SimilarityNegative)] = -2.0;
        w[FeatureNames.IndexOf(FeatureNames.GenreAffinity)] = 1.0;
        w[FeatureNames.IndexOf(FeatureNames.GenreBlendAffinity)] = 1.3;
        w[FeatureNames.IndexOf(FeatureNames.TagAffinity)] = 0.8;
        w[FeatureNames.IndexOf(FeatureNames.DirectorAffinity)] = 0.9;
        w[FeatureNames.IndexOf(FeatureNames.ActorAffinity)] = 0.5;
        w[FeatureNames.IndexOf(FeatureNames.CollectionContinuation)] = 1.1;
        w[FeatureNames.IndexOf(FeatureNames.CollaborativeAffinity)] = 1.6;
        w[FeatureNames.IndexOf(FeatureNames.CommunityRating)] = 0.9;
        w[FeatureNames.IndexOf(FeatureNames.EraAffinity)] = 0.3;
        w[FeatureNames.IndexOf(FeatureNames.RuntimeFit)] = 0.2;
        w[FeatureNames.IndexOf(FeatureNames.ServerPopularity)] = 0.6;
        w[FeatureNames.IndexOf(FeatureNames.ExternalWatchlist)] = 1.5;
        w[FeatureNames.IndexOf(FeatureNames.ExternalRequested)] = 1.0;
        w[FeatureNames.IndexOf(FeatureNames.ExternalTrending)] = 0.4;
        w[FeatureNames.IndexOf(FeatureNames.Bias)] = -1.2;
        return new RankerWeights { Values = w };
    }
}

/// <summary>
/// Per-user logistic regression fitted by full-batch gradient descent.
///
/// Sixteen features and a few hundred samples: this converges in milliseconds and needs no
/// ML library. It replaces twenty-one hand-tuned global sliders, which could never express
/// that one user cares about the director and another does not.
/// </summary>
public static class LogisticRanker
{
    public static RankerWeights Fit(
        IReadOnlyList<TrainingSample> samples,
        RankerWeights? prior = null,
        LogisticOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(samples);
        var o = options ?? new LogisticOptions();
        var basis = prior ?? RankerWeights.ColdStartPrior();

        if (samples.Count == 0)
        {
            return basis;
        }

        var n = FeatureNames.Count;
        var w = (double[])basis.Values.Clone();

        // Class balancing: abandoned items are far rarer than watched ones, and without
        // this the model would learn to always predict "yes".
        var positiveWeight = 0.0;
        var negativeWeight = 0.0;
        foreach (var s in samples)
        {
            if (s.Label > 0.5)
            {
                positiveWeight += s.Weight;
            }
            else
            {
                negativeWeight += s.Weight;
            }
        }

        if (positiveWeight <= 0 || negativeWeight <= 0)
        {
            // Only one class present — nothing to learn, keep the prior.
            return basis;
        }

        var total = positiveWeight + negativeWeight;
        var posScale = total / (2 * positiveWeight);
        var negScale = total / (2 * negativeWeight);

        var gradient = new double[n];

        for (var epoch = 0; epoch < o.Epochs; epoch++)
        {
            Array.Clear(gradient);
            double weightSum = 0;

            foreach (var sample in samples)
            {
                var x = sample.Features;
                if (x.Length != n)
                {
                    throw new ArgumentException(
                        $"Sample has {x.Length} features, expected {n}.", nameof(samples));
                }

                var scale = sample.Weight * (sample.Label > 0.5 ? posScale : negScale);
                weightSum += scale;

                var z = 0.0;
                for (var i = 0; i < n; i++)
                {
                    z += w[i] * x[i];
                }

                var error = Sigmoid(z) - sample.Label;
                for (var i = 0; i < n; i++)
                {
                    gradient[i] += scale * error * x[i];
                }
            }

            if (weightSum <= 0)
            {
                break;
            }

            var biasIndex = FeatureNames.IndexOf(FeatureNames.Bias);
            for (var i = 0; i < n; i++)
            {
                var g = gradient[i] / weightSum;

                // Regularise toward the prior rather than toward zero.
                if (i != biasIndex)
                {
                    g += o.L2 * (w[i] - basis.Values[i]);
                }

                w[i] -= o.LearningRate * g;
            }
        }

        // Shrink toward the prior in proportion to how little we know.
        var blend = Math.Clamp(o.PriorBlend, 0, 1);
        if (blend > 0)
        {
            for (var i = 0; i < n; i++)
            {
                w[i] = ((1 - blend) * w[i]) + (blend * basis.Values[i]);
            }
        }

        return new RankerWeights { Values = w };
    }

    /// <summary>Probability that this user engages with the item, 0..1.</summary>
    public static double Score(RankerWeights weights, double[] features)
    {
        ArgumentNullException.ThrowIfNull(weights);
        ArgumentNullException.ThrowIfNull(features);

        var z = 0.0;
        var n = Math.Min(weights.Values.Length, features.Length);
        for (var i = 0; i < n; i++)
        {
            z += weights.Values[i] * features[i];
        }

        return Sigmoid(z);
    }

    /// <summary>Per-feature contributions, largest absolute first — the explanation surface.</summary>
    public static IReadOnlyList<FeatureContribution> Explain(
        RankerWeights weights,
        double[] features,
        int take = 3)
    {
        ArgumentNullException.ThrowIfNull(weights);
        ArgumentNullException.ThrowIfNull(features);

        var biasIndex = FeatureNames.IndexOf(FeatureNames.Bias);
        var result = new List<FeatureContribution>(FeatureNames.Count);
        var n = Math.Min(weights.Values.Length, features.Length);

        for (var i = 0; i < n; i++)
        {
            if (i == biasIndex || features[i] == 0)
            {
                continue;
            }

            result.Add(new FeatureContribution(FeatureNames.All[i], features[i], weights.Values[i]));
        }

        return result
            .OrderByDescending(c => c.Contribution)
            .Take(take)
            .ToArray();
    }

    private static double Sigmoid(double z)
    {
        // Branch on sign for numerical stability at the tails.
        if (z >= 0)
        {
            return 1.0 / (1.0 + Math.Exp(-z));
        }

        var e = Math.Exp(z);
        return e / (1.0 + e);
    }
}

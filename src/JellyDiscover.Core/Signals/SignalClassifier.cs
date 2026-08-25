using JellyDiscover.Core.Models;

namespace JellyDiscover.Core.Signals;

public sealed record SignalOptions
{
    /// <summary>Below this completion percentage, a started item counts as abandoned.</summary>
    public double AbandonBelowPercent { get; init; } = 20;

    /// <summary>Above this, treat as genuinely watched.</summary>
    public double CompleteAbovePercent { get; init; } = 90;

    /// <summary>Ignore a low completion until this long has passed — otherwise we would
    /// label something the user is part-way through as a dislike.</summary>
    public TimeSpan AbandonGracePeriod { get; init; } = TimeSpan.FromDays(30);

    /// <summary>An unstarted item must have been available at least this long before its
    /// absence means anything at all.</summary>
    public TimeSpan IgnoredAfter { get; init; } = TimeSpan.FromDays(90);

    public double StrongRatingThreshold { get; init; } = 8;

    public double DislikeRatingThreshold { get; init; } = 4;

    /// <summary>Half-life for recency weighting of a signal, in days.</summary>
    public double RecencyHalfLifeDays { get; init; } = 240;
}

/// <summary>
/// Turns raw playback facts into labelled training data.
/// This is the step that gives the ranker something to learn from.
/// </summary>
public static class SignalClassifier
{
    public static Interaction? Classify(
        string itemId,
        PlaybackFacts facts,
        DateTimeOffset now,
        SignalOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(itemId);
        var o = options ?? new SignalOptions();

        // Explicit dislike beats every implicit signal.
        if (facts.UserRating is { } low && low > 0 && low <= o.DislikeRatingThreshold)
        {
            return Make(itemId, Sentiment.StrongNegative, facts, now, o, 1.0);
        }

        if (facts.IsFavorite
            || facts.PlayCount >= 2
            || facts.UserRating >= o.StrongRatingThreshold)
        {
            return Make(itemId, Sentiment.StrongPositive, facts, now, o, 1.0);
        }

        if (facts.Played || facts.PlayedPercentage >= o.CompleteAbovePercent)
        {
            return Make(itemId, Sentiment.Positive, facts, now, o, 1.0);
        }

        // Abandoned: they chose it, started it, and did not come back.
        var started = facts.PlayedPercentage > 2;
        if (started && facts.PlayedPercentage < o.AbandonBelowPercent)
        {
            var stale = facts.LastPlayed is null
                || now - facts.LastPlayed.Value >= o.AbandonGracePeriod;
            if (stale)
            {
                return Make(itemId, Sentiment.StrongNegative, facts, now, o, 0.9);
            }

            // Still plausibly in progress — no label rather than a wrong one.
            return null;
        }

        // Never started, and it has been sitting there a long time. Weak, but plentiful.
        if (!started && facts.AddedToLibrary is { } added && now - added >= o.IgnoredAfter)
        {
            return Make(itemId, Sentiment.WeakNegative, facts, now, o, 0.25);
        }

        return null;
    }

    private static Interaction Make(
        string itemId,
        Sentiment sentiment,
        PlaybackFacts facts,
        DateTimeOffset now,
        SignalOptions o,
        double baseConfidence)
        => new()
        {
            ItemId = itemId,
            Sentiment = sentiment,
            LastPlayed = facts.LastPlayed,
            Confidence = baseConfidence * RecencyWeight(facts.LastPlayed, now, o.RecencyHalfLifeDays),
        };

    /// <summary>
    /// Exponential half-life decay. Note there is no date parsing here and no catch-all
    /// returning a default constant — the 1.x recency function silently collapsed to a
    /// fixed value whenever parsing threw, which no test could see.
    /// </summary>
    public static double RecencyWeight(DateTimeOffset? last, DateTimeOffset now, double halfLifeDays)
    {
        if (last is null)
        {
            return 0.6;
        }

        var days = (now - last.Value).TotalDays;
        if (days <= 0)
        {
            return 1.0;
        }

        var w = Math.Pow(0.5, days / halfLifeDays);
        return Math.Clamp(w, 0.15, 1.0);
    }
}

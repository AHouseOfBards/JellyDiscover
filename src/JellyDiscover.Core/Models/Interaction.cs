namespace JellyDiscover.Core.Models;

/// <summary>
/// How a user feels about an item, as inferred from playback facts.
/// The negative half is the signal 1.x fetched on every run and never once read.
/// </summary>
public enum Sentiment
{
    StrongNegative = -2,
    WeakNegative = -1,
    Positive = 1,
    StrongPositive = 2,
}

public sealed record Interaction
{
    public required string ItemId { get; init; }

    public required Sentiment Sentiment { get; init; }

    public DateTimeOffset? LastPlayed { get; init; }

    /// <summary>0..1 — how much to trust this label. Lets weak negatives contribute
    /// without drowning out the handful of high-confidence rewatches.</summary>
    public double Confidence { get; init; } = 1.0;

    public bool IsPositive => Sentiment > 0;

    public bool IsNegative => Sentiment < 0;
}

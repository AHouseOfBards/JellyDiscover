namespace JellyDiscover.Core.Signals;

/// <summary>
/// The raw per-user playback record for one item, normalised out of the host.
///
/// Every field here was already being fetched by JellyDiscover 1.x inside the UserData
/// object and then discarded — the only sub-field it ever read was Played, on a query
/// that had already filtered to unplayed items.
/// </summary>
public readonly record struct PlaybackFacts
{
    public bool Played { get; init; }

    public int PlayCount { get; init; }

    public bool IsFavorite { get; init; }

    /// <summary>Explicit user rating, 0..10, if they set one.</summary>
    public double? UserRating { get; init; }

    /// <summary>0..100. The abandonment signal lives here.</summary>
    public double PlayedPercentage { get; init; }

    public DateTimeOffset? LastPlayed { get; init; }

    public DateTimeOffset? AddedToLibrary { get; init; }
}

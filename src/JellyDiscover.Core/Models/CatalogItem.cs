namespace JellyDiscover.Core.Models;

/// <summary>
/// A host-agnostic view of one recommendable thing.
///
/// Deliberately not a Jellyfin BaseItem: the adapter maps into this, which is what
/// lets the whole algorithm be exercised from tests with no server running.
/// </summary>
public sealed record CatalogItem
{
    public required string Id { get; init; }

    public required string Name { get; init; }

    public required MediaKind Kind { get; init; }

    public IReadOnlyList<string> Genres { get; init; } = Array.Empty<string>();

    /// <summary>Free-form tags. On a TMDB-backed server these are the keywords, which are
    /// far more discriminative than genres and were never used by the 1.x engine.</summary>
    public IReadOnlyList<string> Tags { get; init; } = Array.Empty<string>();

    public IReadOnlyList<Person> People { get; init; } = Array.Empty<Person>();

    public string? CollectionName { get; init; }

    public string? Studio { get; init; }

    public int? ProductionYear { get; init; }

    public double? CommunityRating { get; init; }

    public string? Overview { get; init; }

    public string? OfficialRating { get; init; }

    public TimeSpan? Runtime { get; init; }

    /// <summary>How many distinct users on this server have played it. Cheap popularity prior.</summary>
    public int ServerPlayCount { get; init; }

    /// <summary>Provider ids (tmdb, imdb, tvdb, musicbrainz...). Used for dedupe, external
    /// matching, and — critically — writing an exact-match .nfo alongside generated content.</summary>
    public IReadOnlyDictionary<string, string> ProviderIds { get; init; }
        = new Dictionary<string, string>();

    /// <summary>The path as the host sees it. In-process this is also the path the host can
    /// open, which is the entire reason path substitution no longer exists.</summary>
    public string? Path { get; init; }

    public IEnumerable<string> PeopleOf(PersonRole role)
        => People.Where(p => p.Role == role).Select(p => p.Name);
}

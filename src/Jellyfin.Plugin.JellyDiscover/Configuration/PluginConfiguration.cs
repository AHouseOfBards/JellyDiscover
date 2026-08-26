using MediaBrowser.Model.Plugins;

namespace Jellyfin.Plugin.JellyDiscover.Configuration;

/// <summary>
/// Admin settings.
///
/// Note what is absent: the twenty-one scoring-bias sliders from JellyDiscover 1.x. The
/// ranker learns per-user weights from each user's own history, which a single global
/// slider could never express. Two of those sliders were also wired to nothing.
/// </summary>
public class PluginConfiguration : BasePluginConfiguration
{
    public bool EnableMovies { get; set; } = true;

    public bool EnableShows { get; set; } = true;

    /// <summary>
    /// Music requires symlinks: Jellyfin does not play .strm files in music libraries.
    /// On Windows that needs Developer Mode or an elevated service account.
    /// </summary>
    public bool EnableMusic { get; set; }

    public int RecommendationCount { get; set; } = 25;

    /// <summary>Display name prefix. Each user's library is this name plus an invisible,
    /// stable per-user suffix, so every user sees the same title.</summary>
    public string MovieLibraryName { get; set; } = "Discover Movies";

    public string ShowLibraryName { get; set; } = "Discover Shows";

    public string MusicLibraryName { get; set; } = "Discover Music";

    /// <summary>Refresh a user's libraries shortly after they finish something.</summary>
    public bool RefreshOnWatch { get; set; } = true;

    /// <summary>
    /// Coalescing window for that refresh. UserDataSaved fires constantly during playback,
    /// so events are filtered by reason and then debounced rather than acted on directly.
    /// </summary>
    public int RefreshDebounceMinutes { get; set; } = 5;

    /// <summary>How adventurous the list is. 1.0 = pure predicted relevance, lower trades
    /// some relevance for variety. This is MMR's lambda, not random noise.</summary>
    public double DiversityLambda { get; set; } = 0.72;

    /// <summary>Match the list's genre mix to the user's own viewing mix.</summary>
    public double CalibrationStrength { get; set; } = 0.35;

    /// <summary>Write .nfo files with provider ids next to generated content so Jellyfin
    /// matches exactly instead of guessing from a year-less folder name.</summary>
    public bool WriteNfoFiles { get; set; } = true;

    /// <summary>One-time removal of libraries left behind by the Python 1.x releases.</summary>
    public bool RemoveLegacyLibraries { get; set; } = true;

    public bool LegacyCleanupCompleted { get; set; }

    /// <summary>Set by the teardown action so nothing recreates libraries afterwards.</summary>
    public bool Suspended { get; set; }

    // ---- Trakt --------------------------------------------------------------------

    public bool TraktEnabled { get; set; }

    public string TraktClientId { get; set; } = string.Empty;

    /// <summary>Exclude items the user already watched on another platform.</summary>
    public bool TraktExcludeWatched { get; set; } = true;

    // ---- Jellyseerr ---------------------------------------------------------------

    public bool JellyseerrEnabled { get; set; }

    public string JellyseerrUrl { get; set; } = string.Empty;

    public string JellyseerrApiKey { get; set; } = string.Empty;

    // ---- Email digests ------------------------------------------------------------

    public bool EmailEnabled { get; set; }

    public string SmtpHost { get; set; } = string.Empty;

    public int SmtpPort { get; set; } = 587;

    public bool SmtpUseSsl { get; set; } = true;

    public string SmtpUsername { get; set; } = string.Empty;

    /// <summary>
    /// Stored as written. Jellyfin's plugin configuration lives in the server's own config
    /// directory, which is already the trust boundary for the API keys above; 1.x
    /// "encrypted" this with a key kept in the same folder, which was obfuscation, not
    /// protection. Use an app password, never a primary account password.
    /// </summary>
    public string SmtpPassword { get; set; } = string.Empty;

    public string SmtpFromAddress { get; set; } = string.Empty;

    public string SmtpFromName { get; set; } = "JellyDiscover";

    /// <summary>Public base URL so images and links in the digest resolve outside the LAN.</summary>
    public string PublicServerUrl { get; set; } = string.Empty;
}

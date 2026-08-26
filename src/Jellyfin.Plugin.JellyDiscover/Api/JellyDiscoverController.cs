using Jellyfin.Plugin.JellyDiscover.Data;
using Jellyfin.Plugin.JellyDiscover.Integration;
using Jellyfin.Plugin.JellyDiscover.Tasks;
using JellyDiscover.Core.Models;
using MediaBrowser.Controller.Library;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Api;

/// <summary>
/// Admin endpoints backing the config page.
///
/// Jellyfin discovers plugin controllers automatically and applies its own authentication,
/// so there is no separate port, no separate login, and no unauthenticated dashboard —
/// which is what JellyDiscover 1.x shipped on 0.0.0.0:5000 with the Jellyfin API key
/// rendered into a plain text input.
/// </summary>
[ApiController]
[Authorize(Policy = "RequiresElevation")]
[Route("JellyDiscover")]
[Produces("application/json")]
public sealed class JellyDiscoverController : ControllerBase
{
    private readonly DiscoveryStore _store;
    private readonly LegacyCleanup _legacyCleanup;
    private readonly TeardownService _teardown;
    private readonly RefreshCoordinator _coordinator;
    private readonly IUserManager _userManager;
    private readonly ILogger<JellyDiscoverController> _logger;

    public JellyDiscoverController(
        DiscoveryStore store,
        LegacyCleanup legacyCleanup,
        TeardownService teardown,
        RefreshCoordinator coordinator,
        IUserManager userManager,
        ILogger<JellyDiscoverController> logger)
    {
        _store = store;
        _legacyCleanup = legacyCleanup;
        _teardown = teardown;
        _coordinator = coordinator;
        _userManager = userManager;
        _logger = logger;
    }

    /// <summary>What the plugin currently owns.</summary>
    [HttpGet("Status")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ActionResult<object> GetStatus()
    {
        var libraries = _store.GetAllLibraries()
            .Select(l => new
            {
                User = _userManager.GetUserById(l.UserId)?.Username ?? l.UserId.ToString("N"),
                Kind = l.Kind.ToString(),
                l.LibraryName,
                Visible = LegacyNaming.Describe(l.LibraryName),
                l.ContentPath,
                l.Slot,
            })
            .ToArray();

        return Ok(new { Libraries = libraries, Count = libraries.Length });
    }

    /// <summary>Kicks off a full refresh for every user. Returns immediately.</summary>
    [HttpPost("Run")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public ActionResult Run()
    {
        _ = Task.Run(async () =>
        {
            try
            {
                await _coordinator.RefreshAllAsync(null, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Manual refresh failed");
                _store.Record("error", "manual-run", ex.Message);
            }
        });

        return NoContent();
    }

    /// <summary>
    /// Preview only. Lists libraries that match the 1.x naming scheme exactly, with their
    /// paths, so an admin can confirm before anything is removed.
    /// </summary>
    [HttpGet("Legacy")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<object>> PreviewLegacy(CancellationToken cancellationToken)
    {
        var result = await _legacyCleanup.RunAsync(apply: false, cancellationToken).ConfigureAwait(false);

        return Ok(new
        {
            result.WasDryRun,
            Count = result.Matched.Count,
            Matched = result.Matched.Select(m => new
            {
                Visible = LegacyNaming.Describe(m.Name),
                m.ItemId,
                m.Locations,
            }),
        });
    }

    /// <summary>Removes the previewed libraries. Content folders are left on disk on purpose.</summary>
    [HttpPost("Legacy/Apply")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<object>> ApplyLegacy(CancellationToken cancellationToken)
    {
        var result = await _legacyCleanup.RunAsync(apply: true, cancellationToken).ConfigureAwait(false);

        return Ok(new
        {
            result.Removed,
            Matched = result.Matched.Count,
            result.Problems,
            Note = "Old content folders were left on disk deliberately; remove them manually.",
        });
    }

    /// <summary>
    /// Full teardown. The reliable uninstall path — the OnUninstalling hook is best effort
    /// and Jellyfin does not guarantee it runs.
    /// </summary>
    [HttpPost("RemoveEverything")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<object>> RemoveEverything(
        [FromQuery] bool dropLocalState,
        CancellationToken cancellationToken)
    {
        // Stop anything from rebuilding libraries the moment we finish deleting them.
        if (Plugin.Instance is { } plugin)
        {
            plugin.Configuration.Suspended = true;
            plugin.SaveConfiguration();
        }

        var report = await _teardown
            .RemoveEverythingAsync(dropLocalState, cancellationToken)
            .ConfigureAwait(false);

        return Ok(new
        {
            report.LibrariesRemoved,
            report.FoldersDeleted,
            report.PoliciesCleaned,
            report.Problems,
            report.Clean,
        });
    }

    /// <summary>Per-user settings the plugin owns: Trakt account and digest opt-in.</summary>
    [HttpGet("Users")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ActionResult<object> GetUsers()
        => Ok(_userManager.Users.Select(u =>
        {
            var prefs = _store.GetUserPreferences(u.Id);
            return new
            {
                Id = u.Id.ToString("N"),
                u.Username,
                prefs.TraktUsername,
                prefs.EmailDigest,
                prefs.EmailOverride,
                prefs.LastDigestSent,
            };
        }).ToArray());

    [HttpPost("Users/{userId}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult SaveUser(
        [FromRoute] Guid userId,
        [FromBody] UserPreferences preferences)
    {
        ArgumentNullException.ThrowIfNull(preferences);

        if (_userManager.GetUserById(userId) is null)
        {
            return NotFound();
        }

        // Preserve the send timestamp: it is bookkeeping, not a user setting, and letting
        // the form overwrite it would defeat the once-a-day digest limit.
        var existing = _store.GetUserPreferences(userId);
        preferences.LastDigestSent = existing.LastDigestSent;

        _store.SaveUserPreferences(userId, preferences);
        return NoContent();
    }

    /// <summary>Items that must never be recommended to anyone.</summary>
    [HttpGet("Blocklist")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ActionResult<object> GetBlocklist()
        => Ok(_store.GetBlocklistWithReasons()
            .Select(kv => new { ItemId = kv.Key, Reason = kv.Value })
            .ToArray());

    [HttpPost("Blocklist/{itemId}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public ActionResult Block([FromRoute] string itemId, [FromQuery] string? reason)
    {
        _store.Block(itemId, reason ?? "Blocked by admin");
        _store.Record("info", "blocklist", $"blocked {itemId}");
        return NoContent();
    }

    [HttpDelete("Blocklist/{itemId}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult Unblock([FromRoute] string itemId)
        => _store.Unblock(itemId) ? NoContent() : NotFound();

    /// <summary>Recent structured run events — what actually happened, not a log grep.</summary>
    [HttpGet("Events")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public ActionResult<object> GetEvents([FromQuery] int take = 50)
        => Ok(_store.GetEvents(Math.Clamp(take, 1, 500)));

    /// <summary>Re-enables generation after a teardown.</summary>
    [HttpPost("Resume")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public ActionResult Resume()
    {
        if (Plugin.Instance is { } plugin)
        {
            plugin.Configuration.Suspended = false;
            plugin.SaveConfiguration();
        }

        return NoContent();
    }
}

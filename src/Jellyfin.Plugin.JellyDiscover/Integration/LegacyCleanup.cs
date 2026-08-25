using Jellyfin.Plugin.JellyDiscover.Data;
using JellyDiscover.Core.Models;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

public sealed record LegacyLibrary(string Name, string ItemId, IReadOnlyList<string> Locations);

public sealed record LegacyCleanupResult
{
    public required IReadOnlyList<LegacyLibrary> Matched { get; init; }

    public int Removed { get; init; }

    public bool WasDryRun { get; init; }

    public IReadOnlyList<string> Problems { get; init; } = Array.Empty<string>();
}

/// <summary>
/// Removes libraries left behind by the Python 1.x releases.
///
/// This is the only code in the plugin that deletes something it did not create, so the
/// predicate is deliberately narrow and it defaults to a dry run.
///
/// 1.x identified its own libraries with `"Discover" in name or "Recommended" in name`,
/// and then issued DELETE against every match. That is how a user's hand-curated
/// "Recommended Classics" library got destroyed. We do not do that. We match only the
/// exact invisible-character naming scheme 1.x generated, which no human types by accident:
/// a U+3164 Hangul Filler prefix followed by the configured name followed by one or more
/// U+200B zero-width spaces.
/// </summary>
public sealed class LegacyCleanup
{
    private readonly ILibraryManager _libraryManager;
    private readonly DiscoveryStore _store;
    private readonly ILogger<LegacyCleanup> _logger;

    public LegacyCleanup(
        ILibraryManager libraryManager,
        DiscoveryStore store,
        ILogger<LegacyCleanup> logger)
    {
        _libraryManager = libraryManager;
        _store = store;
        _logger = logger;
    }

    /// <summary>
    /// Delegates to <see cref="LegacyNaming.LooksLegacy"/>, which lives in Core precisely
    /// so this decision is covered by tests that run without a Jellyfin server.
    /// </summary>
    public static bool LooksLegacy(string? name) => LegacyNaming.LooksLegacy(name);

    public IReadOnlyList<LegacyLibrary> Find()
    {
        var ours = _store.GetAllLibraries()
            .Select(l => l.LibraryItemId.ToString("N"))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var ourNames = _store.GetAllLibraries()
            .Select(l => l.LibraryName)
            .ToHashSet(StringComparer.Ordinal);

        return _libraryManager.GetVirtualFolders()
            .Where(folder => LooksLegacy(folder.Name))
            .Where(folder => !ours.Contains(folder.ItemId?.Replace("-", string.Empty, StringComparison.Ordinal) ?? string.Empty))
            .Where(folder => !ourNames.Contains(folder.Name))
            .Select(folder => new LegacyLibrary(
                folder.Name,
                folder.ItemId ?? string.Empty,
                folder.Locations ?? Array.Empty<string>()))
            .ToArray();
    }

    /// <summary>
    /// Dry run by default. Nothing is removed unless the caller explicitly asks, and every
    /// candidate is written to the event log first so there is a record of what happened.
    /// </summary>
    public async Task<LegacyCleanupResult> RunAsync(bool apply, CancellationToken cancellationToken)
    {
        var matched = Find();
        var problems = new List<string>();
        var removed = 0;

        foreach (var legacy in matched)
        {
            _store.Record(
                "info",
                "legacy-cleanup",
                $"{(apply ? "removing" : "would remove")}: {Describe(legacy)}");
        }

        if (!apply)
        {
            return new LegacyCleanupResult { Matched = matched, WasDryRun = true };
        }

        foreach (var legacy in matched)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await _libraryManager
                    .RemoveVirtualFolder(legacy.Name, refreshLibrary: false)
                    .ConfigureAwait(false);
                removed++;
            }
            catch (Exception ex)
            {
                problems.Add($"{Describe(legacy)}: {ex.Message}");
                _logger.LogWarning(ex, "Could not remove legacy library {Name}", Describe(legacy));
            }
        }

        // Deliberately NOT deleting the old content directories. They live outside this
        // plugin's data root, they may be on another machine's mount, and a stray recursive
        // delete there is exactly the failure this rewrite exists to prevent. The config
        // page tells the admin where they are so they can remove them by hand.
        _store.Record("info", "legacy-cleanup", $"removed {removed} of {matched.Count} legacy libraries");

        return new LegacyCleanupResult
        {
            Matched = matched,
            Removed = removed,
            Problems = problems,
        };
    }

    /// <summary>Invisible characters make log lines useless, so render them visibly.</summary>
    public static string Describe(LegacyLibrary legacy)
    {
        var visible = LegacyNaming.Describe(legacy.Name);

        var where = legacy.Locations.Count > 0
            ? string.Join("; ", legacy.Locations)
            : "(no locations)";

        return $"\"{visible}\" [{where}]";
    }
}

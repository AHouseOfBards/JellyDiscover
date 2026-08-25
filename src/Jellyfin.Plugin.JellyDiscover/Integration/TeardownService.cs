using System.Globalization;
using Jellyfin.Plugin.JellyDiscover.Data;
using JellyDiscover.Core.Models;
using MediaBrowser.Controller.Library;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

public sealed record TeardownReport
{
    public int LibrariesRemoved { get; init; }

    public int FoldersDeleted { get; init; }

    public int PoliciesCleaned { get; init; }

    public int LegacyLibrariesRemoved { get; init; }

    public IReadOnlyList<string> Problems { get; init; } = Array.Empty<string>();

    public bool Clean => Problems.Count == 0;

    public override string ToString()
        => string.Create(
            CultureInfo.InvariantCulture,
            $"removed {LibrariesRemoved} libraries ({LegacyLibrariesRemoved} legacy), deleted {FoldersDeleted} folders, cleaned {PoliciesCleaned} user policies, {Problems.Count} problem(s)");
}

/// <summary>
/// Everything that removes what this plugin created.
///
/// Reached three ways, because Jellyfin cannot guarantee any single one of them runs:
///   1. The admin disables a kind, or disables a user  -> partial teardown
///   2. The admin clicks "Remove everything"           -> full teardown (the reliable path)
///   3. Plugin.OnUninstalling()                        -> best effort, may not fire at all
///
/// Every operation is idempotent and resumable: if it dies halfway, running it again
/// finishes the job. Jellyfin 1.x had no teardown at all — its own README told users to go
/// delete the generated libraries by hand after uninstalling.
/// </summary>
public sealed class TeardownService
{
    private readonly ILibraryManager _libraryManager;
    private readonly AccessController _accessController;
    private readonly DiscoveryStore _store;
    private readonly ILogger<TeardownService> _logger;

    public TeardownService(
        ILibraryManager libraryManager,
        AccessController accessController,
        DiscoveryStore store,
        ILogger<TeardownService> logger)
    {
        _libraryManager = libraryManager;
        _accessController = accessController;
        _store = store;
        _logger = logger;
    }

    /// <summary>Removes every library this plugin owns, plus its own state.</summary>
    public async Task<TeardownReport> RemoveEverythingAsync(
        bool dropLocalState,
        CancellationToken cancellationToken)
    {
        var report = await RemoveAsync(_store.GetAllLibraries(), cancellationToken)
            .ConfigureAwait(false);

        if (dropLocalState && report.Clean)
        {
            _store.DropEverything();
        }

        _logger.LogInformation("JellyDiscover teardown: {Report}", report);
        return report;
    }

    public Task<TeardownReport> RemoveForUserAsync(Guid userId, CancellationToken cancellationToken)
        => RemoveAsync(_store.GetLibrariesFor(userId), cancellationToken);

    public Task<TeardownReport> RemoveKindAsync(MediaKind kind, CancellationToken cancellationToken)
        => RemoveAsync(
            _store.GetAllLibraries().Where(l => l.Kind == kind).ToArray(),
            cancellationToken);

    /// <summary>
    /// The ordering matters and is the whole reason ghost items existed in 1.x.
    ///
    /// Revoke access FIRST, then remove the library, then delete content, then forget the
    /// row. Doing it the other way round leaves user policies referencing an item id that
    /// no longer resolves — which is exactly the "ghost item" state the old Cleaner Utility
    /// existed to mop up.
    /// </summary>
    private async Task<TeardownReport> RemoveAsync(
        IReadOnlyList<DiscoveryLibrary> libraries,
        CancellationToken cancellationToken)
    {
        var problems = new List<string>();
        var removed = 0;
        var folders = 0;
        var policies = 0;

        foreach (var library in libraries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 1. Revoke access while the id is still valid.
            try
            {
                if (await _accessController.RevokeAsync(library.UserId, library.LibraryItemId)
                        .ConfigureAwait(false))
                {
                    policies++;
                }
            }
            catch (Exception ex)
            {
                problems.Add($"policy for {library.UserId:N}: {ex.Message}");
                _logger.LogWarning(ex, "Could not revoke access to {Library}", library.LibraryName);
            }

            // 2. Remove the virtual folder. Addressed by the name we recorded when we
            //    created it -- never by a name-similarity match against the server.
            try
            {
                await _libraryManager
                    .RemoveVirtualFolder(library.LibraryName, refreshLibrary: false)
                    .ConfigureAwait(false);
                removed++;
            }
            catch (Exception ex)
            {
                problems.Add($"library {library.LibraryName}: {ex.Message}");
                _logger.LogWarning(ex, "Could not remove library {Library}", library.LibraryName);
            }

            // 3. Delete generated content. Only ever the path we recorded.
            try
            {
                if (DeleteContentFolder(library.ContentPath))
                {
                    folders++;
                }
            }
            catch (Exception ex)
            {
                problems.Add($"folder {library.ContentPath}: {ex.Message}");
                _logger.LogWarning(ex, "Could not delete {Path}", library.ContentPath);
            }

            // 4. Forget it, but only once Jellyfin-side state is actually gone. If we
            //    failed above, the row survives so a later run retries instead of orphaning.
            if (problems.Count == 0)
            {
                _store.ForgetLibrary(library.UserId, library.Kind);
            }
        }

        var report = new TeardownReport
        {
            LibrariesRemoved = removed,
            FoldersDeleted = folders,
            PoliciesCleaned = policies,
            Problems = problems,
        };

        _store.Record(
            report.Clean ? "info" : "warn",
            "teardown",
            report.ToString());

        return report;
    }

    private bool DeleteContentFolder(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !Directory.Exists(path))
        {
            return false;
        }

        // Generated content only ever lives under the plugin's own data root. Refuse to
        // recurse anywhere else, whatever the registry happens to say.
        var root = Plugin.Instance?.ContentRoot;
        if (root is null || !IsInside(path, root))
        {
            _logger.LogError(
                "Refusing to delete {Path}: outside the plugin content root {Root}", path, root);
            return false;
        }

        Directory.Delete(path, recursive: true);
        return true;
    }

    private static bool IsInside(string candidate, string root)
    {
        var fullCandidate = Path.GetFullPath(candidate);
        var fullRoot = Path.GetFullPath(root);
        if (!fullRoot.EndsWith(Path.DirectorySeparatorChar))
        {
            fullRoot += Path.DirectorySeparatorChar;
        }

        return fullCandidate.StartsWith(fullRoot, StringComparison.OrdinalIgnoreCase);
    }
}

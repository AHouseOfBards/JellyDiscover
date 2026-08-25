using System.Collections.Concurrent;
using Jellyfin.Plugin.JellyDiscover.Data;
using Jellyfin.Plugin.JellyDiscover.Integration;
using JellyDiscover.Core.Models;
using MediaBrowser.Controller.Library;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Tasks;

/// <summary>
/// Serialises all refresh work and debounces the "they just watched something" signal.
///
/// Jellyfin raises UserDataSaved constantly during playback (there is a well-known issue
/// about a webhook plugin being spammed by it), so events are filtered by reason, coalesced
/// per user into a single pending refresh, and executed by one background worker. Nothing
/// heavy ever runs on the event thread.
///
/// This replaces 1.x's PID lock file, status.json, last_engine_run mtime, and 60-second
/// cooldown constant with one queue.
/// </summary>
public sealed class RefreshCoordinator : BackgroundService
{
    private readonly ConcurrentDictionary<Guid, DateTimeOffset> _pending = new();
    private readonly SemaphoreSlim _wake = new(0);

    private readonly IUserManager _userManager;
    private readonly CatalogueReader _reader;
    private readonly LibrarySynchronizer _synchronizer;
    private readonly DiscoveryStore _store;
    private readonly ILogger<RefreshCoordinator> _logger;

    public RefreshCoordinator(
        IUserManager userManager,
        CatalogueReader reader,
        LibrarySynchronizer synchronizer,
        DiscoveryStore store,
        ILogger<RefreshCoordinator> logger)
    {
        _userManager = userManager;
        _reader = reader;
        _synchronizer = synchronizer;
        _store = store;
        _logger = logger;
    }

    /// <summary>Marks a user as needing a refresh once their debounce window elapses.</summary>
    public void RequestRefresh(Guid userId)
    {
        var configuration = Plugin.Instance?.Configuration;
        if (configuration is null || !configuration.RefreshOnWatch || configuration.Suspended)
        {
            return;
        }

        var due = DateTimeOffset.UtcNow.AddMinutes(Math.Max(1, configuration.RefreshDebounceMinutes));

        // Each new event pushes the deadline out, so a binge session produces one refresh
        // at the end rather than one per episode.
        _pending[userId] = due;
        _wake.Release();
    }

    /// <summary>Refreshes every user. Used by the scheduled task and by manual runs.</summary>
    public async Task RefreshAllAsync(IProgress<double>? progress, CancellationToken cancellationToken)
    {
        var configuration = Plugin.Instance?.Configuration;
        if (configuration is null || configuration.Suspended)
        {
            return;
        }

        var kinds = LibrarySynchronizer.EnabledKinds(configuration);
        if (kinds.Count == 0)
        {
            _logger.LogInformation("No media kinds enabled; nothing to do");
            return;
        }

        var users = _userManager.Users.ToArray();
        var raw = _reader.ReadRawCatalogue(kinds);

        // One pass, shared across every user, rather than 1.x's per-user popularity scan.
        var plays = _reader.CountServerPlays(users, raw);
        var catalogue = _reader.ToCatalogue(raw, plays);

        _logger.LogInformation(
            "Refreshing {UserCount} users against {ItemCount} items", users.Length, catalogue.Count);

        for (var i = 0; i < users.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await SafeSynchronizeAsync(users[i], raw, catalogue, cancellationToken).ConfigureAwait(false);
            progress?.Report((i + 1) * 100.0 / users.Length);
        }
    }

    private async Task RefreshOneAsync(Guid userId, CancellationToken cancellationToken)
    {
        var configuration = Plugin.Instance?.Configuration;
        if (configuration is null)
        {
            return;
        }

        var user = _userManager.GetUserById(userId);
        if (user is null)
        {
            return;
        }

        var kinds = LibrarySynchronizer.EnabledKinds(configuration);
        if (kinds.Count == 0)
        {
            return;
        }

        var raw = _reader.ReadRawCatalogue(kinds);
        var plays = _reader.CountServerPlays([user], raw);
        var catalogue = _reader.ToCatalogue(raw, plays);

        await SafeSynchronizeAsync(user, raw, catalogue, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// One user's failure must never abort the run for everyone else — but unlike 1.x it
    /// is recorded as a structured event rather than swallowed into silence.
    /// </summary>
    private async Task SafeSynchronizeAsync(
        Jellyfin.Database.Implementations.Entities.User user,
        IReadOnlyList<MediaBrowser.Controller.Entities.BaseItem> raw,
        IReadOnlyList<CatalogItem> catalogue,
        CancellationToken cancellationToken)
    {
        try
        {
            await _synchronizer
                .SynchronizeUserAsync(user, raw, catalogue, Plugin.Instance!.Configuration, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to refresh recommendations for {User}", user.Username);
            _store.Record("error", "generate", $"{user.Username}: {ex.Message}");
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _wake.WaitAsync(TimeSpan.FromMinutes(1), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var now = DateTimeOffset.UtcNow;
            foreach (var (userId, due) in _pending.ToArray())
            {
                if (due > now)
                {
                    continue;
                }

                if (!_pending.TryRemove(userId, out _))
                {
                    continue;
                }

                try
                {
                    await RefreshOneAsync(userId, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Debounced refresh failed for {UserId}", userId);
                }
            }
        }
    }

    public override void Dispose()
    {
        _wake.Dispose();
        base.Dispose();
    }
}

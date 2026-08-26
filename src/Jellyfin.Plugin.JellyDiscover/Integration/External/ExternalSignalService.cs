using Jellyfin.Plugin.JellyDiscover.Configuration;
using Jellyfin.Plugin.JellyDiscover.Data;
using JellyDiscover.Core.Features;
using JellyDiscover.Core.Models;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration.External;

/// <summary>Server-wide signals, fetched once per refresh rather than once per user.</summary>
public sealed record SharedExternalSignals
{
    public IReadOnlySet<string> TrendingItemIds { get; init; } = new HashSet<string>();

    public IReadOnlySet<string> RequestedItemIds { get; init; } = new HashSet<string>();

    public static SharedExternalSignals None { get; } = new();
}

/// <summary>
/// Assembles the external signals the ranker already had feature slots for.
///
/// Those three features existed in the vector from the start and nothing populated them,
/// so Trakt and Jellyseerr were configured but inert.
/// </summary>
public sealed class ExternalSignalService
{
    private readonly TraktClient _trakt;
    private readonly JellyseerrClient _jellyseerr;
    private readonly DiscoveryStore _store;
    private readonly ILogger<ExternalSignalService> _logger;

    public ExternalSignalService(
        TraktClient trakt,
        JellyseerrClient jellyseerr,
        DiscoveryStore store,
        ILogger<ExternalSignalService> logger)
    {
        _trakt = trakt;
        _jellyseerr = jellyseerr;
        _store = store;
        _logger = logger;
    }

    /// <summary>Fetched once per full refresh and shared by every user.</summary>
    public async Task<SharedExternalSignals> GetSharedAsync(
        PluginConfiguration configuration,
        ProviderIdLookup lookup,
        CancellationToken cancellationToken)
    {
        var trending = new HashSet<string>(StringComparer.Ordinal);
        var requested = new HashSet<string>(StringComparer.Ordinal);

        if (configuration.TraktEnabled && !string.IsNullOrWhiteSpace(configuration.TraktClientId))
        {
            var ids = await _trakt
                .GetTrendingTmdbIdsAsync(configuration.TraktClientId, cancellationToken)
                .ConfigureAwait(false);

            trending = lookup.ResolveTmdb(ids).ToHashSet(StringComparer.Ordinal);
            _store.Record("info", "trakt", $"trending: {ids.Count} fetched, {trending.Count} in library");
        }

        if (configuration.JellyseerrEnabled
            && !string.IsNullOrWhiteSpace(configuration.JellyseerrUrl)
            && !string.IsNullOrWhiteSpace(configuration.JellyseerrApiKey))
        {
            var ids = await _jellyseerr
                .GetRequestedTmdbIdsAsync(
                    configuration.JellyseerrUrl, configuration.JellyseerrApiKey, cancellationToken)
                .ConfigureAwait(false);

            requested = lookup.ResolveTmdb(ids).ToHashSet(StringComparer.Ordinal);
            _store.Record("info", "jellyseerr", $"requests: {ids.Count} fetched, {requested.Count} in library");
        }

        return new SharedExternalSignals
        {
            TrendingItemIds = trending,
            RequestedItemIds = requested,
        };
    }

    /// <summary>
    /// Adds this user's own Trakt watchlist to the shared signals, and returns the items
    /// they have already watched elsewhere so those can be excluded outright.
    /// </summary>
    public async Task<(ExternalSignals Signals, IReadOnlySet<string> WatchedElsewhere)> GetForUserAsync(
        Guid userId,
        PluginConfiguration configuration,
        SharedExternalSignals shared,
        ProviderIdLookup lookup,
        CancellationToken cancellationToken)
    {
        var watchlist = new HashSet<string>(StringComparer.Ordinal);
        var watchedElsewhere = new HashSet<string>(StringComparer.Ordinal);

        var traktUsername = _store.GetTraktUsername(userId);

        if (configuration.TraktEnabled
            && !string.IsNullOrWhiteSpace(configuration.TraktClientId)
            && !string.IsNullOrWhiteSpace(traktUsername))
        {
            try
            {
                var watchlistIds = await _trakt
                    .GetWatchlistTmdbIdsAsync(configuration.TraktClientId, traktUsername, cancellationToken)
                    .ConfigureAwait(false);
                watchlist = lookup.ResolveTmdb(watchlistIds).ToHashSet(StringComparer.Ordinal);

                if (configuration.TraktExcludeWatched)
                {
                    var watchedIds = await _trakt
                        .GetWatchedTmdbIdsAsync(configuration.TraktClientId, traktUsername, cancellationToken)
                        .ConfigureAwait(false);
                    watchedElsewhere = lookup.ResolveTmdb(watchedIds).ToHashSet(StringComparer.Ordinal);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Trakt lookup failed for {User}", traktUsername);
                _store.Record("warn", "trakt", $"{traktUsername}: {ex.Message}");
            }
        }

        return (
            new ExternalSignals
            {
                WatchlistItemIds = watchlist,
                RequestedItemIds = shared.RequestedItemIds,
                TrendingItemIds = shared.TrendingItemIds,
            },
            watchedElsewhere);
    }
}

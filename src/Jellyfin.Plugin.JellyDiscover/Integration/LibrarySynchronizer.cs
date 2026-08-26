using Jellyfin.Database.Implementations.Entities;
using Jellyfin.Plugin.JellyDiscover.Configuration;
using Jellyfin.Plugin.JellyDiscover.Data;
using Jellyfin.Plugin.JellyDiscover.Integration.External;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Pipeline;
using JellyDiscover.Core.Selection;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Configuration;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

/// <summary>
/// Generates and publishes one user's discovery libraries.
///
/// Library identity comes exclusively from the registry. Nothing here inspects a library
/// name to decide whether it is ours, which is the structural difference from 1.x.
/// </summary>
public sealed class LibrarySynchronizer
{
    /// <summary>
    /// Hangul filler. Renders as nothing, so every user's library displays the same title
    /// while remaining unique server-side.
    /// </summary>
    private const string InvisiblePrefix = "ㅤ";

    /// <summary>Zero-width space, repeated once per allocated slot.</summary>
    private const string InvisibleSuffix = "​";

    private readonly ILibraryManager _libraryManager;
    private readonly CatalogueReader _reader;
    private readonly ContentWriter _writer;
    private readonly AccessController _access;
    private readonly DiscoveryStore _store;
    private readonly ExternalSignalService _external;
    private readonly DigestMailer _mailer;
    private readonly ILogger<LibrarySynchronizer> _logger;

    public LibrarySynchronizer(
        ILibraryManager libraryManager,
        CatalogueReader reader,
        ContentWriter writer,
        AccessController access,
        DiscoveryStore store,
        ExternalSignalService external,
        DigestMailer mailer,
        ILogger<LibrarySynchronizer> logger)
    {
        _libraryManager = libraryManager;
        _reader = reader;
        _writer = writer;
        _access = access;
        _store = store;
        _external = external;
        _mailer = mailer;
        _logger = logger;
    }

    public static IReadOnlyList<MediaKind> EnabledKinds(PluginConfiguration configuration)
    {
        var kinds = new List<MediaKind>(3);
        if (configuration.EnableMovies)
        {
            kinds.Add(MediaKind.Movie);
        }

        if (configuration.EnableShows)
        {
            kinds.Add(MediaKind.Series);
        }

        if (configuration.EnableMusic)
        {
            kinds.Add(MediaKind.MusicAlbum);
        }

        return kinds;
    }

    /// <summary>
    /// Refreshes every enabled kind for one user. Safe to call concurrently for different
    /// users; the queue serialises them anyway.
    /// </summary>
    public async Task SynchronizeUserAsync(
        User user,
        RefreshContext context,
        CancellationToken cancellationToken)
    {
        var configuration = context.Configuration;
        var kinds = EnabledKinds(configuration);
        if (kinds.Count == 0 || configuration.Suspended)
        {
            return;
        }

        var history = _reader.ReadHistory(user, context.RawCatalogue, DateTimeOffset.UtcNow);

        var (signals, watchedElsewhere) = await _external
            .GetForUserAsync(user.Id, configuration, context.Shared, context.ProviderIds, cancellationToken)
            .ConfigureAwait(false);

        // Anything they already watched on another platform is excluded outright rather
        // than scored with a magic negative number.
        var excluded = new HashSet<string>(_store.GetBlocklist(), StringComparer.Ordinal);
        excluded.UnionWith(watchedElsewhere);

        var result = new Recommender().Recommend(new RecommendationRequest
        {
            Catalogue = context.Catalogue,
            History = history,
            ExcludedItemIds = excluded,
            External = signals,
            CoOccurrence = context.CoOccurrence,
            ExistingModel = _store.GetModel(user.Id),
            ServerPrior = _store.GetServerPrior(),
            CountPerKind = configuration.RecommendationCount,
            List = new ListOptions
            {
                Lambda = configuration.DiversityLambda,
                CalibrationStrength = configuration.CalibrationStrength,
                Count = configuration.RecommendationCount,
            },
        });

        if (result.ModelWasFitted)
        {
            _store.SaveModel(user.Id, result.Model);
        }

        var slot = _store.GetOrAllocateSlot(user.Id);

        foreach (var kind in kinds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!result.ByKind.TryGetValue(kind, out var recommendations) || recommendations.Count == 0)
            {
                continue;
            }

            await PublishAsync(user, kind, slot, recommendations, configuration, cancellationToken)
                .ConfigureAwait(false);
        }

        await _mailer.SendDigestAsync(user, result, configuration, cancellationToken).ConfigureAwait(false);

        _store.Record(
            "info",
            "generate",
            $"{user.Username}: {result.All.Count} recommendations from {result.CandidatesConsidered} candidates "
            + $"(catalogue {result.CatalogueSize}, model {(result.ModelWasFitted ? "fitted" : "prior")}, "
            + $"collaborative {(context.CoOccurrence.IsActive ? "on" : "off")})");
    }

    private async Task PublishAsync(
        User user,
        MediaKind kind,
        int slot,
        IReadOnlyList<Recommendation> recommendations,
        PluginConfiguration configuration,
        CancellationToken cancellationToken)
    {
        var contentPath = Path.Combine(
            Plugin.Instance!.ContentRoot,
            user.Id.ToString("N"),
            kind.ToString());

        // Content is keyed on the user's immutable id, never on their display name.
        // Renaming a user in 1.x orphaned their folder and their library permanently.
        _writer.Sync(contentPath, recommendations);

        var existing = _store.GetLibrariesFor(user.Id).FirstOrDefault(l => l.Kind == kind);
        var displayName = BuildName(kind, slot, configuration);

        if (existing is not null && LibraryStillExists(existing))
        {
            await _access.GrantAsync(user.Id, existing.LibraryItemId).ConfigureAwait(false);
            await _access.EnforceExclusivityAsync(user.Id, existing.LibraryItemId, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        await _libraryManager.AddVirtualFolder(
                displayName,
                ToCollectionType(kind),
                new LibraryOptions
                {
                    PathInfos = [new MediaPathInfo { Path = contentPath }],

                    // Generated content changes only when we change it, so filesystem
                    // watching is pure overhead here.
                    EnableRealtimeMonitor = false,
                    SaveLocalMetadata = false,
                    EnableAutomaticSeriesGrouping = kind == MediaKind.Series,
                },
                refreshLibrary: true)
            .ConfigureAwait(false);

        var created = _libraryManager.GetVirtualFolders()
            .FirstOrDefault(f => string.Equals(f.Name, displayName, StringComparison.Ordinal));

        if (created is null || !Guid.TryParse(created.ItemId, out var itemId))
        {
            _logger.LogError(
                "Created library {Name} but could not resolve its item id; not registering it",
                displayName);
            _store.Record("error", "publish", $"could not resolve item id for {displayName}");
            return;
        }

        _store.UpsertLibrary(new DiscoveryLibrary
        {
            UserId = user.Id,
            Kind = kind,
            LibraryItemId = itemId,
            LibraryName = displayName,
            ContentPath = contentPath,
            Slot = slot,
        });

        await _access.GrantAsync(user.Id, itemId).ConfigureAwait(false);
        await _access.EnforceExclusivityAsync(user.Id, itemId, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Created {Kind} discovery library for {User}", kind, user.Username);
    }

    private bool LibraryStillExists(DiscoveryLibrary library)
        => _libraryManager.GetVirtualFolders()
            .Any(f => string.Equals(f.ItemId, library.LibraryItemId.ToString("N"), StringComparison.OrdinalIgnoreCase)
                   || string.Equals(f.Name, library.LibraryName, StringComparison.Ordinal));

    /// <summary>
    /// The invisible suffix comes from a slot allocated once and stored. In 1.x it came
    /// from the user's index in a sorted list, so adding or removing any user renamed
    /// everyone else's libraries and the engine then created fresh duplicates.
    /// </summary>
    private static string BuildName(MediaKind kind, int slot, PluginConfiguration configuration)
    {
        var baseName = kind switch
        {
            MediaKind.Movie => configuration.MovieLibraryName,
            MediaKind.Series => configuration.ShowLibraryName,
            MediaKind.MusicAlbum => configuration.MusicLibraryName,
            _ => "Discover",
        };

        return string.Concat(
            InvisiblePrefix,
            baseName,
            string.Concat(Enumerable.Repeat(InvisibleSuffix, Math.Max(1, slot))));
    }

    private static CollectionTypeOptions ToCollectionType(MediaKind kind) => kind switch
    {
        MediaKind.Movie => CollectionTypeOptions.movies,
        MediaKind.Series => CollectionTypeOptions.tvshows,
        MediaKind.MusicAlbum => CollectionTypeOptions.music,
        _ => CollectionTypeOptions.movies,
    };
}

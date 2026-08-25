using Jellyfin.Data.Enums;
using Jellyfin.Database.Implementations.Entities;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Signals;
using MediaBrowser.Controller.Entities;
using MediaBrowser.Controller.Entities.Audio;
using MediaBrowser.Controller.Entities.TV;
using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Logging;
using CorePerson = JellyDiscover.Core.Models.Person;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

/// <summary>
/// Maps Jellyfin's world into the host-free model the recommender consumes.
///
/// This is the only place Jellyfin types are translated, which is what keeps the algorithm
/// testable. Note there is no Limit and no paging cap: in-process, reading the whole
/// catalogue is a database query, not 600 items over HTTP.
/// </summary>
public sealed class CatalogueReader
{
    private readonly ILibraryManager _libraryManager;
    private readonly IUserDataManager _userDataManager;
    private readonly ILogger<CatalogueReader> _logger;

    public CatalogueReader(
        ILibraryManager libraryManager,
        IUserDataManager userDataManager,
        ILogger<CatalogueReader> logger)
    {
        _libraryManager = libraryManager;
        _userDataManager = userDataManager;
        _logger = logger;
    }

    public static BaseItemKind ToItemKind(MediaKind kind) => kind switch
    {
        MediaKind.Movie => BaseItemKind.Movie,
        MediaKind.Series => BaseItemKind.Series,
        MediaKind.MusicAlbum => BaseItemKind.MusicAlbum,
        _ => BaseItemKind.Movie,
    };

    /// <summary>Reads every item of the requested kinds, server-wide.</summary>
    public IReadOnlyList<BaseItem> ReadRawCatalogue(IReadOnlyList<MediaKind> kinds)
    {
        if (kinds.Count == 0)
        {
            return Array.Empty<BaseItem>();
        }

        var query = new InternalItemsQuery
        {
            IncludeItemTypes = kinds.Select(ToItemKind).ToArray(),
            Recursive = true,
            IsVirtualItem = false,

            // No ordering and no Limit on purpose: we want the entire catalogue, and
            // sorting it would be wasted work before scoring.
        };

        return _libraryManager.GetItemList(query);
    }

    /// <summary>
    /// Converts to CatalogItem, attaching how many distinct users have played each one.
    /// </summary>
    public IReadOnlyList<CatalogItem> ToCatalogue(
        IReadOnlyList<BaseItem> items,
        IReadOnlyDictionary<Guid, int> serverPlayCounts)
    {
        var result = new List<CatalogItem>(items.Count);
        foreach (var item in items)
        {
            try
            {
                result.Add(Convert(item, serverPlayCounts.GetValueOrDefault(item.Id)));
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Skipping unreadable item {Id}", item.Id);
            }
        }

        return result;
    }

    private CatalogItem Convert(BaseItem item, int serverPlayCount)
    {
        var people = new List<CorePerson>();
        foreach (var person in _libraryManager.GetPeople(item))
        {
            var role = person.Type switch
            {
                PersonKind.Director => PersonRole.Director,
                PersonKind.Writer => PersonRole.Writer,
                PersonKind.Composer => PersonRole.Composer,
                PersonKind.Actor => PersonRole.Actor,
                _ => (PersonRole?)null,
            };

            if (role is not null)
            {
                people.Add(new CorePerson(person.Name, role.Value));
            }
        }

        if (item is MusicAlbum album && !string.IsNullOrWhiteSpace(album.AlbumArtist))
        {
            people.Add(new CorePerson(album.AlbumArtist, PersonRole.AlbumArtist));
        }

        return new CatalogItem
        {
            Id = item.Id.ToString("N"),
            Name = item.Name ?? "Untitled",
            Kind = ToMediaKind(item),
            Genres = item.Genres ?? Array.Empty<string>(),
            Tags = item.Tags ?? Array.Empty<string>(),
            People = people,
            Studio = item.Studios?.FirstOrDefault(),
            ProductionYear = item.ProductionYear,
            CommunityRating = item.CommunityRating,
            Overview = item.Overview,
            OfficialRating = item.OfficialRating,
            Runtime = item.RunTimeTicks is { } ticks ? TimeSpan.FromTicks(ticks) : null,
            ServerPlayCount = serverPlayCount,
            ProviderIds = item.ProviderIds ?? new Dictionary<string, string>(),
            Path = item.Path,
        };
    }

    private static MediaKind ToMediaKind(BaseItem item) => item switch
    {
        MusicAlbum => MediaKind.MusicAlbum,
        Series => MediaKind.Series,
        _ => MediaKind.Movie,
    };

    /// <summary>
    /// Reads one user's labelled history.
    ///
    /// This is where the signals 1.x downloaded and discarded finally get used: rewatches,
    /// favourites, explicit ratings, and — the one that actually teaches the model what to
    /// stop suggesting — items the user started and abandoned.
    /// </summary>
    public IReadOnlyList<Interaction> ReadHistory(
        User user,
        IReadOnlyList<BaseItem> catalogue,
        DateTimeOffset now)
    {
        var interactions = new List<Interaction>();

        foreach (var item in catalogue)
        {
            var data = _userDataManager.GetUserData(user, item);
            if (data is null)
            {
                continue;
            }

            var facts = new PlaybackFacts
            {
                Played = data.Played,
                PlayCount = data.PlayCount,
                IsFavorite = data.IsFavorite,
                UserRating = data.Rating,
                PlayedPercentage = Percentage(data.PlaybackPositionTicks, item.RunTimeTicks),
                LastPlayed = data.LastPlayedDate is { } last
                    ? new DateTimeOffset(last, TimeSpan.Zero)
                    : null,
                AddedToLibrary = item.DateCreated == default
                    ? null
                    : new DateTimeOffset(item.DateCreated, TimeSpan.Zero),
            };

            var interaction = SignalClassifier.Classify(item.Id.ToString("N"), facts, now);
            if (interaction is not null)
            {
                interactions.Add(interaction);
            }
        }

        return interactions;
    }

    /// <summary>How many distinct users have played each item. One pass, reused for all users.</summary>
    public IReadOnlyDictionary<Guid, int> CountServerPlays(
        IReadOnlyList<User> users,
        IReadOnlyList<BaseItem> catalogue)
    {
        var counts = new Dictionary<Guid, int>();
        foreach (var item in catalogue)
        {
            var played = 0;
            foreach (var user in users)
            {
                var data = _userDataManager.GetUserData(user, item);
                if (data?.Played == true)
                {
                    played++;
                }
            }

            if (played > 0)
            {
                counts[item.Id] = played;
            }
        }

        return counts;
    }

    private static double Percentage(long positionTicks, long? runtimeTicks)
    {
        if (runtimeTicks is not { } total || total <= 0 || positionTicks <= 0)
        {
            return 0;
        }

        return Math.Clamp((double)positionTicks / total * 100.0, 0, 100);
    }
}

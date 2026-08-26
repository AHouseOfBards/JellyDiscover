using Jellyfin.Plugin.JellyDiscover.Configuration;
using Jellyfin.Plugin.JellyDiscover.Integration.External;
using JellyDiscover.Core.Collaborative;
using JellyDiscover.Core.Models;
using MediaBrowser.Controller.Entities;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

/// <summary>
/// Everything computed once per refresh and shared by every user.
///
/// The point is that none of this is per-user work. 1.x re-fetched the catalogue, the
/// popularity counts, and the external services for every user on every run.
/// </summary>
public sealed record RefreshContext
{
    public required IReadOnlyList<BaseItem> RawCatalogue { get; init; }

    public required IReadOnlyList<CatalogItem> Catalogue { get; init; }

    public required PluginConfiguration Configuration { get; init; }

    public required ProviderIdLookup ProviderIds { get; init; }

    public SharedExternalSignals Shared { get; init; } = SharedExternalSignals.None;

    /// <summary>Server-wide co-watch signal. Inactive on servers with too few users.</summary>
    public CoOccurrenceIndex CoOccurrence { get; init; } = CoOccurrenceIndex.Empty;
}

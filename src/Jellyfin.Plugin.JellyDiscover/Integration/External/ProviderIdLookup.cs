using JellyDiscover.Core.Models;

namespace Jellyfin.Plugin.JellyDiscover.Integration.External;

/// <summary>
/// Translates external provider ids (TMDB, IMDb, TVDB) into this server's item ids.
///
/// External services speak TMDB; Jellyfin speaks its own GUIDs. Everything crossing that
/// boundary has to be mapped once, here, rather than each integration inventing its own
/// matching rules.
/// </summary>
public sealed class ProviderIdLookup
{
    private readonly Dictionary<string, string> _byProviderKey;

    private ProviderIdLookup(Dictionary<string, string> byProviderKey)
    {
        _byProviderKey = byProviderKey;
    }

    public static ProviderIdLookup Build(IEnumerable<CatalogItem> catalogue)
    {
        ArgumentNullException.ThrowIfNull(catalogue);
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var item in catalogue)
        {
            foreach (var (provider, value) in item.ProviderIds)
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    continue;
                }

                // First writer wins: duplicates in a library are a metadata problem, and
                // silently preferring the later one would make results non-deterministic.
                map.TryAdd(Key(provider, value), item.Id);
            }
        }

        return new ProviderIdLookup(map);
    }

    public static string Key(string provider, string value)
        => string.Concat(provider.ToLowerInvariant(), ":", value.Trim());

    public bool TryResolve(string provider, string? value, out string itemId)
    {
        itemId = string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return _byProviderKey.TryGetValue(Key(provider, value), out itemId!);
    }

    /// <summary>Resolves a batch of TMDB ids, dropping anything not in this library.</summary>
    public IReadOnlySet<string> ResolveTmdb(IEnumerable<string> tmdbIds)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tmdb in tmdbIds)
        {
            if (TryResolve("Tmdb", tmdb, out var itemId))
            {
                result.Add(itemId);
            }
        }

        return result;
    }
}

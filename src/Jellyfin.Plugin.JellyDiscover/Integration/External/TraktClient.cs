using System.Net.Http.Json;
using System.Text.Json.Serialization;
using MediaBrowser.Common.Net;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration.External;

/// <summary>
/// Trakt.tv: global trending, plus a user's watchlist and off-server watch history.
///
/// Every call is best-effort and time-boxed. A third-party outage must degrade the
/// recommendations slightly, never stall or fail a refresh — but unlike 1.x, a failure is
/// recorded as a structured event rather than swallowed by a bare except.
/// </summary>
public sealed class TraktClient
{
    private const string BaseUrl = "https://api.trakt.tv";

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<TraktClient> _logger;

    public TraktClient(IHttpClientFactory httpClientFactory, ILogger<TraktClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    private HttpClient Create(string clientId)
    {
        var client = _httpClientFactory.CreateClient(NamedClient.Default);
        client.Timeout = TimeSpan.FromSeconds(15);
        client.DefaultRequestHeaders.Remove("trakt-api-key");
        client.DefaultRequestHeaders.Add("trakt-api-key", clientId);
        client.DefaultRequestHeaders.Remove("trakt-api-version");
        client.DefaultRequestHeaders.Add("trakt-api-version", "2");
        return client;
    }

    /// <summary>Globally trending TMDB ids across movies and shows.</summary>
    public async Task<IReadOnlyList<string>> GetTrendingTmdbIdsAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        var result = new List<string>();
        foreach (var category in new[] { "movies", "shows" })
        {
            var items = await GetAsync<List<TraktEntry>>(
                clientId, $"/{category}/trending?limit=100", cancellationToken).ConfigureAwait(false);

            if (items is null)
            {
                continue;
            }

            result.AddRange(items.Select(e => e.Tmdb(category)).Where(id => id is not null)!);
        }

        return result;
    }

    /// <summary>A user's watchlist. Requires the profile to be public.</summary>
    public async Task<IReadOnlyList<string>> GetWatchlistTmdbIdsAsync(
        string clientId,
        string username,
        CancellationToken cancellationToken)
    {
        var result = new List<string>();
        foreach (var category in new[] { "movies", "shows" })
        {
            var items = await GetAsync<List<TraktEntry>>(
                clientId,
                $"/users/{Uri.EscapeDataString(username)}/watchlist/{category}",
                cancellationToken).ConfigureAwait(false);

            if (items is null)
            {
                continue;
            }

            result.AddRange(items.Select(e => e.Tmdb(category)).Where(id => id is not null)!);
        }

        return result;
    }

    /// <summary>
    /// What the user already watched elsewhere. Used to suppress recommendations, not to
    /// build taste — 1.x returned a hard -100 for these, which is the right instinct but
    /// was applied as a magic score rather than an exclusion.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetWatchedTmdbIdsAsync(
        string clientId,
        string username,
        CancellationToken cancellationToken)
    {
        var result = new List<string>();
        foreach (var category in new[] { "movies", "shows" })
        {
            var items = await GetAsync<List<TraktEntry>>(
                clientId,
                $"/users/{Uri.EscapeDataString(username)}/watched/{category}",
                cancellationToken).ConfigureAwait(false);

            if (items is null)
            {
                continue;
            }

            result.AddRange(items.Select(e => e.Tmdb(category)).Where(id => id is not null)!);
        }

        return result;
    }

    private async Task<T?> GetAsync<T>(string clientId, string path, CancellationToken cancellationToken)
        where T : class
    {
        try
        {
            using var client = Create(clientId);
            var response = await client.GetAsync(BaseUrl + path, cancellationToken).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "Trakt {Path} returned {Status}", path, (int)response.StatusCode);
                return null;
            }

            return await response.Content
                .ReadFromJsonAsync<T>(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("Trakt {Path} timed out", path);
            return null;
        }
        catch (Exception ex) when (ex is HttpRequestException or System.Text.Json.JsonException)
        {
            _logger.LogWarning(ex, "Trakt {Path} failed", path);
            return null;
        }
    }

    private sealed class TraktEntry
    {
        [JsonPropertyName("movie")]
        public TraktMedia? Movie { get; set; }

        [JsonPropertyName("show")]
        public TraktMedia? Show { get; set; }

        public string? Tmdb(string category)
        {
            var media = category == "movies" ? Movie : Show;
            return media?.Ids?.Tmdb?.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
    }

    private sealed class TraktMedia
    {
        [JsonPropertyName("ids")]
        public TraktIds? Ids { get; set; }
    }

    private sealed class TraktIds
    {
        [JsonPropertyName("tmdb")]
        public int? Tmdb { get; set; }
    }
}

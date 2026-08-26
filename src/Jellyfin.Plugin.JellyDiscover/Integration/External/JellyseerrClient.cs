using System.Globalization;
using System.Net.Http.Json;
using System.Text.Json.Serialization;
using MediaBrowser.Common.Net;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration.External;

/// <summary>
/// Jellyseerr / Overseerr: things somebody on this server asked for.
///
/// A request is a strong signal — it is the closest thing to a user telling you outright
/// that they want to watch something.
/// </summary>
public sealed class JellyseerrClient
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<JellyseerrClient> _logger;

    public JellyseerrClient(IHttpClientFactory httpClientFactory, ILogger<JellyseerrClient> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    /// <summary>TMDB ids of requested media, paging until exhausted or capped.</summary>
    public async Task<IReadOnlyList<string>> GetRequestedTmdbIdsAsync(
        string baseUrl,
        string apiKey,
        CancellationToken cancellationToken)
    {
        var result = new List<string>();

        try
        {
            using var client = _httpClientFactory.CreateClient(NamedClient.Default);
            client.Timeout = TimeSpan.FromSeconds(15);
            client.DefaultRequestHeaders.Remove("X-Api-Key");
            client.DefaultRequestHeaders.Add("X-Api-Key", apiKey);

            var root = baseUrl.TrimEnd('/');
            const int pageSize = 100;

            // Cap the paging: a very large Jellyseerr history is not worth stalling a
            // refresh over, and the newest requests are the ones that matter.
            for (var skip = 0; skip < 1000; skip += pageSize)
            {
                var url = string.Create(
                    CultureInfo.InvariantCulture,
                    $"{root}/api/v1/request?take={pageSize}&skip={skip}&sort=added");

                var response = await client.GetAsync(url, cancellationToken).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogWarning(
                        "Jellyseerr returned {Status}", (int)response.StatusCode);
                    break;
                }

                var page = await response.Content
                    .ReadFromJsonAsync<RequestPage>(cancellationToken)
                    .ConfigureAwait(false);

                if (page?.Results is not { Count: > 0 })
                {
                    break;
                }

                foreach (var request in page.Results)
                {
                    var tmdb = request.Media?.TmdbId;
                    if (tmdb is not null)
                    {
                        result.Add(tmdb.Value.ToString(CultureInfo.InvariantCulture));
                    }
                }

                if (page.Results.Count < pageSize)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("Jellyseerr request timed out");
        }
        catch (Exception ex) when (ex is HttpRequestException or System.Text.Json.JsonException)
        {
            _logger.LogWarning(ex, "Jellyseerr request failed");
        }

        return result;
    }

    private sealed class RequestPage
    {
        [JsonPropertyName("results")]
        public List<RequestEntry>? Results { get; set; }
    }

    private sealed class RequestEntry
    {
        [JsonPropertyName("media")]
        public RequestMedia? Media { get; set; }
    }

    private sealed class RequestMedia
    {
        [JsonPropertyName("tmdbId")]
        public int? TmdbId { get; set; }
    }
}

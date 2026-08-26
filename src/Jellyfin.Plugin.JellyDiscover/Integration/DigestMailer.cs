using System.Globalization;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Web;
using Jellyfin.Database.Implementations.Entities;
using Jellyfin.Plugin.JellyDiscover.Configuration;
using Jellyfin.Plugin.JellyDiscover.Data;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Pipeline;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

/// <summary>
/// Optional email digest of a user's new recommendations.
///
/// Opt-in per user, rate-limited to once a day, and every value interpolated into the HTML
/// is escaped — 1.x built its digest by string concatenation with raw item names, so a
/// title containing markup was injected straight into the message.
/// </summary>
public sealed class DigestMailer
{
    private const int ItemsPerSection = 3;

    private readonly DiscoveryStore _store;
    private readonly ILogger<DigestMailer> _logger;

    public DigestMailer(DiscoveryStore store, ILogger<DigestMailer> logger)
    {
        _store = store;
        _logger = logger;
    }

    public async Task SendDigestAsync(
        User user,
        RecommendationResult result,
        PluginConfiguration configuration,
        CancellationToken cancellationToken)
    {
        if (!configuration.EmailEnabled || string.IsNullOrWhiteSpace(configuration.SmtpHost))
        {
            return;
        }

        var preferences = _store.GetUserPreferences(user.Id);
        if (!preferences.EmailDigest)
        {
            return;
        }

        var address = preferences.EmailOverride;
        if (string.IsNullOrWhiteSpace(address))
        {
            _logger.LogDebug("No email address for {User}; skipping digest", user.Username);
            return;
        }

        // At most one digest per day, however often recommendations are regenerated.
        if (DateTimeOffset.TryParse(
                preferences.LastDigestSent, CultureInfo.InvariantCulture, out var last)
            && DateTimeOffset.UtcNow - last < TimeSpan.FromHours(20))
        {
            return;
        }

        var recommendations = result.All.Take(ItemsPerSection * 3).ToArray();
        if (recommendations.Length == 0)
        {
            return;
        }

        try
        {
            using var message = BuildMessage(user, address, recommendations, configuration);
            using var client = new SmtpClient(configuration.SmtpHost, configuration.SmtpPort)
            {
                EnableSsl = configuration.SmtpUseSsl,
                DeliveryMethod = SmtpDeliveryMethod.Network,
            };

            if (!string.IsNullOrWhiteSpace(configuration.SmtpUsername))
            {
                client.Credentials = new NetworkCredential(
                    configuration.SmtpUsername, configuration.SmtpPassword);
            }

            await client.SendMailAsync(message, cancellationToken).ConfigureAwait(false);

            preferences.LastDigestSent = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
            _store.SaveUserPreferences(user.Id, preferences);
            _store.Record("info", "digest", $"sent to {user.Username}");
        }
        catch (Exception ex) when (ex is SmtpException or InvalidOperationException or FormatException)
        {
            _logger.LogWarning(ex, "Could not send digest to {User}", user.Username);
            _store.Record("warn", "digest", $"{user.Username}: {ex.Message}");
        }
    }

    private static MailMessage BuildMessage(
        User user,
        string address,
        IReadOnlyList<Recommendation> recommendations,
        PluginConfiguration configuration)
    {
        var baseUrl = (configuration.PublicServerUrl ?? string.Empty).TrimEnd('/');
        var body = new StringBuilder();

        body.Append(CultureInfo.InvariantCulture, $"""
            <div style="font-family:system-ui,sans-serif;max-width:640px;margin:0 auto;color:#1b1b1f">
              <h1 style="font-size:20px;margin:0 0 4px">New picks for {Escape(user.Username)}</h1>
              <p style="margin:0 0 20px;color:#66646f;font-size:14px">
                Chosen from what you've watched, finished, and skipped.
              </p>
            """);

        foreach (var recommendation in recommendations)
        {
            var item = recommendation.Item;
            var link = string.IsNullOrEmpty(baseUrl)
                ? null
                : $"{baseUrl}/web/index.html#!/details?id={Escape(item.Id)}";

            var title = Escape(item.Name);
            var year = item.ProductionYear is { } y
                ? $" ({y.ToString(CultureInfo.InvariantCulture)})"
                : string.Empty;

            var heading = link is null
                ? title
                : $"""<a href="{link}" style="color:#5b45a8;text-decoration:none">{title}</a>""";

            body.Append(CultureInfo.InvariantCulture, $"""
                <div style="padding:12px 0;border-top:1px solid #e6e4ee">
                  <div style="font-weight:600;font-size:15px">{heading}{year}</div>
                  <div style="color:#66646f;font-size:13px;margin-top:2px">{Escape(recommendation.Explain())}</div>
                </div>
                """);
        }

        body.Append("</div>");

        var message = new MailMessage
        {
            From = new MailAddress(
                configuration.SmtpFromAddress,
                configuration.SmtpFromName),
            Subject = $"Your new picks — {DateTime.Now.ToString("dddd d MMMM", CultureInfo.CurrentCulture)}",
            Body = body.ToString(),
            IsBodyHtml = true,
            BodyEncoding = Encoding.UTF8,
        };

        message.To.Add(address);
        return message;
    }

    /// <summary>
    /// Every interpolated value goes through this. A media title is untrusted input as far
    /// as an HTML email is concerned.
    /// </summary>
    private static string Escape(string? value)
        => HttpUtility.HtmlEncode(value ?? string.Empty);
}

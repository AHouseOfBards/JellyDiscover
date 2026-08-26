using Jellyfin.Plugin.JellyDiscover.Configuration;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Plugins;
using MediaBrowser.Model.Plugins;
using MediaBrowser.Model.Serialization;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover;

public sealed class Plugin : BasePlugin<PluginConfiguration>, IHasWebPages
{
    private readonly ILogger<Plugin>? _logger;

    public Plugin(
        IApplicationPaths applicationPaths,
        IXmlSerializer xmlSerializer,
        ILogger<Plugin>? logger = null)
        : base(applicationPaths, xmlSerializer)
    {
        _logger = logger;
        Instance = this;
        ContentRoot = Path.Combine(applicationPaths.DataPath, "jellydiscover");
        StatePath = Path.Combine(applicationPaths.DataPath, "jellydiscover", "state.json");
        Directory.CreateDirectory(ContentRoot);
    }

    public static Plugin? Instance { get; private set; }

    public override string Name => "JellyDiscover";

    public override Guid Id => new("6f1a7c48-2b3d-4a91-9d0e-5c7b8e2f4a11");

    public override string Description =>
        "Personalised recommendation libraries for every user, learned from what they "
        + "actually watch, finish, and abandon.";

    /// <summary>
    /// Where generated libraries live. Teardown refuses to delete anything outside this
    /// root regardless of what the registry claims.
    /// </summary>
    public string ContentRoot { get; }

    /// <summary>Registry, learned models, and the run-event log.</summary>
    public string StatePath { get; }

    public IEnumerable<PluginPageInfo> GetPages() =>
    [
        new PluginPageInfo
        {
            Name = Name,
            EmbeddedResourcePath = $"{GetType().Namespace}.Configuration.configPage.html",
        },
    ];

    /// <summary>
    /// Deliberately does nothing but log.
    ///
    /// This previously ran a full teardown and blocked on it with .Wait(2 minutes). That
    /// wedged the very operations an admin needs: uninstall, update (which uninstalls
    /// first), and — because Jellyfin routes disable through the same path — disable as
    /// well. The plugin became impossible to remove through the UI.
    ///
    /// Removing generated libraries is the explicit "Remove all JellyDiscover libraries"
    /// button on the config page, which is cancellable, reports what it did, and cannot
    /// take the server down with it. Jellyfin does not guarantee this hook runs at all, so
    /// it was never a dependable place for that work in the first place.
    /// </summary>
    public override void OnUninstalling()
    {
        _logger?.LogInformation(
            "JellyDiscover is being uninstalled. Generated libraries are NOT removed here — "
            + "use the config page's Remove-everything action before uninstalling, or delete "
            + "the libraries from Dashboard > Libraries afterwards.");

        base.OnUninstalling();
    }
}

using Jellyfin.Plugin.JellyDiscover.Configuration;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Common.Plugins;
using MediaBrowser.Model.Plugins;
using MediaBrowser.Model.Serialization;

namespace Jellyfin.Plugin.JellyDiscover;

public sealed class Plugin : BasePlugin<PluginConfiguration>, IHasWebPages
{
    public Plugin(IApplicationPaths applicationPaths, IXmlSerializer xmlSerializer)
        : base(applicationPaths, xmlSerializer)
    {
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
    /// Best-effort cleanup on uninstall.
    ///
    /// Deliberately not the only teardown path: Jellyfin documents that it cannot
    /// guarantee this runs, and there are open issues where uninstall fails outright or
    /// where merely disabling a plugin triggers it. The reliable path is the explicit
    /// "Remove everything" action on the config page, which the README tells admins to use
    /// first. This hook is the safety net, not the plan.
    /// </summary>
    public override void OnUninstalling()
    {
        try
        {
            UninstallHook?.Invoke();
        }
        catch (Exception)
        {
            // Never throw out of an uninstall hook: doing so can wedge the uninstall
            // itself and leave the user worse off than the libraries we failed to remove.
        }

        base.OnUninstalling();
    }

    /// <summary>
    /// Set during service registration so the hook can reach a fully constructed
    /// TeardownService without the plugin type depending on the DI container.
    /// </summary>
    public Action? UninstallHook { get; set; }
}

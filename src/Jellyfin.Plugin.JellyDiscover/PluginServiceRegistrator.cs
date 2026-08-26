using Jellyfin.Plugin.JellyDiscover.Data;
using Jellyfin.Plugin.JellyDiscover.Integration;
using Jellyfin.Plugin.JellyDiscover.Integration.External;
using Jellyfin.Plugin.JellyDiscover.Tasks;
using MediaBrowser.Controller;
using MediaBrowser.Controller.Plugins;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover;

public sealed class PluginServiceRegistrator : IPluginServiceRegistrator
{
    public void RegisterServices(IServiceCollection serviceCollection, IServerApplicationHost applicationHost)
    {
        serviceCollection.AddSingleton(_ =>
            new DiscoveryStore(Plugin.Instance!.StatePath));

        serviceCollection.AddSingleton(provider =>
            new ContentWriter(
                provider.GetRequiredService<ILogger<ContentWriter>>(),
                Plugin.Instance!.Configuration.WriteNfoFiles));

        serviceCollection.AddSingleton<CatalogueReader>();
        serviceCollection.AddSingleton<AccessController>();
        serviceCollection.AddSingleton<LibrarySynchronizer>();
        serviceCollection.AddSingleton<TeardownService>();
        serviceCollection.AddSingleton<LegacyCleanup>();
        serviceCollection.AddSingleton<TraktClient>();
        serviceCollection.AddSingleton<JellyseerrClient>();
        serviceCollection.AddSingleton<ExternalSignalService>();
        serviceCollection.AddSingleton<DigestMailer>();

        serviceCollection.AddSingleton<RefreshCoordinator>();
        serviceCollection.AddHostedService(p => p.GetRequiredService<RefreshCoordinator>());
        serviceCollection.AddHostedService<WatchActivityListener>();
        serviceCollection.AddHostedService<UninstallHookInstaller>();
    }
}

/// <summary>
/// Hands the plugin a callback into the fully-constructed TeardownService.
///
/// OnUninstalling is a plain virtual on the plugin type, which has no access to the DI
/// container. Rather than reach for a service locator, we wire the delegate once at
/// startup. It is a best-effort path either way: Jellyfin does not guarantee the hook runs,
/// which is why the config page carries an explicit "Remove everything" action.
/// </summary>
public sealed class UninstallHookInstaller : IHostedService
{
    private readonly TeardownService _teardown;
    private readonly ILogger<UninstallHookInstaller> _logger;

    public UninstallHookInstaller(TeardownService teardown, ILogger<UninstallHookInstaller> logger)
    {
        _teardown = teardown;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (Plugin.Instance is { } plugin)
        {
            plugin.UninstallHook = () =>
            {
                _logger.LogInformation("Uninstall detected; attempting to remove generated libraries");

                // Uninstall gives us no async context and no guarantee of time, so this is
                // bounded rather than open-ended. A failure here is recoverable: the admin
                // can reinstall and use "Remove everything", or delete the libraries by hand.
                _teardown
                    .RemoveEverythingAsync(dropLocalState: true, CancellationToken.None)
                    .Wait(TimeSpan.FromMinutes(2));
            };
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

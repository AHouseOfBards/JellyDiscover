using Jellyfin.Plugin.JellyDiscover.Data;
using Jellyfin.Plugin.JellyDiscover.Integration;
using Jellyfin.Plugin.JellyDiscover.Integration.External;
using Jellyfin.Plugin.JellyDiscover.Tasks;
using MediaBrowser.Controller;
using MediaBrowser.Controller.Plugins;
using Microsoft.Extensions.DependencyInjection;
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
    }
}

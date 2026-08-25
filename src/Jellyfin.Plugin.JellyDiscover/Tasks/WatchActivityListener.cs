using MediaBrowser.Controller.Library;
using MediaBrowser.Model.Entities;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Tasks;

/// <summary>
/// "Recommendations update as you watch."
///
/// Subscribes to UserDataSaved and filters hard. That event fires on every playback
/// progress tick, so acting on it directly would regenerate libraries continuously — the
/// same class of mistake as 1.x's scheduler, in the opposite direction. Only reasons that
/// represent a real change of opinion get through, and even those are debounced by the
/// coordinator.
/// </summary>
public sealed class WatchActivityListener : IHostedService
{
    private readonly IUserDataManager _userDataManager;
    private readonly RefreshCoordinator _coordinator;
    private readonly ILogger<WatchActivityListener> _logger;

    public WatchActivityListener(
        IUserDataManager userDataManager,
        RefreshCoordinator coordinator,
        ILogger<WatchActivityListener> logger)
    {
        _userDataManager = userDataManager;
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _userDataManager.UserDataSaved += OnUserDataSaved;
        _logger.LogInformation("JellyDiscover is listening for watch activity");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _userDataManager.UserDataSaved -= OnUserDataSaved;
        return Task.CompletedTask;
    }

    private void OnUserDataSaved(object? sender, UserDataSaveEventArgs e)
    {
        if (e is null)
        {
            return;
        }

        // PlaybackProgress fires every few seconds during playback. Everything here is a
        // deliberate act or a completed viewing.
        var meaningful = e.SaveReason is UserDataSaveReason.PlaybackFinished
            or UserDataSaveReason.TogglePlayed
            or UserDataSaveReason.UpdateUserRating
            or UserDataSaveReason.UpdateUserData;

        if (!meaningful)
        {
            return;
        }

        try
        {
            _coordinator.RequestRefresh(e.UserId);
        }
        catch (Exception ex)
        {
            // An event handler that throws can take the caller down with it.
            _logger.LogError(ex, "Failed to queue a refresh for {UserId}", e.UserId);
        }
    }
}

using MediaBrowser.Model.Tasks;

namespace Jellyfin.Plugin.JellyDiscover.Tasks;

/// <summary>
/// The full refresh, registered with Jellyfin's own scheduler.
///
/// It shows up in Dashboard > Scheduled Tasks with progress, cancellation, and trigger
/// editing supplied by the host. 1.x hand-rolled all of this: a thread comparing
/// DateTime.Now.strftime("%H:%M") to a configured string, which meant the frequency
/// setting could only ever suppress a run and a delayed loop silently skipped the day.
/// </summary>
public sealed class GenerateRecommendationsTask : IScheduledTask
{
    private readonly RefreshCoordinator _coordinator;

    public GenerateRecommendationsTask(RefreshCoordinator coordinator)
    {
        _coordinator = coordinator;
    }

    public string Name => "Generate discovery libraries";

    public string Key => "JellyDiscoverGenerate";

    public string Description =>
        "Rebuilds every user's personalised recommendation libraries from their watch history.";

    public string Category => "JellyDiscover";

    public Task ExecuteAsync(IProgress<double> progress, CancellationToken cancellationToken)
        => _coordinator.RefreshAllAsync(progress, cancellationToken);

    public IEnumerable<TaskTriggerInfo> GetDefaultTriggers() =>
    [
        new TaskTriggerInfo
        {
            Type = TaskTriggerInfoType.DailyTrigger,
            TimeOfDayTicks = TimeSpan.FromHours(4).Ticks,
        },
    ];
}

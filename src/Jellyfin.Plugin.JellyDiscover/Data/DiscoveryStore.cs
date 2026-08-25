using System.Text.Json;
using System.Text.Json.Serialization;
using JellyDiscover.Core.Models;
using JellyDiscover.Core.Ranking;

namespace Jellyfin.Plugin.JellyDiscover.Data;

/// <summary>One library this plugin created, and therefore one library it may remove.</summary>
public sealed record DiscoveryLibrary
{
    public required Guid UserId { get; init; }

    public required MediaKind Kind { get; init; }

    /// <summary>Jellyfin's item id for the virtual folder. Identity — never re-derived.</summary>
    public required Guid LibraryItemId { get; init; }

    /// <summary>Display name, including the invisible per-user suffix. Needed because
    /// ILibraryManager.RemoveVirtualFolder addresses libraries by name, not id.</summary>
    public required string LibraryName { get; init; }

    public required string ContentPath { get; init; }

    /// <summary>Allocated once per user and never reused. This is the fix for the 1.x bug
    /// where the suffix came from a list index, so adding a user renamed everyone's
    /// libraries and duplicated them all.</summary>
    public required int Slot { get; init; }
}

public sealed record RunEvent(string Occurred, string Level, string Stage, string Message);

/// <summary>The on-disk shape. Kept deliberately boring and versioned.</summary>
internal sealed class StoreState
{
    public int Version { get; set; } = 1;

    public List<DiscoveryLibrary> Libraries { get; set; } = [];

    public Dictionary<string, int> Slots { get; set; } = [];

    public Dictionary<string, StoredModel> Models { get; set; } = [];

    public List<RunEvent> Events { get; set; } = [];

    public Dictionary<string, string> Blocklist { get; set; } = [];
}

internal sealed class StoredModel
{
    public int Version { get; set; }

    public double[] Values { get; set; } = [];

    public string Updated { get; set; } = string.Empty;
}

/// <summary>
/// The plugin's own state.
///
/// A JSON file rather than a database, on purpose. The whole thing is a few rows per user,
/// and taking a SQLite dependency would mean either shipping an assembly the host already
/// loads (a version-conflict risk) or carrying native binaries into a per-plugin
/// AssemblyLoadContext. Neither is worth it for this much data.
///
/// The registry is the single most important structural change from 1.x. That version
/// re-derived its own identity every run by substring-matching library names and file
/// paths, which is why it deleted user libraries called "Recommended Classics", duplicated
/// libraries whenever the user roster changed, and orphaned folders on rename. Here,
/// nothing is ever inferred: if there is no entry, we did not create it, and we do not
/// touch it.
/// </summary>
public sealed class DiscoveryStore
{
    private const int MaxEvents = 500;

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly string _path;
    private readonly Lock _gate = new();
    private StoreState _state;

    public DiscoveryStore(string statePath)
    {
        ArgumentException.ThrowIfNullOrEmpty(statePath);
        _path = statePath;

        var directory = Path.GetDirectoryName(statePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _state = Load(statePath);
    }

    private static StoreState Load(string path)
    {
        if (!File.Exists(path))
        {
            return new StoreState();
        }

        try
        {
            var json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<StoreState>(json, SerializerOptions) ?? new StoreState();
        }
        catch (Exception)
        {
            // A corrupt state file must not stop the plugin loading. Keep the bad copy so
            // it can be inspected rather than silently discarding the user's registry.
            try
            {
                File.Move(path, path + ".corrupt-" + DateTime.UtcNow.Ticks, overwrite: false);
            }
            catch (IOException)
            {
                // Nothing further to do; we still start with an empty registry.
            }

            return new StoreState();
        }
    }

    /// <summary>Write to a temp file then replace, so a crash mid-write cannot truncate state.</summary>
    private void Save()
    {
        var temp = _path + ".tmp";
        File.WriteAllText(temp, JsonSerializer.Serialize(_state, SerializerOptions));

        if (File.Exists(_path))
        {
            File.Replace(temp, _path, null);
        }
        else
        {
            File.Move(temp, _path);
        }
    }

    // ---- registry -----------------------------------------------------------------

    public IReadOnlyList<DiscoveryLibrary> GetAllLibraries()
    {
        lock (_gate)
        {
            return _state.Libraries.ToArray();
        }
    }

    public IReadOnlyList<DiscoveryLibrary> GetLibrariesFor(Guid userId)
    {
        lock (_gate)
        {
            return _state.Libraries.Where(l => l.UserId == userId).ToArray();
        }
    }

    public void UpsertLibrary(DiscoveryLibrary library)
    {
        ArgumentNullException.ThrowIfNull(library);
        lock (_gate)
        {
            _state.Libraries.RemoveAll(l => l.UserId == library.UserId && l.Kind == library.Kind);
            _state.Libraries.Add(library);
            Save();
        }
    }

    public void ForgetLibrary(Guid userId, MediaKind kind)
    {
        lock (_gate)
        {
            _state.Libraries.RemoveAll(l => l.UserId == userId && l.Kind == kind);
            Save();
        }
    }

    /// <summary>
    /// Allocates this user's invisible name suffix once, then returns the same value
    /// forever. Slots are never reused, so a departed user cannot cause a survivor's
    /// library to be renamed.
    /// </summary>
    public int GetOrAllocateSlot(Guid userId)
    {
        var key = userId.ToString("N");
        lock (_gate)
        {
            if (_state.Slots.TryGetValue(key, out var existing))
            {
                return existing;
            }

            var slot = _state.Slots.Count == 0 ? 1 : _state.Slots.Values.Max() + 1;
            _state.Slots[key] = slot;
            Save();
            return slot;
        }
    }

    // ---- learned models -------------------------------------------------------------

    public RankerWeights? GetModel(Guid userId)
    {
        lock (_gate)
        {
            if (!_state.Models.TryGetValue(userId.ToString("N"), out var stored))
            {
                return null;
            }

            // Feature vector changed shape; stored positional weights are meaningless now.
            if (stored.Version != RankerWeights.CurrentVersion
                || stored.Values.Length != FeatureNames.Count)
            {
                return null;
            }

            return new RankerWeights { Values = stored.Values, Version = stored.Version };
        }
    }

    public void SaveModel(Guid userId, RankerWeights weights)
    {
        ArgumentNullException.ThrowIfNull(weights);
        lock (_gate)
        {
            _state.Models[userId.ToString("N")] = new StoredModel
            {
                Version = weights.Version,
                Values = weights.Values,
                Updated = DateTimeOffset.UtcNow.ToString("O"),
            };
            Save();
        }
    }

    // ---- diagnostics ------------------------------------------------------------------

    /// <summary>
    /// Structured run history. The dashboard reads this instead of grepping a log file for
    /// the substring "ERROR", which is how 1.x decided what to show — and why months-old
    /// errors surfaced as current status.
    /// </summary>
    public void Record(string level, string stage, string message)
    {
        lock (_gate)
        {
            _state.Events.Add(new RunEvent(
                DateTimeOffset.UtcNow.ToString("O"), level, stage, message));

            if (_state.Events.Count > MaxEvents)
            {
                _state.Events.RemoveRange(0, _state.Events.Count - MaxEvents);
            }

            Save();
        }
    }

    public IReadOnlyList<RunEvent> GetEvents(int take = 50)
    {
        lock (_gate)
        {
            return _state.Events.AsEnumerable().Reverse().Take(take).ToArray();
        }
    }

    public IReadOnlySet<string> GetBlocklist()
    {
        lock (_gate)
        {
            return _state.Blocklist.Keys.ToHashSet(StringComparer.Ordinal);
        }
    }

    /// <summary>Clears all state. Used by full teardown, after Jellyfin state is undone.</summary>
    public void DropEverything()
    {
        lock (_gate)
        {
            _state = new StoreState();
            Save();
        }
    }
}

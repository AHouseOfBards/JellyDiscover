using MediaBrowser.Controller.Library;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

/// <summary>
/// Per-user library visibility, done as a two-element diff.
///
/// JellyDiscover 1.x set EnableAllFolders=false on every user (administrators included)
/// and then rebuilt the entire EnabledFolders list from scratch on every run, silently
/// dropping any library whose Locations array came back empty. Users lost access to their
/// own media and the setting that did it had no UI and no documented off switch.
///
/// Here we read the current policy, add or remove ONLY the library ids we own, and write
/// back a list that is otherwise byte-identical. If a user already has EnableAllFolders,
/// they can see their discovery library and there is nothing to do.
/// </summary>
public sealed class AccessController
{
    private readonly IUserManager _userManager;
    private readonly ILogger<AccessController> _logger;

    public AccessController(IUserManager userManager, ILogger<AccessController> logger)
    {
        _userManager = userManager;
        _logger = logger;
    }

    /// <summary>Ensures the user can see this library. Returns true if a write happened.</summary>
    public Task<bool> GrantAsync(Guid userId, Guid libraryItemId)
        => MutateAsync(userId, libraryItemId, grant: true);

    /// <summary>Ensures the user cannot see this library. Returns true if a write happened.</summary>
    public Task<bool> RevokeAsync(Guid userId, Guid libraryItemId)
        => MutateAsync(userId, libraryItemId, grant: false);

    private async Task<bool> MutateAsync(Guid userId, Guid libraryItemId, bool grant)
    {
        var user = _userManager.GetUserById(userId);
        if (user is null)
        {
            return false;
        }

        var policy = _userManager.GetUserDto(user, null)?.Policy;
        if (policy is null)
        {
            _logger.LogWarning("No policy available for user {UserId}", userId);
            return false;
        }

        // A user who can already see everything needs no help from us, and flipping
        // EnableAllFolders off to "add" one folder is exactly how 1.x demoted admins.
        if (policy.EnableAllFolders && grant)
        {
            return false;
        }

        var current = policy.EnabledFolders ?? Array.Empty<Guid>();
        var contains = current.Contains(libraryItemId);

        if (grant == contains)
        {
            return false;
        }

        policy.EnabledFolders = grant
            ? current.Append(libraryItemId).Distinct().ToArray()
            : current.Where(id => id != libraryItemId).ToArray();

        await _userManager.UpdatePolicyAsync(userId, policy).ConfigureAwait(false);

        _logger.LogDebug(
            "{Action} library {Library} for user {User}",
            grant ? "Granted" : "Revoked",
            libraryItemId,
            userId);

        return true;
    }

    /// <summary>
    /// Removes any of our library ids from every user except the one that owns it. Cheap
    /// insurance against a half-finished run leaving someone able to see another user's
    /// recommendations.
    /// </summary>
    public async Task<int> EnforceExclusivityAsync(
        Guid ownerUserId,
        Guid libraryItemId,
        CancellationToken cancellationToken)
    {
        var changed = 0;
        foreach (var user in _userManager.Users)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (user.Id == ownerUserId)
            {
                continue;
            }

            if (await RevokeAsync(user.Id, libraryItemId).ConfigureAwait(false))
            {
                changed++;
            }
        }

        return changed;
    }
}

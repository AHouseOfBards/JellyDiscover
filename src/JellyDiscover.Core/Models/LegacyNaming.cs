namespace JellyDiscover.Core.Models;

/// <summary>
/// Recognises the library-naming scheme used by the Python 1.x releases.
///
/// This lives in Core, with no Jellyfin dependency, for one reason: it is the predicate
/// that decides whether something gets deleted, and it must be covered by fast tests that
/// run without a server. JellyDiscover 1.x decided the same question with
/// `"Discover" in name or "Recommended" in name` and destroyed user libraries as a result.
/// </summary>
public static class LegacyNaming
{
    /// <summary>U+3164 HANGUL FILLER — renders as blank, prefixed every 1.x library.</summary>
    public const char HangulFiller = 'ㅤ';

    /// <summary>U+200B ZERO WIDTH SPACE — repeated once per user index as the 1.x suffix.</summary>
    public const char ZeroWidthSpace = '​';

    /// <summary>
    /// True only for the exact 1.x shape: a Hangul Filler, then visible text, then one or
    /// more zero-width spaces. No substring matching, no keyword list, no guessing.
    /// </summary>
    public static bool LooksLegacy(string? name)
    {
        if (string.IsNullOrEmpty(name) || name.Length < 3)
        {
            return false;
        }

        if (name[0] != HangulFiller || name[^1] != ZeroWidthSpace)
        {
            return false;
        }

        // There must be real text between the invisible markers.
        var inner = name.AsSpan(1).TrimEnd(ZeroWidthSpace);
        if (inner.Length == 0)
        {
            return false;
        }

        foreach (var ch in inner)
        {
            if (ch != HangulFiller && ch != ZeroWidthSpace)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Renders invisible characters visibly, so log lines and previews are readable.</summary>
    public static string Describe(string name)
        => (name ?? string.Empty)
            .Replace(HangulFiller.ToString(), "<filler>", StringComparison.Ordinal)
            .Replace(ZeroWidthSpace.ToString(), "<zwsp>", StringComparison.Ordinal);
}

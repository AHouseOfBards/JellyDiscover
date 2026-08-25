using System.Globalization;
using System.Text;

namespace JellyDiscover.Core.Text;

/// <summary>
/// Field-aware tokenizer. Structured fields emit namespaced terms ("genre:science_fiction")
/// so a genre match is not confused with the same word appearing in a plot summary, while
/// free text emits plain terms.
/// </summary>
public static class Tokenizer
{
    private const char Space = ' ';
    private const char Underscore = '_';

    private static readonly HashSet<string> Stopwords = new(StringComparer.Ordinal)
    {
        "the", "and", "but", "for", "with", "from", "are", "was", "were", "been",
        "his", "her", "their", "its", "they", "who", "that", "this", "these", "those",
        "when", "while", "after", "before", "into", "out", "over", "under",
        "then", "than", "not", "all", "one", "two", "new", "own", "must",
        "have", "has", "had", "will", "would", "can", "could", "about", "him", "them",
        "she", "you", "your", "him", "himself", "herself", "there", "here", "what",
    };

    public static IEnumerable<string> FreeText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            yield break;
        }

        foreach (var token in Split(text))
        {
            if (token.Length < 3 || Stopwords.Contains(token))
            {
                continue;
            }

            yield return token;
        }
    }

    /// <summary>
    /// A structured value becomes one atomic term, so "Science Fiction" stays a single
    /// concept rather than two common words. This is why the genre vocabulary no longer
    /// has to be hardcoded to match a particular metadata provider.
    /// </summary>
    public static string Field(string prefix, string? value)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        var normalized = Normalize(value).Replace(Space, Underscore);
        return string.Concat(prefix, ":", normalized);
    }

    private static IEnumerable<string> Split(string text)
    {
        var sb = new StringBuilder();
        foreach (var ch in text)
        {
            if (char.IsLetterOrDigit(ch))
            {
                sb.Append(char.ToLowerInvariant(ch));
            }
            else if (sb.Length > 0)
            {
                yield return sb.ToString();
                sb.Clear();
            }
        }

        if (sb.Length > 0)
        {
            yield return sb.ToString();
        }
    }

    /// <summary>Lowercases and strips diacritics, so "Amelie" and "Amélie" agree.</summary>
    private static string Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var decomposed = value.Normalize(NormalizationForm.FormD);
        var sb = new StringBuilder(decomposed.Length);
        foreach (var ch in decomposed)
        {
            if (CharUnicodeInfo.GetUnicodeCategory(ch) == UnicodeCategory.NonSpacingMark)
            {
                continue;
            }

            if (char.IsLetterOrDigit(ch))
            {
                sb.Append(char.ToLowerInvariant(ch));
            }
            else if (sb.Length > 0 && sb[sb.Length - 1] != Space)
            {
                sb.Append(Space);
            }
        }

        return sb.ToString().Trim();
    }
}

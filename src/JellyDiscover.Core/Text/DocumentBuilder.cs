using System.Globalization;
using JellyDiscover.Core.Models;

namespace JellyDiscover.Core.Text;

/// <summary>
/// Builds the bag of terms that represents an item.
///
/// JellyDiscover 1.x embedded the Overview alone. Plot summaries are the weakest text on
/// an item — they share heavy generic vocabulary ("a young man must confront his past"),
/// so unrelated titles land close together. Structured metadata carries far more signal,
/// so it is included here and repeated to weight it up.
/// </summary>
public static class DocumentBuilder
{
    public const int GenreRepeat = 3;
    public const int BlendRepeat = 3;
    public const int TagRepeat = 2;
    public const int DirectorRepeat = 3;
    public const int SupportingRepeat = 1;
    public const int StudioRepeat = 2;

    /// <summary>
    /// Genre pairs, canonically ordered, so "sci-fi horror" is its own concept rather than
    /// the coincidence of two independent labels.
    ///
    /// This matters because a hybrid is often its own genre: Alien is not "a sci-fi film
    /// that is also a horror film" to anyone choosing what to watch, and someone who loves
    /// it may have no appetite for Arrival or for a haunted-house picture. Because blends
    /// are rarer than their component genres, BM25's inverse document frequency gives them
    /// more weight automatically — no special-casing required.
    /// </summary>
    public static IEnumerable<string> BlendTerms(IReadOnlyList<string> genres)
    {
        ArgumentNullException.ThrowIfNull(genres);
        if (genres.Count < 2)
        {
            yield break;
        }

        var ordered = genres
            .Where(g => !string.IsNullOrWhiteSpace(g))
            .Select(g => g.Trim())
            .OrderBy(g => g, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        for (var i = 0; i < ordered.Length; i++)
        {
            for (var j = i + 1; j < ordered.Length; j++)
            {
                yield return Tokenizer.Field("blend", $"{ordered[i]} {ordered[j]}");
            }
        }
    }

    public static IReadOnlyList<string> Build(CatalogItem item)
    {
        ArgumentNullException.ThrowIfNull(item);
        var terms = new List<string>(64);

        foreach (var genre in item.Genres)
        {
            Repeat(terms, Tokenizer.Field("genre", genre), GenreRepeat);
        }

        foreach (var blend in BlendTerms(item.Genres))
        {
            Repeat(terms, blend, BlendRepeat);
        }

        foreach (var tag in item.Tags)
        {
            Repeat(terms, Tokenizer.Field("tag", tag), TagRepeat);
        }

        foreach (var person in item.People)
        {
            var (prefix, repeat) = RoleTerm(person.Role);
            Repeat(terms, Tokenizer.Field(prefix, person.Name), repeat);
        }

        if (!string.IsNullOrWhiteSpace(item.Studio))
        {
            Repeat(terms, Tokenizer.Field("studio", item.Studio), StudioRepeat);
        }

        if (!string.IsNullOrWhiteSpace(item.CollectionName))
        {
            Repeat(terms, Tokenizer.Field("collection", item.CollectionName), GenreRepeat);
        }

        if (item.ProductionYear is { } year)
        {
            // Decade, not year: era is a taste dimension, an exact year is noise.
            var decade = (year / 10 * 10).ToString(CultureInfo.InvariantCulture);
            terms.Add(Tokenizer.Field("decade", decade));
        }

        terms.AddRange(Tokenizer.FreeText(item.Name));
        terms.AddRange(Tokenizer.FreeText(item.Overview));

        return terms;
    }

    private static (string Prefix, int Repeat) RoleTerm(PersonRole role) => role switch
    {
        PersonRole.Director => ("director", DirectorRepeat),
        PersonRole.AlbumArtist => ("artist", DirectorRepeat),
        PersonRole.Writer => ("writer", SupportingRepeat),
        PersonRole.Composer => ("composer", SupportingRepeat),
        PersonRole.Actor => ("actor", SupportingRepeat),
        _ => ("person", SupportingRepeat),
    };

    private static void Repeat(List<string> into, string term, int times)
    {
        for (var i = 0; i < times; i++)
        {
            into.Add(term);
        }
    }
}

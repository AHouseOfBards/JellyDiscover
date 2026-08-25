using System.Globalization;
using System.Text;
using System.Xml;
using JellyDiscover.Core.Models;
using Microsoft.Extensions.Logging;

namespace Jellyfin.Plugin.JellyDiscover.Integration;

/// <summary>
/// Materialises a recommendation list as a folder Jellyfin can scan.
///
/// Two things are different from 1.x and both matter:
///
/// 1. The .strm contains the path the HOST can open. Running in-process, that is simply
///    the item's own path, untouched. 1.x ran the path through a substitution table that
///    translated it into ITS OWN view of the filesystem and then wrote that into the file
///    Jellyfin has to read — so the moment path substitution did anything at all, every
///    generated file pointed somewhere Jellyfin could not reach.
///
/// 2. An .nfo carrying the provider id is written alongside. Jellyfin always reads local
///    .nfo metadata and it cannot be disabled, so this turns identification from a fuzzy
///    match on a mangled, year-less folder name into an exact id lookup.
/// </summary>
public sealed class ContentWriter
{
    private static readonly HashSet<string> AudioExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        ".mp3", ".flac", ".m4a", ".wav", ".ogg", ".opus", ".wma", ".aac", ".alac",
    };

    private readonly ILogger<ContentWriter> _logger;
    private readonly bool _writeNfo;

    public ContentWriter(ILogger<ContentWriter> logger, bool writeNfo)
    {
        _logger = logger;
        _writeNfo = writeNfo;
    }

    /// <summary>
    /// Synchronises a folder so it contains exactly these items and nothing else.
    /// Unchanged entries are left completely alone — 1.x rewrote every .strm on every run,
    /// updating mtimes and provoking a rescan of content that had not changed.
    /// </summary>
    public void Sync(string folder, IReadOnlyList<Recommendation> recommendations)
    {
        ArgumentException.ThrowIfNullOrEmpty(folder);
        ArgumentNullException.ThrowIfNull(recommendations);

        Directory.CreateDirectory(folder);

        var expected = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var recommendation in recommendations)
        {
            var name = SafeFolderName(recommendation.Item);
            expected.Add(name);
            try
            {
                WriteItem(Path.Combine(folder, name), recommendation.Item);
            }
            catch (Exception ex)
            {
                // One bad item must not cost the user their whole library.
                _logger.LogWarning(
                    ex, "Could not materialise {Item} ({Id})",
                    recommendation.Item.Name, recommendation.Item.Id);
            }
        }

        Prune(folder, expected);
    }

    private void WriteItem(string itemFolder, CatalogItem item)
    {
        if (string.IsNullOrWhiteSpace(item.Path))
        {
            return;
        }

        Directory.CreateDirectory(itemFolder);

        if (item.Kind == MediaKind.MusicAlbum)
        {
            // Jellyfin does not play .strm in music libraries, so audio must be symlinked.
            LinkAudio(itemFolder, item.Path);
        }
        else if (Directory.Exists(item.Path))
        {
            MirrorDirectory(itemFolder, item.Path);
        }
        else
        {
            WriteStrm(Path.Combine(itemFolder, SafeFileName(item.Name) + ".strm"), item.Path);
        }

        if (_writeNfo)
        {
            WriteNfo(itemFolder, item);
        }
    }

    /// <summary>Mirrors a series folder as .strm stubs, preserving season structure.</summary>
    private void MirrorDirectory(string targetRoot, string sourceRoot)
    {
        foreach (var sourceFile in Directory.EnumerateFiles(sourceRoot, "*", SearchOption.AllDirectories))
        {
            var extension = Path.GetExtension(sourceFile);
            if (AudioExtensions.Contains(extension))
            {
                continue;
            }

            if (!IsVideo(extension))
            {
                continue;
            }

            var relative = Path.GetRelativePath(sourceRoot, sourceFile);
            var targetDirectory = Path.Combine(targetRoot, Path.GetDirectoryName(relative) ?? string.Empty);
            Directory.CreateDirectory(targetDirectory);

            var strm = Path.Combine(
                targetDirectory,
                Path.GetFileNameWithoutExtension(sourceFile) + ".strm");

            WriteStrm(strm, sourceFile);
        }
    }

    private void LinkAudio(string targetFolder, string sourcePath)
    {
        if (!Directory.Exists(sourcePath))
        {
            return;
        }

        foreach (var sourceFile in Directory.EnumerateFiles(sourcePath, "*", SearchOption.AllDirectories))
        {
            if (!AudioExtensions.Contains(Path.GetExtension(sourceFile)))
            {
                continue;
            }

            var relative = Path.GetRelativePath(sourcePath, sourceFile);
            var target = Path.Combine(targetFolder, relative);
            Directory.CreateDirectory(Path.GetDirectoryName(target)!);

            if (File.Exists(target) || Directory.Exists(target))
            {
                continue;
            }

            try
            {
                File.CreateSymbolicLink(target, sourceFile);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // On Windows this needs Developer Mode or an elevated service account.
                _logger.LogWarning(
                    "Symlink failed for {File}. Music recommendations require symlink "
                    + "permission; on Windows enable Developer Mode. {Message}",
                    sourceFile,
                    ex.Message);
                return;
            }
        }
    }

    /// <summary>Writes only if the content would actually change.</summary>
    private static void WriteStrm(string path, string hostPath)
    {
        if (File.Exists(path))
        {
            var existing = File.ReadAllText(path).Trim();
            if (string.Equals(existing, hostPath, StringComparison.Ordinal))
            {
                return;
            }
        }

        File.WriteAllText(path, hostPath, Encoding.UTF8);
    }

    /// <summary>
    /// An .nfo with the provider id. This is the difference between Jellyfin identifying
    /// "Dune" (1984? 2000? 2021? 2024?) from a folder name that has lost its punctuation
    /// and its year, versus looking up an exact tmdb id.
    /// </summary>
    private void WriteNfo(string itemFolder, CatalogItem item)
    {
        var root = item.Kind switch
        {
            MediaKind.Movie => "movie",
            MediaKind.Series => "tvshow",
            MediaKind.MusicAlbum => "album",
            _ => "movie",
        };

        var fileName = item.Kind switch
        {
            MediaKind.Series => "tvshow.nfo",
            MediaKind.MusicAlbum => "album.nfo",
            _ => "movie.nfo",
        };

        var path = Path.Combine(itemFolder, fileName);

        var settings = new XmlWriterSettings { Indent = true, Encoding = new UTF8Encoding(false) };
        using var buffer = new MemoryStream();
        using (var writer = XmlWriter.Create(buffer, settings))
        {
            writer.WriteStartElement(root);
            writer.WriteElementString("title", item.Name);

            if (item.ProductionYear is { } year)
            {
                writer.WriteElementString("year", year.ToString(CultureInfo.InvariantCulture));
            }

            foreach (var (provider, value) in item.ProviderIds)
            {
                if (string.IsNullOrWhiteSpace(value))
                {
                    continue;
                }

                writer.WriteStartElement("uniqueid");
                writer.WriteAttributeString("type", provider.ToLowerInvariant());
                writer.WriteString(value);
                writer.WriteEndElement();
            }

            writer.WriteEndElement();
        }

        var content = Encoding.UTF8.GetString(buffer.ToArray());
        if (File.Exists(path) && string.Equals(File.ReadAllText(path), content, StringComparison.Ordinal))
        {
            return;
        }

        File.WriteAllText(path, content, new UTF8Encoding(false));
    }

    private void Prune(string folder, IReadOnlySet<string> expected)
    {
        foreach (var directory in Directory.EnumerateDirectories(folder))
        {
            var name = Path.GetFileName(directory);
            if (expected.Contains(name))
            {
                continue;
            }

            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not prune stale entry {Path}", directory);
            }
        }
    }

    private static bool IsVideo(string extension) => extension.ToLowerInvariant() switch
    {
        ".mkv" or ".mp4" or ".avi" or ".m4v" or ".wmv" or ".ts" or ".mov" or ".iso" or ".mpg"
            or ".mpeg" or ".webm" or ".flv" or ".m2ts" => true,
        _ => false,
    };

    /// <summary>
    /// Folder names still have to be filesystem-safe, but they are no longer load-bearing
    /// for identification — that is the .nfo's job now. The year is included regardless,
    /// so even a server with NFO reading somehow disabled has a fighting chance.
    /// </summary>
    private static string SafeFolderName(CatalogItem item)
    {
        var name = SafeFileName(item.Name);
        return item.ProductionYear is { } year
            ? $"{name} ({year.ToString(CultureInfo.InvariantCulture)})"
            : name;
    }

    private static string SafeFileName(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var builder = new StringBuilder(name.Length);
        foreach (var ch in name)
        {
            builder.Append(Array.IndexOf(invalid, ch) >= 0 ? ' ' : ch);
        }

        var cleaned = builder.ToString().Trim().TrimEnd('.');
        return cleaned.Length == 0 ? "Untitled" : cleaned;
    }
}

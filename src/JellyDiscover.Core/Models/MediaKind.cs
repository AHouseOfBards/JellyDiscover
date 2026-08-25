namespace JellyDiscover.Core.Models;

/// <summary>The three catalogue kinds JellyDiscover produces libraries for.</summary>
public enum MediaKind
{
    Movie,
    Series,
    MusicAlbum,
}

public enum PersonRole
{
    Director,
    Actor,
    Writer,
    Composer,
    AlbumArtist,
}

public readonly record struct Person(string Name, PersonRole Role);

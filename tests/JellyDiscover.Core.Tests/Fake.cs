using JellyDiscover.Core.Models;

namespace JellyDiscover.Core.Tests;

/// <summary>
/// A synthetic library with genuinely separable clusters, so a test can assert that a
/// recommender found the right neighbourhood rather than just "returned something".
/// </summary>
public static class Fake
{
    public static CatalogItem Item(
        string id,
        string name,
        MediaKind kind = MediaKind.Movie,
        string[]? genres = null,
        string[]? tags = null,
        string? director = null,
        string[]? actors = null,
        string? collection = null,
        int year = 2015,
        double rating = 7.0,
        string? overview = null,
        int serverPlays = 0,
        int runtimeMinutes = 110)
    {
        var people = new List<Person>();
        if (director is not null)
        {
            people.Add(new Person(director, PersonRole.Director));
        }

        foreach (var a in actors ?? Array.Empty<string>())
        {
            people.Add(new Person(a, PersonRole.Actor));
        }

        return new CatalogItem
        {
            Id = id,
            Name = name,
            Kind = kind,
            Genres = genres ?? Array.Empty<string>(),
            Tags = tags ?? Array.Empty<string>(),
            People = people,
            CollectionName = collection,
            ProductionYear = year,
            CommunityRating = rating,
            Overview = overview,
            ServerPlayCount = serverPlays,
            Runtime = TimeSpan.FromMinutes(runtimeMinutes),
            Path = $"/media/{id}",
        };
    }

    /// <summary>
    /// Three well-separated clusters plus filler. Horror and romance share no vocabulary,
    /// which is exactly the situation a mean-pooled user profile handles badly.
    /// </summary>
    public static List<CatalogItem> Library()
    {
        var items = new List<CatalogItem>();

        for (var i = 0; i < 12; i++)
        {
            items.Add(Item(
                $"horror{i}",
                $"Nightfall {i}",
                genres: ["Horror", "Thriller"],
                tags: ["haunted house", "supernatural", "dread"],
                director: i < 6 ? "Ari Vance" : "Mira Kell",
                actors: ["Toby Rook"],
                year: 2012 + i,
                rating: 6.5 + (i % 4 * 0.3),
                overview: "A family moves into an old house where something malevolent waits in the dark."));
        }

        for (var i = 0; i < 12; i++)
        {
            items.Add(Item(
                $"romcom{i}",
                $"Something Like Love {i}",
                genres: ["Romance", "Comedy"],
                tags: ["wedding", "meet cute", "workplace romance"],
                director: i < 6 ? "Dana Pell" : "Joss Winter",
                actors: ["Nina Hale"],
                year: 2012 + i,
                rating: 6.4 + (i % 4 * 0.3),
                overview: "Two rivals are forced together by a wedding and fall in love despite themselves."));
        }

        for (var i = 0; i < 12; i++)
        {
            items.Add(Item(
                $"scifi{i}",
                $"Outer Dark {i}",
                genres: ["Science Fiction", "Drama"],
                tags: ["space travel", "first contact", "dystopia"],
                director: "Corin Alba",
                actors: ["Vic Sandoval"],
                year: 2012 + i,
                rating: 7.2 + (i % 3 * 0.3),
                overview: "A linguist is recruited to communicate with an alien craft in orbit above the planet."));
        }

        for (var i = 0; i < 20; i++)
        {
            items.Add(Item(
                $"filler{i}",
                $"Assorted Picture {i}",
                genres: ["Drama"],
                tags: ["family"],
                director: $"Director {i}",
                year: 2000 + i,
                rating: 6.0,
                overview: "A quiet drama about ordinary people in an ordinary town."));
        }

        return items;
    }

    public static Interaction Liked(string id, Sentiment sentiment = Sentiment.Positive)
        => new()
        {
            ItemId = id,
            Sentiment = sentiment,
            LastPlayed = DateTimeOffset.UtcNow.AddDays(-10),
            Confidence = 1.0,
        };

    public static Interaction Abandoned(string id)
        => new()
        {
            ItemId = id,
            Sentiment = Sentiment.StrongNegative,
            LastPlayed = DateTimeOffset.UtcNow.AddDays(-60),
            Confidence = 0.9,
        };
}

using JellyDiscover.Core.Models;
using Xunit;

namespace JellyDiscover.Core.Tests;

/// <summary>
/// The safety net for the only code path that deletes something the plugin did not create.
///
/// Every "must not match" case below is a library a real user could plausibly own. The 1.x
/// predicate — substring "Discover" or "Recommended" — matched most of them, which is how
/// it destroyed hand-curated libraries.
/// </summary>
public sealed class LegacyNamingTests
{
    private const string Filler = "ㅤ";
    private const string Zwsp = "​";

    [Theory]
    [InlineData(Filler + "Discover Movies" + Zwsp)]
    [InlineData(Filler + "Discover Shows" + Zwsp + Zwsp)]
    [InlineData(Filler + "Discover Music" + Zwsp + Zwsp + Zwsp + Zwsp + Zwsp)]
    [InlineData(Filler + "My Custom Discovery Name" + Zwsp)]
    public void Matches_TheExactLegacyScheme(string name)
        => Assert.True(LegacyNaming.LooksLegacy(name), LegacyNaming.Describe(name));

    [Theory]
    // Ordinary user libraries. All of these matched the 1.x keyword test.
    [InlineData("Recommended Classics")]
    [InlineData("Discover Weekly Rips")]
    [InlineData("Movies")]
    [InlineData("Discover Movies")]
    [InlineData("4K Discover")]
    [InlineData("Kids Recommended")]
    // Structurally close but not the scheme.
    [InlineData(Filler + "Discover Movies")]
    [InlineData("Discover Movies" + Zwsp)]
    [InlineData(Filler + Zwsp)]
    [InlineData(Filler + Zwsp + Zwsp)]
    [InlineData("")]
    [InlineData(null)]
    public void DoesNotMatch_AnythingElse(string? name)
        => Assert.False(LegacyNaming.LooksLegacy(name), LegacyNaming.Describe(name ?? "<null>"));

    [Fact]
    public void Describe_MakesInvisibleCharactersVisible()
    {
        var described = LegacyNaming.Describe(Filler + "Discover Movies" + Zwsp + Zwsp);

        Assert.Equal("<filler>Discover Movies<zwsp><zwsp>", described);
    }

    /// <summary>
    /// The 2.x scheme reuses the same invisible trick, so by shape our own libraries look
    /// legacy. They are excluded by registry id and name at the call site, not here — this
    /// test documents that the shape test alone is deliberately not sufficient.
    /// </summary>
    [Fact]
    public void OurOwnLibrariesShareTheShape_SoIdExclusionIsRequired()
        => Assert.True(LegacyNaming.LooksLegacy(Filler + "Discover Movies" + Zwsp));
}

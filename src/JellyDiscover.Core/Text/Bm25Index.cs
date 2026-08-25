using JellyDiscover.Core.Models;
using JellyDiscover.Core.Maths;

namespace JellyDiscover.Core.Text;

public sealed record Bm25Options
{
    /// <summary>Term-frequency saturation. Higher means repeats keep mattering longer.</summary>
    public double K1 { get; init; } = 1.2;

    /// <summary>Length normalisation, 0..1. Keeps a long plot summary from dominating.</summary>
    public double B { get; init; } = 0.6;

    /// <summary>Terms appearing in fewer documents than this are dropped as noise.</summary>
    public int MinDocumentFrequency { get; init; } = 2;

    /// <summary>Terms appearing in more than this fraction of the catalogue carry no
    /// discriminative power and are dropped.</summary>
    public double MaxDocumentFrequencyRatio { get; init; } = 0.5;
}

/// <summary>
/// BM25-weighted term vectors over the catalogue, compared by cosine.
///
/// The important property is that inverse document frequency is computed from *this
/// server's actual library*. JellyDiscover 1.x hardcoded a 22-entry IMDb genre list and a
/// ten-word "generic" penalty list, which silently mismatched TMDB's vocabulary ("Sci-Fi"
/// versus "Science Fiction") and left the resulting vectors nearly empty. Learning the
/// vocabulary from the data makes that class of bug unexpressible.
/// </summary>
public sealed class Bm25Index
{
    private readonly Dictionary<string, int> _termIds;
    private readonly double[] _idf;
    private readonly Dictionary<string, SparseVector> _vectors;

    private Bm25Index(
        Dictionary<string, int> termIds,
        double[] idf,
        Dictionary<string, SparseVector> vectors)
    {
        _termIds = termIds;
        _idf = idf;
        _vectors = vectors;
    }

    public int VocabularySize => _termIds.Count;

    public int DocumentCount => _vectors.Count;

    public static Bm25Index Build(IEnumerable<CatalogItem> catalogue, Bm25Options? options = null)
    {
        ArgumentNullException.ThrowIfNull(catalogue);
        var o = options ?? new Bm25Options();

        // Pass 1: tokenise once, collect document frequency.
        var documents = new List<(string Id, Dictionary<string, int> Counts, int Length)>();
        var documentFrequency = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var item in catalogue)
        {
            var terms = DocumentBuilder.Build(item);
            var counts = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var term in terms)
            {
                counts[term] = counts.TryGetValue(term, out var c) ? c + 1 : 1;
            }

            foreach (var term in counts.Keys)
            {
                documentFrequency[term] = documentFrequency.TryGetValue(term, out var d) ? d + 1 : 1;
            }

            documents.Add((item.Id, counts, terms.Count));
        }

        var n = documents.Count;
        if (n == 0)
        {
            return new Bm25Index(new Dictionary<string, int>(StringComparer.Ordinal), [], []);
        }

        // Build the vocabulary, pruning terms that are too rare or too common to help.
        var maxDf = Math.Max(1, (int)(n * o.MaxDocumentFrequencyRatio));
        var minDf = Math.Min(o.MinDocumentFrequency, n);
        var termIds = new Dictionary<string, int>(StringComparer.Ordinal);
        var idfList = new List<double>();

        foreach (var (term, df) in documentFrequency)
        {
            if (df < minDf || df > maxDf)
            {
                continue;
            }

            termIds[term] = idfList.Count;

            // Probabilistic IDF, floored so a near-universal surviving term cannot go negative.
            var ratio = (n - df + 0.5) / (df + 0.5);
            idfList.Add(Math.Max(0.05, Math.Log(1 + ratio)));
        }

        var idf = idfList.ToArray();
        var avgLength = documents.Average(d => (double)d.Length);
        if (avgLength <= 0)
        {
            avgLength = 1;
        }

        // Pass 2: weight and normalise.
        var vectors = new Dictionary<string, SparseVector>(StringComparer.Ordinal);
        foreach (var (id, counts, length) in documents)
        {
            var weights = new Dictionary<int, double>(counts.Count);
            foreach (var (term, tf) in counts)
            {
                if (!termIds.TryGetValue(term, out var termId))
                {
                    continue;
                }

                var denominator = tf + (o.K1 * (1 - o.B + (o.B * length / avgLength)));
                var saturated = tf * (o.K1 + 1) / denominator;
                weights[termId] = saturated * idf[termId];
            }

            vectors[id] = SparseVector.FromWeights(weights);
        }

        return new Bm25Index(termIds, idf, vectors);
    }

    public SparseVector Vector(string itemId)
        => _vectors.TryGetValue(itemId, out var v) ? v : SparseVector.Empty;

    public double Similarity(string leftItemId, string rightItemId)
    {
        if (string.Equals(leftItemId, rightItemId, StringComparison.Ordinal))
        {
            return 1.0;
        }

        return Vector(leftItemId).Cosine(Vector(rightItemId));
    }

    /// <summary>Inverse document frequency for a term, for tests and diagnostics.</summary>
    public double IdfOf(string term)
        => _termIds.TryGetValue(term, out var id) ? _idf[id] : 0;

    public bool Knows(string term) => _termIds.ContainsKey(term);
}

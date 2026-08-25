namespace JellyDiscover.Core.Maths;

/// <summary>
/// An L2-normalised sparse vector with sorted indices, so similarity is a linear merge
/// and cosine collapses to a plain dot product.
/// </summary>
public sealed class SparseVector
{
    private readonly int[] _indices;
    private readonly float[] _values;

    private SparseVector(int[] indices, float[] values)
    {
        _indices = indices;
        _values = values;
    }

    public static SparseVector Empty { get; } = new(Array.Empty<int>(), Array.Empty<float>());

    public int NonZeroCount => _indices.Length;

    /// <summary>Builds from term weights, dropping zeros and normalising to unit length.</summary>
    public static SparseVector FromWeights(IEnumerable<KeyValuePair<int, double>> weights)
    {
        ArgumentNullException.ThrowIfNull(weights);

        var pairs = weights.Where(kv => kv.Value != 0).OrderBy(kv => kv.Key).ToArray();
        if (pairs.Length == 0)
        {
            return Empty;
        }

        var norm = Math.Sqrt(pairs.Sum(kv => kv.Value * kv.Value));
        if (norm <= 0)
        {
            return Empty;
        }

        var idx = new int[pairs.Length];
        var val = new float[pairs.Length];
        for (var i = 0; i < pairs.Length; i++)
        {
            idx[i] = pairs[i].Key;
            val[i] = (float)(pairs[i].Value / norm);
        }

        return new SparseVector(idx, val);
    }

    /// <summary>
    /// Cosine similarity. Both operands are unit length, so this is just the dot product.
    /// Linear merge over sorted indices: O(nnz(a) + nnz(b)).
    /// </summary>
    public double Cosine(SparseVector other)
    {
        ArgumentNullException.ThrowIfNull(other);

        double sum = 0;
        int i = 0, j = 0;
        while (i < _indices.Length && j < other._indices.Length)
        {
            var ai = _indices[i];
            var bj = other._indices[j];
            if (ai == bj)
            {
                sum += (double)_values[i] * other._values[j];
                i++;
                j++;
            }
            else if (ai < bj)
            {
                i++;
            }
            else
            {
                j++;
            }
        }

        return sum;
    }
}

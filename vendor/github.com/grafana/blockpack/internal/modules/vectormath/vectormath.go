// Package vectormath provides Product Quantization (PQ) training, encoding, and
// asymmetric distance computation for float32 vectors.
// All distance operations use vek32 for SIMD-accelerated float32 arithmetic where available.
// This package has no imports from internal/modules/blockio.
package vectormath

import (
	"fmt"
	"math/rand"

	"github.com/viterin/vek/vek32"
)

// Codebook is a trained PQ codebook.
// numSubvecs is the number of subvectors; each subspace has numCentroids centroids of dimension Dim/numSubvecs.
type Codebook struct {
	// Centroids[m] is a flat row-major float32 slice of length numCentroids*subvecDim.
	// Centroid k in subspace m starts at index k*subvecDim.
	Centroids [][]float32
	M         int // number of subvectors (subspaces)
	K         int // number of centroids per subspace
	Dim       int // full vector dimension
}

// SubvecDim returns the dimension of each subvector.
func (cb *Codebook) SubvecDim() int { return cb.Dim / cb.M }

// Train runs Lloyd's k-means to build a PQ codebook from training vectors.
// vectors must be non-empty; all must have the same length (== Dim).
// numSubvecs must divide Dim evenly. numCentroids is capped at len(vectors) to avoid
// degenerate centroids. Returns error if Dim % numSubvecs != 0 or len(vectors) == 0.
func Train(vectors [][]float32, numSubvecs, numCentroids int) (Codebook, error) {
	if len(vectors) == 0 {
		return Codebook{}, fmt.Errorf("vectormath.Train: vectors must be non-empty")
	}
	dim := len(vectors[0])
	if dim == 0 {
		return Codebook{}, fmt.Errorf("vectormath.Train: vector dimension must be > 0")
	}
	if numSubvecs <= 0 {
		return Codebook{}, fmt.Errorf("vectormath.Train: numSubvecs must be > 0")
	}
	if dim%numSubvecs != 0 {
		return Codebook{}, fmt.Errorf("vectormath.Train: dim %d not divisible by numSubvecs %d", dim, numSubvecs)
	}
	subvecDim := dim / numSubvecs
	// Cap numCentroids at the number of training vectors to avoid empty centroids.
	effectiveK := numCentroids
	if effectiveK > len(vectors) {
		effectiveK = len(vectors)
	}
	if effectiveK < 1 {
		effectiveK = 1
	}

	// Create a per-call local rng to avoid data races when Train is called concurrently.
	//nolint:gosec
	rng := rand.New(rand.NewSource(42))
	centroids := make([][]float32, numSubvecs)
	for m := 0; m < numSubvecs; m++ {
		subvecs := extractSubvecs(vectors, m, subvecDim)
		centroids[m] = trainSubspace(subvecs, effectiveK, subvecDim, 20, rng)
	}

	return Codebook{
		Centroids: centroids,
		M:         numSubvecs,
		K:         effectiveK,
		Dim:       dim,
	}, nil
}

// extractSubvecs extracts the m-th subvector (of subvecDim floats) from each vector.
func extractSubvecs(vectors [][]float32, m, subvecDim int) [][]float32 {
	start := m * subvecDim
	end := start + subvecDim
	subs := make([][]float32, len(vectors))
	for i, v := range vectors {
		subs[i] = v[start:end]
	}
	return subs
}

// trainSubspace runs Lloyd's k-means on subvectors and returns a flat centroid array
// of length numCentroids*subvecDim. rng is a caller-supplied random source to avoid
// shared state across concurrent Train calls.
func trainSubspace(subvecs [][]float32, numCentroids, subvecDim, iters int, rng *rand.Rand) []float32 {
	n := len(subvecs)
	if numCentroids > n {
		numCentroids = n
	}

	// Initialize centroids by random sampling without replacement.
	perm := rng.Perm(n)
	centroids := make([][]float32, numCentroids)
	for k := 0; k < numCentroids; k++ {
		c := make([]float32, subvecDim)
		copy(c, subvecs[perm[k]])
		centroids[k] = c
	}

	assignments := make([]int, n)
	for iter := 0; iter < iters; iter++ {
		changed := assignVectors(subvecs, centroids, assignments)
		if !changed && iter > 0 {
			break
		}
		recomputeCentroids(subvecs, centroids, assignments, numCentroids, subvecDim, rng)
	}

	// Flatten into a single slice.
	flat := make([]float32, numCentroids*subvecDim)
	for k, c := range centroids {
		copy(flat[k*subvecDim:], c)
	}
	return flat
}

// assignVectors assigns each subvector to its nearest centroid.
// Returns true if any assignment changed.
func assignVectors(subvecs [][]float32, centroids [][]float32, assignments []int) bool {
	changed := false
	for i, v := range subvecs {
		best := nearestCentroid(v, centroids)
		if best != assignments[i] {
			assignments[i] = best
			changed = true
		}
	}
	return changed
}

// nearestCentroid returns the index of the centroid nearest to v (by squared L2).
func nearestCentroid(v []float32, centroids [][]float32) int {
	best := 0
	bestDist := l2SquaredDist(v, centroids[0])
	for k := 1; k < len(centroids); k++ {
		d := l2SquaredDist(v, centroids[k])
		if d < bestDist {
			bestDist = d
			best = k
		}
	}
	return best
}

// l2SquaredDist returns the squared L2 distance between two vectors via vek32.
func l2SquaredDist(a, b []float32) float32 {
	diff := vek32.Sub(a, b)
	return vek32.Dot(diff, diff)
}

// recomputeCentroids updates centroids as the mean of assigned subvectors.
// Empty clusters get re-initialized with a random assigned vector from rng.
func recomputeCentroids(
	subvecs [][]float32,
	centroids [][]float32,
	assignments []int,
	numCentroids, subvecDim int,
	rng *rand.Rand,
) {
	sums := make([][]float32, numCentroids)
	counts := make([]int, numCentroids)
	for k := 0; k < numCentroids; k++ {
		sums[k] = make([]float32, subvecDim)
	}
	for i, v := range subvecs {
		k := assignments[i]
		vek32.Add_Inplace(sums[k], v)
		counts[k]++
	}
	for k := 0; k < numCentroids; k++ {
		if counts[k] == 0 {
			j := rng.Intn(len(subvecs))
			copy(centroids[k], subvecs[j])
			continue
		}
		scale := float32(1.0) / float32(counts[k])
		vek32.MulNumber_Inplace(sums[k], scale)
		copy(centroids[k], sums[k])
	}
}

// Encode produces an M-byte PQ code for vector v using codebook cb.
// Each byte is the index of the nearest centroid in that subspace.
func Encode(v []float32, cb Codebook) []byte {
	subvecDim := cb.SubvecDim()
	code := make([]byte, cb.M)
	for m := 0; m < cb.M; m++ {
		sub := v[m*subvecDim : (m+1)*subvecDim]
		best := 0
		bestDist := squaredDistToFlat(sub, cb.Centroids[m], 0, subvecDim)
		for k := 1; k < cb.K; k++ {
			d := squaredDistToFlat(sub, cb.Centroids[m], k, subvecDim)
			if d < bestDist {
				bestDist = d
				best = k
			}
		}
		code[m] = byte(best)
	}
	return code
}

// squaredDistToFlat computes squared L2 distance between sub and the k-th centroid
// stored in flat (row-major, each centroid occupying subvecDim floats).
func squaredDistToFlat(sub, flat []float32, k, subvecDim int) float32 {
	centroid := flat[k*subvecDim : (k+1)*subvecDim]
	return l2SquaredDist(sub, centroid)
}

// DistanceTable precomputes a [M][K] distance table for query q against codebook cb.
// Each entry [m][k] is the squared L2 distance between q's m-th subvector and centroid k.
func DistanceTable(q []float32, cb Codebook) [][]float32 {
	subvecDim := cb.SubvecDim()
	table := make([][]float32, cb.M)
	for m := 0; m < cb.M; m++ {
		sub := q[m*subvecDim : (m+1)*subvecDim]
		row := make([]float32, cb.K)
		for k := 0; k < cb.K; k++ {
			row[k] = squaredDistToFlat(sub, cb.Centroids[m], k, subvecDim)
		}
		table[m] = row
	}
	return table
}

// ADCScoreFromTable sums precomputed distances for the given PQ code.
// Bounds guard: if code[m] >= len(table[m]) (corrupt PQ code byte), that subspace
// contributes 0 to the score rather than panicking. Valid codes produced by Encode
// always satisfy code[m] < K == len(table[m]).
func ADCScoreFromTable(code []byte, table [][]float32) float32 {
	var score float32
	for m, b := range code {
		if int(b) < len(table[m]) {
			score += table[m][b]
		}
	}
	return score
}

// ADCScore computes the asymmetric distance between query vector q and PQ code code.
// Equivalent to building a DistanceTable and calling ADCScoreFromTable, but less
// efficient for multiple codes — prefer DistanceTable + ADCScoreFromTable in hot loops.
func ADCScore(q []float32, code []byte, cb Codebook) float32 {
	table := DistanceTable(q, cb)
	return ADCScoreFromTable(code, table)
}

// CosineDistance returns 1 - cosine_similarity(a, b).
// Returns 1.0 (maximum distance) when either vector has zero norm.
func CosineDistance(a, b []float32) float32 {
	normA := vek32.Norm(a)
	normB := vek32.Norm(b)
	if normA == 0 || normB == 0 {
		return 1.0
	}
	dot := vek32.Dot(a, b)
	cosSim := dot / (normA * normB)
	// Clamp to [-1, 1] to guard against floating-point overshoot.
	if cosSim > 1.0 {
		cosSim = 1.0
	} else if cosSim < -1.0 {
		cosSim = -1.0
	}
	return 1.0 - cosSim
}

// Mean computes the element-wise mean of vectors. Returns a zero-length vector for empty input.
func Mean(vectors [][]float32) []float32 {
	if len(vectors) == 0 {
		return []float32{}
	}
	dim := len(vectors[0])
	result := make([]float32, dim)
	for _, v := range vectors {
		vek32.Add_Inplace(result, v)
	}
	scale := float32(1.0) / float32(len(vectors))
	vek32.MulNumber_Inplace(result, scale)
	return result
}

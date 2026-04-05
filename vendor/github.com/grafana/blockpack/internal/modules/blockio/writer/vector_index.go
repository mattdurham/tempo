package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.
// NOTE-VEC-001: vectorAccumulator follows the same accumulate-at-flush pattern as
// sketchIdx and fileBloomSvcNames. Do NOT serialize vectors inside flushBlocks().

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/vectormath"
)

const (
	pqM                = 96     // subvectors per vector (768 / 96 = 8-dim subvecs for nomic-embed-text-v1.5)
	pqK                = 256    // centroids per subspace
	maxTrainingSamples = 50_000 // reservoir sample cap for PQ training
)

// vectorBlockEntry records per-block data for the vector index.
type vectorBlockEntry struct {
	centroid    []float32   // mean of all vectors in this block
	pqCodes     [][]byte    // one M-byte PQ code per present vector in block order (set by build)
	vectors     [][]float32 // raw vectors for PQ encoding; cleared after build() to free memory
	vectorCount int
}

// vectorAccumulator accumulates per-block centroids and all vector data for PQ training.
// Fed during block build (one accumulateBlock call per block); consumed once at Flush.
// rng is per-instance to avoid data races when multiple Writers operate concurrently.
// Per-block raw vectors are stored in vectorBlockEntry.vectors; no global allVectors slice is kept.
type vectorAccumulator struct {
	//nolint:gosec
	rng            *rand.Rand // per-instance rng for reservoir sampling (lazy-initialized)
	blocks         []vectorBlockEntry
	trainingSample [][]float32 // reservoir-sampled vectors for PQ training (capped at maxTrainingSamples)
	dim            int         // set on first accumulateBlock; 0 = no vectors yet
	totalVectors   int
}

// newVectorAccumulator creates a vectorAccumulator with a per-instance rng.
// dim is the expected vector dimension (may be 0 if not yet known).
func newVectorAccumulator(dim int) *vectorAccumulator {
	//nolint:gosec
	return &vectorAccumulator{
		dim: dim,
		rng: rand.New(rand.NewSource(rand.Int63())), //nolint:gosec
	}
}

// accumulateBlock records all vectors from one block.
// vectors must be the slice of all present float32 vectors in block order.
// The centroid is computed as the mean of vectors; reservoir sampling updates trainingSample.
// Per-block vectors are stored in the entry for post-training PQ encoding; allVectors is not used.
func (a *vectorAccumulator) accumulateBlock(_ int, vectors [][]float32) {
	if len(vectors) == 0 {
		return
	}
	incomingDim := len(vectors[0])
	if a.dim == 0 {
		a.dim = incomingDim
	} else if incomingDim != a.dim {
		// Dimension mismatch — skip this block to avoid corrupting codebook training.
		return
	}
	if a.rng == nil {
		//nolint:gosec
		a.rng = rand.New(rand.NewSource(rand.Int63())) //nolint:gosec
	}

	centroid := vectormath.Mean(vectors)

	// Copy vectors for per-block storage used by build() for PQ encoding.
	perBlockVecs := make([][]float32, len(vectors))
	for i, v := range vectors {
		cp := make([]float32, len(v))
		copy(cp, v)
		perBlockVecs[i] = cp
	}

	entry := vectorBlockEntry{
		centroid:    centroid,
		vectorCount: len(vectors),
		vectors:     perBlockVecs,
	}

	for _, v := range perBlockVecs {
		a.totalVectors++
		if len(a.trainingSample) < maxTrainingSamples {
			cp := make([]float32, len(v))
			copy(cp, v)
			a.trainingSample = append(a.trainingSample, cp)
		} else {
			j := a.rng.Intn(a.totalVectors)
			if j < maxTrainingSamples {
				cp := make([]float32, len(v))
				copy(cp, v)
				a.trainingSample[j] = cp
			}
		}
	}
	a.blocks = append(a.blocks, entry)
}

// build trains the PQ codebook and encodes all vectors block-by-block.
// Returns (nil, nil) when no vectors were accumulated or insufficient training samples exist.
// Returns error if PQ training fails unexpectedly.
// After encoding, per-block raw vectors are cleared to free memory.
func (a *vectorAccumulator) build() ([]byte, error) {
	if a.totalVectors == 0 {
		return nil, nil
	}

	effectiveM, effectiveK := a.computePQParams()
	if effectiveK < 2 {
		return nil, nil // not enough training data to build a useful codebook
	}

	cb, err := vectormath.Train(a.trainingSample, effectiveM, effectiveK)
	if err != nil {
		return nil, fmt.Errorf("vectorAccumulator.build: PQ training: %w", err)
	}

	// Encode vectors block-by-block using per-block storage.
	// This avoids holding all file vectors in one giant slice simultaneously.
	for i := range a.blocks {
		vecs := a.blocks[i].vectors
		codes := make([][]byte, len(vecs))
		for j, v := range vecs {
			codes[j] = vectormath.Encode(v, cb)
		}
		a.blocks[i].pqCodes = codes
		a.blocks[i].vectors = nil // free per-block vectors after encoding
	}

	blockCentroids := make([][]float32, len(a.blocks))
	for i, b := range a.blocks {
		blockCentroids[i] = b.centroid
	}
	fileCentroid := vectormath.Mean(blockCentroids)

	return serializeVectorIndexSection(fileCentroid, a.blocks, cb), nil
}

// computePQParams calculates effective M and K given the accumulated dimension and training set size.
// effectiveM must divide dim; effectiveK = floor(trainingSamples / effectiveM), capped at pqK.
// We require effectiveK >= 2 so that PQ is meaningful. To satisfy this with small training sets,
// we cap effectiveM to floor(trainingSamples / 2) before choosing the largest valid divisor of dim.
func (a *vectorAccumulator) computePQParams() (effectiveM, effectiveK int) {
	// Maximum M allowed by training-set size: need at least 2 samples per subspace.
	maxMByTraining := len(a.trainingSample) / 2
	if maxMByTraining < 1 {
		maxMByTraining = 1
	}
	maxM := min(pqM, a.dim, maxMByTraining)
	effectiveM = largestDivisorUpTo(a.dim, maxM)
	effectiveK = min(pqK, len(a.trainingSample)/effectiveM)
	return effectiveM, effectiveK
}

// largestDivisorUpTo returns the largest divisor of n that is <= maxVal.
func largestDivisorUpTo(n, maxVal int) int {
	best := 1
	for d := 1; d*d <= n; d++ {
		if n%d == 0 {
			if d <= maxVal && d > best {
				best = d
			}
			other := n / d
			if other <= maxVal && other > best {
				best = other
			}
		}
	}
	return best
}

// serializeVectorIndexSection serializes the VectorIndex wire format.
//
// Wire format:
//
//	magic[4 LE] + version[1] + dim[2 LE] + m[2 LE] + k[2 LE] + num_blocks[4 LE]
//	file_centroid[dim*4]
//	per block: vector_count[4 LE] + centroid[dim*4] + pq_codes[vector_count*M bytes]
//	codebook[M * K * subvec_dim * 4]  (written last for sequential reader access)
func serializeVectorIndexSection(fileCentroid []float32, blocks []vectorBlockEntry, cb vectormath.Codebook) []byte {
	dim := cb.Dim
	subvecDim := cb.SubvecDim()

	headerSize := 4 + 1 + 2 + 2 + 2 + 4 // magic[4] + version[1] + dim[2] + m[2] + k[2] + num_blocks[4]
	fileCentroidSize := dim * 4
	blockBodySize := 0
	for _, b := range blocks {
		blockBodySize += 4 + dim*4 + b.vectorCount*cb.M // vector_count + centroid + pq_codes
	}
	codebookSize := cb.M * cb.K * subvecDim * 4

	total := headerSize + fileCentroidSize + blockBodySize + codebookSize
	buf := make([]byte, total)
	off := 0

	// Header.
	binary.LittleEndian.PutUint32(buf[off:], shared.VectorIndexMagic)
	off += 4
	buf[off] = shared.VectorIndexVersion
	off++
	binary.LittleEndian.PutUint16(buf[off:], uint16(dim)) //nolint:gosec // safe: dim <= 65535 (vector dimensions bounded by model spec)
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], uint16(cb.M)) //nolint:gosec // safe: M <= dim <= 65535
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], uint16(cb.K)) //nolint:gosec // safe: K <= 256 (pqK constant)
	off += 2
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(blocks))) //nolint:gosec // safe: block count bounded by writer limits
	off += 4

	// File centroid.
	off = writeFloat32Slice(buf, off, fileCentroid)

	// Per-block data.
	for _, b := range blocks {
		binary.LittleEndian.PutUint32(buf[off:], uint32(b.vectorCount)) //nolint:gosec // safe: vector count bounded by block size
		off += 4
		off = writeFloat32Slice(buf, off, b.centroid)
		for _, code := range b.pqCodes {
			copy(buf[off:], code)
			off += len(code)
		}
	}

	// Codebook: M subspaces, each K*subvecDim floats (stored flat per subspace).
	for _, centroidsFlat := range cb.Centroids {
		off = writeFloat32Slice(buf, off, centroidsFlat)
	}

	return buf
}

// writeFloat32Slice writes a []float32 to buf at offset off in IEEE 754 LE byte order.
// Returns the new offset.
func writeFloat32Slice(buf []byte, off int, vals []float32) int {
	for _, f := range vals {
		binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(f))
		off += 4
	}
	return off
}

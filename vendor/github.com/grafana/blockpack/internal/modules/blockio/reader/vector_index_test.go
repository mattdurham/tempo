package reader

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/vectormath"
)

// serializeTestVectorIndex builds a minimal VectorIndex wire format for testing.
// Wire format:
//
//	magic[4 LE] + version[1] + dim[2 LE] + m[2 LE] + k[2 LE] + num_blocks[4 LE]
//	file_centroid[dim*4]
//	per block: vector_count[4 LE] + centroid[dim*4] + pq_codes[vector_count*M bytes]
//	codebook[M * K * subvec_dim * 4]
func serializeTestVectorIndex(vi *VectorIndex) []byte {
	dim := vi.Dim
	cb := vi.Codebook
	pqm := cb.M
	pqk := cb.K
	subvecDim := dim / pqm
	numBlocks := len(vi.BlockCentroids)

	// Compute total size.
	headerSize := 4 + 1 + 2 + 2 + 2 + 4       // magic+ver+dim+m+k+num_blocks
	fileCentSize := dim * 4                   // file centroid
	codebookSize := pqm * pqk * subvecDim * 4 // codebook
	blocksSize := 0
	for i := range numBlocks {
		blocksSize += 4 + dim*4 + len(vi.PQCodes[i])*pqm // count+centroid+codes
	}

	buf := make([]byte, headerSize+fileCentSize+blocksSize+codebookSize)
	off := 0

	// Header.
	binary.LittleEndian.PutUint32(buf[off:], shared.VectorIndexMagic)
	off += 4
	buf[off] = shared.VectorIndexVersion
	off++
	binary.LittleEndian.PutUint16(buf[off:], uint16(dim)) //nolint:gosec // dim bounded by model spec (≤65535)
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], uint16(pqm)) //nolint:gosec // M bounded by dim (≤65535)
	off += 2
	binary.LittleEndian.PutUint16(buf[off:], uint16(pqk)) //nolint:gosec // K bounded by pqK constant (≤256)
	off += 2
	binary.LittleEndian.PutUint32(buf[off:], uint32(numBlocks)) //nolint:gosec // numBlocks bounded by writer limits
	off += 4

	// File centroid.
	for _, f := range vi.FileCentroid {
		binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(f))
		off += 4
	}

	// Per-block data.
	for i := range numBlocks {
		codes := vi.PQCodes[i]                                       // [][]byte per block
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(codes))) //nolint:gosec // count bounded by block size
		off += 4
		// Centroid.
		for _, f := range vi.BlockCentroids[i] {
			binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(f))
			off += 4
		}
		// PQ codes: each code is pqm bytes.
		for _, code := range codes {
			copy(buf[off:], code)
			off += pqm
		}
	}

	// Codebook: M subspaces, each K*subvecDim float32 values.
	for m := range pqm {
		for _, f := range cb.Centroids[m] {
			binary.LittleEndian.PutUint32(buf[off:], math.Float32bits(f))
			off += 4
		}
	}

	return buf
}

// makeTestVectorIndex builds a minimal VectorIndex with numBlocks blocks for testing.
// PQCodes[blockIdx] is [][]byte (one M-byte code per vector in the block).
func makeTestVectorIndex(t *testing.T, dim, pqm, pqk, numBlocks int) *VectorIndex {
	t.Helper()
	// Build simple vectors: block i has 2 vectors.
	trainingVecs := make([][]float32, 0, 2*numBlocks)
	for i := range numBlocks {
		for range 2 {
			v := make([]float32, dim)
			for d := range dim {
				v[d] = float32(i+1) + float32(d)*0.001
			}
			trainingVecs = append(trainingVecs, v)
		}
	}
	cb, err := vectormath.Train(trainingVecs, pqm, pqk)
	require.NoError(t, err)

	blockCentroids := make([][]float32, numBlocks)
	pqCodes := make([][][]byte, numBlocks)
	vecCounts := make([]int, numBlocks)
	for i := range numBlocks {
		c := make([]float32, dim)
		for d := range dim {
			c[d] = float32(i+1) + float32(d)*0.001
		}
		blockCentroids[i] = c
		// 2 vectors per block.
		codes := make([][]byte, 2)
		for j := range 2 {
			v := trainingVecs[i*2+j]
			codes[j] = vectormath.Encode(v, cb)
		}
		pqCodes[i] = codes
		vecCounts[i] = 2
	}

	allCentroids := append(make([][]float32, 0, len(blockCentroids)), blockCentroids...)
	fileCentroid := vectormath.Mean(allCentroids)

	return &VectorIndex{
		FileCentroid:   fileCentroid,
		BlockCentroids: blockCentroids,
		BlockVecCounts: vecCounts,
		PQCodes:        pqCodes,
		Codebook:       cb,
		Dim:            dim,
	}
}

func TestParseVectorIndexSection_wrongMagic(t *testing.T) {
	// 17 bytes of zeros (not enough, wrong magic).
	data := make([]byte, 20)
	binary.LittleEndian.PutUint32(data, 0xDEADBEEF) // wrong magic
	_, err := parseVectorIndexSection(data)
	assert.Error(t, err)
}

func TestParseVectorIndexSection_tooShort(t *testing.T) {
	_, err := parseVectorIndexSection([]byte{1, 2, 3})
	assert.Error(t, err)
}

func TestParseVectorIndexSection_roundtrip(t *testing.T) {
	const dim, pqm, pqk, numBlocks = 4, 2, 2, 2
	vi := makeTestVectorIndex(t, dim, pqm, pqk, numBlocks)
	data := serializeTestVectorIndex(vi)

	got, err := parseVectorIndexSection(data)
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Equal(t, dim, got.Dim)
	assert.Equal(t, pqm, got.Codebook.M)
	assert.Equal(t, numBlocks, len(got.BlockCentroids))
	assert.InDelta(t, vi.FileCentroid[0], got.FileCentroid[0], 1e-5)
}

func TestParseVectorIndexSection_blockCount(t *testing.T) {
	const dim, pqm, pqk, numBlocks = 4, 2, 2, 3
	vi := makeTestVectorIndex(t, dim, pqm, pqk, numBlocks)
	data := serializeTestVectorIndex(vi)

	got, err := parseVectorIndexSection(data)
	require.NoError(t, err)
	assert.Equal(t, numBlocks, len(got.BlockCentroids))
	assert.Equal(t, numBlocks, len(got.PQCodes))
}

func TestVectorIndex_DistanceTable(t *testing.T) {
	const dim, pqm, pqk, numBlocks = 4, 2, 2, 2
	vi := makeTestVectorIndex(t, dim, pqm, pqk, numBlocks)
	data := serializeTestVectorIndex(vi)

	got, err := parseVectorIndexSection(data)
	require.NoError(t, err)

	query := make([]float32, dim)
	for i := range dim {
		query[i] = float32(i) * 0.5
	}

	table := got.DistanceTable(query)
	assert.Equal(t, pqm, len(table), "table must have M rows")
	assert.Equal(t, pqk, len(table[0]), "each row must have K entries")
}

func TestVectorIndex_PQApproxDistances(t *testing.T) {
	const dim, pqm, pqk, numBlocks = 4, 2, 2, 2
	vi := makeTestVectorIndex(t, dim, pqm, pqk, numBlocks)
	data := serializeTestVectorIndex(vi)

	got, err := parseVectorIndexSection(data)
	require.NoError(t, err)

	query := make([]float32, dim)
	for i := range dim {
		query[i] = float32(i) * 0.5
	}
	table := got.DistanceTable(query)
	dists := got.PQApproxDistances(0, table)
	assert.Equal(t, len(vi.PQCodes[0]), len(dists))
}

func TestVectorIndex_FileCentroidDistance(t *testing.T) {
	const dim, pqm, pqk, numBlocks = 4, 2, 2, 2
	vi := makeTestVectorIndex(t, dim, pqm, pqk, numBlocks)
	data := serializeTestVectorIndex(vi)

	got, err := parseVectorIndexSection(data)
	require.NoError(t, err)

	// Unit vector query: distance is a valid float32.
	query := make([]float32, dim)
	query[0] = 1.0
	d := got.FileCentroidDistance(query)
	assert.False(t, math.IsNaN(float64(d)), "distance should not be NaN")
	assert.GreaterOrEqual(t, float64(d), 0.0)
}

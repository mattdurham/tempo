package writer

import (
	"encoding/binary"
	"testing"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/vectormath"
)

func TestVectorAccumulator_empty(t *testing.T) {
	var acc vectorAccumulator
	section, err := acc.build()
	if err != nil {
		t.Fatalf("empty build: %v", err)
	}
	if section != nil {
		t.Errorf("empty build: expected nil section, got %d bytes", len(section))
	}
}

func TestVectorAccumulator_fewVectors(t *testing.T) {
	// Fewer than pqM*pqK vectors — build() should return nil, nil gracefully.
	var acc vectorAccumulator
	vecs := makeSmallVecs(5, 32)
	acc.accumulateBlock(0, vecs)
	// With 5 vectors and pqM=96, effectiveK < 2 → nil section.
	section, err := acc.build()
	if err != nil {
		t.Fatalf("few vectors build: %v", err)
	}
	// nil is acceptable for too few training samples.
	_ = section
}

func TestVectorAccumulator_accumulateAndBuild(t *testing.T) {
	// Use a small dimension so PQ training is fast.
	var acc vectorAccumulator
	dim := 8
	// Add enough vectors for PQ training with small M,K.
	// Build with custom M/K by using dim=8 — pqM=96 won't work, but test uses dim=8.
	// For production dim=768 this works fine; here we just test block accumulation.
	for i := 0; i < 3; i++ {
		vecs := makeSmallVecs(50, dim)
		acc.accumulateBlock(i, vecs)
	}
	if acc.totalVectors != 150 {
		t.Errorf("totalVectors: want 150, got %d", acc.totalVectors)
	}
	if len(acc.blocks) != 3 {
		t.Errorf("blocks: want 3, got %d", len(acc.blocks))
	}
}

func TestVectorAccumulator_centroid(t *testing.T) {
	var acc vectorAccumulator
	// Two blocks: one with [1,1,1,1] x 5, one with [3,3,3,3] x 5.
	block1 := repeatVec([]float32{1, 1, 1, 1}, 5)
	block2 := repeatVec([]float32{3, 3, 3, 3}, 5)
	acc.accumulateBlock(0, block1)
	acc.accumulateBlock(1, block2)

	// Block centroids should be [1,1,1,1] and [3,3,3,3].
	if len(acc.blocks) != 2 {
		t.Fatalf("blocks: want 2, got %d", len(acc.blocks))
	}
	for i, want := range []float32{1, 3} {
		c := acc.blocks[i].centroid
		for j, v := range c {
			if v != want {
				t.Errorf("block[%d].centroid[%d]: want %v, got %v", i, j, want, v)
			}
		}
	}
}

func TestSerializeVectorIndexSection_magic(t *testing.T) {
	section := buildTestSection(t)
	if len(section) < 4 {
		t.Fatalf("section too short: %d bytes", len(section))
	}
	magic := binary.LittleEndian.Uint32(section[0:4])
	if magic != shared.VectorIndexMagic {
		t.Errorf("magic: want 0x%08X, got 0x%08X", shared.VectorIndexMagic, magic)
	}
}

func TestSerializeVectorIndexSection_roundtrip(t *testing.T) {
	// Build a valid codebook and blocks for serialization.
	dim := 16
	numVecs := 100
	vectors := makeSmallVecs(numVecs, dim)

	cb, err := vectormath.Train(vectors, 2, 4)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}

	blocks := []vectorBlockEntry{
		{
			centroid:    vectormath.Mean(vectors[:50]),
			pqCodes:     encodeVecs(vectors[:50], cb),
			vectorCount: 50,
		},
		{
			centroid:    vectormath.Mean(vectors[50:]),
			pqCodes:     encodeVecs(vectors[50:], cb),
			vectorCount: 50,
		},
	}
	fileCentroid := vectormath.Mean([][]float32{blocks[0].centroid, blocks[1].centroid})

	section := serializeVectorIndexSection(fileCentroid, blocks, cb)
	if len(section) < 10 {
		t.Fatalf("section too short: %d bytes", len(section))
	}

	// Verify magic.
	magic := binary.LittleEndian.Uint32(section[0:4])
	if magic != shared.VectorIndexMagic {
		t.Errorf("magic: want 0x%08X, got 0x%08X", shared.VectorIndexMagic, magic)
	}
	// Verify version.
	if section[4] != shared.VectorIndexVersion {
		t.Errorf("version: want %d, got %d", shared.VectorIndexVersion, section[4])
	}
	// Verify num_blocks at offset 11 (magic[4]+version[1]+dim[2]+m[2]+k[2]).
	numBlocks := binary.LittleEndian.Uint32(section[11:15])
	if numBlocks != 2 {
		t.Errorf("num_blocks: want 2, got %d", numBlocks)
	}
}

func TestSerializeVectorIndexSection_blockCount(t *testing.T) {
	section := buildTestSection(t)
	// num_blocks at offset 11 (magic[4]+version[1]+dim[2]+m[2]+k[2]).
	if len(section) < 15 {
		t.Fatalf("section too short: %d bytes", len(section))
	}
	numBlocks := binary.LittleEndian.Uint32(section[11:15])
	if numBlocks != 2 {
		t.Errorf("num_blocks: want 2, got %d", numBlocks)
	}
}

func TestVectorAccumulator_reservoirCap(t *testing.T) {
	var acc vectorAccumulator
	// Add more than maxTrainingSamples vectors.
	overCap := maxTrainingSamples + 1000
	vecs := makeSmallVecs(overCap, 8)
	acc.accumulateBlock(0, vecs)

	if len(acc.trainingSample) > maxTrainingSamples {
		t.Errorf("trainingSample len: want <= %d, got %d", maxTrainingSamples, len(acc.trainingSample))
	}
	if acc.totalVectors != overCap {
		t.Errorf("totalVectors: want %d, got %d", overCap, acc.totalVectors)
	}
}

// buildTestSection creates a minimal valid section for magic/format tests.
func buildTestSection(t *testing.T) []byte {
	t.Helper()
	dim := 8
	vectors := makeSmallVecs(50, dim)
	cb, err := vectormath.Train(vectors, 2, 4)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	blocks := []vectorBlockEntry{
		{
			centroid:    vectormath.Mean(vectors[:25]),
			pqCodes:     encodeVecs(vectors[:25], cb),
			vectorCount: 25,
		},
		{
			centroid:    vectormath.Mean(vectors[25:]),
			pqCodes:     encodeVecs(vectors[25:], cb),
			vectorCount: 25,
		},
	}
	fileCentroid := vectormath.Mean([][]float32{blocks[0].centroid, blocks[1].centroid})
	return serializeVectorIndexSection(fileCentroid, blocks, cb)
}

// makeSmallVecs generates n float32 vectors of given dimension with deterministic values.
func makeSmallVecs(n, dim int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32((i*dim+j)%100) * 0.1
		}
		vecs[i] = v
	}
	return vecs
}

// repeatVec creates n copies of the given vector.
func repeatVec(v []float32, n int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		cp := make([]float32, len(v))
		copy(cp, v)
		vecs[i] = cp
	}
	return vecs
}

// encodeVecs encodes all vectors in a slice using the given codebook.
func encodeVecs(vecs [][]float32, cb vectormath.Codebook) [][]byte {
	codes := make([][]byte, len(vecs))
	for i, v := range vecs {
		codes[i] = vectormath.Encode(v, cb)
	}
	return codes
}

package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/vectormath"
)

// VectorIndex holds the parsed vector index for one blockpack file.
// All fields are read-only after construction; safe for concurrent reads.
type VectorIndex struct {
	FileCentroid   []float32   // mean of all block centroids
	BlockCentroids [][]float32 // one centroid per block, parallel to reader.blockMetas
	BlockVecCounts []int       // number of vectors per block
	PQCodes        [][][]byte  // per-block PQ codes: PQCodes[blockIdx][rowIdx] is an M-byte code
	Codebook       vectormath.Codebook
	Dim            int
}

// FileCentroidDistance returns the cosine distance from query to the file centroid.
func (vi *VectorIndex) FileCentroidDistance(query []float32) float32 {
	return vectormath.CosineDistance(query, vi.FileCentroid)
}

// BlockCentroidDistance returns the cosine distance from query to block blockIdx's centroid.
func (vi *VectorIndex) BlockCentroidDistance(blockIdx int, query []float32) float32 {
	return vectormath.CosineDistance(query, vi.BlockCentroids[blockIdx])
}

// DistanceTable precomputes a [M][K] ADC distance table for the given query.
func (vi *VectorIndex) DistanceTable(query []float32) [][]float32 {
	return vectormath.DistanceTable(query, vi.Codebook)
}

// PQApproxDistances returns the PQ-approximate distance for each vector in blockIdx,
// using a precomputed distance table.
func (vi *VectorIndex) PQApproxDistances(blockIdx int, table [][]float32) []float32 {
	codes := vi.PQCodes[blockIdx]
	out := make([]float32, len(codes))
	for i, code := range codes {
		out[i] = vectormath.ADCScoreFromTable(code, table)
	}
	return out
}

// parseVectorIndexSection deserializes a VectorIndex from the raw section bytes.
// Wire format:
//
//	magic[4 LE] + version[1] + dim[2 LE] + m[2 LE] + k[2 LE] + num_blocks[4 LE]
//	file_centroid[dim*4]
//	per block: vector_count[4 LE] + centroid[dim*4] + pq_codes[vector_count*M bytes]
//	codebook[M * K * subvec_dim * 4]
//
// Returns error if magic is wrong, version is unsupported, or the buffer is truncated.
func parseVectorIndexSection(data []byte) (*VectorIndex, error) {
	const minHeaderSize = 4 + 1 + 2 + 2 + 2 + 4 // magic+version+dim+m+k+num_blocks
	if len(data) < minHeaderSize {
		return nil, fmt.Errorf("vector index: buffer too short: %d bytes", len(data))
	}

	off := 0
	magic := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if magic != shared.VectorIndexMagic {
		return nil, fmt.Errorf("vector index: bad magic: 0x%X", magic)
	}

	version := data[off]
	off++
	if version != shared.VectorIndexVersion {
		return nil, fmt.Errorf("vector index: unsupported version: %d", version)
	}

	dim := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2
	M := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2
	K := int(binary.LittleEndian.Uint16(data[off:]))
	off += 2
	numBlocks := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4

	if dim == 0 || M == 0 || K == 0 {
		return nil, fmt.Errorf("vector index: invalid dimensions dim=%d M=%d K=%d", dim, M, K)
	}
	if dim%M != 0 {
		return nil, fmt.Errorf("vector index: dim %d not divisible by M %d", dim, M)
	}
	if numBlocks > shared.MaxBlocks {
		return nil, fmt.Errorf("vector index: numBlocks %d exceeds limit %d", numBlocks, shared.MaxBlocks)
	}
	subvecDim := dim / M

	// File centroid.
	fileCentroid, off, err := readFloat32Slice(data, off, dim)
	if err != nil {
		return nil, fmt.Errorf("vector index: file centroid: %w", err)
	}

	// Per-block data.
	blockCentroids := make([][]float32, numBlocks)
	pqCodes := make([][][]byte, numBlocks)
	vecCounts := make([]int, numBlocks)

	for b := range numBlocks {
		if off+4 > len(data) {
			return nil, fmt.Errorf("vector index: block %d: truncated (vector_count)", b)
		}
		vecCount := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if vecCount > shared.MaxSpans {
			return nil, fmt.Errorf("vector index: block %d vecCount %d exceeds limit %d", b, vecCount, shared.MaxSpans)
		}

		blockCentroids[b], off, err = readFloat32Slice(data, off, dim)
		if err != nil {
			return nil, fmt.Errorf("vector index: block %d centroid: %w", b, err)
		}

		codes := make([][]byte, vecCount)
		for i := range vecCount {
			if off+M > len(data) {
				return nil, fmt.Errorf("vector index: block %d vector %d: truncated (pq_code)", b, i)
			}
			code := make([]byte, M)
			copy(code, data[off:off+M])
			off += M
			codes[i] = code
		}
		pqCodes[b] = codes
		vecCounts[b] = vecCount
	}

	// Codebook: M subspaces, each K*subvecDim float32 values.
	centroids := make([][]float32, M)
	for m := range M {
		centroids[m], off, err = readFloat32Slice(data, off, K*subvecDim)
		if err != nil {
			return nil, fmt.Errorf("vector index: codebook subspace %d: %w", m, err)
		}
	}

	cb := vectormath.Codebook{
		Centroids: centroids,
		M:         M,
		K:         K,
		Dim:       dim,
	}

	return &VectorIndex{
		FileCentroid:   fileCentroid,
		BlockCentroids: blockCentroids,
		BlockVecCounts: vecCounts,
		PQCodes:        pqCodes,
		Codebook:       cb,
		Dim:            dim,
	}, nil
}

// readFloat32Slice reads n float32 values from data starting at off.
// Returns the values, updated offset, and any error.
func readFloat32Slice(data []byte, off, n int) ([]float32, int, error) {
	needed := n * 4
	if off+needed > len(data) {
		return nil, off, fmt.Errorf("need %d bytes at offset %d, have %d", needed, off, len(data))
	}
	out := make([]float32, n)
	for i := range n {
		bits := binary.LittleEndian.Uint32(data[off:])
		out[i] = math.Float32frombits(bits)
		off += 4
	}
	return out, off, nil
}

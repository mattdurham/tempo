package reader

import "github.com/grafana/blockpack/internal/modules/vectormath"

// VectorIndex is a blockpack data type.
type VectorIndex struct {
	FileCentroid   []float32
	BlockCentroids [][]float32
	BlockVecCounts []int
	PQCodes        [][][]byte
	Codebook       vectormath.Codebook
	Dim            int
}

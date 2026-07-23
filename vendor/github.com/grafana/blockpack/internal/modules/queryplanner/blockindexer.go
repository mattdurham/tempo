package queryplanner

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// BlockIndexer is a blockpack data type.
type BlockIndexer interface {
	BlockCount() int
	BlockMeta(blockIdx int) shared.BlockMeta
	ReadBlocks(blockIndices []int) (map[int][]byte, error)
	BlocksInTimeRange(minNano, maxNano uint64) []int
	// MayContainColumn reports whether the block at blockIdx may contain a column named
	// name (issue #531). Returns false only when definitively absent (no false
	// negatives); true when present, unknown (old files), or out of range. See
	// reader.Reader.MayContainColumn, which satisfies this method.
	MayContainColumn(blockIdx int, name string) bool
}

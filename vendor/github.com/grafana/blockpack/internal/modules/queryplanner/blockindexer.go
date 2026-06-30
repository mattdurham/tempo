package queryplanner

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// BlockIndexer is a blockpack data type.
type BlockIndexer interface {
	BlockCount() int
	BlockMeta(blockIdx int) shared.BlockMeta
	ReadBlocks(blockIndices []int) (map[int][]byte, error)
	BlocksInTimeRange(minNano, maxNano uint64) []int
}

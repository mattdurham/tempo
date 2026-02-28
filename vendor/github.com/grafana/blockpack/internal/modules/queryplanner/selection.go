package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// pruneByBloom removes from candidates any block that cannot satisfy pred.
//
// A block is removed when the bloom filter reports ALL of pred.Columns as definitely
// absent (none of the OR columns are present). If pred.Columns is empty, no blocks
// are removed. Returns the count of removed blocks.
func pruneByBloom(r BlockIndexer, candidates map[int]struct{}, pred Predicate) int {
	if len(pred.Columns) == 0 {
		return 0
	}
	pruned := 0
	for blockIdx := range candidates {
		bloom := r.BlockMeta(blockIdx).ColumnNameBloom
		if !anyColumnPresent(bloom[:], pred.Columns) {
			delete(candidates, blockIdx)
			pruned++
		}
	}
	return pruned
}

// pruneByIndex narrows candidates using the range index for pred.
//
// When pred.Values is non-empty and pred.Columns has exactly one column that is
// indexed, the planner unions BlocksForRange results for each value, then
// intersects the resulting set with the current candidates. Blocks that survive
// bloom but are not in the union are removed (pruned by range index).
//
// Returns the number of additionally pruned blocks and any lookup error.
// On error, candidates is left unchanged and the error is returned.
func pruneByIndex(r BlockIndexer, candidates map[int]struct{}, pred Predicate) (int, error) {
	if len(pred.Values) == 0 || len(pred.Columns) != 1 {
		return 0, nil
	}
	col := pred.Columns[0]
	if _, ok := r.RangeColumnType(col); !ok {
		return 0, nil
	}

	// Union all block sets for each query value.
	union := make(map[int]struct{})
	for _, val := range pred.Values {
		blocks, err := r.BlocksForRange(col, val)
		if err != nil {
			return 0, err
		}
		for _, b := range blocks {
			union[b] = struct{}{}
		}
	}

	// Intersect: remove candidates not in union.
	pruned := 0
	for blockIdx := range candidates {
		if _, ok := union[blockIdx]; !ok {
			delete(candidates, blockIdx)
			pruned++
		}
	}
	return pruned, nil
}

// anyColumnPresent returns true if the bloom filter reports any column as possibly
// present. Returns false only when every column is definitely absent.
func anyColumnPresent(bloom []byte, columns []string) bool {
	for _, col := range columns {
		if shared.TestBloom(bloom, col) {
			return true
		}
	}
	return false
}

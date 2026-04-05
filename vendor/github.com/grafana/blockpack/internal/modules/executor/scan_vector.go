package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"sort"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/vm"
)

// applyVectorScorerToBlock runs program.VectorScorer against the candidates RowSet
// using point-lookup VectorF32Value on the named vector column.
// Returns the scored rows (already threshold-filtered, sorted descending by score).
// If the block has no embedding column or program.VectorScorer is nil, returns nil.
func applyVectorScorerToBlock(block *modules_reader.Block, program *vm.Program, candidates vm.RowSet) []vm.ScoredRow {
	if program.VectorScorer == nil {
		return nil
	}
	col := block.GetColumn(program.VectorColumn)
	if col == nil {
		return nil
	}
	getVec := func(rowIdx int) ([]float32, bool) {
		return col.VectorF32Value(rowIdx)
	}
	return program.VectorScorer(getVec, candidates)
}

// vectorTopKFromScoredRows collects the global top-K MatchedRows across all blocks
// from accumulated []vm.ScoredRow results. limit <= 0 uses DefaultVectorLimit.
// Rows are sorted by Score descending before truncation.
func vectorTopKFromScoredRows(rows []MatchedRow, limit int) []MatchedRow {
	if limit <= 0 {
		limit = vm.DefaultVectorLimit
	}
	// Sort by score descending.
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Score > rows[j].Score
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

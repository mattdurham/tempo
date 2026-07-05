package executor

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// MatchedRow is a blockpack data type.
type MatchedRow struct {
	Block    *modules_reader.Block
	BlockIdx int
	RowIdx   int
}

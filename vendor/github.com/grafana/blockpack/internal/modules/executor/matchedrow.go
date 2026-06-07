package executor

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// MatchedRow is a blockpack data type.
type MatchedRow struct {
	Block           *modules_reader.Block
	IntrinsicFields modules_shared.SpanFieldsProvider
	Score           float32
	BlockIdx        int
	RowIdx          int
}

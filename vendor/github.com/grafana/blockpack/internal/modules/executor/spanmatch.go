package executor

import modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"

// SpanMatch is a blockpack data type.
type SpanMatch struct {
	Block    *modules_reader.Block
	SpanID   []byte
	BlockIdx int
	RowIdx   int
	TraceID  [16]byte
}

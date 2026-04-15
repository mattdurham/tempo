// Package executor executes TraceQL filter queries against modules blockpack files.
//
// Responsibility boundary:
//   - executor owns: block scanning, span-level predicate evaluation, result collection
//   - queryplanner owns: which blocks to read (bloom filter, range index pruning)
//   - blockio/reader owns: how to read them (coalescing, wire parsing)
//
// # Usage
//
//	rows, err := executor.Collect(r, program, executor.CollectOptions{})
//	// rows contains matched MatchedRow values; use SpanMatchFromRow to extract TraceID/SpanID
//
//	rows, logErr = executor.CollectLogs(r, program, nil, executor.CollectOptions{TimestampColumn: "log:timestamp"})
package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// SpanMatch is a span that matched the query.
type SpanMatch struct {
	SpanID   []byte   // 8-byte span ID
	BlockIdx int      // block index within the file
	RowIdx   int      // row (span) index within the block
	TraceID  [16]byte // 16-byte trace ID
}

// Result is the output of Execute.
type Result struct {
	Plan          *queryplanner.Plan // block selection plan (pruning stats)
	Matches       []SpanMatch        // matching spans
	BytesRead     int64              // total raw bytes read from the provider
	BlocksScanned int                // blocks that were actually read and evaluated
}

// Options controls query execution behavior.
type Options struct {
	// TimeRange constrains block scanning to blocks whose time window overlaps this range.
	// A zero-value TimeRange disables time pruning.
	TimeRange queryplanner.TimeRange
	// Limit caps the number of returned matches. 0 means no limit.
	Limit int
	// StartBlock is the first internal block index to include (0-based, inclusive).
	// Used by the frontend sharder to partition a single file across multiple jobs.
	// 0 with BlockCount==0 means scan all blocks (no sub-file sharding).
	StartBlock int
	// BlockCount is the number of internal blocks to include starting from StartBlock.
	// 0 means no sub-file sharding (scan all blocks selected by the planner).
	BlockCount int
}

// SpanMatchFromRow extracts a SpanMatch from a MatchedRow by reading the appropriate
// trace and span identity columns for the given signal type. For trace signals it
// reads "trace:id" and "span:id"; for log signals it reads "log:trace_id" and "log:span_id".
//
// r is the Reader used to look up trace identity fields from the intrinsic section when
// row.IntrinsicFields is nil and the columns are not present in the decoded Block.
// Pass nil only for log signals (log identity columns remain in block columns).
//
// Supports both row representations:
//   - IntrinsicFields-populated rows (range-predicate Case A, Case B): reads from IntrinsicFields.
//   - Block-populated rows (block-scan path): reads from Block columns, then falls back
//     to intrinsic section via r when trace identity columns are absent from the Block.
func SpanMatchFromRow(row MatchedRow, signalType uint8, r *modules_reader.Reader) SpanMatch {
	m := SpanMatch{BlockIdx: row.BlockIdx, RowIdx: row.RowIdx}

	traceIDCol := "trace:id"
	spanIDCol := "span:id"
	if signalType == modules_shared.SignalTypeLog {
		traceIDCol = "log:trace_id"
		spanIDCol = "log:span_id"
	}

	if row.IntrinsicFields != nil {
		// Range-predicate Case A and Case B: IDs are in the IntrinsicFields map.
		if v, ok := row.IntrinsicFields.GetField(traceIDCol); ok {
			if b, ok := v.([]byte); ok && len(b) == 16 {
				copy(m.TraceID[:], b)
			}
		}
		if v, ok := row.IntrinsicFields.GetField(spanIDCol); ok {
			if b, ok := v.([]byte); ok {
				m.SpanID = make([]byte, len(b))
				copy(m.SpanID, b)
			}
		}
		return m
	}

	if row.Block == nil {
		return m
	}

	// Block-scan path: try to read from decoded Block columns first.
	// PATTERN: block-column-first with intrinsic-section fallback (shared across
	// compaction/compaction.go, writer/writer.go, executor.go, executor/metrics_trace.go).
	// v3 files store identity columns in block payloads; v4 files store them exclusively
	// in the intrinsic section. Try the block column first for backwards compat.
	// For trace signals, trace:id and span:id are no longer present in block columns
	// (intrinsic separation). Fall back to intrinsic section via r when missing.
	if col := row.Block.GetColumn(traceIDCol); col != nil {
		if v, ok := col.BytesValue(row.RowIdx); ok && len(v) == 16 {
			copy(m.TraceID[:], v)
		}
	} else if r != nil && signalType != modules_shared.SignalTypeLog {
		// Trace identity columns are intrinsic-only; look up via reader.
		spanRef := modules_shared.BlockRef{
			BlockIdx: uint16(row.BlockIdx), //nolint:gosec
			RowIdx:   uint16(row.RowIdx),   //nolint:gosec
		}
		idCols := map[string]struct{}{traceIDCol: {}, spanIDCol: {}}
		fieldMaps := lookupIntrinsicFields(r, []modules_shared.BlockRef{spanRef}, idCols)
		if len(fieldMaps) > 0 && fieldMaps[0] != nil {
			if v, ok := fieldMaps[0][traceIDCol]; ok {
				if b, ok := v.([]byte); ok && len(b) == 16 {
					copy(m.TraceID[:], b)
				}
			}
			if v, ok := fieldMaps[0][spanIDCol]; ok {
				if b, ok := v.([]byte); ok {
					m.SpanID = make([]byte, len(b))
					copy(m.SpanID, b)
				}
			}
		}
		return m
	}

	if col := row.Block.GetColumn(spanIDCol); col != nil {
		if v, ok := col.BytesValue(row.RowIdx); ok {
			m.SpanID = make([]byte, len(v))
			copy(m.SpanID, v)
		}
	}
	return m
}

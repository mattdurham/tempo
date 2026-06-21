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
package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// SpanMatch is a span that matched the query.

// block containing this span; populated by structural executor
// 8-byte span ID
// block index within the file
// row (span) index within the block
// 16-byte trace ID

// Options controls query execution behavior.

// TimeRange constrains block scanning to blocks whose time window overlaps this range.
// A zero-value TimeRange disables time pruning.

// Limit caps the number of returned matches. 0 means no limit.

// StartBlock is the first internal block index to include (0-based, inclusive).
// Used by the frontend sharder to partition a single file across multiple jobs.
// 0 with BlockCount==0 means scan all blocks (no sub-file sharding).

// BlockCount is the number of internal blocks to include starting from StartBlock.
// 0 means no sub-file sharding (scan all blocks selected by the planner).

// SpanMatchFromRow extracts a SpanMatch from a MatchedRow by reading the "trace:id"
// and "span:id" identity columns.
//
// r is the Reader used to look up trace identity fields from the intrinsic section when
// row.IntrinsicFields is nil and the columns are not present in the decoded Block.
//
// Supports both row representations:
//   - IntrinsicFields-populated rows (range-predicate Case A, Case B): reads from IntrinsicFields.
//   - Block-populated rows (block-scan path): reads from Block columns, then falls back
//     to intrinsic section via r when trace identity columns are absent from the Block.
func SpanMatchFromRow(row MatchedRow, r *modules_reader.Reader) (SpanMatch, error) {
	m := SpanMatch{BlockIdx: row.BlockIdx, RowIdx: row.RowIdx}

	const traceIDCol = "trace:id"
	const spanIDCol = "span:id"

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
		return m, nil
	}

	if row.Block == nil {
		return m, nil
	}

	// Block-scan path: try to read from decoded Block columns first.
	// PATTERN: block-column-first with intrinsic-section fallback (shared across
	// compaction/compaction.go, writer/writer.go, executor.go, executor/metrics_trace.go).
	// Current files store trace:id and span:id BOTH as per-row block columns and in the
	// intrinsic section; read the block column first (no extra I/O — the block is decoded
	// already) and fall back to the intrinsic section only when a block lacks the column.
	if col := row.Block.GetColumn(traceIDCol); col != nil {
		if v, ok := col.BytesValue(row.RowIdx); ok && len(v) == 16 {
			copy(m.TraceID[:], v)
		}
	} else if r != nil {
		// Block lacks the identity column: look it up via the intrinsic section.
		spanRef := modules_shared.BlockRef{
			BlockIdx: uint16(row.BlockIdx), //nolint:gosec
			RowIdx:   uint16(row.RowIdx),   //nolint:gosec
		}
		idCols := map[string]struct{}{traceIDCol: {}, spanIDCol: {}}
		fieldMaps, intrinsicErr := lookupIntrinsicFields(r, []modules_shared.BlockRef{spanRef}, idCols)
		if intrinsicErr != nil {
			return m, fmt.Errorf("SpanMatchFromRow lookupIntrinsicFields: %w", intrinsicErr)
		}
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
		return m, nil
	}

	if col := row.Block.GetColumn(spanIDCol); col != nil {
		if v, ok := col.BytesValue(row.RowIdx); ok {
			m.SpanID = make([]byte, len(v))
			copy(m.SpanID, v)
		}
	}
	return m, nil
}

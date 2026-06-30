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
// and "span:id" identity columns directly from the decoded block.
//
// NOTE-436: identity columns are regular per-row block columns; there is no intrinsic
// section to fall back to.
func SpanMatchFromRow(row MatchedRow) (SpanMatch, error) {
	m := SpanMatch{BlockIdx: row.BlockIdx, RowIdx: row.RowIdx}

	const traceIDCol = "trace:id"
	const spanIDCol = "span:id"

	if row.Block == nil {
		return m, nil
	}

	if col := row.Block.GetColumn(traceIDCol); col != nil {
		if v, ok := col.BytesValue(row.RowIdx); ok && len(v) == 16 {
			copy(m.TraceID[:], v)
		}
	}

	if col := row.Block.GetColumn(spanIDCol); col != nil {
		if v, ok := col.BytesValue(row.RowIdx); ok {
			m.SpanID = make([]byte, len(v))
			copy(m.SpanID, v)
		}
	}
	return m, nil
}

package executor

// NOTE-357: field types narrowed from int to width-appropriate types to shrink the
// per-record footprint from 48 to 32 bytes (-33%). structuralSpanRec is appended once per
// matching span across EVERY selected block of a structural query and retained in the
// result map for the whole query, so for heavy multi-block structural scans the slice of
// records is a large per-query peak allocation (collectBlockStructuralSpanRecs was a top
// inuse_space frame, ~414 MB live). The narrowed types are all provably sufficient:
//   - blockIdx, rowIdx: a block index and a span row index, both bounded by the file's
//     block count and SpanCount (≤65535, MaxBlockSpans) — uint16 covers them exactly.
//   - parentIdx: an index into the per-trace spans slice (count of spans for one trace),
//     with -1 as the "no parent" sentinel; int32 is ample and preserves the signed sentinel.
//
// NOTE-373: traceID ([16]byte) is carried on the record itself so records can be accumulated
// into a single FLAT slice across all blocks and grouped by trace ID in one final pass, instead
// of appending into a per-trace result[traceID] slice per row (the prior dominant alloc). It is
// only read by groupMatchingStructuralTraces; once grouped it is redundant with the map key but
// retaining it costs nothing on the hot path and keeps the grouping pass branchless.
//
// Field order places the 16-byte array first, then the two 8-byte arrays, then the 4-byte
// int32, then the 2-byte uint16s and the two uint8 flags, so the struct packs with no interior
// padding (16 + 8 + 8 + 4 + 2 + 2 + 1 + 1 = 42 → 44 bytes after trailing alignment).
type structuralSpanRec struct {
	traceID   [16]byte
	spanID    [8]byte
	parentID  [8]byte
	parentIdx int32
	blockIdx  uint16
	rowIdx    uint16
	nodeMatch uint8
	present   uint8
}

package executor

import "sync"

// NOTE-436: pool the cross-block `flat []structuralSpanRec` accumulator.
//
// collectAllStructuralSpans sizes `flat` to the summed SpanCount of EVERY block in the union
// of both structural nodes' selected-block sets (the worst-case record count), appends one
// record per trace-ID-bearing span across all those blocks, and then hands the whole slice to
// groupMatchingStructuralTraces, which scatters survivors into a FRESH backing array and
// returns windows aliasing that backing — never `flat`. So `flat` is fully consumed within
// collectAllStructuralSpans and never escapes: a textbook pool target. The 2026-06-17 alloc
// profile attributed ~9.8 GB self to collectAllStructuralSpans, dominated by this single
// per-query make([]structuralSpanRec, 0, totalSpans). structuralSpanRec is pointer-free
// (NOTE-357: all-value [16]byte/[8]byte/int32/uint16/uint8 fields), so the pooled backing
// carries no live pointers for the GC to scan between uses and the reused array does not pin
// any string/byte payload.
// The pool stores *[]structuralSpanRec (pointer-to-slice) rather than the slice value, so
// boxing into interface{} for Put never allocates the slice header (staticcheck SA6002).
var structuralSpanRecPool sync.Pool

// acquireStructuralSpanRecs returns a zero-length []structuralSpanRec with capacity ≥ n drawn
// from the pool (or a fresh allocation on miss/undersize). It is returned length 0 so the
// caller appends exactly as before; the backing contents are irrelevant because every appended
// element is fully written.
func acquireStructuralSpanRecs(n int) []structuralSpanRec {
	if v := structuralSpanRecPool.Get(); v != nil {
		if p, ok := v.(*[]structuralSpanRec); ok && cap(*p) >= n {
			return (*p)[:0]
		}
	}
	return make([]structuralSpanRec, 0, n)
}

// releaseStructuralSpanRecs returns the backing array to the pool. Oversized outliers are
// dropped (NOTE-355) so a single huge structural query does not pin a multi-hundred-MB array
// in the pool forever. structuralSpanRec is 44 bytes; the 32 MiB ceiling pools up to ~760K
// records, covering the common structural shape.
func releaseStructuralSpanRecs(s []structuralSpanRec) {
	if cap(s)*structuralSpanRecBytes > compactPoolMaxPooledBytes { // NOTE-355: drop oversized outlier
		return
	}
	full := s[:cap(s)]
	structuralSpanRecPool.Put(&full)
}

// structuralSpanRecBytes is the in-memory size of one structuralSpanRec used to bound the
// pooled backing array (see releaseStructuralSpanRecs). It mirrors the field layout below.
const structuralSpanRecBytes = 44

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

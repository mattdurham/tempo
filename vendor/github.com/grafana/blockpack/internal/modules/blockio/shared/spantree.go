package shared

// NOTE-462 (issue #381): SpanTree is a parent-child structural index stored as a new ToC
// entry (ToCSubTypeSpanTree). Each record carries (traceID, spanID, parentID, dfsIn, dfsOut,
// blockIdx, rowIdx). The DFS numbering (depth-first entry/exit counters assigned per trace)
// collapses ancestor/descendant checks to two integer comparisons:
//
//	J is a descendant of I  iff  I.dfsIn < J.dfsIn  AND  J.dfsOut < I.dfsOut
//
// (equivalently J.dfsIn falls strictly inside (I.dfsIn, I.dfsOut)). Siblings share the same
// parentID within the same trace; root spans have a zero parentID.
//
// This file holds the WIRE-LEVEL record codec and the relationship predicates shared by the
// writer (encode) and reader (decode). The section framing (header + chunk directory + bloom)
// mirrors the chunked trace index and lives in writer/spantree_tempfile.go (encode) and
// reader/spantree.go (decode). DFS numbering is restricted to uint32 per trace, which bounds
// the largest single trace, not the whole file — consistent with the issue's streaming model.

import "encoding/binary"

// SpanTreeRecord is one decoded SpanTree row. Field order matches the on-disk record layout.
type SpanTreeRecord struct {
	TraceID  [16]byte
	SpanID   [8]byte
	ParentID [8]byte // zero = root span
	DFSIn    uint32
	DFSOut   uint32
	BlockIdx uint16
	RowIdx   uint16
}

// EncodeSpanTreeRecord writes one fixed-stride SpanTreeRecord into dst, which must be at
// least SpanTreeRecordSize bytes. It returns the number of bytes written (SpanTreeRecordSize).
func EncodeSpanTreeRecord(dst []byte, rec SpanTreeRecord) int {
	_ = dst[SpanTreeRecordSize-1] // bounds-check hint
	copy(dst[0:16], rec.TraceID[:])
	copy(dst[16:24], rec.SpanID[:])
	copy(dst[24:32], rec.ParentID[:])
	binary.LittleEndian.PutUint32(dst[32:36], rec.DFSIn)
	binary.LittleEndian.PutUint32(dst[36:40], rec.DFSOut)
	binary.LittleEndian.PutUint16(dst[40:42], rec.BlockIdx)
	binary.LittleEndian.PutUint16(dst[42:44], rec.RowIdx)
	return SpanTreeRecordSize
}

// DecodeSpanTreeRecord reads one fixed-stride SpanTreeRecord from src, which must be at least
// SpanTreeRecordSize bytes.
func DecodeSpanTreeRecord(src []byte) SpanTreeRecord {
	_ = src[SpanTreeRecordSize-1] // bounds-check hint
	var rec SpanTreeRecord
	copy(rec.TraceID[:], src[0:16])
	copy(rec.SpanID[:], src[16:24])
	copy(rec.ParentID[:], src[24:32])
	rec.DFSIn = binary.LittleEndian.Uint32(src[32:36])
	rec.DFSOut = binary.LittleEndian.Uint32(src[36:40])
	rec.BlockIdx = binary.LittleEndian.Uint16(src[40:42])
	rec.RowIdx = binary.LittleEndian.Uint16(src[42:44])
	return rec
}

// IsDescendant reports whether span is a (strict) descendant of ancestor within the same
// trace, using only the DFS in/out counters. The caller is responsible for ensuring both
// records belong to the same trace (DFS numbering is per-trace, so cross-trace comparisons
// are meaningless). A span is never its own descendant.
func IsDescendant(ancestor, span SpanTreeRecord) bool {
	return ancestor.DFSIn < span.DFSIn && span.DFSOut < ancestor.DFSOut
}

// IsAncestor reports whether span is a (strict) ancestor of descendant within the same trace.
func IsAncestor(ancestor, descendant SpanTreeRecord) bool {
	return IsDescendant(ancestor, descendant)
}

// IsRoot reports whether the record is a root span (zero parent ID).
func (r SpanTreeRecord) IsRoot() bool {
	return r.ParentID == [8]byte{}
}

// AreSiblings reports whether two records are siblings: same non-zero parent ID within the
// same trace. Two root spans of the same trace are also siblings (shared zero parent).
func AreSiblings(a, b SpanTreeRecord) bool {
	return a.TraceID == b.TraceID && a.ParentID == b.ParentID && a.SpanID != b.SpanID
}

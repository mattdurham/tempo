package reader

import "sync"

type compactTraceIndex struct {
	traceIndexFetchErr error
	traceIndexRaw      []byte
	blockTable         []compactBlockEntry
	traceIDBloom       []byte
	// NOTE-260/267: dense offset index for binary-search lookups into the
	// variable-stride sorted trace-index table. Built lazily on first scan.
	// traceIdxOffsets holds the byte offset of EVERY trace entry within
	// traceIndexRaw (one int32 per entry). scanTraceIndexRaw binary-searches it,
	// reading each candidate's 16-byte trace ID directly from traceIndexRaw -- so
	// the lookup makes zero traceEntryStride calls (the entry layout is only walked
	// once, at build time). Trace IDs are not stored: they are read on the fly from
	// the offsets, keeping the index at 4 bytes/entry.
	traceIdxOffsets   []int32
	traceIndexOffset  uint64
	traceIndexLen     uint64
	traceIdxSampleOK  bool
	isV14TraceSection bool
	traceIndexOnce    sync.Once
	traceIdxIndexOnce sync.Once
}

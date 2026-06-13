package reader

import "sync"

type compactTraceIndex struct {
	traceIndexFetchErr error
	traceIndexRaw      []byte
	blockTable         []compactBlockEntry
	traceIDBloom       []byte
	// NOTE-260: sparse offset index for binary-search lookups into the
	// variable-stride sorted trace-index table. Built lazily on first scan.
	// traceIdxSamples holds (traceID, byteOffset) for every traceIdxSampleStride-th
	// entry; scanTraceIndexRaw binary-searches it to bound the linear scan to a
	// single stride window instead of walking the whole table.
	traceIdxSamples   []traceIdxSample
	traceIndexOffset  uint64
	traceIndexLen     uint64
	traceIdxSampleOK  bool
	isV14TraceSection bool
	traceIndexOnce    sync.Once
	traceIdxIndexOnce sync.Once
}

// traceIdxSample is one sparse-index entry: the trace ID at a sampled position and
// the byte offset of that entry within traceIndexRaw.
type traceIdxSample struct {
	traceID [16]byte
	offset  int
}

// traceIdxSampleStride is the number of trace entries between consecutive samples.
// A lookup binary-searches the samples (O(log(traceCount/stride))) then linearly
// scans at most this many entries within the located window. 64 keeps the sample
// table small (~24 bytes/sample => ~37 KB for 100k traces) while bounding the scan.
const traceIdxSampleStride = 64

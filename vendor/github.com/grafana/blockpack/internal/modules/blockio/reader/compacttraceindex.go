package reader

import "sync"

type compactTraceIndex struct {
	traceIndexFetchErr error
	traceIndexRaw      []byte
	blockTable         []compactBlockEntry
	traceIDBloom       []byte
	// NOTE-260/279: sparse offset index for binary-search lookups into the
	// variable-stride sorted trace-index table. Built lazily on first scan.
	// traceIdxOffsets holds the byte offset of every traceIdxSampleStride-th trace
	// entry within traceIndexRaw (one int32 per sample). scanTraceIndexRaw
	// binary-searches the samples to bound the target to a single window of at most
	// traceIdxSampleStride entries, reading each sample's 16-byte trace ID directly
	// from traceIndexRaw, then linear-walks that window via traceEntryStride.
	// Sampling keeps the index traceIdxSampleStride× smaller than a dense per-entry
	// index, so it survives the parsedTraceSparseCache budget without eviction
	// (avoiding the expensive O(traceCount) rebuild walk on the next lookup).
	traceIdxOffsets   []int32
	traceIndexOffset  uint64
	traceIndexLen     uint64
	traceIdxSampleOK  bool
	isV14TraceSection bool
	traceIndexOnce    sync.Once
	traceIdxIndexOnce sync.Once
}

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
	traceIdxOffsets []int32
	// NOTE-289: traceIdxSampleIDs holds each sample's 16-byte trace ID as a big-endian
	// uint64 pair (hi at index 2k, lo at index 2k+1, parallel to traceIdxOffsets[k]).
	// In the warm steady state the offset index is a process-cache hit (no build), but
	// traceIndexRaw is freshly fetched from memcache per query and thus cold; the old
	// binary search read each probe's trace ID from data[offs[mid]:] — log2(len(offs))
	// random accesses scattered across the multi-MB section, each a likely cache miss.
	// Probing this small (≈2×len(offs)×8 byte) cache-resident slice instead removes
	// those misses from the per-query lookup. The IDs are captured for free at the
	// sample's offset during the (cold, once-per-section, cached) build walk.
	traceIdxSampleIDs []uint64
	traceIndexOffset  uint64
	traceIndexLen     uint64
	traceIdxSampleOK  bool
	isV14TraceSection bool
	traceIndexOnce    sync.Once
	traceIdxIndexOnce sync.Once
}

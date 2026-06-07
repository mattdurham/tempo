package reader

import "sync"

type compactTraceIndex struct {
	traceIndexFetchErr error
	traceIndexRaw      []byte
	blockTable         []compactBlockEntry
	traceIDBloom       []byte
	traceIndexOffset   uint64
	traceIndexLen      uint64
	isV14TraceSection  bool
	traceIndexOnce     sync.Once
}

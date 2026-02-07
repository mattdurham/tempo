package ondiskio

import (
	"bytes"
)

// traceEntry represents a unique trace in a block for trace-level deduplication.
type traceEntry struct {
	traceID        [16]byte
	resourceAttrs  map[string]*columnBuilder // resource.* columns for this trace
	resourceSchema string
}

// blockBuilder accumulates spans for a single block.
type blockBuilder struct {
	spanCount int
	columns   map[string]*columnBuilder

	minStart uint64
	maxStart uint64
	minTrace [16]byte
	maxTrace [16]byte

	columnBloom ColumnNameBloom
	// BOT: Are these ever read?
	dedicated map[string]map[string]struct{}

	// Trace-level deduplication
	traces      []*traceEntry    // List of unique traces in this block
	traceIndex  map[[16]byte]int // traceID -> index in traces array
	spanToTrace []int            // spanIdx -> trace index

	// Value statistics (v10 feature)
	valueStats map[string]AttributeStats // Nil for v9 format
}

// newBlockBuilder creates a new block builder with initialized maps.
func newBlockBuilder() *blockBuilder {
	return &blockBuilder{
		columns:     make(map[string]*columnBuilder),
		minStart:    ^uint64(0),
		maxStart:    0,
		dedicated:   make(map[string]map[string]struct{}),
		traces:      make([]*traceEntry, 0),
		traceIndex:  make(map[[16]byte]int),
		spanToTrace: make([]int, 0),
	}
}

// recordDedicated records a dedicated value for a column in this block.
func (b *blockBuilder) recordDedicated(name string, key DedicatedValueKey) {
	encoded := key.Encode()
	vals, ok := b.dedicated[name]
	if !ok {
		vals = make(map[string]struct{})
		b.dedicated[name] = vals
	}
	vals[encoded] = struct{}{}
}

// updateTraceRange updates the min/max trace ID range for this block.
func (b *blockBuilder) updateTraceRange(traceID []byte) {
	if len(traceID) != 16 {
		return
	}
	var id [16]byte
	copy(id[:], traceID)
	if b.minTrace == ([16]byte{}) || bytes.Compare(id[:], b.minTrace[:]) < 0 {
		b.minTrace = id
	}
	if bytes.Compare(id[:], b.maxTrace[:]) > 0 {
		b.maxTrace = id
	}
}

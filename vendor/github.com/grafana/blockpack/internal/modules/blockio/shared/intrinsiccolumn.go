package shared

import "sync"

// IntrinsicColumn is a blockpack data type.
//
// NOTE-340: BlockRefs is decoded lazily for paged Flat/Delta/XOR columns. The value side
// (Uint64Values/BytesValues) + Count decode eagerly; refsDecode is a closure that fills
// BlockRefs by re-walking the page ref sections, run at most once under refsOnce by
// EnsureBlockRefs(). The unfiltered no-group-by rate() path (M1/M4) reads only values + Count
// and never materializes BlockRefs (appendVariableWidthRefs was ~10.7s of querier self-time).
// The decoded column is process-cached and shared, so a later ref-needing query (rate()-by,
// histogram, predicate filter) triggers the decode once and every reader sees the memoized
// slice under the Once's happens-before. refsDecode == nil ⇒ BlockRefs already materialized
// (dict columns, legacy v1 blobs, synthesized span:end) and EnsureBlockRefs is a no-op.
// ALL readers of the BlockRefs field MUST call EnsureBlockRefs() first.
type IntrinsicColumn struct {
	refsDecode   func() []BlockRef
	Name         string
	BlockRefs    []BlockRef
	BytesValues  [][]byte
	DictEntries  []IntrinsicDictEntry
	refIndex     []RefIndexEntry
	Uint64Values []uint64
	refIndexOnce sync.Once
	refsOnce     sync.Once
	// NOTE-229: dense single-block RowIdx permutation fast path for refIndex reverse lookups.
	refDenseMin  uint32
	refDenseHi16 uint32
	Count        uint32
	Type         ColumnType
	Format       uint8
	refDense     bool
}

// EnsureBlockRefs materializes the BlockRefs array if it was deferred by a lazy decode
// (NOTE-340). It is idempotent and safe for concurrent use: the decode runs at most once
// under refsOnce, establishing a happens-before so every caller observes the same fully
// populated BlockRefs slice. Columns decoded eagerly (refsDecode == nil) return immediately.
func (col *IntrinsicColumn) EnsureBlockRefs() {
	if col.refsDecode == nil {
		return
	}
	col.refsOnce.Do(func() {
		col.BlockRefs = col.refsDecode()
		col.refsDecode = nil
	})
}

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
	// NOTE-344: refsBlobLen records the byte length of the compressed column blob retained
	// by the refsDecode closure (NOTE-340). The closure pins the whole blob alive for the
	// lifetime of the (process-cached) column so the deferred ref re-walk can run; that
	// retained blob is real heap the LRU budget must account for — otherwise a cache holding
	// many lazy-ref columns under-counts its true footprint by the blob size of each. Zero
	// once refs are materialized (the closure and its captured blob are released by
	// EnsureBlockRefs). See SizeBytes.
	refsBlobLen uint32
	// NOTE-229: dense single-block RowIdx permutation fast path for refIndex reverse lookups.
	refDenseMin  uint32
	refDenseHi16 uint32
	// NOTE-352: refDenseCount records the number of entries in the dense permutation when the
	// refIndex slice has been dropped (refDenseFlat). For a flat/XOR/Delta column whose refs
	// are emitted in row order, the dense sorted index is the identity {Packed: minRow+i,
	// Pos: i}, so refIndex[rank].Pos == rank — the slice carries NO information beyond
	// (refDenseMin, count). We therefore drop the whole refIndex slice (8 bytes/row, the
	// LARGEST per-column array — larger than the value array itself for bytes columns and
	// equal to it for uint64) and answer every reverse lookup arithmetically: pos == rank ==
	// (RowIdx - refDenseMin). This is the dominant cached-column shape (span:duration/
	// span:start single-block flat columns), so it is a large structural retained-memory cut.
	// Zero/false for dict-dense (Pos == entryIdx ≠ rank, refIndex retained) and sparse columns.
	refDenseCount uint32
	Count         uint32
	Type          ColumnType
	Format        uint8
	refDense      bool
	// refDenseFlat is true when refIndex was dropped because Pos == rank (flat-dense, NOTE-352).
	// denseLookupPos and the scatter fast path then synthesize entries from
	// (refDenseMin, refDenseCount) instead of reading the (nil) refIndex slice.
	refDenseFlat bool
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
		// NOTE-344: drop the closure so its captured blob can be GC'd, and clear refsBlobLen
		// — the materialized BlockRefs slice is now counted directly by SizeBytes.
		col.refsDecode = nil
		col.refsBlobLen = 0
	})
}

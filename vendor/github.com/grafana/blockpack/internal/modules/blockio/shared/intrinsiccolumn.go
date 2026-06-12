package shared

import "sync"

// IntrinsicColumn is a blockpack data type.
type IntrinsicColumn struct {
	Name         string
	Uint64Values []uint64
	BytesValues  [][]byte
	BlockRefs    []BlockRef
	DictEntries  []IntrinsicDictEntry
	refIndex     []RefIndexEntry
	refIndexOnce sync.Once
	// NOTE-229: when the built refIndex is a dense contiguous single-block RowIdx
	// permutation (all refs share refDenseHi16 and the low-16 RowIdx values are exactly
	// [refDenseMin, refDenseMin+len(refIndex))), reverse lookups are O(1) rank arithmetic
	// instead of a binary search. refDense gates that fast path; it is only set inside the
	// sync.Once body, so it is published together with refIndex under the Once's happens-before.
	refDenseMin  uint32
	refDenseHi16 uint32
	Count        uint32
	Type         ColumnType
	Format       uint8
	refDense     bool
}

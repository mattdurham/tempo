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
	Count        uint32
	Type         ColumnType
	Format       uint8
}

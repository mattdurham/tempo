package reader

import (
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// BOT: Should we separate the components of a block into there own folders?
// For example, we could have a "blockindex" package that defines the block index structures and serialization logic, and then the main "io" package would just use it. This would help keep the code organized as we add more components like bloom filters, column indexes, etc.
//
// columnMetadata holds column location information within a block.
// This is an internal type used during metadata parsing.
//
// Note: This is distinct from shared.ColumnIndexEntry:
//   - columnMetadata: Internal use, includes Name field, uses uint32
//   - shared.ColumnIndexEntry: Wire format, no Name field, uses uint64
//
// The Name field is needed during parsing to map columns by name,
// but is not part of the wire format's ColumnIndexEntry type.
type columnMetadata struct {
	Name   string
	Offset uint32 // Offset relative to block start
	Length uint32 // Compressed column data length
}

type blockEntryKind uint8

const (
	blockEntryKindLeaf blockEntryKind = iota
	blockEntryKindBlockpack
)

// blockIndexEntry captures per-block metadata used for pruning.
type blockIndexEntry struct {
	ValueStats      map[string]shared.AttributeStats // V10: Per-attribute value statistics for query optimization (nil for v9 compat)
	ColumnIndex     []columnMetadata                 // Per-column offsets for selective I/O
	Offset          uint64
	Length          uint64
	MinStart        uint64
	MaxStart        uint64
	SpanCount       uint32
	ColumnNameBloom ColumnNameBloom
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
	Kind            blockEntryKind
}

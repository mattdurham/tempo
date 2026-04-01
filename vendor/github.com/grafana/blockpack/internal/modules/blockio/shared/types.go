package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "sync"

// ColumnType is the logical column type (0–12); values 13–255 are reserved.
type ColumnType uint8

// ColumnType constants.
const (
	ColumnTypeString        ColumnType = 0
	ColumnTypeInt64         ColumnType = 1
	ColumnTypeUint64        ColumnType = 2
	ColumnTypeFloat64       ColumnType = 3
	ColumnTypeBool          ColumnType = 4
	ColumnTypeBytes         ColumnType = 5
	ColumnTypeRangeInt64    ColumnType = 6
	ColumnTypeRangeUint64   ColumnType = 7
	ColumnTypeRangeDuration ColumnType = 8
	ColumnTypeRangeFloat64  ColumnType = 9
	ColumnTypeRangeBytes    ColumnType = 10
	ColumnTypeRangeString   ColumnType = 11
	ColumnTypeUUID          ColumnType = 12 // string column stored as 16-byte binary UUID; StringValue() returns formatted UUID
)

// BlockKind is the kind byte in a block index entry.
type BlockKind uint8

// BlockKindLeaf is the only defined block kind; leaf blocks contain span data.
const BlockKindLeaf BlockKind = 0

// BlockMeta holds the parsed block index entry.
type BlockMeta struct {
	Offset     uint64
	Length     uint64
	MinStart   uint64
	MaxStart   uint64
	SpanCount  uint32
	MinTraceID [16]byte
	MaxTraceID [16]byte
	Kind       BlockKind
}

// AttrValue is a typed attribute value.
type AttrValue struct {
	Str   string
	Bytes []byte
	Int   int64
	Uint  uint64
	Float float64
	Bool  bool
	Type  ColumnType
}

// AttrKV is a key-value pair for span, resource, or scope attributes.
// Using a slice of AttrKV instead of map[string]AttrValue eliminates per-span map
// allocations (map headers + hash buckets), reducing GC scan work significantly.
type AttrKV struct {
	Key string
	Val AttrValue
}

// ColumnKey is a composite key for a column: the combination of name and type
// uniquely identifies a column within type-aware column maps (for example,
// within a block's column metadata). OTLP allows the same attribute key to
// appear with different types across spans (e.g. "foo" as string on one span
// and int64 on another); using ColumnKey in these maps prevents silent data loss.
// Note: some indices (e.g. bloom filter, range index) remain name-only intentionally
// — they are used for block-level pruning where false positives are acceptable.
type ColumnKey struct {
	Name string
	Type ColumnType
}

// RangeValueKey is a string key used in the range index.
type RangeValueKey = string

// BlockRef is a (blockIdx, rowIdx) pair identifying one row's location in a blockpack file.
// Used in IntrinsicColumn to map sorted column values back to their source rows.
type BlockRef struct {
	BlockIdx uint16
	RowIdx   uint16
}

// IntrinsicColumn is the decoded result of reading one intrinsic column blob.
// Returned by GetIntrinsicColumn on a Reader.
type IntrinsicColumn struct {
	Name string
	// For flat columns (IntrinsicFormatFlat):
	Uint64Values []uint64   // non-nil for ColumnTypeUint64
	BytesValues  [][]byte   // non-nil for ColumnTypeBytes
	BlockRefs    []BlockRef // parallel to Uint64Values / BytesValues; sorted by VALUE
	// For dict columns (IntrinsicFormatDict):
	DictEntries []IntrinsicDictEntry

	// refIndex is a sorted-by-packed-ref lookup table for O(log N) reverse lookup.
	// Built lazily by EnsureRefIndex on first reverse-lookup call.
	// For flat: Pos indexes into BlockRefs/Uint64Values/BytesValues.
	// For dict: Pos is the DictEntries index.
	// Thread safety: built under refIndexOnce; safe for concurrent reads after that.
	refIndex     []RefIndexEntry
	refIndexOnce sync.Once
	Count        uint32
	Type         ColumnType
	Format       uint8
}

// SizeBytes returns an estimate of the in-memory size of this column for LRU cache budgeting.
func (col *IntrinsicColumn) SizeBytes() int64 {
	n := int64(len(col.Uint64Values)) * 8
	for _, b := range col.BytesValues {
		n += int64(len(b))
	}
	n += int64(len(col.BlockRefs)) * 4
	for _, e := range col.DictEntries {
		n += int64(len(e.Value)) + int64(len(e.BlockRefs))*4 + 8
	}
	n += int64(len(col.refIndex)) * 8
	return n
}

// RefIndexEntry is one entry in IntrinsicColumn.refIndex.
// Enables O(log N) reverse lookup from packed ref to column value.
type RefIndexEntry struct {
	Packed uint32 // blockIdx<<16 | rowIdx
	Pos    int32  // index into BlockRefs/values (flat) or DictEntries (dict)
}

// IntrinsicDictEntry is one entry in a decoded dictionary intrinsic column.
type IntrinsicDictEntry struct {
	Value     string // string representation (for Int64: decimal string)
	BlockRefs []BlockRef
	Int64Val  int64 // set for Int64 type
}

// PageMeta describes one page in a paged (v2) intrinsic column.
// All page blobs are stored contiguously after the page TOC blob in the on-disk region.
type PageMeta struct {
	// Pointer fields first for better GC scan alignment.
	Min   string // encoded min value (same encoding as IntrinsicColMeta)
	Max   string // encoded max value
	Bloom []byte // bloom filter bytes; nil for flat columns
	// Scalar fields.
	Offset   uint32 // byte offset of this page blob relative to first page blob start
	Length   uint32 // compressed page blob size in bytes
	RowCount uint32 // number of records in this page
}

// PagedIntrinsicTOC holds the page index (table of contents) for one v2 paged column.
// It is decoded from the snappy-compressed TOC blob that precedes the page blobs.
type PagedIntrinsicTOC struct {
	Pages         []PageMeta
	BlockIdxWidth uint8
	RowIdxWidth   uint8
	Format        uint8 // IntrinsicFormatFlat or IntrinsicFormatDict
	ColType       ColumnType
}

// IntrinsicColMeta is one entry in the intrinsic column TOC (table of contents).
// Each entry records the location and summary statistics for one intrinsic column blob.
type IntrinsicColMeta struct {
	// Pointer fields first for better GC scan alignment.
	Name string // column name (e.g. "span:duration", "span:name")
	Min  string // encoded lower boundary value (8-byte LE for numeric, raw string for string/bytes)
	Max  string // encoded upper boundary value
	// Scalar fields.
	Offset uint64     // absolute file offset of the column data blob
	Length uint32     // byte length of the column data blob (snappy-compressed)
	Count  uint32     // total number of rows stored (present rows only)
	Type   ColumnType // ColumnTypeUint64, ColumnTypeBytes, ColumnTypeString, ColumnTypeInt64
	Format uint8      // IntrinsicFormatFlat or IntrinsicFormatDict
}

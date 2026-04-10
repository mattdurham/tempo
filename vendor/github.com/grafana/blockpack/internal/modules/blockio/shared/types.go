package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// DirEntryType describes a type-keyed entry in the V14 section directory.
// Used for the 6 fixed file-level sections (block index, range index, etc.).
// Wire format: entry_kind[1]=0x00 + section_type[1] + offset[8] + compressed_len[4] = 14 bytes.
// Fields are ordered for minimal struct padding (string header first, then scalar fields).
type DirEntryType struct {
	Offset        uint64
	CompressedLen uint32
	SectionType   uint8
}

// DirEntryTypeWireSize is the fixed on-wire size of one type-keyed directory entry.
// entry_kind[1]+section_type[1]+offset[8]+compressed_len[4] = 14 bytes.
const DirEntryTypeWireSize = 14

// Marshal serializes the entry to its 14-byte wire format.
func (e DirEntryType) Marshal() []byte {
	buf := make([]byte, DirEntryTypeWireSize)
	buf[0] = DirEntryKindType
	buf[1] = e.SectionType
	binary.LittleEndian.PutUint64(buf[2:10], e.Offset)
	binary.LittleEndian.PutUint32(buf[10:14], e.CompressedLen)
	return buf
}

// MarshalInto serializes the entry into dst (must be at least 14 bytes).
func (e DirEntryType) MarshalInto(dst []byte) {
	dst[0] = DirEntryKindType
	dst[1] = e.SectionType
	binary.LittleEndian.PutUint64(dst[2:10], e.Offset)
	binary.LittleEndian.PutUint32(dst[10:14], e.CompressedLen)
}

// UnmarshalDirEntryType parses a DirEntryType from a buffer starting at position after the kind byte.
// data must start at section_type[1] (i.e., the kind byte was already consumed by the caller).
func UnmarshalDirEntryType(data []byte) (DirEntryType, error) {
	const want = DirEntryTypeWireSize - 1 // minus the kind byte already consumed
	if len(data) < want {
		return DirEntryType{}, fmt.Errorf("UnmarshalDirEntryType: need %d bytes, got %d", want, len(data))
	}
	return DirEntryType{
		SectionType:   data[0],
		Offset:        binary.LittleEndian.Uint64(data[1:9]),
		CompressedLen: binary.LittleEndian.Uint32(data[9:13]),
	}, nil
}

// DirEntryName describes a name-keyed entry in the V14 section directory.
// Used for file-level intrinsic column blobs (one entry per column).
// Wire format: entry_kind[1]=0x01 + name_len[2] + name + offset[8] + compressed_len[4] = 15+len(name) bytes.
// Fields are ordered for minimal struct padding (string header first, then scalar fields).
type DirEntryName struct {
	Name          string
	Offset        uint64
	CompressedLen uint32
}

// WireSize returns the variable on-wire size of this name-keyed directory entry.
// entry_kind[1]+name_len[2]+name+offset[8]+compressed_len[4] = 15+len(name) bytes.
func (e DirEntryName) WireSize() int {
	return 15 + len(e.Name)
}

// Marshal serializes the entry to its wire format.
func (e DirEntryName) Marshal() []byte {
	buf := make([]byte, e.WireSize())
	e.MarshalInto(buf)
	return buf
}

// MarshalInto serializes the entry into dst (must be at least WireSize() bytes).
func (e DirEntryName) MarshalInto(dst []byte) {
	dst[0] = DirEntryKindName
	binary.LittleEndian.PutUint16(dst[1:3], uint16(len(e.Name))) //nolint:gosec // safe: name length bounded by MaxNameLen (1024), fits in uint16
	copy(dst[3:3+len(e.Name)], e.Name)
	off := 3 + len(e.Name)
	binary.LittleEndian.PutUint64(dst[off:off+8], e.Offset)
	binary.LittleEndian.PutUint32(dst[off+8:off+12], e.CompressedLen)
}

// UnmarshalDirEntryName parses a DirEntryName from a buffer starting after the kind byte.
// data must start at name_len[2] (i.e., the kind byte was already consumed by the caller).
// Returns the entry and the number of bytes consumed from data.
func UnmarshalDirEntryName(data []byte) (DirEntryName, int, error) {
	if len(data) < 2 {
		return DirEntryName{}, 0, fmt.Errorf("UnmarshalDirEntryName: need at least 2 bytes for name_len, got %d", len(data))
	}
	nameLen := int(binary.LittleEndian.Uint16(data[0:2]))
	need := 2 + nameLen + 8 + 4
	if len(data) < need {
		return DirEntryName{}, 0, fmt.Errorf("UnmarshalDirEntryName: need %d bytes, got %d", need, len(data))
	}
	name := string(data[2 : 2+nameLen])
	off := 2 + nameLen
	return DirEntryName{
		Name:          name,
		Offset:        binary.LittleEndian.Uint64(data[off : off+8]),
		CompressedLen: binary.LittleEndian.Uint32(data[off+8 : off+12]),
	}, need, nil
}

// SectionDirectory holds the decoded V14 section directory.
// TypeEntries maps section_type (0x01–0x06) to its type-keyed entry.
// NameEntries maps intrinsic column name to its name-keyed entry.
// SignalType holds the file's signal type (SignalTypeTrace=0x01, SignalTypeLog=0x02).
// Defaults to 0 (unknown) if no DirEntryKindSignal entry is present.
type SectionDirectory struct {
	TypeEntries map[uint8]DirEntryType
	NameEntries map[string]DirEntryName
	SignalType  uint8
}

// ColumnType is the logical column type (0–13); values 14–255 are reserved.
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
	ColumnTypeVectorF32     ColumnType = 13 // flat float32 array; dim stored in column encoding header
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

package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
)

// DirEntryType describes a type-keyed entry in the V14 section directory.
// Used for the 6 fixed file-level sections (block index, range index, etc.).
// Wire format: entry_kind[1]=0x00 + section_type[1] + offset[8] + compressed_len[4] = 14 bytes.
// Fields are ordered for minimal struct padding (string header first, then scalar fields).

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

// DirEntryName wire field widths (after the kind byte is consumed).
const (
	dirEntryNameLenSize       = 2 // name_len field: uint16 LE
	dirEntryOffsetSize        = 8 // offset field: uint64 LE
	dirEntryCompressedLenSize = 4 // compressed_len field: uint32 LE
)

// DirEntryName describes a name-keyed entry in the V14 section directory.
// Used for file-level intrinsic column blobs (one entry per column).
// Wire format: entry_kind[1]=0x01 + name_len[2] + name + offset[8] + compressed_len[4] = 15+len(name) bytes.
// Fields are ordered for minimal struct padding (string header first, then scalar fields).

// WireSize returns the variable on-wire size of this name-keyed directory entry.
// entry_kind[1]+name_len[2]+name+offset[8]+compressed_len[4] = 15+len(name) bytes.
func (e DirEntryName) WireSize() int {
	return 1 + dirEntryNameLenSize + len(e.Name) + dirEntryOffsetSize + dirEntryCompressedLenSize
}

// Marshal serializes the entry to its wire format.
func (e DirEntryName) Marshal() []byte {
	buf := make([]byte, e.WireSize())
	e.MarshalInto(buf)
	return buf
}

// MarshalInto serializes the entry into dst (must be at least WireSize() bytes).
func (e DirEntryName) MarshalInto(dst []byte) {
	const kindSize = 1
	dst[0] = DirEntryKindName
	binary.LittleEndian.PutUint16(
		dst[kindSize:kindSize+dirEntryNameLenSize],
		uint16(len(e.Name)), //nolint:gosec // safe: name length bounded by MaxNameLen (1024), fits in uint16
	)
	copy(dst[kindSize+dirEntryNameLenSize:kindSize+dirEntryNameLenSize+len(e.Name)], e.Name)
	off := kindSize + dirEntryNameLenSize + len(e.Name)
	binary.LittleEndian.PutUint64(dst[off:off+dirEntryOffsetSize], e.Offset)
	binary.LittleEndian.PutUint32(
		dst[off+dirEntryOffsetSize:off+dirEntryOffsetSize+dirEntryCompressedLenSize],
		e.CompressedLen,
	)
}

// UnmarshalDirEntryName parses a DirEntryName from a buffer starting after the kind byte.
// data must start at name_len[2] (i.e., the kind byte was already consumed by the caller).
// Returns the entry and the number of bytes consumed from data.
func UnmarshalDirEntryName(data []byte) (DirEntryName, int, error) {
	if len(data) < dirEntryNameLenSize {
		return DirEntryName{}, 0, fmt.Errorf(
			"UnmarshalDirEntryName: need at least %d bytes for name_len, got %d",
			dirEntryNameLenSize, len(data),
		)
	}
	nameLen := int(binary.LittleEndian.Uint16(data[0:dirEntryNameLenSize]))
	need := dirEntryNameLenSize + nameLen + dirEntryOffsetSize + dirEntryCompressedLenSize
	if len(data) < need {
		return DirEntryName{}, 0, fmt.Errorf("UnmarshalDirEntryName: need %d bytes, got %d", need, len(data))
	}
	name := string(data[dirEntryNameLenSize : dirEntryNameLenSize+nameLen])
	off := dirEntryNameLenSize + nameLen
	return DirEntryName{
		Name:   name,
		Offset: binary.LittleEndian.Uint64(data[off : off+dirEntryOffsetSize]),
		CompressedLen: binary.LittleEndian.Uint32(
			data[off+dirEntryOffsetSize : off+dirEntryOffsetSize+dirEntryCompressedLenSize],
		),
	}, need, nil
}

// SectionDirectory holds the decoded V14 section directory.
// TypeEntries maps section_type (0x01–0x06) to its type-keyed entry.
// NameEntries maps intrinsic column name to its name-keyed entry.
// SignalType holds the file's signal type (SignalTypeTrace=0x01).
// Defaults to 0 (unknown) if no DirEntryKindSignal entry is present.

// ToCKey is the lookup key in a V8 unified Table of Contents.
// Struct equality is used for map lookups — no custom hash needed.
// Fields are ordered for minimal struct padding (string header first, then scalar fields).

// ToCEntry is one record in the V8 unified ToC blob.
// Wire format: type[4 LE] + subtype[4 LE] + name_len[2 LE] + name[name_len] + offset[8 LE] + length[4 LE]
// Minimum wire size (name=""): 4+4+2+0+8+4 = 22 bytes.

// ToCEntryMinWireSize is the wire size of a ToCEntry with Name="".
// type[4]+subtype[4]+name_len[2]+offset[8]+length[4] = 22 bytes.
const ToCEntryMinWireSize = 22

// ToCBlobHeaderSize is the fixed prefix of the decompressed ToC blob before entries.
// entry_count[4 LE] + signal_type[1] + reserved[3] = 8 bytes.
const ToCBlobHeaderSize = 8

// WireSize returns the on-wire byte count for this ToCEntry.
func (e ToCEntry) WireSize() int {
	return ToCEntryMinWireSize + len(e.Key.Name)
}

// Marshal serializes e to a newly allocated byte slice of length e.WireSize().
func (e ToCEntry) Marshal() []byte {
	buf := make([]byte, e.WireSize())
	e.MarshalInto(buf)
	return buf
}

// MarshalInto serializes e into dst.
// dst must be at least e.WireSize() bytes long.
func (e ToCEntry) MarshalInto(dst []byte) {
	binary.LittleEndian.PutUint32(dst[0:], e.Key.Type)
	binary.LittleEndian.PutUint32(dst[4:], e.Key.SubType)
	binary.LittleEndian.PutUint16(
		dst[8:],
		uint16(len(e.Key.Name)), //nolint:gosec // bounded by MaxNameLen (1024), fits in uint16
	)
	copy(dst[10:], e.Key.Name)
	off := 10 + len(e.Key.Name)
	binary.LittleEndian.PutUint64(dst[off:], e.Offset)
	binary.LittleEndian.PutUint32(dst[off+8:], e.Length)
}

// UnmarshalToCEntry parses one ToCEntry from data[0:].
// Returns the entry and the number of bytes consumed from data.
func UnmarshalToCEntry(data []byte) (ToCEntry, int, error) {
	const minHeader = 10 // type[4]+subtype[4]+name_len[2]
	if len(data) < minHeader {
		return ToCEntry{}, 0, fmt.Errorf("UnmarshalToCEntry: need at least %d bytes, got %d", minHeader, len(data))
	}
	tocType := binary.LittleEndian.Uint32(data[0:])
	subType := binary.LittleEndian.Uint32(data[4:])
	nameLen := int(binary.LittleEndian.Uint16(data[8:]))
	if nameLen > MaxNameLen {
		return ToCEntry{}, 0, fmt.Errorf("UnmarshalToCEntry: name_len %d exceeds MaxNameLen %d", nameLen, MaxNameLen)
	}
	need := minHeader + nameLen + 8 + 4 // name + offset[8] + length[4]
	if len(data) < need {
		return ToCEntry{}, 0, fmt.Errorf("UnmarshalToCEntry: need %d bytes, got %d", need, len(data))
	}
	name := string(data[minHeader : minHeader+nameLen])
	off := minHeader + nameLen
	return ToCEntry{
		Key:    ToCKey{Type: tocType, SubType: subType, Name: name},
		Offset: binary.LittleEndian.Uint64(data[off:]),
		Length: binary.LittleEndian.Uint32(data[off+8:]),
	}, need, nil
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

// AttrValue is a typed attribute value.

// AttrKV is a key-value pair for span, resource, or scope attributes.
// Using a slice of AttrKV instead of map[string]AttrValue eliminates per-span map
// allocations (map headers + hash buckets), reducing GC scan work significantly.

// ColumnKey is a composite key for a column: the combination of name and type
// uniquely identifies a column within type-aware column maps (for example,
// within a block's column metadata). OTLP allows the same attribute key to
// appear with different types across spans (e.g. "foo" as string on one span
// and int64 on another); using ColumnKey in these maps prevents silent data loss.
// Note: some indices (e.g. bloom filter, range index) remain name-only intentionally
// — they are used for block-level pruning where false positives are acceptable.

// RangeValueKey is a string key used in the range index.
type RangeValueKey = string

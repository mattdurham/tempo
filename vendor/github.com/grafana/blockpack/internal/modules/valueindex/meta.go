package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Meta holds the decoded content of a ToCSubTypeValueIndexMeta (VIMT) section.
// It is the authoritative source of column identity and compaction level for a file.
//
// Wire layout (all little-endian):
//
//	magic[4]           = 0x56494D54 ("VIMT")
//	version[1]         = 0x01
//	col_type[1]        = shared.ColumnType
//	col_hash[16]       = SHA-256(col_name)[:16]
//	col_name_len[2 LE]
//	col_name[N]
//	wall_min_ts[8 LE]
//	wall_max_ts[8 LE]
//	compaction_level[1]
//	reserved[7]        = zero
type Meta struct {
	ColName         string
	WallMinTS       uint64
	WallMaxTS       uint64
	ColHash         [16]byte
	ColType         shared.ColumnType
	CompactionLevel uint8
}

// metaFixedSize is the size of the fixed portion before col_name:
// magic[4]+version[1]+col_type[1]+col_hash[16]+col_name_len[2] = 24 bytes.
// Then wall_min_ts[8]+wall_max_ts[8]+compaction_level[1]+reserved[7] = 24 bytes after col_name.
const (
	metaHeaderSize  = 24 // bytes before col_name
	metaTrailerSize = 24 // bytes after col_name
)

// EncodeMeta serializes m into the VIMT wire format.
func EncodeMeta(m Meta) []byte {
	nameBytes := []byte(m.ColName)
	size := metaHeaderSize + len(nameBytes) + metaTrailerSize
	b := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint32(b[pos:], shared.ValueIndexMetaMagic)
	pos += 4
	b[pos] = shared.ValueIndexMetaVersion
	pos++
	b[pos] = byte(m.ColType)
	pos++
	copy(b[pos:], m.ColHash[:])
	pos += 16
	binary.LittleEndian.PutUint16(b[pos:], uint16(len(nameBytes))) //nolint:gosec // name bounded by MaxNameLen
	pos += 2
	copy(b[pos:], nameBytes)
	pos += len(nameBytes)
	binary.LittleEndian.PutUint64(b[pos:], m.WallMinTS)
	pos += 8
	binary.LittleEndian.PutUint64(b[pos:], m.WallMaxTS)
	pos += 8
	b[pos] = m.CompactionLevel
	// reserved[7] already zero from make
	return b
}

// DecodeMeta parses a VIMT blob. Returns an error on magic or version mismatch,
// or if the blob is too short.
func DecodeMeta(b []byte) (Meta, error) {
	if len(b) < metaHeaderSize {
		return Meta{}, fmt.Errorf("valueindex: VIMT blob too short (%d bytes)", len(b))
	}
	if magic := binary.LittleEndian.Uint32(b[:4]); magic != shared.ValueIndexMetaMagic {
		return Meta{}, fmt.Errorf("valueindex: VIMT wrong magic 0x%08X", magic)
	}
	if ver := b[4]; ver != shared.ValueIndexMetaVersion {
		return Meta{}, fmt.Errorf("valueindex: VIMT unsupported version %d", ver)
	}

	var m Meta
	m.ColType = shared.ColumnType(b[5])
	copy(m.ColHash[:], b[6:22])
	nameLen := int(binary.LittleEndian.Uint16(b[22:24]))
	pos := 24

	if len(b) < pos+nameLen+metaTrailerSize {
		return Meta{}, fmt.Errorf(
			"valueindex: VIMT blob truncated (need %d bytes, have %d)",
			pos+nameLen+metaTrailerSize,
			len(b),
		)
	}
	m.ColName = string(b[pos : pos+nameLen])
	pos += nameLen
	m.WallMinTS = binary.LittleEndian.Uint64(b[pos:])
	pos += 8
	m.WallMaxTS = binary.LittleEndian.Uint64(b[pos:])
	pos += 8
	m.CompactionLevel = b[pos]
	return m, nil
}

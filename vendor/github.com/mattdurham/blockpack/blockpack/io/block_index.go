package ondiskio

import (
	"bytes"
	"encoding/binary"
)

// ColumnIndexEntry stores offset and length for a single column within a block.
// Offsets are relative to the block start (blockIndexEntry.Offset).
type ColumnIndexEntry struct {
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
	Offset          uint64
	Length          uint64
	Kind            blockEntryKind
	SpanCount       uint32
	MinStart        uint64
	MaxStart        uint64
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
	ColumnNameBloom ColumnNameBloom
	ColumnIndex     []ColumnIndexEntry        // Per-column offsets for selective I/O
	ValueStats      map[string]AttributeStats // V10: Per-attribute value statistics for query optimization (nil for v9 compat)
}

func blockIndexSerializedSize() int {
	// offset + length + span count + min/max start + min/max trace + bloom
	return blockIndexSerializedSizeForVersion(versionV11)
}

func blockIndexSerializedSizeForVersion(version uint8) int {
	base := 8 + 8 + 4 + 8 + 8 + 16 + 16 + (columnNameBloomBits / 8)
	if version >= versionV11 {
		base += 1
	}
	return base
}

func (e *blockIndexEntry) write(buf *bytes.Buffer, version uint8) error {
	_ = binary.Write(buf, binary.LittleEndian, e.Offset)
	_ = binary.Write(buf, binary.LittleEndian, e.Length)
	if version >= versionV11 {
		_ = buf.WriteByte(byte(e.Kind))
	}
	_ = binary.Write(buf, binary.LittleEndian, e.SpanCount)
	_ = binary.Write(buf, binary.LittleEndian, e.MinStart)
	_ = binary.Write(buf, binary.LittleEndian, e.MaxStart)
	_, _ = buf.Write(e.MinTraceID[:])
	_, _ = buf.Write(e.MaxTraceID[:])
	_, _ = buf.Write(e.ColumnNameBloom[:])

	// Write value statistics for v10 format
	if version >= versionV10 {
		if err := writeValueStats(buf, e.ValueStats); err != nil {
			return err
		}
	}

	return nil
}

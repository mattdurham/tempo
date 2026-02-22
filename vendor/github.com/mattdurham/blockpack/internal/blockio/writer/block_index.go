package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// internalColumnIndexEntry stores offset and length for a single column within a block.
// Offsets are relative to the block start (blockIndexEntry.Offset).
// This is distinct from shared.ColumnIndexEntry which lacks the Name field.
type internalColumnIndexEntry struct {
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
	ValueStats      map[string]AttributeStats  // V10: Per-attribute value statistics for query optimization (nil for v9 compat)
	ColumnIndex     []internalColumnIndexEntry // Per-column offsets for selective I/O
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

func (e *blockIndexEntry) write(buf *bytes.Buffer, version uint8) error {
	if err := binary.Write(buf, binary.LittleEndian, e.Offset); err != nil {
		return fmt.Errorf("write block offset: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, e.Length); err != nil {
		return fmt.Errorf("write block length: %w", err)
	}
	if version >= versionV11 {
		if err := buf.WriteByte(byte(e.Kind)); err != nil {
			return fmt.Errorf("write block kind: %w", err)
		}
	}
	if err := binary.Write(buf, binary.LittleEndian, e.SpanCount); err != nil {
		return fmt.Errorf("write span count: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, e.MinStart); err != nil {
		return fmt.Errorf("write min start: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, e.MaxStart); err != nil {
		return fmt.Errorf("write max start: %w", err)
	}
	if _, err := buf.Write(e.MinTraceID[:]); err != nil {
		return fmt.Errorf("write min trace ID: %w", err)
	}
	if _, err := buf.Write(e.MaxTraceID[:]); err != nil {
		return fmt.Errorf("write max trace ID: %w", err)
	}
	if _, err := buf.Write(e.ColumnNameBloom[:]); err != nil {
		return fmt.Errorf("write column name bloom: %w", err)
	}

	// Write value statistics (introduced in v10, omitted for v9 to preserve compatibility).
	if version >= versionV10 {
		if err := writeValueStats(buf, e.ValueStats); err != nil {
			return fmt.Errorf("write value stats: %w", err)
		}
	}

	return nil
}

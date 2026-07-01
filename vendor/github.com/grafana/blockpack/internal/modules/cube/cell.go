package cube

// NOTE: SPEC-CUBE-001 — Cell is a 12-byte sparse record: (minute, dim1_id, dim2_id) → count.
// Sort order: minute ASC, dim1_id ASC, dim2_id ASC. All fields LittleEndian.

import (
	"cmp"
	"encoding/binary"
	"fmt"
)

// Cell is one pre-aggregated counter: (minute, dim1, dim2) → count.
// Cells are sorted by (Minute ASC, Dim1ID ASC, Dim2ID ASC).
type Cell struct {
	Minute uint32 // unix timestamp / 60
	Dim1ID uint16 // dictionary ID for dimension 1 (e.g., service.name)
	Dim2ID uint16 // dictionary ID for dimension 2 (e.g., http.status_code)
	Count  uint32 // exact span count
}

// EncodeCell serializes a Cell to 12 bytes (LittleEndian).
func EncodeCell(c Cell) []byte {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], c.Minute)
	binary.LittleEndian.PutUint16(buf[4:6], c.Dim1ID)
	binary.LittleEndian.PutUint16(buf[6:8], c.Dim2ID)
	binary.LittleEndian.PutUint32(buf[8:12], c.Count)
	return buf
}

// DecodeCell parses a 12-byte buffer into a Cell.
func DecodeCell(buf []byte) (Cell, error) {
	if len(buf) < 12 {
		return Cell{}, fmt.Errorf("cube: cell buffer too short (%d bytes)", len(buf))
	}
	return Cell{
		Minute: binary.LittleEndian.Uint32(buf[0:4]),
		Dim1ID: binary.LittleEndian.Uint16(buf[4:6]),
		Dim2ID: binary.LittleEndian.Uint16(buf[6:8]),
		Count:  binary.LittleEndian.Uint32(buf[8:12]),
	}, nil
}

// CompareCell returns -1 if a < b, 0 if a == b, +1 if a > b.
// Sort order: minute ASC, dim1_id ASC, dim2_id ASC.
func CompareCell(a, b Cell) int {
	if a.Minute != b.Minute {
		return cmp.Compare(a.Minute, b.Minute)
	}
	if a.Dim1ID != b.Dim1ID {
		return cmp.Compare(a.Dim1ID, b.Dim1ID)
	}
	return cmp.Compare(a.Dim2ID, b.Dim2ID)
}

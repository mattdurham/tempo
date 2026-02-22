package blockio

// ColumnIndexEntry stores offset and length for a single column within a block.
// Offsets are relative to the block start.
type ColumnIndexEntry struct {
	Name   string
	Offset uint32 // Offset relative to block start
	Length uint32 // Compressed column data length
}

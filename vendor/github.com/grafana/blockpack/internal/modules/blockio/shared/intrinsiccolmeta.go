package shared

// IntrinsicColMeta is a blockpack data type.
type IntrinsicColMeta struct {
	Name   string
	Min    string
	Max    string
	Offset uint64
	Length uint32
	Count  uint32
	Type   ColumnType
	Format uint8
}

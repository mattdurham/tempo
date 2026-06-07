package shared

// PageMeta is a blockpack data type.
type PageMeta struct {
	Min      string
	Max      string
	Bloom    []byte
	Offset   uint32
	Length   uint32
	RowCount uint32
}

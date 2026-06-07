package shared

// AttrValue is a blockpack data type.
type AttrValue struct {
	Str   string
	Bytes []byte
	Int   int64
	Uint  uint64
	Float float64
	Bool  bool
	Type  ColumnType
}

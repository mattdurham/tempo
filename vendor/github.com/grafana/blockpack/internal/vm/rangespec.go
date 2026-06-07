package vm

// RangeSpec is a blockpack data type.
type RangeSpec struct {
	MinValue     any
	MaxValue     any
	MinInclusive bool
	MaxInclusive bool
}

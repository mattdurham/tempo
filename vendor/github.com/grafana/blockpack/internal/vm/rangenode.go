package vm

// RangeNode is a blockpack data type.
type RangeNode struct {
	Min             *Value
	Max             *Value
	Column          string
	Pattern         string
	Values          []Value
	Children        []RangeNode
	QueryVector     []float32
	VectorThreshold float32
	MinInclusive    bool
	MaxInclusive    bool
	IsOR            bool
}

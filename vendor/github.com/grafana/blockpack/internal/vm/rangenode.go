package vm

// RangeNode is a blockpack data type.
type RangeNode struct {
	Min          *Value
	Max          *Value
	Column       string
	Pattern      string
	Values       []Value
	Children     []RangeNode
	MinInclusive bool
	MaxInclusive bool
	IsOR         bool
	// RequirePresent marks a leaf that can match only rows where Column is present
	// (non-null). Set for "!= \"\"" predicates, which cannot be expressed as a value
	// range but still require the attribute to exist. Used by the executor's ColStats
	// block pruning (NOTE-446, issue #364) to skip blocks where present_count == 0.
	// A RequirePresent leaf carries no Values/Min/Max — it constrains existence only.
	RequirePresent bool
}

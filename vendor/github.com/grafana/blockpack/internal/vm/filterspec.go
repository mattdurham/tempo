package vm

// FilterSpec is a blockpack data type.
type FilterSpec struct {
	AttributeEquals map[string][]any
	AttributeRanges map[string]*RangeSpec
	IsMatchAll      bool
}

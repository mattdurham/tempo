package vm

// QueryPredicates is a blockpack data type.
type QueryPredicates struct {
	Nodes   []RangeNode
	Columns []string
}

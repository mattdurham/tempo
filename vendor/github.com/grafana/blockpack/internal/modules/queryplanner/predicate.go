package queryplanner

// Predicate is a blockpack data type.
type Predicate struct {
	Columns  []string
	Children []Predicate
	Op       LogicalOp
}

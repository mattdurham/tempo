package queryplanner

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

// Predicate is a blockpack data type.
type Predicate struct {
	Columns       []string
	Values        []string
	Children      []Predicate
	ColType       shared.ColumnType
	IntervalMatch bool
	Op            LogicalOp
}

package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import "slices"

// rowSet is a sorted, deduplicated set of row indices implementing vm.RowSet.
// Rows are maintained in ascending order; Add must be called with ascending indices
// (as in a sequential 0..n-1 scan) for binary search to work in Contains.
type rowSet struct {
	rows []int
}

func newRowSet() *rowSet { return &rowSet{} }

// Add appends rowIdx. Callers must add indices in ascending order.
func (r *rowSet) Add(rowIdx int) {
	r.rows = append(r.rows, rowIdx)
}

// Contains reports whether rowIdx is in the set (binary search, requires sorted).
func (r *rowSet) Contains(rowIdx int) bool {
	_, found := slices.BinarySearch(r.rows, rowIdx)
	return found
}

// Size returns the number of rows.
func (r *rowSet) Size() int { return len(r.rows) }

// IsEmpty reports whether the set has no rows.
func (r *rowSet) IsEmpty() bool { return len(r.rows) == 0 }

// ToSlice returns the backing slice (sorted ascending). Caller must not modify it.
func (r *rowSet) ToSlice() []int { return r.rows }

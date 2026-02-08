package executor

import "sort"

// RowSet represents a set of row indices
// This is the foundation for set operations (AND/OR/NOT) on scan results
type RowSet struct {
	indices map[int]struct{} // Use empty struct for memory efficiency
}

// NewRowSet creates a new empty row set
func NewRowSet() *RowSet {
	return &RowSet{
		indices: make(map[int]struct{}),
	}
}

// NewRowSetFromSlice creates a row set from a slice of row indices
func NewRowSetFromSlice(rows []int) *RowSet {
	rs := NewRowSet()
	for _, row := range rows {
		rs.Add(row)
	}
	return rs
}

// Add adds a row index to the set
func (rs *RowSet) Add(rowIdx int) {
	rs.indices[rowIdx] = struct{}{}
}

// Contains checks if a row index is in the set
func (rs *RowSet) Contains(rowIdx int) bool {
	_, ok := rs.indices[rowIdx]
	return ok
}

// Remove removes a row index from the set
func (rs *RowSet) Remove(rowIdx int) {
	delete(rs.indices, rowIdx)
}

// Size returns the number of rows in the set
func (rs *RowSet) Size() int {
	return len(rs.indices)
}

// IsEmpty returns true if the set is empty
func (rs *RowSet) IsEmpty() bool {
	return len(rs.indices) == 0
}

// ToSlice returns a sorted slice of row indices
func (rs *RowSet) ToSlice() []int {
	result := make([]int, 0, len(rs.indices))
	for idx := range rs.indices {
		result = append(result, idx)
	}

	// Sort for deterministic results using standard library (O(n log n))
	sort.Ints(result)

	return result
}

// Union returns a new RowSet containing all rows from both sets
func (rs *RowSet) Union(other *RowSet) *RowSet {
	result := NewRowSet()

	// Add all rows from first set
	for idx := range rs.indices {
		result.Add(idx)
	}

	// Add all rows from second set
	for idx := range other.indices {
		result.Add(idx)
	}

	return result
}

// Intersect returns a new RowSet containing only rows in both sets
func (rs *RowSet) Intersect(other *RowSet) *RowSet {
	result := NewRowSet()

	// Iterate over smaller set for efficiency
	smaller, larger := rs, other
	if len(other.indices) < len(rs.indices) {
		smaller, larger = other, rs
	}

	for idx := range smaller.indices {
		if larger.Contains(idx) {
			result.Add(idx)
		}
	}

	return result
}

// Difference returns a new RowSet containing rows in this set but not in other
func (rs *RowSet) Difference(other *RowSet) *RowSet {
	result := NewRowSet()

	for idx := range rs.indices {
		if !other.Contains(idx) {
			result.Add(idx)
		}
	}

	return result
}

// Complement returns a new RowSet containing all rows NOT in this set
// up to blockSize
func (rs *RowSet) Complement(blockSize int) *RowSet {
	result := NewRowSet()

	for i := 0; i < blockSize; i++ {
		if !rs.Contains(i) {
			result.Add(i)
		}
	}

	return result
}

// Clone creates a deep copy of the row set
func (rs *RowSet) Clone() *RowSet {
	result := NewRowSet()
	for idx := range rs.indices {
		result.Add(idx)
	}
	return result
}

// Clear removes all rows from the set
func (rs *RowSet) Clear() {
	rs.indices = make(map[int]struct{})
}

// UnionMany returns the union of multiple row sets
func UnionMany(sets ...*RowSet) *RowSet {
	if len(sets) == 0 {
		return NewRowSet()
	}

	result := sets[0].Clone()
	for i := 1; i < len(sets); i++ {
		for idx := range sets[i].indices {
			result.Add(idx)
		}
	}

	return result
}

// IntersectMany returns the intersection of multiple row sets
func IntersectMany(sets ...*RowSet) *RowSet {
	if len(sets) == 0 {
		return NewRowSet()
	}

	if len(sets) == 1 {
		return sets[0].Clone()
	}

	// Start with first set
	result := sets[0].Clone()

	// Intersect with remaining sets
	for i := 1; i < len(sets); i++ {
		result = result.Intersect(sets[i])

		// Early termination if result becomes empty
		if result.IsEmpty() {
			return result
		}
	}

	return result
}

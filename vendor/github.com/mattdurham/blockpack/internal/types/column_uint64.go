//nolint:dupl // Type-specific column implementation; separate files are intentional for type safety
package ondisk

// Uint64Value returns the value at the given row if present.
func (c *Column) Uint64Value(idx int) (uint64, bool) {
	// Support uint64, range uint64, and range duration columns (all use uint64 storage)
	// Note: RangeDuration columns are written with uint64 values but may be marked as RangeDuration type in metadata
	if c == nil ||
		(c.Type != ColumnTypeUint64 && c.Type != ColumnTypeRangeUint64 && c.Type != ColumnTypeRangeDuration) ||
		!c.isPresent(idx) ||
		idx >= len(c.uintIndexes) {
		return 0, false
	}
	dictIdx := c.uintIndexes[idx]
	if dictIdx == nullIndex || int(dictIdx) >= len(c.uintDict) {
		return 0, false
	}
	return c.uintDict[dictIdx], true
}

// Uint64Dict returns the uint64 dictionary for optimized scanning.
// Returns the dictionary and index array for direct access.
func (c *Column) Uint64Dict() (dict []uint64, indices []uint32) {
	// Support uint64, range uint64, and range duration columns (all use uint64 storage)
	if c.Type != ColumnTypeUint64 && c.Type != ColumnTypeRangeUint64 && c.Type != ColumnTypeRangeDuration {
		return nil, nil
	}
	return c.uintDict, c.uintIndexes
}

// FindInUint64Dict finds the dictionary index for a target uint64.
// Returns (dictIdx, true) if found, (0, false) if not found.
func (c *Column) FindInUint64Dict(target uint64) (uint32, bool) {
	// Support uint64, range uint64, and range duration columns (all use uint64 storage)
	if c.Type != ColumnTypeUint64 && c.Type != ColumnTypeRangeUint64 && c.Type != ColumnTypeRangeDuration {
		return 0, false
	}
	for i, val := range c.uintDict {
		if val == target {
			return uint32(i), true //nolint:gosec
		}
	}
	return 0, false
}

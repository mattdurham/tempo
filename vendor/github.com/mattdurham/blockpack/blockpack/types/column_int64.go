package ondisk

// Int64Value returns the value at the given row if present.
func (c *Column) Int64Value(idx int) (int64, bool) {
	if c.Type != ColumnTypeInt64 || !c.isPresent(idx) || idx >= len(c.intIndexes) {
		return 0, false
	}
	dictIdx := c.intIndexes[idx]
	if dictIdx == nullIndex || int(dictIdx) >= len(c.intDict) {
		return 0, false
	}
	return c.intDict[dictIdx], true
}

// Int64Dict returns the int64 dictionary for optimized scanning.
// Returns the dictionary and index array for direct access.
func (c *Column) Int64Dict() (dict []int64, indices []uint32) {
	if c.Type != ColumnTypeInt64 {
		return nil, nil
	}
	return c.intDict, c.intIndexes
}

// FindInInt64Dict finds the dictionary index for a target int64.
// Returns (dictIdx, true) if found, (0, false) if not found.
func (c *Column) FindInInt64Dict(target int64) (uint32, bool) {
	if c.Type != ColumnTypeInt64 {
		return 0, false
	}
	for i, val := range c.intDict {
		if val == target {
			return uint32(i), true
		}
	}
	return 0, false
}

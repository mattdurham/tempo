package ondisk

// Float64Value returns the value at the given row if present.
func (c *Column) Float64Value(idx int) (float64, bool) {
	if c == nil || (c.Type != ColumnTypeFloat64 && c.Type != ColumnTypeRangeFloat64) || !c.isPresent(idx) ||
		idx >= len(c.floatIndexes) {
		return 0, false
	}
	dictIdx := c.floatIndexes[idx]
	if dictIdx == nullIndex || int(dictIdx) >= len(c.floatDict) {
		return 0, false
	}
	return c.floatDict[dictIdx], true
}

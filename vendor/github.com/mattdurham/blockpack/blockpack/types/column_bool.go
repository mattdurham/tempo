package ondisk

// BoolValue returns the value at the given row if present.
func (c *Column) BoolValue(idx int) (bool, bool) {
	if c.Type != ColumnTypeBool || !c.isPresent(idx) || idx >= len(c.boolIndexes) {
		return false, false
	}
	dictIdx := c.boolIndexes[idx]
	if dictIdx == nullIndex || int(dictIdx) >= len(c.boolDict) {
		return false, false
	}
	val := c.boolDict[dictIdx]
	return val == 1, true
}

package ondisk

// StringValues returns the string dictionary values.
func (c *Column) StringValues() []string {
	if c.Type != ColumnTypeString {
		return nil
	}
	return append([]string(nil), c.stringDict...)
}

// StringValue returns the value at the given row if present.
func (c *Column) StringValue(idx int) (string, bool) {
	if c.Type != ColumnTypeString || !c.isPresent(idx) || idx >= len(c.stringValues) {
		return "", false
	}
	dictIdx := c.stringValues[idx]
	if dictIdx == nullStringIdx || int(dictIdx) >= len(c.stringDict) {
		return "", false
	}
	return c.stringDict[dictIdx], true
}

// StringDict returns the string dictionary for optimized scanning.
// Returns the dictionary and index array for direct access.
func (c *Column) StringDict() (dict []string, indices []uint32) {
	if c.Type != ColumnTypeString {
		return nil, nil
	}
	return c.stringDict, c.stringValues
}

// FindInStringDict finds the dictionary index for a target string.
// Returns (dictIdx, true) if found, (0, false) if not found.
// This enables optimized scanning by searching the dictionary once
// instead of comparing strings for every row.
func (c *Column) FindInStringDict(target string) (uint32, bool) {
	if c.Type != ColumnTypeString {
		return 0, false
	}
	for i, val := range c.stringDict {
		if val == target {
			return uint32(i), true
		}
	}
	return 0, false
}

package ondisk

// BytesValue returns a copy of the bytes at the given row if present.
func (c *Column) BytesValue(idx int) ([]byte, bool) {
	if c == nil || (c.Type != ColumnTypeBytes && c.Type != ColumnTypeRangeBytes) || !c.isPresent(idx) {
		return nil, false
	}
	view, ok := c.BytesValueView(idx)
	if !ok {
		return nil, false
	}
	out := make([]byte, len(view))
	copy(out, view)
	return out, true
}

// BytesValueView returns the raw dictionary-backed bytes without copying.
// The returned slice must be treated as immutable.
func (c *Column) BytesValueView(idx int) ([]byte, bool) {
	if c == nil || (c.Type != ColumnTypeBytes && c.Type != ColumnTypeRangeBytes) || !c.isPresent(idx) {
		return nil, false
	}
	if c.bytesInline != nil {
		if idx >= len(c.bytesInline) {
			return nil, false
		}
		return c.bytesInline[idx], true
	}
	if idx >= len(c.bytesIndexes) {
		return nil, false
	}
	dictIdx := c.bytesIndexes[idx]
	if dictIdx == nullBytesIdx || int(dictIdx) >= len(c.bytesDict) {
		return nil, false
	}
	return c.bytesDict[dictIdx], true
}

package ondisk

// SetStringData assigns decoded dictionary and index data for string columns.
func (c *Column) SetStringData(dict []string, values []uint32, present []byte) {
	c.stringDict = dict
	c.stringValues = values
	c.present = present
}

// SetInt64Data assigns decoded dictionary and index data for int64 columns.
func (c *Column) SetInt64Data(dict []int64, indexes []uint32, present []byte) {
	c.intDict = dict
	c.intIndexes = indexes
	c.present = present
}

// SetUint64Data assigns decoded dictionary and index data for uint64 columns.
func (c *Column) SetUint64Data(dict []uint64, indexes []uint32, present []byte) {
	c.uintDict = dict
	c.uintIndexes = indexes
	c.present = present
}

// SetBoolData assigns decoded dictionary and index data for bool columns.
func (c *Column) SetBoolData(dict []uint8, indexes []uint32, present []byte) {
	c.boolDict = dict
	c.boolIndexes = indexes
	c.present = present
}

// SetFloat64Data assigns decoded dictionary and index data for float64 columns.
func (c *Column) SetFloat64Data(dict []float64, indexes []uint32, present []byte) {
	c.floatDict = dict
	c.floatIndexes = indexes
	c.present = present
}

// SetBytesDictData assigns decoded dictionary and index data for bytes columns.
func (c *Column) SetBytesDictData(dict [][]byte, indexes []uint32, present []byte) {
	c.bytesDict = dict
	c.bytesIndexes = indexes
	c.present = present
}

// SetBytesInlineData assigns inline bytes data for bytes columns.
func (c *Column) SetBytesInlineData(values [][]byte, present []byte) {
	c.bytesInline = values
	c.present = present
}

// BoolDict returns the bool dictionary and index array for optimized scanning.
func (c *Column) BoolDict() (dict []uint8, indices []uint32) {
	if c.Type != ColumnTypeBool {
		return nil, nil
	}
	return c.boolDict, c.boolIndexes
}

// Float64Dict returns the float64 dictionary and index array for optimized scanning.
func (c *Column) Float64Dict() (dict []float64, indices []uint32) {
	if c.Type != ColumnTypeFloat64 && c.Type != ColumnTypeRangeFloat64 {
		return nil, nil
	}
	return c.floatDict, c.floatIndexes
}

// BytesDict returns the bytes dictionary and index array when using dictionary encoding.
func (c *Column) BytesDict() (dict [][]byte, indices []uint32) {
	if (c.Type != ColumnTypeBytes && c.Type != ColumnTypeRangeBytes) || c.bytesInline != nil {
		return nil, nil
	}
	return c.bytesDict, c.bytesIndexes
}

// BytesInline returns inline bytes values when using inline encoding.
func (c *Column) BytesInline() [][]byte {
	if c.Type != ColumnTypeBytes && c.Type != ColumnTypeRangeBytes {
		return nil
	}
	return c.bytesInline
}

// ReuseIndexSlice returns a zeroed index slice reusing the current column storage when possible.
func (c *Column) ReuseIndexSlice(rowCount uint32) []uint32 {
	var existing []uint32
	switch c.Type {
	case ColumnTypeString, ColumnTypeRangeString:
		existing = c.stringValues
	case ColumnTypeInt64, ColumnTypeRangeInt64, ColumnTypeRangeDuration:
		existing = c.intIndexes
	case ColumnTypeUint64, ColumnTypeRangeUint64:
		existing = c.uintIndexes
	case ColumnTypeBool:
		existing = c.boolIndexes
	case ColumnTypeFloat64, ColumnTypeRangeFloat64:
		existing = c.floatIndexes
	case ColumnTypeBytes, ColumnTypeRangeBytes:
		existing = c.bytesIndexes
	default:
		// For unknown types, create a new slice
		return make([]uint32, rowCount)
	}

	if cap(existing) >= int(rowCount) {
		values := existing[:rowCount]
		for i := range values {
			values[i] = 0
		}
		return values
	}
	return make([]uint32, rowCount)
}

package reader

// Block holds decoded columns and metadata for a single block.
type Block struct {
	columns map[string]*Column // private field for columns

	spanCount int // private field for span count

	MinStart        uint64
	MaxStart        uint64
	ColumnNameBloom ColumnNameBloom
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
}

// SpanCount returns number of spans in the block.
func (b *Block) SpanCount() int {
	return b.spanCount
}

// Columns returns all decoded columns keyed by name.
func (b *Block) Columns() map[string]*Column {
	return b.columns
}

// GetColumn returns a specific column by name, or nil if not found.
//
// Callers MUST check for nil before accessing the returned column.
// A nil return indicates the column does not exist in this block.
//
// Example usage:
//
//	col := block.GetColumn("span.name")
//	if col == nil {
//	    return nil, false  // Column not found
//	}
//	// Safe to use col here
//	value, ok := col.StringValue(spanIdx)
func (b *Block) GetColumn(name string) *Column {
	return b.columns[name]
}

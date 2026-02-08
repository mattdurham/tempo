package executor

import (
	"fmt"

	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

// ColumnInfo provides metadata about a column
type ColumnInfo struct {
	Name   string
	Type   blockpack.ColumnType
	Exists bool
}

// BlockColumnProvider implements ColumnDataProvider for a single block
type BlockColumnProvider struct {
	block   *blockpackio.Block
	scanner *ColumnScanner
}

// NewBlockColumnProvider creates a provider for a block
func NewBlockColumnProvider(block *blockpackio.Block) *BlockColumnProvider {
	return &BlockColumnProvider{
		block:   block,
		scanner: NewColumnScanner(block),
	}
}

// GetColumns returns metadata about all columns
func (bcp *BlockColumnProvider) GetColumns() map[string]ColumnInfo {
	result := make(map[string]ColumnInfo)
	for name, col := range bcp.block.Columns() {
		result[name] = ColumnInfo{
			Name:   name,
			Type:   col.Type,
			Exists: true,
		}
	}
	return result
}

// GetRowCount returns the number of rows in the block
func (bcp *BlockColumnProvider) GetRowCount() int {
	return bcp.block.SpanCount()
}

// ScanEqual scans for rows where column equals value
func (bcp *BlockColumnProvider) ScanEqual(column string, value interface{}) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanEqual(column, value)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanNotEqual scans for rows where column does not equal value
func (bcp *BlockColumnProvider) ScanNotEqual(column string, value interface{}) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanNotEqual(column, value)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanLessThan scans for rows where column < value
func (bcp *BlockColumnProvider) ScanLessThan(column string, value interface{}) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanLessThan(column, value)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanLessThanOrEqual scans for rows where column <= value
func (bcp *BlockColumnProvider) ScanLessThanOrEqual(column string, value interface{}) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanLessThanOrEqual(column, value)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanGreaterThan scans for rows where column > value
func (bcp *BlockColumnProvider) ScanGreaterThan(column string, value interface{}) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanGreaterThan(column, value)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanGreaterThanOrEqual scans for rows where column >= value
func (bcp *BlockColumnProvider) ScanGreaterThanOrEqual(column string, value interface{}) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanGreaterThanOrEqual(column, value)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanIsNull scans for rows where column is NULL
func (bcp *BlockColumnProvider) ScanIsNull(column string) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanIsNull(column)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanIsNotNull scans for rows where column is NOT NULL
func (bcp *BlockColumnProvider) ScanIsNotNull(column string) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanIsNotNull(column)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanRegex scans for rows where column matches regex
func (bcp *BlockColumnProvider) ScanRegex(column string, pattern string) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanRegex(column, pattern)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanRegexNotMatch scans for rows where column does not match regex
func (bcp *BlockColumnProvider) ScanRegexNotMatch(column string, pattern string) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanRegexNotMatch(column, pattern)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// ScanContains scans for rows where column contains substring
func (bcp *BlockColumnProvider) ScanContains(column string, substring string) (vm.RowSet, error) {
	result, err := bcp.scanner.ScanContains(column, substring)
	if err != nil {
		return nil, err
	}
	return columnMatchResultToRowSet(result), nil
}

// GetValue returns the value at a specific row index
func (bcp *BlockColumnProvider) GetValue(column string, rowIdx int) (interface{}, bool, error) {
	col := bcp.block.GetColumn(column)
	if col == nil {
		return nil, false, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		val, ok := col.StringValue(rowIdx)
		return val, ok, nil
	case blockpack.ColumnTypeInt64:
		val, ok := col.Int64Value(rowIdx)
		return val, ok, nil
	case blockpack.ColumnTypeUint64:
		val, ok := col.Uint64Value(rowIdx)
		return val, ok, nil
	case blockpack.ColumnTypeBool:
		val, ok := col.BoolValue(rowIdx)
		return val, ok, nil
	case blockpack.ColumnTypeFloat64:
		val, ok := col.Float64Value(rowIdx)
		return val, ok, nil
	case blockpack.ColumnTypeBytes:
		val, ok := col.BytesValue(rowIdx)
		return val, ok, nil
	default:
		// Range-bucketed types (ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration)
		// do not support direct value retrieval. They are used for dedicated column indexing only.
		return nil, false, fmt.Errorf("column %q with type %v does not support GetValue (range-bucketed columns require dedicated column access)", column, col.Type)
	}
}

// Union returns a new RowSet containing all rows from both sets.
// PANICS if parameters are not *RowSet - this indicates a programmer error
// where the wrong implementation of vm.RowSet was passed to this provider.
func (bcp *BlockColumnProvider) Union(a, b vm.RowSet) vm.RowSet {
	// Type assert to get concrete types with safety check
	aRowSet, ok := a.(*RowSet)
	if !ok {
		// This is a programmer error - wrong RowSet implementation was passed
		panic(fmt.Sprintf("BlockColumnProvider.Union: expected *RowSet for parameter a, got %T - this indicates a bug in query execution", a))
	}
	bRowSet, ok := b.(*RowSet)
	if !ok {
		// This is a programmer error - wrong RowSet implementation was passed
		panic(fmt.Sprintf("BlockColumnProvider.Union: expected *RowSet for parameter b, got %T - this indicates a bug in query execution", b))
	}
	return aRowSet.Union(bRowSet)
}

// Intersect returns a new RowSet containing only rows in both sets.
// PANICS if parameters are not *RowSet - this indicates a programmer error.
func (bcp *BlockColumnProvider) Intersect(a, b vm.RowSet) vm.RowSet {
	aRowSet, ok := a.(*RowSet)
	if !ok {
		panic(fmt.Sprintf("BlockColumnProvider.Intersect: expected *RowSet for parameter a, got %T - this indicates a bug in query execution", a))
	}
	bRowSet, ok := b.(*RowSet)
	if !ok {
		panic(fmt.Sprintf("BlockColumnProvider.Intersect: expected *RowSet for parameter b, got %T - this indicates a bug in query execution", b))
	}
	return aRowSet.Intersect(bRowSet)
}

// Complement returns a new RowSet containing all rows NOT in the set.
// PANICS if parameter is not *RowSet - this indicates a programmer error.
func (bcp *BlockColumnProvider) Complement(rs vm.RowSet) vm.RowSet {
	rowSet, ok := rs.(*RowSet)
	if !ok {
		panic(fmt.Sprintf("BlockColumnProvider.Complement: expected *RowSet for parameter rs, got %T - this indicates a bug in query execution", rs))
	}
	return rowSet.Complement(bcp.block.SpanCount())
}

// FullScan returns a row set with all rows in the block
func (bcp *BlockColumnProvider) FullScan() vm.RowSet {
	rowSet := NewRowSet()
	for i := 0; i < bcp.block.SpanCount(); i++ {
		rowSet.Add(i)
	}
	return rowSet
}

// StreamScanEqual scans for rows where column equals value, calling callback for each match
// This is optimized to avoid RowSet allocation by streaming directly from the scanner
func (bcp *BlockColumnProvider) StreamScanEqual(column string, value interface{}, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanEqual(column, value, callback)
}

// StreamScanNotEqual scans for rows where column != value
func (bcp *BlockColumnProvider) StreamScanNotEqual(column string, value interface{}, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanNotEqual(column, value, callback)
}

// StreamScanLessThan scans for rows where column < value
func (bcp *BlockColumnProvider) StreamScanLessThan(column string, value interface{}, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanLessThan(column, value, callback)
}

// StreamScanLessThanOrEqual scans for rows where column <= value
func (bcp *BlockColumnProvider) StreamScanLessThanOrEqual(column string, value interface{}, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanLessThanOrEqual(column, value, callback)
}

// StreamScanGreaterThan scans for rows where column > value
func (bcp *BlockColumnProvider) StreamScanGreaterThan(column string, value interface{}, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanGreaterThan(column, value, callback)
}

// StreamScanGreaterThanOrEqual scans for rows where column >= value
func (bcp *BlockColumnProvider) StreamScanGreaterThanOrEqual(column string, value interface{}, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanGreaterThanOrEqual(column, value, callback)
}

// StreamScanIsNull scans for rows where column IS NULL
func (bcp *BlockColumnProvider) StreamScanIsNull(column string, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanIsNull(column, callback)
}

// StreamScanIsNotNull scans for rows where column IS NOT NULL
func (bcp *BlockColumnProvider) StreamScanIsNotNull(column string, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanIsNotNull(column, callback)
}

// StreamScanRegex scans for rows where column matches regex pattern
func (bcp *BlockColumnProvider) StreamScanRegex(column string, pattern string, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanRegex(column, pattern, callback)
}

// StreamScanRegexNotMatch scans for rows where column does NOT match regex pattern
func (bcp *BlockColumnProvider) StreamScanRegexNotMatch(column string, pattern string, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanRegexNotMatch(column, pattern, callback)
}

// StreamScanContains scans for rows where column contains substring
func (bcp *BlockColumnProvider) StreamScanContains(column string, substring string, callback vm.RowCallback) (int, error) {
	return bcp.scanner.StreamScanContains(column, substring, callback)
}

// StreamFullScan calls callback for every row
func (bcp *BlockColumnProvider) StreamFullScan(callback vm.RowCallback) (int, error) {
	count := 0
	for i := 0; i < bcp.block.SpanCount(); i++ {
		if !callback(i) {
			break
		}
		count++
	}
	return count, nil
}

// columnMatchResultToRowSet converts a ColumnMatchResult to a RowSet
func columnMatchResultToRowSet(result *ColumnMatchResult) *RowSet {
	rowSet := NewRowSet()
	for rowIdx := range result.MatchedRows {
		rowSet.Add(rowIdx)
	}
	return rowSet
}

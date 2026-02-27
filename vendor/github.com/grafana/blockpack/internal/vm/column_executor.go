package vm

// RowSet represents a set of matching row indices
// This interface is implemented by executor.RowSet to avoid import cycles
type RowSet interface {
	Add(rowIdx int)
	Contains(rowIdx int) bool
	Size() int
	IsEmpty() bool
	ToSlice() []int
}

// ColumnDataProvider is the interface the VM uses to access blockpack data
// This keeps the VM decoupled from the storage implementation
type ColumnDataProvider interface {
	// Metadata operations
	GetRowCount() int

	// Bulk column scans - return row sets
	ScanEqual(column string, value interface{}) (RowSet, error)
	ScanNotEqual(column string, value interface{}) (RowSet, error)
	ScanLessThan(column string, value interface{}) (RowSet, error)
	ScanLessThanOrEqual(column string, value interface{}) (RowSet, error)
	ScanGreaterThan(column string, value interface{}) (RowSet, error)
	ScanGreaterThanOrEqual(column string, value interface{}) (RowSet, error)
	ScanIsNull(column string) (RowSet, error)
	ScanIsNotNull(column string) (RowSet, error)
	ScanRegex(column string, pattern string) (RowSet, error)
	ScanRegexNotMatch(column string, pattern string) (RowSet, error)
	ScanContains(column string, substring string) (RowSet, error)

	// Streaming scans - call callback for each matching row (no RowSet allocation)
	StreamScanEqual(column string, value interface{}, callback RowCallback) (int, error)
	StreamScanNotEqual(column string, value interface{}, callback RowCallback) (int, error)
	StreamScanLessThan(column string, value interface{}, callback RowCallback) (int, error)
	StreamScanLessThanOrEqual(column string, value interface{}, callback RowCallback) (int, error)
	StreamScanGreaterThan(column string, value interface{}, callback RowCallback) (int, error)
	StreamScanGreaterThanOrEqual(column string, value interface{}, callback RowCallback) (int, error)
	StreamScanIsNull(column string, callback RowCallback) (int, error)
	StreamScanIsNotNull(column string, callback RowCallback) (int, error)
	StreamScanRegex(column string, pattern string, callback RowCallback) (int, error)
	StreamScanRegexNotMatch(column string, pattern string, callback RowCallback) (int, error)
	StreamScanContains(column string, substring string, callback RowCallback) (int, error)
	StreamFullScan(callback RowCallback) (int, error)

	// Set operations on row sets
	Union(a, b RowSet) RowSet
	Intersect(a, b RowSet) RowSet
	Complement(rs RowSet) RowSet
	FullScan() RowSet

	// Individual value access (for operations that can't use bulk scans)
	GetValue(column string, rowIdx int) (interface{}, bool, error)
}

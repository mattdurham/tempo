package vm

import regexp "github.com/coregx/coregex"

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
	ScanEqual(column string, value any) (RowSet, error)
	// ScanEqualAny scans a single column for any of the given values in one pass.
	// It is equivalent to unioning N ScanEqual calls but avoids repeating the full
	// column scan for each value (dict fast-path for string columns: O(dict+spans)
	// instead of N×O(spans)).
	ScanEqualAny(column string, values []any) (RowSet, error)
	ScanNotEqual(column string, value any) (RowSet, error)
	ScanLessThan(column string, value any) (RowSet, error)
	ScanLessThanOrEqual(column string, value any) (RowSet, error)
	ScanGreaterThan(column string, value any) (RowSet, error)
	ScanGreaterThanOrEqual(column string, value any) (RowSet, error)
	ScanIsNull(column string) (RowSet, error)
	ScanIsNotNull(column string) (RowSet, error)
	ScanRegex(column string, pattern string) (RowSet, error)
	ScanRegexNotMatch(column string, pattern string) (RowSet, error)
	ScanContains(column string, substring string) (RowSet, error)
	// ScanRegexFast accepts a pre-compiled regexp and optional literal prefixes.
	// Two calling conventions:
	//   - re != nil: standard path. prefixes (if non-nil) are used as a fast-path
	//     pre-filter — rows that contain none of the prefixes are skipped before
	//     applying the regex. prefixes must be case-sensitive literals.
	//   - re == nil: CI fold-contains path. prefixes must be pre-lowercased literals
	//     (as produced by AnalyzeRegex for CaseInsensitive patterns). Implementations
	//     use strings.ToLower(v) + strings.Contains instead of the regex engine.
	ScanRegexFast(column string, re *regexp.Regexp, prefixes []string) (RowSet, error)
	// ScanRegexNotMatchFast follows the same re/prefixes calling convention as ScanRegexFast.
	ScanRegexNotMatchFast(column string, re *regexp.Regexp, prefixes []string) (RowSet, error)

	// Streaming scans - call callback for each matching row (no RowSet allocation)
	StreamScanEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanNotEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanLessThan(column string, value any, callback RowCallback) (int, error)
	StreamScanLessThanOrEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanGreaterThan(column string, value any, callback RowCallback) (int, error)
	StreamScanGreaterThanOrEqual(column string, value any, callback RowCallback) (int, error)
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
	GetValue(column string, rowIdx int) (any, bool, error)
}

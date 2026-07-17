package vm

import regexp "github.com/coregx/coregex"

// ColumnDataProvider is a blockpack data type.
type ColumnDataProvider interface {
	GetRowCount() int
	ScanEqual(column string, value any) (RowSet, error)
	ScanEqualAny(column string, values []any) (RowSet, error)
	ScanNotEqual(column string, value any) (RowSet, error)
	ScanLessThan(column string, value any) (RowSet, error)
	ScanLessThanOrEqual(column string, value any) (RowSet, error)
	ScanGreaterThan(column string, value any) (RowSet, error)
	ScanGreaterThanOrEqual(column string, value any) (RowSet, error)
	ScanIsNull(column string) (RowSet, error)
	ScanIsNotNull(column string) (RowSet, error)
	ScanRegex(column, pattern string) (RowSet, error)
	ScanRegexNotMatch(column, pattern string) (RowSet, error)
	ScanContains(column, substring string) (RowSet, error)
	ScanRegexFast(column string, re *regexp.Regexp, prefixes []string, kind RegexFastKind) (RowSet, error)
	ScanRegexNotMatchFast(column string, re *regexp.Regexp, prefixes []string, kind RegexFastKind) (RowSet, error)
	StreamScanEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanNotEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanLessThan(column string, value any, callback RowCallback) (int, error)
	StreamScanLessThanOrEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanGreaterThan(column string, value any, callback RowCallback) (int, error)
	StreamScanGreaterThanOrEqual(column string, value any, callback RowCallback) (int, error)
	StreamScanIsNull(column string, callback RowCallback) (int, error)
	StreamScanIsNotNull(column string, callback RowCallback) (int, error)
	StreamScanRegex(column, pattern string, callback RowCallback) (int, error)
	StreamScanRegexNotMatch(column, pattern string, callback RowCallback) (int, error)
	StreamScanContains(column, substring string, callback RowCallback) (int, error)
	StreamFullScan(callback RowCallback) (int, error)
	Union(a, b RowSet) RowSet
	Intersect(a, b RowSet) RowSet
	Complement(rs RowSet) RowSet
	FullScan() RowSet
	GetValue(column string, rowIdx int) (any, bool, error)
}

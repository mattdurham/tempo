package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// blockColumnProvider implements vm.ColumnDataProvider for a modules Block.
type blockColumnProvider struct {
	block *modules_reader.Block
}

func newBlockColumnProvider(block *modules_reader.Block) *blockColumnProvider {
	return &blockColumnProvider{block: block}
}

// lookupColumn finds a column by name, falling back to resource./span. prefixes for unscoped names.
func (p *blockColumnProvider) lookupColumn(name string) *modules_reader.Column {
	if col := p.block.GetColumn(name); col != nil {
		return col
	}
	// For unscoped names (no dot or colon prefix), try resource. and span.
	if !strings.ContainsAny(name, ".:") {
		if col := p.block.GetColumn("resource." + name); col != nil {
			return col
		}
		if col := p.block.GetColumn("span." + name); col != nil {
			return col
		}
	}
	return nil
}

// GetRowCount returns the total span count.
func (p *blockColumnProvider) GetRowCount() int { return p.block.SpanCount() }

// rowMatches reports whether row rowIdx of col matches value using equality semantics.
func rowMatches(col *modules_reader.Column, rowIdx int, value interface{}) bool {
	if !col.IsPresent(rowIdx) {
		return false
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			if s, ok2 := value.(string); ok2 {
				return v == s
			}
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			switch t := value.(type) {
			case int64:
				return v == t
			case float64:
				return float64(v) == t
			}
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			switch t := value.(type) {
			case int64:
				return v == uint64(t) //nolint:gosec
			case uint64:
				return v == t
			case float64:
				return float64(v) == t
			}
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			switch t := value.(type) {
			case float64:
				return v == t
			case int64:
				return v == float64(t)
			}
		}
	case modules_shared.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			if b, ok2 := value.(bool); ok2 {
				return v == b
			}
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		if v, ok := col.BytesValue(rowIdx); ok {
			switch t := value.(type) {
			case []byte:
				return bytes.Equal(v, t)
			case string:
				return string(v) == t
			}
		}
	}
	return false
}

// rowCompare returns -1/0/1 for col[rowIdx] <=> value (for range comparisons).
// Returns (0, false) when the comparison is not applicable.
func rowCompare(col *modules_reader.Column, rowIdx int, value interface{}) (int, bool) {
	if !col.IsPresent(rowIdx) {
		return 0, false
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			if s, ok2 := value.(string); ok2 {
				switch {
				case v < s:
					return -1, true
				case v > s:
					return 1, true
				default:
					return 0, true
				}
			}
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			switch t := value.(type) {
			case int64:
				switch {
				case v < t:
					return -1, true
				case v > t:
					return 1, true
				default:
					return 0, true
				}
			case float64:
				switch {
				case float64(v) < t:
					return -1, true
				case float64(v) > t:
					return 1, true
				default:
					return 0, true
				}
			}
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			switch t := value.(type) {
			case int64:
				u := uint64(t) //nolint:gosec
				switch {
				case v < u:
					return -1, true
				case v > u:
					return 1, true
				default:
					return 0, true
				}
			case float64:
				switch {
				case float64(v) < t:
					return -1, true
				case float64(v) > t:
					return 1, true
				default:
					return 0, true
				}
			}
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			switch t := value.(type) {
			case float64:
				switch {
				case v < t:
					return -1, true
				case v > t:
					return 1, true
				default:
					return 0, true
				}
			case int64:
				f := float64(t)
				switch {
				case v < f:
					return -1, true
				case v > f:
					return 1, true
				default:
					return 0, true
				}
			}
		}
	}
	return 0, false
}

// scanWith iterates all rows 0..n-1, calls cond for each, and calls cb for rows where cond returns true.
func (p *blockColumnProvider) scanWith(col *modules_reader.Column, cond func(i int) bool, cb vm.RowCallback) int {
	n := p.block.SpanCount()
	count := 0
	for i := range n {
		if cond(i) {
			if !cb(i) {
				return count
			}
			count++
		}
	}
	return count
}

// --- StreamScan methods ---

// StreamScanEqual scans all rows and calls cb for each row where column equals value.
func (p *blockColumnProvider) StreamScanEqual(column string, value interface{}, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool { return rowMatches(col, i, value) }, cb)
	return n, nil
}

// StreamScanNotEqual scans all rows and calls cb for each row where column does not equal value.
func (p *blockColumnProvider) StreamScanNotEqual(column string, value interface{}, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	n := p.block.SpanCount()
	count := 0
	for i := range n {
		// NotEqual includes null rows (they are not equal to anything).
		present := col != nil && col.IsPresent(i)
		if !present || !rowMatches(col, i, value) {
			if !cb(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// StreamScanLessThan scans all rows and calls cb for each row where column < value.
func (p *blockColumnProvider) StreamScanLessThan(column string, value interface{}, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp < 0
	}, cb)
	return n, nil
}

// StreamScanLessThanOrEqual scans all rows and calls cb for each row where column <= value.
func (p *blockColumnProvider) StreamScanLessThanOrEqual(
	column string,
	value interface{},
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp <= 0
	}, cb)
	return n, nil
}

// StreamScanGreaterThan scans all rows and calls cb for each row where column > value.
func (p *blockColumnProvider) StreamScanGreaterThan(
	column string,
	value interface{},
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp > 0
	}, cb)
	return n, nil
}

// StreamScanGreaterThanOrEqual scans all rows and calls cb for each row where column >= value.
func (p *blockColumnProvider) StreamScanGreaterThanOrEqual(
	column string,
	value interface{},
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp >= 0
	}, cb)
	return n, nil
}

// StreamScanIsNull scans all rows and calls cb for each row where column is null.
func (p *blockColumnProvider) StreamScanIsNull(column string, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	n := p.block.SpanCount()
	count := 0
	for i := range n {
		isNull := col == nil || !col.IsPresent(i)
		if isNull {
			if !cb(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// StreamScanIsNotNull scans all rows and calls cb for each row where column is not null.
func (p *blockColumnProvider) StreamScanIsNotNull(column string, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool { return col.IsPresent(i) }, cb)
	return n, nil
}

// StreamScanRegex scans all rows and calls cb for each row where column matches pattern.
func (p *blockColumnProvider) StreamScanRegex(column string, pattern string, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, fmt.Errorf("invalid regex %q: %w", pattern, err)
	}
	n := p.scanWith(col, func(i int) bool {
		if !col.IsPresent(i) {
			return false
		}
		if v, ok := col.StringValue(i); ok {
			return re.MatchString(v)
		}
		return false
	}, cb)
	return n, nil
}

// StreamScanRegexNotMatch scans all rows and calls cb for each row where column does not match pattern.
func (p *blockColumnProvider) StreamScanRegexNotMatch(
	column string,
	pattern string,
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, fmt.Errorf("invalid regex %q: %w", pattern, err)
	}
	n := p.block.SpanCount()
	count := 0
	for i := range n {
		var matches bool
		if col != nil && col.IsPresent(i) {
			if v, ok := col.StringValue(i); ok {
				matches = re.MatchString(v)
			}
		}
		if !matches {
			if !cb(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// StreamScanContains scans all rows and calls cb for each row where column contains substring.
func (p *blockColumnProvider) StreamScanContains(column string, substring string, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool {
		if !col.IsPresent(i) {
			return false
		}
		if v, ok := col.StringValue(i); ok {
			return strings.Contains(v, substring)
		}
		return false
	}, cb)
	return n, nil
}

// StreamFullScan calls cb for every row in the block.
func (p *blockColumnProvider) StreamFullScan(cb vm.RowCallback) (int, error) {
	n := p.block.SpanCount()
	for i := range n {
		if !cb(i) {
			return i, nil
		}
	}
	return n, nil
}

// --- Scan methods (collect into RowSet) ---

func collectStream(fn func(cb vm.RowCallback) (int, error)) (vm.RowSet, error) {
	rs := newRowSet()
	_, err := fn(func(rowIdx int) bool {
		rs.Add(rowIdx)
		return true
	})
	if err != nil {
		return nil, err
	}
	return rs, nil
}

// ScanEqual returns a RowSet of all rows where column equals value.
func (p *blockColumnProvider) ScanEqual(column string, value interface{}) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanEqual(column, value, cb)
	})
}

// ScanNotEqual returns a RowSet of all rows where column does not equal value.
func (p *blockColumnProvider) ScanNotEqual(column string, value interface{}) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanNotEqual(column, value, cb)
	})
}

// ScanLessThan returns a RowSet of all rows where column < value.
func (p *blockColumnProvider) ScanLessThan(column string, value interface{}) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanLessThan(column, value, cb)
	})
}

// ScanLessThanOrEqual returns a RowSet of all rows where column <= value.
func (p *blockColumnProvider) ScanLessThanOrEqual(column string, value interface{}) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanLessThanOrEqual(column, value, cb)
	})
}

// ScanGreaterThan returns a RowSet of all rows where column > value.
func (p *blockColumnProvider) ScanGreaterThan(column string, value interface{}) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanGreaterThan(column, value, cb)
	})
}

// ScanGreaterThanOrEqual returns a RowSet of all rows where column >= value.
func (p *blockColumnProvider) ScanGreaterThanOrEqual(column string, value interface{}) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanGreaterThanOrEqual(column, value, cb)
	})
}

// ScanIsNull returns a RowSet of all rows where column is null.
func (p *blockColumnProvider) ScanIsNull(column string) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanIsNull(column, cb)
	})
}

// ScanIsNotNull returns a RowSet of all rows where column is not null.
func (p *blockColumnProvider) ScanIsNotNull(column string) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanIsNotNull(column, cb)
	})
}

// ScanRegex returns a RowSet of all rows where column matches pattern.
func (p *blockColumnProvider) ScanRegex(column string, pattern string) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanRegex(column, pattern, cb)
	})
}

// ScanRegexNotMatch returns a RowSet of all rows where column does not match pattern.
func (p *blockColumnProvider) ScanRegexNotMatch(column string, pattern string) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanRegexNotMatch(column, pattern, cb)
	})
}

// ScanContains returns a RowSet of all rows where column contains substring.
func (p *blockColumnProvider) ScanContains(column string, substring string) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanContains(column, substring, cb)
	})
}

// --- Set operations ---

// FullScan returns a RowSet containing all row indices [0, SpanCount).
func (p *blockColumnProvider) FullScan() vm.RowSet {
	n := p.block.SpanCount()
	rs := &rowSet{rows: make([]int, n)}
	for i := range n {
		rs.rows[i] = i
	}
	return rs
}

// Union returns a RowSet that is the union of a and b (sorted, deduplicated).
func (p *blockColumnProvider) Union(a, b vm.RowSet) vm.RowSet {
	as := a.ToSlice()
	bs := b.ToSlice()
	merged := make([]int, 0, len(as)+len(bs))
	i, j := 0, 0
	for i < len(as) && j < len(bs) {
		switch {
		case as[i] < bs[j]:
			merged = append(merged, as[i])
			i++
		case as[i] > bs[j]:
			merged = append(merged, bs[j])
			j++
		default:
			merged = append(merged, as[i])
			i++
			j++
		}
	}
	merged = append(merged, as[i:]...)
	merged = append(merged, bs[j:]...)
	return &rowSet{rows: merged}
}

// Intersect returns a RowSet that is the intersection of a and b.
func (p *blockColumnProvider) Intersect(a, b vm.RowSet) vm.RowSet {
	as := a.ToSlice()
	bs := b.ToSlice()
	result := make([]int, 0, min(len(as), len(bs)))
	i, j := 0, 0
	for i < len(as) && j < len(bs) {
		switch {
		case as[i] < bs[j]:
			i++
		case as[i] > bs[j]:
			j++
		default:
			result = append(result, as[i])
			i++
			j++
		}
	}
	return &rowSet{rows: result}
}

// Complement returns a RowSet of all rows NOT in rs.
func (p *blockColumnProvider) Complement(rs vm.RowSet) vm.RowSet {
	n := p.block.SpanCount()
	included := rs.ToSlice()
	result := make([]int, 0, n-len(included))
	j := 0
	for i := range n {
		if j < len(included) && included[j] == i {
			j++
		} else {
			result = append(result, i)
		}
	}
	return &rowSet{rows: result}
}

// --- GetValue ---

// NewColumnProvider returns a vm.ColumnDataProvider backed by the given modules block.
// Used by api.go's StreamTraceQLModules to evaluate vm.Program.ColumnPredicate per block.
func NewColumnProvider(block *modules_reader.Block) vm.ColumnDataProvider {
	return newBlockColumnProvider(block)
}

// GetValue returns the value at column/rowIdx.
// Returns (nil, false, nil) when the column is absent or the row is null.
func (p *blockColumnProvider) GetValue(column string, rowIdx int) (interface{}, bool, error) {
	col := p.lookupColumn(column)
	if col == nil || !col.IsPresent(rowIdx) {
		return nil, false, nil
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			return v, true, nil
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			return v, true, nil
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			return v, true, nil
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			return v, true, nil
		}
	case modules_shared.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			return v, true, nil
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		if v, ok := col.BytesValue(rowIdx); ok {
			return v, true, nil
		}
	}
	return nil, false, nil
}

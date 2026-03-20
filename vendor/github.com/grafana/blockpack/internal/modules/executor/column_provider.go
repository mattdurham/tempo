package executor

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"fmt"
	"strconv"
	"strings"

	regexp "github.com/coregx/coregex"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/vm"
)

// cmp3 returns cmp.Compare(a, b) and true. Wraps cmp.Compare to match rowCompare's (int, bool) signature.
// NOTE-039: reduces rowCompare cyclomatic complexity from 40 to ~20 by replacing repeated
// three-way switch blocks with a single generic call.
func cmp3[T cmp.Ordered](a, b T) (int, bool) {
	return cmp.Compare(a, b), true
}

// blockColumnProvider implements vm.ColumnDataProvider for a modules Block.
type blockColumnProvider struct {
	block *modules_reader.Block
}

func newBlockColumnProvider(block *modules_reader.Block) *blockColumnProvider {
	return &blockColumnProvider{block: block}
}

// lookupColumn finds a column by its fully-qualified name.
// All callers (TraceQL via unscopedOrScoped, LogQL via compileSinglePushdownPredicate)
// pre-expand unscoped attribute names to resource./span./log. at compile time,
// so no runtime fallback is needed here.
func (p *blockColumnProvider) lookupColumn(name string) *modules_reader.Column {
	return p.block.GetColumn(name)
}

// GetRowCount returns the total row count (spans for traces, records for logs).
func (p *blockColumnProvider) GetRowCount() int { return p.block.SpanCount() }

// rowEqual reports whether col[rowIdx] equals value using type-strict semantics.
// String columns only match string values (no float coercion) — use rowCompare for
// range operators where numeric coercion of string columns is intentional.
func rowEqual(col *modules_reader.Column, rowIdx int, value interface{}) bool {
	cmp, ok := rowCompare(col, rowIdx, value)
	if !ok {
		return false
	}
	// For string columns, rowCompare may succeed with float64 values via ParseFloat.
	// Reject that path here: equality must be type-strict (string value == string query).
	if col.Type == modules_shared.ColumnTypeString || col.Type == modules_shared.ColumnTypeRangeString {
		if _, isFloat := value.(float64); isFloat {
			return false
		}
	}
	return cmp == 0
}

// rowCompare returns -1/0/1 for col[rowIdx] <=> value (for range comparisons).
// Returns (0, false) when the comparison is not applicable.
// NOTE-039: uses cmp3 to eliminate repeated three-way switch blocks per type.
func rowCompare(col *modules_reader.Column, rowIdx int, value interface{}) (int, bool) {
	if !col.IsPresent(rowIdx) {
		return 0, false
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			return rowCompareString(v, value)
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			switch t := value.(type) {
			case int64:
				return cmp3(v, t)
			case float64:
				return cmp3(float64(v), t)
			}
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			switch t := value.(type) {
			case int64:
				return cmp3(v, uint64(t)) //nolint:gosec
			case float64:
				return cmp3(float64(v), t)
			}
		}
	case modules_shared.ColumnTypeUUID:
		if v, ok := col.StringValue(rowIdx); ok {
			if s, ok2 := value.(string); ok2 {
				return cmp3(v, s)
			}
		}
	case modules_shared.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			return rowCompareBool(v, value)
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		if v, ok := col.BytesValue(rowIdx); ok {
			return rowCompareBytes(v, value)
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			switch t := value.(type) {
			case float64:
				return cmp3(v, t)
			case int64:
				return cmp3(v, float64(t))
			}
		}
	}
	return 0, false
}

// rowCompareString compares a stored string value against a query value (string or float64).
// For float64 targets, the stored string is parsed as float64 (LabelFilterStage semantics).
func rowCompareString(v string, value interface{}) (int, bool) {
	if s, ok := value.(string); ok {
		return cmp3(v, s)
	}
	if f, ok := value.(float64); ok {
		parsed, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return cmp3(parsed, f)
	}
	return 0, false
}

// rowCompareBool compares a stored bool value against a query bool (false < true).
func rowCompareBool(v bool, value interface{}) (int, bool) {
	b, ok := value.(bool)
	if !ok {
		return 0, false
	}
	switch {
	case v == b:
		return 0, true
	case !v: // false < true
		return -1, true
	default:
		return 1, true
	}
}

// rowCompareBytes compares a stored byte slice against a query []byte or string value.
func rowCompareBytes(v []byte, value interface{}) (int, bool) {
	switch t := value.(type) {
	case []byte:
		return cmp3(bytes.Compare(v, t), 0)
	case string:
		return cmp3(bytes.Compare(v, []byte(t)), 0)
	}
	return 0, false
}

// scanStringDictFloat is a fast path for numeric threshold comparisons against string-typed
// columns. It pre-parses the string dictionary as float64 once per block, marks which
// dictionary entries satisfy the threshold condition, then scans rows using array lookups.
// This eliminates per-row strconv.ParseFloat calls, replacing them with O(dictSize) parses
// and O(spanCount) boolean array lookups.
// NOTE-026: dictionary-level float parse for numeric threshold scans on string columns.
func scanStringDictFloat(
	col *modules_reader.Column,
	threshold float64,
	check func(v, t float64) bool,
	cb vm.RowCallback,
) int {
	// Ensure StringDict/StringIdx are populated. Predicate columns are always decoded
	// (they are in wantColumns), but call EnsureDecoded defensively — accessing
	// StringDict on a lazily-registered column returns empty slices, yielding 0 matches.
	col.EnsureDecoded()
	// Pre-parse the dictionary and mark which entries satisfy the condition.
	matches := make([]bool, len(col.StringDict))
	for i, s := range col.StringDict {
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			matches[i] = check(f, threshold)
		}
	}
	spanCount := col.SpanCount
	count := 0
	for i := range spanCount {
		if !col.IsPresent(i) {
			continue
		}
		if i >= len(col.StringIdx) {
			continue
		}
		di := int(col.StringIdx[i])
		if di < len(matches) && matches[di] {
			if !cb(i) {
				return count
			}
			count++
		}
	}
	return count
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
// Uses rowEqual (type-strict) not rowCompare, so float64 queries never coerce string
// column values via ParseFloat — that numeric coercion is only appropriate for GT/GTE/LT/LTE.
func (p *blockColumnProvider) StreamScanEqual(column string, value interface{}, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	n := p.scanWith(col, func(i int) bool { return rowEqual(col, i, value) }, cb)
	return n, nil
}

// StreamScanEqualAny scans all rows and calls cb for each row where column equals any of the
// given values. For string columns it uses a dict fast-path: build a bool mask over the string
// dictionary once (O(dict+nValues)), then scan rows via index lookups (O(spans)) — a single pass
// regardless of how many values are checked. For other column types it falls back to sequential
// rowEqual checks, which is still one pass over the rows (O(spans×nValues)) but avoids re-scanning
// the column N times.
func (p *blockColumnProvider) StreamScanEqualAny(column string, values []any, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) == 1 {
		return p.StreamScanEqual(column, values[0], cb)
	}

	// Dict fast-path for string columns: build a match mask over the dictionary once,
	// then scan rows via StringIdx with a single bool lookup per row.
	if col.Type == modules_shared.ColumnTypeString || col.Type == modules_shared.ColumnTypeRangeString {
		col.EnsureDecoded()
		// Build a set of wanted strings.
		wantSet := make(map[string]struct{}, len(values))
		for _, v := range values {
			if s, ok := v.(string); ok {
				wantSet[s] = struct{}{}
			}
		}
		if len(wantSet) == 0 {
			// All values are non-string; no string column can match.
			return 0, nil
		}
		// Build a per-dict-entry match mask.
		dictMatch := make([]bool, len(col.StringDict))
		for i, s := range col.StringDict {
			if _, ok := wantSet[s]; ok {
				dictMatch[i] = true
			}
		}
		spanCount := col.SpanCount
		count := 0
		for i := range spanCount {
			if !col.IsPresent(i) {
				continue
			}
			if i >= len(col.StringIdx) {
				continue
			}
			di := int(col.StringIdx[i])
			if di < len(dictMatch) && dictMatch[di] {
				if !cb(i) {
					return count, nil
				}
				count++
			}
		}
		return count, nil
	}

	// Generic fallback: one pass, checking each value per row.
	n := p.scanWith(col, func(i int) bool {
		for _, v := range values {
			if rowEqual(col, i, v) {
				return true
			}
		}
		return false
	}, cb)
	return n, nil
}

// ScanEqualAny returns a RowSet of all rows where column equals any of the given values.
func (p *blockColumnProvider) ScanEqualAny(column string, values []any) (vm.RowSet, error) {
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.StreamScanEqualAny(column, values, cb)
	})
}

// StreamScanNotEqual scans all rows and calls cb for each row where column does not equal value.
func (p *blockColumnProvider) StreamScanNotEqual(column string, value interface{}, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	n := p.block.SpanCount()
	count := 0
	for i := range n {
		// NotEqual includes null rows (they are not equal to anything).
		present := col != nil && col.IsPresent(i)
		if !present {
			if !cb(i) {
				return count, nil
			}
			count++
			continue
		}
		if !rowEqual(col, i, value) {
			if !cb(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// StreamScanLessThan scans all rows and calls cb for each row where column < value.
// NOTE-022: for string columns with float64 threshold, uses dict-level pre-parse fast path.
func (p *blockColumnProvider) StreamScanLessThan(column string, value interface{}, cb vm.RowCallback) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	if f, ok := value.(float64); ok {
		switch col.Type {
		case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
			return scanStringDictFloat(col, f, func(v, t float64) bool { return v < t }, cb), nil
		}
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp < 0
	}, cb)
	return n, nil
}

// StreamScanLessThanOrEqual scans all rows and calls cb for each row where column <= value.
// NOTE-022: for string columns with float64 threshold, uses dict-level pre-parse fast path.
func (p *blockColumnProvider) StreamScanLessThanOrEqual(
	column string,
	value interface{},
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	if f, ok := value.(float64); ok {
		switch col.Type {
		case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
			return scanStringDictFloat(col, f, func(v, t float64) bool { return v <= t }, cb), nil
		}
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp <= 0
	}, cb)
	return n, nil
}

// StreamScanGreaterThan scans all rows and calls cb for each row where column > value.
// NOTE-022: for string columns with float64 threshold, uses dict-level pre-parse fast path.
func (p *blockColumnProvider) StreamScanGreaterThan(
	column string,
	value interface{},
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	if f, ok := value.(float64); ok {
		switch col.Type {
		case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
			return scanStringDictFloat(col, f, func(v, t float64) bool { return v > t }, cb), nil
		}
	}
	n := p.scanWith(col, func(i int) bool {
		cmp, ok := rowCompare(col, i, value)
		return ok && cmp > 0
	}, cb)
	return n, nil
}

// StreamScanGreaterThanOrEqual scans all rows and calls cb for each row where column >= value.
// NOTE-022: for string columns with float64 threshold, uses dict-level pre-parse fast path.
func (p *blockColumnProvider) StreamScanGreaterThanOrEqual(
	column string,
	value interface{},
	cb vm.RowCallback,
) (int, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return 0, nil
	}
	if f, ok := value.(float64); ok {
		switch col.Type {
		case modules_shared.ColumnTypeRangeString, modules_shared.ColumnTypeString:
			return scanStringDictFloat(col, f, func(v, t float64) bool { return v >= t }, cb), nil
		}
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

// containsAnySubstring reports whether s contains any element of subs.
func containsAnySubstring(s string, subs []string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// streamScanRegexFast is the optimized scan: pre-compiled regex, optional prefix
// pre-filter, and batch string extraction via col.StringValues().
// When re is nil, prefixes are pre-lowercased CI literals and fold-contains is used
// instead of the regex engine (avoids NFA backtracking for (?i)literal patterns).
func (p *blockColumnProvider) streamScanRegexFast(
	col *modules_reader.Column, re *regexp.Regexp, prefixes []string, cb vm.RowCallback,
) (int, error) {
	// Regex applies only to string-typed columns. For other types (int64, bool, etc.),
	// StringValue returns ("", false) for present rows; without this guard, patterns like
	// ".*" would incorrectly match non-string values via the empty-string fallback.
	if col.Type != modules_shared.ColumnTypeString && col.Type != modules_shared.ColumnTypeUUID {
		return 0, nil
	}
	values := col.StringValues()
	count := 0
	if re == nil {
		// CI fold-contains path: prefixes are pre-lowercased by AnalyzeRegex.
		for i, v := range values {
			if v == "" && !col.IsPresent(i) {
				continue
			}
			if containsAnySubstring(strings.ToLower(v), prefixes) {
				if !cb(i) {
					return count, nil
				}
				count++
			}
		}
		return count, nil
	}
	for i, v := range values {
		if v == "" && !col.IsPresent(i) {
			continue // absent row
		}
		// Prefix pre-filter: skip regex if no prefix matches (safe — no false negatives).
		if len(prefixes) > 0 && !containsAnySubstring(v, prefixes) {
			continue
		}
		if re.MatchString(v) {
			if !cb(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanRegexNotMatchFast emits rows that do NOT match the pre-compiled regex.
// When re is nil, fold-contains with pre-lowercased prefixes is used (CI literal bypass).
func (p *blockColumnProvider) streamScanRegexNotMatchFast(
	col *modules_reader.Column, re *regexp.Regexp, prefixes []string, cb vm.RowCallback,
) (int, error) {
	count := 0
	n := p.block.SpanCount()
	// Regex applies only to string-typed columns. Non-string present rows have no string
	// value to match, so they always satisfy NOT MATCH.
	if col.Type != modules_shared.ColumnTypeString && col.Type != modules_shared.ColumnTypeUUID {
		for i := range n {
			if !cb(i) {
				return count, nil
			}
			count++
		}
		return count, nil
	}
	values := col.StringValues()
	if re == nil {
		// CI fold-contains path: prefixes are pre-lowercased by AnalyzeRegex.
		for i := range n {
			v := values[i]
			absent := v == "" && !col.IsPresent(i)
			matches := !absent && containsAnySubstring(strings.ToLower(v), prefixes)
			if !matches {
				if !cb(i) {
					return count, nil
				}
				count++
			}
		}
		return count, nil
	}
	for i := range n {
		v := values[i]
		absent := v == "" && !col.IsPresent(i)
		// Absent rows do not match regex → they satisfy NOT MATCH.
		matches := !absent && re.MatchString(v)
		if !matches {
			if !cb(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// NOTE-015: ScanRegexFast implements the three-layer regex optimization (pre-compiled regex,
// prefix pre-filter, flat batch string extraction). See executor/NOTES.md NOTE-015.
// ScanRegexFast returns a RowSet of rows where column matches the pre-compiled regex.
func (p *blockColumnProvider) ScanRegexFast(column string, re *regexp.Regexp, prefixes []string) (vm.RowSet, error) {
	col := p.lookupColumn(column)
	if col == nil {
		return &rowSet{}, nil
	}
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.streamScanRegexFast(col, re, prefixes, cb)
	})
}

// ScanRegexNotMatchFast returns a RowSet of rows where column does not match the pre-compiled regex.
func (p *blockColumnProvider) ScanRegexNotMatchFast(
	column string,
	re *regexp.Regexp,
	prefixes []string,
) (vm.RowSet, error) {
	col := p.lookupColumn(column)
	if col == nil {
		// No column → all rows are absent → all satisfy NOT MATCH.
		return p.FullScan(), nil
	}
	return collectStream(func(cb vm.RowCallback) (int, error) {
		return p.streamScanRegexNotMatchFast(col, re, prefixes, cb)
	})
}

// StreamFullScan calls cb for every row in the block.
func (p *blockColumnProvider) StreamFullScan(cb vm.RowCallback) (int, error) {
	n := p.block.SpanCount()
	count := 0
	for i := range n {
		if !cb(i) {
			return count, nil
		}
		count++
	}
	return count, nil
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
	case modules_shared.ColumnTypeUUID:
		// UUID columns are logically strings; return the formatted UUID string.
		if v, ok := col.StringValue(rowIdx); ok {
			return v, true, nil
		}
	}
	return nil, false, nil
}

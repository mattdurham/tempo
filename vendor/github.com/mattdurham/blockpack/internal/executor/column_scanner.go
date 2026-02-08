package executor

import (
	"bytes"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	blockpack "github.com/mattdurham/blockpack/internal/types"
)

// ColumnMatchResult tracks which rows matched a column scan condition
type ColumnMatchResult struct {
	ColumnName  string
	MatchedRows map[int]struct{} // row index -> matched (value not stored to save memory)
}

// NewColumnMatchResult creates a new empty match result
func NewColumnMatchResult(columnName string) *ColumnMatchResult {
	return &ColumnMatchResult{
		ColumnName:  columnName,
		MatchedRows: make(map[int]struct{}),
	}
}

// AddMatch records that a row matched
// Note: We don't store the value to save memory. The value is only needed during
// the scan itself, not after. This saves 600+ MB for queries with millions of matches.
func (cmr *ColumnMatchResult) AddMatch(rowIdx int, value any) {
	cmr.MatchedRows[rowIdx] = struct{}{}
}

// GetMatchedRowIndices returns a set of row indices that matched
func (cmr *ColumnMatchResult) GetMatchedRowIndices() map[int]bool {
	indices := make(map[int]bool, len(cmr.MatchedRows))
	for idx := range cmr.MatchedRows {
		indices[idx] = true
	}
	return indices
}

// SpanMatchInfo contains information about which columns contributed to a span's match
type SpanMatchInfo struct {
	RowIndex       int
	MatchedColumns map[string]any // column name -> value that matched
}

// NewSpanMatchInfo creates a new span match info
func NewSpanMatchInfo(rowIdx int) *SpanMatchInfo {
	return &SpanMatchInfo{
		RowIndex:       rowIdx,
		MatchedColumns: make(map[string]any),
	}
}

// AddMatchedColumn records that a column contributed to this span's match
func (smi *SpanMatchInfo) AddMatchedColumn(columnName string, value any) {
	smi.MatchedColumns[columnName] = value
}

// ColumnScanner scans a single column and returns matching rows
type ColumnScanner struct {
	block *blockpackio.Block
}

// NewColumnScanner creates a new column scanner
func NewColumnScanner(block *blockpackio.Block) *ColumnScanner {
	return &ColumnScanner{block: block}
}

// getColumnWithFallback tries to get a column, checking all possible names
func (cs *ColumnScanner) getColumnWithFallback(columnName string) *blockpack.Column {
	// Try all possible column names (new and old .attr. format)
	possibleNames := GetPossibleColumnNames(columnName)
	for _, name := range possibleNames {
		if col := cs.block.GetColumn(name); col != nil {
			return col
		}
	}
	return nil
}

// arrayValueMatches checks if an array value matches the target
func arrayValueMatches(av blockpack.ArrayValue, target any) bool {
	switch av.Type {
	case blockpack.ArrayTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			if val, err := strconv.ParseFloat(av.Str, 64); err == nil {
				return val == targetNumeric
			}
			return false
		}
		if targetStr, ok := target.(string); ok {
			return av.Str == targetStr
		}
	case blockpack.ArrayTypeInt64:
		if targetInt, ok := toInt64(target); ok {
			return av.Int == targetInt
		}
	case blockpack.ArrayTypeFloat64:
		if targetFloat, ok := toFloat64(target); ok {
			return av.Float == targetFloat
		}
	case blockpack.ArrayTypeBool:
		if targetBool, ok := target.(bool); ok {
			return av.Bool == targetBool
		}
	case blockpack.ArrayTypeBytes:
		if targetBytes, ok := toBytesOrString(target); ok {
			return bytes.Equal(av.Bytes, targetBytes)
		}
	case blockpack.ArrayTypeDuration:
		// Duration is stored as int64 nanoseconds
		if targetInt, ok := toInt64(target); ok {
			return av.Int == targetInt
		}
	}
	return false
}

// arrayValueLessThan checks if an array value is less than the target
func arrayValueLessThan(av blockpack.ArrayValue, target any) bool {
	switch av.Type {
	case blockpack.ArrayTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			if val, err := strconv.ParseFloat(av.Str, 64); err == nil {
				return val < targetNumeric
			}
			return false
		}
		if targetStr, ok := target.(string); ok {
			return av.Str < targetStr
		}
	case blockpack.ArrayTypeInt64:
		if targetInt, ok := toInt64(target); ok {
			return av.Int < targetInt
		}
	case blockpack.ArrayTypeFloat64:
		if targetFloat, ok := toFloat64(target); ok {
			return av.Float < targetFloat
		}
	case blockpack.ArrayTypeDuration:
		// Duration is stored as int64 nanoseconds
		if targetInt, ok := toInt64(target); ok {
			return av.Int < targetInt
		}
	case blockpack.ArrayTypeBool, blockpack.ArrayTypeBytes:
		// Bool and bytes don't support < comparison
		return false
	}
	return false
}

// arrayValueGreaterThan checks if an array value is greater than the target
func arrayValueGreaterThan(av blockpack.ArrayValue, target any) bool {
	switch av.Type {
	case blockpack.ArrayTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			if val, err := strconv.ParseFloat(av.Str, 64); err == nil {
				return val > targetNumeric
			}
			return false
		}
		if targetStr, ok := target.(string); ok {
			return av.Str > targetStr
		}
	case blockpack.ArrayTypeInt64:
		if targetInt, ok := toInt64(target); ok {
			return av.Int > targetInt
		}
	case blockpack.ArrayTypeFloat64:
		if targetFloat, ok := toFloat64(target); ok {
			return av.Float > targetFloat
		}
	case blockpack.ArrayTypeDuration:
		// Duration is stored as int64 nanoseconds
		if targetInt, ok := toInt64(target); ok {
			return av.Int > targetInt
		}
	case blockpack.ArrayTypeBool, blockpack.ArrayTypeBytes:
		// Bool and bytes don't support > comparison
		return false
	}
	return false
}

// arrayValueLessThanOrEqual checks if an array value is less than or equal to the target
func arrayValueLessThanOrEqual(av blockpack.ArrayValue, target any) bool {
	switch av.Type {
	case blockpack.ArrayTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			if val, err := strconv.ParseFloat(av.Str, 64); err == nil {
				return val <= targetNumeric
			}
			return false
		}
		if targetStr, ok := target.(string); ok {
			return av.Str <= targetStr
		}
	case blockpack.ArrayTypeInt64:
		if targetInt, ok := toInt64(target); ok {
			return av.Int <= targetInt
		}
	case blockpack.ArrayTypeFloat64:
		if targetFloat, ok := toFloat64(target); ok {
			return av.Float <= targetFloat
		}
	case blockpack.ArrayTypeDuration:
		// Duration is stored as int64 nanoseconds
		if targetInt, ok := toInt64(target); ok {
			return av.Int <= targetInt
		}
	case blockpack.ArrayTypeBool, blockpack.ArrayTypeBytes:
		// Bool and bytes don't support <= comparison
		return false
	}
	return false
}

// arrayValueGreaterThanOrEqual checks if an array value is greater than or equal to the target
func arrayValueGreaterThanOrEqual(av blockpack.ArrayValue, target any) bool {
	switch av.Type {
	case blockpack.ArrayTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			if val, err := strconv.ParseFloat(av.Str, 64); err == nil {
				return val >= targetNumeric
			}
			return false
		}
		if targetStr, ok := target.(string); ok {
			return av.Str >= targetStr
		}
	case blockpack.ArrayTypeInt64:
		if targetInt, ok := toInt64(target); ok {
			return av.Int >= targetInt
		}
	case blockpack.ArrayTypeFloat64:
		if targetFloat, ok := toFloat64(target); ok {
			return av.Float >= targetFloat
		}
	case blockpack.ArrayTypeDuration:
		// Duration is stored as int64 nanoseconds
		if targetInt, ok := toInt64(target); ok {
			return av.Int >= targetInt
		}
	case blockpack.ArrayTypeBool, blockpack.ArrayTypeBytes:
		// Bool and bytes don't support >= comparison
		return false
	}
	return false
}

// arrayValueMatchesRegex checks if a string array value matches the regex pattern
func arrayValueMatchesRegex(av blockpack.ArrayValue, re *regexp.Regexp) bool {
	if av.Type == blockpack.ArrayTypeString {
		return re.MatchString(av.Str)
	}
	return false
}

// ScanEqual scans a column for rows where value equals the target
func (cs *ColumnScanner) ScanEqual(columnName string, target any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	// For unscoped attributes, check all possible column names
	possibleColumns := GetPossibleColumnNames(columnName)

	// Track which rows have matched across all possible columns (to avoid duplicates)
	matchedRows := make(map[int]bool)

	for _, colName := range possibleColumns {
		col := cs.block.GetColumn(colName)
		if col == nil {
			continue
		}

		// Scan this column and add matches
		if err := cs.scanSingleColumnEqual(col, colName, target, matchedRows, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// scanStringColumn scans a string column for equality matches.
func (cs *ColumnScanner) scanStringColumn(col *blockpack.Column, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	if targetNumeric, ok := numericTarget(target); ok {
		for i := 0; i < cs.block.SpanCount(); i++ {
			if matchedRows[i] {
				continue
			}
			val, ok := col.StringValue(i)
			if !ok {
				continue
			}
			if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed == targetNumeric {
				result.AddMatch(i, val)
				matchedRows[i] = true
			}
		}
		return nil
	}
	targetStr, ok := target.(string)
	if !ok {
		return nil
	}
	targetDictIdx, found := col.FindInStringDict(targetStr)
	if !found {
		return nil
	}
	dict, indices := col.StringDict()
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] && i < len(indices) {
			if col.IsPresent(i) && indices[i] == targetDictIdx {
				result.AddMatch(i, dict[targetDictIdx])
				matchedRows[i] = true
			}
		}
	}
	return nil
}

// scanInt64Column scans an int64 column for equality matches.
func (cs *ColumnScanner) scanInt64Column(col *blockpack.Column, columnName string, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	targetInt, ok := toInt64(target)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
	}
	targetDictIdx, found := col.FindInInt64Dict(targetInt)
	if !found {
		return nil
	}
	dict, indices := col.Int64Dict()
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] && i < len(indices) {
			if col.IsPresent(i) && indices[i] == targetDictIdx {
				result.AddMatch(i, dict[targetDictIdx])
				matchedRows[i] = true
			}
		}
	}
	return nil
}

// scanUint64Column scans a uint64 column for equality matches.
func (cs *ColumnScanner) scanUint64Column(col *blockpack.Column, columnName string, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	targetUint, ok := toUint64(target)
	if !ok {
		return fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
	}
	targetDictIdx, found := col.FindInUint64Dict(targetUint)
	if !found {
		return nil
	}
	dict, indices := col.Uint64Dict()
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] && i < len(indices) {
			if col.IsPresent(i) && indices[i] == targetDictIdx {
				result.AddMatch(i, dict[targetDictIdx])
				matchedRows[i] = true
			}
		}
	}
	return nil
}

// scanBoolColumn scans a boolean column for equality matches.
func (cs *ColumnScanner) scanBoolColumn(col *blockpack.Column, columnName string, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	targetBool, ok := target.(bool)
	if !ok {
		return fmt.Errorf("type mismatch: expected bool for column %s", columnName)
	}
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] {
			if val, ok := col.BoolValue(i); ok && val == targetBool {
				result.AddMatch(i, val)
				matchedRows[i] = true
			}
		}
	}
	return nil
}

// scanFloat64Column scans a float64 column for equality matches.
func (cs *ColumnScanner) scanFloat64Column(col *blockpack.Column, columnName string, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
	}
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] {
			if val, ok := col.Float64Value(i); ok && val == targetFloat {
				result.AddMatch(i, val)
				matchedRows[i] = true
			}
		}
	}
	return nil
}

// scanBytesColumn scans a bytes column for equality matches.
func (cs *ColumnScanner) scanBytesColumn(col *blockpack.Column, columnName string, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	isRawBytesColumn := columnName == "trace:id" ||
		columnName == "span:id" ||
		columnName == "span:parent_id" ||
		columnName == "span:trace_state"

	for i := 0; i < cs.block.SpanCount(); i++ {
		if matchedRows[i] {
			continue
		}

		val, ok := col.BytesValueView(i)
		if !ok {
			continue
		}

		if isRawBytesColumn {
			targetBytes, ok := toBytesOrString(target)
			if !ok {
				return fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
			}
			if bytes.Equal(val, targetBytes) {
				result.AddMatch(i, val)
				matchedRows[i] = true
			}
		} else {
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueMatches(av, target) {
						matched = true
						break
					}
				}
				if matched {
					result.AddMatch(i, val)
					matchedRows[i] = true
				}
			} else {
				targetBytes, ok := toBytesOrString(target)
				if !ok {
					return fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
				}
				if bytes.Equal(val, targetBytes) {
					result.AddMatch(i, val)
					matchedRows[i] = true
				}
			}
		}
	}
	return nil
}

// scanSingleColumnEqual scans a single column for matching values and adds to result
func (cs *ColumnScanner) scanSingleColumnEqual(col *blockpack.Column, columnName string, target any, matchedRows map[int]bool, result *ColumnMatchResult) error {
	switch col.Type {
	case blockpack.ColumnTypeString:
		return cs.scanStringColumn(col, target, matchedRows, result)
	case blockpack.ColumnTypeInt64:
		return cs.scanInt64Column(col, columnName, target, matchedRows, result)
	case blockpack.ColumnTypeUint64:
		return cs.scanUint64Column(col, columnName, target, matchedRows, result)
	case blockpack.ColumnTypeBool:
		return cs.scanBoolColumn(col, columnName, target, matchedRows, result)
	case blockpack.ColumnTypeFloat64:
		return cs.scanFloat64Column(col, columnName, target, matchedRows, result)
	case blockpack.ColumnTypeBytes:
		return cs.scanBytesColumn(col, columnName, target, matchedRows, result)
	default:
		return fmt.Errorf("unsupported column type for %s", columnName)
	}
}

// ScanLessThan scans a column for rows where value < target
func (cs *ColumnScanner) ScanLessThan(columnName string, target any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.getColumnWithFallback(columnName)
	if col == nil {
		return result, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			for i := 0; i < cs.block.SpanCount(); i++ {
				if val, ok := col.StringValue(i); ok {
					if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed < targetNumeric {
						result.AddMatch(i, val)
					}
				}
			}
			return result, nil
		}
		targetStr, ok := target.(string)
		if !ok {
			// Type mismatch - no matches (e.g., comparing string column to int)
			return result, nil
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && val < targetStr {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeInt64:
		targetInt, ok := toInt64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Int64Value(i); ok && val < targetInt {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeUint64:
		targetUint, ok := toUint64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Uint64Value(i); ok && val < targetUint {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeFloat64:
		targetFloat, ok := toFloat64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Float64Value(i); ok && val < targetFloat {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBytes:
		// Bytes columns may contain array-encoded data (event.*, link.*, instrumentation.*, attributes)
		// Try to decode as array and compare array values
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			// Try to decode as array
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueLessThan(av, target) {
						matched = true
						break
					}
				}
				if matched {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeBool:
		// Bool columns don't support < comparison
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported column type for < comparison on %s", columnName)
	}

	return result, nil
}

// ScanGreaterThan scans a column for rows where value > target
func (cs *ColumnScanner) ScanGreaterThan(columnName string, target any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.getColumnWithFallback(columnName)
	if col == nil {
		return result, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			for i := 0; i < cs.block.SpanCount(); i++ {
				if val, ok := col.StringValue(i); ok {
					if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed > targetNumeric {
						result.AddMatch(i, val)
					}
				}
			}
			return result, nil
		}
		targetStr, ok := target.(string)
		if !ok {
			// Type mismatch - no matches (e.g., comparing string column to int)
			return result, nil
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && val > targetStr {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeInt64:
		targetInt, ok := toInt64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Int64Value(i); ok && val > targetInt {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeUint64:
		targetUint, ok := toUint64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Uint64Value(i); ok && val > targetUint {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeFloat64:
		targetFloat, ok := toFloat64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Float64Value(i); ok && val > targetFloat {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBytes:
		// Bytes columns may contain array-encoded data (event.*, link.*, instrumentation.*, attributes)
		// Try to decode as array and compare array values
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			// Try to decode as array
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueGreaterThan(av, target) {
						matched = true
						break
					}
				}
				if matched {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeBool:
		// Bool columns don't support > comparison
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported column type for > comparison on %s", columnName)
	}

	return result, nil
}

// ScanGreaterThanOrEqual scans a column for rows where value >= target
func (cs *ColumnScanner) ScanGreaterThanOrEqual(columnName string, target any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.getColumnWithFallback(columnName)
	if col == nil {
		return result, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			for i := 0; i < cs.block.SpanCount(); i++ {
				if val, ok := col.StringValue(i); ok {
					if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed >= targetNumeric {
						result.AddMatch(i, val)
					}
				}
			}
			return result, nil
		}
		targetStr, ok := target.(string)
		if !ok {
			// Type mismatch - no matches (e.g., comparing string column to int)
			return result, nil
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && val >= targetStr {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeInt64:
		targetInt, ok := toInt64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Int64Value(i); ok && val >= targetInt {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeUint64:
		targetUint, ok := toUint64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Uint64Value(i); ok && val >= targetUint {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeFloat64:
		targetFloat, ok := toFloat64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Float64Value(i); ok && val >= targetFloat {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBytes:
		// Bytes columns may contain array-encoded data (event.*, link.*, instrumentation.*, attributes)
		// Try to decode as array and compare array values
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			// Try to decode as array
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueGreaterThanOrEqual(av, target) {
						matched = true
						break
					}
				}
				if matched {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeBool:
		// Bool columns don't support >= comparison
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported column type for >= comparison on %s", columnName)
	}

	return result, nil
}

// ScanLessThanOrEqual scans a column for rows where value <= target
func (cs *ColumnScanner) ScanLessThanOrEqual(columnName string, target any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.getColumnWithFallback(columnName)
	if col == nil {
		return result, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			for i := 0; i < cs.block.SpanCount(); i++ {
				if val, ok := col.StringValue(i); ok {
					if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed <= targetNumeric {
						result.AddMatch(i, val)
					}
				}
			}
			return result, nil
		}
		targetStr, ok := target.(string)
		if !ok {
			// Type mismatch - no matches (e.g., comparing string column to int)
			return result, nil
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && val <= targetStr {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeInt64:
		targetInt, ok := toInt64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Int64Value(i); ok && val <= targetInt {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeUint64:
		targetUint, ok := toUint64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Uint64Value(i); ok && val <= targetUint {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeFloat64:
		targetFloat, ok := toFloat64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Float64Value(i); ok && val <= targetFloat {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBytes:
		// Bytes columns may contain array-encoded data (event.*, link.*, instrumentation.*, attributes)
		// Try to decode as array and compare array values
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			// Try to decode as array
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueLessThanOrEqual(av, target) {
						matched = true
						break
					}
				}
				if matched {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeBool:
		// Bool columns don't support <= comparison
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported column type for <= comparison on %s", columnName)
	}

	return result, nil
}

// ScanNotEqual scans a column for rows where value != target
func (cs *ColumnScanner) ScanNotEqual(columnName string, target any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		if targetNumeric, ok := numericTarget(target); ok {
			for i := 0; i < cs.block.SpanCount(); i++ {
				val, ok := col.StringValue(i)
				if !ok {
					continue
				}
				if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed != targetNumeric {
					result.AddMatch(i, val)
				}
			}
			return result, nil
		}
		targetStr, ok := target.(string)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected string for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && val != targetStr {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeInt64:
		targetInt, ok := toInt64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Int64Value(i); ok && val != targetInt {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeUint64:
		targetUint, ok := toUint64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Uint64Value(i); ok && val != targetUint {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBool:
		targetBool, ok := target.(bool)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected bool for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.BoolValue(i); ok && val != targetBool {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeFloat64:
		targetFloat, ok := toFloat64(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.Float64Value(i); ok && val != targetFloat {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBytes:
		targetBytes, ok := toBytesOrString(target)
		if !ok {
			return nil, fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
		}
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.BytesValue(i); ok && !bytes.Equal(val, targetBytes) {
				result.AddMatch(i, val)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported column type for %s", columnName)
	}

	return result, nil
}

// ScanRegex scans a column for rows where value matches the regex pattern
func (cs *ColumnScanner) ScanRegex(columnName string, pattern string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		// OPTIMIZATION: Use dictionary-based filtering for string columns
		// Instead of matching regex against every row (potentially 100K+ rows),
		// match against dictionary entries (typically 10-100 entries), then
		// scan rows using the matched dictionary indices.
		dict, indices := col.StringDict()
		if len(dict) > 0 {
			// Step 1: Match regex against dictionary entries only
			matchedDictIndices := make(map[uint32]bool, len(dict))
			for i, dictValue := range dict {
				if re.MatchString(dictValue) {
					matchedDictIndices[uint32(i)] = true
				}
			}

			// Early exit if no dictionary entries match
			if len(matchedDictIndices) == 0 {
				return result, nil
			}

			// Fast path: if ALL dictionary entries match, return all rows
			if len(matchedDictIndices) == len(dict) {
				for i := 0; i < cs.block.SpanCount(); i++ {
					if col.IsPresent(i) && i < len(indices) {
						dictIdx := indices[i]
						if int(dictIdx) < len(dict) {
							result.AddMatch(i, dict[dictIdx])
						}
					}
				}
				return result, nil
			}

			// Step 2: Fast scan using dictionary indices
			// This is much faster than calling StringValue() for each row
			for i := 0; i < cs.block.SpanCount(); i++ {
				if col.IsPresent(i) && i < len(indices) {
					dictIdx := indices[i]
					if matchedDictIndices[dictIdx] && int(dictIdx) < len(dict) {
						result.AddMatch(i, dict[dictIdx])
					}
				}
			}
		} else {
			// Fallback to row-by-row scanning if no dictionary
			for i := 0; i < cs.block.SpanCount(); i++ {
				if val, ok := col.StringValue(i); ok && re.MatchString(val) {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeBytes:
		// Bytes columns may contain array-encoded data with string values
		// (event.*, link.*, instrumentation.*, resource.*, span.*, span:name)
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			// Try to decode as array and match string values
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueMatchesRegex(av, re) {
						matched = true
						break
					}
				}
				if matched {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeUint64,
		blockpack.ColumnTypeBool, blockpack.ColumnTypeFloat64:
		return nil, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)

	default:
		return nil, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)
	}

	return result, nil
}

// ScanRegexNotMatch scans a column for rows where value does NOT match the regex pattern
func (cs *ColumnScanner) ScanRegexNotMatch(columnName string, pattern string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
	}

	switch col.Type {
	case blockpack.ColumnTypeString:
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && !re.MatchString(val) {
				result.AddMatch(i, val)
			}
		}

	case blockpack.ColumnTypeBytes:
		// Bytes columns may contain array-encoded data with string values
		// For !~ operator, match if NO array value matches the regex
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			// Try to decode as array and check if NO string values match
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueMatchesRegex(av, re) {
						matched = true
						break
					}
				}
				// Add match if NO array values matched the regex
				if !matched {
					result.AddMatch(i, val)
				}
			}
		}

	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeUint64,
		blockpack.ColumnTypeBool, blockpack.ColumnTypeFloat64:
		return nil, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)

	default:
		return nil, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)
	}

	return result, nil
}

// ScanContains scans a column for rows where string value contains the substring
func (cs *ColumnScanner) ScanContains(columnName string, substring string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("contains matching only supported for string columns, got %s", columnName)
	}

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && strings.Contains(val, substring) {
			result.AddMatch(i, val)
		}
	}

	return result, nil
}

// ScanIsNull scans a column for rows where the value is NULL (not present)
func (cs *ColumnScanner) ScanIsNull(columnName string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		// Column doesn't exist - all rows are NULL for this column
		for i := 0; i < cs.block.SpanCount(); i++ {
			result.AddMatch(i, nil)
		}
		return result, nil
	}

	// Check each row for NULL values (not present in the column)
	for i := 0; i < cs.block.SpanCount(); i++ {
		var isNull bool
		switch col.Type {
		case blockpack.ColumnTypeString:
			_, isNull = col.StringValue(i)
		case blockpack.ColumnTypeInt64:
			_, isNull = col.Int64Value(i)
		case blockpack.ColumnTypeUint64:
			_, isNull = col.Uint64Value(i)
		case blockpack.ColumnTypeBool:
			_, isNull = col.BoolValue(i)
		case blockpack.ColumnTypeFloat64:
			_, isNull = col.Float64Value(i)
		case blockpack.ColumnTypeBytes:
			_, isNull = col.BytesValue(i)
		}
		if !isNull {
			result.AddMatch(i, nil)
		}
	}

	return result, nil
}

// ScanIsNotNull scans a column for rows where the value is NOT NULL (present)
func (cs *ColumnScanner) ScanIsNotNull(columnName string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		// Column doesn't exist - no rows have non-NULL values
		return result, nil
	}

	// Check each row for non-NULL values (present in the column)
	for i := 0; i < cs.block.SpanCount(); i++ {
		var value any
		var hasValue bool
		switch col.Type {
		case blockpack.ColumnTypeString:
			value, hasValue = col.StringValue(i)
		case blockpack.ColumnTypeInt64:
			value, hasValue = col.Int64Value(i)
		case blockpack.ColumnTypeUint64:
			value, hasValue = col.Uint64Value(i)
		case blockpack.ColumnTypeBool:
			value, hasValue = col.BoolValue(i)
		case blockpack.ColumnTypeFloat64:
			value, hasValue = col.Float64Value(i)
		case blockpack.ColumnTypeBytes:
			value, hasValue = col.BytesValue(i)
		}
		if hasValue {
			result.AddMatch(i, value)
		}
	}

	return result, nil
}

// Helper functions for type conversions
func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case uint64:
		return int64(val), true
	case string:
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			return parsed, true
		}
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			if math.Trunc(parsed) == parsed {
				return int64(parsed), true
			}
		}
	default:
		return 0, false
	}
	return 0, false
}

func toUint64(v any) (uint64, bool) {
	switch val := v.(type) {
	case uint64:
		return val, true
	case int64:
		if val >= 0 {
			return uint64(val), true
		}
		return 0, false
	case int:
		if val >= 0 {
			return uint64(val), true
		}
		return 0, false
	case string:
		if parsed, err := strconv.ParseUint(val, 10, 64); err == nil {
			return parsed, true
		}
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			if parsed >= 0 && math.Trunc(parsed) == parsed {
				return uint64(parsed), true
			}
		}
	default:
		return 0, false
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case uint64:
		return float64(val), true
	case int:
		return float64(val), true
	case string:
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed, true
		}
	default:
		return 0, false
	}
	return 0, false
}

func numericTarget(v any) (float64, bool) {
	switch val := v.(type) {
	case int64:
		return float64(val), true
	case int:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float64:
		return val, true
	case string:
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func toBytesOrString(v any) ([]byte, bool) {
	switch val := v.(type) {
	case []byte:
		return val, true
	case string:
		return []byte(val), true
	default:
		return nil, false
	}
}

// ScanStartsWith scans a string column for rows where value starts with prefix
func (cs *ColumnScanner) ScanStartsWith(columnName string, prefix string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("StartsWith only supported for string columns, got %s", columnName)
	}

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && strings.HasPrefix(val, prefix) {
			result.AddMatch(i, val)
		}
	}

	return result, nil
}

// ScanEndsWith scans a string column for rows where value ends with suffix
func (cs *ColumnScanner) ScanEndsWith(columnName string, suffix string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("EndsWith only supported for string columns, got %s", columnName)
	}

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && strings.HasSuffix(val, suffix) {
			result.AddMatch(i, val)
		}
	}

	return result, nil
}

// ScanStringLength scans a string column for rows where string length matches the condition
func (cs *ColumnScanner) ScanStringLength(
	columnName string, op ComparisonOp, length int64,
) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("StringLength only supported for string columns, got %s", columnName)
	}

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok {
			strLen := int64(len(val))
			matched := false

			switch op {
			case ComparisonEqual:
				matched = strLen == length
			case ComparisonNotEqual:
				matched = strLen != length
			case ComparisonLessThan:
				matched = strLen < length
			case ComparisonLessThanOrEqual:
				matched = strLen <= length
			case ComparisonGreaterThan:
				matched = strLen > length
			case ComparisonGreaterThanOrEqual:
				matched = strLen >= length
			}

			if matched {
				result.AddMatch(i, val)
			}
		}
	}

	return result, nil
}

// ScanStringLower scans a string column for rows where lowercase value equals target
func (cs *ColumnScanner) ScanStringLower(columnName string, target string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("StringLower only supported for string columns, got %s", columnName)
	}

	targetLower := strings.ToLower(target)

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && strings.EqualFold(val, targetLower) {
			result.AddMatch(i, val)
		}
	}

	return result, nil
}

// ScanStringUpper scans a string column for rows where uppercase value equals target
func (cs *ColumnScanner) ScanStringUpper(columnName string, target string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("StringUpper only supported for string columns, got %s", columnName)
	}

	targetUpper := strings.ToUpper(target)

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && strings.EqualFold(val, targetUpper) {
			result.AddMatch(i, val)
		}
	}

	return result, nil
}

// ScanSubstring scans a string column for rows where substring matches target
func (cs *ColumnScanner) ScanSubstring(
	columnName string, start, length int64, target string,
) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("substring only supported for string columns, got %s", columnName)
	}

	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok {
			if start >= 0 && start < int64(len(val)) {
				end := min(start+length, int64(len(val)))
				substr := val[start:end]
				if substr == target {
					result.AddMatch(i, val)
				}
			}
		}
	}

	return result, nil
}

// ComparisonOp represents a comparison operator for numeric/length comparisons
type ComparisonOp int

// ComparisonOp values define the various comparison operations supported
const (
	ComparisonEqual ComparisonOp = iota
	ComparisonNotEqual
	ComparisonLessThan
	ComparisonLessThanOrEqual
	ComparisonGreaterThan
	ComparisonGreaterThanOrEqual
)

// ScanJSONExists scans a string column containing JSON for rows where the JSON path exists
// This is a simplified implementation - a full implementation would parse JSON and evaluate paths
func (cs *ColumnScanner) ScanJSONExists(columnName string, path string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("JSONExists only supported for string columns, got %s", columnName)
	}

	// Simple implementation: check if path substring exists in JSON
	// A full implementation would use a JSON parser and path evaluator
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok {
			// Simple heuristic: check if the path key exists in the JSON string
			// This is a placeholder - proper implementation would parse JSON
			if strings.Contains(val, path) {
				result.AddMatch(i, val)
			}
		}
	}

	return result, nil
}

// ScanJSONContains scans a string column containing JSON for rows where JSON contains a value
// This is a simplified implementation
func (cs *ColumnScanner) ScanJSONContains(columnName string, searchValue string) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("JSONContains only supported for string columns, got %s", columnName)
	}

	// Simple implementation: check if search value exists in JSON string
	// A full implementation would parse JSON and check containment semantically
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok {
			if strings.Contains(val, searchValue) {
				result.AddMatch(i, val)
			}
		}
	}

	return result, nil
}

// ScanJSONPath scans a string column containing JSON for rows where JSONPath query matches
// This is a simplified implementation - full implementation would use jsonpath library
func (cs *ColumnScanner) ScanJSONPath(columnName string, path string, op ComparisonOp, value any) (*ColumnMatchResult, error) {
	result := NewColumnMatchResult(columnName)

	col := cs.block.GetColumn(columnName)
	if col == nil {
		return result, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return nil, fmt.Errorf("JSONPath only supported for string columns, got %s", columnName)
	}

	// Placeholder implementation
	// Full implementation would:
	// 1. Parse JSON string to object
	// 2. Evaluate JSONPath expression
	// 3. Compare result with value using op
	// For now, return empty result (not implemented)
	return result, nil
}

// SetOperations provides OR and AND operations on match results
type SetOperations struct{}

// OR combines multiple match results with OR logic
// Returns a map of row index -> list of (columnName, value) pairs that matched
func (so *SetOperations) OR(results ...*ColumnMatchResult) map[int]*SpanMatchInfo {
	combined := make(map[int]*SpanMatchInfo)

	for _, result := range results {
		for rowIdx := range result.MatchedRows {
			if _, exists := combined[rowIdx]; !exists {
				combined[rowIdx] = NewSpanMatchInfo(rowIdx)
			}
			combined[rowIdx].AddMatchedColumn(result.ColumnName, nil)
		}
	}

	return combined
}

// AND combines multiple match results with AND logic
// Only includes rows that appear in ALL results
func (so *SetOperations) AND(results ...*ColumnMatchResult) map[int]*SpanMatchInfo {
	if len(results) == 0 {
		return make(map[int]*SpanMatchInfo)
	}

	// Start with first result's rows
	combined := make(map[int]*SpanMatchInfo)
	for rowIdx := range results[0].MatchedRows {
		combined[rowIdx] = NewSpanMatchInfo(rowIdx)
		combined[rowIdx].AddMatchedColumn(results[0].ColumnName, nil)
	}

	// Intersect with remaining results
	for _, result := range results[1:] {
		// Remove rows that don't appear in this result
		for rowIdx := range combined {
			if _, exists := result.MatchedRows[rowIdx]; exists {
				combined[rowIdx].AddMatchedColumn(result.ColumnName, nil)
			} else {
				delete(combined, rowIdx)
			}
		}
	}

	return combined
}

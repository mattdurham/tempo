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
	"github.com/mattdurham/blockpack/internal/vm"
)

// ColumnScanner scans a single column and returns matching rows
//
// Thread Safety: NOT safe for concurrent use. Each goroutine should create
// its own ColumnScanner instance. The underlying block data is immutable after
// creation, so multiple scanners can safely operate on the same block concurrently.
// Pattern: One scanner per goroutine, multiple goroutines can share the same block.
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

// Helper functions for type conversions
func toInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case uint64:
		return int64(val), true //nolint:gosec
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

// StreamScanEqual scans for rows where column equals value, calling callback for each match
// This avoids RowSet allocation by calling the callback directly during scanning
func (cs *ColumnScanner) StreamScanEqual(columnName string, target any, callback vm.RowCallback) (int, error) {
	// For unscoped attributes, check all possible column names
	possibleColumns := GetPossibleColumnNames(columnName)

	// Track which rows have matched to avoid duplicates when checking multiple columns
	// Capacity hint with upper bound to prevent large allocations on big blocks
	capacity := cs.block.SpanCount() / 10
	if capacity > 1000 {
		capacity = 1000
	}
	matchedRows := make(map[int]bool, capacity)
	count := 0

	for _, colName := range possibleColumns {
		col := cs.block.GetColumn(colName)
		if col == nil {
			continue
		}

		// Scan this column and call callback for matches
		err := cs.streamScanSingleColumnEqual(col, colName, target, matchedRows, &count, callback)
		if err != nil {
			return count, err
		}
	}

	return count, nil
}

// streamScanStringColumn scans a string column for equality matches.
func (cs *ColumnScanner) streamScanStringColumn(
	col *blockpack.Column,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
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
				matchedRows[i] = true
				if !callback(i) {
					return nil
				}
				*count++
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
	_, indices := col.StringDict()
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] && i < len(indices) {
			if col.IsPresent(i) && indices[i] == targetDictIdx {
				matchedRows[i] = true
				if !callback(i) {
					return nil
				}
				*count++
			}
		}
	}
	return nil
}

// streamScanInt64Column scans an int64 column for equality matches.
func (cs *ColumnScanner) streamScanInt64Column(
	col *blockpack.Column,
	columnName string,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
	targetInt, ok := toInt64(target)
	if !ok {
		return fmt.Errorf("type mismatch: expected int64 for column %s", columnName)
	}
	targetDictIdx, found := col.FindInInt64Dict(targetInt)
	if !found {
		return nil
	}
	_, indices := col.Int64Dict()
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] && i < len(indices) {
			if col.IsPresent(i) && indices[i] == targetDictIdx {
				matchedRows[i] = true
				if !callback(i) {
					return nil
				}
				*count++
			}
		}
	}
	return nil
}

// streamScanUint64Column scans a uint64 column for equality matches.
func (cs *ColumnScanner) streamScanUint64Column(
	col *blockpack.Column,
	columnName string,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
	targetUint, ok := toUint64(target)
	if !ok {
		return fmt.Errorf("type mismatch: expected uint64 for column %s", columnName)
	}
	targetDictIdx, found := col.FindInUint64Dict(targetUint)
	if !found {
		return nil
	}
	_, indices := col.Uint64Dict()
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] && i < len(indices) {
			if col.IsPresent(i) && indices[i] == targetDictIdx {
				matchedRows[i] = true
				if !callback(i) {
					return nil
				}
				*count++
			}
		}
	}
	return nil
}

// streamScanBoolColumn scans a boolean column for equality matches.
func (cs *ColumnScanner) streamScanBoolColumn(
	col *blockpack.Column,
	columnName string,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
	targetBool, ok := target.(bool)
	if !ok {
		return fmt.Errorf("type mismatch: expected bool for column %s", columnName)
	}
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] {
			if val, ok := col.BoolValue(i); ok && val == targetBool {
				matchedRows[i] = true
				if !callback(i) {
					return nil
				}
				*count++
			}
		}
	}
	return nil
}

// streamScanFloat64Column scans a float64 column for equality matches.
func (cs *ColumnScanner) streamScanFloat64Column(
	col *blockpack.Column,
	columnName string,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return fmt.Errorf("type mismatch: expected float64 for column %s", columnName)
	}
	for i := 0; i < cs.block.SpanCount(); i++ {
		if !matchedRows[i] {
			if val, ok := col.Float64Value(i); ok && val == targetFloat {
				matchedRows[i] = true
				if !callback(i) {
					return nil
				}
				*count++
			}
		}
	}
	return nil
}

// streamScanBytesColumn scans a bytes column for equality matches.
func (cs *ColumnScanner) streamScanBytesColumn(
	col *blockpack.Column,
	columnName string,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
	isRawBytesColumn := columnName == IntrinsicTraceID ||
		columnName == IntrinsicSpanID ||
		columnName == IntrinsicSpanParentID ||
		columnName == IntrinsicSpanTraceState

	for i := 0; i < cs.block.SpanCount(); i++ {
		if matchedRows[i] {
			continue
		}

		val, ok := col.BytesValueView(i)
		if !ok {
			continue
		}

		matched := false
		if isRawBytesColumn {
			targetBytes, ok := toBytesOrString(target)
			if !ok {
				return fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
			}
			if bytes.Equal(val, targetBytes) {
				matched = true
			}
		} else {
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				for _, av := range arrayValues {
					if arrayValueMatches(av, target) {
						matched = true
						break
					}
				}
			} else {
				targetBytes, ok := toBytesOrString(target)
				if !ok {
					return fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
				}
				if bytes.Equal(val, targetBytes) {
					matched = true
				}
			}
		}

		if matched {
			matchedRows[i] = true
			if !callback(i) {
				return nil
			}
			*count++
		}
	}
	return nil
}

// streamScanSingleColumnEqual scans a single column for matching values and calls callback
func (cs *ColumnScanner) streamScanSingleColumnEqual(
	col *blockpack.Column,
	columnName string,
	target any,
	matchedRows map[int]bool,
	count *int,
	callback vm.RowCallback,
) error {
	switch col.Type {
	case blockpack.ColumnTypeString, blockpack.ColumnTypeRangeString:
		return cs.streamScanStringColumn(col, target, matchedRows, count, callback)
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeRangeInt64:
		return cs.streamScanInt64Column(col, columnName, target, matchedRows, count, callback)
	case blockpack.ColumnTypeUint64, blockpack.ColumnTypeRangeUint64, blockpack.ColumnTypeRangeDuration:
		return cs.streamScanUint64Column(col, columnName, target, matchedRows, count, callback)
	case blockpack.ColumnTypeBool:
		return cs.streamScanBoolColumn(col, columnName, target, matchedRows, count, callback)
	case blockpack.ColumnTypeFloat64, blockpack.ColumnTypeRangeFloat64:
		return cs.streamScanFloat64Column(col, columnName, target, matchedRows, count, callback)
	case blockpack.ColumnTypeBytes, blockpack.ColumnTypeRangeBytes:
		return cs.streamScanBytesColumn(col, columnName, target, matchedRows, count, callback)
	default:
		return fmt.Errorf("unsupported column type for %s", columnName)
	}
}

// StreamScanNotEqual scans for rows where column != value, calling callback for each match
// streamScanStringNotEqual scans string column for values not equal to target
func (cs *ColumnScanner) streamScanStringNotEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	count := 0

	// Handle numeric target (string to number comparison)
	if targetNumeric, ok := numericTarget(target); ok {
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.StringValue(i)
			if !ok {
				continue
			}
			if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed != targetNumeric {
				if !callback(i) {
					return count, nil
				}
				count++
			}
		}
		return count, nil
	}

	// Handle string target
	targetStr, ok := target.(string)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected string for column")
	}
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && val != targetStr {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanInt64NotEqual scans int64 column for values not equal to target
func (cs *ColumnScanner) streamScanInt64NotEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetInt, ok := toInt64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected int64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Int64Value(i); ok && val != targetInt {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanUint64NotEqual scans uint64 column for values not equal to target
func (cs *ColumnScanner) streamScanUint64NotEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetUint, ok := toUint64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected uint64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Uint64Value(i); ok && val != targetUint {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanBoolNotEqual scans bool column for values not equal to target
func (cs *ColumnScanner) streamScanBoolNotEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetBool, ok := target.(bool)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected bool for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.BoolValue(i); ok && val != targetBool {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanFloat64NotEqual scans float64 column for values not equal to target
func (cs *ColumnScanner) streamScanFloat64NotEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected float64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Float64Value(i); ok && val != targetFloat {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanBytesNotEqual scans bytes column for values not equal to target
func (cs *ColumnScanner) streamScanBytesNotEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetBytes, ok := toBytesOrString(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected bytes or string for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.BytesValue(i); ok && !bytes.Equal(val, targetBytes) {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// StreamScanNotEqual scans for rows where the column value is not equal to the target.
func (cs *ColumnScanner) StreamScanNotEqual(columnName string, target any, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString, blockpack.ColumnTypeRangeString:
		return cs.streamScanStringNotEqual(col, target, callback)
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeRangeInt64:
		return cs.streamScanInt64NotEqual(col, target, callback)
	case blockpack.ColumnTypeUint64, blockpack.ColumnTypeRangeUint64, blockpack.ColumnTypeRangeDuration:
		return cs.streamScanUint64NotEqual(col, target, callback)
	case blockpack.ColumnTypeBool:
		return cs.streamScanBoolNotEqual(col, target, callback)
	case blockpack.ColumnTypeFloat64, blockpack.ColumnTypeRangeFloat64:
		return cs.streamScanFloat64NotEqual(col, target, callback)
	case blockpack.ColumnTypeBytes, blockpack.ColumnTypeRangeBytes:
		return cs.streamScanBytesNotEqual(col, target, callback)
	default:
		return 0, fmt.Errorf("unsupported column type for %s", columnName)
	}
}

// StreamScanLessThan scans for rows where column < value, calling callback for each match
// streamScanStringLessThan scans string column for values less than target
func (cs *ColumnScanner) streamScanStringLessThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	// Handle numeric target (string to number comparison)
	if targetNumeric, ok := numericTarget(target); ok {
		count := 0
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok {
				if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed < targetNumeric {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}
		return count, nil
	}

	// Handle string target
	targetStr, ok := target.(string)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected string for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && val < targetStr {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanInt64LessThan scans int64 column for values less than target
func (cs *ColumnScanner) streamScanInt64LessThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetInt, ok := toInt64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected int64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Int64Value(i); ok && val < targetInt {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanUint64LessThan scans uint64 column for values less than target
func (cs *ColumnScanner) streamScanUint64LessThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetUint, ok := toUint64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected uint64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Uint64Value(i); ok && val < targetUint {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanFloat64LessThan scans float64 column for values less than target
func (cs *ColumnScanner) streamScanFloat64LessThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected float64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Float64Value(i); ok && val < targetFloat {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanBytesLessThan scans bytes column for values less than target
func (cs *ColumnScanner) streamScanBytesLessThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		val, ok := col.BytesValueView(i)
		if !ok {
			continue
		}
		if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
			matched := false
			for _, av := range arrayValues {
				if arrayValueLessThan(av, target) {
					matched = true
					break
				}
			}
			if matched {
				if !callback(i) {
					return count, nil
				}
				count++
			}
		}
	}
	return count, nil
}

// StreamScanLessThan scans for rows where column < value, calling callback for each match
//
//nolint:dupl // Type-specific column scanner; inline RangeBytes comparison is intentional
func (cs *ColumnScanner) StreamScanLessThan(columnName string, target any, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString, blockpack.ColumnTypeRangeString:
		return cs.streamScanStringLessThan(col, target, callback)
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeRangeInt64:
		return cs.streamScanInt64LessThan(col, target, callback)
	case blockpack.ColumnTypeUint64, blockpack.ColumnTypeRangeUint64, blockpack.ColumnTypeRangeDuration:
		return cs.streamScanUint64LessThan(col, target, callback)
	case blockpack.ColumnTypeFloat64, blockpack.ColumnTypeRangeFloat64:
		return cs.streamScanFloat64LessThan(col, target, callback)
	case blockpack.ColumnTypeBytes:
		return cs.streamScanBytesLessThan(col, target, callback)
	case blockpack.ColumnTypeRangeBytes:
		targetBytes, ok := toBytesOrString(target)
		if !ok {
			return 0, fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
		}

		count := 0

		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			if bytes.Compare(val, targetBytes) < 0 {
				if !callback(i) {
					return count, nil
				}

				count++
			}
		}

		return count, nil
	case blockpack.ColumnTypeBool:
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported column type for < comparison")
	}
}

// StreamScanLessThanOrEqual scans for rows where column <= value
// streamScanStringLessThanOrEqual scans string column for values less than or equal to target
func (cs *ColumnScanner) streamScanStringLessThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	// Handle numeric target (string to number comparison)
	if targetNumeric, ok := numericTarget(target); ok {
		count := 0
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok {
				if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed <= targetNumeric {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}
		return count, nil
	}

	// Handle string target
	targetStr, ok := target.(string)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected string for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && val <= targetStr {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanInt64LessThanOrEqual scans int64 column for values less than or equal to target
func (cs *ColumnScanner) streamScanInt64LessThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetInt, ok := toInt64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected int64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Int64Value(i); ok && val <= targetInt {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanUint64LessThanOrEqual scans uint64 column for values less than or equal to target
func (cs *ColumnScanner) streamScanUint64LessThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetUint, ok := toUint64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected uint64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Uint64Value(i); ok && val <= targetUint {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanFloat64LessThanOrEqual scans float64 column for values less than or equal to target
func (cs *ColumnScanner) streamScanFloat64LessThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected float64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Float64Value(i); ok && val <= targetFloat {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanBytesLessThanOrEqual scans bytes column for values less than or equal to target
func (cs *ColumnScanner) streamScanBytesLessThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		val, ok := col.BytesValueView(i)
		if !ok {
			continue
		}
		if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
			matched := false
			for _, av := range arrayValues {
				if arrayValueLessThanOrEqual(av, target) {
					matched = true
					break
				}
			}
			if matched {
				if !callback(i) {
					return count, nil
				}
				count++
			}
		}
	}
	return count, nil
}

// StreamScanLessThanOrEqual scans for rows where column <= value
//
//nolint:dupl // Type-specific column scanner; inline RangeBytes comparison is intentional
func (cs *ColumnScanner) StreamScanLessThanOrEqual(
	columnName string,
	target any,
	callback vm.RowCallback,
) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString, blockpack.ColumnTypeRangeString:
		return cs.streamScanStringLessThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeRangeInt64:
		return cs.streamScanInt64LessThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeUint64, blockpack.ColumnTypeRangeUint64, blockpack.ColumnTypeRangeDuration:
		return cs.streamScanUint64LessThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeFloat64, blockpack.ColumnTypeRangeFloat64:
		return cs.streamScanFloat64LessThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeBytes:
		return cs.streamScanBytesLessThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeRangeBytes:
		targetBytes, ok := toBytesOrString(target)
		if !ok {
			return 0, fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
		}

		count := 0

		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			if bytes.Compare(val, targetBytes) <= 0 {
				if !callback(i) {
					return count, nil
				}

				count++
			}
		}

		return count, nil
	case blockpack.ColumnTypeBool:
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported column type for <= comparison")
	}
}

// StreamScanGreaterThan scans for rows where column > value
// streamScanStringGreaterThan scans string column for values greater than target
func (cs *ColumnScanner) streamScanStringGreaterThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	// Handle numeric target (string to number comparison)
	if targetNumeric, ok := numericTarget(target); ok {
		count := 0
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok {
				if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed > targetNumeric {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}
		return count, nil
	}

	// Handle string target
	targetStr, ok := target.(string)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected string for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && val > targetStr {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanInt64GreaterThan scans int64 column for values greater than target
func (cs *ColumnScanner) streamScanInt64GreaterThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetInt, ok := toInt64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected int64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Int64Value(i); ok && val > targetInt {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanUint64GreaterThan scans uint64 column for values greater than target
func (cs *ColumnScanner) streamScanUint64GreaterThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetUint, ok := toUint64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected uint64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Uint64Value(i); ok && val > targetUint {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanFloat64GreaterThan scans float64 column for values greater than target
func (cs *ColumnScanner) streamScanFloat64GreaterThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected float64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Float64Value(i); ok && val > targetFloat {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanBytesGreaterThan scans bytes column for values greater than target
func (cs *ColumnScanner) streamScanBytesGreaterThan(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		val, ok := col.BytesValueView(i)
		if !ok {
			continue
		}
		if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
			matched := false
			for _, av := range arrayValues {
				if arrayValueGreaterThan(av, target) {
					matched = true
					break
				}
			}
			if matched {
				if !callback(i) {
					return count, nil
				}
				count++
			}
		}
	}
	return count, nil
}

// StreamScanGreaterThan scans for rows where column > value
//
//nolint:dupl // Type-specific column scanner; inline RangeBytes comparison is intentional
func (cs *ColumnScanner) StreamScanGreaterThan(columnName string, target any, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString, blockpack.ColumnTypeRangeString:
		return cs.streamScanStringGreaterThan(col, target, callback)
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeRangeInt64:
		return cs.streamScanInt64GreaterThan(col, target, callback)
	case blockpack.ColumnTypeUint64, blockpack.ColumnTypeRangeUint64, blockpack.ColumnTypeRangeDuration:
		return cs.streamScanUint64GreaterThan(col, target, callback)
	case blockpack.ColumnTypeFloat64, blockpack.ColumnTypeRangeFloat64:
		return cs.streamScanFloat64GreaterThan(col, target, callback)
	case blockpack.ColumnTypeBytes:
		return cs.streamScanBytesGreaterThan(col, target, callback)
	case blockpack.ColumnTypeRangeBytes:
		targetBytes, ok := toBytesOrString(target)
		if !ok {
			return 0, fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
		}

		count := 0

		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			if bytes.Compare(val, targetBytes) > 0 {
				if !callback(i) {
					return count, nil
				}

				count++
			}
		}

		return count, nil
	case blockpack.ColumnTypeBool:
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported column type for > comparison")
	}
}

// StreamScanGreaterThanOrEqual scans for rows where column >= value
// streamScanStringGreaterThanOrEqual scans string column for values greater than or equal to target
func (cs *ColumnScanner) streamScanStringGreaterThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	// Handle numeric target (string to number comparison)
	if targetNumeric, ok := numericTarget(target); ok {
		count := 0
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok {
				if parsed, err := strconv.ParseFloat(val, 64); err == nil && parsed >= targetNumeric {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}
		return count, nil
	}

	// Handle string target
	targetStr, ok := target.(string)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected string for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && val >= targetStr {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanInt64GreaterThanOrEqual scans int64 column for values greater than or equal to target
func (cs *ColumnScanner) streamScanInt64GreaterThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetInt, ok := toInt64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected int64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Int64Value(i); ok && val >= targetInt {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanUint64GreaterThanOrEqual scans uint64 column for values greater than or equal to target
func (cs *ColumnScanner) streamScanUint64GreaterThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetUint, ok := toUint64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected uint64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Uint64Value(i); ok && val >= targetUint {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanFloat64GreaterThanOrEqual scans float64 column for values greater than or equal to target
func (cs *ColumnScanner) streamScanFloat64GreaterThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	targetFloat, ok := toFloat64(target)
	if !ok {
		return 0, fmt.Errorf("type mismatch: expected float64 for column")
	}
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.Float64Value(i); ok && val >= targetFloat {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}
	return count, nil
}

// streamScanBytesGreaterThanOrEqual scans bytes column for values greater than or equal to target
func (cs *ColumnScanner) streamScanBytesGreaterThanOrEqual(
	col *blockpack.Column,
	target any,
	callback vm.RowCallback,
) (int, error) {
	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		val, ok := col.BytesValueView(i)
		if !ok {
			continue
		}
		if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
			matched := false
			for _, av := range arrayValues {
				if arrayValueGreaterThanOrEqual(av, target) {
					matched = true
					break
				}
			}
			if matched {
				if !callback(i) {
					return count, nil
				}
				count++
			}
		}
	}
	return count, nil
}

// StreamScanGreaterThanOrEqual scans for rows where column >= value
//
//nolint:dupl // Type-specific column scanner; inline RangeBytes comparison is intentional
func (cs *ColumnScanner) StreamScanGreaterThanOrEqual(
	columnName string,
	target any,
	callback vm.RowCallback,
) (int, error) {
	col := cs.getColumnWithFallback(columnName)
	if col == nil {
		return 0, nil
	}

	switch col.Type {
	case blockpack.ColumnTypeString, blockpack.ColumnTypeRangeString:
		return cs.streamScanStringGreaterThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeRangeInt64:
		return cs.streamScanInt64GreaterThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeUint64, blockpack.ColumnTypeRangeUint64, blockpack.ColumnTypeRangeDuration:
		return cs.streamScanUint64GreaterThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeFloat64, blockpack.ColumnTypeRangeFloat64:
		return cs.streamScanFloat64GreaterThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeBytes:
		return cs.streamScanBytesGreaterThanOrEqual(col, target, callback)
	case blockpack.ColumnTypeRangeBytes:
		targetBytes, ok := toBytesOrString(target)
		if !ok {
			return 0, fmt.Errorf("type mismatch: expected bytes or string for column %s", columnName)
		}

		count := 0

		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}

			if bytes.Compare(val, targetBytes) >= 0 {
				if !callback(i) {
					return count, nil
				}

				count++
			}
		}

		return count, nil
	case blockpack.ColumnTypeBool:
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported column type for >= comparison")
	}
}

// StreamScanIsNull scans for rows where column IS NULL
func (cs *ColumnScanner) StreamScanIsNull(columnName string, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		// Column doesn't exist - all rows are NULL for this column
		count := 0
		for i := 0; i < cs.block.SpanCount(); i++ {
			if !callback(i) {
				return count, nil
			}
			count++
		}
		return count, nil
	}

	count := 0
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
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}

	return count, nil
}

// StreamScanIsNotNull scans for rows where column IS NOT NULL
func (cs *ColumnScanner) StreamScanIsNotNull(columnName string, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		// Column doesn't exist - no rows have non-NULL values
		return 0, nil
	}

	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		var hasValue bool
		switch col.Type {
		case blockpack.ColumnTypeString:
			_, hasValue = col.StringValue(i)
		case blockpack.ColumnTypeInt64:
			_, hasValue = col.Int64Value(i)
		case blockpack.ColumnTypeUint64:
			_, hasValue = col.Uint64Value(i)
		case blockpack.ColumnTypeBool:
			_, hasValue = col.BoolValue(i)
		case blockpack.ColumnTypeFloat64:
			_, hasValue = col.Float64Value(i)
		case blockpack.ColumnTypeBytes:
			_, hasValue = col.BytesValue(i)
		}
		if hasValue {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}

	return count, nil
}

// StreamScanRegex scans for rows where column matches regex, calling callback for each match
func (cs *ColumnScanner) StreamScanRegex(columnName string, pattern string, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	// Anchor regex pattern for full string matching.
	// TraceQL regex semantics require matching the ENTIRE string, not substrings.
	if !strings.HasPrefix(pattern, "^") {
		pattern = "^" + pattern
	}

	if !strings.HasSuffix(pattern, "$") {
		pattern = pattern + "$"
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
	}

	count := 0
	switch col.Type {
	case blockpack.ColumnTypeString:
		// OPTIMIZATION: Use dictionary-based filtering
		dict, indices := col.StringDict()
		if len(dict) > 0 {
			matchedDictIndices := make(map[uint32]bool, len(dict))
			for i, dictValue := range dict {
				if re.MatchString(dictValue) {
					matchedDictIndices[uint32(i)] = true //nolint:gosec
				}
			}

			if len(matchedDictIndices) == 0 {
				return 0, nil
			}

			// Fast path: if ALL dictionary entries match, return all rows
			if len(matchedDictIndices) == len(dict) {
				for i := 0; i < cs.block.SpanCount(); i++ {
					if col.IsPresent(i) && i < len(indices) {
						dictIdx := indices[i]
						if int(dictIdx) < len(dict) {
							if !callback(i) {
								return count, nil
							}
							count++
						}
					}
				}
				return count, nil
			}

			// Fast scan using dictionary indices
			for i := 0; i < cs.block.SpanCount(); i++ {
				if col.IsPresent(i) && i < len(indices) {
					dictIdx := indices[i]
					if matchedDictIndices[dictIdx] && int(dictIdx) < len(dict) {
						if !callback(i) {
							return count, nil
						}
						count++
					}
				}
			}
		} else {
			// Fallback to row-by-row scanning
			for i := 0; i < cs.block.SpanCount(); i++ {
				if val, ok := col.StringValue(i); ok && re.MatchString(val) {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}

	case blockpack.ColumnTypeBytes:
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueMatchesRegex(av, re) {
						matched = true
						break
					}
				}
				if matched {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}

	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeUint64,
		blockpack.ColumnTypeBool, blockpack.ColumnTypeFloat64:
		return 0, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)

	default:
		return 0, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)
	}

	return count, nil
}

// StreamScanRegexNotMatch scans for rows where column does NOT match regex
func (cs *ColumnScanner) StreamScanRegexNotMatch(
	columnName string,
	pattern string,
	callback vm.RowCallback,
) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
	}

	count := 0
	switch col.Type {
	case blockpack.ColumnTypeString:
		for i := 0; i < cs.block.SpanCount(); i++ {
			if val, ok := col.StringValue(i); ok && !re.MatchString(val) {
				if !callback(i) {
					return count, nil
				}
				count++
			}
		}

	case blockpack.ColumnTypeBytes:
		for i := 0; i < cs.block.SpanCount(); i++ {
			val, ok := col.BytesValueView(i)
			if !ok {
				continue
			}
			if arrayValues, err := blockpack.DecodeArray(val); err == nil && len(arrayValues) > 0 {
				matched := false
				for _, av := range arrayValues {
					if arrayValueMatchesRegex(av, re) {
						matched = true
						break
					}
				}
				if !matched {
					if !callback(i) {
						return count, nil
					}
					count++
				}
			}
		}

	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeUint64,
		blockpack.ColumnTypeBool, blockpack.ColumnTypeFloat64:
		return 0, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)

	default:
		return 0, fmt.Errorf("regex matching only supported for string columns, got %s", columnName)
	}

	return count, nil
}

// StreamScanContains scans for rows where column contains substring
func (cs *ColumnScanner) StreamScanContains(columnName string, substring string, callback vm.RowCallback) (int, error) {
	col := cs.block.GetColumn(columnName)
	if col == nil {
		return 0, nil
	}

	if col.Type != blockpack.ColumnTypeString {
		return 0, fmt.Errorf("contains matching only supported for string columns, got %s", columnName)
	}

	count := 0
	for i := 0; i < cs.block.SpanCount(); i++ {
		if val, ok := col.StringValue(i); ok && strings.Contains(val, substring) {
			if !callback(i) {
				return count, nil
			}
			count++
		}
	}

	return count, nil
}

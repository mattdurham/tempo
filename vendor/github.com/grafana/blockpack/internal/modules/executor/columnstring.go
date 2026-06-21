package executor

import (
	"strconv"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// metricsColumnString converts a column value at rowIdx to its string form for
// group-by key construction on the metrics path. Non-string column types are
// formatted via strconv; absent rows yield "".
func metricsColumnString(col *modules_reader.Column, rowIdx int) string {
	// NOTE-159: the leading IsPresent guard was redundant. Every typed accessor below
	// (StringValue/Int64Value/Uint64Value/Float64Value/BoolValue/BytesValue) already runs
	// needsDecode() + expandDenseIdx() + IsPresent(idx) internally and returns ok=false for
	// absent rows. Calling IsPresent here first duplicated all three (atomic load, sync.Once
	// fast-path, presence-bitmap read) on every span — doubling that work on the metrics
	// group-by hot path (M4/M6/M8 traceAccumulateRow). The accessor's ok=false already yields
	// the empty-string label for absent rows, so the guard is dropped; only the nil check
	// (which must precede the col.Type read) remains.
	if col == nil {
		return ""
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		v, ok := col.StringValue(rowIdx)
		if ok {
			return v
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		v, ok := col.Int64Value(rowIdx)
		if ok {
			return strconv.FormatInt(v, 10)
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		v, ok := col.Uint64Value(rowIdx)
		if ok {
			return strconv.FormatUint(v, 10)
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		v, ok := col.Float64Value(rowIdx)
		if ok {
			return strconv.FormatFloat(v, 'f', -1, 64)
		}
	case modules_shared.ColumnTypeBool:
		v, ok := col.BoolValue(rowIdx)
		if ok {
			return strconv.FormatBool(v)
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		v, ok := col.BytesValue(rowIdx)
		if ok {
			return string(v)
		}
	}
	return ""
}

package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"strings"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// modulesSpanFieldsAdapter implements executor.SpanFieldsProvider for a single
// row within a modules-format block. Field access is lazy — values are read from
// the block column data on demand, without pre-materializing all fields.
type modulesSpanFieldsAdapter struct {
	block  *modules_reader.Block
	rowIdx int
}

// NewSpanFieldsAdapter returns a SpanFieldsProvider that reads fields
// for the span at rowIdx from block. Used by api.go to bridge the modules
// block row to the SpanMatchCallback contract.
func NewSpanFieldsAdapter(block *modules_reader.Block, rowIdx int) modules_shared.SpanFieldsProvider {
	return &modulesSpanFieldsAdapter{block: block, rowIdx: rowIdx}
}

// modulesLookupColumn finds a column by exact name, falling back to resource./span.
// prefixes for unscoped names (no dot or colon). Mirrors blockColumnProvider.lookupColumn
// from internal/modules/executor/column_provider.go.
func modulesLookupColumn(block *modules_reader.Block, name string) *modules_reader.Column {
	if col := block.GetColumn(name); col != nil {
		return col
	}
	// For unscoped names (no dot or colon prefix), try resource. then span.
	if !strings.ContainsAny(name, ".:") {
		if col := block.GetColumn("resource." + name); col != nil {
			return col
		}
		if col := block.GetColumn("span." + name); col != nil {
			return col
		}
	}
	return nil
}

// modulesGetValue extracts the typed value from column col at rowIdx.
// Returns (nil, false) when the column is nil or the row is absent.
// Mirrors blockColumnProvider.GetValue from internal/modules/executor/column_provider.go.
func modulesGetValue(col *modules_reader.Column, rowIdx int) (any, bool) {
	if col == nil || !col.IsPresent(rowIdx) {
		return nil, false
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		if v, ok := col.StringValue(rowIdx); ok {
			return v, true
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		if v, ok := col.Int64Value(rowIdx); ok {
			return v, true
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			return v, true
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			return v, true
		}
	case modules_shared.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			return v, true
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		if v, ok := col.BytesValue(rowIdx); ok {
			return v, true
		}
	}
	return nil, false
}

// GetField implements executor.SpanFieldsProvider.
// Returns the typed value for the named field at this adapter's row.
// Supports exact column names (e.g., "resource.service.name", "span.http.method",
// "trace:id") and unscoped names with resource./span. fallback (e.g., "service.name").
func (a *modulesSpanFieldsAdapter) GetField(name string) (any, bool) {
	col := modulesLookupColumn(a.block, name)
	return modulesGetValue(col, a.rowIdx)
}

// IterateFields implements executor.SpanFieldsProvider.
// Calls fn for each column present at this row. Stops early if fn returns false.
// Iterates all block columns, including intrinsics (trace:id, span:id, etc.).
func (a *modulesSpanFieldsAdapter) IterateFields(fn func(name string, value any) bool) {
	for name, col := range a.block.Columns() {
		v, ok := modulesGetValue(col, a.rowIdx)
		if !ok {
			continue
		}
		if !fn(name, v) {
			return
		}
	}
}

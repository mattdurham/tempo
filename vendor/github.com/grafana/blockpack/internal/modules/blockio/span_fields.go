package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"strings"
	"sync"

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

// modulesSpanFieldsAdapterPool recycles *modulesSpanFieldsAdapter to avoid
// per-span heap allocation. Use getSpanFieldsAdapter / putSpanFieldsAdapter.
// NOTE-ALLOC-4: see blockio/NOTES.md §50.
var modulesSpanFieldsAdapterPool = sync.Pool{
	New: func() any { return &modulesSpanFieldsAdapter{} },
}

// getSpanFieldsAdapter returns a pooled *modulesSpanFieldsAdapter configured
// for block and rowIdx. Caller MUST call putSpanFieldsAdapter when done.
// NOTE-ALLOC-4: replaces direct &modulesSpanFieldsAdapter{} allocation.
func getSpanFieldsAdapter(block *modules_reader.Block, rowIdx int) *modulesSpanFieldsAdapter {
	a := modulesSpanFieldsAdapterPool.Get().(*modulesSpanFieldsAdapter)
	a.block = block
	a.rowIdx = rowIdx
	return a
}

// putSpanFieldsAdapter returns the adapter to the pool. Do NOT use a after this call.
func putSpanFieldsAdapter(a *modulesSpanFieldsAdapter) {
	a.block = nil
	a.rowIdx = 0
	modulesSpanFieldsAdapterPool.Put(a)
}

// ReleaseSpanFieldsAdapter returns a pooled adapter to the pool.
// Must only be called after Clone() or after the last use of the adapter.
// NOTE-ALLOC-4: release point is after SpanMatch.Clone() is called or after
// the span callback returns (whichever comes first).
func ReleaseSpanFieldsAdapter(p modules_shared.SpanFieldsProvider) {
	if a, ok := p.(*modulesSpanFieldsAdapter); ok && a != nil {
		putSpanFieldsAdapter(a)
	}
}

// NewSpanFieldsAdapter returns a SpanFieldsProvider that reads fields
// for the span at rowIdx from block. Used by api.go to bridge the modules
// block row to the SpanMatchCallback contract.
// NOTE-ALLOC-4: delegates to pooled getSpanFieldsAdapter; caller should release
// via ReleaseSpanFieldsAdapter after the adapter's last use.
func NewSpanFieldsAdapter(block *modules_reader.Block, rowIdx int) modules_shared.SpanFieldsProvider {
	return getSpanFieldsAdapter(block, rowIdx)
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
	case modules_shared.ColumnTypeUUID:
		// UUID columns were originally string attributes; return the UUID-formatted string
		// so callers see the same type as the original OTLP StringValue.
		if v, ok := col.StringValue(rowIdx); ok {
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
// When multiple typed variants of the same name are present (same-name/different-type
// OTLP attributes), only the first found variant with a present value is emitted;
// this prevents duplicate name emissions since the callback key is name-only.
//
// NOTE-ITER-1: ColumnTypeRangeString columns are auto-parsed body fields (SPEC-11.5).
// They are derivable from log:body and should not appear as explicit attributes in
// enumeration. GetField() still resolves them for direct lookups.
func (a *modulesSpanFieldsAdapter) IterateFields(fn func(name string, value any) bool) {
	// NOTE-049: Use pre-computed deduplicated slice when available — zero allocs.
	if entries := a.block.IterFields(); entries != nil {
		for i := range entries {
			v, ok := modulesGetValue(entries[i].Col, a.rowIdx)
			if !ok {
				continue
			}
			if !fn(entries[i].Name, v) {
				return
			}
		}
		return
	}
	// Fallback: block was parsed without BuildIterFields (tests, compaction, AddColumnsToBlock).
	seen := make(map[string]struct{})
	for key, col := range a.block.Columns() {
		// NOTE-ITER-1: skip body-parsed auto-columns; they are not original attributes.
		if key.Type == modules_shared.ColumnTypeRangeString {
			continue
		}
		if _, already := seen[key.Name]; already {
			continue
		}
		v, ok := modulesGetValue(col, a.rowIdx)
		if !ok {
			continue
		}
		seen[key.Name] = struct{}{}
		if !fn(key.Name, v) {
			return
		}
	}
}

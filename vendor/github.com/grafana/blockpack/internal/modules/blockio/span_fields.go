package blockio

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"strings"
	"sync"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// modulesSpanFieldsAdapter implements executor.SpanFieldsProvider for a single
// row within a modules-format block. Field access reads directly from block column
// data — the block payload is the sole authoritative source for all fields.

// modulesSpanFieldsAdapterPool recycles *modulesSpanFieldsAdapter to avoid
// per-span heap allocation. Use getSpanFieldsAdapter / putSpanFieldsAdapter.
// NOTE-ALLOC-4: see blockio/NOTES.md § NOTE-ALLOC-4 (adapter pool avoids per-span heap allocation).
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
	a.reader = nil
	a.blockIdx = 0
	return a
}

// getSpanFieldsAdapterWithReader returns a pooled adapter. The block payload is the
// authoritative source for all non-identity fields; reader+blockIdx enable the SpanTree
// identity fallback (NOTE-476, issue #394) for trace:id/span:id/span:parent_id when those
// columns are absent from both the block payload and the IntrinsicTOC.
func getSpanFieldsAdapterWithReader(
	block *modules_reader.Block, reader *modules_reader.Reader, blockIdx, rowIdx int,
	_ map[string]struct{},
) *modulesSpanFieldsAdapter {
	a := modulesSpanFieldsAdapterPool.Get().(*modulesSpanFieldsAdapter)
	a.block = block
	a.rowIdx = rowIdx
	a.reader = reader
	a.blockIdx = blockIdx
	return a
}

// putSpanFieldsAdapter returns the adapter to the pool. Do NOT use a after this call.
func putSpanFieldsAdapter(a *modulesSpanFieldsAdapter) {
	a.block = nil
	a.rowIdx = 0
	a.reader = nil
	a.blockIdx = 0
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

// NewSpanFieldsAdapterWithReader returns a SpanFieldsProvider backed by the block payload, with
// a SpanTree identity fallback (NOTE-476, issue #394) for trace:id/span:id/span:parent_id when
// those columns are absent from the block payload and IntrinsicTOC. wantCols is accepted for API
// compatibility. Release via ReleaseSpanFieldsAdapter.
func NewSpanFieldsAdapterWithReader(
	block *modules_reader.Block, reader *modules_reader.Reader, blockIdx, rowIdx int,
	wantCols map[string]struct{},
) modules_shared.SpanFieldsProvider {
	return getSpanFieldsAdapterWithReader(block, reader, blockIdx, rowIdx, wantCols)
}

// modulesGetValue returns the typed value for col at rowIdx. Returns (nil, false) when
// the column is nil, the row is absent, or the column type is unrecognized.
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
		if v, ok := col.StringValue(rowIdx); ok {
			return v, true
		}
	}
	return nil, false
}

// modulesLookupColumn searches block for the named column. Tries exact match first,
// then unscoped fallback (resource./span.) for names without a dot/colon prefix.
// Extracted from internal/modules/executor/column_provider.go.
func modulesLookupColumn(block *modules_reader.Block, name string) *modules_reader.Column {
	if block == nil {
		return nil
	}
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

// GetField implements executor.SpanFieldsProvider.
// Returns the typed value for the named field at this adapter's row.
// Supports exact column names (e.g., "resource.service.name", "span.http.method",
// "trace:id") and unscoped names with resource./span. fallback (e.g., "service.name").
func (a *modulesSpanFieldsAdapter) GetField(name string) (any, bool) {
	col := modulesLookupColumn(a.block, name)
	if col != nil {
		if v, ok := modulesGetValue(col, a.rowIdx); ok {
			return v, true
		}
		return nil, false
	}
	// NOTE-434: After SpanTree removal (#434), identity fields not in block columns
	// fall back to IntrinsicTOC. This covers v1 files where trace:id/span:id/span:parent_id
	// live in the IntrinsicTOC rather than block payload columns.
	if a.reader != nil {
		if v, ok := a.intrinsicTOCField(name); ok {
			return v, true
		}
	}
	return nil, false
}

// IterateFields implements executor.SpanFieldsProvider.
// Calls fn for each column present at this row. Stops early if fn returns false.
// Iterates all block columns. The block payload is the sole authoritative source.
//
// NOTE-ITER-1: ColumnTypeRangeString columns are auto-parsed body fields (SPEC-11.5).
// They are derivable from log:body and should not appear as explicit attributes in
// enumeration. GetField() still resolves them for direct lookups.
func (a *modulesSpanFieldsAdapter) IterateFields(fn func(name string, value any) bool) {
	if a.block == nil {
		return
	}
	// NOTE-049/NOTE-243: IterFields() returns the pre-computed deduplicated slice, built
	// lazily on this first call. Entries are already deduplicated by name, so iteration is
	// fully allocation-free (no per-call seen map).
	entries := a.block.IterFields()
	for i := range entries {
		v, ok := modulesGetValue(entries[i].Col, a.rowIdx)
		if !ok {
			continue
		}
		if !fn(entries[i].Name, v) {
			return
		}
	}
	// NOTE-434: After SpanTree removal (#434), emit identity fields from IntrinsicTOC when
	// absent from the block payload. Covers v1 files and OmitIntrinsicIdentityColumns blocks.
	if a.reader != nil {
		for _, name := range [...]string{modules_shared.TraceIDColumnName, modules_shared.SpanIDColumnName, modules_shared.SpanParentIDColumnName} {
			if modulesLookupColumn(a.block, name) != nil {
				continue // already emitted from the block payload above
			}
			if v, ok := a.intrinsicTOCField(name); ok {
				if !fn(name, v) {
					return
				}
			}
		}
	}
}

// intrinsicTOCField resolves trace:id/span:id/span:parent_id for this adapter's row from
// the IntrinsicTOC (via GetIntrinsicColumn). This is the fallback path after SpanTree removal
// (#434) for v1 files where identity is stored in IntrinsicTOC rather than block columns.
// Returns (nil, false) when the column doesn't exist, the row isn't found, or the value
// would be empty (span:parent_id absent for root spans).
func (a *modulesSpanFieldsAdapter) intrinsicTOCField(name string) (any, bool) {
	if a.reader == nil {
		return nil, false
	}
	if name != modules_shared.TraceIDColumnName &&
		name != modules_shared.SpanIDColumnName &&
		name != modules_shared.SpanParentIDColumnName {
		return nil, false
	}
	if a.blockIdx < 0 || a.blockIdx > int(^uint16(0)) || a.rowIdx < 0 || a.rowIdx > int(^uint16(0)) {
		return nil, false
	}
	col, err := a.reader.GetIntrinsicColumn(name)
	if err != nil || col == nil {
		return nil, false
	}
	blockIdx := uint16(a.blockIdx) //nolint:gosec // bounded above
	rowIdx := uint16(a.rowIdx)     //nolint:gosec // bounded above
	for i, ref := range col.BlockRefs {
		if ref.BlockIdx != blockIdx || ref.RowIdx != rowIdx {
			continue
		}
		if i >= len(col.BytesValues) {
			return nil, false
		}
		val := col.BytesValues[i]
		// Root spans have a zero parent_id: treat as absent (same as SpanTree convention).
		if name == modules_shared.SpanParentIDColumnName && isZeroBytes(val) {
			return nil, false
		}
		if len(val) == 0 {
			return nil, false
		}
		return val, true
	}
	return nil, false
}

// isZeroBytes reports whether b is all zero bytes.
func isZeroBytes(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

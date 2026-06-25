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
	// NOTE-476 (issue #394): identity fields may be absent from the block payload AND the
	// IntrinsicTOC (OmitIntrinsicIdentityColumns); resolve them from the SpanTree. Critical for
	// SpanMatch.IsRoot, which keys root detection on span:parent_id absence.
	if v, ok := a.spanTreeIdentityField(name); ok {
		return v, true
	}
	return nil, false
}

// spanTreeIdentityField resolves trace:id/span:id/span:parent_id for this adapter's row from
// the SpanTree identity reverse map. Returns (nil, false) when the field is not an identity
// field, no reader is attached, the file has no SpanTree, the row is absent, or — for
// span:parent_id — the span is a root (zero parent, matching the writer's "absent when empty"
// convention so IsRoot detects it). NOTE-476 (issue #394).
func (a *modulesSpanFieldsAdapter) spanTreeIdentityField(name string) (any, bool) {
	if a.reader == nil {
		return nil, false
	}
	if name != "trace:id" && name != "span:id" && name != "span:parent_id" {
		return nil, false
	}
	if a.blockIdx < 0 || a.blockIdx > int(^uint16(0)) || a.rowIdx < 0 || a.rowIdx > int(^uint16(0)) {
		return nil, false
	}
	idMap, err := a.reader.SpanTreeIdentityForBlock(uint16(a.blockIdx)) //nolint:gosec // bounded above
	if err != nil || idMap == nil {
		return nil, false
	}
	rec, ok := idMap[uint16(a.rowIdx)] //nolint:gosec // bounded above
	if !ok {
		return nil, false
	}
	switch name {
	case "trace:id":
		b := make([]byte, 16)
		copy(b, rec.TraceID[:])
		return b, true
	case "span:id":
		b := make([]byte, 8)
		copy(b, rec.SpanID[:])
		return b, true
	case "span:parent_id":
		// Root spans have a zero parent: report ABSENT so IsRoot treats them as roots, exactly
		// as a missing intrinsic/block column would.
		if rec.ParentID == ([8]byte{}) {
			return nil, false
		}
		b := make([]byte, 8)
		copy(b, rec.ParentID[:])
		return b, true
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
	// NOTE-476 (issue #394): emit the identity fields from the SpanTree when they are absent
	// from the block payload (OmitIntrinsicIdentityColumns). SpanMatch.Clone materializes via
	// IterateFields, so the cloned result would otherwise lose trace:id/span:id/span:parent_id
	// — breaking root detection (IsRoot keys on span:parent_id) and any downstream identity use.
	// Root spans (zero parent) intentionally emit no span:parent_id, matching column absence.
	if a.reader != nil {
		for _, name := range [...]string{"trace:id", "span:id", "span:parent_id"} {
			if modulesLookupColumn(a.block, name) != nil {
				continue // already emitted from the block payload above
			}
			if v, ok := a.spanTreeIdentityField(name); ok {
				if !fn(name, v) {
					return
				}
			}
		}
	}
}

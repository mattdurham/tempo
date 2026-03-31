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
//
// When reader is non-nil, GetField and IterateFields fall back to the intrinsic
// section for columns that are no longer stored as block columns (e.g.
// resource.service.name, span:status). The intrinsic lookup is lazy: the per-row
// field map is loaded on first call and cached for the adapter's lifetime.
type modulesSpanFieldsAdapter struct {
	block  *modules_reader.Block
	reader *modules_reader.Reader
	// intrinsicCache is populated lazily on first intrinsic fallback.
	// nil means "not yet loaded"; empty map means "loaded, nothing found".
	intrinsicCache map[string]any
	rowIdx         int
	blockIdx       int
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
	a.reader = nil
	a.blockIdx = 0
	a.intrinsicCache = nil
	return a
}

// getSpanFieldsAdapterWithReader returns a pooled adapter with intrinsic fallback support.
// reader and blockIdx are used to load intrinsic column values for fields absent from
// block columns (e.g. resource.service.name, span:status after dual-storage removal).
func getSpanFieldsAdapterWithReader(
	block *modules_reader.Block, reader *modules_reader.Reader, blockIdx, rowIdx int,
) *modulesSpanFieldsAdapter {
	a := modulesSpanFieldsAdapterPool.Get().(*modulesSpanFieldsAdapter)
	a.block = block
	a.rowIdx = rowIdx
	a.reader = reader
	a.blockIdx = blockIdx
	a.intrinsicCache = nil
	return a
}

// putSpanFieldsAdapter returns the adapter to the pool. Do NOT use a after this call.
func putSpanFieldsAdapter(a *modulesSpanFieldsAdapter) {
	a.block = nil
	a.rowIdx = 0
	a.reader = nil
	a.blockIdx = 0
	a.intrinsicCache = nil
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

// NewSpanFieldsAdapterWithReader returns a SpanFieldsProvider with intrinsic fallback.
// When a field is absent from block columns, GetField falls back to the reader's
// intrinsic section using (blockIdx, rowIdx) as the lookup key.
// Use this variant when block columns no longer store intrinsic columns (e.g. after
// dual-storage removal). Release via ReleaseSpanFieldsAdapter.
func NewSpanFieldsAdapterWithReader(
	block *modules_reader.Block, reader *modules_reader.Reader, blockIdx, rowIdx int,
) modules_shared.SpanFieldsProvider {
	return getSpanFieldsAdapterWithReader(block, reader, blockIdx, rowIdx)
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

// loadIntrinsicCache populates a.intrinsicCache from the reader's intrinsic section
// for the row at (a.blockIdx, a.rowIdx). Called lazily on first intrinsic fallback.
// With dual storage (restored after PR #172 rollback), new files have intrinsic
// columns in block payloads, so GetField finds them via modulesGetValue before
// reaching this fallback. This path is kept as a backward-compatibility safety net
// for files written between the PR #172 merge and this fix, where intrinsic columns
// were absent from block payloads and could only be resolved from the intrinsic section.
// No-op when a.reader is nil.
func (a *modulesSpanFieldsAdapter) loadIntrinsicCache() {
	if a.reader == nil {
		a.intrinsicCache = map[string]any{} // mark as loaded (empty)
		return
	}
	key := uint32(a.blockIdx)<<16 | uint32(a.rowIdx) //nolint:gosec
	cols := a.reader.IntrinsicColumnNames()
	a.intrinsicCache = make(map[string]any, len(cols))
	for _, colName := range cols {
		col, err := a.reader.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			continue
		}
		switch col.Format {
		case modules_shared.IntrinsicFormatFlat:
			// Linear scan with early break — O(N) worst case.
			// For dual-storage files the block payload already has this column,
			// so IterateFields won't reach here for it; the scan is a no-op fallback.
			for i, r := range col.BlockRefs {
				if uint32(r.BlockIdx)<<16|uint32(r.RowIdx) != key {
					continue
				}
				if i < len(col.Uint64Values) {
					a.intrinsicCache[colName] = col.Uint64Values[i]
				} else if i < len(col.BytesValues) {
					a.intrinsicCache[colName] = col.BytesValues[i]
				}
				break
			}
		case modules_shared.IntrinsicFormatDict:
			for _, entry := range col.DictEntries {
				found := false
				for _, r := range entry.BlockRefs {
					if uint32(r.BlockIdx)<<16|uint32(r.RowIdx) != key {
						continue
					}
					if col.Type == modules_shared.ColumnTypeInt64 ||
						col.Type == modules_shared.ColumnTypeRangeInt64 {
						a.intrinsicCache[colName] = entry.Int64Val
					} else {
						a.intrinsicCache[colName] = entry.Value
					}
					found = true
					break
				}
				if found {
					break
				}
			}
		}
	}

	// Synthesize span:end from span:start + span:duration when both are present.
	// span:end has no TOC entry; executor/stream.go synthesizes it the same way
	// via r.GetIntrinsicColumn("span:end"). Mirror that here so GetField("span:end")
	// works on the intrinsic fallback path.
	if startVal, hasStart := a.intrinsicCache["span:start"]; hasStart {
		if durVal, hasDur := a.intrinsicCache["span:duration"]; hasDur {
			startU, startOK := startVal.(uint64)
			durU, durOK := durVal.(uint64)
			if startOK && durOK {
				a.intrinsicCache["span:end"] = startU + durU
			}
		}
	}
}

// GetField implements executor.SpanFieldsProvider.
// Returns the typed value for the named field at this adapter's row.
// Supports exact column names (e.g., "resource.service.name", "span.http.method",
// "trace:id") and unscoped names with resource./span. fallback (e.g., "service.name").
// Falls back to the intrinsic section when the block column is absent and a reader
// was provided at construction time.
func (a *modulesSpanFieldsAdapter) GetField(name string) (any, bool) {
	col := modulesLookupColumn(a.block, name)
	if col != nil {
		// Column exists in block payload. For dual-storage files, if the span is absent
		// here it is also absent in the intrinsic section — skip the expensive O(N) scan.
		if v, ok := modulesGetValue(col, a.rowIdx); ok {
			return v, true
		}
		return nil, false
	}
	// Column absent from block payload — try intrinsic section (backward compat with
	// files written during the PR #172 window when intrinsics were payload-absent).
	if a.intrinsicCache == nil {
		a.loadIntrinsicCache()
	}
	if v, ok := a.intrinsicCache[name]; ok {
		return v, true
	}
	return nil, false
}

// IterateFields implements executor.SpanFieldsProvider.
// Calls fn for each column present at this row. Stops early if fn returns false.
// Iterates all block columns, including intrinsics (trace:id, span:id, etc.),
// followed by any intrinsic section fields not already emitted from block columns.
// When multiple typed variants of the same name are present (same-name/different-type
// OTLP attributes), only the first found variant with a present value is emitted;
// this prevents duplicate name emissions since the callback key is name-only.
//
// NOTE-ITER-1: ColumnTypeRangeString columns are auto-parsed body fields (SPEC-11.5).
// They are derivable from log:body and should not appear as explicit attributes in
// enumeration. GetField() still resolves them for direct lookups.
func (a *modulesSpanFieldsAdapter) IterateFields(fn func(name string, value any) bool) {
	seen := make(map[string]struct{})
	// NOTE-049: Use pre-computed deduplicated slice when available — zero allocs.
	if entries := a.block.IterFields(); entries != nil {
		for i := range entries {
			v, ok := modulesGetValue(entries[i].Col, a.rowIdx)
			if !ok {
				continue
			}
			seen[entries[i].Name] = struct{}{}
			if !fn(entries[i].Name, v) {
				return
			}
		}
	} else {
		// Fallback: block was parsed without BuildIterFields (tests, compaction, AddColumnsToBlock).
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
	// Emit intrinsic fields not already emitted from block columns.
	// For dual-storage files (PR #174+), all intrinsics are present in block payloads
	// so we can skip the expensive O(N) intrinsic-section scan entirely.
	// We detect dual-storage by checking whether every intrinsic column name from the
	// reader's TOC exists in the block's column map (regardless of whether the value
	// is present for this specific span — a root span may lack span:parent_id but the
	// column still exists in the block).
	if a.reader != nil {
		needFallback := false
		for _, name := range a.reader.IntrinsicColumnNames() {
			if a.block.GetColumn(name) == nil {
				// Intrinsic column absent from block payload — need intrinsic fallback.
				// This happens for files written during the PR #172 window (no dual storage).
				needFallback = true
				break
			}
		}
		if needFallback && a.intrinsicCache == nil {
			a.loadIntrinsicCache()
		}
	} else if a.intrinsicCache == nil {
		a.loadIntrinsicCache()
	}
	for k, v := range a.intrinsicCache {
		if _, already := seen[k]; already {
			continue
		}
		if !fn(k, v) {
			return
		}
	}
}

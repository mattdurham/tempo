package blockpack

// valueindex_extract.go — reusable value-index extraction (NOTE-VI-018, issue #401).
//
// The value-index consumer reads a blockpack and yields one entry per
// (column, span) observation. The high-value tag-value columns — span:name,
// span:kind, span:status, span:status_message, span:duration,
// resource.service.name — are INTRINSIC columns. Depending on the file version
// these intrinsics may surface in the per-block column map (block.Columns()) OR
// live only in the IntrinsicTOC. A prior extractor that iterated only
// block.Columns() therefore silently dropped them for TOC-only files. This helper
// guarantees they are indexed regardless of version:
//
//	Phase 1 — per-block attribute + intrinsic columns. Each inner block is parsed
//	one at a time (peak memory is one block's decoded columns, not the whole file)
//	and every non-denied column it exposes is yielded.
//
//	Phase 2 — IntrinsicTOC fallback. Any non-denied intrinsic column that phase 1
//	did NOT yield (TOC-only files) is read straight from the IntrinsicTOC via cheap
//	ranged GETs and yielded.
//
// Both phases share one denylist and one yield callback, so the standalone
// value-index-consumer binary and tempo's in-process module share one correct
// implementation rather than each maintaining a divergent copy.

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// ValueIndexEntry is one extracted (column, span) observation. It carries the
// fully-resolved column name, the typed value, the column type, the zero-based
// inner block index, and the span's wall-clock start time in seconds (0 when the
// block has no span:start intrinsic). Callers map this into their own per-entry
// record (e.g. valueindexconsumer.ColumnEntry) and supply the source object ref.
type ValueIndexEntry struct {
	// Value is the typed column value: string, int64, uint64, bool, float64, or
	// []byte, matching ColType. A []byte value aliases reader-owned memory valid
	// only for the duration of the yield call; a caller that retains it must copy.
	Value any
	// ColName is the resolved column name. Intrinsic columns keep their colon form
	// (e.g. "span:name", "resource.service.name"); attribute columns keep their
	// dotted form (e.g. "span.http.method", "resource.region").
	ColName string
	// ColType is the column's data type.
	ColType ColumnType
	// BlockID is the zero-based inner block index the observation came from.
	BlockID uint32
	// TimeSec is the span's start time in whole seconds, or 0 when unavailable.
	TimeSec uint64
}

// DefaultValueIndexDenylist is the set of intrinsic columns that are never worth
// indexing for tag-value lookups (NOTE-VI-018): the three identity columns are
// unique per span (useless as a tag value), and span:start is a high-cardinality
// timestamp already covered by the time-range index. Everything else — including
// every attribute column and the remaining intrinsics — is indexed by default.
// Callers may pass their own denylist to ExtractValueIndexEntries; nil selects
// this default.
var DefaultValueIndexDenylist = map[string]struct{}{
	modules_shared.SpanIDColumnName:       {}, // unique per span
	modules_shared.SpanParentIDColumnName: {}, // unique per span
	modules_shared.TraceIDColumnName:      {}, // unique per span
	modules_shared.SpanStartColumnName:    {}, // timestamp; covered by time-range index
}

// ExtractValueIndexEntries reads every indexable (column, span) observation from
// one blockpack and streams it to yield (NOTE-VI-018, issue #401).
//
// Columns in denylist are skipped; a nil denylist selects
// DefaultValueIndexDenylist. yield is called once per present span value; if it
// returns an error, extraction stops and returns that error. A column absent from
// a block simply produces no calls.
//
// TimeSec on each entry is the span's start time in seconds, resolved from the
// span:start intrinsic by (blockIdx, rowIdx); it is 0 when the file has no
// span:start column.
func ExtractValueIndexEntries(
	r *Reader,
	denylist map[string]struct{},
	yield func(ValueIndexEntry) error,
) error {
	if r == nil {
		return nil
	}
	if denylist == nil {
		denylist = DefaultValueIndexDenylist
	}

	// span:start (nanoseconds) → per-ref second resolution for TimeSec. Built once
	// up front from the intrinsic column so each yielded entry can be stamped.
	_ = r.EnsureIntrinsicTOC()
	startSecByRef := buildSpanStartSecByRef(r)

	// Phase 1 — per-block attribute + intrinsic columns. Track which column names
	// were yielded so phase 2 only covers intrinsics this phase missed.
	yielded := make(map[string]struct{})
	if err := extractBlockColumns(r, denylist, startSecByRef, yielded, yield); err != nil {
		return err
	}

	// Phase 2 — IntrinsicTOC fallback for any intrinsic column phase 1 did not yield.
	return extractIntrinsicColumns(r, denylist, startSecByRef, yielded, yield)
}

// buildSpanStartSecByRef builds a packed-key (uint32(blockIdx)<<16 | rowIdx) → seconds
// map from the span:start intrinsic column, converting nanoseconds to whole seconds.
// Returns nil when the column is absent so callers fall back to TimeSec == 0.
func buildSpanStartSecByRef(r *modules_reader.Reader) map[uint32]uint64 {
	col, err := r.GetIntrinsicColumn(modules_shared.SpanStartColumnName)
	if err != nil || col == nil || len(col.Uint64Values) == 0 {
		return nil
	}
	col.EnsureBlockRefs()
	m := make(map[uint32]uint64, len(col.BlockRefs))
	for i, ref := range col.BlockRefs {
		if i >= len(col.Uint64Values) {
			break
		}
		key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
		m[key] = col.Uint64Values[i] / 1_000_000_000
	}
	return m
}

// extractBlockColumns runs phase 1: parse each inner block one at a time and yield
// every present value of every non-denied column it exposes. The set of column
// names actually yielded is recorded in yielded so the IntrinsicTOC fallback skips
// them.
func extractBlockColumns(
	r *modules_reader.Reader,
	denylist map[string]struct{},
	startSecByRef map[uint32]uint64,
	yielded map[string]struct{},
	yield func(ValueIndexEntry) error,
) error {
	for bi := range r.BlockCount() {
		meta := r.BlockMeta(bi)
		raw, rerr := r.ReadBlockRaw(bi)
		if rerr != nil {
			continue
		}
		block, perr := r.ParseBlockFromBytes(raw, WantAll(), meta)
		if perr != nil {
			continue
		}
		for colKey, col := range block.Block.Columns() {
			if _, denied := denylist[colKey.Name]; denied {
				continue
			}
			colType := col.Type
			for row := range block.Block.SpanCount() {
				if !col.IsPresent(row) {
					continue
				}
				val, ok := blockColumnValueAt(col, colType, row)
				if !ok {
					continue
				}
				yielded[colKey.Name] = struct{}{}
				key := uint32(bi)<<16 | uint32(row) //nolint:gosec // bounded by block/span counts
				if err := yield(ValueIndexEntry{
					ColName: colKey.Name,
					Value:   val,
					ColType: colType,
					BlockID: uint32(bi), //nolint:gosec // bounded by BlockCount
					TimeSec: startSecByRef[key],
				}); err != nil {
					return err
				}
			}
		}
		// block goes out of scope here — GC reclaims decoded column memory.
	}
	return nil
}

// extractIntrinsicColumns runs phase 2: for every non-denied IntrinsicTOC column
// that phase 1 did NOT already yield (TOC-only files), yield each present value.
func extractIntrinsicColumns(
	r *modules_reader.Reader,
	denylist map[string]struct{},
	startSecByRef map[uint32]uint64,
	yielded map[string]struct{},
	yield func(ValueIndexEntry) error,
) error {
	for _, name := range r.IntrinsicColumnNames() {
		if _, denied := denylist[name]; denied {
			continue
		}
		if _, done := yielded[name]; done {
			continue // already covered by the per-block phase
		}
		col, err := r.GetIntrinsicColumn(name)
		if err != nil || col == nil {
			continue
		}
		if err := yieldIntrinsicColumn(col, name, startSecByRef, yield); err != nil {
			return err
		}
	}
	return nil
}

// yieldIntrinsicColumn yields every present value of one intrinsic column. Flat/
// XOR/Delta columns store values positionally aligned with BlockRefs; Dict columns
// store one value per dictionary entry, each carrying its own ref list.
func yieldIntrinsicColumn(
	col *modules_shared.IntrinsicColumn,
	name string,
	startSecByRef map[uint32]uint64,
	yield func(ValueIndexEntry) error,
) error {
	emit := func(ref modules_shared.BlockRef, val any) error {
		key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx)
		return yield(ValueIndexEntry{
			ColName: name,
			Value:   val,
			ColType: col.Type,
			BlockID: uint32(ref.BlockIdx),
			TimeSec: startSecByRef[key],
		})
	}

	switch col.Format {
	case modules_shared.IntrinsicFormatDict:
		isInt := col.Type == modules_shared.ColumnTypeInt64 ||
			col.Type == modules_shared.ColumnTypeRangeInt64
		for _, entry := range col.DictEntries {
			var val any
			if isInt {
				val = entry.Int64Val
			} else {
				val = entry.Value
			}
			for _, ref := range entry.BlockRefs {
				if err := emit(ref, val); err != nil {
					return err
				}
			}
		}
	case modules_shared.IntrinsicFormatFlat,
		modules_shared.IntrinsicFormatXORBytes,
		modules_shared.IntrinsicFormatDeltaUint64:
		col.EnsureBlockRefs()
		for i, ref := range col.BlockRefs {
			val, ok := flatIntrinsicValueAt(col, i)
			if !ok {
				continue
			}
			if err := emit(ref, val); err != nil {
				return err
			}
		}
	}
	return nil
}

// flatIntrinsicValueAt returns the typed value of the i-th ref of a Flat/XOR/Delta
// intrinsic column (value arrays are positionally aligned with BlockRefs[i]).
func flatIntrinsicValueAt(col *modules_shared.IntrinsicColumn, i int) (any, bool) {
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString,
		modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		if i < len(col.BytesValues) {
			return col.BytesValues[i], true
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64,
		modules_shared.ColumnTypeRangeDuration:
		if i < len(col.Uint64Values) {
			return int64(col.Uint64Values[i]), true //nolint:gosec // stored two's-complement
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		if i < len(col.Uint64Values) {
			return col.Uint64Values[i], true
		}
	}
	return nil, false
}

// blockColumnValueAt returns the typed value at row of a parsed block column, or
// ok=false for value types this extractor does not index.
func blockColumnValueAt(col *modules_reader.Column, colType ColumnType, row int) (any, bool) {
	switch colType {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString,
		modules_shared.ColumnTypeUUID:
		return col.StringValue(row)
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64,
		modules_shared.ColumnTypeRangeDuration:
		return col.Int64Value(row)
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		return col.Uint64Value(row)
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		return col.Float64Value(row)
	case modules_shared.ColumnTypeBool:
		return col.BoolValue(row)
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		return col.BytesValue(row)
	}
	return nil, false
}

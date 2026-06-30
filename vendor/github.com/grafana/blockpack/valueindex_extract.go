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
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// ValueIndexEntry is one extracted (column, span) observation.
type ValueIndexEntry struct {
	Value    any    // typed column value (string, int64, uint64, bool, float64, []byte)
	ColName  string // resolved column name
	ColType  ColumnType
	BlockID  uint32              // v1: zero-based block index (NOTE-VI-014)
	BlockRef valueindex.BlockRef // v2+: page-addressed block reference (NOTE-VI-027)
	TimeSec  uint64              // span start time in whole seconds (0 if unavailable)
	SpanID   [8]byte             // span identity for direct lookup (NOTE-VI-029, #428)
	RowIdx   int                 // row index within block for O(1) access (NOTE-VI-029, #428)
}

// ExtractValueIndexEntries reads every indexable (column, span) observation from
// one blockpack and streams it to yield (NOTE-VI-018, issue #401).
//
// The value index is a policy-free, general-purpose lookup structure: it indexes
// every column the reader exposes (NOTE-VI-027, issue #414). There is no built-in
// denylist — callers (the querier) decide which columns are useful at read time,
// not the writer. A nil denylist therefore indexes everything; callers that
// genuinely need to exclude columns pass an explicit non-nil denylist.
//
// The high-cardinality time-domain intrinsics (span:start, span:end,
// span:duration) are stored as raw nanosecond uint64 values, which would
// degenerate the index into a per-span posting list with no value sharing. To
// keep them useful for time-bucketed and duration lookups they are truncated to
// millisecond precision during extraction only (NOTE-VI-027, issue #415): the
// stored block columns remain nanosecond precision for range queries. The column
// type is unchanged (still uint64 milliseconds, not a duration type).
//
// Columns in denylist are skipped; a nil denylist indexes every column. yield is
// called once per present span value; if it returns an error, extraction stops and
// returns that error. A column absent from a block simply produces no calls.
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

	// span:start (nanoseconds) → per-ref second resolution for TimeSec. Built once
	// up front from the block column so each yielded entry can be stamped.
	startSecByRef := buildSpanStartSecByRef(r)

	// Per-block-index v2 file locator (NOTE-V2-002, issue #423). Built once so each
	// entry can be stamped with the block's page-aligned file range without
	// re-reading BlockMeta per entry.
	blockRefByIdx := buildBlockRefByIdx(r)

	// NOTE-436: all columns (intrinsic and attribute alike) are inner-block columns.
	// A single per-block column walk yields every value — there is no IntrinsicTOC
	// fallback phase.
	return extractBlockColumns(r, denylist, startSecByRef, blockRefByIdx, yield)
}

// buildBlockRefByIdx precomputes the v2 page-aligned file locator (NOTE-V2-002) for
// every inner block, indexed by block index. The locator is derived from each
// block's byte Offset and Length in BlockMeta: Page = Offset / 4096 and Length is
// the byte length rounded up to whole 4 KB pages. The v2 writer (#419) pads inner
// blocks to 4 KB boundaries so Offset is page-aligned; for v1 (unpadded) files the
// Page floors to the containing page and is corrected once #419 lands.
func buildBlockRefByIdx(r *modules_reader.Reader) []modules_shared.BlockFileRef {
	n := r.BlockCount()
	refs := make([]modules_shared.BlockFileRef, n)
	for bi := range n {
		refs[bi] = blockFileRefFromMeta(r.BlockMeta(bi))
	}
	return refs
}

// blockFileRefFromMeta converts a block's byte Offset/Length into a v2 BlockFileRef
// in 4 KB page units (NOTE-V2-002). Length is rounded up so the padded block is
// fully covered. Page values beyond the uint24 ceiling saturate to the max, which a
// single file cannot reach (64 GB).
func blockFileRefFromMeta(meta modules_shared.BlockMeta) modules_shared.BlockFileRef {
	const pageSize = modules_shared.BlockFileRefPageSize
	page := meta.Offset / pageSize
	lengthPages := (meta.Length + pageSize - 1) / pageSize
	if page > 0xFFFFFF {
		page = 0xFFFFFF
	}
	if lengthPages > 0xFFFF {
		lengthPages = 0xFFFF
	}
	return modules_shared.BlockFileRef{
		Page:   uint32(page),        //nolint:gosec // saturated to uint24 max above
		Length: uint16(lengthPages), //nolint:gosec // saturated to uint16 max above
	}
}

// truncateTimeValueToMillis truncates the raw nanosecond value of a time-domain
// intrinsic column (span:start, span:end, span:duration) to millisecond precision
// for value-index extraction (NOTE-VI-027, issue #415). These columns are stored
// as nanosecond uint64; without truncation every span gets a unique value hash and
// the index degenerates to a per-span posting list. Truncation gives ~1000x
// cardinality reduction so spans in the same millisecond share one value bucket.
//
// Non-time columns and unexpected value types pass through unchanged. The returned
// value keeps the same dynamic type as the input (uint64 stays uint64, int64 stays
// int64) so the recorded ColumnType is still correct.
func truncateTimeValueToMillis(name string, val any) any {
	switch name {
	case modules_shared.SpanStartColumnName,
		modules_shared.SpanEndColumnName,
		modules_shared.SpanDurationColumnName:
	default:
		return val
	}
	switch v := val.(type) {
	case uint64:
		return v / 1_000_000
	case int64:
		return v / 1_000_000
	default:
		return val
	}
}

// buildSpanStartSecByRef builds a packed-key (uint32(blockIdx)<<16 | rowIdx) → seconds
// map from the span:start block column, converting nanoseconds to whole seconds.
// After #433 (IntrinsicTOC removal), span:start lives in block payload columns.
// Returns nil when the column is absent so callers fall back to TimeSec == 0.
func buildSpanStartSecByRef(r *modules_reader.Reader) map[uint32]uint64 {
	var m map[uint32]uint64
	for bi := range r.BlockCount() {
		bwb, err := r.GetBlockWithBytes(bi, nil)
		if err != nil || bwb == nil {
			continue
		}
		col := bwb.Block.GetColumn(modules_shared.SpanStartColumnName)
		if col == nil {
			continue
		}
		if m == nil {
			m = make(map[uint32]uint64, r.BlockCount()*int(r.BlockMeta(0).SpanCount))
		}
		for rowIdx := range bwb.Block.SpanCount() {
			v, ok := col.Uint64Value(rowIdx)
			if !ok {
				continue
			}
			key := uint32(bi)<<16 | uint32(rowIdx) //nolint:gosec // bounded values
			m[key] = v / 1_000_000_000
		}
	}
	return m
}

// extractBlockColumns parses each inner block one at a time and yields every present
// value of every non-denied column it exposes. NOTE-436: this is the only extraction
// path — all columns live in inner blocks.
func extractBlockColumns(
	r *modules_reader.Reader,
	denylist map[string]struct{},
	startSecByRef map[uint32]uint64,
	blockRefByIdx []modules_shared.BlockFileRef,
	yield func(ValueIndexEntry) error,
) error {
	for bi := range r.BlockCount() {
		meta := r.BlockMeta(bi)
		// V2 lean format unconditional (2026-06-29): every block is page-aligned and addressed
		// by a page-unit BlockRef. Block 0 sits at offset 0 (PageNum==0). Legacy BlockID is unused.
		//nolint:gosec // meta.Offset/Length bounded by valid file size (<64 GiB)
		blockRef, _ := valueindex.BlockRefFromByteRange(int64(meta.Offset), int64(meta.Length))
		raw, rerr := r.ReadBlockRaw(bi)
		if rerr != nil {
			continue
		}
		block, perr := r.ParseBlockFromBytes(raw, WantAll(), meta)
		if perr != nil {
			continue
		}
		// Look up span:id column for this block once per block (for v4 span identity).
		spanIDCol := block.Block.GetColumn("span:id")
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
				// Time-domain intrinsics (span:start/end/duration) flow through the block
				// columns now that the IntrinsicTOC is gone (#436); truncate ns→ms here so
				// the value index keeps the cardinality reduction of NOTE-VI-027 (#415).
				val = truncateTimeValueToMillis(colKey.Name, val)
				key := uint32(bi)<<16 | uint32(row) //nolint:gosec
				var spanID [8]byte
				if spanIDCol != nil {
					if v, ok2 := spanIDCol.BytesValue(row); ok2 && len(v) == 8 {
						copy(spanID[:], v)
					}
				}
				if err := yield(ValueIndexEntry{
					ColName:  colKey.Name,
					Value:    val,
					ColType:  colType,
					BlockRef: blockRef,
					TimeSec:  startSecByRef[key],
					SpanID:   spanID,
					RowIdx:   row,
				}); err != nil {
					return err
				}
			}
		}
		// block goes out of scope here — GC reclaims decoded column memory.
	}
	return nil
}

// blockColumnValueAt returns the typed value at row of a parsed block column, or
// ok=false for value types this extractor does not index.
func blockColumnValueAt(col *modules_reader.Column, colType ColumnType, row int) (any, bool) {
	switch colType {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		return col.StringValue(row)
	case modules_shared.ColumnTypeUUID:
		// StringValue returns a formatted UUID string, but CanonicalValue expects [16]byte.
		// Use BytesValue to get the raw 16 bytes and convert.
		b, ok := col.BytesValue(row)
		if !ok || len(b) != 16 {
			return nil, false
		}
		var uid [16]byte
		copy(uid[:], b)
		return uid, true
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

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
	Value        any    // typed column value (string, int64, uint64, bool, float64, []byte)
	ColName      string // resolved column name
	ColType      ColumnType
	BlockID      uint32              // v1: zero-based block index (NOTE-VI-014)
	BlockRef     valueindex.BlockRef // v2+: page-addressed block reference (NOTE-VI-027)
	TimeSec      uint64              // span start time in whole seconds (0 if unavailable)
	SpanID       [8]byte             // span identity for direct lookup (NOTE-VI-029, #428)
	ParentSpanID [8]byte             // parent span identity; zero for a root span or absent column
	TraceID      [16]byte            // trace identity for the row this entry belongs to
	RowIdx       int                 // row index within block for O(1) access (NOTE-VI-029, #428)
}

// ExtractValueIndexEntries reads every indexable (column, span) observation from
// one blockpack and streams it to yield (NOTE-VI-018, issue #401).
//
// The value index is a policy-free, general-purpose lookup structure by default:
// a disabled policy (the zero value) indexes every column the reader exposes
// (NOTE-VI-027, issue #414). Callers that need to restrict which columns are
// extracted (#496's dedicated-column + usage-triggered-backfill policy) pass an
// explicit Enabled policy — see ColumnPolicy.
//
// The high-cardinality time-domain intrinsics (span:start, span:end,
// span:duration) are stored as raw nanosecond uint64 values, which would
// degenerate the index into a per-span posting list with no value sharing. To
// keep them useful for time-bucketed and duration lookups they are truncated to
// millisecond precision during extraction only (NOTE-VI-027, issue #415): the
// stored block columns remain nanosecond precision for range queries. The column
// type is unchanged (still uint64 milliseconds, not a duration type).
//
// Columns policy.Allowed rejects are skipped; a disabled (zero-value) policy
// indexes every column. yield is called once per present span value; if it
// returns an error, extraction stops and returns that error. A column absent
// from a block simply produces no calls.
//
// TimeSec on each entry is the span's start time in seconds, resolved from the
// span:start intrinsic by (blockIdx, rowIdx); it is 0 when the file has no
// span:start column.
func ExtractValueIndexEntries(
	r *Reader,
	policy ColumnPolicy,
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
	return extractBlockColumns(r, policy, startSecByRef, blockRefByIdx, yield)
}

// ExtractValueIndexEntriesForColumns is a thin allowlist wrapper around
// ExtractValueIndexEntries for the #496 backfill engine's use (plan.md Section
// 4.5): it reuses the exact same per-block walk and column-value decoding,
// filtering at the yield boundary to only pass through entries for columns
// named in allowlist. This keeps extractBlockColumns' policy semantics and
// every existing caller (WriteValueIndexL0, valueindexconsumer) completely
// unchanged — the backfill engine's "index only this one triggered column
// against history" requirement is satisfied without touching the extraction
// internals at all. Extraction itself runs with a disabled (zero-value)
// policy so structural columns (span:id, trace:id, etc.) still flow through
// unfiltered; allowlist is applied only at the yield boundary.
func ExtractValueIndexEntriesForColumns(
	r *Reader,
	allowlist map[string]struct{},
	yield func(ValueIndexEntry) error,
) error {
	return ExtractValueIndexEntries(r, ColumnPolicy{}, func(e ValueIndexEntry) error {
		if _, ok := allowlist[e.ColName]; !ok {
			return nil
		}
		return yield(e)
	})
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

// buildSpanStartSecByRef builds a packed-key (uint32(blockIdx)<<16 | rowIdx) →
// minute-aligned-seconds map from the span:start block column, converting
// nanoseconds to whole seconds and flooring to the minute (60s) boundary for
// cardinality reduction (NOTE-VI-051). The read-side query bound in tempo-mrd's
// nanoWindowToSec MUST floor to the same 60s alignment or legitimate search
// matches near the start of a query window are silently dropped — this is a
// coordinated, cross-repo invariant, not a standalone change (SPEC-VI-4).
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
			// SPEC-VI-4, NOTE-VI-051: TimeSec is floored to the minute boundary.
			const secondsPerMinute = 60
			m[key] = (v / 1_000_000_000) / secondsPerMinute * secondsPerMinute
		}
	}
	return m
}

// extractBlockColumns parses each inner block one at a time and yields every present
// value of every column policy.Allowed permits. NOTE-436: this is the only extraction
// path — all columns live in inner blocks.
func extractBlockColumns(
	r *modules_reader.Reader,
	policy ColumnPolicy,
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
		// Look up span:parent_id column once per block (Stage 1, traceindex.go wiring).
		// The writer only sets this column when ParentSpanId is non-empty (spanmatch.go),
		// so a root span's row is legitimately absent from this column, not zero-but-present.
		parentSpanIDCol := block.Block.GetColumn(modules_shared.SpanParentIDColumnName)
		// Look up trace:id column once per block (Stage 5 fix, traceindex.go wiring plan):
		// every yielded entry must carry its row's TraceID, not just the entries for the
		// trace:id column itself -- ColumnEntry.TraceID/ValueIndexEntry.TraceID were
		// otherwise always zero in production (only hand-built test fixtures set it),
		// which silently broke any TraceID-keyed consumer of the value index, including
		// the new trace-group buffer's per-row grouping.
		traceIDCol := block.Block.GetColumn(modules_shared.TraceIDColumnName)
		for colKey, col := range block.Block.Columns() {
			// SPEC-VI-11: ColumnPolicy enforcement point for extraction.
			if !policy.Allowed(colKey.Name) {
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
				var parentSpanID [8]byte
				if parentSpanIDCol != nil && parentSpanIDCol.IsPresent(row) {
					if v, ok2 := parentSpanIDCol.BytesValue(row); ok2 && len(v) == 8 {
						copy(parentSpanID[:], v)
					}
				}
				var traceID [16]byte
				if traceIDCol != nil {
					if v, ok2 := traceIDCol.BytesValue(row); ok2 && len(v) == 16 {
						copy(traceID[:], v)
					}
				}
				if err := yield(ValueIndexEntry{
					ColName:      colKey.Name,
					Value:        val,
					ColType:      colType,
					BlockRef:     blockRef,
					TimeSec:      startSecByRef[key],
					SpanID:       spanID,
					ParentSpanID: parentSpanID,
					TraceID:      traceID,
					RowIdx:       row,
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

package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// NOTE-100: Block-boundary scatter for the structural hot path.
// See executor/NOTES.md NOTE-100 and shared/intrinsic_ref_index.go BlockRefRange (NOTE-016).

import (
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// lookupIntrinsicFieldsTypedForBlock reads intrinsic column values for all spans in a
// single block, identified by blockIdx and spanCount. result[rowIdx] holds the fields for
// the span at that row index. wantCols limits which columns are loaded (nil = all).
//
// This replaces lookupIntrinsicFieldsTyped(r, allRefs, wantCols) for the structural path
// where allRefs is always the full sorted range rowIdx 0..spanCount-1 for a fixed blockIdx.
// Complexity: O(K × (log(B×N) + N)) vs O(K × N × log(B×N)) for the generic path,
// where K = column count, B = file block count, N = spans per block. NOTE-100.
//
// SPEC-ROOT-010: I/O errors must not be silently swallowed — all errors are wrapped and returned.
func lookupIntrinsicFieldsTypedForBlock(
	r *modules_reader.Reader,
	blockIdx uint16,
	spanCount int,
	wantCols map[string]struct{},
) ([]intrinsicRowFields, error) {
	// NOTE-349: pooled, zeroed scratch — the caller fully consumes the returned slice in a
	// single row loop and then hands it back via putIntrinsicRowFields. Avoids one
	// SpanCount-sized heap alloc (and its GC scan) per block per query.
	result := getIntrinsicRowFields(spanCount)

	wantCol := func(name string) bool {
		if wantCols == nil {
			return true
		}
		_, ok := wantCols[name]
		return ok
	}

	for _, colName := range r.IntrinsicColumnNames() {
		if !wantCol(colName) {
			continue
		}
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil {
			putIntrinsicRowFields(result)
			return nil, fmt.Errorf("lookupIntrinsicFieldsTypedForBlock: GetIntrinsicColumn %q: %w", colName, err)
		}
		if col == nil {
			continue
		}
		populateTypedColumnForBlock(colName, col, blockIdx, result)
	}

	// span:end is synthesized — not in IntrinsicColumnNames() — handle explicitly.
	if wantCol(colNameSpanEnd) {
		col, err := r.GetIntrinsicColumn(colNameSpanEnd)
		if err != nil {
			putIntrinsicRowFields(result)
			return nil, fmt.Errorf("lookupIntrinsicFieldsTypedForBlock: GetIntrinsicColumn %q: %w", colNameSpanEnd, err)
		}
		if col != nil {
			populateTypedColumnForBlock(colNameSpanEnd, col, blockIdx, result)
		}
	}

	// NOTE-476 (issue #394): identity columns (trace:id/span:id/span:parent_id) may be absent
	// from the IntrinsicTOC for blocks written without them — those blocks store identity
	// solely in the SpanTree. Fall back to the per-block SpanTree identity map for any
	// requested identity column the loop above did not populate.
	if err := fillIdentityFromSpanTreeBlock(r, blockIdx, wantCol, result); err != nil {
		putIntrinsicRowFields(result)
		return nil, err
	}

	return result, nil
}

// fillIdentityFromSpanTreeBlock populates trace:id/span:id/span:parent_id from the block's
// SpanTree identity map for any of the three columns that is requested but absent from the
// IntrinsicTOC. Identity columns are present-together in any given block (all three or none),
// so the SpanTree map is fetched once if any is missing. result is indexed by RowIdx, which
// is exactly the SpanTree map key. NOTE-476 (issue #394).
func fillIdentityFromSpanTreeBlock(
	r *modules_reader.Reader,
	blockIdx uint16,
	wantCol func(string) bool,
	result []intrinsicRowFields,
) error {
	wantTrace := wantCol(colNameTraceID) && !r.HasIntrinsicColumn(colNameTraceID)
	wantSpan := wantCol(colNameSpanID) && !r.HasIntrinsicColumn(colNameSpanID)
	wantParent := wantCol(colNameParentID) && !r.HasIntrinsicColumn(colNameParentID)
	if !wantTrace && !wantSpan && !wantParent {
		return nil
	}
	idMap, err := r.SpanTreeIdentityForBlock(blockIdx)
	if err != nil {
		return fmt.Errorf("lookupIntrinsicFieldsTypedForBlock: SpanTreeIdentityForBlock: %w", err)
	}
	if idMap == nil {
		return nil
	}
	for rowIdx, rec := range idMap {
		idx := int(rowIdx)
		if idx >= len(result) {
			continue
		}
		if wantTrace {
			result[idx].traceID = rec.TraceID
			result[idx].present |= intrinsicPresentTraceID
		}
		if wantSpan {
			result[idx].spanID = rec.SpanID
			result[idx].present |= intrinsicPresentSpanID
		}
		if wantParent {
			result[idx].parentID = rec.ParentID
			result[idx].present |= intrinsicPresentParentID
		}
	}
	return nil
}

// populateTypedColumnForBlock fills one column's values into result for all spans
// in blockIdx (NOTE-100).
//
// Dict columns (NOTE-423) scatter directly from col.DictEntries, skipping EnsureRefIndex
// (and its radix/low-16 sort) entirely — the full-block scatter indexes result by the ref's
// RowIdx and never needs the sorted refIndex. Flat/uint64 identity columns use the
// BlockRefRange scatter pattern (usually short-circuited by the flat-dense DenseFlatRange path).
//
// The colName switch is hoisted outside the scatter loop so each column type executes
// a tight, branch-free typed write with no any boxing per entry. This eliminates the
// per-entry interface allocation that storeTypedField/LookupRefFast incur.
//
// Each column type is dispatched to a dedicated scatter function to keep cyclomatic
// complexity per function low. Format note: IntrinsicFormatXORBytes and
// IntrinsicFormatDeltaUint64 decode into the same BytesValues/Uint64Values arrays as
// IntrinsicFormatFlat (per shared SPECS.md §9); the refIndex Pos is format-agnostic
// after EnsureRefIndex builds it.
//
// UPDATE ME: add a new case when new intrinsic columns are introduced in constants.go.
func populateTypedColumnForBlock(
	colName string,
	col *modules_shared.IntrinsicColumn,
	blockIdx uint16,
	result []intrinsicRowFields,
) {
	// NOTE-354: flat-dense columns (span:start/end/duration/trace:id/span:id/parent:id on the
	// dominant single-block fully-present shape) dropped their refIndex slice — pos == rank ==
	// (rowIdx - minRow). Scatter directly from the synthesized (minRow, count) range without
	// materializing []RefIndexEntry. Only the flat (uint64/bytes) columns can be flat-dense;
	// dict columns (span:name/service.name/status/kind) keep refIndex and take the path below.
	if minRow, count, ok := col.DenseFlatRange(blockIdx); ok {
		switch colName {
		case colNameTraceID:
			scatterTraceIDDense(minRow, count, col.BytesValues, result)
		case colNameSpanID:
			scatterSpanIDDense(minRow, count, col.BytesValues, result)
		case colNameParentID:
			scatterParentIDDense(minRow, count, col.BytesValues, result)
		case colNameSpanStart:
			scatterSpanStartDense(minRow, count, col.Uint64Values, result)
		case colNameSpanEnd:
			scatterSpanEndDense(minRow, count, col.Uint64Values, result)
		case colNameSpanDuration:
			scatterSpanDurationDense(minRow, count, col.Uint64Values, result)
		}
		return
	}

	// NOTE-423: dict columns scatter directly from DictEntries for the target block, skipping
	// EnsureRefIndex (and its radix sort / dense-detection) entirely. The full-block scatter
	// writes result[rowIdx] indexed by the ref's low-16 RowIdx and is order-INDEPENDENT: it
	// never does a reverse (packedRef -> pos) lookup, so the sorted refIndex that BlockRefRange
	// builds is pure waste on this path. radixSortRefIndexPrepared was the #1 self-time frame on
	// the structural Q9 profile (the union of block sets across both nodes drives many
	// per-block dict ref-index builds). Walking DictEntries here emits (rowIdx, entryIdx) pairs
	// in decode order and scatters them straight into result — same writes, same final state,
	// no sort. Flat/uint64 identity columns keep the BlockRefRange path: they are usually
	// flat-dense (handled above) and otherwise rare on this path.
	if col.Format == modules_shared.IntrinsicFormatDict {
		// NOTE-424: for a MULTI-BLOCK dict column the direct-DictEntries scatter (NOTE-423)
		// re-walks every entry's every ref on EACH per-block call, filtering by blockIdx —
		// O(totalRefs) per block, so O(N_blocks × totalRefs) across the file. That is the
		// structural Q9 blowup (scatterSpanKindDict was the #1 self-time frame at ~5s).
		// Build the sorted refIndex ONCE (cached via EnsureRefIndex) and binary-search this
		// block's contiguous range instead — O(log totalRefs + blockRefs) per block. The
		// single-block case keeps the NOTE-423 direct scatter (no sort needed, the dominant
		// shape). DictMultiBlock caches its O(totalRefs) one-time scan, free against the
		// (N_blocks − 1) full re-scans it removes.
		if col.DictMultiBlock() {
			entries := col.BlockRefRange(blockIdx)
			if len(entries) == 0 {
				return
			}
			switch colName {
			case colNameSpanName:
				scatterSpanNameDictRefIndex(entries, col.DictEntries, result)
			case colNameServiceName:
				scatterServiceNameDictRefIndex(entries, col.DictEntries, result)
			case colNameStatusMessage:
				scatterStatusMessageDictRefIndex(entries, col.DictEntries, result)
			case colNameSpanKind:
				scatterSpanKindDictRefIndex(entries, col.DictEntries, result)
			case colNameSpanStatus:
				scatterSpanStatusDictRefIndex(entries, col.DictEntries, result)
				// Unknown dict column names are silently skipped.
			}
			return
		}
		switch colName {
		case colNameSpanName:
			scatterSpanNameDict(col.DictEntries, blockIdx, result)
		case colNameServiceName:
			scatterServiceNameDict(col.DictEntries, blockIdx, result)
		case colNameStatusMessage:
			scatterStatusMessageDict(col.DictEntries, blockIdx, result)
		case colNameSpanKind:
			scatterSpanKindDict(col.DictEntries, blockIdx, result)
		case colNameSpanStatus:
			scatterSpanStatusDict(col.DictEntries, blockIdx, result)
			// Unknown dict column names are silently skipped (consistent with storeTypedField).
		}
		return
	}

	entries := col.BlockRefRange(blockIdx)
	if len(entries) == 0 {
		return
	}
	switch colName {
	case colNameTraceID:
		scatterTraceID(entries, col.BytesValues, result)
	case colNameSpanID:
		scatterSpanID(entries, col.BytesValues, result)
	case colNameParentID:
		scatterParentID(entries, col.BytesValues, result)
	case colNameSpanStart:
		scatterSpanStart(entries, col.Uint64Values, result)
	case colNameSpanEnd:
		scatterSpanEnd(entries, col.Uint64Values, result)
	case colNameSpanDuration:
		scatterSpanDuration(entries, col.Uint64Values, result)
		// Unknown column names are silently skipped (future-proof; consistent with storeTypedField).
	}
}

// NOTE-423: dict scatter variants that walk DictEntries directly for one block, skipping the
// sorted refIndex build. For each dict entry, every BlockRef whose BlockIdx matches blockIdx
// contributes one scatter write result[ref.RowIdx] = entry value. This visits the same
// (rowIdx, entryIdx) pairs BlockRefRange would yield, just in decode order rather than sorted
// by packed ref — which is irrelevant because the scatter indexes result by rowIdx. The
// per-row bounds check (rowIdx >= len(result)) mirrors the general scatter variants exactly.

func scatterSpanNameDict(
	dictEntries []modules_shared.IntrinsicDictEntry,
	blockIdx uint16,
	result []intrinsicRowFields,
) {
	for pos := range dictEntries {
		val := dictEntries[pos].Value
		for _, ref := range dictEntries[pos].BlockRefs {
			if ref.BlockIdx != blockIdx {
				continue
			}
			rowIdx := int(ref.RowIdx)
			if rowIdx >= len(result) {
				continue
			}
			result[rowIdx].spanName = val
			result[rowIdx].present |= intrinsicPresentSpanName
		}
	}
}

func scatterServiceNameDict(
	dictEntries []modules_shared.IntrinsicDictEntry,
	blockIdx uint16,
	result []intrinsicRowFields,
) {
	for pos := range dictEntries {
		val := dictEntries[pos].Value
		for _, ref := range dictEntries[pos].BlockRefs {
			if ref.BlockIdx != blockIdx {
				continue
			}
			rowIdx := int(ref.RowIdx)
			if rowIdx >= len(result) {
				continue
			}
			result[rowIdx].serviceName = val
			result[rowIdx].present |= intrinsicPresentServiceName
		}
	}
}

func scatterStatusMessageDict(
	dictEntries []modules_shared.IntrinsicDictEntry,
	blockIdx uint16,
	result []intrinsicRowFields,
) {
	for pos := range dictEntries {
		val := dictEntries[pos].Value
		for _, ref := range dictEntries[pos].BlockRefs {
			if ref.BlockIdx != blockIdx {
				continue
			}
			rowIdx := int(ref.RowIdx)
			if rowIdx >= len(result) {
				continue
			}
			result[rowIdx].statusMessage = val
			result[rowIdx].present |= intrinsicPresentStatusMessage
		}
	}
}

func scatterSpanKindDict(
	dictEntries []modules_shared.IntrinsicDictEntry,
	blockIdx uint16,
	result []intrinsicRowFields,
) {
	for pos := range dictEntries {
		val := dictEntries[pos].Int64Val
		for _, ref := range dictEntries[pos].BlockRefs {
			if ref.BlockIdx != blockIdx {
				continue
			}
			rowIdx := int(ref.RowIdx)
			if rowIdx >= len(result) {
				continue
			}
			result[rowIdx].spanKind = val
			result[rowIdx].present |= intrinsicPresentSpanKind
		}
	}
}

func scatterSpanStatusDict(
	dictEntries []modules_shared.IntrinsicDictEntry,
	blockIdx uint16,
	result []intrinsicRowFields,
) {
	for pos := range dictEntries {
		val := dictEntries[pos].Int64Val
		for _, ref := range dictEntries[pos].BlockRefs {
			if ref.BlockIdx != blockIdx {
				continue
			}
			rowIdx := int(ref.RowIdx)
			if rowIdx >= len(result) {
				continue
			}
			result[rowIdx].spanStatus = val
			result[rowIdx].present |= intrinsicPresentSpanStatus
		}
	}
}

// NOTE-424: refIndex-based dict scatter variants for MULTI-BLOCK columns. entries is the
// pre-sorted, block-bounded window returned by BlockRefRange(blockIdx): every entry's high-16
// BlockIdx already equals blockIdx (the binary-search range), so no per-ref blockIdx filter is
// needed. entry.Pos is the dict-entry index; entry.Packed&0xFFFF is the row index. Each entry
// contributes one scatter write result[rowIdx] = DictEntries[Pos].value — the same (rowIdx,
// value) pairs the direct DictEntries scatter would produce for this block, sourced from the
// cached sorted index instead of an O(totalRefs) re-walk. The per-row bounds check mirrors the
// direct variants exactly.

func scatterSpanNameDictRefIndex(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for i := range entries {
		rowIdx := int(entries[i].Packed & 0xFFFF)
		if rowIdx >= len(result) {
			continue
		}
		result[rowIdx].spanName = dictEntries[entries[i].Pos].Value
		result[rowIdx].present |= intrinsicPresentSpanName
	}
}

func scatterServiceNameDictRefIndex(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for i := range entries {
		rowIdx := int(entries[i].Packed & 0xFFFF)
		if rowIdx >= len(result) {
			continue
		}
		result[rowIdx].serviceName = dictEntries[entries[i].Pos].Value
		result[rowIdx].present |= intrinsicPresentServiceName
	}
}

func scatterStatusMessageDictRefIndex(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for i := range entries {
		rowIdx := int(entries[i].Packed & 0xFFFF)
		if rowIdx >= len(result) {
			continue
		}
		result[rowIdx].statusMessage = dictEntries[entries[i].Pos].Value
		result[rowIdx].present |= intrinsicPresentStatusMessage
	}
}

func scatterSpanKindDictRefIndex(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for i := range entries {
		rowIdx := int(entries[i].Packed & 0xFFFF)
		if rowIdx >= len(result) {
			continue
		}
		result[rowIdx].spanKind = dictEntries[entries[i].Pos].Int64Val
		result[rowIdx].present |= intrinsicPresentSpanKind
	}
}

func scatterSpanStatusDictRefIndex(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for i := range entries {
		rowIdx := int(entries[i].Packed & 0xFFFF)
		if rowIdx >= len(result) {
			continue
		}
		result[rowIdx].spanStatus = dictEntries[entries[i].Pos].Int64Val
		result[rowIdx].present |= intrinsicPresentSpanStatus
	}
}

// NOTE-354: dense flat scatter variants. For a flat-dense column (refIndex dropped) the
// entries are the identity range [refDenseMin, refDenseMin+count): the i-th present row has
// rowIdx == minRow+i and its value sits at Uint64Values/BytesValues position i. Iterating the
// synthesized range with a single counter avoids both the per-call []RefIndexEntry
// reconstruction and the 8-byte/row retained slice (its sole reason to exist was to map
// rowIdx->pos, which here is a fixed offset). Each variant mirrors its general counterpart's
// per-row body exactly (same bounds checks, same present bit, same source array).

// NOTE-378: hoist the two per-row bounds checks (rowIdx < len(result), i < len(vals)) out of
// the dense scatter loops. On the structural path lookupIntrinsicFieldsTypedForBlock calls
// these once per intrinsic column per block over the full SpanCount range, so the checks run
// O(SpanCount × columns) times per block. The dense invariant (NOTE-354) is rowIdx == minRow+i
// and value position == i for i in [0,count); since minRow, count and both slice lengths are
// known up front, the safe iteration count n = min(count, len(vals), len(result)-minRow) can
// be computed once. Re-slicing the destination to res := result[minRow:minRow+n] and the
// source to vals[:n] makes res[i] and vals[i] provably in range for i < n, so the compiler
// discharges both checks from the loop body. When minRow >= len(result) (degenerate, should
// not happen on a valid dense column) n clamps to 0 and the loop is skipped — identical
// behavior to the former per-row `continue`. The shorter-slice clamp preserves the former
// `i >= len(vals)` guard exactly: rows past either slice end were skipped before and are not
// iterated now.

// denseScatterBound returns the safe loop count n and the result sub-slice aligned so that
// res[i] corresponds to source position i, for a dense column with the given minRow/count.
func denseScatterBound(
	minRow uint32,
	count, valsLen int,
	result []intrinsicRowFields,
) (res []intrinsicRowFields, n int) {
	start := int(minRow)
	if start >= len(result) {
		return nil, 0
	}
	n = count
	if avail := len(result) - start; n > avail {
		n = avail
	}
	if n > valsLen {
		n = valsLen
	}
	if n <= 0 {
		return nil, 0
	}
	return result[start : start+n], n
}

func scatterTraceIDDense(minRow uint32, count int, bytesVals [][]byte, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(bytesVals), result)
	src := bytesVals[:n:n]
	res = res[:n:n] // tie len(res) == len(src) == n so the loop body needs no bounds check
	for i := range src {
		// NOTE-426: trace IDs are fixed OTel-spec 16-byte values — direct [16]byte array
		// conversion (single load+store, one bounds check) over the copy() builtin's
		// min(16, len(b)) memmove-style lowering. Matches the span/parent ID scatter below.
		b := src[i]
		if len(b) == traceIDByteLen {
			res[i].traceID = [traceIDByteLen]byte(b)
			res[i].present |= intrinsicPresentTraceID
		}
	}
}

func scatterSpanIDDense(minRow uint32, count int, bytesVals [][]byte, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(bytesVals), result)
	src := bytesVals[:n:n]
	res = res[:n:n]
	for i := range src {
		// NOTE-426: span IDs are fixed OTel-spec 8-byte values. When len==8 use a direct
		// [8]byte array conversion (single 8-byte load+store, one bounds check) instead of
		// copy8's `copy(dst[:], b)` builtin, which computes min(8, len(b)) and lowers to a
		// runtime memmove-style sequence. This is the dominant identity scatter on the
		// structural hot path (flat-dense single-block span:id), where copy8 was the #1
		// blockpack self-time frame. The len guard preserves OTel-spec safety (non-8-byte
		// values are skipped, as in copy8).
		b := src[i]
		if len(b) == spanIDByteLen {
			res[i].spanID = [spanIDByteLen]byte(b)
			res[i].present |= intrinsicPresentSpanID
		}
	}
}

func scatterParentIDDense(minRow uint32, count int, bytesVals [][]byte, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(bytesVals), result)
	src := bytesVals[:n:n]
	res = res[:n:n]
	for i := range src {
		// NOTE-426: see scatterSpanIDDense — direct [8]byte conversion over copy8.
		b := src[i]
		if len(b) == spanIDByteLen {
			res[i].parentID = [spanIDByteLen]byte(b)
			res[i].present |= intrinsicPresentParentID
		}
	}
}

func scatterSpanStartDense(minRow uint32, count int, uint64Vals []uint64, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(uint64Vals), result)
	src := uint64Vals[:n:n]
	res = res[:n:n]
	for i := range src {
		res[i].spanStart = src[i]
		res[i].present |= intrinsicPresentSpanStart
	}
}

func scatterSpanEndDense(minRow uint32, count int, uint64Vals []uint64, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(uint64Vals), result)
	src := uint64Vals[:n:n]
	res = res[:n:n]
	for i := range src {
		res[i].spanEnd = src[i]
		res[i].present |= intrinsicPresentSpanEnd
	}
}

func scatterSpanDurationDense(minRow uint32, count int, uint64Vals []uint64, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(uint64Vals), result)
	src := uint64Vals[:n:n]
	res = res[:n:n]
	for i := range src {
		res[i].spanDuration = src[i]
		res[i].present |= intrinsicPresentSpanDuration
	}
}

// scatterTraceID copies the 16-byte trace ID into each result row.
func scatterTraceID(
	entries []modules_shared.RefIndexEntry,
	bytesVals [][]byte,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(
			uint16(e.Packed), //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(bytesVals) {
			continue
		}
		// NOTE-426: direct [16]byte conversion over the copy() builtin.
		if b := bytesVals[pos]; len(b) == traceIDByteLen {
			result[rowIdx].traceID = [traceIDByteLen]byte(b)
			result[rowIdx].present |= intrinsicPresentTraceID
		}
	}
}

// scatterSpanID copies the 8-byte span ID into each result row.
// NOTE-093: [8]byte eliminates clone; copy8 enforces OTel spec spanIDByteLen requirement.
func scatterSpanID(
	entries []modules_shared.RefIndexEntry,
	bytesVals [][]byte,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(
			uint16(e.Packed), //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(bytesVals) {
			continue
		}
		// NOTE-426: direct [8]byte conversion over copy8 (single load+store).
		if b := bytesVals[pos]; len(b) == spanIDByteLen {
			result[rowIdx].spanID = [spanIDByteLen]byte(b)
			result[rowIdx].present |= intrinsicPresentSpanID
		}
	}
}

// scatterParentID copies the 8-byte parent span ID into each result row.
// NOTE-093: [8]byte eliminates clone; copy8 enforces OTel spec spanIDByteLen requirement.
func scatterParentID(
	entries []modules_shared.RefIndexEntry,
	bytesVals [][]byte,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(
			uint16(e.Packed), //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(bytesVals) {
			continue
		}
		// NOTE-426: direct [8]byte conversion over copy8 (single load+store).
		if b := bytesVals[pos]; len(b) == spanIDByteLen {
			result[rowIdx].parentID = [spanIDByteLen]byte(b)
			result[rowIdx].present |= intrinsicPresentParentID
		}
	}
}

// NOTE-345: the per-column scatter loops below write the destination field DIRECTLY
// instead of through a `set func(*intrinsicRowFields, T)` closure passed per call. The
// closure variant (former scatterUint64Field/scatterDictStringField/scatterDictInt64Field)
// forced an indirect, non-inlinable call on EVERY scattered entry — O(spans) per column on
// the metrics intrinsic path (M4/M6/M8 read span:duration/span:kind/service.name through
// these). Inlining the field store removes that per-entry call and lets the compiler keep
// the bounds-checked loop tight, mirroring the already-direct scatterTraceID/SpanID/ParentID.
// Each function below has the identical body shape (the closure was the only difference), so
// dispatch correctness is unchanged: the colName switch in populateTypedColumnForBlock picks
// exactly one, and the present bit / source array match the former call's arguments.

// scatterSpanStart scatters a flat uint64 column into result[*].spanStart.
func scatterSpanStart(entries []modules_shared.RefIndexEntry, uint64Vals []uint64, result []intrinsicRowFields) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(uint64Vals) {
			continue
		}
		result[rowIdx].spanStart = uint64Vals[pos]
		result[rowIdx].present |= intrinsicPresentSpanStart
	}
}

// scatterSpanEnd scatters a flat uint64 column into result[*].spanEnd.
func scatterSpanEnd(entries []modules_shared.RefIndexEntry, uint64Vals []uint64, result []intrinsicRowFields) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(uint64Vals) {
			continue
		}
		result[rowIdx].spanEnd = uint64Vals[pos]
		result[rowIdx].present |= intrinsicPresentSpanEnd
	}
}

// scatterSpanDuration scatters a flat uint64 column into result[*].spanDuration.
func scatterSpanDuration(entries []modules_shared.RefIndexEntry, uint64Vals []uint64, result []intrinsicRowFields) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(uint64Vals) {
			continue
		}
		result[rowIdx].spanDuration = uint64Vals[pos]
		result[rowIdx].present |= intrinsicPresentSpanDuration
	}
}

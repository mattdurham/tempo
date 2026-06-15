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

	return result, nil
}

// populateTypedColumnForBlock fills one column's values into result for all spans
// in blockIdx using the BlockRefRange scatter pattern (NOTE-100).
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
	case colNameSpanName:
		scatterSpanName(entries, col.DictEntries, result)
	case colNameServiceName:
		scatterServiceName(entries, col.DictEntries, result)
	case colNameStatusMessage:
		scatterStatusMessage(entries, col.DictEntries, result)
	case colNameSpanKind:
		scatterSpanKind(entries, col.DictEntries, result)
	case colNameSpanStatus:
		scatterSpanStatus(entries, col.DictEntries, result)
		// Unknown column names are silently skipped (future-proof; consistent with storeTypedField).
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
		b := src[i]
		if len(b) == traceIDByteLen {
			copy(res[i].traceID[:], b)
			res[i].present |= intrinsicPresentTraceID
		}
	}
}

func scatterSpanIDDense(minRow uint32, count int, bytesVals [][]byte, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(bytesVals), result)
	src := bytesVals[:n:n]
	res = res[:n:n]
	for i := range src {
		if copy8(&res[i].spanID, src[i]) {
			res[i].present |= intrinsicPresentSpanID
		}
	}
}

func scatterParentIDDense(minRow uint32, count int, bytesVals [][]byte, result []intrinsicRowFields) {
	res, n := denseScatterBound(minRow, count, len(bytesVals), result)
	src := bytesVals[:n:n]
	res = res[:n:n]
	for i := range src {
		if copy8(&res[i].parentID, src[i]) {
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
		b := bytesVals[pos]
		if len(b) == traceIDByteLen {
			copy(result[rowIdx].traceID[:], b)
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
		if copy8(&result[rowIdx].spanID, bytesVals[pos]) {
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
		if copy8(&result[rowIdx].parentID, bytesVals[pos]) {
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

// scatterSpanName scatters a dict string column into result[*].spanName.
func scatterSpanName(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		result[rowIdx].spanName = dictEntries[pos].Value
		result[rowIdx].present |= intrinsicPresentSpanName
	}
}

// scatterServiceName scatters a dict string column into result[*].serviceName.
func scatterServiceName(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		result[rowIdx].serviceName = dictEntries[pos].Value
		result[rowIdx].present |= intrinsicPresentServiceName
	}
}

// scatterStatusMessage scatters a dict string column into result[*].statusMessage.
func scatterStatusMessage(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		result[rowIdx].statusMessage = dictEntries[pos].Value
		result[rowIdx].present |= intrinsicPresentStatusMessage
	}
}

// scatterSpanKind scatters a dict int64 column into result[*].spanKind.
func scatterSpanKind(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		result[rowIdx].spanKind = dictEntries[pos].Int64Val
		result[rowIdx].present |= intrinsicPresentSpanKind
	}
}

// scatterSpanStatus scatters a dict int64 column into result[*].spanStatus.
func scatterSpanStatus(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits are rowIdx; blockIdx in high 16 bits
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		result[rowIdx].spanStatus = dictEntries[pos].Int64Val
		result[rowIdx].present |= intrinsicPresentSpanStatus
	}
}

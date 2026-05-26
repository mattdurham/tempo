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
	result := make([]intrinsicRowFields, spanCount)

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
		scatterUint64Field(entries, col.Uint64Values, result, intrinsicPresentSpanStart,
			func(r *intrinsicRowFields, v uint64) { r.spanStart = v })
	case colNameSpanEnd:
		scatterUint64Field(entries, col.Uint64Values, result, intrinsicPresentSpanEnd,
			func(r *intrinsicRowFields, v uint64) { r.spanEnd = v })
	case colNameSpanDuration:
		scatterUint64Field(entries, col.Uint64Values, result, intrinsicPresentSpanDuration,
			func(r *intrinsicRowFields, v uint64) { r.spanDuration = v })
	case colNameSpanName:
		scatterDictStringField(entries, col.DictEntries, result, intrinsicPresentSpanName,
			func(r *intrinsicRowFields, v string) { r.spanName = v })
	case colNameServiceName:
		scatterDictStringField(entries, col.DictEntries, result, intrinsicPresentServiceName,
			func(r *intrinsicRowFields, v string) { r.serviceName = v })
	case colNameStatusMessage:
		scatterDictStringField(entries, col.DictEntries, result, intrinsicPresentStatusMessage,
			func(r *intrinsicRowFields, v string) { r.statusMessage = v })
	case colNameSpanKind:
		scatterDictInt64Field(entries, col.DictEntries, result, intrinsicPresentSpanKind,
			func(r *intrinsicRowFields, v int64) { r.spanKind = v })
	case colNameSpanStatus:
		scatterDictInt64Field(entries, col.DictEntries, result, intrinsicPresentSpanStatus,
			func(r *intrinsicRowFields, v int64) { r.spanStatus = v })
		// Unknown column names are silently skipped (future-proof; consistent with storeTypedField).
	}
}

// scatterTraceID copies the 16-byte trace ID into each result row.
func scatterTraceID(
	entries []modules_shared.RefIndexEntry,
	bytesVals [][]byte,
	result []intrinsicRowFields,
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
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
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
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
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
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

// scatterUint64Field scatters a flat uint64 column into the designated field of each result row.
func scatterUint64Field(
	entries []modules_shared.RefIndexEntry,
	uint64Vals []uint64,
	result []intrinsicRowFields,
	presentBit uint16,
	set func(*intrinsicRowFields, uint64),
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(uint64Vals) {
			continue
		}
		set(&result[rowIdx], uint64Vals[pos])
		result[rowIdx].present |= presentBit
	}
}

// scatterDictStringField scatters a dict string column into the designated field of each result row.
func scatterDictStringField(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
	presentBit uint16,
	set func(*intrinsicRowFields, string),
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		set(&result[rowIdx], dictEntries[pos].Value)
		result[rowIdx].present |= presentBit
	}
}

// scatterDictInt64Field scatters a dict int64 column into the designated field of each result row.
func scatterDictInt64Field(
	entries []modules_shared.RefIndexEntry,
	dictEntries []modules_shared.IntrinsicDictEntry,
	result []intrinsicRowFields,
	presentBit uint16,
	set func(*intrinsicRowFields, int64),
) {
	for _, e := range entries {
		rowIdx := int(uint16(e.Packed)) //nolint:gosec // low 16 bits of Packed are the rowIdx (blockIdx in high 16 bits)
		if rowIdx >= len(result) {
			continue
		}
		pos := int(e.Pos)
		if pos >= len(dictEntries) {
			continue
		}
		set(&result[rowIdx], dictEntries[pos].Int64Val)
		result[rowIdx].present |= presentBit
	}
}

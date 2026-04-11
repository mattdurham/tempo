package blockpack

// query_helpers.go — shared helpers used by both TraceQL and LogQL query paths.

import (
	"encoding/hex"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// extractIDs extracts hex-encoded trace ID and span ID strings from a block row.
// Falls back to intrinsic section lookup maps when block columns are absent
// (intrinsic-only storage after dual-storage removal).
func extractIDs(
	block *modules_reader.Block, rowIdx int, blockIdx int,
	traceIDByRef, spanIDByRef map[uint32][]byte,
) (traceID, spanID string) {
	if block != nil {
		if col := block.GetColumn("trace:id"); col != nil {
			if v, ok := col.BytesValue(rowIdx); ok {
				traceID = hex.EncodeToString(v)
			}
		}
		if col := block.GetColumn("span:id"); col != nil {
			if v, ok := col.BytesValue(rowIdx); ok {
				spanID = hex.EncodeToString(v)
			}
		}
	}
	// Fall back to intrinsic section when block columns are absent.
	key := uint32(blockIdx)<<16 | uint32(rowIdx) //nolint:gosec // blockIdx and rowIdx are bounded
	if traceID == "" {
		if v, ok := traceIDByRef[key]; ok {
			traceID = hex.EncodeToString(v)
		}
	}
	if spanID == "" {
		if v, ok := spanIDByRef[key]; ok {
			spanID = hex.EncodeToString(v)
		}
	}
	return traceID, spanID
}

// buildIntrinsicBytesMap builds a packed-key → bytes lookup map from an intrinsic flat column.
// Key encoding: uint32(blockIdx)<<16 | uint32(rowIdx). Returns nil if the column is absent.
func buildIntrinsicBytesMap(r *modules_reader.Reader, colName string) map[uint32][]byte {
	col, err := r.GetIntrinsicColumn(colName)
	if err != nil || col == nil || len(col.BytesValues) == 0 {
		return nil
	}
	m := make(map[uint32][]byte, len(col.BlockRefs))
	for i, ref := range col.BlockRefs {
		if i < len(col.BytesValues) {
			key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec // ref values are bounded
			m[key] = col.BytesValues[i]
		}
	}
	return m
}

// buildIntrinsicBytesMapForRows builds a packed-key → bytes lookup map from an intrinsic flat
// column, restricted to the (blockIdx, rowIdx) pairs present in wantRows.
// wantRows maps blockID → []rowIdx (e.g. from GetTraceByID's rowsByBlock).
// This avoids scanning the entire column when only a small subset of blocks are relevant.
// Key encoding: uint32(blockIdx)<<16 | uint32(rowIdx). Returns nil if the column is absent.
func buildIntrinsicBytesMapForRows(r *modules_reader.Reader, colName string, wantRows map[int][]int) map[uint32][]byte {
	if len(wantRows) == 0 {
		return nil
	}
	col, err := r.GetIntrinsicColumn(colName)
	if err != nil || col == nil || len(col.BytesValues) == 0 {
		return nil
	}
	// Build a set of all (blockIdx, rowIdx) pairs we care about for fast lookup.
	type blockRow struct{ block, row uint16 }
	wantSet := make(map[blockRow]struct{})
	for blockID, rows := range wantRows {
		for _, rowIdx := range rows {
			wantSet[blockRow{uint16(blockID), uint16(rowIdx)}] = struct{}{} //nolint:gosec // bounded values
		}
	}
	m := make(map[uint32][]byte, len(wantSet))
	for i, ref := range col.BlockRefs {
		if i >= len(col.BytesValues) {
			break
		}
		if _, ok := wantSet[blockRow{ref.BlockIdx, ref.RowIdx}]; ok {
			key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec // ref values are bounded
			m[key] = col.BytesValues[i]
		}
	}
	return m
}

// hexEncodeField converts a field value (string or []byte) to a hex string.
// Used for trace:id and span:id which are stored as bytes in intrinsic columns.
func hexEncodeField(v any) string {
	switch b := v.(type) {
	case string:
		return b
	case []byte:
		return hex.EncodeToString(b)
	}
	return ""
}

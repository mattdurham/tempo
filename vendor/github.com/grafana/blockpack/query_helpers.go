package blockpack

// query_helpers.go — shared helpers used by the TraceQL query path.

import (
	"encoding/hex"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// extractIDs extracts hex-encoded trace ID and span ID strings from a block row.
// Falls back to intrinsic section lookup maps when block columns are absent
// (intrinsic-only storage after dual-storage removal).
func extractIDs(
	block *modules_reader.Block, rowIdx, blockIdx int,
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
//
// NOTE-476 (issue #394): for the three identity columns (trace:id/span:id/span:parent_id), if
// the IntrinsicTOC lacks the column but the file carries a SpanTree, the map is built from the
// SpanTree reverse map instead — those columns are omitted from the TOC for such files.
func buildIntrinsicBytesMap(r *modules_reader.Reader, colName string) map[uint32][]byte {
	col, err := r.GetIntrinsicColumn(colName)
	if err == nil && col != nil && len(col.BytesValues) > 0 {
		m := make(map[uint32][]byte, len(col.BlockRefs))
		for i, ref := range col.BlockRefs {
			if i < len(col.BytesValues) {
				key := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec // ref values are bounded
				m[key] = col.BytesValues[i]
			}
		}
		return m
	}
	if isIdentityColumnName(colName) && !r.HasIntrinsicColumn(colName) && r.HasSpanTree() {
		return buildIdentityBytesMapFromSpanTree(r, colName)
	}
	return nil
}

// isIdentityColumnName reports whether colName is one of the three identity columns that may
// be served from the SpanTree instead of the IntrinsicTOC. NOTE-476.
func isIdentityColumnName(colName string) bool {
	return colName == "trace:id" || colName == "span:id" || colName == "span:parent_id"
}

// buildIdentityBytesMapFromSpanTree builds a packed-key → identity-bytes map for an identity
// column by scanning every block's SpanTree reverse map. Key encoding matches buildIntrinsicBytesMap
// (uint32(blockIdx)<<16 | uint32(rowIdx)). NOTE-476 (issue #394).
func buildIdentityBytesMapFromSpanTree(r *modules_reader.Reader, colName string) map[uint32][]byte {
	m := make(map[uint32][]byte)
	for blockIdx := 0; blockIdx < r.BlockCount(); blockIdx++ {
		idMap, err := r.SpanTreeIdentityForBlock(uint16(blockIdx)) //nolint:gosec // bounded by BlockCount
		if err != nil || idMap == nil {
			continue
		}
		for rowIdx, rec := range idMap {
			key := uint32(blockIdx)<<16 | uint32(rowIdx) //nolint:gosec // bounded
			switch colName {
			case "trace:id":
				b := make([]byte, 16)
				copy(b, rec.TraceID[:])
				m[key] = b
			case "span:id":
				b := make([]byte, 8)
				copy(b, rec.SpanID[:])
				m[key] = b
			case "span:parent_id":
				if rec.ParentID != ([8]byte{}) {
					b := make([]byte, 8)
					copy(b, rec.ParentID[:])
					m[key] = b
				}
			}
		}
	}
	if len(m) == 0 {
		return nil
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

package blockpack

// query_helpers.go — shared helpers used by the TraceQL query path.

import (
	"encoding/hex"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// extractIDs extracts hex-encoded trace ID and span ID strings from a block row.
//
// NOTE-436: trace:id and span:id are regular per-row block columns. There is no
// intrinsic-section fallback — the columns are always present in the decoded block.
func extractIDs(block *modules_reader.Block, rowIdx int) (traceID, spanID string) {
	if block == nil {
		return "", ""
	}
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
	return traceID, spanID
}

package executor

// intrinsic_lookup.go — identity field extraction for the structural query path.
//
// NOTE-436 (corrected #490 A-11, per .bob/state/identity-investigation.md): the file-level
// IntrinsicTOC/SpanTree sections and the intrinsic-index-based lookupIntrinsicFieldsTyped/
// rowSatisfiesIntrinsicNodesTyped functions that once read from them were deleted under
// #433/#434/#436. identityFieldsFromBlockColsTyped below — reading identity directly from
// block payload columns — is the SOLE and CURRENT identity-population mechanism for the
// structural query path, for every file; it is not a legacy fallback and has no alternative
// to fall back from.

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// identityFieldsFromBlockColsTyped reads identity fields from block payload columns.
func identityFieldsFromBlockColsTyped(block *modules_reader.Block, n int) []intrinsicRowFields {
	traceCol := block.GetColumn("trace:id")
	spanCol := block.GetColumn("span:id")
	parentCol := block.GetColumn("span:parent_id")
	result := getIntrinsicRowFields(n)
	for rowIdx := range n {
		row := &result[rowIdx]
		if traceCol != nil {
			if v, ok := traceCol.BytesValue(rowIdx); ok && len(v) == traceIDByteLen {
				row.traceID = [traceIDByteLen]byte(v)
				row.present |= intrinsicPresentTraceID
			}
		}
		if spanCol != nil {
			if v, ok := spanCol.BytesValue(rowIdx); ok && copy8(&row.spanID, v) {
				row.present |= intrinsicPresentSpanID
			}
		}
		if parentCol != nil {
			if v, ok := parentCol.BytesValue(rowIdx); ok && copy8(&row.parentID, v) {
				row.present |= intrinsicPresentParentID
			}
		}
	}
	return result
}

// copy8 copies b into dst if len(b) == spanIDByteLen. Returns true if copied.
func copy8(dst *[spanIDByteLen]byte, b []byte) bool {
	if len(b) != spanIDByteLen {
		return false
	}
	*dst = [spanIDByteLen]byte(b)
	return true
}

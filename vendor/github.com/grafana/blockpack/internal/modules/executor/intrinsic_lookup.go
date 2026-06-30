package executor

// intrinsic_lookup.go — stubs/implementations for lookupIntrinsicFields* functions
// after IntrinsicTOC removal (#433/#436).
// After removal, identity and well-known fields come from block columns.

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// lookupIntrinsicFieldsTyped returns per-ref typed field values for predicate evaluation.
// After #433: IntrinsicTOC is gone; only SpanTree (also gone) and block columns remain.
// Returns an empty slice — callers fall through to block-column evaluation.
func lookupIntrinsicFieldsTyped(
	_ *modules_reader.Reader,
	selected []modules_shared.BlockRef,
	_ map[string]struct{},
) ([]intrinsicRowFields, error) {
	return getIntrinsicRowFields(len(selected)), nil
}

// lookupIntrinsicFieldsTypedForBlock returns per-row typed fields from block columns.
func lookupIntrinsicFieldsTypedForBlock(
	r *modules_reader.Reader,
	blockIdx uint16,
	spanCount int,
	_ map[string]struct{},
) ([]intrinsicRowFields, error) {
	result := getIntrinsicRowFields(spanCount)
	if r == nil || int(blockIdx) >= r.BlockCount() {
		return result, nil
	}
	bwb, err := r.GetBlockWithBytes(int(blockIdx), nil)
	if err != nil || bwb == nil {
		// Graceful degradation: a block fetch failure yields empty identity fields
		// (the caller falls back to block-column scan), not a hard query error.
		return result, nil //nolint:nilerr // intentional: degrade to empty fields on fetch failure
	}
	return identityFieldsFromBlockColsTyped(bwb.Block, spanCount), nil
}

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

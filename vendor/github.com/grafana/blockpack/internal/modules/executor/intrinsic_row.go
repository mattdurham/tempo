package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// NOTE-081: typed struct replaces []map[string]any in the structural hot path.

import (
	"fmt"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Intrinsic column name constants used in switch dispatch across multiple files.
// These are the canonical string names returned by the reader for each intrinsic column.
const (
	colNameTraceID       = "trace:id"
	colNameSpanID        = "span:id"
	colNameParentID      = "span:parent_id"
	colNameSpanName      = "span:name"
	colNameServiceName   = "resource.service.name"
	colNameStatusMessage = "span:status_message"
	colNameSpanStart     = "span:start"
	colNameSpanEnd       = "span:end"
	colNameSpanDuration  = "span:duration"
	colNameSpanKind      = "span:kind"
	colNameSpanStatus    = "span:status"
)

// spanIDByteLen is the fixed OTel spec byte length for span IDs (W3C TraceContext, 8 bytes).
const spanIDByteLen = 8

// traceIDByteLen is the fixed OTel spec byte length for trace IDs (W3C TraceContext, 16 bytes).
const traceIDByteLen = 16

// copy8 copies b into dst if len(b) == spanIDByteLen. Returns true if copied.
func copy8(dst *[8]byte, b []byte) bool {
	if len(b) != spanIDByteLen {
		return false
	}
	copy(dst[:], b)
	return true
}

// intrinsicRowFields holds per-row data for all 11 trace intrinsic columns.
// Used by the structural hot path to eliminate per-row map allocations.
// A single []intrinsicRowFields slice replaces []map[string]any (one alloc for N rows).
// Fields are ordered per betteralign output: strings first (16-byte headers), then 8-byte scalars
// (uint64, int64), then [8]byte fixed arrays, then smaller scalars (uint16), then [16]byte arrays.
// NOTE-093: [8]byte eliminates clone; [8]byte{} is the zero value; the present bitmask (field: present) is the authoritative absent indicator.
type intrinsicRowFields struct {
	spanName      string   // span:name (dict string)
	serviceName   string   // resource.service.name (dict string)
	statusMessage string   // span:status_message (dict string)
	spanStart     uint64   // span:start (flat uint64 nanoseconds)
	spanEnd       uint64   // span:end (synthesized flat uint64 nanoseconds)
	spanDuration  uint64   // span:duration (flat uint64 nanoseconds)
	spanKind      int64    // span:kind (dict int64)
	spanStatus    int64    // span:status (dict int64)
	spanID        [8]byte  // span:id ([8]byte value type; [8]byte{} if absent — see present bitmask)
	parentID      [8]byte  // span:parent_id ([8]byte value type; [8]byte{} if absent — see present bitmask)
	present       uint16   // bitmask: which fields were populated
	traceID       [16]byte // trace:id ([16]byte value type, always 16 bytes)
}

// Bitmask constants for intrinsicRowFields.present (one per field).
const (
	intrinsicPresentTraceID       uint16 = 1 << iota // bit 0
	intrinsicPresentSpanID                           // bit 1
	intrinsicPresentParentID                         // bit 2
	intrinsicPresentSpanName                         // bit 3
	intrinsicPresentServiceName                      // bit 4
	intrinsicPresentStatusMessage                    // bit 5
	intrinsicPresentSpanStart                        // bit 6
	intrinsicPresentSpanEnd                          // bit 7
	intrinsicPresentSpanDuration                     // bit 8
	intrinsicPresentSpanKind                         // bit 9
	intrinsicPresentSpanStatus                       // bit 10
)

// lookupIntrinsicFieldsTyped reads intrinsic column values for the given refs and returns
// one intrinsicRowFields per ref. wantCols limits which columns are loaded. A single
// []intrinsicRowFields allocation replaces N map allocations (one per row).
// When wantCols is nil, all columns including span:end are fetched.
//
// SPEC-ROOT-010: I/O errors must not be silently swallowed.
func lookupIntrinsicFieldsTyped(
	r *modules_reader.Reader,
	selected []modules_shared.BlockRef,
	wantCols map[string]struct{},
) ([]intrinsicRowFields, error) {
	result := make([]intrinsicRowFields, len(selected))

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
			return nil, fmt.Errorf("lookupIntrinsicFieldsTyped: GetIntrinsicColumn %q: %w", colName, err)
		}
		if col == nil {
			continue
		}
		populateTypedColumn(colName, col, selected, result)
	}

	// span:end is synthesized and not in IntrinsicColumnNames() — handle explicitly.
	if wantCol(colNameSpanEnd) {
		col, err := r.GetIntrinsicColumn(colNameSpanEnd)
		if err != nil {
			return nil, fmt.Errorf("lookupIntrinsicFieldsTyped: GetIntrinsicColumn %q: %w", colNameSpanEnd, err)
		}
		if col != nil {
			populateTypedColumn(colNameSpanEnd, col, selected, result)
		}
	}

	return result, nil
}

// populateTypedColumn fills one column's values into the result slice using LookupRefFast.
func populateTypedColumn(
	colName string,
	col *modules_shared.IntrinsicColumn,
	selected []modules_shared.BlockRef,
	result []intrinsicRowFields,
) {
	for i, ref := range selected {
		packed := uint32(ref.BlockIdx)<<16 | uint32(ref.RowIdx) //nolint:gosec
		val, ok := col.LookupRefFast(packed)
		if !ok {
			continue
		}
		storeTypedField(colName, val, &result[i])
	}
}

// storeTypedField writes val into the appropriate field of row and sets the present bit.
// Type assertions use ok-checks to skip unexpected wire formats without panicking.
func storeTypedField(colName string, val any, row *intrinsicRowFields) {
	switch colName {
	case colNameTraceID:
		if b, ok := val.([]byte); ok && len(b) == traceIDByteLen {
			copy(row.traceID[:], b)
			row.present |= intrinsicPresentTraceID
		}
	case colNameSpanID:
		// NOTE-093: [8]byte eliminates clone; blockio/shared NOTE-012 guarantees BytesValues are independent copies.
		// Non-spanIDByteLen values silently skipped per NOTE-093 (OTel spec forbids non-8-byte span IDs).
		if b, ok := val.([]byte); ok && copy8(&row.spanID, b) {
			row.present |= intrinsicPresentSpanID
		}
	case colNameParentID:
		// NOTE-093: [8]byte eliminates clone; blockio/shared NOTE-012 guarantees BytesValues are independent copies.
		// Non-spanIDByteLen values silently skipped per NOTE-093 (OTel spec forbids non-8-byte span IDs).
		if b, ok := val.([]byte); ok && copy8(&row.parentID, b) {
			row.present |= intrinsicPresentParentID
		}
	case colNameSpanName:
		if s, ok := val.(string); ok {
			row.spanName = s
			row.present |= intrinsicPresentSpanName
		}
	case colNameServiceName:
		if s, ok := val.(string); ok {
			row.serviceName = s
			row.present |= intrinsicPresentServiceName
		}
	case colNameStatusMessage:
		if s, ok := val.(string); ok {
			row.statusMessage = s
			row.present |= intrinsicPresentStatusMessage
		}
	case colNameSpanStart:
		if u, ok := val.(uint64); ok {
			row.spanStart = u
			row.present |= intrinsicPresentSpanStart
		}
	case colNameSpanEnd:
		if u, ok := val.(uint64); ok {
			row.spanEnd = u
			row.present |= intrinsicPresentSpanEnd
		}
	case colNameSpanDuration:
		if u, ok := val.(uint64); ok {
			row.spanDuration = u
			row.present |= intrinsicPresentSpanDuration
		}
	case colNameSpanKind:
		if i, ok := val.(int64); ok {
			row.spanKind = i
			row.present |= intrinsicPresentSpanKind
		}
	case colNameSpanStatus:
		if i, ok := val.(int64); ok {
			row.spanStatus = i
			row.present |= intrinsicPresentSpanStatus
		}
	}
}

// identityFieldsFromBlockColsTyped builds a per-row typed struct by reading
// trace:id, span:id, and span:parent_id directly from block columns.
// Used for legacy files (no intrinsic section). Only identity fields are populated.
func identityFieldsFromBlockColsTyped(block *modules_reader.Block, n int) []intrinsicRowFields {
	traceCol := block.GetColumn("trace:id")
	spanCol := block.GetColumn("span:id")
	parentCol := block.GetColumn("span:parent_id")
	result := make([]intrinsicRowFields, n)
	for rowIdx := range n {
		row := &result[rowIdx]
		if traceCol != nil {
			if v, ok := traceCol.BytesValue(rowIdx); ok && len(v) == traceIDByteLen {
				copy(row.traceID[:], v)
				row.present |= intrinsicPresentTraceID
			}
		}
		if spanCol != nil {
			// NOTE-093: [8]byte eliminates clone; copy8 enforces OTel spec spanIDByteLen requirement.
			if v, ok := spanCol.BytesValue(rowIdx); ok && copy8(&row.spanID, v) {
				row.present |= intrinsicPresentSpanID
			}
		}
		if parentCol != nil {
			// NOTE-093: [8]byte eliminates clone; copy8 enforces OTel spec spanIDByteLen requirement.
			if v, ok := parentCol.BytesValue(rowIdx); ok && copy8(&row.parentID, v) {
				row.present |= intrinsicPresentParentID
			}
		}
	}
	return result
}

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
//
// NOTE-427: write via a direct slice→array conversion (*dst = [spanIDByteLen]byte(b))
// rather than copy(dst[:], b). The copy builtin computes min(len(dst), len(b)) and lowers
// to a runtime memmove-style sequence even though the len(b) != spanIDByteLen guard above
// fixes the length; the array conversion is a single 8-byte load+store with one bounds
// check (its own len(b) >= spanIDByteLen, already proven by the guard). copy8 was still
// ~0.6% of querier self-time on the structural/legacy identity path (profile 2026-06-16,
// after NOTE-426 fixed only the dense scatter twins). Semantics are byte-identical: the
// len(b) != spanIDByteLen guard rejects non-spec widths exactly as before.
func copy8(dst *[8]byte, b []byte) bool {
	if len(b) != spanIDByteLen {
		return false
	}
	*dst = [spanIDByteLen]byte(b)
	return true
}

// intrinsicRowFields holds per-row data for all 11 trace intrinsic columns.
// Used by the structural hot path to eliminate per-row map allocations.
// A single []intrinsicRowFields slice replaces []map[string]any (one alloc for N rows).
// Fields are ordered per betteralign output: strings first (16-byte headers), then 8-byte scalars
// (uint64, int64), then [8]byte fixed arrays, then smaller scalars (uint16), then [16]byte arrays.
// NOTE-093: [8]byte eliminates clone; [8]byte{} is the zero value; the present bitmask (field: present) is the authoritative absent indicator.

// span:name (dict string)
// resource.service.name (dict string)
// span:status_message (dict string)
// span:start (flat uint64 nanoseconds)
// span:end (synthesized flat uint64 nanoseconds)
// span:duration (flat uint64 nanoseconds)
// span:kind (dict int64)
// span:status (dict int64)
// span:id ([8]byte value type; [8]byte{} if absent — see present bitmask)
// span:parent_id ([8]byte value type; [8]byte{} if absent — see present bitmask)
// bitmask: which fields were populated
// trace:id ([16]byte value type, always 16 bytes)

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
	// NOTE-349: pooled, zeroed scratch — the caller (filterRowSetByIntrinsicNodes) consumes
	// the returned slice in a single filter loop and releases it via putIntrinsicRowFields.
	result := getIntrinsicRowFields(len(selected))

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
			putIntrinsicRowFields(result)
			return nil, fmt.Errorf("lookupIntrinsicFieldsTyped: GetIntrinsicColumn %q: %w", colNameSpanEnd, err)
		}
		if col != nil {
			populateTypedColumn(colNameSpanEnd, col, selected, result)
		}
	}

	return result, nil
}

// populateTypedColumn fills one column's values into the result slice via typed ref lookups.
// NOTE-429: hoist the colName type switch OUT of the per-ref loop and call the typed
// LookupRefFast{Uint64,Int64,String,Bytes} accessors directly, so each ref does one typed
// binary-search lookup with NO any boxing and NO per-row storeTypedField switch. The previous
// body called LookupRefFast (returns any) per ref, which boxed every uint64/int64 value onto the
// heap (the dominant alloc on the predicate-filtered search post-filter path,
// filterRowSetByIntrinsicNodes → lookupIntrinsicFieldsTyped → populateTypedColumn for Q6/Q7),
// then re-dispatched the same colName switch per row in storeTypedField (which re-asserts the
// boxed type). The typed accessors return concrete values (zero-alloc, NOTE-015), and dispatching
// the column type once per column instead of once per ref removes N interface-assertion branches
// and N boxing allocations (N = selected refs). This is the per-ref twin of the structural
// full-block scatter's hoisted switch (populateTypedColumnForBlock, NOTE-100/423).
func populateTypedColumn(
	colName string,
	col *modules_shared.IntrinsicColumn,
	selected []modules_shared.BlockRef,
	result []intrinsicRowFields,
) {
	// The column type is dispatched ONCE here and each group runs a tight typed per-ref loop.
	// Split into per-kind helpers (bytes-identity / string / uint64 / int64) to keep each
	// function's cyclomatic complexity low while preserving the zero-boxing hot loop.
	switch colName {
	case colNameTraceID, colNameSpanID, colNameParentID:
		populateBytesIdentityColumn(colName, col, selected, result)
	case colNameSpanName, colNameServiceName, colNameStatusMessage:
		populateStringColumn(colName, col, selected, result)
	case colNameSpanStart, colNameSpanEnd, colNameSpanDuration:
		populateUint64Column(colName, col, selected, result)
	case colNameSpanKind, colNameSpanStatus:
		populateInt64Column(colName, col, selected, result)
	}
}

// populateBytesIdentityColumn fills trace:id/span:id/span:parent_id from the column's
// per-ref []byte values without any boxing (NOTE-429). The colName is dispatched once by the
// caller; this helper still branches per identity field because the destination field and
// present bit differ, but the typed accessor is the same.
func populateBytesIdentityColumn(
	colName string,
	col *modules_shared.IntrinsicColumn,
	selected []modules_shared.BlockRef,
	result []intrinsicRowFields,
) {
	switch colName {
	case colNameTraceID:
		for i, ref := range selected {
			if b, ok := col.LookupRefFastBytes(packRef(ref)); ok && len(b) == traceIDByteLen {
				// NOTE-427: array conversion, not copy() — single 16-byte load+store.
				result[i].traceID = [traceIDByteLen]byte(b)
				result[i].present |= intrinsicPresentTraceID
			}
		}
	case colNameSpanID:
		for i, ref := range selected {
			if b, ok := col.LookupRefFastBytes(packRef(ref)); ok && copy8(&result[i].spanID, b) {
				result[i].present |= intrinsicPresentSpanID
			}
		}
	case colNameParentID:
		for i, ref := range selected {
			if b, ok := col.LookupRefFastBytes(packRef(ref)); ok && copy8(&result[i].parentID, b) {
				result[i].present |= intrinsicPresentParentID
			}
		}
	}
}

// populateStringColumn fills span:name/resource.service.name/span:status_message from the
// column's per-ref string values without any boxing (NOTE-429).
func populateStringColumn(
	colName string,
	col *modules_shared.IntrinsicColumn,
	selected []modules_shared.BlockRef,
	result []intrinsicRowFields,
) {
	switch colName {
	case colNameSpanName:
		for i, ref := range selected {
			if s, ok := col.LookupRefFastString(packRef(ref)); ok {
				result[i].spanName = s
				result[i].present |= intrinsicPresentSpanName
			}
		}
	case colNameServiceName:
		for i, ref := range selected {
			if s, ok := col.LookupRefFastString(packRef(ref)); ok {
				result[i].serviceName = s
				result[i].present |= intrinsicPresentServiceName
			}
		}
	case colNameStatusMessage:
		for i, ref := range selected {
			if s, ok := col.LookupRefFastString(packRef(ref)); ok {
				result[i].statusMessage = s
				result[i].present |= intrinsicPresentStatusMessage
			}
		}
	}
}

// populateUint64Column fills span:start/span:end/span:duration from the column's per-ref
// uint64 values without any boxing (NOTE-429).
func populateUint64Column(
	colName string,
	col *modules_shared.IntrinsicColumn,
	selected []modules_shared.BlockRef,
	result []intrinsicRowFields,
) {
	switch colName {
	case colNameSpanStart:
		for i, ref := range selected {
			if u, ok := col.LookupRefFastUint64(packRef(ref)); ok {
				result[i].spanStart = u
				result[i].present |= intrinsicPresentSpanStart
			}
		}
	case colNameSpanEnd:
		for i, ref := range selected {
			if u, ok := col.LookupRefFastUint64(packRef(ref)); ok {
				result[i].spanEnd = u
				result[i].present |= intrinsicPresentSpanEnd
			}
		}
	case colNameSpanDuration:
		for i, ref := range selected {
			if u, ok := col.LookupRefFastUint64(packRef(ref)); ok {
				result[i].spanDuration = u
				result[i].present |= intrinsicPresentSpanDuration
			}
		}
	}
}

// populateInt64Column fills span:kind/span:status from the column's per-ref int64 values
// without any boxing (NOTE-429).
func populateInt64Column(
	colName string,
	col *modules_shared.IntrinsicColumn,
	selected []modules_shared.BlockRef,
	result []intrinsicRowFields,
) {
	switch colName {
	case colNameSpanKind:
		for i, ref := range selected {
			if iv, ok := col.LookupRefFastInt64(packRef(ref)); ok {
				result[i].spanKind = iv
				result[i].present |= intrinsicPresentSpanKind
			}
		}
	case colNameSpanStatus:
		for i, ref := range selected {
			if iv, ok := col.LookupRefFastInt64(packRef(ref)); ok {
				result[i].spanStatus = iv
				result[i].present |= intrinsicPresentSpanStatus
			}
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
	// NOTE-349: pooled, zeroed scratch — the caller releases via putIntrinsicRowFields
	// after its row loop, the same lifetime as the intrinsic-section branch.
	result := getIntrinsicRowFields(n)
	for rowIdx := range n {
		row := &result[rowIdx]
		if traceCol != nil {
			if v, ok := traceCol.BytesValue(rowIdx); ok && len(v) == traceIDByteLen {
				// NOTE-427: array conversion, not copy() — see copy8.
				row.traceID = [traceIDByteLen]byte(v)
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

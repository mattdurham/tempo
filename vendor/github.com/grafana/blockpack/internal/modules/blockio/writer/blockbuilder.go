package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type blockBuilder struct {
	builderCache     map[shared.ColumnKey]columnBuilder
	dedicatedCols    map[string]struct{}
	spanColNames     map[string]string
	resourceColNames map[string]string
	scopeColNames    map[string]string
	colMinMax        map[string]*blockColMinMax
	columns          map[shared.ColumnKey]columnBuilder
	// NOTE: colStats field removed (2026-06-29, in-file block pruning removal).
	spanCount        int
	spanHint         int
	minStart         uint64
	maxStart         uint64
	intrinsicBlockID uint16
	minTraceID       [16]byte
	maxTraceID       [16]byte
	// v2IdentityInBlock signals that identity columns (trace:id, span:id, span:parent_id)
	// must be written to the inner block payload via addPresent, because the file-level
	// IntrinsicTOC and SpanTree are not emitted in v2 format (NOTE-V2-003, issue #417).
	v2IdentityInBlock bool
}

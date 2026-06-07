package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type blockBuilder struct {
	columns          map[shared.ColumnKey]columnBuilder
	traceRows        map[[16]byte]struct{}
	intrinsicAccum   *intrinsicAccumulator
	dedicatedCols    map[string]struct{}
	spanColNames     map[string]string
	resourceColNames map[string]string
	scopeColNames    map[string]string
	colMinMax        map[string]*blockColMinMax
	colSketches      blockSketchSet
	builderCache     map[shared.ColumnKey]columnBuilder
	spanCount        int
	spanHint         int
	minStart         uint64
	maxStart         uint64
	intrinsicBlockID uint16
	minTraceID       [16]byte
	maxTraceID       [16]byte
}

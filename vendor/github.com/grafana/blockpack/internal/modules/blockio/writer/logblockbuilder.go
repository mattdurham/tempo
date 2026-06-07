package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type logBlockBuilder struct {
	colTimestamp         *uint64ColumnBuilder
	colObservedTimestamp *uint64ColumnBuilder
	colBody              *stringColumnBuilder
	colSeverityNumber    *int64ColumnBuilder
	colSeverityText      *stringColumnBuilder
	colTraceID           *bytesColumnBuilder
	colSpanID            *bytesColumnBuilder
	colFlags             *uint64ColumnBuilder
	columns              map[shared.ColumnKey]columnBuilder
	logColNames          map[string]string
	resourceColNames     map[string]string
	scopeColNames        map[string]string
	colMinMax            map[string]*blockColMinMax
	colNumericMinMax     map[string]*blockColMinMax
	colFloatMinMax       map[string]*blockColMinMax
	colNonNumeric        map[string]bool
	colNonFloat          map[string]bool
	colSketches          blockSketchSet
	sparseColumns        []columnBuilder
	recordCount          int
	recordHint           int
	minStart             uint64
	maxStart             uint64
}

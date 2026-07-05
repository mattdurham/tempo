package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type columnBuilder interface {
	addString(val string, present bool)
	addInt64(val int64, present bool)
	addUint64(val uint64, present bool)
	addFloat64(val float64, present bool)
	addBool(val, present bool)
	addBytes(val []byte, present bool)
	rowCount() int
	nullCount() int
	colType() shared.ColumnType
	buildData() ([]byte, error)
	resetForReuse(colName string)
	prepare(nRows int)
}

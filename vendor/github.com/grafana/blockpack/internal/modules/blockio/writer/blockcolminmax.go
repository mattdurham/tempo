package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type blockColMinMax struct {
	colName   string
	minKey    string
	maxKey    string
	numMinKey [8]byte
	numMaxKey [8]byte
	isNum     bool
	colType   shared.ColumnType
}

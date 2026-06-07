package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type rangeIndexMeta struct {
	typ    shared.ColumnType
	offset int
	length int
}

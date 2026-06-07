package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type flatAccum struct {
	uint64Values []uint64
	bytesValues  [][]byte
	refs         []shared.BlockRef
	colType      shared.ColumnType
}

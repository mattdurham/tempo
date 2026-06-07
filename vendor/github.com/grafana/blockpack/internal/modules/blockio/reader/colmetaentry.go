package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type colMetaEntry struct {
	name            string
	colType         shared.ColumnType
	dataOffset      uint64
	compressedLen   uint32
	uncompressedLen uint32
}

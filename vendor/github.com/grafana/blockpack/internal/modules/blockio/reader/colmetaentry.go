package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type colMetaEntry struct {
	name string
	// inlineData holds the column's raw (un-snappy) blob when the V15 ColFlagInline bit
	// is set (NOTE-220). It sub-slices the block bytes directly. When nil the column uses
	// the offset-addressed, snappy-compressed form described by the fields below.
	inlineData      []byte
	dataOffset      uint64
	compressedLen   uint32
	uncompressedLen uint32
	colType         shared.ColumnType
}

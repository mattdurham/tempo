package reader

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type colMetaEntry struct {
	name string
	// inlineData holds the column's raw (un-snappy) blob when the V15 ColFlagInline bit
	// is set (NOTE-220). It sub-slices the block bytes directly. When nil the column uses
	// the offset-addressed, snappy-compressed form described by the fields below.
	inlineData []byte
	// NOTE-362: dataOffset is the column's byte offset WITHIN the block, bounded by
	// MaxBlockSize (1 GiB) — it always fits in uint32. Narrowing it from uint64 shrinks
	// colMetaEntry from 64 -> 56 bytes (the trailing uint8 colType field still pads to an
	// 8-byte boundary). This entry is retained one-per-column-per-block in
	// blockColTypesCache, so the 8-byte saving is multiplied across every cached block's
	// full column set. The wire format still stores the offset as 8 LE bytes; the parser
	// bounds-checks it before the narrowing conversion.
	dataOffset      uint32
	compressedLen   uint32
	uncompressedLen uint32
	colType         shared.ColumnType
	// zstd is set when the V15 ColFlagZstd bit is present on this column's flags byte
	// (NOTE-405, issue #355): the offset-addressed blob is zstd-compressed rather than
	// snappy. Always false for V14 and for inline columns. The decompress path selects the
	// codec from this flag; a false value keeps the historical snappy path byte-for-byte.
	zstd bool
}

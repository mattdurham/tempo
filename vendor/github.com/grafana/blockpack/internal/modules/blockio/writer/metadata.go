package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// writeBlockIndexSection serializes the block index.
// Returns the serialized bytes (without the length prefix — caller adds it).
func writeBlockIndexSection(_ io.Writer, metas []shared.BlockMeta) ([]byte, error) {
	var buf bytes.Buffer

	// block_count[4 LE]
	var tmp [4]byte
	binary.LittleEndian.PutUint32(
		tmp[:],
		uint32(len(metas)), //nolint:gosec // safe: block count bounded by MaxBlocks (100_000)
	)
	buf.Write(tmp[:])

	for _, m := range metas {
		// offset[8 LE]
		var off [8]byte
		binary.LittleEndian.PutUint64(off[:], m.Offset)
		buf.Write(off[:])

		// length[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.Length)
		buf.Write(off[:])

		// kind[1]
		buf.WriteByte(byte(m.Kind))

		// span_count[4 LE]
		binary.LittleEndian.PutUint32(tmp[:], m.SpanCount)
		buf.Write(tmp[:])

		// min_start[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.MinStart)
		buf.Write(off[:])

		// max_start[8 LE]
		binary.LittleEndian.PutUint64(off[:], m.MaxStart)
		buf.Write(off[:])

		// V13: MinTraceID/MaxTraceID omitted (never used for pruning).
	}

	return buf.Bytes(), nil
}

// V8/V9 footer field offsets (18-byte footer: magic[4]+version[2]+toc_offset[8]+toc_len[4]).
// Matches the reader constants footerV7OffVersion/DirOff/DirLen in parser.go.
const (
	footerV8OffVersion = 4  // uint16 version field
	footerV8OffDirOff  = 6  // uint64 toc_offset field
	footerV8OffDirLen  = 14 // uint32 toc_len field
)

// writeFooterVersion writes an 18-byte V8/V9 footer with the given version.
func writeFooterVersion(w io.Writer, version uint16, tocOffset uint64, tocLen uint32) error {
	var buf [18]byte
	binary.LittleEndian.PutUint32(buf[0:], shared.MagicNumber)
	binary.LittleEndian.PutUint16(buf[footerV8OffVersion:], version)
	binary.LittleEndian.PutUint64(buf[footerV8OffDirOff:], tocOffset)
	binary.LittleEndian.PutUint32(buf[footerV8OffDirLen:], tocLen)
	_, err := w.Write(buf[:])
	return err
}

package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.
// NOTE-45: file-level bloom filter section — written at Flush time for resource.service.name.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

// writeFileBloomSection builds a BinaryFuse8 for resource.service.name and serializes
// it as a FileBloom metadata section.
// Returns nil, nil when svcNames is empty (section is omitted entirely).
// Wire format: magic[4 LE] + version[1] + col_count[4 LE] + per-column entries.
// Per column: name_len[2 LE] + name[N] + fuse_len[4 LE] + fuse_data[fuse_len].
func writeFileBloomSection(svcNames map[string]struct{}) ([]byte, error) {
	if len(svcNames) == 0 {
		return nil, nil
	}

	const colName = "resource.service.name"
	keys := make([]uint64, 0, len(svcNames))
	for name := range svcNames {
		keys = append(keys, sketch.HashForFuse(name))
	}
	f, err := sketch.NewBinaryFuse8(keys)
	if err != nil {
		return nil, fmt.Errorf("fileBloom: %q: %w", colName, err)
	}
	fuseBytes, err := f.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("fileBloom: %q: marshal: %w", colName, err)
	}

	// magic[4] + version[1] + col_count[4] + name_len[2] + name + fuse_len[4] + fuse_data
	buf := make([]byte, 0, 9+2+len(colName)+4+len(fuseBytes))
	var tmp4 [4]byte
	var tmp2 [2]byte

	binary.LittleEndian.PutUint32(tmp4[:], shared.FileBloomMagic)
	buf = append(buf, tmp4[:]...)
	buf = append(buf, shared.FileBloomVersion)
	binary.LittleEndian.PutUint32(tmp4[:], 1) // col_count = 1
	buf = append(buf, tmp4[:]...)

	binary.LittleEndian.PutUint16(tmp2[:], uint16(len(colName))) //nolint:gosec
	buf = append(buf, tmp2[:]...)
	buf = append(buf, colName...)
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(fuseBytes))) //nolint:gosec
	buf = append(buf, tmp4[:]...)
	buf = append(buf, fuseBytes...)

	return buf, nil
}

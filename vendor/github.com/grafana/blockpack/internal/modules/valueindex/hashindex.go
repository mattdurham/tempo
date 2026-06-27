package valueindex

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// HashEntry is one entry in the ToCSubTypeValueIndexHashIndex (VHIX) section.
// The section is a sorted array of entries keyed by ValueHash ascending.
// One entry per distinct value in the file; ChunkIdx points to the first chunk
// in the VINX section that contains rows for this value.
//
// Wire: value_hash[16] + chunk_idx[4 LE] = 20 bytes per entry.
type HashEntry struct {
	ValueHash [16]byte
	ChunkIdx  uint32
}

// hashIndexHeaderSize is the fixed header: magic[4]+version[1]+reserved[3]+entry_count[4] = 12 bytes.
const hashIndexHeaderSize = 12

// EncodeHashIndex serializes entries into the VHIX wire format.
// entries must be sorted by ValueHash ascending (the caller is responsible for ordering).
func EncodeHashIndex(entries []HashEntry) []byte {
	size := hashIndexHeaderSize + len(entries)*20
	b := make([]byte, size)
	binary.LittleEndian.PutUint32(b[0:], shared.ValueIndexHashIndexMagic)
	b[4] = shared.ValueIndexHashIndexVersion
	// b[5:8] reserved — zero from make
	binary.LittleEndian.PutUint32(b[8:], uint32(len(entries))) //nolint:gosec // entry count bounded
	pos := hashIndexHeaderSize
	for _, e := range entries {
		copy(b[pos:], e.ValueHash[:])
		binary.LittleEndian.PutUint32(b[pos+16:], e.ChunkIdx)
		pos += 20
	}
	return b
}

// DecodeHashIndex parses a VHIX blob and returns the sorted hash entries.
func DecodeHashIndex(b []byte) ([]HashEntry, error) {
	if len(b) < hashIndexHeaderSize {
		return nil, fmt.Errorf("valueindex: VHIX blob too short (%d bytes)", len(b))
	}
	if magic := binary.LittleEndian.Uint32(b[:4]); magic != shared.ValueIndexHashIndexMagic {
		return nil, fmt.Errorf("valueindex: VHIX wrong magic 0x%08X", magic)
	}
	if ver := b[4]; ver != shared.ValueIndexHashIndexVersion {
		return nil, fmt.Errorf("valueindex: VHIX unsupported version %d", ver)
	}
	count := int(binary.LittleEndian.Uint32(b[8:12]))
	if len(b) < hashIndexHeaderSize+count*20 {
		return nil, fmt.Errorf(
			"valueindex: VHIX blob truncated: need %d bytes for %d entries, have %d",
			hashIndexHeaderSize+count*20, count, len(b),
		)
	}
	if count == 0 {
		return nil, nil
	}
	entries := make([]HashEntry, count)
	pos := hashIndexHeaderSize
	for i := range entries {
		copy(entries[i].ValueHash[:], b[pos:pos+16])
		entries[i].ChunkIdx = binary.LittleEndian.Uint32(b[pos+16:])
		pos += 20
	}
	return entries, nil
}

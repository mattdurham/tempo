package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"encoding/binary"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// tsIndexEntry holds a single entry in the per-file timestamp index.
// One entry per block, sorted by minTS ascending.
type tsIndexEntry struct {
	minTS uint64
	// maxTS is the maximum span *start* time for this block (BlockMeta.MaxStart).
	// This is the latest start timestamp in the block, NOT the latest end timestamp.
	// Used for overlap: a block overlaps [queryMin, queryMax] when
	//   maxTS >= queryMin && minTS <= queryMax.
	maxTS   uint64
	blockID uint32
}

// writeTSIndexSection serializes the per-file TS index for the given block metas.
//
// Wire format (little-endian):
//
//	magic[4]    = shared.TSIndexMagic
//	version[1]  = shared.TSIndexVersion
//	count[4]    = number of entries
//	per entry (20 bytes):
//	  min_ts[8] + max_ts[8] + block_id[4]
//
// Entries are sorted by min_ts ascending. Blocks with MinStart==MaxStart==0 are
// included as-is (unknown time, treated as matching all ranges by the reader).
func writeTSIndexSection(metas []shared.BlockMeta) []byte {
	entries := make([]tsIndexEntry, len(metas))
	for i, m := range metas {
		entries[i] = tsIndexEntry{
			minTS:   m.MinStart,
			maxTS:   m.MaxStart,
			blockID: uint32(i), //nolint:gosec // safe: i bounded by MaxBlocks (100_000) fits uint32
		}
	}
	slices.SortFunc(entries, func(a, b tsIndexEntry) int {
		return cmp.Compare(a.minTS, b.minTS)
	})

	// Header: magic[4] + version[1] + count[4] = 9 bytes.
	// Each entry: min_ts[8] + max_ts[8] + block_id[4] = 20 bytes.
	size := 9 + len(entries)*20
	buf := make([]byte, size)

	binary.LittleEndian.PutUint32(buf[0:], shared.TSIndexMagic)
	buf[4] = shared.TSIndexVersion
	binary.LittleEndian.PutUint32(buf[5:], uint32(len(entries))) //nolint:gosec // safe: bounded by MaxBlocks

	pos := 9
	for _, e := range entries {
		binary.LittleEndian.PutUint64(buf[pos:], e.minTS)
		binary.LittleEndian.PutUint64(buf[pos+8:], e.maxTS)
		binary.LittleEndian.PutUint32(buf[pos+16:], e.blockID)
		pos += 20
	}
	return buf
}

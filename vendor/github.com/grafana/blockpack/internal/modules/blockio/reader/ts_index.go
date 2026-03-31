package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// tsIndexEntry is one parsed entry from the per-file timestamp index.
// Sorted by minTS ascending (as written by the writer).
type tsIndexEntry struct {
	minTS   uint64
	maxTS   uint64
	blockID uint32
}

// parseTSIndex parses the TS index section from data.
// Returns the parsed entries (sorted by minTS) and bytes consumed.
//
// Returns (nil, 0, nil) when:
//   - data is shorter than 9 bytes (too short for a header), or
//   - the magic number does not match (not a TS index section, e.g. old file format).
//
// Returns a non-nil error only when the magic matches but the data is malformed.
func parseTSIndex(data []byte) ([]tsIndexEntry, int, error) {
	if len(data) < 9 {
		return nil, 0, nil // graceful: old file, no TS index
	}

	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.TSIndexMagic {
		return nil, 0, nil // graceful: not a TS index section
	}

	version := data[4]
	if version != shared.TSIndexVersion {
		return nil, 0, fmt.Errorf("ts_index: unsupported version %d", version)
	}

	count := int(binary.LittleEndian.Uint32(data[5:]))
	if count > shared.MaxBlocks {
		return nil, 0, fmt.Errorf("ts_index: count %d exceeds MaxBlocks %d", count, shared.MaxBlocks)
	}
	need := 9 + count*20
	if len(data) < need {
		return nil, 0, fmt.Errorf(
			"ts_index: short data for %d entries (need %d, have %d)",
			count, need, len(data),
		)
	}

	entries := make([]tsIndexEntry, count)
	pos := 9
	for i := range count {
		entries[i] = tsIndexEntry{
			minTS:   binary.LittleEndian.Uint64(data[pos:]),
			maxTS:   binary.LittleEndian.Uint64(data[pos+8:]),
			blockID: binary.LittleEndian.Uint32(data[pos+16:]),
		}
		pos += 20
	}
	return entries, need, nil
}

// TimeIndexLen returns the number of entries in the per-file timestamp index.
// Returns 0 for files written before the TS index was introduced.
func (r *Reader) TimeIndexLen() int {
	return len(r.tsEntries)
}

// BlocksInTimeRange returns block indices whose time window overlaps [minNano, maxNano].
//
// A block overlaps the query when both conditions hold:
//
//	block.maxTS >= minNano  (block ends at or after query start)
//	block.minTS <= maxNano  (block starts at or before query end)
//
// Special case: blocks with minTS == 0 && maxTS == 0 are always included
// (unknown time; occurs in trace files where MinStart/MaxStart are zero).
//
// Returns nil when the TS index is absent (old files); callers must fall back
// to a full BlockMeta scan in that case.
//
// The returned slice is sorted in ascending blockID order.
func (r *Reader) BlocksInTimeRange(minNano, maxNano uint64) []int {
	if len(r.tsEntries) == 0 {
		return nil
	}

	// Binary search: entries are sorted by minTS. Find the first entry where
	// minTS > maxNano — entries at this index and beyond start after the query
	// window and cannot overlap. We only need to check entries[0:firstAfter].
	firstAfter := sort.Search(len(r.tsEntries), func(i int) bool {
		return r.tsEntries[i].minTS > maxNano
	})

	var result []int
	for i := range firstAfter {
		e := r.tsEntries[i]
		// Include zero-time blocks always; include others only when maxTS >= minNano.
		if (e.minTS == 0 && e.maxTS == 0) || e.maxTS >= minNano {
			result = append(result, int(e.blockID)) //nolint:gosec // safe: blockID bounded by MaxBlocks fits int
		}
	}
	slices.Sort(result)
	return result
}

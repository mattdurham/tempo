package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// parseTSIndex validates the TS index header and returns the raw 20-byte-per-entry body
// as a zero-copy sub-slice of data, plus the entry count and total bytes consumed.
//
// NOTE-PERF-TS: stores raw bytes rather than materializing a parsed []tsIndexEntry,
// eliminating O(count) parse work and the parsed-entry memory footprint (including
// the one large slice allocation). BlocksInTimeRange scans the 20-byte-stride
// entries in-place (minTS[8] + maxTS[8] + blockID[4] per entry).
//
// Returns (nil, 0, 0, nil) when:
//   - data is shorter than 9 bytes (too short for a header), or
//   - the magic number does not match (not a TS index section, e.g. old file format).
//
// Returns a non-nil error only when the magic matches but the data is malformed.
func parseTSIndex(data []byte) (rawEntries []byte, entryCount int, consumed int, err error) {
	if len(data) < 9 {
		return nil, 0, 0, nil // graceful: old file, no TS index
	}

	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != shared.TSIndexMagic {
		return nil, 0, 0, nil // graceful: not a TS index section
	}

	version := data[4]
	if version != shared.TSIndexVersion {
		return nil, 0, 0, fmt.Errorf("ts_index: unsupported version %d", version)
	}

	n := int(binary.LittleEndian.Uint32(data[5:]))
	if n > shared.MaxBlocks {
		return nil, 0, 0, fmt.Errorf("ts_index: count %d exceeds MaxBlocks %d", n, shared.MaxBlocks)
	}
	need := 9 + n*20
	if len(data) < need {
		return nil, 0, 0, fmt.Errorf(
			"ts_index: short data for %d entries (need %d, have %d)",
			n, need, len(data),
		)
	}

	return data[9:need], n, need, nil
}

// TimeIndexLen returns the number of entries in the per-file timestamp index.
// Returns 0 for files written before the TS index was introduced.
func (r *Reader) TimeIndexLen() int {
	_ = r.ensureV14TSSection()
	return r.tsCount
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
//
// NOTE-PERF-TS: scans the raw 20-byte-per-entry buffer (minTS[8]+maxTS[8]+blockID[4])
// in-place instead of iterating a pre-parsed []tsIndexEntry. Zero allocation on
// every call; the caller's result append is the only allocation.
func (r *Reader) BlocksInTimeRange(minNano, maxNano uint64) []int {
	_ = r.ensureV14TSSection()
	if r.tsCount == 0 {
		return nil
	}

	// Binary search: entries are sorted by minTS. Find the first entry where
	// minTS > maxNano — entries at this index and beyond start after the query
	// window and cannot overlap. We only need to check entries[0:firstAfter].
	const stride = 20
	firstAfter := sort.Search(r.tsCount, func(i int) bool {
		return binary.LittleEndian.Uint64(r.tsRaw[i*stride:]) > maxNano
	})

	var result []int
	for i := range firstAfter {
		base := i * stride
		minTS := binary.LittleEndian.Uint64(r.tsRaw[base:])
		maxTS := binary.LittleEndian.Uint64(r.tsRaw[base+8:])
		blockID := binary.LittleEndian.Uint32(r.tsRaw[base+16:])
		// Include zero-time blocks always; include others only when maxTS >= minNano.
		if (minTS == 0 && maxTS == 0) || maxTS >= minNano {
			result = append(result, int(blockID)) //nolint:gosec // safe: blockID bounded by MaxBlocks fits int
		}
	}
	slices.Sort(result)
	return result
}

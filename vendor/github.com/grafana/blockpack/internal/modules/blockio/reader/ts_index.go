package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TS index wire-format constants: magic[4]+version[1]+count[4] = 9-byte header, 20-byte entries.
const (
	tsIndexHeaderSize = 9  // magic[4] + version[1] + count[4]
	tsIndexEntrySize  = 20 // minTS[8] + maxTS[8] + blockID[4]
	tsCountOffset     = 5  // byte offset of the count field within the header
)

// TS entry field offsets within each 20-byte TS index entry (M-30).
// Entry layout: minTS[8]+maxTS[8]+blockID[4].
const (
	tsEntryMaxTSOff   = 8  // byte offset of maxTS uint64 field within a 20-byte TS entry
	tsEntryBlockIDOff = 16 // byte offset of blockID uint32 field within a 20-byte TS entry
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
//   - data is shorter than tsIndexHeaderSize bytes (too short for a header), or
//   - the magic number does not match (not a TS index section, e.g. old file format).
//
// Returns a non-nil error only when the magic matches but the data is malformed.
func parseTSIndex(data []byte) (rawEntries []byte, entryCount, consumed int, err error) {
	if len(data) < tsIndexHeaderSize {
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

	n := int(binary.LittleEndian.Uint32(data[tsCountOffset:]))
	if n > shared.MaxBlocks {
		return nil, 0, 0, fmt.Errorf("ts_index: count %d exceeds MaxBlocks %d", n, shared.MaxBlocks)
	}
	need := tsIndexHeaderSize + n*tsIndexEntrySize
	if len(data) < need {
		return nil, 0, 0, fmt.Errorf(
			"ts_index: short data for %d entries (need %d, have %d)",
			n, need, len(data),
		)
	}

	return data[tsIndexHeaderSize:need], n, need, nil
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
		maxTS := binary.LittleEndian.Uint64(r.tsRaw[base+tsEntryMaxTSOff:])
		blockID := binary.LittleEndian.Uint32(r.tsRaw[base+tsEntryBlockIDOff:])
		// Include zero-time blocks always; include others only when maxTS >= minNano.
		if (minTS == 0 && maxTS == 0) || maxTS >= minNano {
			result = append(result, int(blockID)) //nolint:gosec // safe: blockID bounded by MaxBlocks fits int
		}
	}
	slices.Sort(result)
	return result
}

// tsEntryMinTSDesc pairs one TS-index entry's minTS with its blockID, for
// BlockIndicesNewestFirst's own sort (by minTS descending) -- distinct from
// BlocksInTimeRange's ascending blockID output.
type tsEntryMinTSDesc struct {
	minTS   uint64
	blockID int
}

// BlockIndicesNewestFirst mirrors BlocksInTimeRange but returns overlapping block
// indices in DESCENDING minTS order (newest first) instead of ascending blockID order
// -- the ordering plan-scan-fallback.md's Phase 6b match-all materializer needs to walk
// blocks newest-first without ever decoding block bodies to determine order. Falls back
// to sorting BlockMeta by MaxStart DESCENDING (zero body I/O -- BlockMeta is already
// resident in memory) when no TS index section exists (legacy files predating the
// TS-index format), mirroring BlocksInTimeRange's own documented "callers fall back to a
// full BlockMeta scan" contract, except the fallback lives HERE so every caller of this
// specific method gets a consistent, complete answer without re-deriving the fallback
// itself.
func (r *Reader) BlockIndicesNewestFirst(minNano, maxNano uint64) []int {
	_ = r.ensureV14TSSection()
	if r.tsCount == 0 {
		return r.blockIndicesNewestFirstFromBlockMeta(minNano, maxNano)
	}

	const stride = 20
	entries := make([]tsEntryMinTSDesc, 0, r.tsCount)
	for i := 0; i < r.tsCount; i++ {
		base := i * stride
		minTS := binary.LittleEndian.Uint64(r.tsRaw[base:])
		maxTS := binary.LittleEndian.Uint64(r.tsRaw[base+tsEntryMaxTSOff:])
		blockID := binary.LittleEndian.Uint32(r.tsRaw[base+tsEntryBlockIDOff:])
		// Same overlap rule as BlocksInTimeRange (zero-time blocks always included;
		// others only when both bounds overlap) -- this function cannot reuse
		// BlocksInTimeRange's own binary-search-prefix shortcut since it needs every
		// overlapping entry re-sorted by minTS descending, not just an ascending-blockID
		// prefix.
		if (minTS == 0 && maxTS == 0) || (maxTS >= minNano && minTS <= maxNano) {
			entries = append(
				entries,
				tsEntryMinTSDesc{minTS: minTS, blockID: int(blockID)},
			) //nolint:gosec // safe: blockID bounded by MaxBlocks fits int
		}
	}
	slices.SortFunc(entries, func(a, b tsEntryMinTSDesc) int {
		switch {
		case a.minTS > b.minTS:
			return -1
		case a.minTS < b.minTS:
			return 1
		default:
			return 0
		}
	})
	result := make([]int, len(entries))
	for i, e := range entries {
		result[i] = e.blockID
	}
	return result
}

// blockIndicesNewestFirstFromBlockMeta is BlockIndicesNewestFirst's fallback for files
// with no TS index section: sorts the already-resident BlockMeta list by MaxStart
// descending, at zero body-I/O cost. Mirrors BlockIndicesNewestFirst's own overlap rule.
func (r *Reader) blockIndicesNewestFirstFromBlockMeta(minNano, maxNano uint64) []int {
	type metaIdx struct {
		maxStart uint64
		idx      int
	}
	candidates := make([]metaIdx, 0, len(r.blockMetas))
	for i, m := range r.blockMetas {
		if (m.MinStart == 0 && m.MaxStart == 0) || (m.MaxStart >= minNano && m.MinStart <= maxNano) {
			candidates = append(candidates, metaIdx{maxStart: m.MaxStart, idx: i})
		}
	}
	slices.SortFunc(candidates, func(a, b metaIdx) int {
		switch {
		case a.maxStart > b.maxStart:
			return -1
		case a.maxStart < b.maxStart:
			return 1
		default:
			return 0
		}
	})
	result := make([]int, len(candidates))
	for i, c := range candidates {
		result[i] = c.idx
	}
	return result
}

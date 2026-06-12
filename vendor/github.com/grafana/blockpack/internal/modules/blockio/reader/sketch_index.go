// Package reader provides block reading and index parsing for blockpack files.
// NOTE: Sketch index parsing for column-major HLL + SketchBloom + TopK data.
// Three magic values are recognized:
//   - 0x534B5445 ("SKTE") — bloom only, no CMS (current writer)
//   - 0x534B5444 ("SKTD") — bloom + CMS — skip CMS bytes, read bloom
//   - 0x534B5443 ("SKTC") — legacy fuse-based format; degrades gracefully (no pruning)
//
// Files without any recognized magic degrade gracefully:
// parseSketchIndexSection returns (nil, 0, nil) and ColumnSketch returns nil.
//
// Bloom data is stored as zero-copy slices into the retained metadata buffer (no allocation).
package reader

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

// Ensure columnSketchData satisfies queryplanner.ColumnSketch at compile time.
var _ queryplanner.ColumnSketch = (*columnSketchData)(nil)

// columnSketchData holds parsed column-major sketch data for one column across all blocks.
// Bloom data is stored as a zero-copy sub-slice of the metadata buffer; no copy is made at parse time.

// bitset: 1 bit per block
// [presentIdx][entries] fingerprints
// [presentIdx][entries] counts
// presentMap[i] = blockIdx of the i-th present block

// NOTE-PERF-SKETCH: distinctRaw stores raw 4-byte-per-block distinct counts as a zero-copy
// sub-slice of the metadata buffer. Distinct() decodes on demand, eliminating make([]uint32,
// numBlocks) per column at parse time. distinctAt() provides single-block access used by
// layout and file_sketch_summary.
// numBlocks×4 LE uint32s; zero-copy sub-slice of metadataBytes

// Bloom filters: raw byte slices into the metadata buffer, one per present block.
// Zero-copy: slices reference the decompressed metadata buffer retained by the Reader.
// Nil/empty for blocks parsed from legacy fuse-format files (FuseContains returns true).
// [presentIdx] each slice is exactly sketch.SketchBloomBytes bytes

// sketchIndex holds all column sketch data for the file.

// SizeBytes returns the estimated in-memory size of this sketchIndex for LRU cache budgeting.
// Counts owned heap allocations (topkFP, topkCount, presentMap, presence) plus the bloom and
// distinctRaw bytes referenced into the metadata buffer, so the cache budget reflects the true
// cost of keeping this entry alive.
func (si *sketchIndex) SizeBytes() int64 {
	const overhead = 64 // map + struct header
	n := int64(overhead)
	for _, cd := range si.columns {
		// presence bitset
		n += int64(len(cd.presence)) * 8
		// topkFP + topkCount per present block
		for i := range cd.topkFP {
			n += int64(len(cd.topkFP[i])) * 8
			if i < len(cd.topkCount) {
				n += int64(len(cd.topkCount[i])) * 2
			}
		}
		// presentMap
		n += int64(len(cd.presentMap)) * 8
		// distinctRaw and bloom: zero-copy into metadataBytes, but charge their size
		// here so the cache budget accounts for the buffer they keep alive.
		n += int64(len(cd.distinctRaw))
		for _, b := range cd.bloom {
			n += int64(len(b))
		}
	}
	return n
}

// Presence returns a bitset with 1 bit per block (1 = column present in block).
func (cd *columnSketchData) Presence() []uint64 { return cd.presence }

// Distinct decodes and returns per-block HLL cardinality (0 for absent blocks).
// NOTE-PERF-SKETCH: decodes from distinctRaw on each call rather than returning a
// pre-built slice, eliminating make([]uint32, numBlocks) per column at parse time.
// Callers (scoring.go) capture the result in a local variable, so this runs once
// per column per scoring pass — not per span.
func (cd *columnSketchData) Distinct() []uint32 {
	if len(cd.distinctRaw) == 0 {
		return nil
	}
	count := len(cd.distinctRaw) / 4
	out := make([]uint32, count)
	for i := range count {
		out[i] = binary.LittleEndian.Uint32(cd.distinctRaw[i*4:])
	}
	return out
}

// distinctAt returns the distinct count for blockIdx, reading directly from raw bytes.
// Returns 0 for out-of-range indices or when no distinct data is present.
func (cd *columnSketchData) distinctAt(blockIdx int) uint32 {
	offset := blockIdx * 4
	if offset+4 > len(cd.distinctRaw) {
		return 0
	}
	return binary.LittleEndian.Uint32(cd.distinctRaw[offset:])
}

// presentIdxFor returns the present-index for blockIdx using binary search.
// presentMap is always sorted ascending by blockIdx (see parseColumnPresence).
// Returns (i, true) when blockIdx is found at presentMap[i]; (0, false) otherwise.
// O(log presentCount).
func (cd *columnSketchData) presentIdxFor(blockIdx int) (int, bool) {
	n := len(cd.presentMap)
	i := sort.Search(n, func(i int) bool {
		return cd.presentMap[i] >= blockIdx
	})
	if i < n && cd.presentMap[i] == blockIdx {
		return i, true
	}
	return 0, false
}

// DistinctAt returns the distinct count for blockIdx via the ColumnSketch interface.
// Delegates to the private distinctAt method for zero-allocation per-block scoring.
// Returns 0 when blockIdx < 0 or out of range.
// NOTE-022: scalar accessor — no slice allocation.
func (cd *columnSketchData) DistinctAt(blockIdx int) uint32 {
	if blockIdx < 0 {
		return 0
	}
	return cd.distinctAt(blockIdx)
}

// TopKMatchAt returns the TopK count for valFP at blockIdx.
// Uses binary search over presentMap — O(log presentCount + K).
// Returns 0 if blockIdx < 0, has no topk data, or valFP is not in top-K.
// presentMap is always sorted ascending (built in blockIdx order by parseColumnPresence).
// NOTE-022: scalar accessor — no slice allocation.
func (cd *columnSketchData) TopKMatchAt(valFP uint64, blockIdx int) uint16 {
	if blockIdx < 0 {
		return 0
	}
	pi, ok := cd.presentIdxFor(blockIdx)
	if !ok {
		return 0
	}
	for j, fp := range cd.topkFP[pi] {
		if fp == valFP {
			return cd.topkCount[pi][j]
		}
	}
	return 0
}

// FuseContainsAt returns whether the bloom filter for blockIdx indicates valHash may be present.
// Uses binary search over presentMap — O(log presentCount).
// Returns false when blockIdx < 0 or column is absent from that block.
// Returns true (conservative) for present blocks without bloom data.
// presentMap is always sorted ascending (built in blockIdx order by parseColumnPresence).
// NOTE-022: scalar accessor — no slice allocation.
func (cd *columnSketchData) FuseContainsAt(valHash uint64, blockIdx int) bool {
	if blockIdx < 0 {
		return false
	}
	pi, ok := cd.presentIdxFor(blockIdx)
	if !ok {
		// blockIdx not in presentMap — column absent from block; value cannot be present.
		return false
	}
	if pi >= len(cd.bloom) || cd.bloom[pi] == nil {
		return true // conservative: no bloom data for this block
	}
	return sketch.BloomContains(cd.bloom[pi], valHash)
}

// TopKMatch returns the TopK count for valFP per block (0 if not in top-K or absent).
func (cd *columnSketchData) TopKMatch(valFP uint64) []uint16 {
	out := make([]uint16, cd.numBlocks)
	for pi, blockIdx := range cd.presentMap {
		for j, fp := range cd.topkFP[pi] {
			if fp == valFP {
				out[blockIdx] = cd.topkCount[pi][j]
				break
			}
		}
	}
	return out
}

// FuseContains returns true per block if the bloom filter indicates valHash may be present.
// Returns true (conservative) for present blocks that have no bloom data (legacy fuse files).
// Queries bloom data directly from the raw byte slices — no allocation.
// Implements queryplanner.ColumnSketch; name kept for interface compatibility.
func (cd *columnSketchData) FuseContains(valHash uint64) []bool {
	out := make([]bool, cd.numBlocks)
	for pi, blockIdx := range cd.presentMap {
		if pi >= len(cd.bloom) || cd.bloom[pi] == nil {
			out[blockIdx] = true // conservative: no bloom data for this block
		} else {
			out[blockIdx] = sketch.BloomContains(cd.bloom[pi], valHash)
		}
	}
	return out
}

// parseSketchIndexSection parses the sketch index section from data (column-major format).
// Returns (*sketchIndex, bytesConsumed, nil) on success.
// Returns (nil, 0, nil) when data does not start with a recognized magic (graceful degradation).
// Returns (nil, 0, error) on parse failure after the magic is confirmed.
//
// Three magic values are handled:
//   - 0x534B5445 ("SKTE"): bloom only, no CMS (current) — bloom data parsed per block.
//   - 0x534B5444 ("SKTD"): bloom + CMS — CMS bytes skipped, bloom parsed per block.
//   - 0x534B5443 ("SKTC"): legacy fuse-based format — fuse bytes skipped; bloom left nil
//     so FuseContains returns true (conservative, no pruning for old blocks).
//
// Returns nil, nil if data is empty or too short.
func parseColumnPresence(
	data []byte,
	pos int,
	name string,
	numBlocks, presenceBytes int,
	cd *columnSketchData,
) (int, int, error) {
	if pos+presenceBytes > len(data) {
		return pos, 0, fmt.Errorf("sketch_index: col %q: too short for presence", name)
	}
	presenceRaw := data[pos : pos+presenceBytes]
	pos += presenceBytes

	// Build presence bitset ([]uint64) and presentMap.
	presenceWords := (numBlocks + 63) / 64
	if presenceWords > 0 {
		cd.presence = make([]uint64, presenceWords)
		for byteIdx, b := range presenceRaw {
			wordIdx := byteIdx / 8
			bitShift := uint(byteIdx%8) * 8
			cd.presence[wordIdx] |= uint64(b) << bitShift
		}
	}

	// Build presentMap: which block indices have this column.
	for blockIdx := range numBlocks {
		byteIdx := blockIdx / 8
		bitIdx := uint(blockIdx % 8)
		if byteIdx < len(presenceRaw) && presenceRaw[byteIdx]>>bitIdx&1 == 1 {
			cd.presentMap = append(cd.presentMap, blockIdx)
		}
	}
	return pos, len(cd.presentMap), nil
}

// parseColumnDistinct stores the per-block distinct count section as a zero-copy sub-slice.
// NOTE-PERF-SKETCH: avoids make([]uint32, numBlocks) per column at parse time.
// Distinct() and distinctAt() decode on demand from distinctRaw.
func parseColumnDistinct(data []byte, pos int, name string, numBlocks int, cd *columnSketchData) (int, error) {
	need := numBlocks * 4
	if pos+need > len(data) {
		return pos, fmt.Errorf("sketch_index: col %q: too short for distinct counts", name)
	}
	cd.distinctRaw = data[pos : pos+need]
	return pos + need, nil
}

// parseColumnTopK parses topk_k and per-present-block top-K fingerprint/count entries.
func parseColumnTopK(data []byte, pos int, name string, presentCount int, cd *columnSketchData) (int, error) {
	// topk_k[1]
	if pos >= len(data) {
		return pos, fmt.Errorf("sketch_index: col %q: missing topk_k", name)
	}
	topkK := int(data[pos])
	pos++
	// Accept any topkK in [1, 255] for forward compatibility — a future writer may
	// use a larger TopKSize. Per-block entry counts (1-byte) are bounded independently.
	if topkK <= 0 {
		return pos, fmt.Errorf("sketch_index: col %q: invalid topk_k=%d", name, topkK)
	}

	// Per present block: topk_entry_count[1] + entries (fp[8 LE] + count[2 LE]).
	cd.topkFP = make([][]uint64, presentCount)
	cd.topkCount = make([][]uint16, presentCount)
	for pi := range presentCount {
		if pos >= len(data) {
			return pos, fmt.Errorf("sketch_index: col %q: present block %d: missing topk entry count", name, pi)
		}
		entryCount := int(data[pos])
		pos++
		if entryCount > topkK {
			return pos, fmt.Errorf(
				"sketch_index: col %q: present block %d: topk entry count %d exceeds declared topk_k=%d",
				name, pi, entryCount, topkK,
			)
		}
		fps := make([]uint64, entryCount)
		counts := make([]uint16, entryCount)
		for ei := range entryCount {
			if pos+10 > len(data) {
				return pos, fmt.Errorf("sketch_index: col %q: topk entry %d/%d: too short", name, ei, entryCount)
			}
			fps[ei] = binary.LittleEndian.Uint64(data[pos:])
			pos += 8
			counts[ei] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		}
		cd.topkFP[pi] = fps
		cd.topkCount[pi] = counts
	}
	return pos, nil
}

// skipColumnCMS advances pos past CMS bytes without any allocation.
// Used when reading old "SKTD" or "SKTC" files that contain CMS data.
// Size math is done in uint64 to prevent int overflow on corrupt inputs.
func parseColumnBloom(data []byte, pos int, name string, presentCount int, cd *columnSketchData) (int, error) {
	// bloom_size[2 LE]: the fixed byte size for all blocks in this column.
	if pos+2 > len(data) {
		return pos, fmt.Errorf("sketch_index: col %q: too short for bloom_size", name)
	}
	bloomSize := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if bloomSize == 0 {
		return pos, fmt.Errorf("sketch_index: col %q: invalid bloom_size=0", name)
	}

	cd.bloom = make([][]byte, presentCount)
	for pi := range presentCount {
		if pos+bloomSize > len(data) {
			return pos, fmt.Errorf("sketch_index: col %q: present block %d: too short for bloom_data", name, pi)
		}
		cd.bloom[pi] = data[pos : pos+bloomSize] // zero-copy slice into metadata buffer
		pos += bloomSize
	}
	return pos, nil
}

// skipColumnFuse advances pos past the legacy fuse section without storing data.
// Used when reading old "SKTC" files; bloom is left nil so FuseContains returns true.
// fuseLen is validated as uint32 against remaining data to prevent int overflow.
func parseOneColumnSketchBlob(data []byte) (*columnSketchData, error) {
	if len(data) < 4 {
		return nil, nil
	}
	rawBlocks := binary.LittleEndian.Uint32(data[0:])
	if rawBlocks > uint32(shared.MaxBlocks) { //nolint:gosec
		return nil, fmt.Errorf("parseOneColumnSketchBlob: numBlocks %d exceeds limit", rawBlocks)
	}
	numBlocks := int(rawBlocks)
	presenceBytes := (numBlocks + 7) / 8
	pos := 4

	cd := &columnSketchData{numBlocks: numBlocks}

	var err error
	var presentCount int
	pos, presentCount, err = parseColumnPresence(data, pos, "", numBlocks, presenceBytes, cd)
	if err != nil {
		return nil, fmt.Errorf("parseOneColumnSketchBlob: presence: %w", err)
	}

	pos, err = parseColumnDistinct(data, pos, "", numBlocks, cd)
	if err != nil {
		return nil, fmt.Errorf("parseOneColumnSketchBlob: distinct: %w", err)
	}

	pos, err = parseColumnTopK(data, pos, "", presentCount, cd)
	if err != nil {
		return nil, fmt.Errorf("parseOneColumnSketchBlob: topk: %w", err)
	}

	pos, err = parseColumnBloom(data, pos, "", presentCount, cd)
	if err != nil {
		return nil, fmt.Errorf("parseOneColumnSketchBlob: bloom: %w", err)
	}

	_ = pos
	return cd, nil
}

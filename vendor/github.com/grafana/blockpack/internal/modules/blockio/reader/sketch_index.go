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

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

const (
	sketchSectionMagicNoCMS  = uint32(0x534B5445) // "SKTE" — bloom only, no CMS (current)
	sketchSectionMagicBloom  = uint32(0x534B5444) // "SKTD" — bloom + CMS (skip CMS bytes)
	sketchSectionMagicLegacy = uint32(0x534B5443) // "SKTC" — fuse variant (old, read-only)
)

// Ensure columnSketchData satisfies queryplanner.ColumnSketch at compile time.
var _ queryplanner.ColumnSketch = (*columnSketchData)(nil)

// columnSketchData holds parsed column-major sketch data for one column across all blocks.
// Bloom data is fixed-size and copied directly at parse time.
type columnSketchData struct {
	presence   []uint64   // bitset: 1 bit per block
	topkFP     [][]uint64 // [presentIdx][entries] fingerprints
	topkCount  [][]uint16 // [presentIdx][entries] counts
	presentMap []int      // presentMap[i] = blockIdx of the i-th present block

	// NOTE-PERF-SKETCH: distinctRaw stores raw 4-byte-per-block distinct counts as a zero-copy
	// sub-slice of the metadata buffer. Distinct() decodes on demand, eliminating make([]uint32,
	// numBlocks) per column at parse time. distinctAt() provides single-block access used by
	// layout and file_sketch_summary.
	distinctRaw []byte // numBlocks×4 LE uint32s; zero-copy sub-slice of metadataBytes

	// Bloom filters: raw byte slices into the metadata buffer, one per present block.
	// Zero-copy: slices reference the decompressed metadata buffer retained by the Reader.
	// Nil/empty for blocks parsed from legacy fuse-format files (FuseContains returns true).
	bloom [][]byte // [presentIdx] each slice is exactly sketch.SketchBloomBytes bytes

	numBlocks int
}

// sketchIndex holds all column sketch data for the file.
type sketchIndex struct {
	columns   map[string]*columnSketchData
	numBlocks int
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
func parseSketchIndexSection(data []byte) (*sketchIndex, int, error) {
	if len(data) < 12 {
		return nil, 0, nil // too short for magic+num_blocks+num_columns
	}
	magic := binary.LittleEndian.Uint32(data[0:])
	isNoCMS := magic == sketchSectionMagicNoCMS
	isBloom := magic == sketchSectionMagicBloom
	isLegacy := magic == sketchSectionMagicLegacy
	if !isNoCMS && !isBloom && !isLegacy {
		return nil, 0, nil // not a sketch section — old file format, degrade gracefully
	}

	// hasCMS is true for old formats (SKTD, SKTC) that contain CMS bytes in the stream.
	hasCMS := isBloom || isLegacy

	rawBlocks := binary.LittleEndian.Uint32(data[4:])
	rawColumns := binary.LittleEndian.Uint32(data[8:])
	pos := 12

	// Validate as uint32 before converting to int to avoid wrap-around on 32-bit platforms.
	if rawBlocks > uint32(shared.MaxBlocks) || rawColumns > uint32(shared.MaxColumns) {
		return nil, 0, fmt.Errorf(
			"sketch index: numBlocks %d or numColumns %d exceeds limits (%d/%d)",
			rawBlocks, rawColumns, shared.MaxBlocks, shared.MaxColumns,
		)
	}
	numBlocks := int(rawBlocks)
	numColumns := int(rawColumns)

	presenceBytes := (numBlocks + 7) / 8

	idx := &sketchIndex{
		numBlocks: numBlocks,
		columns:   make(map[string]*columnSketchData, numColumns),
	}

	for colI := range numColumns {
		// col_name_len[2 LE] + col_name[N]
		if pos+2 > len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %d: too short for name_len", colI)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+nameLen > len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %d: name_len %d exceeds data", colI, nameLen)
		}
		name := string(data[pos : pos+nameLen])
		pos += nameLen

		cd := &columnSketchData{numBlocks: numBlocks}

		var err error
		var presentCount int
		pos, presentCount, err = parseColumnPresence(data, pos, name, numBlocks, presenceBytes, cd)
		if err != nil {
			return nil, 0, err
		}

		pos, err = parseColumnDistinct(data, pos, name, numBlocks, cd)
		if err != nil {
			return nil, 0, err
		}

		pos, err = parseColumnTopK(data, pos, name, presentCount, cd)
		if err != nil {
			return nil, 0, err
		}

		if hasCMS {
			pos, err = skipColumnCMS(data, pos, name, presentCount)
			if err != nil {
				return nil, 0, err
			}
		}

		if isNoCMS || isBloom {
			pos, err = parseColumnBloom(data, pos, name, presentCount, cd)
		} else {
			pos, err = skipColumnFuse(data, pos, name, presentCount)
		}
		if err != nil {
			return nil, 0, err
		}

		idx.columns[name] = cd
	}

	return idx, pos, nil
}

// parseColumnPresence parses the presence bitset and builds presentMap.
// Returns (newPos, presentCount, error).
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
func skipColumnCMS(data []byte, pos int, name string, presentCount int) (int, error) {
	// cms_depth[1]
	if pos >= len(data) {
		return pos, fmt.Errorf("sketch_index: col %q: missing cms_depth", name)
	}
	cmsDepth := int(data[pos])
	pos++
	if cmsDepth == 0 {
		return pos, fmt.Errorf("sketch_index: col %q: invalid cms_depth=0", name)
	}

	// cms_width[2 LE]
	if pos+2 > len(data) {
		return pos, fmt.Errorf("sketch_index: col %q: missing cms_width", name)
	}
	cmsWidth := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if cmsWidth == 0 {
		return pos, fmt.Errorf("sketch_index: col %q: invalid cms_width=0", name)
	}

	// Use uint64 to avoid int overflow on corrupt/large depth*width*presentCount values.
	//nolint:gosec // G115: conversions to uint64 are intentional to prevent overflow
	totalCMS64 := uint64(presentCount) * uint64(cmsDepth) * uint64(cmsWidth) * 2
	remaining := uint64(len(data) - pos) //nolint:gosec // G115: len-pos is non-negative; validated above
	if totalCMS64 > remaining {
		return pos, fmt.Errorf("sketch_index: col %q: too short for CMS data", name)
	}
	pos += int(totalCMS64) //nolint:gosec // safe: validated against remaining data length above
	return pos, nil
}

// parseColumnBloom parses bloom_size[2 LE] and per-present-block bloom_data[bloom_size].
// Bloom data is stored as zero-copy slices into the existing metadata buffer — no allocation.
// The Reader retains the full decompressed metadata buffer, so these slices remain valid
// for the lifetime of the Reader.
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
func skipColumnFuse(data []byte, pos int, name string, presentCount int) (int, error) {
	for pi := range presentCount {
		if pos+4 > len(data) {
			return pos, fmt.Errorf("sketch_index: col %q: present block %d: too short for fuse_len", name, pi)
		}
		fuseLen := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		if fuseLen > 0 {
			if uint64(fuseLen) > uint64(len(data)-pos) { //nolint:gosec // G115: intentional promotion to uint64
				return pos, fmt.Errorf("sketch_index: col %q: present block %d: fuse data too short", name, pi)
			}
			pos += int(fuseLen) //nolint:gosec // safe: validated against remaining data length above
		}
	}
	return pos, nil
}

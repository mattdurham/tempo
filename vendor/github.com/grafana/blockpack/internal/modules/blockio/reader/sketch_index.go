// Package reader provides block reading and index parsing for blockpack files.
// NOTE: Sketch index parsing for column-major HLL + CMS + BinaryFuse8 + TopK data.
// Files without the SKTC magic at the current parse position degrade gracefully:
// parseSketchIndexSection returns (nil, 0, nil) and ColumnSketch returns nil.
//
// CMS and fuse filters use lazy deserialization: raw byte slices are retained at
// parse time and deserialized on first query access. This keeps ReaderOpen fast
// and avoids allocating memory for columns that are never queried.
package reader

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/modules/sketch"
)

const (
	sketchSectionMagic = uint32(0x534B5443) // "SKTC" — must match writer/sketch_index.go
)

// columnSketchData holds parsed column-major sketch data for one column across all blocks.
// CMS and fuse are lazily deserialized on first access to avoid paying the cost at file open.
type columnSketchData struct {
	presence   []uint64   // bitset: 1 bit per block
	distinct   []uint32   // one per block (0 for absent)
	topkFP     [][]uint64 // [presentIdx][entries] fingerprints
	topkCount  [][]uint16 // [presentIdx][entries] counts
	presentMap []int      // presentMap[i] = blockIdx of the i-th present block

	// Lazy CMS: raw byte slices stored at parse time, deserialized on first CMSEstimate call.
	cmsRaw [][]byte                 // [presentIdx] raw CMS data (nil after inflate)
	cms    []*sketch.CountMinSketch // [presentIdx] populated lazily

	// Lazy fuse: raw byte slices stored at parse time, deserialized on first FuseContains call.
	fuseRaw     [][]byte              // [presentIdx] raw fuse data (nil after inflate)
	fuseRawLens []int                 // [presentIdx] original fuse data byte lengths (preserved after inflate)
	fuse        []*sketch.BinaryFuse8 // [presentIdx] populated lazily
	numBlocks   int

	cmsOnce sync.Once

	fuseOnce sync.Once
}

// sketchIndex holds all column sketch data for the file.
type sketchIndex struct {
	columns   map[string]*columnSketchData
	numBlocks int
}

// Presence returns a bitset with 1 bit per block (1 = column present in block).
func (cd *columnSketchData) Presence() []uint64 { return cd.presence }

// Distinct returns pre-computed HLL cardinality per block (0 for absent blocks).
func (cd *columnSketchData) Distinct() []uint32 { return cd.distinct }

// inflateCMS deserializes all CMS instances from raw bytes (called once).
func (cd *columnSketchData) inflateCMS() {
	cd.cms = make([]*sketch.CountMinSketch, len(cd.cmsRaw))
	for pi, raw := range cd.cmsRaw {
		if raw == nil {
			continue
		}
		cms := sketch.NewCountMinSketch()
		if err := cms.Unmarshal(raw); err == nil {
			cd.cms[pi] = cms
		}
	}
	cd.cmsRaw = nil // release raw bytes
}

// CMSEstimate returns CMS frequency estimate for val per block (0 for absent).
// For present blocks where CMS deserialization failed, returns math.MaxUint32 as a
// conservative "possibly present" signal to avoid false-negative pruning.
func (cd *columnSketchData) CMSEstimate(val string) []uint32 {
	cd.cmsOnce.Do(cd.inflateCMS)
	out := make([]uint32, cd.numBlocks)
	for pi, blockIdx := range cd.presentMap {
		if pi < len(cd.cms) && cd.cms[pi] != nil {
			out[blockIdx] = uint32(cd.cms[pi].Estimate(val)) //nolint:gosec // safe: uint16 fits in uint32
		} else if pi < len(cd.cms) {
			// Deserialization failed — conservative: treat as possibly present.
			out[blockIdx] = math.MaxUint32
		}
	}
	return out
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

// inflateFuse deserializes all fuse filters from raw bytes (called once).
func (cd *columnSketchData) inflateFuse() {
	cd.fuse = make([]*sketch.BinaryFuse8, len(cd.fuseRaw))
	for pi, raw := range cd.fuseRaw {
		if len(raw) == 0 {
			continue
		}
		f := &sketch.BinaryFuse8{}
		if err := f.UnmarshalBinary(raw); err == nil {
			cd.fuse[pi] = f
		}
	}
	cd.fuseRaw = nil // release raw bytes
}

// FuseContains returns true per block if the fuse filter indicates valHash may be present.
// Returns true (conservative) for blocks without a fuse filter.
func (cd *columnSketchData) FuseContains(valHash uint64) []bool {
	cd.fuseOnce.Do(cd.inflateFuse)
	out := make([]bool, cd.numBlocks)
	for pi, blockIdx := range cd.presentMap {
		if pi >= len(cd.fuse) || cd.fuse[pi] == nil {
			out[blockIdx] = true // conservative
		} else {
			out[blockIdx] = cd.fuse[pi].Contains(valHash)
		}
	}
	return out
}

// Ensure columnSketchData satisfies queryplanner.ColumnSketch at compile time.
var _ queryplanner.ColumnSketch = (*columnSketchData)(nil)

// parseSketchIndexSection parses the sketch index section from data (column-major format).
// Returns (*sketchIndex, bytesConsumed, nil) on success.
// Returns (nil, 0, nil) when data does not start with the SKTC magic (graceful degradation).
// Returns (nil, 0, error) on parse failure after the magic is confirmed.
//
// CMS and fuse data are stored as raw byte slices and deserialized lazily on first access.
func parseSketchIndexSection(data []byte) (*sketchIndex, int, error) {
	if len(data) < 12 {
		return nil, 0, nil // too short for magic+num_blocks+num_columns
	}
	magic := binary.LittleEndian.Uint32(data[0:])
	if magic != sketchSectionMagic {
		return nil, 0, nil // not a sketch section — old file format, degrade gracefully
	}

	numBlocks := int(binary.LittleEndian.Uint32(data[4:]))
	numColumns := int(binary.LittleEndian.Uint32(data[8:]))
	pos := 12

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

		// presence_bytes[ceil(numBlocks/8)]
		if pos+presenceBytes > len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %q: too short for presence", name)
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
		presentCount := len(cd.presentMap)

		// distinct_count[numBlocks × 4 LE uint32]
		if pos+numBlocks*4 > len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %q: too short for distinct counts", name)
		}
		cd.distinct = make([]uint32, numBlocks)
		for i := range numBlocks {
			cd.distinct[i] = binary.LittleEndian.Uint32(data[pos:])
			pos += 4
		}

		// topk_k[1]
		if pos >= len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %q: missing topk_k", name)
		}
		topkK := int(data[pos])
		pos++
		// Accept any topkK in [1, 255] for forward compatibility — a future writer may
		// use a larger TopKSize. Per-block entry counts (1-byte) are bounded independently.
		if topkK <= 0 {
			return nil, 0, fmt.Errorf("sketch_index: col %q: invalid topk_k=%d", name, topkK)
		}

		// Per present block: topk_entry_count[1] + entries (fp[8 LE] + count[2 LE]).
		cd.topkFP = make([][]uint64, presentCount)
		cd.topkCount = make([][]uint16, presentCount)
		for pi := range presentCount {
			if pos >= len(data) {
				return nil, 0, fmt.Errorf("sketch_index: col %q: present block %d: missing topk entry count", name, pi)
			}
			entryCount := int(data[pos])
			pos++
			if entryCount > topkK {
				return nil, 0, fmt.Errorf(
					"sketch_index: col %q: present block %d: topk entry count %d exceeds declared topk_k=%d",
					name, pi, entryCount, topkK,
				)
			}
			fps := make([]uint64, entryCount)
			counts := make([]uint16, entryCount)
			for ei := range entryCount {
				if pos+10 > len(data) {
					return nil, 0, fmt.Errorf("sketch_index: col %q: topk entry %d/%d: too short", name, ei, entryCount)
				}
				fps[ei] = binary.LittleEndian.Uint64(data[pos:])
				pos += 8
				counts[ei] = binary.LittleEndian.Uint16(data[pos:])
				pos += 2
			}
			cd.topkFP[pi] = fps
			cd.topkCount[pi] = counts
		}

		// cms_depth[1]
		if pos >= len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %q: missing cms_depth", name)
		}
		cmsDepth := int(data[pos])
		pos++
		if cmsDepth != sketch.CMSDepth {
			return nil, 0, fmt.Errorf(
				"sketch_index: col %q: unexpected cms_depth=%d, want %d",
				name,
				cmsDepth,
				sketch.CMSDepth,
			)
		}

		// cms_width[2 LE]
		if pos+2 > len(data) {
			return nil, 0, fmt.Errorf("sketch_index: col %q: missing cms_width", name)
		}
		cmsWidth := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if cmsWidth != sketch.CMSWidth {
			return nil, 0, fmt.Errorf(
				"sketch_index: col %q: unexpected cms_width=%d, want %d",
				name,
				cmsWidth,
				sketch.CMSWidth,
			)
		}

		// Per present block: cms_counters[depth × width × 2 LE].
		// Lazy: store raw byte slices, defer Unmarshal to first CMSEstimate call.
		cmsMarshalSize := sketch.CMSDepth * sketch.CMSWidth * 2
		cd.cmsRaw = make([][]byte, presentCount)
		for pi := range presentCount {
			if pos+cmsMarshalSize > len(data) {
				return nil, 0, fmt.Errorf("sketch_index: col %q: present block %d: too short for CMS", name, pi)
			}
			// Copy to avoid pinning the entire metadata buffer until lazy inflation.
			rawCopy := make([]byte, cmsMarshalSize)
			copy(rawCopy, data[pos:pos+cmsMarshalSize])
			cd.cmsRaw[pi] = rawCopy
			pos += cmsMarshalSize
		}

		// Per present block: fuse_len[4 LE] + fuse_data[fuse_len].
		// Lazy: store raw byte slices, defer UnmarshalBinary to first FuseContains call.
		// fuseRawLens records the original byte length so estimateSketchSectionSize can
		// report accurate sizes without calling MarshalBinary after raw bytes are freed.
		cd.fuseRaw = make([][]byte, presentCount)
		cd.fuseRawLens = make([]int, presentCount)
		for pi := range presentCount {
			if pos+4 > len(data) {
				return nil, 0, fmt.Errorf("sketch_index: col %q: present block %d: too short for fuse_len", name, pi)
			}
			fuseLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			cd.fuseRawLens[pi] = fuseLen
			if fuseLen > 0 {
				if pos+fuseLen > len(data) {
					return nil, 0, fmt.Errorf("sketch_index: col %q: present block %d: fuse data too short", name, pi)
				}
				// Copy to avoid pinning the entire metadata buffer until lazy inflation.
				fuseCopy := make([]byte, fuseLen)
				copy(fuseCopy, data[pos:pos+fuseLen])
				cd.fuseRaw[pi] = fuseCopy
				pos += fuseLen
			}
		}

		idx.columns[name] = cd
	}

	return idx, pos, nil
}

package reader

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// FileSketchSummary is a file-level aggregation of per-block sketch data.
// It merges CMS counters, HLL cardinalities, and TopK entries across all blocks
// so callers can make cross-file pruning decisions without opening the file.
//
// Usage:
//
//	summary := r.FileSketchSummary()
//	b, _ := MarshalFileSketchSummary(summary)
//	// store b in a cache keyed by (path, fileSize)
//
//	// later, from cache:
//	summary, _ := UnmarshalFileSketchSummary(b)
//	col := summary.Columns["resource.service.name"]
//	if col != nil && col.CMS.Estimate("my-service") == 0 {
//	    // skip this file — value definitely absent
//	}
//
// NOTE: No file-level fuse filter is included. Fuse filters are not mergeable without
// the original fingerprint keys, and CMS=0 is an equivalent absence signal with zero
// false negatives. Block-level fuse filters continue to serve within-file pruning.
import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

const fileSketchSummaryMagic = uint32(0x46534B54) // "FSKT"

// FileColumnSketch holds file-level aggregated sketch data for one column.
type FileColumnSketch struct {
	// CMS is the merged Count-Min Sketch across all blocks (element-wise sum, saturating).
	// Estimate(value)==0 means the value is definitely absent from the entire file.
	CMS *sketch.CountMinSketch

	// TopK holds the top-K most frequent values across all blocks, merged by fingerprint.
	// Entries are sorted by count descending.
	TopK []FileTopKEntry

	// TotalDistinct is the sum of per-block HLL cardinalities.
	// This is an over-estimate when the same value appears in multiple blocks,
	// but useful for selectivity hints.
	TotalDistinct uint32
}

// FileTopKEntry is one entry in the file-level TopK list.
type FileTopKEntry struct {
	FP    uint64 // value fingerprint (sketch.HashForFuse)
	Count uint32 // aggregate count across all blocks
}

// FileSketchSummary is the file-level aggregation of per-block sketch data.
// Computed lazily on first call to Reader.FileSketchSummary().
type FileSketchSummary struct {
	// Columns maps column name to aggregated sketch data.
	// Nil when the file has no sketch section (old format).
	Columns map[string]*FileColumnSketch
}

// FileSketchSummary returns the file-level aggregated sketch summary.
// The result is computed once per file across all queries using the process-level
// parsedSketchSummaryCache (GC-cooperative weak.Pointer). Falls back to building
// from sketchIdx when the cache entry has been reclaimed.
// Returns nil when the file has no sketch section (old format — degrade gracefully).
func (r *Reader) FileSketchSummary() *FileSketchSummary {
	if r.sketchIdx == nil {
		return nil
	}
	r.fileSummaryOnce.Do(func() {
		// Check process-level cache — avoids the expensive CMS merge + TopK
		// aggregation on every query for the same file.
		if r.fileID != "" {
			if cached := parsedSketchSummaryCache.Get(r.fileID + "/sketch-summary"); cached != nil {
				r.fileSummary = cached
				return
			}
		}
		r.fileSummary = buildFileSketchSummary(r.sketchIdx)
		if r.fileID != "" {
			_ = parsedSketchSummaryCache.Put(r.fileID+"/sketch-summary", r.fileSummary)
		}
	})
	return r.fileSummary
}

// buildFileSketchSummary aggregates per-block sketch data into a file-level summary.
func buildFileSketchSummary(idx *sketchIndex) *FileSketchSummary {
	summary := &FileSketchSummary{
		Columns: make(map[string]*FileColumnSketch, len(idx.columns)),
	}

	for name, cd := range idx.columns {
		col := buildFileColumnSketch(cd)
		if col != nil {
			summary.Columns[name] = col
		}
	}
	return summary
}

// buildFileColumnSketch merges all block-level sketch data for one column.
func buildFileColumnSketch(cd *columnSketchData) *FileColumnSketch {
	if len(cd.presentMap) == 0 {
		return nil
	}

	// Inflate CMS if not already done.
	cd.cmsOnce.Do(cd.inflateCMS)

	// Merge CMS: element-wise sum across all present blocks (saturating uint16).
	merged := sketch.NewCountMinSketch()
	for _, cms := range cd.cms {
		if cms != nil {
			merged.Merge(cms)
		}
	}

	// Sum distinct counts (over-estimate when values repeat across blocks).
	var totalDistinct uint32
	for _, d := range cd.distinct {
		totalDistinct += d
	}

	// Merge TopK: aggregate counts by fingerprint across all blocks.
	topkAgg := make(map[uint64]uint32, sketch.TopKSize*len(cd.presentMap))
	for pi := range cd.presentMap {
		for j, fp := range cd.topkFP[pi] {
			topkAgg[fp] += uint32(cd.topkCount[pi][j])
		}
	}
	topk := make([]FileTopKEntry, 0, len(topkAgg))
	for fp, count := range topkAgg {
		topk = append(topk, FileTopKEntry{FP: fp, Count: count})
	}
	slices.SortFunc(topk, func(a, b FileTopKEntry) int { return cmp.Compare(b.Count, a.Count) })
	if len(topk) > sketch.TopKSize {
		topk = topk[:sketch.TopKSize]
	}

	return &FileColumnSketch{
		CMS:           merged,
		TotalDistinct: totalDistinct,
		TopK:          topk,
	}
}

// MarshalFileSketchSummary serializes a FileSketchSummary to bytes.
// Wire format:
//
//	magic[4 LE] = 0x46534B54 "FSKT"
//	num_columns[4 LE]
//	per column (sorted by name):
//	  name_len[2 LE] + name[N]
//	  total_distinct[4 LE]
//	  cms_bytes[CMSDepth × CMSWidth × 2]
//	  topk_count[1]
//	  per topk entry: fp[8 LE] + count[4 LE]
func MarshalFileSketchSummary(s *FileSketchSummary) ([]byte, error) {
	if s == nil {
		return nil, nil
	}

	// Sort column names for deterministic output, excluding nil entries.
	names := make([]string, 0, len(s.Columns))
	for name, col := range s.Columns {
		if col != nil {
			names = append(names, name)
		}
	}
	slices.Sort(names)

	cmsSz := sketch.CMSDepth * sketch.CMSWidth * 2
	buf := make([]byte, 0, 8+len(names)*(32+4+cmsSz+1+sketch.TopKSize*12))

	var tmp4 [4]byte
	var tmp2 [2]byte

	// magic
	binary.LittleEndian.PutUint32(tmp4[:], fileSketchSummaryMagic)
	buf = append(buf, tmp4[:]...)

	// num_columns — count of non-nil columns only (nil entries are excluded above).
	binary.LittleEndian.PutUint32(tmp4[:], uint32(len(names))) //nolint:gosec // safe: len fits uint32
	buf = append(buf, tmp4[:]...)

	for _, name := range names {
		col := s.Columns[name]

		// name_len[2] + name
		binary.LittleEndian.PutUint16(tmp2[:], uint16(len(name))) //nolint:gosec // safe: name len fits uint16
		buf = append(buf, tmp2[:]...)
		buf = append(buf, name...)

		// total_distinct[4]
		binary.LittleEndian.PutUint32(tmp4[:], col.TotalDistinct)
		buf = append(buf, tmp4[:]...)

		// cms_bytes
		if col.CMS != nil {
			buf = append(buf, col.CMS.Marshal()...)
		} else {
			buf = append(buf, make([]byte, cmsSz)...)
		}

		// topk_count[1] + entries
		buf = append(buf, byte(len(col.TopK))) //nolint:gosec // safe: len(col.TopK) <= TopKSize which fits byte
		for _, entry := range col.TopK {
			var tmp8 [8]byte
			binary.LittleEndian.PutUint64(tmp8[:], entry.FP)
			buf = append(buf, tmp8[:]...)
			binary.LittleEndian.PutUint32(tmp4[:], entry.Count)
			buf = append(buf, tmp4[:]...)
		}
	}

	return buf, nil
}

// UnmarshalFileSketchSummary deserializes a FileSketchSummary from bytes.
// Returns nil, nil for a nil or empty input (old files without summary).
func UnmarshalFileSketchSummary(b []byte) (*FileSketchSummary, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) < 8 {
		return nil, fmt.Errorf("UnmarshalFileSketchSummary: too short")
	}
	magic := binary.LittleEndian.Uint32(b[0:])
	if magic != fileSketchSummaryMagic {
		return nil, fmt.Errorf("UnmarshalFileSketchSummary: bad magic %08x", magic)
	}
	numCols := int(binary.LittleEndian.Uint32(b[4:]))
	pos := 8

	cmsSz := sketch.CMSDepth * sketch.CMSWidth * 2
	summary := &FileSketchSummary{
		Columns: make(map[string]*FileColumnSketch, numCols),
	}

	for range numCols {
		if pos+2 > len(b) {
			return nil, fmt.Errorf("UnmarshalFileSketchSummary: truncated at name_len")
		}
		nameLen := int(binary.LittleEndian.Uint16(b[pos:]))
		pos += 2
		if pos+nameLen > len(b) {
			return nil, fmt.Errorf("UnmarshalFileSketchSummary: truncated at name")
		}
		name := string(b[pos : pos+nameLen])
		pos += nameLen

		if pos+4 > len(b) {
			return nil, fmt.Errorf("UnmarshalFileSketchSummary: col %q: truncated at total_distinct", name)
		}
		totalDistinct := binary.LittleEndian.Uint32(b[pos:])
		pos += 4

		if pos+cmsSz > len(b) {
			return nil, fmt.Errorf("UnmarshalFileSketchSummary: col %q: truncated at cms", name)
		}
		cms := sketch.NewCountMinSketch()
		if err := cms.Unmarshal(b[pos : pos+cmsSz]); err != nil {
			return nil, fmt.Errorf("UnmarshalFileSketchSummary: col %q: cms: %w", name, err)
		}
		pos += cmsSz

		// topk_count[1] is always written by MarshalFileSketchSummary even when TopK is empty,
		// so pos == len(b) here means the data is genuinely truncated (not a valid empty-TopK column).
		if pos >= len(b) {
			return nil, fmt.Errorf("UnmarshalFileSketchSummary: col %q: truncated at topk_count", name)
		}
		topkCount := int(b[pos])
		pos++

		topk := make([]FileTopKEntry, topkCount)
		for i := range topkCount {
			if pos+12 > len(b) {
				return nil, fmt.Errorf("UnmarshalFileSketchSummary: col %q: topk entry %d: truncated", name, i)
			}
			topk[i].FP = binary.LittleEndian.Uint64(b[pos:])
			pos += 8
			topk[i].Count = binary.LittleEndian.Uint32(b[pos:])
			pos += 4
		}

		summary.Columns[name] = &FileColumnSketch{
			CMS:           cms,
			TotalDistinct: totalDistinct,
			TopK:          topk,
		}
	}

	return summary, nil
}

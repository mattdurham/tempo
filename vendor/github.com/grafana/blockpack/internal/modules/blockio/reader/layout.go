package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/golang/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// FileLayoutReport is the top-level result of AnalyzeFileLayout.
type FileLayoutReport struct {
	Sections    []FileLayoutSection `json:"sections"`
	RangeIndex  []RangeIndexColumn  `json:"range_index,omitempty"`
	SketchIndex *SketchIndexInfo    `json:"sketch_index,omitempty"`
	// FileBloom summarizes the file-level bloom filter section, if present.
	FileBloom       *FileBloomInfo `json:"file_bloom,omitempty"`
	BlockSpanCounts []uint32       `json:"block_span_counts,omitempty"`
	FileSize        int64          `json:"file_size"`
	TotalSpans      int64          `json:"total_spans"`
	BlockCount      int            `json:"block_count"`
	FileVersion     uint8          `json:"file_version"`
}

// SketchIndexInfo summarizes the sketch index stored in the file.
type SketchIndexInfo struct {
	// Blocks holds one summary per block (parallel to FileLayoutReport.BlockSpanCounts).
	Blocks []BlockSketchSummary `json:"blocks"`
	// TotalBytes is the actual computed uncompressed size of the sketch section.
	TotalBytes int `json:"total_bytes"`
	// HeaderBytes is the fixed 12-byte sketch section header (magic + num_blocks + num_columns).
	HeaderBytes int `json:"header_bytes"`
	// SketchedBlockCount is the number of blocks that have at least one sketched column.
	SketchedBlockCount int `json:"sketched_block_count"`
}

// BlockSketchSummary holds per-column sketch statistics for one block.
type BlockSketchSummary struct {
	// Columns holds sketch stats for each column that has sketch data in this block.
	Columns []ColumnSketchStat `json:"columns"`
}

// ColumnSketchStat holds sketch statistics for one column in one block.
type ColumnSketchStat struct {
	ColumnName string `json:"column_name"`
	// HLLCardinality is the estimated number of distinct values (HyperLogLog).
	HLLCardinality uint64 `json:"hll_cardinality"`
	// FuseBytes is the byte size of the membership filter (SketchBloom for SKTE/SKTD, absent for legacy SKTC).
	FuseBytes int `json:"fuse_bytes,omitempty"`
	// TopKCount is the number of TopK entries for this column (0 if none).
	TopKCount int `json:"top_k_count,omitempty"`
	// TopKBytes is the actual byte size of the TopK entries for this column in this
	// block (1 + len(entries) × 10 bytes).
	TopKBytes int `json:"top_k_bytes,omitempty"`
}

// RangeIndexColumn describes the pruning index for one column.
type RangeIndexColumn struct {
	ColumnName string `json:"column_name"`
	ColumnType string `json:"column_type"`
	// BucketMin is the global minimum value across all blocks for this column.
	BucketMin string `json:"bucket_min,omitempty"`
	// BucketMax is the global maximum value across all blocks for this column.
	BucketMax string             `json:"bucket_max,omitempty"`
	Buckets   []RangeIndexBucket `json:"buckets"`
}

// RangeIndexBucket is one entry in a column's range index: the lower boundary
// of a value bucket and the set of block indexes that cover it.
type RangeIndexBucket struct {
	Start string `json:"start"`
	// End is the upper boundary of this bucket (exclusive). For the last bucket this
	// equals BucketMax of the column. Empty string for string/bytes columns where the
	// upper bound is not encoded.
	End      string   `json:"end,omitempty"`
	BlockIDs []uint32 `json:"block_ids"`
}

// FileLayoutSection describes one contiguous byte range in a blockpack file.
type FileLayoutSection struct {
	Section    string `json:"section"`
	ColumnName string `json:"column_name,omitempty"`
	ColumnType string `json:"column_type,omitempty"`
	Encoding   string `json:"encoding,omitempty"`
	// MinValue is the minimum value of this page (human-readable string).
	MinValue string `json:"min_value,omitempty"`
	// MaxValue is the maximum value of this page (human-readable string).
	MaxValue         string `json:"max_value,omitempty"`
	Offset           int64  `json:"offset"`
	CompressedSize   int64  `json:"compressed_size"`
	UncompressedSize int64  `json:"uncompressed_size,omitempty"`
	BlockIndex       int    `json:"block_index,omitempty"`
	// RowCount is the number of records in this page (intrinsic paged columns only).
	RowCount int `json:"row_count,omitempty"`
	// IsLogical is true for V12 metadata sub-sections whose Offset is relative to
	// the start of the decompressed metadata buffer, not a physical file offset.
	IsLogical bool `json:"is_logical,omitempty"`
}

// FileBloomInfo summarizes the file-level bloom filter section (FBLM).
type FileBloomInfo struct {
	// Columns holds per-column name and filter size.
	Columns []FileBloomColumnInfo `json:"columns"`
	// TotalBytes is the total uncompressed byte size of the FBLM section.
	TotalBytes int `json:"total_bytes"`
}

// FileBloomColumnInfo describes one column's entry in the file bloom section.
type FileBloomColumnInfo struct {
	ColumnName string `json:"column_name"`
	// FuseBytes is the byte size of the BinaryFuse8 filter for this column.
	FuseBytes int `json:"fuse_bytes"`
}

// FileLayout computes a byte-level layout of the blockpack file, returning a report
// that accounts for every byte. The returned Sections slice is sorted by Offset ascending.
// Invariant: sum(section.CompressedSize where !IsLogical) == FileSize.
// Logical sections (IsLogical=true) describe sub-structure within the decompressed
// metadata buffer; their Offset is relative to that buffer, not the physical file.
func (r *Reader) FileLayout() (*FileLayoutReport, error) {
	if r.footerVersion == shared.FooterV7Version {
		return r.fileLayoutV14()
	}

	const headerSize = int64(22) // magic[4] + version[1] + metadataOffset[8] + metadataLen[8] + signalType[1]
	footerSize := int64(shared.FooterV3Size)
	switch r.footerVersion {
	case shared.FooterV4Version:
		footerSize = int64(shared.FooterV4Size)
	case shared.FooterV5Version:
		footerSize = int64(shared.FooterV5Size)
	case shared.FooterV6Version:
		footerSize = int64(shared.FooterV6Size)
	}

	var sections []FileLayoutSection

	// Footer: last 22 bytes.
	sections = append(sections, FileLayoutSection{
		Section:        "footer",
		Offset:         r.fileSize - footerSize,
		CompressedSize: footerSize,
	})

	// File header: 22 bytes at r.headerOffset.
	sections = append(sections, FileLayoutSection{
		Section:        "file_header",
		Offset:         int64(r.headerOffset), //nolint:gosec
		CompressedSize: headerSize,
	})

	// Blocks.
	for blockIdx, meta := range r.blockMetas {
		blockSections, err := r.layoutBlock(blockIdx, meta)
		if err != nil {
			return nil, fmt.Errorf("block %d layout: %w", blockIdx, err)
		}

		sections = append(sections, blockSections...)
	}

	// Metadata sub-sections.
	metaSections, err := r.layoutMetadata()
	if err != nil {
		return nil, fmt.Errorf("metadata layout: %w", err)
	}

	sections = append(sections, metaSections...)

	// Compact trace index (if present).
	if r.compactLen > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        "compact_trace_index",
			Offset:         int64(r.compactOffset), //nolint:gosec
			CompressedSize: int64(r.compactLen),    //nolint:gosec
		})
	}
	// V6: separate compact traces section (snappy-compressed trace index).
	if r.compactTracesLen > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        "compact_trace_index_traces",
			Offset:         int64(r.compactTracesOffset), //nolint:gosec
			CompressedSize: int64(r.compactTracesLen),    //nolint:gosec
		})
	}

	// Intrinsic section (v4+ footer): per-column blobs + TOC.
	isV4Plus := r.footerVersion == shared.FooterV4Version ||
		r.footerVersion == shared.FooterV5Version ||
		r.footerVersion == shared.FooterV6Version
	// V7 is V14 section-directory format — handled by fileLayoutV14 above, never reaches here.
	if isV4Plus && r.intrinsicIndexLen > 0 {
		sections = append(sections, r.layoutIntrinsicSections()...)
	}

	slices.SortFunc(sections, func(a, b FileLayoutSection) int {
		return cmp.Compare(a.Offset, b.Offset)
	})

	rangeIndex := r.buildRangeIndex()
	sketchIndex := r.buildSketchIndexInfo()
	fileBloom := r.buildFileBloomInfo()

	spanCounts := make([]uint32, len(r.blockMetas))
	var totalSpans int64
	for i, m := range r.blockMetas {
		spanCounts[i] = m.SpanCount
		totalSpans += int64(m.SpanCount) //nolint:gosec
	}

	return &FileLayoutReport{
		FileSize:        r.fileSize,
		FileVersion:     r.fileVersion,
		BlockCount:      len(r.blockMetas),
		TotalSpans:      totalSpans,
		BlockSpanCounts: spanCounts,
		Sections:        sections,
		RangeIndex:      rangeIndex,
		SketchIndex:     sketchIndex,
		FileBloom:       fileBloom,
	}, nil
}

// fileLayoutV14 builds the FileLayoutReport for V14 (FooterV7) files.
// V14 layout: blocks + sections (type-keyed + name-keyed) + section_directory + footer.
// There is no file_header or metadata.compressed blob in V14.
func (r *Reader) fileLayoutV14() (*FileLayoutReport, error) {
	sections := make([]FileLayoutSection, 0, 16)

	// Footer: last 18 bytes.
	footerSize := int64(shared.FooterV7Size) //nolint:gosec
	sections = append(sections, FileLayoutSection{
		Section:        "footer",
		Offset:         r.fileSize - footerSize,
		CompressedSize: footerSize,
	})

	// Section directory: snappy-compressed blob at v7DirOffset.
	sections = append(sections, FileLayoutSection{
		Section:        "section_directory",
		Offset:         int64(r.v7DirOffset), //nolint:gosec
		CompressedSize: int64(r.v7DirLen),    //nolint:gosec
	})

	// Type-keyed sections from the section directory.
	sectionNames := map[uint8]string{
		shared.SectionBlockIndex:  "block_index",
		shared.SectionRangeIndex:  "range_index",
		shared.SectionTraceIndex:  "trace_index",
		shared.SectionTSIndex:     "ts_index",
		shared.SectionSketchIndex: "sketch_index",
		shared.SectionFileBloom:   "file_bloom",
	}
	for sType, e := range r.sectionDir.TypeEntries {
		name, ok := sectionNames[sType]
		if !ok {
			name = fmt.Sprintf("section.0x%02X", sType)
		}
		sections = append(sections, FileLayoutSection{
			Section:        "section." + name,
			Offset:         int64(e.Offset),        //nolint:gosec
			CompressedSize: int64(e.CompressedLen), //nolint:gosec
		})
	}

	// Name-keyed entries: one intrinsic column blob per entry.
	// Read each blob to detect paged format and emit per-page sections.
	for name, e := range r.sectionDir.NameEntries {
		pageSections, err := r.layoutIntrinsicColumnV14(name, e)
		if err != nil || len(pageSections) == 0 {
			// Fallback: emit as single section.
			sections = append(sections, FileLayoutSection{
				Section:        "intrinsic.column[" + name + "]",
				ColumnName:     name,
				Offset:         int64(e.Offset),        //nolint:gosec
				CompressedSize: int64(e.CompressedLen), //nolint:gosec
			})
		} else {
			sections = append(sections, pageSections...)
		}
	}

	// Blocks.
	for blockIdx, meta := range r.blockMetas {
		blockSections, err := r.layoutBlockV14(blockIdx, meta)
		if err != nil {
			return nil, fmt.Errorf("block %d layout: %w", blockIdx, err)
		}
		sections = append(sections, blockSections...)
	}

	slices.SortFunc(sections, func(a, b FileLayoutSection) int {
		return cmp.Compare(a.Offset, b.Offset)
	})

	rangeIndex := r.buildRangeIndex()
	sketchIndex := r.buildSketchIndexInfo()
	fileBloom := r.buildFileBloomInfo()

	spanCounts := make([]uint32, len(r.blockMetas))
	var totalSpans int64
	for i, m := range r.blockMetas {
		spanCounts[i] = m.SpanCount
		totalSpans += int64(m.SpanCount) //nolint:gosec
	}

	return &FileLayoutReport{
		FileSize:        r.fileSize,
		FileVersion:     r.fileVersion,
		BlockCount:      len(r.blockMetas),
		TotalSpans:      totalSpans,
		BlockSpanCounts: spanCounts,
		Sections:        sections,
		RangeIndex:      rangeIndex,
		SketchIndex:     sketchIndex,
		FileBloom:       fileBloom,
	}, nil
}

// layoutIntrinsicColumnV14 reads a V14 name-keyed intrinsic column blob and, if it is
// in paged format, returns one physical section per page plus a page_toc header section.
// For non-paged blobs, returns nil so the caller emits a single section instead.
func (r *Reader) layoutIntrinsicColumnV14(colName string, e shared.DirEntryName) ([]FileLayoutSection, error) {
	// GetIntrinsicColumnBlob returns raw (snappy-compressed) bytes for V14 files.
	// We need the decompressed bytes to check for paged format.
	rawBlob, err := r.GetIntrinsicColumnBlob(colName)
	if err != nil || len(rawBlob) == 0 {
		return nil, err
	}

	// For V14 files, blobs on disk are snappy-compressed; decompress to inspect content.
	// Check decoded size before decompressing to guard against decompression bombs.
	blob := rawBlob
	if decodedLen, lenErr := snappy.DecodedLen(rawBlob); lenErr == nil && decodedLen <= shared.MaxBlockSize {
		if dec, decErr := snappy.Decode(nil, rawBlob); decErr == nil {
			blob = dec
		}
		// On decode error: treat as non-paged (not snappy-compressed for V14).
	}
	// If decoded size exceeds MaxBlockSize or DecodedLen fails, treat as non-paged.

	// Non-paged blob: let caller emit the simple section.
	if len(blob) == 0 || blob[0] != shared.IntrinsicPagedVersion {
		return nil, nil
	}

	// Paged format: blob[1:5] = toc_len[4], then toc_blob, then page blobs.
	if len(blob) < 5 {
		return nil, nil
	}
	tocLen := int(binary.LittleEndian.Uint32(blob[1:5]))
	if 5+tocLen > len(blob) {
		return nil, nil
	}

	ptoc, tocErr := shared.DecodePageTOC(blob[5 : 5+tocLen])
	if tocErr != nil {
		return nil, tocErr
	}
	if len(ptoc.Pages) == 0 {
		return nil, nil
	}

	// The blob on disk is snappy-compressed, stored at e.Offset with e.CompressedLen bytes.
	// The uncompressed paged blob has: sentinel[1] + toc_len[4] + toc_blob[tocLen] + pages.
	// page absolute offsets are relative to the compressed blob start on disk —
	// however the paged TOC stores offsets relative to the start of the first page blob
	// within the uncompressed blob. For layout purposes, we emit logical sections.
	headerLen := int64(1) + 4 + int64(tocLen) //nolint:gosec

	sections := make([]FileLayoutSection, 0, 1+len(ptoc.Pages))

	// Emit the TOC header as a logical section (describes uncompressed blob sub-structure).
	// Offset=0: the header occupies bytes [0, headerLen) within the uncompressed blob.
	sections = append(sections, FileLayoutSection{
		Section:        "intrinsic.column[" + colName + "].page_toc",
		ColumnName:     colName,
		Offset:         0, // relative to uncompressed blob; header starts at byte 0
		CompressedSize: headerLen,
		IsLogical:      true,
	})

	// Emit per-page sections as logical sections.
	for pageIdx, pm := range ptoc.Pages {
		pageOffset := headerLen + int64(pm.Offset) //nolint:gosec
		sections = append(sections, FileLayoutSection{
			Section:        fmt.Sprintf("intrinsic.column[%s].page[%d]", colName, pageIdx),
			ColumnName:     colName,
			Offset:         pageOffset,
			CompressedSize: int64(pm.Length), //nolint:gosec
			RowCount:       int(pm.RowCount), //nolint:gosec
			IsLogical:      true,
		})
	}

	// Emit the compressed blob on disk as a single physical section that accounts for the bytes.
	sections = append(sections, FileLayoutSection{
		Section:          "intrinsic.column[" + colName + "]",
		ColumnName:       colName,
		Offset:           int64(e.Offset),        //nolint:gosec
		CompressedSize:   int64(e.CompressedLen), //nolint:gosec
		UncompressedSize: int64(len(blob)),       //nolint:gosec
	})

	return sections, nil
}

// layoutIntrinsicSections returns FileLayoutSection entries for all intrinsic columns
// in legacy (V4/V5/V6 footer) files. It handles both flat and paged column formats.
func (r *Reader) layoutIntrinsicSections() []FileLayoutSection {
	var sections []FileLayoutSection
	var columnsEnd int64

	for _, name := range r.IntrinsicColumnNames() {
		meta, ok := r.IntrinsicColumnMeta(name)
		if !ok {
			continue
		}
		formatName := "flat"
		if meta.Format == shared.IntrinsicFormatDict {
			formatName = "dict"
		}

		blob, blobErr := r.GetIntrinsicColumnBlob(name)
		isPaged := blobErr == nil && len(blob) > 0 && blob[0] == shared.IntrinsicPagedVersion

		pagedEmitted := false
		if isPaged && len(blob) >= 5 {
			tocLen := int(binary.LittleEndian.Uint32(blob[1:5]))
			if 5+tocLen <= len(blob) {
				ptoc, tocErr := shared.DecodePageTOC(blob[5 : 5+tocLen])
				if tocErr == nil && len(ptoc.Pages) > 0 {
					headerLen := int64(1) + 4 + int64(tocLen)            //nolint:gosec
					firstPageAbsOffset := int64(meta.Offset) + headerLen //nolint:gosec
					sections = append(sections, FileLayoutSection{
						Section:        "intrinsic.column[" + name + "].page_toc",
						ColumnName:     name,
						ColumnType:     columnTypeName(meta.Type),
						Offset:         int64(meta.Offset), //nolint:gosec
						CompressedSize: headerLen,
					})
					if end := int64(meta.Offset) + headerLen; end > columnsEnd { //nolint:gosec
						columnsEnd = end
					}
					for pageIdx, pm := range ptoc.Pages {
						pageAbsOffset := firstPageAbsOffset + int64(pm.Offset) //nolint:gosec
						sections = append(sections, FileLayoutSection{
							Section:        fmt.Sprintf("intrinsic.column[%s].page[%d]", name, pageIdx),
							ColumnName:     name,
							ColumnType:     columnTypeName(meta.Type),
							Encoding:       fmt.Sprintf("%s/paged", formatName),
							Offset:         pageAbsOffset,
							CompressedSize: int64(pm.Length), //nolint:gosec
							RowCount:       int(pm.RowCount), //nolint:gosec
							MinValue:       formatIntrinsicBound(meta.Type, pm.Min),
							MaxValue:       formatIntrinsicBound(meta.Type, pm.Max),
						})
						if end := pageAbsOffset + int64(pm.Length); end > columnsEnd { //nolint:gosec
							columnsEnd = end
						}
					}
					pagedEmitted = true
				}
			}
		}

		if !pagedEmitted {
			if isPaged {
				formatName += "/paged"
			}
			sections = append(sections, FileLayoutSection{
				Section:        "intrinsic.column[" + name + "]",
				ColumnName:     name,
				ColumnType:     columnTypeName(meta.Type),
				Encoding:       formatName,
				Offset:         int64(meta.Offset), //nolint:gosec
				CompressedSize: int64(meta.Length), //nolint:gosec
			})
			if end := int64(meta.Offset) + int64(meta.Length); end > columnsEnd { //nolint:gosec
				columnsEnd = end
			}
		}
	}

	// TOC blob: from end of last column blob to end of intrinsic index region.
	tocStart := columnsEnd
	if tocStart == 0 {
		tocStart = int64(r.intrinsicIndexOffset) //nolint:gosec
	}
	tocEnd := int64(r.intrinsicIndexOffset) + int64(r.intrinsicIndexLen) //nolint:gosec
	if tocEnd > tocStart {
		sections = append(sections, FileLayoutSection{
			Section:        "intrinsic.toc",
			Offset:         tocStart,
			CompressedSize: tocEnd - tocStart,
		})
	}
	return sections
}

// layoutBlockV14 returns FileLayoutSection entries for one V14 block.
// Column blobs are snappy-compressed on disk; the encoding kind is extracted by
// snappy-decoding the first 2 bytes of each blob.
func (r *Reader) layoutBlockV14(blockIdx int, meta shared.BlockMeta) ([]FileLayoutSection, error) {
	raw, err := r.ReadBlockRaw(blockIdx)
	if err != nil {
		return nil, fmt.Errorf("ReadBlockRaw: %w", err)
	}

	hdr, err := parseBlockHeader(raw)
	if err != nil {
		return nil, fmt.Errorf("parseBlockHeader: %w", err)
	}

	metas, colMetaEndPos, err := parseColumnMetadataArray(raw, 24, int(hdr.columnCount))
	if err != nil {
		return nil, fmt.Errorf("parseColumnMetadataArray: %w", err)
	}

	prefix := fmt.Sprintf("block[%d]", blockIdx)
	base := int64(meta.Offset) //nolint:gosec
	sections := make([]FileLayoutSection, 0, 3+len(metas)*2)

	// Block header: always 24 bytes.
	sections = append(sections, FileLayoutSection{
		Section:        prefix + ".header",
		Offset:         base,
		CompressedSize: 24,
		BlockIndex:     blockIdx,
	})

	// Column metadata array: bytes [24, colMetaEndPos).
	if colMetaSize := int64(colMetaEndPos - 24); colMetaSize > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        prefix + ".column_metadata",
			Offset:         base + 24,
			CompressedSize: colMetaSize,
			BlockIndex:     blockIdx,
		})
	}

	// Per-column data: each blob is snappy-compressed on disk.
	for _, m := range metas {
		if m.compressedLen == 0 {
			continue
		}
		colType := columnTypeName(m.colType)

		// Get encoding kind by snappy-decoding the first 2 bytes of the blob.
		var encKind string
		start := int(m.dataOffset) //nolint:gosec
		end := start + int(m.compressedLen)
		if end <= len(raw) {
			// SPEC-ROOT-012: guard against decompression-bomb OOM before decoding column blob.
			if m.uncompressedLen <= uint32(shared.MaxBlockSize) { //nolint:gosec
				if decoded, decErr := snappy.Decode(nil, raw[start:end]); decErr == nil && len(decoded) >= 2 {
					encKind = encodingKindName(decoded[1])
				}
			}
		}

		sections = append(sections, FileLayoutSection{
			Section:          prefix + ".column[" + m.name + "].data",
			ColumnName:       m.name,
			ColumnType:       colType,
			Encoding:         encKind,
			Offset:           base + int64(m.dataOffset), //nolint:gosec
			CompressedSize:   int64(m.compressedLen),     //nolint:gosec
			UncompressedSize: int64(m.uncompressedLen),   //nolint:gosec
			BlockIndex:       blockIdx,
		})
	}

	return sections, nil
}

// buildSketchIndexInfo builds the SketchIndexInfo from the reader's parsed column-major sketch data.
// Returns nil when no sketches are present.
func (r *Reader) buildSketchIndexInfo() *SketchIndexInfo {
	_ = r.ensureV14SketchSection()
	if r.sketchIdx == nil || len(r.sketchIdx.columns) == 0 {
		return nil
	}

	numBlocks := r.sketchIdx.numBlocks
	presenceBytes := (numBlocks + 7) / 8
	info := &SketchIndexInfo{
		Blocks:      make([]BlockSketchSummary, numBlocks),
		HeaderBytes: 12,
	}

	type blockColStat struct {
		name        string
		cardinality uint64
		topkCount   int
		topkBytes   int
		fuseBytes   int
	}
	blockCols := make([][]blockColStat, numBlocks)

	totalBytes := 12 // header: magic[4] + num_blocks[4] + num_columns[4]

	for name, cd := range r.sketchIdx.columns {
		presentCount := len(cd.presentMap)
		// Per-column byte accounting.
		totalBytes += 2 + len(name)    // name_len[2] + name
		totalBytes += presenceBytes    // presence bitset
		totalBytes += numBlocks * 4    // distinct counts
		totalBytes += 1 + presentCount // topk_k[1] + entry_count per present block
		hasBloom := cd.bloom != nil
		if hasBloom {
			totalBytes += 2 // bloom_size[2] — only present in SKTE/SKTD formats
		}

		for pi, blockIdx := range cd.presentMap {
			topkEntries := len(cd.topkFP[pi])
			topkBytesForBlock := 1 + topkEntries*10 // entry_count[1] + fp[8]+count[2] per entry
			totalBytes += topkEntries * 10          // (entry_count already counted above)
			bloomB := 0
			if hasBloom && pi < len(cd.bloom) && cd.bloom[pi] != nil {
				bloomB = len(cd.bloom[pi])
				totalBytes += bloomB
			}

			stat := blockColStat{
				name:        name,
				cardinality: uint64(cd.distinctAt(blockIdx)),
				topkCount:   topkEntries,
				topkBytes:   topkBytesForBlock,
				fuseBytes:   bloomB,
			}
			blockCols[blockIdx] = append(blockCols[blockIdx], stat)
		}
	}

	info.TotalBytes = totalBytes

	for blockIdx := range numBlocks {
		cols := blockCols[blockIdx]
		if len(cols) == 0 {
			continue
		}
		info.SketchedBlockCount++

		// Sort by column name for deterministic output.
		slices.SortFunc(cols, func(a, b blockColStat) int { return cmp.Compare(a.name, b.name) })

		stats := make([]ColumnSketchStat, 0, len(cols))
		for _, c := range cols {
			stats = append(stats, ColumnSketchStat{
				ColumnName:     c.name,
				HLLCardinality: c.cardinality,
				FuseBytes:      c.fuseBytes,
				TopKCount:      c.topkCount,
				TopKBytes:      c.topkBytes,
			})
		}
		info.Blocks[blockIdx] = BlockSketchSummary{Columns: stats}
	}

	return info
}

// buildRangeIndex parses every column's range index and returns the result sorted by column name.
func (r *Reader) buildRangeIndex() []RangeIndexColumn {
	_ = r.ensureV14RangeSection()
	if len(r.rangeOffsets) == 0 {
		return nil
	}

	cols := make([]RangeIndexColumn, 0, len(r.rangeOffsets))

	for colName := range r.rangeOffsets {
		if err := r.ensureRangeColumnParsed(colName); err != nil {
			continue
		}

		idx := r.rangeParsed[colName]
		col := RangeIndexColumn{
			ColumnName: colName,
			ColumnType: columnTypeName(idx.colType),
			BucketMin:  formatBucketBound(idx.colType, idx.bucketMin),
			BucketMax:  formatBucketBound(idx.colType, idx.bucketMax),
			Buckets:    make([]RangeIndexBucket, 0, len(idx.entries)),
		}

		for _, entry := range idx.entries {
			col.Buckets = append(col.Buckets, RangeIndexBucket{
				Start:    formatRangeKey(idx.colType, entry.lower),
				BlockIDs: entry.blockIDs,
			})
		}

		// Populate End for each bucket where an upper bound is defined:
		// End[i] = Start[i+1]; End[last] = BucketMax.
		// For string/bytes range columns, BucketMax is empty and End must remain empty
		// because the wire format does not encode an upper boundary.
		if col.BucketMax != "" {
			for i := range col.Buckets {
				if i+1 < len(col.Buckets) {
					col.Buckets[i].End = col.Buckets[i+1].Start
				} else {
					col.Buckets[i].End = col.BucketMax
				}
			}
		}

		cols = append(cols, col)
	}

	slices.SortFunc(cols, func(a, b RangeIndexColumn) int { return cmp.Compare(a.ColumnName, b.ColumnName) })

	return cols
}

// formatBucketBound formats a bucket global min/max stored as int64 bits in parsedRangeIndex.
// The bits field is the raw int64 from bucketMin/bucketMax (wire format: LE uint64 reread as int64).
func formatBucketBound(colType shared.ColumnType, bits int64) string {
	switch colType {
	case shared.ColumnTypeRangeInt64:
		return fmt.Sprintf("%d", bits)
	case shared.ColumnTypeRangeDuration:
		return time.Duration(bits).String()
	case shared.ColumnTypeRangeUint64:
		return fmt.Sprintf("%d", uint64(bits)) //nolint:gosec
	case shared.ColumnTypeRangeFloat64:
		return fmt.Sprintf("%g", math.Float64frombits(uint64(bits))) //nolint:gosec
	default:
		// String/bytes: bucketMin/Max are 0 (not stored in wire format for these types).
		return ""
	}
}

// formatRangeKey decodes an encoded lower-boundary key to a human-readable string.
func formatRangeKey(colType shared.ColumnType, key string) string {
	switch colType {
	case shared.ColumnTypeRangeInt64:
		return fmt.Sprintf("%d", decodeInt64Key(key))
	case shared.ColumnTypeRangeDuration:
		return time.Duration(decodeInt64Key(key)).String()
	case shared.ColumnTypeRangeUint64:
		return fmt.Sprintf("%d", decodeUint64Key(key))
	case shared.ColumnTypeRangeFloat64:
		return fmt.Sprintf("%g", decodeFloat64Key(key))
	default: // RangeString, RangeBytes, plain types
		return key
	}
}

// layoutBlock returns FileLayoutSection entries for one block.
func (r *Reader) layoutBlock(blockIdx int, meta shared.BlockMeta) ([]FileLayoutSection, error) {
	raw, err := r.ReadBlockRaw(blockIdx)
	if err != nil {
		return nil, fmt.Errorf("ReadBlockRaw: %w", err)
	}

	hdr, err := parseBlockHeader(raw)
	if err != nil {
		return nil, fmt.Errorf("parseBlockHeader: %w", err)
	}

	metas, colMetaEndPos, err := parseColumnMetadataArray(raw, 24, int(hdr.columnCount))
	if err != nil {
		return nil, fmt.Errorf("parseColumnMetadataArray: %w", err)
	}

	prefix := fmt.Sprintf("block[%d]", blockIdx)
	base := int64(meta.Offset) //nolint:gosec
	sections := make([]FileLayoutSection, 0, 3+len(metas)*2)

	// Block header: always 24 bytes.
	sections = append(sections, FileLayoutSection{
		Section:        prefix + ".header",
		Offset:         base,
		CompressedSize: 24,
		BlockIndex:     blockIdx,
	})

	// Column metadata array: bytes [24, colMetaEndPos).
	if colMetaSize := int64(colMetaEndPos - 24); colMetaSize > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        prefix + ".column_metadata",
			Offset:         base + 24,
			CompressedSize: colMetaSize,
			BlockIndex:     blockIdx,
		})
	}

	// Per-column data.
	for _, m := range metas {
		colType := columnTypeName(m.colType)

		if m.compressedLen > 0 {
			start := int(m.dataOffset) //nolint:gosec
			var encKind string
			if start+1 < len(raw) {
				encKind = encodingKindName(raw[start+1])
			}

			sections = append(sections, FileLayoutSection{
				Section:        prefix + ".column[" + m.name + "].data",
				ColumnName:     m.name,
				ColumnType:     colType,
				Encoding:       encKind,
				Offset:         base + int64(m.dataOffset), //nolint:gosec
				CompressedSize: int64(m.compressedLen),     //nolint:gosec
				BlockIndex:     blockIdx,
			})
		}
	}

	return sections, nil
}

// layoutMetadata returns FileLayoutSection entries for the metadata section.
//
// All V12 metadata is a single snappy-compressed blob. The physical section
// (metadata.compressed) accounts for r.metadataLen compressed bytes, preserving
// the byte invariant. Logical sub-sections (IsLogical: true) describe the
// decompressed content without adding to the physical byte count.
func (r *Reader) layoutMetadata() ([]FileLayoutSection, error) {
	if len(r.metadataBytes) < 8 {
		return nil, fmt.Errorf("metadataBytes too short: need at least 8 bytes, have %d", len(r.metadataBytes))
	}

	base := int64(r.metadataOffset) //nolint:gosec

	sections := make([]FileLayoutSection, 0, 1+len(r.rangeOffsets))
	sections = append(sections, FileLayoutSection{
		Section:          "metadata.compressed",
		Offset:           base,
		CompressedSize:   int64(r.metadataLen),        //nolint:gosec
		UncompressedSize: int64(len(r.metadataBytes)), //nolint:gosec
	})

	// Logical sub-sections within the decompressed metadata buffer.
	sections = append(sections, r.layoutMetadataRangeIndex()...)

	return sections, nil
}

// layoutMetadataRangeIndex returns logical FileLayoutSection entries for each
// range-indexed column within the decompressed metadata buffer.
func (r *Reader) layoutMetadataRangeIndex() []FileLayoutSection {
	_ = r.ensureV14RangeSection()
	if len(r.rangeOffsets) == 0 {
		return nil
	}
	sections := make([]FileLayoutSection, 0, len(r.rangeOffsets))
	for colName, dMeta := range r.rangeOffsets {
		sections = append(sections, FileLayoutSection{
			Section:        "metadata.range_index.column[" + colName + "]",
			ColumnName:     colName,
			ColumnType:     columnTypeName(dMeta.typ),
			Offset:         int64(dMeta.offset), //nolint:gosec
			CompressedSize: int64(dMeta.length), //nolint:gosec
			IsLogical:      true,
		})
	}
	return sections
}

// buildFileBloomInfo builds a FileBloomInfo summary from the reader's raw file-bloom data.
// Returns nil when no FileBloom section is present.
//
// This re-walks the raw FBLM wire format to extract per-column fuse filter byte sizes,
// which are not retained by parseFileBloomSection (it only keeps the unmarshalled filters).
// The raw bytes have already been validated by parseFileBloomSection during file open, so
// magic/version checks here are defensive guards, not primary validation.
func (r *Reader) buildFileBloomInfo() *FileBloomInfo {
	_ = r.ensureV14BloomSection()
	raw := r.fileBloomRaw
	if len(raw) == 0 {
		return nil
	}

	info := &FileBloomInfo{
		TotalBytes: len(raw),
	}

	// Wire: magic[4] + version[1] + col_count[4] = 9 bytes header.
	if len(raw) < fileBloomMinLen {
		return info
	}
	magic := binary.LittleEndian.Uint32(raw[0:])
	if magic != shared.FileBloomMagic {
		return info
	}
	if raw[4] != shared.FileBloomVersion {
		return info
	}
	colCount := int(binary.LittleEndian.Uint32(raw[5:]))
	pos := shared.CompactIndexHeaderSize
	for range colCount {
		if pos+2 > len(raw) {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(raw[pos:]))
		pos += 2
		if pos+nameLen > len(raw) {
			break
		}
		name := string(raw[pos : pos+nameLen])
		pos += nameLen
		if pos+4 > len(raw) {
			break
		}
		fuseLen := int(binary.LittleEndian.Uint32(raw[pos:]))
		pos += 4
		if pos+fuseLen > len(raw) {
			break
		}
		pos += fuseLen
		info.Columns = append(info.Columns, FileBloomColumnInfo{
			ColumnName: name,
			FuseBytes:  fuseLen,
		})
	}

	slices.SortFunc(info.Columns, func(a, b FileBloomColumnInfo) int {
		return cmp.Compare(a.ColumnName, b.ColumnName)
	})

	return info
}

// formatIntrinsicBound decodes an encoded intrinsic column boundary to a human-readable string.
// For ColumnTypeUint64 (span:duration, span:start) the bound is an 8-byte LE uint64.
// For ColumnTypeInt64 the bound is an 8-byte LE int64.
// For string/bytes types the bound is the raw string.
func formatIntrinsicBound(colType shared.ColumnType, bound string) string {
	if len(bound) == 0 {
		return ""
	}
	switch colType {
	case shared.ColumnTypeUint64:
		if len(bound) >= 8 {
			v := binary.LittleEndian.Uint64([]byte(bound))
			return fmt.Sprintf("%d", v)
		}
	case shared.ColumnTypeInt64:
		if len(bound) >= 8 {
			v := int64(binary.LittleEndian.Uint64([]byte(bound))) //nolint:gosec
			return fmt.Sprintf("%d", v)
		}
	}
	return bound
}

// columnTypeNames maps ColumnType values to their string names for layout reporting.
var columnTypeNames = map[shared.ColumnType]string{ //nolint:gochecknoglobals
	shared.ColumnTypeString:        "String",
	shared.ColumnTypeInt64:         "Int64",
	shared.ColumnTypeUint64:        "Uint64",
	shared.ColumnTypeFloat64:       "Float64",
	shared.ColumnTypeBool:          "Bool",
	shared.ColumnTypeBytes:         "Bytes",
	shared.ColumnTypeRangeInt64:    "RangeInt64",
	shared.ColumnTypeRangeUint64:   "RangeUint64",
	shared.ColumnTypeRangeDuration: "RangeDuration",
	shared.ColumnTypeRangeFloat64:  "RangeFloat64",
	shared.ColumnTypeRangeBytes:    "RangeBytes",
	shared.ColumnTypeRangeString:   "RangeString",
	shared.ColumnTypeUUID:          "UUID",
	shared.ColumnTypeVectorF32:     "VectorF32",
}

// columnTypeName maps a ColumnType to its string name for layout reporting.
func columnTypeName(t shared.ColumnType) string {
	if name, ok := columnTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", t)
}

// encodingKindNames maps encoding kind bytes to their string names.
var encodingKindNames = map[uint8]string{ //nolint:gochecknoglobals
	shared.KindDictionary:            "Dictionary",
	shared.KindSparseDictionary:      "SparseDictionary",
	shared.KindInlineBytes:           "InlineBytes",
	shared.KindSparseInlineBytes:     "SparseInlineBytes",
	shared.KindDeltaUint64:           "DeltaUint64",
	shared.KindRLEIndexes:            "RLEIndexes",
	shared.KindSparseRLEIndexes:      "SparseRLEIndexes",
	shared.KindXORBytes:              "XORBytes",
	shared.KindSparseXORBytes:        "SparseXORBytes",
	shared.KindPrefixBytes:           "PrefixBytes",
	shared.KindSparsePrefixBytes:     "SparsePrefixBytes",
	shared.KindDeltaDictionary:       "DeltaDictionary",
	shared.KindSparseDeltaDictionary: "SparseDeltaDictionary",
	shared.KindVectorF32:             "VectorF32",
}

// encodingKindName maps the encoding kind byte (byte 1 of each column data blob) to its name.
func encodingKindName(kind uint8) string {
	if name, ok := encodingKindNames[kind]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", kind)
}

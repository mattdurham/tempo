package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/sketch"
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
	// FuseBytes is the byte size of the BinaryFuse8 filter for this column (0 if absent).
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

// layoutDecoderPool pools zstd decoders for the layout analysis path. A separate pool
// from the hot-path decoder ensures FileLayout() does not interfere with concurrent
// query decoding. Each pooled decoder is configured with concurrency=1 (single-goroutine
// streaming) and is Reset per chunk rather than reallocated, matching the recommendation
// in the zstd library docs for streaming-to-Discard size measurement.
var layoutDecoderPool = &sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic("layout: zstd.NewReader: " + err.Error())
		}
		return dec
	},
}

// FileLayout computes a byte-level layout of the blockpack file, returning a report
// that accounts for every byte. The returned Sections slice is sorted by Offset ascending.
// Invariant: sum(section.CompressedSize where !IsLogical) == FileSize.
// Logical sections (IsLogical=true) describe sub-structure within the decompressed
// metadata buffer; their Offset is relative to that buffer, not the physical file.
func (r *Reader) FileLayout() (*FileLayoutReport, error) {
	const headerSize = int64(22) // magic[4] + version[1] + metadataOffset[8] + metadataLen[8] + signalType[1]
	footerSize := int64(shared.FooterV3Size)
	if r.footerVersion == shared.FooterV4Version {
		footerSize = int64(shared.FooterV4Size)
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

	// Intrinsic section (v4 footer only): per-column blobs + TOC.
	if r.footerVersion == shared.FooterV4Version && r.intrinsicIndexLen > 0 {
		// Emit per-column sections from the TOC.
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

			// Check if this column uses v2 paged format by reading the first byte.
			blob, blobErr := r.GetIntrinsicColumnBlob(name)
			isPaged := blobErr == nil && len(blob) > 0 && blob[0] == shared.IntrinsicPagedVersion

			pagedEmitted := false
			if isPaged && len(blob) >= 5 {
				tocLen := int(binary.LittleEndian.Uint32(blob[1:5]))
				if 5+tocLen <= len(blob) {
					ptoc, tocErr := shared.DecodePageTOC(blob[5 : 5+tocLen])
					if tocErr == nil && len(ptoc.Pages) > 0 {
						// Compute absolute offset of first page blob:
						// column blob offset + 1 (version byte) + 4 (toc_len prefix) + tocLen
						headerLen := int64(1) + 4 + int64(tocLen)            //nolint:gosec
						firstPageAbsOffset := int64(meta.Offset) + headerLen //nolint:gosec

						// Emit a physical section for the per-page TOC header bytes
						// (version + toc_len prefix + toc data) that precede the pages.
						// headerLen is always >= 5 (1 version byte + 4 toc_len bytes).
						sections = append(sections, FileLayoutSection{
							Section:        "intrinsic.column[" + name + "].page_toc",
							ColumnName:     name,
							ColumnType:     columnTypeName(meta.Type),
							Offset:         int64(meta.Offset), //nolint:gosec
							CompressedSize: headerLen,
						})
						end := int64(meta.Offset) + headerLen //nolint:gosec
						if end > columnsEnd {
							columnsEnd = end
						}

						for pageIdx, pm := range ptoc.Pages {
							pageAbsOffset := firstPageAbsOffset + int64(pm.Offset) //nolint:gosec
							pageFmt := fmt.Sprintf("%s/paged", formatName)
							sections = append(sections, FileLayoutSection{
								Section:        fmt.Sprintf("intrinsic.column[%s].page[%d]", name, pageIdx),
								ColumnName:     name,
								ColumnType:     columnTypeName(meta.Type),
								Encoding:       pageFmt,
								Offset:         pageAbsOffset,
								CompressedSize: int64(pm.Length), //nolint:gosec
								RowCount:       int(pm.RowCount), //nolint:gosec
								MinValue:       formatIntrinsicBound(meta.Type, pm.Min),
								MaxValue:       formatIntrinsicBound(meta.Type, pm.Max),
							})
							end := pageAbsOffset + int64(pm.Length) //nolint:gosec
							if end > columnsEnd {
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
				end := int64(meta.Offset) + int64(meta.Length) //nolint:gosec
				if end > columnsEnd {
					columnsEnd = end
				}
			}
		}
		// TOC blob: from end of last column blob to end of intrinsic index region.
		// When no column blobs are present (empty TOC), columnsEnd == 0 so the TOC
		// section starts at intrinsicIndexOffset itself.
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

// buildSketchIndexInfo builds the SketchIndexInfo from the reader's parsed column-major sketch data.
// Returns nil when no sketches are present.
func (r *Reader) buildSketchIndexInfo() *SketchIndexInfo {
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
		fuseBytes   int // bloom bytes (field name kept for JSON compat)
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
		totalBytes += 2               // bloom_size[2]

		for pi, blockIdx := range cd.presentMap {
			topkEntries := len(cd.topkFP[pi])
			topkBytesForBlock := 1 + topkEntries*10 // entry_count[1] + fp[8]+count[2] per entry
			totalBytes += topkEntries * 10           // (entry_count already counted above)
			bloomB := sketch.SketchBloomBytes
			totalBytes += bloomB

			stat := blockColStat{
				name:        name,
				cardinality: uint64(cd.distinct[blockIdx]),
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

		if m.dataLen > 0 {
			start := int(m.dataOffset) //nolint:gosec
			var encKind string
			if start+1 < len(raw) {
				encKind = encodingKindName(raw[start+1])
			}

			end := start + int(m.dataLen) //nolint:gosec
			var uncompSize int64
			if end <= len(raw) {
				uncompSize, err = columnDataUncompressedSize(raw[start:end])
				if err != nil {
					return nil, fmt.Errorf("column %q uncompressed size: %w", m.name, err)
				}
			}

			sections = append(sections, FileLayoutSection{
				Section:          prefix + ".column[" + m.name + "].data",
				ColumnName:       m.name,
				ColumnType:       colType,
				Encoding:         encKind,
				Offset:           base + int64(m.dataOffset), //nolint:gosec
				CompressedSize:   int64(m.dataLen),           //nolint:gosec
				UncompressedSize: uncompSize,
				BlockIndex:       blockIdx,
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
	pos := 9
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

// columnTypeName maps a ColumnType to its string name for layout reporting.
func columnTypeName(t shared.ColumnType) string {
	switch t {
	case shared.ColumnTypeString:
		return "String"
	case shared.ColumnTypeInt64:
		return "Int64"
	case shared.ColumnTypeUint64:
		return "Uint64"
	case shared.ColumnTypeFloat64:
		return "Float64"
	case shared.ColumnTypeBool:
		return "Bool"
	case shared.ColumnTypeBytes:
		return "Bytes"
	case shared.ColumnTypeRangeInt64:
		return "RangeInt64"
	case shared.ColumnTypeRangeUint64:
		return "RangeUint64"
	case shared.ColumnTypeRangeDuration:
		return "RangeDuration"
	case shared.ColumnTypeRangeFloat64:
		return "RangeFloat64"
	case shared.ColumnTypeRangeBytes:
		return "RangeBytes"
	case shared.ColumnTypeRangeString:
		return "RangeString"
	case shared.ColumnTypeUUID:
		return "UUID"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// encodingKindName maps the encoding kind byte (byte 1 of each column data blob) to its name.
func encodingKindName(kind uint8) string {
	switch kind {
	case 1:
		return "Dictionary"
	case 2:
		return "SparseDictionary"
	case 3:
		return "InlineBytes"
	case 4:
		return "SparseInlineBytes"
	case 5:
		return "DeltaUint64"
	case 6:
		return "RLEIndexes"
	case 7:
		return "SparseRLEIndexes"
	case 8:
		return "XORBytes"
	case 9:
		return "SparseXORBytes"
	case 10:
		return "PrefixBytes"
	case 11:
		return "SparsePrefixBytes"
	case 12:
		return "DeltaDictionary"
	case 13:
		return "SparseDeltaDictionary"
	default:
		return fmt.Sprintf("Unknown(%d)", kind)
	}
}

// columnDataUncompressedSize computes the total uncompressed size of a column data blob
// by walking the encoding wire format, finding zstd-compressed chunks, decompressing them,
// and returning the inflated size (all zstd chunks replaced by their decompressed contents).
// For encodings without zstd (InlineBytes), returns the data length as-is.
func columnDataUncompressedSize(data []byte) (int64, error) {
	if len(data) < 2 {
		return int64(len(data)), nil
	}

	kind := data[1]
	body := data[2:] // skip enc_version[1] + kind[1]
	overhead := int64(2)

	var (
		bodySize int64
		err      error
	)

	switch kind {
	case 1, 2: // Dictionary / SparseDictionary
		bodySize, err = inflatedDictKind(body)

	case 3, 4: // InlineBytes / SparseInlineBytes — no zstd compression
		return int64(len(data)), nil

	case 5: // DeltaUint64
		bodySize, err = inflatedDeltaUint64(body)

	case 6, 7: // RLEIndexes / SparseRLEIndexes
		bodySize, err = inflatedRLEIndexes(body)

	case 8, 9: // XORBytes / SparseXORBytes
		bodySize, err = inflatedXORBytes(body)

	case 10, 11: // PrefixBytes / SparsePrefixBytes
		bodySize, err = inflatedPrefixBytes(body)

	case 12, 13: // DeltaDictionary / SparseDeltaDictionary
		bodySize, err = inflatedDeltaDict(body)

	default:
		return int64(len(data)), nil
	}

	if err != nil {
		return 0, err
	}

	return overhead + bodySize, nil
}

// inflatedZstdChunk reads a len[4]+zstd_data[len] chunk at body[pos:], decompresses
// to measure the uncompressed size, and returns (inflated_chunk_size, new_pos, error).
// inflated_chunk_size = 4 (length prefix) + decompressed_size.
func inflatedZstdChunk(body []byte, pos int) (int64, int, error) {
	if pos+4 > len(body) {
		return int64(len(body) - pos), len(body), nil
	}

	cLen := int(binary.LittleEndian.Uint32(body[pos:]))
	pos += 4

	if cLen == 0 {
		// Zero-length chunk: only the 4-byte length prefix is present.
		return 4, pos, nil
	}
	if pos+cLen > len(body) {
		// Truncated chunk: clamp to remaining bytes so new_pos never exceeds len(body).
		return int64(4 + max(len(body)-pos, 0)), len(body), nil
	}

	n, err := zstdStreamedSize(body[pos : pos+cLen])
	if err != nil {
		return 0, pos + cLen, fmt.Errorf("inflatedZstdChunk: %w", err)
	}

	return 4 + n, pos + cLen, nil
}

// zstdStreamedSize decompresses compressed into io.Discard via a pooled decoder and
// returns the number of output bytes without materializing the full payload. The decoder
// is returned to the pool after use, avoiding per-chunk allocation overhead for large
// files with many column chunks.
func zstdStreamedSize(compressed []byte) (int64, error) {
	dec := layoutDecoderPool.Get().(*zstd.Decoder)
	if err := dec.Reset(bytes.NewReader(compressed)); err != nil {
		// Do not return a broken decoder to the pool; close it instead.
		dec.Close()
		return 0, err
	}
	n, err := io.Copy(io.Discard, dec)
	if err == nil {
		layoutDecoderPool.Put(dec)
	} else {
		dec.Close()
	}
	return n, err
}

// skipPresenceRLE skips a prl_len[4]+prl_data segment, returning (bytes_consumed, new_pos).
func skipPresenceRLE(body []byte, pos int) (int64, int) {
	if pos+4 > len(body) {
		return int64(len(body) - pos), len(body)
	}

	prlLen := int(binary.LittleEndian.Uint32(body[pos:]))

	newPos := pos + 4 + prlLen
	if newPos > len(body) {
		// Corrupt/too-large prlLen: clamp to end of body.
		return int64(len(body) - pos), len(body)
	}

	return int64(4 + prlLen), newPos
}

// inflatedDictKind computes inflated size for encodings whose layout starts with a shared
// dict chunk (kinds 1, 2, 6, 7: Dictionary, SparseDictionary, RLEIndexes, SparseRLEIndexes).
// body starts after enc_version+kind.
// Wire: index_width[1] + dict_len[4]+zstd(dict) + rest...
func inflatedDictKind(body []byte) (int64, error) {
	if len(body) < 1 {
		return int64(len(body)), nil
	}

	total := int64(1) // index_width
	pos := 1

	// zstd dict chunk
	chunkSize, newPos, err := inflatedZstdChunk(body, pos)
	if err != nil {
		return 0, err
	}

	total += chunkSize
	// Remaining bytes (row_count, presence RLE, indexes) are not zstd-compressed.
	total += int64(len(body) - newPos)

	return total, nil
}

// inflatedRLEIndexes computes inflated size for RLEIndexes/SparseRLEIndexes kinds.
// Same structure as dictKind: index_width[1] + dict_len[4]+zstd + rest...
func inflatedRLEIndexes(body []byte) (int64, error) {
	return inflatedDictKind(body)
}

// inflatedDeltaUint64 computes inflated size for DeltaUint64 kind.
// Wire: span_count[4] + prl_len[4]+prl + base[8]+width[1] + [if width>0: len[4]+zstd]
func inflatedDeltaUint64(body []byte) (int64, error) {
	if len(body) < 4 {
		return int64(len(body)), nil
	}

	total := int64(4) // span_count
	pos := 4

	// Skip presence RLE
	prlSize, newPos := skipPresenceRLE(body, pos)
	total += prlSize
	pos = newPos

	// base[8] + width[1]
	if pos+9 > len(body) {
		total += int64(len(body) - pos)
		return total, nil
	}

	total += 9
	width := body[pos+8]
	pos += 9

	if width > 0 {
		chunkSize, newPos, err := inflatedZstdChunk(body, pos)
		if err != nil {
			return 0, err
		}

		total += chunkSize
		total += int64(len(body) - newPos)
	}

	return total, nil
}

// inflatedXORBytes computes inflated size for XORBytes/SparseXORBytes kinds.
// Wire: span_count[4] + prl_len[4]+prl + xor_len[4]+zstd(xor)
func inflatedXORBytes(body []byte) (int64, error) {
	if len(body) < 4 {
		return int64(len(body)), nil
	}

	total := int64(4) // span_count
	pos := 4

	prlSize, newPos := skipPresenceRLE(body, pos)
	total += prlSize
	pos = newPos

	chunkSize, newPos, err := inflatedZstdChunk(body, pos)
	if err != nil {
		return 0, err
	}

	total += chunkSize
	total += int64(len(body) - newPos)

	return total, nil
}

// inflatedPrefixBytes computes inflated size for PrefixBytes/SparsePrefixBytes kinds.
// Wire: span_count[4] + prl_len[4]+prl + prefix_dict_len[4]+zstd1 + suffix_data_len[4]+zstd2
func inflatedPrefixBytes(body []byte) (int64, error) {
	if len(body) < 4 {
		return int64(len(body)), nil
	}

	total := int64(4) // span_count
	pos := 4

	prlSize, newPos := skipPresenceRLE(body, pos)
	total += prlSize
	pos = newPos

	// First zstd chunk: prefix dict
	chunk1Size, newPos, err := inflatedZstdChunk(body, pos)
	if err != nil {
		return 0, err
	}

	total += chunk1Size
	pos = newPos

	// Second zstd chunk: suffix data
	chunk2Size, newPos, err := inflatedZstdChunk(body, pos)
	if err != nil {
		return 0, err
	}

	total += chunk2Size
	total += int64(len(body) - newPos)

	return total, nil
}

// inflatedDeltaDict computes inflated size for DeltaDictionary/SparseDeltaDictionary kinds.
// Wire: index_width[1] + dict_len[4]+zstd1 + row_count[4] + prl_len[4]+prl + delta_len[4]+zstd2
func inflatedDeltaDict(body []byte) (int64, error) {
	if len(body) < 1 {
		return int64(len(body)), nil
	}

	total := int64(1) // index_width
	pos := 1

	// First zstd chunk: dict
	chunk1Size, newPos, err := inflatedZstdChunk(body, pos)
	if err != nil {
		return 0, err
	}

	total += chunk1Size
	pos = newPos

	// row_count[4]
	if pos+4 > len(body) {
		total += int64(len(body) - pos)
		return total, nil
	}

	total += 4
	pos += 4

	// presence RLE
	prlSize, newPos := skipPresenceRLE(body, pos)
	total += prlSize
	pos = newPos

	// Second zstd chunk: deltas
	chunk2Size, newPos, err := inflatedZstdChunk(body, pos)
	if err != nil {
		return 0, err
	}

	total += chunk2Size
	total += int64(len(body) - newPos)

	return total, nil
}

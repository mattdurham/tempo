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

// FileBloom summarizes the file-level bloom filter section, if present.

// SketchIndexInfo summarizes the sketch index stored in the file.

// Blocks holds one summary per block (parallel to FileLayoutReport.BlockSpanCounts).

// TotalBytes is the actual computed uncompressed size of the sketch section.

// HeaderBytes is the fixed 12-byte sketch section header (magic + num_blocks + num_columns).

// SketchedBlockCount is the number of blocks that have at least one sketched column.

// BlockSketchSummary holds per-column sketch statistics for one block.

// Columns holds sketch stats for each column that has sketch data in this block.

// ColumnSketchStat holds sketch statistics for one column in one block.

// HLLCardinality is the estimated number of distinct values (HyperLogLog).

// FuseBytes is the byte size of the membership filter (SketchBloom for SKTE/SKTD, absent for legacy SKTC).

// TopKCount is the number of TopK entries for this column (0 if none).

// TopKBytes is the actual byte size of the TopK entries for this column in this
// block (1 + len(entries) × 10 bytes).

// RangeIndexColumn describes the pruning index for one column.

// BucketMin is the global minimum value across all blocks for this column.

// BucketMax is the global maximum value across all blocks for this column.

// RangeIndexBucket is one entry in a column's range index: the lower boundary
// of a value bucket and the set of block indexes that cover it.

// End is the upper boundary of this bucket (exclusive). For the last bucket this
// equals BucketMax of the column. Empty string for string/bytes columns where the
// upper bound is not encoded.

// FileLayoutSection describes one contiguous byte range in a blockpack file.

// MinValue is the minimum value of this page (human-readable string).

// MaxValue is the maximum value of this page (human-readable string).

// RowCount is the number of records in this page (intrinsic paged columns only).

// IsLogical is true for V12 metadata sub-sections whose Offset is relative to
// the start of the decompressed metadata buffer, not a physical file offset.

// FileBloomInfo summarizes the file-level bloom filter section (FBLM).

// Columns holds per-column name and filter size.

// TotalBytes is the total uncompressed byte size of the FBLM section.

// FileBloomColumnInfo describes one column's entry in the file bloom section.

// FuseBytes is the byte size of the BinaryFuse8 filter for this column.

// FileLayout computes a byte-level layout of the blockpack file, returning a report
// that accounts for every byte. The returned Sections slice is sorted by Offset ascending.
// Invariant: sum(section.CompressedSize where !IsLogical) == FileSize.
// Logical sections (IsLogical=true) describe sub-structure within the decompressed
// metadata buffer; their Offset is relative to that buffer, not the physical file.
func (r *Reader) FileLayout() (*FileLayoutReport, error) {
	return r.fileLayoutV8()
}

// fileLayoutV8 builds the FileLayoutReport for V8 (unified ToC) files.
func (r *Reader) fileLayoutV8() (*FileLayoutReport, error) {
	sections := make([]FileLayoutSection, 0, 16+len(r.tocMap)+len(r.blockMetas)*3)

	// Footer: last 18 bytes.
	footerSize := int64(shared.FooterV8Size) //nolint:gosec
	sections = append(sections, FileLayoutSection{
		Section:        "footer",
		Offset:         r.fileSize - footerSize,
		CompressedSize: footerSize,
	})

	// ToC blob.
	if r.v8ToCLen > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        "toc",
			Offset:         int64(r.v8ToCOffset), //nolint:gosec
			CompressedSize: int64(r.v8ToCLen),    //nolint:gosec
		})
	}

	// ToC entries: each is a snappy-compressed blob (or pre-compressed for intrinsic).
	for key, e := range r.tocMap {
		var sectionName string
		var colType string
		switch {
		case key.Type == shared.ToCTypeIndex && key.SubType == shared.ToCSubTypeBlockIndex:
			sectionName = "section.block_index"
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeRange:
			sectionName = "section.range_index[" + key.Name + "]"
			if ct, ok := r.RangeColumnType(key.Name); ok {
				colType = columnTypeName(ct)
			}
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeSketch:
			sectionName = "section.sketch_index[" + key.Name + "]"
			// Don't set colType for sketch blobs — column type not available without parsing.
			sections = append(sections, FileLayoutSection{
				Section:        sectionName,
				Offset:         int64(e.Offset), //nolint:gosec
				CompressedSize: int64(e.Length), //nolint:gosec
			})
			continue
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeTrace:
			sectionName = "section.trace_index"
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeTS:
			sectionName = "section.ts_index"
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeBloom:
			sectionName = "section.file_bloom"
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeIntrinsic:
			sectionName = "intrinsic.column[" + key.Name + "]"
		default:
			sectionName = fmt.Sprintf("section.type%d.subtype%d[%s]", key.Type, key.SubType, key.Name)
		}
		sections = append(sections, FileLayoutSection{
			Section:        sectionName,
			ColumnName:     key.Name,
			ColumnType:     colType,
			Offset:         int64(e.Offset), //nolint:gosec
			CompressedSize: int64(e.Length), //nolint:gosec
		})
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

func (r *Reader) layoutBlockV14(blockIdx int, meta shared.BlockMeta) ([]FileLayoutSection, error) {
	raw, err := r.ReadBlockRaw(blockIdx)
	if err != nil {
		return nil, fmt.Errorf("ReadBlockRaw: %w", err)
	}

	hdr, err := parseBlockHeader(raw)
	if err != nil {
		return nil, fmt.Errorf("parseBlockHeader: %w", err)
	}

	metas, colMetaEndPos, err := parseColumnMetadataArray(raw, 24, int(hdr.columnCount), hdr.version)
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
	// Collect range column names from the V8 ToC.
	var rangeNames []string
	for key := range r.tocMap {
		if key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeRange && key.Name != "" {
			rangeNames = append(rangeNames, key.Name)
		}
	}
	if len(rangeNames) == 0 {
		return nil
	}

	cols := make([]RangeIndexColumn, 0, len(rangeNames))

	for _, colName := range rangeNames {
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

	// AllPresent encoding kinds (NOTE-AP-001).
	shared.KindDictionaryAllPresent:      "DictionaryAllPresent",
	shared.KindInlineBytesAllPresent:     "InlineBytesAllPresent",
	shared.KindDeltaUint64AllPresent:     "DeltaUint64AllPresent",
	shared.KindRLEIndexesAllPresent:      "RLEIndexesAllPresent",
	shared.KindXORBytesAllPresent:        "XORBytesAllPresent",
	shared.KindPrefixBytesAllPresent:     "PrefixBytesAllPresent",
	shared.KindDeltaDictionaryAllPresent: "DeltaDictionaryAllPresent",

	// Bit-packed DeltaUint64 kinds (NOTE-215).
	shared.KindDeltaUint64BitPacked:           "DeltaUint64BitPacked",
	shared.KindDeltaUint64BitPackedAllPresent: "DeltaUint64BitPackedAllPresent",

	// Uniform-length byte-column kinds (NOTE-217).
	shared.KindXORBytesUniform:           "XORBytesUniform",
	shared.KindSparseXORBytesUniform:     "SparseXORBytesUniform",
	shared.KindInlineBytesUniform:        "InlineBytesUniform",
	shared.KindSparseInlineBytesUniform:  "SparseInlineBytesUniform",
	shared.KindXORBytesUniformAllPresent: "XORBytesUniformAllPresent",

	// Per-page DeltaUint64 kind (NOTE-218).
	shared.KindDeltaUint64Paged: "DeltaUint64Paged",

	// Gorilla-XOR Float64 kinds (NOTE-219).
	shared.KindGorillaFloat64:           "GorillaFloat64",
	shared.KindGorillaFloat64AllPresent: "GorillaFloat64AllPresent",
}

// encodingKindName maps the encoding kind byte (byte 1 of each column data blob) to its name.
func encodingKindName(kind uint8) string {
	if name, ok := encodingKindNames[kind]; ok {
		return name
	}
	return fmt.Sprintf("Unknown(%d)", kind)
}

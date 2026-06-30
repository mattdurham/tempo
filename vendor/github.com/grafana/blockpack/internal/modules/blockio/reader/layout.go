package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/klauspost/compress/snappy"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// FileLayoutReport is the top-level result of AnalyzeFileLayout.

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

// FileLayoutSection describes one contiguous byte range in a blockpack file.

// MinValue is the minimum value of this page (human-readable string).

// MaxValue is the maximum value of this page (human-readable string).

// RowCount is the number of records in this page (intrinsic paged columns only).

// IsLogical is true for V12 metadata sub-sections whose Offset is relative to
// the start of the decompressed metadata buffer, not a physical file offset.

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
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeValueIndexEntries:
			sectionName = "section.valueindex.entries"
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeValueIndexMeta:
			sectionName = "section.valueindex.meta"
		case key.Type == shared.ToCTypeMetadata && key.SubType == shared.ToCSubTypeValueIndexHashIndex:
			sectionName = "section.valueindex.hashindex"
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

	var sketchIndex *SketchIndexInfo

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
		SketchIndex:     sketchIndex,
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

	// Trailing page padding (NOTE-V2-003, issue #419): the v2 writer pads each
	// inner block's bytes to the next 4 KB boundary so block file offsets are
	// page-aligned. BlockMeta.Length is the UNPADDED payload length, so the bytes
	// in [Offset+Length, nextPageBoundary) are zero padding. Report them as their
	// own physical section so the file-layout byte invariant (sum of physical
	// sections == FileSize) continues to hold. v1 (unpadded) files produce zero
	// padding here and add no section.
	const page = shared.BlockFileRefPageSize
	payloadEnd := meta.Offset + meta.Length
	paddedEnd := ((payloadEnd + page - 1) / page) * page
	if pad := int64(paddedEnd - payloadEnd); pad > 0 { //nolint:gosec
		sections = append(sections, FileLayoutSection{
			Section:        prefix + ".padding",
			Offset:         int64(payloadEnd), //nolint:gosec
			CompressedSize: pad,
			BlockIndex:     blockIdx,
		})
	}

	return sections, nil
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

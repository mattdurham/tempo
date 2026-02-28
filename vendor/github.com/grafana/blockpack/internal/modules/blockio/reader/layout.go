package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// FileLayoutReport is the top-level result of AnalyzeFileLayout.
type FileLayoutReport struct {
	Sections    []FileLayoutSection `json:"sections"`
	FileSize    int64               `json:"file_size"`
	BlockCount  int                 `json:"block_count"`
	FileVersion uint8               `json:"file_version"`
}

// FileLayoutSection describes one contiguous byte range in a blockpack file.
type FileLayoutSection struct {
	Section          string `json:"section"`
	ColumnName       string `json:"column_name,omitempty"`
	ColumnType       string `json:"column_type,omitempty"`
	Encoding         string `json:"encoding,omitempty"`
	Offset           int64  `json:"offset"`
	CompressedSize   int64  `json:"compressed_size"`
	UncompressedSize int64  `json:"uncompressed_size,omitempty"`
	BlockIndex       int    `json:"block_index,omitempty"`
}

// FileLayout computes a byte-level layout of the blockpack file, returning a report
// that accounts for every byte. The returned Sections slice is sorted by Offset ascending.
// Invariant: sum(section.CompressedSize) == FileSize.
func (r *Reader) FileLayout() (*FileLayoutReport, error) {
	const (
		footerSize = int64(shared.FooterV3Size) // 22
		headerSize = int64(21)
	)

	var sections []FileLayoutSection

	// Footer: last 22 bytes.
	sections = append(sections, FileLayoutSection{
		Section:        "footer",
		Offset:         r.fileSize - footerSize,
		CompressedSize: footerSize,
	})

	// File header: 21 bytes at r.headerOffset.
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

	sort.Slice(sections, func(i, j int) bool {
		return sections[i].Offset < sections[j].Offset
	})

	return &FileLayoutReport{
		FileSize:    r.fileSize,
		FileVersion: r.fileVersion,
		BlockCount:  len(r.blockMetas),
		Sections:    sections,
	}, nil
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

			sections = append(sections, FileLayoutSection{
				Section:        prefix + ".column[" + m.name + "].data",
				ColumnName:     m.name,
				ColumnType:     colType,
				Encoding:       encKind,
				Offset:         base + int64(m.dataOffset), //nolint:gosec
				CompressedSize: int64(m.dataLen),           //nolint:gosec
				BlockIndex:     blockIdx,
			})
		}
	}

	return sections, nil
}

// layoutMetadata returns FileLayoutSection entries for the metadata section.
// The metadata.block_index section includes the 4-byte range_count prefix so that
// every byte in r.metadataBytes is accounted for across the returned sections.
func (r *Reader) layoutMetadata() ([]FileLayoutSection, error) {
	data := r.metadataBytes
	if len(data) < 8 {
		return nil, fmt.Errorf("metadataBytes too short: need at least 8 bytes, have %d", len(data))
	}

	base := int64(r.metadataOffset) //nolint:gosec
	blockCount := len(r.blockMetas)
	pos := 4 // skip block_count[4]

	_, blockConsumed, err := parseBlockIndex(data[pos:], r.fileVersion, blockCount)
	if err != nil {
		return nil, fmt.Errorf("re-parse block_index: %w", err)
	}

	pos += blockConsumed
	pos += 4 // skip range_count[4]; include it in block_index section

	blockIdxSize := int64(pos)
	var sections []FileLayoutSection

	sections = append(sections, FileLayoutSection{
		Section:        "metadata.block_index",
		Offset:         base,
		CompressedSize: blockIdxSize,
	})

	// Range index: one section per column, using pre-parsed byte ranges.
	// Sum of entry lengths = total bytes consumed by range index entries.
	var dedConsumed int
	for _, dMeta := range r.rangeOffsets {
		dedConsumed += dMeta.length
	}

	for colName, dMeta := range r.rangeOffsets {
		sections = append(sections, FileLayoutSection{
			Section:        "metadata.range_index.column[" + colName + "]",
			ColumnName:     colName,
			ColumnType:     columnTypeName(dMeta.typ),
			Offset:         base + int64(dMeta.offset), //nolint:gosec
			CompressedSize: int64(dMeta.length),        //nolint:gosec
		})
	}

	pos += dedConsumed

	// Column index.
	colIdxStart := pos

	colConsumed, err := skipColumnIndex(data[pos:], blockCount)
	if err != nil {
		return nil, fmt.Errorf("skip column_index: %w", err)
	}

	pos += colConsumed

	if colIdxSize := int64(pos - colIdxStart); colIdxSize > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        "metadata.column_index",
			Offset:         base + int64(colIdxStart),
			CompressedSize: colIdxSize,
		})
	}

	// Trace index: remaining bytes.
	if traceIdxSize := int64(len(data) - pos); traceIdxSize > 0 {
		sections = append(sections, FileLayoutSection{
			Section:        "metadata.trace_index",
			Offset:         base + int64(pos),
			CompressedSize: traceIdxSize,
		})
	}

	return sections, nil
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

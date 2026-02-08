package blockio

import "bytes"

// FileFooter mirrors the on-disk footer layout with exported fields.
type FileFooter struct {
	Magic              uint32
	Version            uint8
	MetadataOffset     uint64
	MetadataLen        uint64
	MetadataCRC        uint32
	MetricStreamOffset uint64 // offset to metric stream blocks section
	MetricStreamLen    uint64 // length of metric stream blocks section
}

// BlockIndexEntry mirrors the on-disk block index entry with exported fields.
type BlockIndexEntry struct {
	Offset          uint64
	Length          uint64
	Kind            uint8
	SpanCount       uint32
	MinStart        uint64
	MaxStart        uint64
	MinTraceID      [16]byte
	MaxTraceID      [16]byte
	ColumnNameBloom ColumnNameBloom
	ColumnIndex     []ColumnIndexEntry        // Per-column offsets for selective I/O
	ValueStats      map[string]AttributeStats // Per-attribute value statistics
}

// MagicNumber is the file magic value for blockpack data.
const MagicNumber = magicNumber

// VersionV10 is the legacy blockpack format version.
const VersionV10 = versionV10

// VersionV11 is the current blockpack format version.
const VersionV11 = versionV11

// FooterSize is the fixed byte size of the file footer.
const FooterSize = footerSize

// ColumnNameBloomBits is the bit size for the column-name bloom filter.
const ColumnNameBloomBits = columnNameBloomBits

// Encoding kinds exposed for tests and diagnostics.
const (
	EncodingKindDictionary            = encodingKindDictionary
	EncodingKindSparseDictionary      = encodingKindSparseDictionary
	EncodingKindInlineBytes           = encodingKindInlineBytes
	EncodingKindSparseInlineBytes     = encodingKindSparseInlineBytes
	EncodingKindDeltaUint64           = encodingKindDeltaUint64
	EncodingKindRLEIndexes            = encodingKindRLEIndexes
	EncodingKindSparseRLEIndexes      = encodingKindSparseRLEIndexes
	EncodingKindXORBytes              = encodingKindXORBytes
	EncodingKindSparseXORBytes        = encodingKindSparseXORBytes
	EncodingKindPrefixBytes           = encodingKindPrefixBytes
	EncodingKindSparsePrefixBytes     = encodingKindSparsePrefixBytes
	EncodingKindDeltaDictionary       = encodingKindDeltaDictionary
	EncodingKindSparseDeltaDictionary = encodingKindSparseDeltaDictionary
)

// ReadFooter parses a file footer from raw bytes.
func ReadFooter(data []byte) (FileFooter, error) {
	footer, err := readFooter(data)
	if err != nil {
		return FileFooter{}, err
	}
	return FileFooter{
		Magic:              footer.magic,
		Version:            footer.version,
		MetadataOffset:     footer.metadataOffset,
		MetadataLen:        footer.metadataLen,
		MetadataCRC:        footer.metadataCRC,
		MetricStreamOffset: footer.metricStreamOffset,
		MetricStreamLen:    footer.metricStreamLen,
	}, nil
}

// BlockIndexSerializedSize returns the encoded size of a block index entry.
func BlockIndexSerializedSize() int {
	return blockIndexSerializedSize()
}

// ReadBlockIndexEntry parses a block index entry from a reader.
// version parameter specifies the format version (v10/v11).
func ReadBlockIndexEntry(rd *bytes.Reader, version uint8) (BlockIndexEntry, error) {
	entry, err := readBlockIndexEntry(rd, version)
	if err != nil {
		return BlockIndexEntry{}, err
	}
	return BlockIndexEntry{
		Offset:          entry.Offset,
		Length:          entry.Length,
		Kind:            uint8(entry.Kind),
		SpanCount:       entry.SpanCount,
		MinStart:        entry.MinStart,
		MaxStart:        entry.MaxStart,
		MinTraceID:      entry.MinTraceID,
		MaxTraceID:      entry.MaxTraceID,
		ColumnNameBloom: entry.ColumnNameBloom,
		ColumnIndex:     entry.ColumnIndex,
		ValueStats:      entry.ValueStats,
	}, nil
}

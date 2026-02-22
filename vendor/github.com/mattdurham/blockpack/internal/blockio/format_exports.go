package blockio

import (
	"github.com/mattdurham/blockpack/internal/blockio/reader"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

// FileFooter mirrors the on-disk footer layout with exported fields.
// This is a simplified version for backward compatibility.
type FileFooter struct {
	Magic              uint32
	Version            uint8
	MetadataOffset     uint64
	MetadataLen        uint64
	MetadataCRC        uint32
	MetricStreamOffset uint64 // offset to metric stream blocks section
	MetricStreamLen    uint64 // length of metric stream blocks section
}

// BlockIndexEntry is an alias to shared.BlockIndexEntry for backward compatibility.
// This type is used in the public API surface for format exports.
// The canonical definition is in internal/blockio/shared/types.go.
type BlockIndexEntry = shared.BlockIndexEntry

// MagicNumber is the file magic value for blockpack data.
const MagicNumber = reader.MagicNumber

// VersionV10 is the legacy blockpack format version.
const VersionV10 = reader.VersionV10

// VersionV11 is the current blockpack format version.
const VersionV11 = reader.VersionV11

// FooterSize is the fixed byte size of the file footer.
const FooterSize = reader.FooterSize

// ColumnNameBloomBits is the bit size for the column-name bloom filter.
const ColumnNameBloomBits = reader.ColumnNameBloomBits

// Encoding kinds exposed for tests and diagnostics.
const (
	EncodingKindDictionary            = reader.EncodingKindDictionary
	EncodingKindSparseDictionary      = reader.EncodingKindSparseDictionary
	EncodingKindInlineBytes           = reader.EncodingKindInlineBytes
	EncodingKindSparseInlineBytes     = reader.EncodingKindSparseInlineBytes
	EncodingKindDeltaUint64           = reader.EncodingKindDeltaUint64
	EncodingKindRLEIndexes            = reader.EncodingKindRLEIndexes
	EncodingKindSparseRLEIndexes      = reader.EncodingKindSparseRLEIndexes
	EncodingKindXORBytes              = reader.EncodingKindXORBytes
	EncodingKindSparseXORBytes        = reader.EncodingKindSparseXORBytes
	EncodingKindPrefixBytes           = reader.EncodingKindPrefixBytes
	EncodingKindSparsePrefixBytes     = reader.EncodingKindSparsePrefixBytes
	EncodingKindDeltaDictionary       = reader.EncodingKindDeltaDictionary
	EncodingKindSparseDeltaDictionary = reader.EncodingKindSparseDeltaDictionary
)

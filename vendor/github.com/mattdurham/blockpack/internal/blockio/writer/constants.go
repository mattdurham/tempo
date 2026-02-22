package writer

import "github.com/mattdurham/blockpack/internal/blockio/shared"

const (
	encodingKindDictionary            uint8 = 1  // Dictionary + indexes (default)
	encodingKindSparseDictionary      uint8 = 2  // Dictionary + sparse indexes (>50% nulls)
	encodingKindInlineBytes           uint8 = 3  // Inline values (bytes columns)
	encodingKindSparseInlineBytes     uint8 = 4  // Inline + sparse (bytes + >50% nulls)
	encodingKindDeltaUint64           uint8 = 5  // Delta encoding for uint64 (timestamps, monotonic data)
	encodingKindRLEIndexes            uint8 = 6  // Dictionary + RLE-compressed indexes (low cardinality)
	encodingKindSparseRLEIndexes      uint8 = 7  // Dictionary + RLE-compressed sparse indexes
	encodingKindXORBytes              uint8 = 8  // XOR encoding for bytes (IDs with common patterns)
	encodingKindSparseXORBytes        uint8 = 9  // XOR encoding + sparse (bytes + >50% nulls)
	encodingKindPrefixBytes           uint8 = 10 // Prefix compression for bytes (URLs, paths with common prefixes)
	encodingKindSparsePrefixBytes     uint8 = 11 // Prefix compression + sparse (bytes + >50% nulls)
	encodingKindDeltaDictionary       uint8 = 12 // Dictionary + delta-encoded indexes (trace:id optimization)
	encodingKindSparseDeltaDictionary uint8 = 13 // Dictionary + delta-encoded sparse indexes
)

const (
	magicNumber uint32 = 0xC011FEA1 // "COLLFEA1" shortened to fit uint32
	versionV10  uint8  = 10
	versionV11  uint8  = 11 // Blockpack format version (current)
)

const (
	columnEncodingVersion = uint8(2)
	columnNameBloomBits   = 256
	columnNameBloomBytes  = 32
)

// footerSize is the size of the streaming footer format (version + header_offset = 10 bytes)
const footerSize = 2 + 8 // version(2) + header_offset(8)

const (
	defaultBlockTargetBytes = 512 * 1024
	// DefaultBlockTargetBytes is the default target size for block compression.
	DefaultBlockTargetBytes = defaultBlockTargetBytes
)

// Exported constants
const (
	MagicNumber          = magicNumber
	VersionV10           = versionV10
	VersionV11           = versionV11
	FooterSize           = footerSize
	ColumnNameBloomBits  = columnNameBloomBits
	ColumnNameBloomBytes = columnNameBloomBytes
)

// Encoding kinds exposed for tests and diagnostics
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

// Exported limit constants from shared/limits.go.
const (
	MaxSpans          = shared.MaxSpans
	MaxBlocks         = shared.MaxBlocks
	MaxColumns        = shared.MaxColumns
	MaxDictionarySize = shared.MaxDictionarySize
	MaxStringLen      = shared.MaxStringLen
	MaxBytesLen       = shared.MaxBytesLen
	MaxBlockSize      = shared.MaxBlockSize
	MaxMetadataSize   = shared.MaxMetadataSize
	MaxTraceCount     = shared.MaxTraceCount
	MaxNameLen        = shared.MaxNameLen
	FooterVersion     = shared.FooterVersion
)

// Shared function wrappers (keep original unexported names for minimal code changes).
var (
	setBit   = shared.SetBit
	isBitSet = shared.IsBitSet
	bitsLen  = shared.BitsLen

	EncodePresenceRLE = shared.EncodePresenceRLE
	DecodePresenceRLE = shared.DecodePresenceRLE
)

// Exported validation functions from shared/limits.go.
var (
	ValidateSpanCount      = shared.ValidateSpanCount
	ValidateBlockCount     = shared.ValidateBlockCount
	ValidateColumnCount    = shared.ValidateColumnCount
	ValidateStringLen      = shared.ValidateStringLen
	ValidateBytesLen       = shared.ValidateBytesLen
	ValidateDictionarySize = shared.ValidateDictionarySize
	ValidateBlockSize      = shared.ValidateBlockSize
	ValidateMetadataSize   = shared.ValidateMetadataSize
	ValidateTraceCount     = shared.ValidateTraceCount
	ValidateNameLen        = shared.ValidateNameLen
	ValidateColumnType     = shared.ValidateColumnType
)

// Exported overflow-safe arithmetic from shared/limits.go.
var (
	ErrIntegerOverflow = shared.ErrIntegerOverflow
	SafeAddInt         = shared.SafeAddInt
	SafeAddInt64       = shared.SafeAddInt64
	SafeAddUint64      = shared.SafeAddUint64
)

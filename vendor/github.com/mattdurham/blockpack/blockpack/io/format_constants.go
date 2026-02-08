package ondiskio

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
	presenceRLEVersion    = uint8(1)
	columnNameBloomBits   = 256
	columnNameBloomBytes  = 32
)

const footerSize = 4 + 1 + 8 + 8 + 4 + 8 + 8 // magic(4) + version(1) + metadata offset(8) + metadata len(8) + crc(4) + metric stream offset(8) + metric stream len(8)

const (
	defaultBlockTargetBytes = 512 * 1024
	DefaultBlockTargetBytes = defaultBlockTargetBytes
)

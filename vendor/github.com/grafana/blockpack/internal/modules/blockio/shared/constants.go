package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Format constants.
const (
	MagicNumber       uint32 = 0xC011FEA1
	CompactIndexMagic uint32 = 0xC01DC1DE

	VersionV10 uint8 = 10
	VersionV11 uint8 = 11

	FooterV3Version uint16 = 3
	FooterV3Size    uint   = 22

	ColumnEncodingVersion uint8 = 2
	CompactIndexVersion   uint8 = 1
	TraceIndexFmtVersion  uint8 = 0x01

	ColumnNameBloomBits  = 256
	ColumnNameBloomBytes = 32
)

// Limits per SPECS §1.1
const (
	MaxSpans              = 1_000_000
	MaxBlocks             = 100_000
	MaxColumns            = 10_000
	MaxDictionarySize     = 1_000_000
	MaxStringLen          = 10_485_760
	MaxBytesLen           = 10_485_760
	MaxBlockSize          = 1_073_741_824
	MaxMetadataSize       = 104_857_600
	MaxTraceCount         = 1_000_000
	MaxNameLen            = 1_024
	MaxCompactSectionSize = 52_428_800
)

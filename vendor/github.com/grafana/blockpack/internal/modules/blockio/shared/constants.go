package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Format constants.
const (
	MagicNumber       uint32 = 0xC011FEA1
	CompactIndexMagic uint32 = 0xC01DC1DE

	VersionV10 uint8 = 10
	VersionV11 uint8 = 11
	VersionV12 uint8 = 12 // V12: snappy-compressed metadata section + extended header with signal_type byte at offset 21

	SignalTypeTrace uint8 = 0x01 // file contains OTEL trace spans
	SignalTypeLog   uint8 = 0x02 // file contains OTEL log records

	FooterV3Version uint16 = 3
	FooterV3Size    uint   = 22

	ColumnEncodingVersion uint8 = 2
	CompactIndexVersion   uint8 = 1
	TraceIndexFmtVersion  uint8 = 0x01
	TraceIndexFmtVersion2 uint8 = 0x02 // v2: block IDs only — no per-block span indices

	TSIndexMagic   uint32 = 0xC011FEED // per-file timestamp index section
	TSIndexVersion uint8  = 1

	// CompactIndexVersion2 is the compact trace index version that includes the trace ID bloom filter.
	CompactIndexVersion2 uint8 = 2

	// TraceIDBloomK is the number of hash functions for the trace ID bloom filter.
	// Kirsch-Mitzenmacher double-hashing is used, so only 2 hash computations are needed.
	TraceIDBloomK = 7

	// TraceIDBloomBitsPerTrace is the number of bloom filter bits allocated per trace ID.
	// With k=7, this yields a false-positive rate of ~0.8%.
	TraceIDBloomBitsPerTrace = 10

	// TraceIDBloomMinBytes is the minimum trace ID bloom filter size in bytes.
	TraceIDBloomMinBytes = 128

	// TraceIDBloomMaxBytes is the maximum trace ID bloom filter size in bytes (1 MiB cap).
	TraceIDBloomMaxBytes = 1 << 20
)

// Intrinsic columns section constants.
const (
	FooterV4Version uint16 = 4
	FooterV4Size    uint   = 34 // version[2]+headerOffset[8]+compactOffset[8]+compactLen[4]+intrinsicIndexOffset[8]+intrinsicIndexLen[4]

	IntrinsicFormatVersion uint8 = 0x01 // first byte of each intrinsic column blob
	IntrinsicFormatFlat    uint8 = 0x01 // flat array: delta-encoded uint64 or length-prefixed bytes
	IntrinsicFormatDict    uint8 = 0x02 // dictionary (string or int64 enum columns)

	// IntrinsicPagedVersion is the sentinel byte that identifies a v2 paged column region.
	// When the first byte of a column blob is 0x02 the blob is NOT snappy-compressed as a
	// whole; instead it contains: sentinel[1] + toc_len[4 LE] + toc_blob[toc_len] + page blobs.
	IntrinsicPagedVersion uint8 = 0x02

	// IntrinsicPageSize is the maximum number of rows stored in one page of a v2 column.
	// Columns with more than this many rows are written in paged (v2) format.
	IntrinsicPageSize = 10_000

	// Per-page bloom filter parameters for dict columns.
	IntrinsicPageBloomK           = 7  // number of hash functions (Kirsch-Mitzenmacher)
	IntrinsicPageBloomBitsPerItem = 10 // bits per unique value in the bloom filter
	IntrinsicPageBloomMinBytes    = 16 // minimum bloom filter size in bytes

	// MaxIntrinsicRows is the safety cap on accumulated rows. If total rows across all
	// blocks exceeds this, the intrinsic section is written empty (TOC with 0 columns).
	MaxIntrinsicRows = 10_000_000
)

// Limits per SPECS §1.1
const (
	MaxSpans              = 1_000_000
	MaxBlocks             = 65_535 // uint16 block ID in trace index limits to 0–65534
	MaxColumns            = 10_000
	MaxDictionarySize     = 1_000_000
	MaxStringLen          = 10_485_760
	MaxBytesLen           = 10_485_760
	MaxBlockSize          = 1_073_741_824
	MaxMetadataSize       = 268_435_456 // 256 MiB — raised from 100 MiB to accommodate sketch data at scale
	MaxTraceCount         = 1_000_000
	MaxNameLen            = 1_024
	MaxCompactSectionSize = 52_428_800
)

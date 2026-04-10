package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Format constants.
const (
	MagicNumber       uint32 = 0xC011FEA1
	CompactIndexMagic uint32 = 0xC01DC1DE

	VersionV13 uint8 = 13 // V13: snappy-compressed metadata, signal_type byte, no per-block trace IDs

	// VersionBlockV12 is the block-content format version written into each block's 24-byte
	// header. It omits the 16-byte stats_offset/stats_len stub fields from column metadata
	// entries (saves 16 bytes per column per block).
	VersionBlockV12 uint8 = 12

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

	// FileBloomMagic is the magic number for the file-level bloom filter section.
	// "FBLM" in ASCII.
	FileBloomMagic   uint32 = 0x46424C4D
	FileBloomVersion uint8  = 0x01

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

)

// VectorIndex section constants.
const (
	// VectorIndexMagic is the magic number for the vector index section. "VECI" in ASCII.
	VectorIndexMagic   uint32 = 0x56454349
	VectorIndexVersion uint8  = 0x01

	// FooterV5Version extends V4 with vectorIndexOffset[8] + vectorIndexLen[4] = 12 extra bytes.
	// V5 total: 34 (V4) + 12 = 46 bytes.
	FooterV5Version uint16 = 5
	FooterV5Size    uint   = 46 // version[2]+headerOffset[8]+compactOffset[8]+compactLen[4]+intrinsicOffset[8]+intrinsicLen[4]+vectorOffset[8]+vectorLen[4]

	// FooterV6Version extends V5 with compactTracesOffset[8] + compactTracesLen[4] = 12 extra bytes.
	// V6 total: 46 (V5) + 12 = 58 bytes.
	// When compactTracesLen > 0, the compact section is split into two pieces:
	//   - compact_offset / compact_len:        raw (uncompressed) bloom header + block table (v3 format)
	//   - compactTracesOffset / compactTracesLen: snappy-compressed trace index
	FooterV6Version uint16 = 6
	FooterV6Size    uint   = 58 // version[2]+headerOffset[8]+compactOffset[8]+compactLen[4]+intrinsicOffset[8]+intrinsicLen[4]+vectorOffset[8]+vectorLen[4]+compactTracesOffset[8]+compactTracesLen[4]

	// CompactIndexVersion3 marks the compact header section as v3 (split format).
	// The bloom+block_table are stored raw (uncompressed) in the compact header section.
	// The trace index is stored separately in the compact traces section (snappy-compressed).
	CompactIndexVersion3 uint8 = 3
)

// Well-known vector column names. Double-underscore prefix signals internal/synthetic columns.
// VECTOR_AI queries search __embedding__; VECTOR_ALL queries search __embedding_all__.
const (
	// EmbeddingColumnName is the vector column for VECTOR_AI (custom field config).
	EmbeddingColumnName     = "__embedding__"
	EmbeddingTextColumnName = "__embedding_text__"
	// EmbeddingAllColumnName is the vector column for VECTOR_ALL (auto-assembled all fields).
	EmbeddingAllColumnName     = "__embedding_all__"
	EmbeddingAllTextColumnName = "__embedding_all_text__"
)

// MaxIntrinsicRows is the safety cap on accumulated rows. If total rows across all
// blocks exceeds this, the intrinsic section is written empty (TOC with 0 columns).
// Declared as a var (not const) so tests can temporarily lower it without writing 10M spans.
//
// WARNING: production code must never modify this variable. Only tests may override it,
// and they must restore the original value via defer (see TestMaxIntrinsicRows_OverCap).
var MaxIntrinsicRows = 10_000_000

// Limits per SPECS §1.1
const (
	MaxSpans              = 1_000_000
	MaxBlocks             = 65_535 // uint16 block ID in trace index limits to 0–65534
	MaxColumns            = 10_000
	MaxDictionarySize     = 1_000_000
	MaxStringLen          = 10_485_760
	MaxBytesLen           = 10_485_760
	MaxBlockSize          = 1_073_741_824
	MaxMetadataSize       = 2_147_483_648 // 2 GiB — raised from 256 MiB; production blocks can reach 600+ MB after sketch growth
	MaxTraceCount         = 1_000_000
	MaxNameLen            = 1_024
	MaxCompactSectionSize = 52_428_800
)

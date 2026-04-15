package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Format constants.
const (
	MagicNumber       uint32 = 0xC011FEA1
	CompactIndexMagic uint32 = 0xC01DC1DE

	// VersionBlockV14 is the version byte at offset 4 in the 24-byte V14 block header.
	// It is distinct from FooterV7Version (the file-level footer version).
	VersionBlockV14 uint8 = 14

	// VersionBlockEncV3 is the enc_version byte inside each V14 column blob.
	// V3 columns use raw (uncompressed) internal sub-segments; the outer snappy
	// is applied per-column by the block writer.
	VersionBlockEncV3 uint8 = 3

	// FooterV7Version is the footer format version for the V14 section-directory footer (18 bytes).
	// magic[4]+version[2]+dir_offset[8]+dir_len[4] = 18 bytes.
	// Version 7 was chosen because agentic already uses 5 (46-byte vector footer) and 6 (58-byte compact-traces footer).
	FooterV7Version uint16 = 7

	// FooterV7Size is the total size of the V14 section-directory footer in bytes:
	// magic[4]+version[2]+dir_offset[8]+dir_len[4] = 18 bytes.
	FooterV7Size uint = 18

	// BlockHeaderV14Size is the total size of the V14 block header in bytes:
	// magic[4]+version[1]+reserved[3]+span_count[4]+column_count[4]+reserved2[8] = 24 bytes.
	BlockHeaderV14Size uint = 24

	// Section type constants for the V14 section directory type-keyed entries.
	// Each constant identifies one independently snappy-compressed file-level section.
	// Values 0x07+ are reserved for future type-keyed sections.
	SectionBlockIndex  uint8 = 0x01
	SectionRangeIndex  uint8 = 0x02
	SectionTraceIndex  uint8 = 0x03
	SectionTSIndex     uint8 = 0x04
	SectionSketchIndex uint8 = 0x05
	SectionFileBloom   uint8 = 0x06

	// DirEntryKindType identifies a type-keyed section directory entry (one of the 6 fixed sections).
	// Wire: entry_kind[1]=0x00 + section_type[1] + offset[8] + compressed_len[4] = 14 bytes.
	DirEntryKindType uint8 = 0x00

	// DirEntryKindName identifies a name-keyed section directory entry (one file-level intrinsic column).
	// Wire: entry_kind[1]=0x01 + name_len[2] + name + offset[8] + compressed_len[4] = 15+len(name) bytes.
	DirEntryKindName uint8 = 0x01

	// DirEntryKindSignal identifies a signal-type entry in the section directory.
	// Wire: entry_kind[1]=0x02 + signal_type[1] = 2 bytes total.
	// Exactly one such entry is written per V14 file to identify the signal type.
	DirEntryKindSignal uint8 = 0x02

	// Preserved for backward compatibility with existing reader code during V14 migration.
	// These will be removed once the reader/writer are fully migrated to V14.
	VersionV13      uint8  = 13 // V13: snappy-compressed metadata, signal_type byte
	VersionBlockV12 uint8  = 12 // V12: 24-byte block header, no per-column snappy
	FooterV3Version uint16 = 3
	FooterV3Size    uint   = 22

	// FileHeaderV13Size is the size in bytes of the V13 file header.
	// Wire format: magic[4] + version[1] + metadataOffset[8] + metadataLen[8] + signalType[1] = 22 bytes.
	FileHeaderV13Size uint = 22

	SignalTypeTrace uint8 = 0x01 // file contains OTEL trace spans
	SignalTypeLog   uint8 = 0x02 // file contains OTEL log records

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

	// TraceIDBloomMaxBytes is the maximum trace ID bloom filter size in bytes (6 MiB cap).
	// At k=7 and 6 MiB, FPR stays under 1% for up to ~7.9M traces/block, giving
	// ≤350ms warm FindTraceByID with 59 blocks (expected 0.35 false-positive reads/lookup).
	// Previous cap was 1 MiB, which saturated at ~875K traces (~50% FPR for 2.8M-trace blocks).
	TraceIDBloomMaxBytes = 6 << 20
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
	IntrinsicPageBloomK           = 7    // number of hash functions (Kirsch-Mitzenmacher)
	IntrinsicPageBloomBitsPerItem = 10   // bits per unique value in the bloom filter
	IntrinsicPageBloomMinBytes    = 16   // minimum bloom filter size in bytes
	IntrinsicPageBloomMaxBytes    = 4096 // maximum bloom filter size in bytes

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
)

// Paged-column TOC and compact trace index constants.
const (
	// CompactIndexVersion3 marks the compact header section as v3 (split format).
	// The bloom+block_table are stored raw (uncompressed) in the compact header section.
	// The trace index is stored separately in the compact traces section (snappy-compressed).
	CompactIndexVersion3 uint8 = 3

	// PageTOCVersion is the version field (first byte) inside a snappy-decompressed paged column
	// TOC blob. It is distinct from IntrinsicPagedVersion (0x02), which is the outer type-sentinel
	// that appears at byte 0 of the uncompressed column blob and selects the paged code path.
	// Wire (after snappy decompress): page_toc_version[1]=0x01 + page_count[4] + block_idx_width[1]
	//   + row_idx_width[1] + format[1] + col_type[1] + pages...
	PageTOCVersion uint8 = 0x01

	// CompactIndexHeaderSize is the fixed size in bytes of the compact index section header.
	// Wire: magic[4] + version[1] + block_count[4] = 9 bytes.
	CompactIndexHeaderSize = 9
)

// Encoding kind constants per SPECS §9.
// Canonical definitions — writer/constants.go re-exports these as aliases for backward compatibility.
const (
	KindDictionary       uint8 = 1
	KindSparseDictionary uint8 = 2
	// KindInlineBytes and KindSparseInlineBytes are reader-only: they were emitted by
	// earlier writer versions and must remain decodable. The current writer never selects
	// these kinds — all bytes columns are encoded as KindXORBytes (8) or KindPrefixBytes (10).
	KindInlineBytes           uint8 = 3
	KindSparseInlineBytes     uint8 = 4
	KindDeltaUint64           uint8 = 5
	KindRLEIndexes            uint8 = 6
	KindSparseRLEIndexes      uint8 = 7
	KindXORBytes              uint8 = 8
	KindSparseXORBytes        uint8 = 9
	KindPrefixBytes           uint8 = 10
	KindSparsePrefixBytes     uint8 = 11
	KindDeltaDictionary       uint8 = 12
	KindSparseDeltaDictionary uint8 = 13
	KindVectorF32             uint8 = 14 // flat float32 array, per-row presence RLE, LE byte order
)

// Trace intrinsic column name constants — canonical colon-form names used across writer, reader, and vm packages.
const (
	TraceIDColumnName       = "trace:id"
	SpanIDColumnName        = "span:id"
	SpanParentIDColumnName  = "span:parent_id"
	SpanNameColumnName      = "span:name"
	SpanKindColumnName      = "span:kind"
	SpanStartColumnName     = "span:start"
	SpanEndColumnName       = "span:end"
	SpanDurationColumnName  = "span:duration"
	SpanStatusColumnName    = "span:status"
	SpanStatusMsgColumnName = "span:status_message"
	SvcNameColumnName       = "resource.service.name"
	TraceStateColumnName    = "trace:state"
	ResourceSchemaURL       = "resource:schema_url"
	ScopeSchemaURL          = "scope:schema_url"
)

// Log intrinsic column name constants (SPECS §11 log signal extension).
const (
	LogTimestampColumnName         = "log:timestamp"
	LogObservedTimestampColumnName = "log:observed_timestamp"
	LogBodyColumnName              = "log:body"
	LogSeverityNumberColumnName    = "log:severity_number"
	LogSeverityTextColumnName      = "log:severity_text"
	LogTraceIDColumnName           = "log:trace_id"
	LogSpanIDColumnName            = "log:span_id"
	LogFlagsColumnName             = "log:flags"
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

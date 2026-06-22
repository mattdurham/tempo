package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Format constants.
const (
	MagicNumber       uint32 = 0xC011FEA1
	CompactIndexMagic uint32 = 0xC01DC1DE

	// VersionBlockV14 is the version byte at offset 4 in the 24-byte V14 block header.
	// It is distinct from FooterV7Version (the file-level footer version).
	VersionBlockV14 uint8 = 14

	// VersionBlockV15 is the version byte at offset 4 in the 24-byte block header for
	// blocks that use the V15 column TOC. V15 adds a per-column flags byte (NOTE-220);
	// when ColFlagInline is set the column's raw (un-snappy) blob follows the flags byte
	// inline in the TOC entry instead of being addressed by data_offset/compressed_len.
	// The block header layout is identical to V14 (24 bytes); only the per-column TOC
	// entry layout differs. V14 readers continue working on V14 files unchanged.
	VersionBlockV15 uint8 = 15

	// ColFlagInline is the V15 column TOC flag bit indicating the column's raw blob is
	// stored inline directly after the flags byte (no offset indirection, no outer
	// snappy). Inline payload length is bounded by ColInlineMaxLen.
	ColFlagInline uint8 = 0x01

	// ColFlagZstd is the V15 column TOC flag bit indicating the column's offset-addressed
	// data blob is compressed with zstd instead of the default snappy (NOTE-405, issue #355).
	// It is purely additive: a blob without this bit decodes as snappy exactly as before, so
	// existing V15 files keep working. Mutually exclusive with ColFlagInline (an inline column
	// is stored raw and is never compressed). Only the offset-addressed (non-inline) form can
	// carry this bit. The writer sets it per-column only when zstd beats snappy by a benefit
	// margin (zstdBenefitNum/zstdBenefitDen) so incompressible/bit-packed blobs stay on snappy.
	ColFlagZstd uint8 = 0x02

	// ColInlineMaxLen is the maximum byte length of an inline column payload (NOTE-220).
	// The inline length is encoded in a single byte, so the hard cap is 255; the writer
	// only chooses inline when it is strictly smaller than the non-inline encoding, which
	// is well under this cap for tiny low-cardinality columns.
	ColInlineMaxLen = 255

	// VersionBlockEncV3 is the enc_version byte inside each V14 column blob.
	// V3 columns use raw (uncompressed) internal sub-segments; the outer snappy
	// is applied per-column by the block writer.
	VersionBlockEncV3 uint8 = 3

	// FooterV8Version is the footer format version for V8 files (unified ToC footer).
	// Same 18-byte layout as V7; distinguished by version=8.
	FooterV8Version uint16 = 8

	// FooterV8Size is the total size of the V8 footer in bytes (identical to V7).
	// magic[4]+version[2]+toc_offset[8]+toc_length[4] = 18 bytes.
	FooterV8Size uint = 18

	// ToCEntry Type constants — section class in the V8 unified Table of Contents.
	ToCTypeMetadata uint32 = 1 // file-level metadata sections
	ToCTypeIndex    uint32 = 2 // file-level index structures
	ToCTypeBlock    uint32 = 3 // raw block data blobs (reserved; not used in V8 initial)

	// ToCEntry SubType constants for ToCTypeMetadata (Type=1).
	ToCSubTypeRange     uint32 = 1 // per-column range index blob
	ToCSubTypeSketch    uint32 = 2 // per-column KLL/sketch blob
	ToCSubTypeBloom     uint32 = 3 // file-level bloom filter blob
	ToCSubTypeIntrinsic uint32 = 4 // per-column intrinsic column blob
	ToCSubTypeTrace     uint32 = 5 // compact trace index blob
	ToCSubTypeTS        uint32 = 6 // timestamp index blob

	ToCSubTypeTraceChunked uint32 = 8 // range-readable chunked trace index (SPEC: issue #340)

	ToCSubTypeColStats uint32 = 9 // per-block per-column statistics for predicate pruning (SPEC: issue #364)

	ToCSubTypeSpanTree uint32 = 10 // parent-child span tree index for structural query pruning (SPEC: issue #381)

	// ToCEntry SubType constants for ToCTypeIndex (Type=2).
	ToCSubTypeBlockIndex uint32 = 7 // block offset table

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

	// ColumnEncodingVersion is the enc_version byte used by the VectorF32 encoder.
	// VectorF32 uses enc_version=2 for historical reasons; all other column encodings use VersionBlockEncV3.
	ColumnEncodingVersion uint8 = 2

	// VersionBlockV12 is the minimum block version that uses the compact TOC entry format.
	// Retained as a wire-format threshold constant; the V12 block format is no longer written.
	VersionBlockV12 uint8 = 12

	SignalTypeTrace uint8 = 0x01 // file contains OTEL trace spans (the only signal type)

	TraceIndexFmtVersion  uint8 = 0x01 // v1: block IDs + per-block span indices (legacy wire, still parsed in V8 files)
	TraceIndexFmtVersion2 uint8 = 0x02 // v2: block IDs only — no per-block span indices

	// Chunked trace index (ToCSubTypeTraceChunked) — range-readable trace index (issue #340).
	// The section is NOT snappy-compressed as a whole (written raw, like intrinsic blobs); each
	// chunk is independently snappy-compressed so a lookup range-reads only the relevant chunk
	// instead of fetching + decompressing the entire trace index.
	ChunkedTraceMagic   uint32 = 0xC01DC2DE
	ChunkedTraceVersion uint8  = 0x01
	// ChunkedTraceHeaderSize is the fixed leading-header size in bytes:
	// magic[4]+version[1]+reserved[3]+block_count[4]+trace_count[4]+chunk_count[4]+
	// entry_fmt[1]+reserved2[3]+dir_off[4]+bloom_off[4]+bloom_len[4] = 36 bytes.
	ChunkedTraceHeaderSize = 36
	// ChunkedTraceDirEntrySize is the size in bytes of one chunk-directory entry:
	// first_trace_id[16]+comp_off[4]+comp_len[4] = 24 bytes.
	ChunkedTraceDirEntrySize = 24
	// ChunkedTraceEntriesPerChunk is the number of sorted trace entries packed into one chunk.
	// Keeps each decompressed chunk small (a few tens of KB) so trace-by-id reads one chunk.
	ChunkedTraceEntriesPerChunk = 4096

	// Span tree index (ToCSubTypeSpanTree) — parent-child structural index (issue #381).
	// Like the chunked trace index, the section is written RAW (not snappy-compressed as a
	// whole); each chunk is independently snappy-compressed so a lookup range-reads + decodes
	// only the chunk(s) that can contain a target trace ID. Within a chunk the records are
	// fixed-stride (SpanTreeRecordSize), enabling binary search on traceID without full decode.
	SpanTreeMagic   uint32 = 0xC01DC3DE
	SpanTreeVersion uint8  = 0x01
	// SpanTreeHeaderSize is the fixed leading-header size in bytes:
	// magic[4]+version[1]+reserved[3]+block_count[4]+trace_count[4]+span_count[4]+
	// chunk_count[4]+dir_off[4]+bloom_off[4]+bloom_len[4] = 36 bytes.
	SpanTreeHeaderSize = 36
	// SpanTreeDirEntrySize is the size in bytes of one chunk-directory entry:
	// first_trace_id[16]+comp_off[4]+comp_len[4]+span_count[4] = 28 bytes. The per-chunk
	// span_count lets a reader size its decode buffer and walk the chunk without a sub-header.
	SpanTreeDirEntrySize = 28
	// SpanTreeRecordSize is the fixed stride of one decoded SpanTree record:
	// trace_id[16]+span_id[8]+parent_id[8]+dfs_in[4]+dfs_out[4]+block_idx[2]+row_idx[2] = 44 bytes.
	SpanTreeRecordSize = 44
	// SpanTreeRecordsPerChunk is the number of records packed into one independently-compressed
	// chunk. A chunk boundary never splits a trace's records (a trace's spans are always in one
	// chunk), so the chunk that may exceed this slightly to keep a trace intact; the value keeps
	// each decompressed chunk small (a few hundred KB) for cheap range reads.
	SpanTreeRecordsPerChunk = 8192

	// CompactIndexVersion is the legacy compact index version (no trace ID bloom).
	// Still encountered in V8 trace sections written before CompactIndexVersion2 was introduced.
	CompactIndexVersion uint8 = 1

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
	IntrinsicFormatVersion uint8 = 0x01 // first byte of each intrinsic column blob
	IntrinsicFormatFlat    uint8 = 0x01 // flat array: delta-encoded uint64 or length-prefixed bytes
	IntrinsicFormatDict    uint8 = 0x02 // dictionary (string or int64 enum columns)

	// IntrinsicFormatXORBytes is the format byte for large flat bytes columns.
	// Values are XOR-against-previous encoded; the entire payload is single-snappy-compressed.
	// NOTE-013: this avoids N×IntrinsicPageSize independent snappy calls which inflate
	// random-byte columns (span:id, trace:id) to 2× their uncompressed size.
	IntrinsicFormatXORBytes uint8 = 0x03

	// IntrinsicFormatDeltaUint64 is the format byte for large flat uint64 columns.
	// Values are sorted ascending, delta-encoded with unsigned varints, and single-snappy-compressed.
	// NOTE-014: mirrors the XORBytes single-pass snappy rationale for uint64 span:start/duration columns.
	// Unsigned uvarint (not zigzag) is used because sorted ascending deltas are always non-negative.
	IntrinsicFormatDeltaUint64 uint8 = 0x04

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

	// AllPresent encoding kinds (NOTE-AP-001, SPECS §9.x). Each is wire-identical to its
	// base dense kind EXCEPT the presence_rle_len[4] + presence_rle_data segment is omitted
	// entirely — the kind byte itself signals "every row is present". They are selected by the
	// writer only when presentCount == nRows (a fully-dense column). There are no sparse
	// AllPresent variants (sparse-with-all-present is a contradiction). Old readers reject these
	// unknown kinds at readColumnEncoding; no enc_version bump (additive format evolution,
	// NOTE-007 precedent).
	KindDictionaryAllPresent      uint8 = 15
	KindInlineBytesAllPresent     uint8 = 16
	KindDeltaUint64AllPresent     uint8 = 17
	KindRLEIndexesAllPresent      uint8 = 18
	KindXORBytesAllPresent        uint8 = 19
	KindPrefixBytesAllPresent     uint8 = 20
	KindDeltaDictionaryAllPresent uint8 = 21

	// KindDeltaUint64BitPacked (kind 22, NOTE-215, SPECS §9.4) is a bit-packed variant of
	// KindDeltaUint64. Instead of snapping each offset to a byte width (1/2/4/8 bytes), it
	// stores a single bit_width (0..64) chosen as the minimum number of bits needed to
	// represent the largest offset, then packs every offset into a contiguous LSB-first bit
	// stream. This is selected only when bit packing saves a meaningful fraction of the
	// byte-width payload AND the column has enough present rows to amortize the per-column
	// header (see SPEC-006). Old readers reject this unknown kind at readColumnEncoding (no
	// enc_version bump — additive format evolution, NOTE-007 precedent).
	//
	// KindDeltaUint64BitPackedAllPresent (kind 23) is its fully-present variant that omits
	// the presence_rle segment entirely (NOTE-AP-001).
	KindDeltaUint64BitPacked           uint8 = 22
	KindDeltaUint64BitPackedAllPresent uint8 = 23

	// Uniform-length byte-column kinds (NOTE-217, SPECS §9.3/§9.5). When every present
	// value in an XORBytes or InlineBytes column shares the same byte length (extremely
	// common for ID columns: span:id=8B, trace:id=16B, UUIDs=16B), the per-row len[4]
	// prefix is dropped and the payload becomes a packed fixed-width array. A single
	// uniform_len[4 LE] is written once after the presence segment, followed by
	// nPresent × uniform_len payload bytes. This saves the per-row length prefix
	// (−33% to −50% wire bytes for 8/16-byte IDs) and removes the per-row appendUint32LE
	// from the encode loop and the per-row length read from the decode loop.
	//
	// KindXORBytesUniform (24) is the dense XOR variant; KindSparseXORBytesUniform (25)
	// is its sparse form (present_count[4] follows the presence segment, mirroring the
	// non-uniform sparse layout). KindInlineBytesUniform (26) / KindSparseInlineBytesUniform
	// (27) are the reader-only InlineBytes counterparts (the current writer never selects
	// the InlineBytes family — they remain decodable for forward compatibility and for any
	// future writer or external producer). KindXORBytesUniformAllPresent (28) composes the
	// uniform layout with NOTE-AP-001 (fully-present, presence segment omitted).
	//
	// Old readers reject these unknown kinds at readColumnEncoding (no enc_version bump —
	// additive format evolution, NOTE-007 precedent).
	KindXORBytesUniform          uint8 = 24
	KindSparseXORBytesUniform    uint8 = 25
	KindInlineBytesUniform       uint8 = 26
	KindSparseInlineBytesUniform uint8 = 27

	KindXORBytesUniformAllPresent uint8 = 28

	// KindDeltaUint64Paged (kind 39, NOTE-218, SPECS §9.4.2) is a per-page variant of
	// KindDeltaUint64BitPacked. Instead of one column-wide base + bit_width, the present rows
	// are split into fixed-size pages (deltaPageSize rows each) and each page picks its own
	// base + bit_width. This wins on bursty-then-trickle timestamp distributions where a single
	// column-wide bit_width is inflated by a small number of large offsets — each page packs
	// only the bits its local range needs. There is no sparse or AllPresent variant: the gain is
	// in the per-page width adaptation, not the presence layout.
	//
	// Old readers reject this unknown kind at readColumnEncoding (no enc_version bump —
	// additive format evolution, NOTE-007 precedent).
	KindDeltaUint64Paged uint8 = 39

	// KindGorillaFloat64 (kind 40, NOTE-219, SPECS §9.8) is a Gorilla-XOR encoding for
	// high-cardinality, value-correlated Float64 / RangeFloat64 columns where the Dictionary
	// path (kinds 1/2) provides no actual deduplication and just pays raw-value + index
	// overhead. Each present value is XORed against its predecessor and the meaningful bits of
	// the XOR result are packed LSB-first (Pelkonen et al., VLDB 2015). Low-cardinality float
	// columns deliberately stay on Dictionary+RLE — see the two-population analysis in
	// writer/NOTES.md NOTE-219. KindGorillaFloat64AllPresent (kind 41) is its fully-present
	// variant that omits the presence_rle segment (NOTE-AP-001).
	KindGorillaFloat64           uint8 = 40
	KindGorillaFloat64AllPresent uint8 = 41
)

// AllPresentKindFor maps a base dense encoding kind to its AllPresent variant. Returns
// (variant, true) when an AllPresent form exists, or (kind, false) for kinds with no
// AllPresent variant (sparse kinds, VectorF32, and the already-special inline kinds).
// The writer consults this only when a column is fully present (presentCount == nRows).
func AllPresentKindFor(kind uint8) (uint8, bool) {
	switch kind {
	case KindDictionary:
		return KindDictionaryAllPresent, true
	case KindInlineBytes:
		return KindInlineBytesAllPresent, true
	case KindDeltaUint64:
		return KindDeltaUint64AllPresent, true
	case KindRLEIndexes:
		return KindRLEIndexesAllPresent, true
	case KindXORBytes:
		return KindXORBytesAllPresent, true
	case KindPrefixBytes:
		return KindPrefixBytesAllPresent, true
	case KindDeltaDictionary:
		return KindDeltaDictionaryAllPresent, true
	case KindDeltaUint64BitPacked:
		return KindDeltaUint64BitPackedAllPresent, true
	case KindXORBytesUniform:
		return KindXORBytesUniformAllPresent, true
	case KindGorillaFloat64:
		return KindGorillaFloat64AllPresent, true
	default:
		return kind, false
	}
}

// BaseKindFor maps an AllPresent variant back to its base dense kind, so reader dispatch
// can reuse the existing decoder logic. Returns (base, true) for AllPresent variants, or
// (kind, false) otherwise.
func BaseKindFor(kind uint8) (uint8, bool) {
	switch kind {
	case KindDictionaryAllPresent:
		return KindDictionary, true
	case KindInlineBytesAllPresent:
		return KindInlineBytes, true
	case KindDeltaUint64AllPresent:
		return KindDeltaUint64, true
	case KindRLEIndexesAllPresent:
		return KindRLEIndexes, true
	case KindXORBytesAllPresent:
		return KindXORBytes, true
	case KindPrefixBytesAllPresent:
		return KindPrefixBytes, true
	case KindDeltaDictionaryAllPresent:
		return KindDeltaDictionary, true
	case KindDeltaUint64BitPackedAllPresent:
		return KindDeltaUint64BitPacked, true
	case KindXORBytesUniformAllPresent:
		return KindXORBytesUniform, true
	case KindGorillaFloat64AllPresent:
		return KindGorillaFloat64, true
	default:
		return kind, false
	}
}

// IsAllPresentKind returns true if kind is one of the AllPresent encoding variants whose
// wire format omits the presence_rle segment entirely. Used by both writer and reader to
// branch the presence section.
func IsAllPresentKind(kind uint8) bool {
	switch kind {
	case KindDictionaryAllPresent,
		KindInlineBytesAllPresent,
		KindDeltaUint64AllPresent,
		KindRLEIndexesAllPresent,
		KindXORBytesAllPresent,
		KindPrefixBytesAllPresent,
		KindDeltaDictionaryAllPresent,
		KindDeltaUint64BitPackedAllPresent,
		KindXORBytesUniformAllPresent,
		KindGorillaFloat64AllPresent:
		return true
	default:
		return false
	}
}

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

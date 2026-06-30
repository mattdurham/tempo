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

	// FooterV9Version is the footer format version for V9 (v2 lean-format) files.
	// Same 18-byte wire layout as V8; distinguished by version=9.
	// V9 files omit IntrinsicTOC, SpanTree, KLL sketch blobs, file-level bloom,
	// and the chunked trace index. Inner blocks are 4 096-byte page-aligned.
	// See issue #417 and NOTE-V2-001.
	FooterV9Version uint16 = 9

	// FooterV8Size is the total size of the V8/V9 footer in bytes (identical to V7).
	// magic[4]+version[2]+toc_offset[8]+toc_length[4] = 18 bytes.
	FooterV8Size uint = 18

	// ToCEntry Type constants — section class in the V8 unified Table of Contents.
	ToCTypeMetadata uint32 = 1 // file-level metadata sections
	ToCTypeIndex    uint32 = 2 // file-level index structures
	ToCTypeBlock    uint32 = 3 // raw block data blobs (reserved; not used in V8 initial)

	// ToCEntry SubType constants for ToCTypeMetadata (Type=1).
	// SubType 1 (Range) retired 2026-06-30 (range index removed, #439); not reused.
	// SubType 2 (Sketch) retired 2026-06-30 (KLL/column sketch index removed, #435); not reused.
	ToCSubTypeBloom     uint32 = 3 // file-level bloom filter blob
	ToCSubTypeIntrinsic uint32 = 4 // per-column intrinsic column blob
	ToCSubTypeTrace     uint32 = 5 // compact trace index blob
	ToCSubTypeTS        uint32 = 6 // timestamp index blob

	ToCSubTypeTraceChunked uint32 = 8 // range-readable chunked trace index (SPEC: issue #340)

	// SubType 9 (ColStats) retired 2026-06-29 (in-file block pruning removal); not reused.

	ToCSubTypeSpanTree uint32 = 10 // parent-child span tree index for structural query pruning (SPEC: issue #381)

	// Value index ToC subtypes (SPEC: internal/modules/valueindex/SPECS.md).
	ToCSubTypeValueIndexEntries   uint32 = 13 // value index chunked posting list
	ToCSubTypeValueIndexMeta      uint32 = 14 // value index column identity + compaction level + wall min/max ts
	ToCSubTypeValueIndexHashIndex uint32 = 15 // value index sorted (value_hash → chunk_idx) lookup table

	// ToCSubTypeValueCounts (VCNT) records unique values and signed span counts per column
	// for time-bounded tag-value lookups (SPEC: issue #400, internal/modules/valuecounts).
	// Positive count: value appears in count spans within the time range in this file.
	// Negative count: delta accounting — a prior file's contribution is being subtracted
	// (source block deleted by retention, or superseded by a compacted output).
	ToCSubTypeValueCounts uint32 = 16

	// ToCEntry SubType constants for ToCTypeIndex (Type=2).
	ToCSubTypeBlockIndex uint32 = 7 // block offset table

	// BlockHeaderV14Size is the total size of the V14 block header in bytes:
	// magic[4]+version[1]+reserved[3]+span_count[4]+column_count[4]+reserved2[8] = 24 bytes.
	BlockHeaderV14Size uint = 24

	// Section type constants for the V14 section directory type-keyed entries.
	// Each constant identifies one independently snappy-compressed file-level section.
	// Value 0x05 (formerly SectionSketchIndex) was retired with the KLL/column sketch
	// index removal (#435); Value 0x06 (formerly SectionFileBloom) was retired with the
	// file-level bloom removal (#437); 0x05, 0x06 and 0x07+ are reserved for future
	// type-keyed sections.
	SectionBlockIndex uint8 = 0x01
	SectionRangeIndex uint8 = 0x02
	SectionTraceIndex uint8 = 0x03
	SectionTSIndex    uint8 = 0x04

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

	SignalTypeTrace       uint8 = 0x01 // file contains OTEL trace spans
	SignalTypeLog         uint8 = 0x02 // file contains OTEL log records
	SignalTypeValueIndex  uint8 = 0x03 // file is a value index (internal/modules/valueindex)
	SignalTypeValueCounts uint8 = 0x04 // file is a unique-value count index (internal/modules/valuecounts)

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
	SpanTreeMagic uint32 = 0xC01DC3DE
	// SpanTreeVersion is the current SpanTree section version. v2 (issue #388) appends a
	// per-chunk span-ID bloom region after the chunk directory and adds spanBloomOff[4] +
	// spanBloomStride[4] to the header (bytes 36..44). v1 readers stop at SpanTreeHeaderSizeV1
	// and never see those fields, so v2 files are still readable by the v1 trace-by-ID path;
	// v2 readers detect a v1 file by version and skip the span-bloom region.
	SpanTreeVersion   uint8 = 0x02
	SpanTreeVersionV1 uint8 = 0x01
	// SpanTreeHeaderSizeV1 is the v1 fixed leading-header size in bytes:
	// magic[4]+version[1]+reserved[3]+block_count[4]+trace_count[4]+span_count[4]+
	// chunk_count[4]+dir_off[4]+bloom_off[4]+bloom_len[4] = 36 bytes.
	SpanTreeHeaderSizeV1 = 36
	// SpanTreeHeaderSize is the v2 fixed leading-header size in bytes: the v1 header plus
	// span_bloom_off[4] + span_bloom_stride[4] = 44 bytes. span_bloom_off is the section-
	// relative offset of the per-chunk span-ID bloom region; span_bloom_stride is the fixed
	// byte size of each chunk's span-ID bloom (SpanTreeChunkBloomSize). A zero stride means no
	// span-bloom region was written.
	SpanTreeHeaderSize = 44
	// SpanTreeChunkBloomSize is the fixed byte size of one chunk's span-ID bloom filter. 8 KiB
	// (65536 bits) over ~SpanTreeRecordsPerChunk span IDs gives ≈2% false-positive rate with
	// SpanIDBloomK hashes, so SpanTreeChunksForSpan returns ≈1 real chunk + ≈0 false positives.
	SpanTreeChunkBloomSize = 8 << 10
	// SpanIDBloomK is the number of hash functions for a per-chunk span-ID bloom filter
	// (Kirsch-Mitzenmacher double hashing over the 8 random span-ID bytes).
	SpanIDBloomK = 6
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

// Value index constants (internal/modules/valueindex/SPECS.md).
const (
	// Value index file section magic numbers.
	ValueIndexFileMagic      uint32 = 0x5649444C // "VIDL" — file footer magic
	ValueIndexMetaMagic      uint32 = 0x56494D54 // "VIMT" — VIMT section magic
	ValueIndexHashIndexMagic uint32 = 0x56484958 // "VHIX" — VHIX section magic
	ValueIndexEntriesMagic   uint32 = 0x56494E58 // "VINX" — VINX section magic
	ValueIndexKLLMagic       uint32 = 0x564B4C4C // "VKLL" — KLL section magic

	// Value index format versions.
	ValueIndexFileVersion      uint8 = 0x01
	ValueIndexMetaVersion      uint8 = 0x01
	ValueIndexHashIndexVersion uint8 = 0x01
	ValueIndexEntriesVersion   uint8 = 0x02 // includes BlockRef (page+len) per entry (NOTE-VI-027, issue #417)
	ValueIndexEntriesVersionV1 uint8 = 0x01 // legacy: BlockID uint32 per entry (NOTE-VI-014)
	// ValueIndexEntriesVersionV3 is kept for reading legacy files only.
	// New files always use V4 (V4 supplants V3 — V4 adds SpanID+RowIdx, same layout otherwise).
	ValueIndexEntriesVersionV3 uint8 = 0x03 // BlockRef + string-table SourceRef index (issue #432)
	ValueIndexEntriesVersionV4 uint8 = 0x04 // V3 + SpanID[8] + RowIdx[2] per entry (issue #428, default)
	ValueIndexKLLVersion       uint8 = 0x01

	// ValueIndexKLLK is the k parameter for the vi:value KLL sketch (~0.01% quantile error).
	// Value index files carry exactly one KLL column so there is no per-column memory pressure.
	ValueIndexKLLK = 10_000

	// ValueIndexEntriesPerChunk is the nominal number of posting list entries per snappy chunk.
	ValueIndexEntriesPerChunk = 2_048

	// ValueIndexFooterSize is the fixed byte size of the value index file footer.
	ValueIndexFooterSize = 32

	// ValueIndexWriterSpillEntries is the number of buffered posting-list entries at
	// which the value-index writer sorts its in-memory run and spills it to a temp
	// file (NOTE-VI-026, issue #413). At ~72 bytes/rawEntry this caps peak in-memory
	// buffer to roughly 36 MB regardless of column cardinality; the external
	// sort-merge at Flush then streams the runs with bounded memory. Columns smaller
	// than this stay entirely in memory and take the unchanged fast path.
	ValueIndexWriterSpillEntries = 500_000

	// ValueIndexCompactThresholdFiles is the file count above which compaction is triggered.
	ValueIndexCompactThresholdFiles = 8
	// ValueIndexCompactThresholdBytes is the total size above which compaction is triggered.
	ValueIndexCompactThresholdBytes = 64 << 20 // 64 MiB
	// ValueIndexCompactMaxLevel is the max compaction level (0 = unlimited).
	ValueIndexCompactMaxLevel = 0

	// ValueIndexFilenamePattern is fmt.Sprintf(ValueIndexFilenamePattern, level, id).
	//
	// Deprecated: use ValueIndexFilenamePatternV2 which embeds wall time range.
	ValueIndexFilenamePattern = "L%d-%s.blockpack"

	// ValueIndexFilenamePatternV2 embeds wall-clock time range for O(1) file discovery.
	// fmt.Sprintf(ValueIndexFilenamePatternV2, level, wallMinSec, wallMaxSec, id)
	// Example: L0-1750000000-1750003600-ce3sg9bh45cs7fvb.blockpack
	ValueIndexFilenamePatternV2 = "L%d-%d-%d-%s.blockpack"

	// Value index column names.
	ValueIndexValueColumn     = "vi:value"
	ValueIndexTraceIDColumn   = "vi:trace_id"
	ValueIndexSourceRefColumn = "vi:source_ref"
	ValueIndexTimeSecColumn   = "vi:time_sec"

	// ValueCountsRecordsPerChunk is the nominal number of VCNT records per snappy chunk.
	// Records are smaller than value index posting list entries, so a larger nominal count
	// keeps each decompressed chunk a few tens of KB.
	ValueCountsRecordsPerChunk = 4_096

	// ValueCountsFilenamePattern is fmt.Sprintf(ValueCountsFilenamePattern, level, id).
	// Consolidated per-tenant count files live at
	// indexes/<tenant>/unique_values/<column_hash>/L<level>-<id>.vcnt (issue #400).
	ValueCountsFilenamePattern = "L%d-%s.vcnt"
)

// MaxIntrinsicRows is the safety cap on accumulated rows in a single intrinsic column.
// If any one column exceeds this, the entire intrinsic section is written empty (TOC with
// 0 columns), which disables the zero-block-read intrinsic fast path for that file.
// Declared as a var (not const) so tests can temporarily lower it without writing huge files.
//
// NOTE-465 (issue #384): raised from 10_000_000 to 100_000_000. The 10M value was set when
// the file-level intrinsic section was accumulated entirely in memory (O(all columns × all
// spans) — tens of GiB, the OOM source fixed by NOTE-461). After NOTE-461 the writer spills
// per-column rows to disk and rebuilds ONE column at a time at Flush(), so peak write RSS is
// O(largest single column's rows); and the on-disk column blobs are page-encoded
// (deltaPageSize=1024 / paged dict), so the reader never materializes a whole column either.
// With both bounds in place the 10M section-drop was a pure performance cliff: production
// files assigned ~814–3,598 blocks (≈11–50M rows) silently lost their intrinsic section and
// fell back to full block fetch even when a query was fully intrinsic-covered. 100M keeps a
// sanity guard against pathological files (a single-column rebuild at 100M rows is ≈1–2 GiB
// transient, within MaxMetadataSize) while covering realistic large files with headroom.
//
// WARNING: production code must never modify this variable. Only tests may override it,
// and they must restore the original value via defer (see TestMaxIntrinsicRows_OverCap).
var MaxIntrinsicRows = 100_000_000

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

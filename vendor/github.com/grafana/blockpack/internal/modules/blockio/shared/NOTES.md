# blockio/shared — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/blockio/shared` package.

---

## 1. NOTE-BLOOM-REMOVAL: ColumnNameBloom Removed (2026-03-07)
*Added: 2026-03-07*

`ColumnNameBloom [32]byte` has been removed from `BlockMeta`. The column-name bloom filter
functions (`AddToBloom`, `TestBloom`, `BloomHash1`, `BloomHash2`, `SetBit`, `IsBitSet`,
`murmur32`) have been deleted from `bloom.go`.

**Rationale:** CMS subsumes bloom: if a column was never written to a block, `BlockCMS`
returns nil and the planner passes conservatively — identical behavior to a bloom miss.
CMS additionally provides value-level pruning for columns that ARE present. Column-name
bloom provided zero pruning for high-cardinality columns where every block has the column
(e.g. `resource.service.name`).

**Wire format impact:** Block index entries are 32 bytes smaller per block. Files written
before 2026-03-07 are unreadable with this version (breaking change, accepted for internal
format).

**Kept:** Trace-ID bloom functions (`AddTraceIDToBloom`, `TestTraceIDBloom`,
`TraceIDBloomSize`) are **not** removed — they serve a different purpose (compact index
trace-ID lookup).

---

## 2. *(Removed)* 256-Bit Column-Name Bloom Filter Size
*Added: 2026-03-05, removed: 2026-03-07*

The 256-bit (32-byte) `ColumnNameBloom` field and the `ColumnNameBloomBits`/`ColumnNameBloomBytes`
constants have been removed. See NOTE-BLOOM-REMOVAL (§1) for rationale.

---

## 3. ColumnKey for Type-Aware Column Maps
*Added: 2026-03-05*

**Decision:** `ColumnKey{Name, Type}` is used in maps that need to distinguish the same
attribute key with different types (e.g. `"foo"` as string vs. `"foo"` as int64).

**Rationale:** The OTLP data model permits the same attribute key to carry different types
across different spans within the same block. A name-only map key would cause silent
overwrite of the first column's data by the second. Using `ColumnKey` prevents this.

**Scope:** Only writer-internal column maps use `ColumnKey`. Range index
lookups remain name-only intentionally — at the block-pruning level, false positives
(treating int64 "foo" and string "foo" as the same column) are acceptable.

Back-ref: `internal/modules/blockio/shared/types.go:ColumnKey`

---

## 4. RLE Codecs for Presence and Index Data
*Added: 2026-03-05*

**Decision:** Column presence bitsets and dictionary index arrays are RLE-encoded rather
than stored as raw bit arrays.

**Rationale:** Blockpack columns typically have high locality — columns that are absent from
a row tend to be absent from many consecutive rows (e.g. a `span.http.status_code` column
is absent from non-HTTP spans). RLE compresses these runs to a small constant representation
regardless of span count.

For presence bitsets: a block with 2000 spans where a column is present in only 100 spans
can be represented in O(100) bytes rather than 250 bytes (2000/8).

For index arrays: spans from the same service or operation often repeat the same dictionary
entries consecutively, yielding high RLE compression.

Back-ref: `internal/modules/blockio/shared/presence_rle.go`,
`internal/modules/blockio/shared/index_rle.go`

---

## 6. NOTE-006: PageMeta Extended with Ref-Range Index (2026-03-28)
*Added: 2026-03-28*

**Decision:** Added `MinRef uint32`, `MaxRef uint32`, `RefBloom []byte` to `PageMeta`.

**Rationale:** Enables O(M × page_fraction) reverse lookups instead of O(N) full column
scans.

*Superseded by NOTE-007 (2026-03-29): RefBloom/MinRef/MaxRef removed. See NOTE-007 for rationale.*

---

## 7. NOTE-007: RefBloom Removed from Page TOC; v0x02 Not Supported (2026-03-29)
*Added: 2026-03-29*

**Decision:** Removed `RefBloom []byte`, `MinRef uint32`, and `MaxRef uint32` from
`PageMeta`. Removed `IntrinsicPageTOCVersion2` constant. `EncodePageTOC` writes version
0x01 only. `DecodePageTOC` only accepts version 0x01; v0x02 is not supported and returns
an error.

**Rationale:** RefBloom was designed to skip pages during reverse-lookup. After switching
field population entirely to `forEachBlockInGroups` (block reads), there are no remaining
callers. The ref-bloom provided zero pruning benefit at 10K entries/page with 256 bytes
(FPR ≈ 100% when full). Removal saves 256 bytes/page of storage and eliminates the bloom
maintenance cost at write time.

**Backward compat:** v0x02 files are not decoded. All production files write v0x01.

Back-ref: `shared/constants.go`, `shared/types.go`, `shared/intrinsic_codec.go`,
`writer/intrinsic_accum.go`

---

## 5. AttrKV Slice Instead of map[string]AttrValue
*Added: 2026-03-05*

**Decision:** `AttrKV` is a plain struct, and attribute sets are represented as `[]AttrKV`
rather than `map[string]AttrValue`.

**Rationale:** Per-span map allocations (header + hash buckets) create significant GC
pressure when processing millions of spans. A slice of `AttrKV` avoids map header allocation
entirely and is cache-friendly for small attribute sets (most spans have ≤ 20 attributes).
Lookup is O(N) where N is the attribute count, which is acceptable at the write path where
spans are processed once.

Back-ref: `internal/modules/blockio/shared/types.go:AttrKV`

---

## 8. NOTE-008: BUG-1 Fix — Bounds Check Before refsStart Arithmetic in Flat-Column Scan (2026-04-01)
*Added: 2026-04-01*

**Decision:** Added division-based bounds checks in `ScanFlatColumnRefs`, `ScanFlatColumnTopKRefs`,
and `ScanFlatColumnRefsFiltered` before computing `refsStart`. Each check uses `rowCount > (len(raw)-pos)/8`
rather than `pos+rowCount*8 > len(raw)` to avoid 32-bit integer overflow: on 32-bit platforms,
`rowCount*8` can wrap to a negative value when `rowCount > math.MaxInt/8`, making the addition-based
check ineffective.

**Rationale:** `rowCount` is an untrusted uint32 read from a snappy-decoded blob. A corrupt
or adversarially crafted blob can set `rowCount` to any value up to 2^32-1. Without this
check, `pos + rowCount*8` can produce a value larger than `len(raw)`, making `refsStart`
point well past the end of the buffer. Subsequent arithmetic on `refsStart`
(`refPos = refsStart + i*refSize`) could then produce values that look valid to the
`if refPos+refSize > len(raw)` per-ref guard (e.g. wrap-around on 32-bit platforms), leading
to out-of-bounds memory access.

The per-ref bounds checks (`if refPos+refSize > len(raw)`) remain in place as a defense-in-depth
second layer. This new check is the primary gate.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:ScanFlatColumnRefs`,
`ScanFlatColumnTopKRefs`, `ScanFlatColumnRefsFiltered`

---

## 10. NOTE-010: V14 Format Constants — Why These Values (2026-04-10)
*Added: 2026-04-10*

**Decision:** Added `VersionBlockV14=14`, `VersionBlockEncV3=3`, `FooterV7Version=7`,
`FooterV7Size=18`, six `Section*` byte constants, and `DirEntryKindType`/`DirEntryKindName`
to `constants.go`. Added `DirEntryType`, `DirEntryName`, and `SectionDirectory` structs
to `types.go`.

**Version 7 rationale:** The agentic branch uses `FooterV5Version=5` (46-byte vector footer)
and `FooterV6Version=6` (58-byte compact-traces footer). To avoid the version number collision,
our V14 section-directory footer uses version 7.

**Why V14:** The previous on-disk format (V12/V13, footer V3/V4) embedded all per-column
zstd compression inside each column blob and bundled all file-level metadata (block index,
range index, trace index, TS index, sketch, file bloom) into a single snappy-compressed
blob. To check a bloom filter, the reader had to decompress the entire metadata section
(potentially hundreds of MB). V14 makes each section independently addressable by storing
each as its own snappy-compressed blob with a pointer in the section directory. It also
moves compression responsibility to the column boundary (one snappy blob per column at the
block level) and removes all internal zstd sub-segments from encoding types.

**Why FooterV7Size=18:** `magic[4]+version[2]=7+dir_offset[8]+dir_len[4]` = 18 bytes. The
footer is small and fixed-size — readers always know where to find it (last 18 bytes of
file). `dir_len` is uint32 because the section directory (≤504 raw bytes) is small; uint32
provides ample headroom.

**Why 6 section types (0x01–0x06), not 7:** File-level intrinsic columns are name-keyed,
not a single type-keyed section. The original design had `SectionIntrinsic=0x07` as a
monolithic section containing all intrinsic column blobs, which would have required two
I/Os to access any column: (1) read TOC blob → find column offset, (2) read column blob.
The name-keyed entry design eliminates this indirection: each intrinsic column blob has its
own `DirEntryName` in the section directory, enabling direct addressing in one I/O after
reading the directory. Values 0x07+ remain reserved for future type-keyed sections.

**Why two entry kinds (DirEntryKindType=0x00, DirEntryKindName=0x01):** The section
directory must serve two structurally different needs: fixed enum-addressed sections (6
total, stable across files) and dynamically-named columns (variable count, file-specific).
A single fixed-size entry format cannot serve both without wasting bytes on name encoding
for type-keyed entries or truncating names for name-keyed entries. The kind byte at the
start of each entry allows parsers to dispatch to the correct unmarshal path.

**Alternatives considered:**
- *Keep single metadata blob*: Rejected — violates the requirement that each section must
  be independently readable without decompressing unrelated sections.
- *SectionIntrinsic as type-keyed (0x07) containing all intrinsic columns*: Rejected —
  requires two I/Os to access any intrinsic column (TOC read + column read). Name-keyed
  entries give direct per-column addressing.
- *Store section directory at fixed offset 0*: Rejected — blocks at offset 0 means no fixed
  structure before end-of-file; the footer-pointer pattern is consistent with V3/V4.

**How to apply:** These constants are the sole definition of V14 wire format values. All
writer and reader code must reference them rather than hardcoding numeric literals.

Back-ref: `internal/modules/blockio/shared/constants.go`,
`internal/modules/blockio/shared/types.go:DirEntryType`, `types.go:DirEntryName`

---

## 9. NOTE-009: BUG-13 Fix — Validate blockW/rowW Are 1 or 2 in Flat-Column Scan (2026-04-01)
*Added: 2026-04-01*

**Decision:** Added `if (blockW != 1 && blockW != 2) || (rowW != 1 && rowW != 2) { return nil }`
in all flat-column scan callers of `decodeRef`, and added the same check with an error return
in `decodeVariableWidthRef`.

**Rationale:** `blockW` and `rowW` are read from an untrusted snappy-decoded blob header.
Valid encoding uses 1-byte or 2-byte fields (supporting up to 256 or 65536 block/row indices
respectively). Any other value (0, 3, or higher) indicates a corrupt blob. Without this check,
`decodeRef` with `blockW==0` falls through to `binary.LittleEndian.Uint16` (reads 2 bytes for a
0-width field), producing a `BlockRef` whose block index is derived from the wrong bytes.
`decodeVariableWidthRef` had the same flaw. The fix makes both functions fail explicitly on
invalid widths rather than silently producing garbage.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodeVariableWidthRef`,
`ScanFlatColumnRefs`, `ScanFlatColumnTopKRefs`, `ScanFlatColumnRefsFiltered`

---

## 10. NOTE-011: Add ColumnTypeVectorF32 = 13 for Semantic Embeddings (2026-04-02)
*Added: 2026-04-02*

**Decision:** Added `ColumnTypeVectorF32 ColumnType = 13` to the ColumnType enum, along with
`VectorIndexMagic`, `VectorIndexVersion`, `FooterV5Version` (46-byte vector footer),
`FooterV5Size`, `EmbeddingColumnName`, and `EmbeddingTextColumnName` constants.

**Rationale:** Value 13 is the first reserved slot after UUID (12); adding it here does not
reorder or remove any existing constants. Float32 vectors for semantic embeddings are a new
column kind with distinct wire-format requirements (flat IEEE-754 LE float array with a
per-column dimension header), so a dedicated ColumnType is cleaner than overloading an
existing type or using a magic prefix in the column name.

**Consequence:** Readers encountering ColumnType = 13 in a block column header must handle it
gracefully. For non-vector queries, the column is never in `wantColumns` and is lazy-skipped
(no behavioral change). For semantic queries, a dedicated `vectorF32` decoder extracts the
raw float32 slice. Old readers (pre-VectorF32) that encounter type 13 fall through to the
default unknown-type path, which skips the column — backward-compatible by design.

Back-ref: `internal/modules/blockio/shared/types.go:ColumnTypeVectorF32`,
`internal/modules/blockio/shared/constants.go:VectorIndexMagic`,
`internal/modules/blockio/shared/constants.go:EmbeddingColumnName`

---

## NOTE-012: Snappy Decode Buffer Pool for Intrinsic Column Decoding (2026-04-14)
*Added: 2026-04-14*

**Decision:** Added `intrinsicBufPool` (`sync.Pool` of `*[]byte`) with 64KB default
capacity and a 4MB cap guard. `AcquireIntrinsicBuf` / `ReleaseIntrinsicBuf` are used in
`decodePagedColumnBlob` (per-page loop) and `DecodePageTOC` to reuse snappy decode scratch
buffers across calls.

**Rationale:** Before r60, every call to `decodePagedColumnBlob` allocated a new `[]byte`
per page for `snappy.Decode(nil, ...)`. For a paged column with 10 pages of ~64KB each,
that is 10 allocations × 64KB = 640KB of heap per decode. With the pool, the same buffer
is reused across pages within a single decode call (via `defer ReleaseIntrinsicBuf`), and
across calls from different goroutines (pool is shared). Benchmark (M8, histogram-by-service,
10K spans): -59% wall time.

**Pool design:**
- Default cap 64KB — covers typical pages; avoids realloc for sub-64KB pages.
- Cap guard 4MB — prevents pathological large pages from permanently occupying pool slots.
- `ReleaseIntrinsicBuf` resets length to 0 before returning to pool; replaces oversized
  buffers with a fresh 64KB buffer.
- `*pageBuf = pageRaw` after `snappy.Decode` updates the pool pointer if snappy reallocated
  (snappy reuses the buffer in-place when capacity is sufficient, reallocates otherwise).

**Safety prerequisite — BytesValues copy:**
`DecodeFlatPage` and `decodeLegacyFlatBlob` previously stored `BytesValues` as sub-slices
of the raw decode buffer (`raw[pos:pos+vLen]`). With the pool, the same buffer may be
reused on the next call while `IntrinsicColumn.BytesValues` still holds pointers into it,
causing silent data corruption. Fix: both functions now use `make([]byte, vLen) + copy`
before appending to `BytesValues`.

**Invariant:** All `BytesValues` slices returned by intrinsic decode functions are
independent copies that do not alias any pool buffer.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:AcquireIntrinsicBuf`,
          `internal/modules/blockio/shared/intrinsic_codec.go:ReleaseIntrinsicBuf`,
          `internal/modules/blockio/shared/intrinsic_codec.go:decodePagedColumnBlob`,
          `internal/modules/blockio/shared/intrinsic_codec.go:DecodePageTOC`,
          `internal/modules/blockio/shared/intrinsic_codec.go:DecodeFlatPage`,
          `internal/modules/blockio/shared/intrinsic_codec.go:decodeLegacyFlatBlob`

---

## NOTE-013: IntrinsicFormatXORBytes — Single Snappy Pass for Bytes Columns
*Added: 2026-04-22*

**Decision:** Added `IntrinsicFormatXORBytes uint8 = 0x03` to `constants.go`.
When a flat bytes column has more than `IntrinsicPageSize` rows, the writer encodes all
values with XOR-against-previous and compresses the entire payload in a single
`snappy.Encode` call instead of one call per page.

**Rationale:** Flat bytes columns (span:id, trace:id, span:parent_id) consist of
random-looking byte IDs (8–16 bytes each). The existing paged format calls
`snappy.Encode` once per page (10,000 rows), producing 226 calls for a 2.8M-row
column. Each page has ~80KB of random bytes: snappy cannot compress within a page
(no repeating patterns), but its frame overhead expands the output. Measured result:
52.8 MB compressed from 26.4 MB uncompressed (2× inflation).

XOR-against-previous produces deltas that are zero or near-zero in the high bytes for
IDs with shared prefixes (e.g. span IDs from the same trace). A single snappy call
over the full XOR payload achieves much better compression because it sees the global
redundancy across all N rows. Even for fully random IDs, one snappy pass has no
per-page frame overhead, so the worst case approaches raw uncompressed size.

**Wire format:** Outer sentinel is unchanged (`IntrinsicPagedVersion = 0x02`). The TOC
`Format` field is `0x03`. There is exactly one logical page in the TOC covering all N
rows. Inside the single snappy blob: `(xor_len[4]+xor_bytes)×N` then `refs[N×refSize]`.
The `xor_len` field is 4 bytes (uint32 LE) to match the existing `encodeXORBytes` convention.

**NOTE-012 invariant preserved:** `decodeXORBytesPage` reconstructs each value into a
freshly allocated `[]byte` (make+copy pattern). The `prev` pointer is set to the
just-allocated copy, not to pool memory. This guarantees `BytesValues` entries are
independent of the pool buffer and of each other.

**ScanFlatColumnRefs:** Not affected. All scan functions already guard
`colType == ColumnTypeBytes` and return nil. No change needed.

Back-ref: `internal/modules/blockio/shared/constants.go:IntrinsicFormatXORBytes`,
          `internal/modules/blockio/shared/intrinsic_codec.go:decodeXORBytesPage`,
          `internal/modules/blockio/shared/intrinsic_codec.go:xorInvert`,
          `internal/modules/blockio/writer/intrinsic_accum.go:encodeXORBytesIntrinsic`

---

## NOTE-014: IntrinsicFormatDeltaUint64 — Single Snappy Pass for uint64 Columns
*Added: 2026-04-22*

**Decision:** Added `IntrinsicFormatDeltaUint64 uint8 = 0x04` to `constants.go`.
When a flat uint64 column has more than `IntrinsicPageSize` rows, the writer sorts values
ascending, delta-encodes with unsigned uvarints, and compresses the entire payload in a
single `snappy.Encode` call instead of the paged flat format.

**Rationale:** The paged flat format resets the delta accumulator at every page boundary
(10,000 rows), losing cross-page delta patterns. For `span:start` (globally sorted ascending
after `sortFlatAccum`), a single-pass uvarint delta encoding + single snappy compression
exploits the fact that all deltas are small positive integers across the entire column.
Measured: nanosecond timestamps clustered in 100-row groups compress well below `N*8` raw
bytes. Even for monotonically increasing spans, per-page delta resets waste the first value
of every page as a full 8-byte varint.

**Unsigned varint, not zigzag:** `binary.PutUvarint` is used (not `binary.PutVarint`).
After ascending sort, all deltas are non-negative, so zigzag encoding doubles the cost
(zigzag maps 1 → 2, 2 → 4, etc.). Unsigned uvarint encodes positive deltas in the minimum
number of bytes.

**Wire format:** Outer sentinel is unchanged (`IntrinsicPagedVersion = 0x02`). The TOC
`Format` field is `0x04`. There is exactly one logical page in the TOC covering all N rows.
Inside the single snappy blob: `uvarint(value[i] - value[i-1])` for each of the N sorted
rows (value[-1] = 0), followed by `refs[N×refSize]`. No `values_len` prefix — the ref
section begins immediately after all N uvarints (sequential decode locates the boundary).

**Scan path:** `scanDeltaUint64PagedBlob` and `scanDeltaUint64PagedFiltered` are fully
separate helpers that do NOT call `pageRefsStart` (which assumes a `values_len[4]` prefix).
They decompress the page, read all uvarints to locate the refs boundary, then collect refs
for values within the requested [lo, hi] range.

**NOTE-012 invariant:** Not applicable — uint64 columns produce `Uint64Values` (value types),
not byte slices, so no pool aliasing concern exists.

Back-ref: `internal/modules/blockio/shared/constants.go:IntrinsicFormatDeltaUint64`,
          `internal/modules/blockio/shared/intrinsic_codec.go:decodeDeltaUint64Page`,
          `internal/modules/blockio/shared/intrinsic_codec.go:scanDeltaUint64PagedBlob`,
          `internal/modules/blockio/shared/intrinsic_codec.go:scanDeltaUint64PagedFiltered`,
          `internal/modules/blockio/writer/intrinsic_accum.go:encodeDeltaUint64Intrinsic`

---

## NOTE-015: Typed Accessor Methods for Zero-Alloc LookupRefFast (2026-04-28)
*Added: 2026-04-28*

**Decision:** Added `lookupRefIdx` (private), `LookupRefFastUint64`, `LookupRefFastInt64`,
`LookupRefFastString`, and `LookupRefFastBytes` to `intrinsic_ref_index.go`.

**Rationale:** `LookupRefFast` returns `(any, bool)`. Every call that returns a scalar
(uint64, int64, string, []byte) boxes the value into an interface{}, allocating one heap
object per call. At 10K spans × 11 columns × hundreds of blocks, this produced ~171M/120s
alloc_objects (59.6% of total) in production profiling (Pyroscope 2026-04-28).

**Design:** `lookupRefIdx` factors out the binary search (replaces the duplicated
`slices.BinarySearchFunc` call that would otherwise appear in all four typed methods).
The typed methods delegate to `lookupRefIdx` and then access the appropriate value array
directly without any interface conversion. `LookupRef` (the O(N) linear scan, any-returning)
was kept for compatibility; the typed accessors are preferred for hot paths.

**Safety:**
- `LookupRefFastBytes` returns a slice aliasing `col.BytesValues[idx]`. Per NOTE-012,
  `BytesValues` entries are already independent copies (make+copy in `DecodeFlatPage` /
  `decodeXORBytesPage`). Callers that need their own copy must clone explicitly.
- `LookupRefFastString` returns `col.DictEntries[idx].Value` directly. Go strings are
  immutable; aliasing is safe.
- Goroutine safety is preserved: all typed accessors call `lookupRefIdx` which calls
  `col.EnsureRefIndex()` (sync.Once internally).

Back-ref: `internal/modules/blockio/shared/intrinsic_ref_index.go:lookupRefIdx`,
          `internal/modules/blockio/shared/intrinsic_ref_index.go:LookupRefFastUint64`,
          `internal/modules/blockio/shared/intrinsic_ref_index.go:LookupRefFastInt64`,
          `internal/modules/blockio/shared/intrinsic_ref_index.go:LookupRefFastString`,
          `internal/modules/blockio/shared/intrinsic_ref_index.go:LookupRefFastBytes`

*Addendum (2026-04-28):* The four typed accessor methods (`LookupRefFastUint64`, `LookupRefFastInt64`,
`LookupRefFastString`, `LookupRefFastBytes`) and the private `lookupRefIdx` were superseded and removed
in the same session. The consolidated `LookupRefFast(packedRef uint32) (any, bool)` replaces all four,
returning the concrete type directly via a format switch. Per NOTE-094 in executor/NOTES.md, the
`[8]byte` value type for spanID/parentID eliminates the clone that `LookupRefFastBytes` previously required.
The `BENCH-SHARED-001` benchmark targeting `LookupRefFastUint64` was also removed (see BENCHMARKS.md addendum).

---

## NOTE-V8-001: ToCEntry and V8 Footer Design

**Context:** FooterV8 (version=8) uses the same 18-byte layout as FooterV7 but distinguishes
itself by the version field. The ToC blob it points to is a snappy-compressed stream of
`entry_count[4] + signal_type[1] + reserved[3] + []ToCEntry`.

**ToCEntry name_len[2]:** `name_len` is encoded as uint16 LE. With `MaxNameLen=1024`, the
maximum name is 1024 bytes, well within uint16 range (65535). The `MarshalInto` method uses
a `//nolint:gosec` annotation acknowledging this is safe given the MaxNameLen constraint.

**ToCEntryMinWireSize:** 22 bytes (type[4]+subtype[4]+name_len[2]+offset[8]+length[4]).
File-level sections (bloom, trace index, TS index, block index) use Name="" (empty).
Per-column sections (range, sketch, intrinsic) use Name=colName.

**ToCBlobHeaderSize:** 8 bytes (entry_count[4]+signal_type[1]+reserved[3]).

**Constants:** `FooterV8Version=8`, `FooterV8Size=18`, `ToCTypeMetadata=1`, `ToCTypeIndex=2`,
`ToCTypeBlock=3` (reserved), `ToCSubTypeRange=1`..`ToCSubTypeBlockIndex=7`.

Back-ref: `internal/modules/blockio/shared/constants.go:FooterV8Version`,
          `internal/modules/blockio/shared/types.go:ToCEntry`,
          `internal/modules/blockio/shared/types.go:UnmarshalToCEntry`

---

## NOTE-016: BlockRefRange — O(log N) Block-Boundary Find

_Added: 2026-05-04_

**Context:** The structural query executor calls `lookupIntrinsicFieldsTypedForBlock` once
per block per query. To scatter column values into a per-row result slice without N binary
searches, it needs the subslice of `refIndex` that belongs to a single blockIdx.

**Decision:** Add `BlockRefRange(blockIdx uint16) []RefIndexEntry` to `*IntrinsicColumn`.

**Why here (not in executor):** `refIndex` is unexported in the `shared` package. This is the
minimal accessor surface: one binary search to find start, then a linear walk for end.
The returned slice aliases `col.refIndex[start:end]` — no allocation.

**Why linear walk for end (not second binary search):** Avoids overflow at `blockIdx=0xFFFF`:
`(0xFFFF+1)<<16` would overflow uint32 to 0, producing an incorrect upper bound. The linear
walk condition `Packed>>16 == uint32(blockIdx)` is safe for all blockIdx values. For the
N_in_block entries that are sequentially laid out in refIndex, the walk is cache-friendly.

**Correctness invariant:** All entries for a single blockIdx are **contiguous** in refIndex
after `EnsureRefIndex` sorts by Packed. Because `Packed = blockIdx<<16 | rowIdx`, all entries
with the same blockIdx cluster together when sorted ascending.

**Caller:** `executor.populateTypedColumnForBlock` (NOTE-100 in executor/NOTES.md).

Back-ref: `internal/modules/blockio/shared/intrinsic_ref_index.go:BlockRefRange`

---

## NOTE-017: scanDeltaUint64PagedBlob — single-pass streaming decode + min/max page skip
*Added: 2026-06-08*

**Decision:** Replace two-pass decode (allocate `[]uint64`, fill all values, scan) with
single-pass streaming decode that tracks `startIdx`/`endIdx` while advancing `p` through
the uvarint stream, then reads only refs in the matching range.

**Allocation eliminated:** `make([]uint64, rowCount)` — 60MB for 7.5M-span production files.

**Min/max page skip:** Added the same `pm.Min`/`pm.Max` guard as `scanFlatPagedBlob`
(lines 1233-1243). The writer stores these in `PageMeta` (`intrinsic_accum.go`); the reader
was not using them for DeltaUint64 pages. Enables O(pages) skip for selective range predicates.

**Correctness invariant:** When `endIdx` is found before all uvarints are consumed, the inner
advance loop must complete the remaining uvarint scan to position `p` at the refs section start.
`refPos = p + startIdx*refSize` depends on `p` being exact. DeltaUint64 values are monotonically
non-decreasing (deltas >= 0), so early termination on `acc > hi` is safe.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:scanDeltaUint64PagedBlob`

## NOTE-145: decodePagedColumnBlob — decode pages directly into merged, drop dead value slice
*Added: 2026-06-09*

**Problem:** `decodePagedColumnBlob` allocated more than necessary on every paged column decode
(`DecodeIntrinsicColumnBlob` was ~27% of querier allocations per Pyroscope):

1. **Dead value slice.** `merged` pre-allocated *both* `Uint64Values` and `BytesValues` to
   `totalRows` for flat/xor/delta columns, but a column is uint64 OR bytes, never both — one
   full `totalRows`-sized slice was allocated and never written.
2. **Per-page intermediate column.** Each page was decoded into a fresh `*IntrinsicColumn` with
   its own `rowCount`-sized `Uint64Values`/`BytesValues`/`BlockRefs`, then copy-appended into
   `merged` and discarded. For a single-page column this duplicated the entire column
   (struct + slices + a full element copy).

**Decision:** Pre-size only the value slice the column type actually uses, and decode
flat/xor/delta pages *directly into* `merged`'s pre-allocated slices via append helpers
(`appendFlatPage`, `appendXORBytesPage`, `appendDeltaUint64Page`). The exported single-page
entry points (`DecodeFlatPage`, `DecodeDictPage`) are unchanged; the unexported wrappers
`decodeXORBytesPage`/`decodeDeltaUint64Page` were folded into their append variants. Dict still
materializes a page struct (`appendDictPage`) because the merge dedups entries by value.

**Correctness:** Byte-identical output. Delta `acc` and XOR `prev` reset per page (page-local),
so appending into a non-empty `merged` produces the same absolute values as decode-then-copy.
`merged.Count` accumulates identically (dict pages contribute 0, as before). Covered by the
existing multi-page roundtrip tests (`TestDeltaUint64LargeColumnRoundtrip` N=15k spans two
pages; the large XOR-bytes roundtrip) and reader paged-column tests, all green under `-race`.

**Bonus alloc removed:** the XOR path made a redundant `make+copy` of each value even though
`xorInvert` already returns a fresh, non-aliasing buffer — now appended directly.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodePagedColumnBlob`

## NOTE-146: appendDictPage — decode dict pages directly into merged, skip duplicate-entry allocs
*Added: 2026-06-09*

**Problem:** NOTE-145 left the dict path materializing a throwaway `*IntrinsicColumn` per page
(via `DecodeDictPage`) then merging it into `merged`. For multi-page group-by columns (e.g.
`resource.service.name`) the distinct value set repeats on every page, so every entry after
page 0 is a *duplicate*. The old merge, per duplicate entry, allocated a `BlockRefs` slice and
a `Value`-string copy inside `DecodeDictPage` — then immediately discarded both after copying
the refs into the existing merged entry. Each page also allocated a `*IntrinsicColumn` struct
and a `DictEntries` slice header that were discarded.

**Decision:** Parse the dict page inline in `appendDictPage` (no intermediate column). For a
value already present in `merged`, decode its refs straight onto the existing entry's
`BlockRefs` via `slices.Grow` — no per-entry slice, no `Value` copy. Only genuinely new values
allocate (and those allocations are kept in `merged`, so they are not waste). The dedup-map
probe is zero-alloc: `idx[string(window)]` for non-empty values, a reused `keyScratch` buffer
for the synthetic `"\x00"+int64` key of empty-string / int64 values.

**Correctness:** Byte-identical merge. The dedup key construction is unchanged from NOTE-145
(non-empty Value keys on its bytes; empty Value keys on the synthetic int64 form), so entry
identity, encounter-order, and ref order all match. `DecodeDictPage` stays exported and
unchanged for the single-page public API and tests. A `refSize <= 0` guard (and an
overflow-safe `refCount` bound) keeps corrupt blobs to an error rather than a panic — the old
per-iteration `pos+refSize > len(raw)` check was implicitly panic-safe; the new
`refCount > (len(raw)-pos)/refSize` bound divides, so the guard is required.

**Measurement (microbench, authoritative for alloc changes — cluster wall-clock is I/O/cache
bound):** `BenchmarkDecodeIntrinsicColumnBlob_Dict` (6 values × 30k spans = 3 pages):
38 → 31 allocs/op (-18%), bytes flat (~166 KB — the kept 30k×4-byte merged refs dominate).
Delta (8 allocs) and XORBytes (30009 allocs) unchanged — no regression on the other paths.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:appendDictPage`

## NOTE-147: appendXORBytesPage — reconstruct values into one page-sized arena, not one alloc/value
*Added: 2026-06-09*

**Problem:** After NOTE-145/146, the XOR-bytes decode path was the largest remaining allocator
in `DecodeIntrinsicColumnBlob` (Pyroscope: that function ≈ 27% of querier allocs). `xorInvert`
did `make([]byte, len(xored))` once *per row* — one heap allocation for every value. For a
10k-row page that is 10k tiny allocations; the alloc-guard microbench measured **30009 allocs/op**
for a 3-page span:id column. Allocation *count* (the `alloc_objects` axis) drives GC sweep cost
and pacing, so this dominated the path's GC pressure even though each value is small.

**Decision:** Reconstruct all of a page's values into a single page-sized byte **arena** and hand
each value out as a non-overlapping subslice. A cheap pre-scan (`xorBytesPageValueSize`) walks only
the 4-byte length prefixes — skipping the payloads — to sum the exact total value bytes, so the
arena is allocated once at the exact size (zero waste, no chunking). The decode loop then carves
`arena[off:off+xorLen:off+xorLen]` per value (three-index slice caps capacity, so a stray append on
a kept value cannot corrupt the next value's backing) and XOR-inverts into it via the new in-place
`xorInvertInto`. `xorInvert` (alloc-and-return) is replaced by `xorInvertInto` (write-into-dst);
the old allocating wrapper is removed since `appendXORBytesPage` was its only caller.

**Correctness:** Byte-identical reconstruction. The arena is never reallocated once carved, so
`prev` (which points into it) stays a valid, stable backing across iterations — same invariant the
old per-value slices provided. The arena is freshly allocated and never aliases the `pageBuf` pool
buffer (`raw`), preserving NOTE-012/NOTE-013. All truncation/`MaxBytesLen` bounds validation moved
into the pre-scan up front, so the decode loop reads at positions the pre-scan already proved valid
(no per-row bounds re-check needed). Covered by `TestXORBytesVariableLengthRoundtrip` (N=12k = 2
pages, interleaved 8/12/16-byte values — exercises arena carving of mixed sizes and both
length-mismatch branches of `xorInvertInto` across a page boundary), `TestXORBytesLargeColumnRoundtrip`,
and `TestXORBytesExactlyPageSize`, all green under `-race`.

**Measurement (microbench, authoritative for alloc changes — cluster wall-clock is I/O/cache
bound):** `BenchmarkDecodeIntrinsicColumnBlob_XORBytes` (16-byte values × 30k spans = 3 pages):
**30009 → 10 allocs/op (-99.97%)**; B/op flat (1,989,570 → 1,992,798, +0.16% — the contiguous arena
holds the same value bytes the per-value slices did). Delta (8 allocs) and Dict (31 allocs)
unchanged — no regression on the other decode paths.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:appendXORBytesPage`

## NOTE-150: decodePagesParallel — decode independent paged columns concurrently
*Added: 2026-06-09*

**Problem:** `DecodeIntrinsicColumnBlob` (Pyroscope: ≈27% of querier allocs, and the residual
cost of `{} | rate()`/M1 after NOTE-149 made its scan 0.54µs) decoded a paged column's pages
**serially** — one snappy-decode + value/ref decode per page on a single goroutine. For a real
file, `span:start` (Delta) and `trace:id`/`span:id` (XORBytes) are hundreds of pages of ~10k rows
each (`IntrinsicPageSize`), so this is a long single-threaded loop while the querier (per the
mission Pyroscope, ~33% CPU, I/O-latency bound) has spare cores.

**Decision:** Decode the pages of the self-contained formats (Flat / DeltaUint64 / XORBytes) in
parallel across up to `maxPageDecodeWorkers`=8 workers (`decodePagesParallel`). These formats are
page-independent: `appendDeltaUint64Page`/`appendFlatPage` reset their delta accumulator to 0 at
the start of every page and `appendXORBytesPage` resets `prev` to nil, so a page's absolute values
depend on nothing outside the page. Dict stays serial — it dedups values into a shared `dictIdx`
map across pages (cross-page state), so it is excluded by `isParallelPageDecodeFormat`.

The merged value/ref slices are already pre-sized to `cap == totalRows` (NOTE-145). Each page `i`
is handed a **disjoint, capacity-capped** sub-slice — `merged.Uint64Values[off:off:off+rc]` (len 0,
cap rc) and the matching `BlockRefs`/`BytesValues` slot — where `off` is the cumulative RowCount of
prior pages. The existing `append*` helpers are reused **unchanged**: appending exactly `rc` entries
fills `[off, off+rc)` and the three-index cap guarantees a goroutine can never grow past its slot or
touch another's region. The union of all slots is `[0, totalRows)` exactly, so after the workers
join, `merged.{values,BlockRefs}[:totalRows]` is the fully-populated column in page order. Workers
claim pages via an atomic counter (work-stealing → balanced load even when XOR pages vary in size),
each holds its own pooled snappy buffer (`AcquireIntrinsicBuf`), and a panic recover per worker
upholds SPEC-ROOT-001. Gated on `len(pages) >= 2 && totalRows >= 2*IntrinsicPageSize` so tiny
columns keep the zero-overhead serial path.

**Correctness:** Output is byte-identical to the serial loop — disjoint writes, no reduction, page
order preserved by `off`. Verified directly against known source data (not just serial parity) in
`intrinsic_parallel_decode_test.go` for Delta, Flat-uint64, and XORBytes across even pages, uneven
pages, a tiny-tail page (`{20001,5}`), single-page (serial), and under-threshold multi-page (serial)
layouts — all green under `-race` (the race detector confirms the per-page slots never alias).

**Measurement (microbench, authoritative for a parallelism change — cluster wall-clock is
I/O/cache bound with the page-cache memcached crashlooping, NOTE-143/148/149 precedent):**
`BenchmarkDecodePagedColumn{Delta,XORBytes}` (1M rows, 100 pages), serial-vs-parallel A/B at
GOMAXPROCS=8, count=6, min: Delta **9.9ms → 2.2ms (4.5×)**, XORBytes **31ms → 8ms (3.9×)**. B/op
flat; +~16 allocs/op total (per-worker snappy buffers + slot structs + goroutine setup), not per row.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodePagesParallel`

## NOTE-151: pool the cross-page dict-dedup map in decodePagedColumnBlob
*Added: 2026-06-09*

**Problem:** Dict is the one paged format NOTE-150 left serial because it dedups values into a
shared `dictIdx map[string]int` across pages. `appendDictPage` built that map lazily, sized
`valueCount*numPages` — but a dict column **repeats the same distinct value set on every page**
(that is *why* it is dict-encoded), so the map only ever holds ≈`valueCount` keys. For a
group-by column like `resource.service.name` (~245 values) on a real ~100-page file, the map was
sized for ~24,500 entries while ~245 were used: a ~100× over-allocation (~870 KB backing array)
allocated and GC'd **once per block per goroutine** on every M4/M6/M9/M10 `rate() by (...)` query.
Decode is ≈27% of querier allocs (mission Pyroscope, priority #3).

**Decision:** Acquire the dedup map from a `sync.Pool` (`acquireDictIdxMap`), used only for the
Dict format, released (cleared) on return. A cleared map retains its grown backing capacity, so
after warm-up it already holds the real distinct count — no per-call allocation and no rehash in
steady state. `releaseDictIdxMap` drops any map that grew past `dictIdxMapMaxEntries`=65536 (a
high-cardinality outlier) so the pool never pins a huge array. `appendDictPage` keeps its lazy
`make` as a fallback for a nil map (no current caller passes nil for Dict).

**Correctness:** Pure allocation change — the dedup keying, first-encounter dict order, and
in-place ref-merge are untouched. `TestDecodePagedColumnDictEquivalence` builds multi-page dict
blobs (values repeated per page, group-by shape) and asserts the merged dict order and per-value
refs match known source, **decoding each blob twice** to exercise a reused (cleared) pooled map;
green under `-race`.

**Measurement (microbench A/B, pool on vs forced-off, GOMAXPROCS=1, count=6, min;
`BenchmarkDecodePagedColumnDict` = 245 values × 100 pages ≈ 980k refs, the service.name shape):**
time **5.81ms → 4.88ms (−16%)**, **−874 KB/op** (the eliminated oversized map, exactly
`245*100*~36 B`), **−63 allocs/op**. Cluster wall-clock is I/O/cache bound (page-cache memcached
crashlooping) so the microbench is authoritative (NOTE-143/148/149/150 precedent).

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodePagedColumnBlob,appendDictPage,acquireDictIdxMap,releaseDictIdxMap`

## NOTE-152: dict column decode via a single contiguous BlockRefs arena (two-pass)
*Added: 2026-06-09*

**Problem:** A multi-page dict column repeats its distinct value set on every page (NOTE-151),
so after page 0 every value is a duplicate whose refs are spread across all pages. The
`appendDictPage` merge (NOTE-146) extended each existing entry's `BlockRefs` with
`slices.Grow(e.BlockRefs, refCount)` **once per page**. `slices.Grow` grows geometrically, so an
entry appearing on ~100 pages reallocated its backing array ~log₂(pages) times, and the summed
discarded intermediates came to ≈2.5× the final ref bytes. On a querier `alloc_space` profile
during M8 (`histogram_over_time(duration) by (resource.service.name)`) this single line —
`slices.Grow[[]BlockRef]` from `appendDictPage` — was **13.5% of all allocated bytes, the largest
single leaf** (pprof 2026-06-09; cum `decodePagedColumnBlob` 27%, matching the mission's priority-#3
"DecodeIntrinsicColumnBlob is 27% of querier allocs").

**Decision:** `decodeDictPagesArena` replaces the per-page grow with two passes over the pages:
1. **Count** — walk every page's value records header-only (refs skipped via
   `forEachDictPageValue`), materializing each `DictEntry` (`Value`/`Int64Val`) in first-appearance
   order and summing its total ref count across all pages into `refTotals[]` (parallel to
   `DictEntries`).
2. **Fill** — allocate **one** `[]BlockRef` arena of exactly `Σ refTotals` and carve it into
   per-entry sub-slices of exact capacity (`arena[off:off:off+c]`, len 0 cap c). A second walk
   appends each value's refs into its entry's sub-slice; because cap is exact, **no append ever
   reallocates**.

Pages are snappy-decoded **twice** (once per pass) into the pooled `IntrinsicBuf`. The querier is
I/O-latency bound with CPU headroom (mission profile: 33% CPU), so trading a second decompress for
≈zero retained over-allocation and far less GC churn is favorable — and it avoids holding every
decompressed page in memory at once (OOM-sensitive; `GOMEMLIMIT=13GiB`, prior OOM history).

**Why byte-identical:** entries are created in first-appearance order (page order, value order
within page) — exactly as `appendDictPage` did; the dedup key construction is unchanged (non-empty
`Value` keys on its bytes; empty/int64 keys on the synthetic `"\x00"`+LE(int64) form) and shared
between both passes via `forEachDictPageValue`; refs are appended in page order so each entry's ref
sequence matches the old in-place merge. Exact-capacity sub-slices share one backing array but a
later append on any entry reallocates (len==cap) rather than overwriting its neighbor — the same
arena safety contract as the NOTE-150 value/ref slices. Supersedes `appendDictPage` (removed) and
reuses the NOTE-151 pooled dedup map.

**Measurement (microbench A/B, HEAD vs change, GOMAXPROCS=1, count=6, min;
`BenchmarkDecodePagedColumnDict` = 245 values × 100 pages ≈ 980k refs, the service.name shape):**
time **4.87ms → 3.49ms (−28%)**, **B/op 14.37MB → 4.02MB (−72%)**, **allocs/op 2704 → 262 (−90%)**.
The residual 4.02MB ≈ the arena itself (980k refs × 4B) — essentially all transient decode garbage
removed. Faster despite the extra decompress because eliminating ~2440 small per-page regrow allocs
and the GC pressure outweighs the second snappy pass. `TestDecodePagedColumnDictEquivalence`
(multi-page, values-repeat-per-page, decoded twice) stays green under `-race`. Cluster wall-clock is
I/O/cache bound (page-cache memcached crashlooping) so the microbench is authoritative
(NOTE-143/148/149/150/151 precedent).

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodePagedColumnBlob,decodeDictPagesArena,forEachDictPageValue`

---

## NOTE-163: batch variable-width ref decode (appendVariableWidthRefs)

**Problem:** `decodeVariableWidthRef` was called once per row across all four intrinsic
ref-decode loops — `appendDeltaUint64Page` (span:start, hundreds of pages), `appendXORBytesPage`
(trace:id / span:id), the v1 flat/delta tail in `DecodeIntrinsicColumnBlob`, and the v1 legacy
dict tail in `decodeLegacyDictBlob`. Each call:
- re-validated `blockW`/`rowW` (BUG-13 guard) even though both are page-constant,
- re-derived `refSize`,
- re-evaluated the `blockW==1?` / `rowW==1?` width branches per row,
- did a per-row `pos+refSize > len(raw)` bounds check.

A querier CPU profile (2026-06-10) put `decodeVariableWidthRef` at ~0.44% self time on the hot
delta/XOR decode paths, which carry the residual M1/M4 decode cost (~27% of querier allocs are on
`DecodeIntrinsicColumnBlob`).

**Fix:** `appendVariableWidthRefs(raw, pos, blockW, rowW, count, *[]BlockRef)` validates widths
once, does a single up-front bounds check (`pos + count*refSize`), then dispatches on the width
combination **once** and runs a tight unrolled copy-and-advance loop per combination. The four
combinations (1+1, 1+2, 2+1, 2+2) each get a flat loop with no per-iteration width test. Behavior
is byte-identical to calling `decodeVariableWidthRef` `count` times; the single-ref form is removed
(its only remaining users were tests, which now exercise the batch form's BUG-13 validation).

**Why correct:** the refs section is exactly `count*(blockW+rowW)` contiguous bytes, so one bounds
check covers the whole run; the inner loops read the same byte offsets and produce the same
`BlockRef{BlockIdx, RowIdx}` values as the old per-row branch ladder. The slice is grown via the
caller's pre-sized backing (NOTE-145/152), so no extra allocation is introduced.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:appendVariableWidthRefs,appendDeltaUint64Page,appendXORBytesPage,DecodeIntrinsicColumnBlob,decodeLegacyDictBlob`

---

## NOTE-168: EnsureRefIndex — skip the sort when packed refs are already ascending

*Added: 2026-06-10*

`IntrinsicColumn.EnsureRefIndex` builds a sorted-by-`Packed`-ref lookup table once (sync.Once)
for O(log N) reverse lookup. It did this with an unconditional
`slices.SortFunc(idx, func(a,b) { cmp.Compare(a.Packed, b.Packed) })`. After NOTE-167 retired the
merge-join `sort.Search`, the residual sort cost on this path surfaced in the querier CPU profile
(reached via `slices.partitionCmpFunc` / `slices.partitionOrdered`, driven by the per-comparison
comparator closure).

**Key invariant:** for `IntrinsicFormatFlat` / `IntrinsicFormatXORBytes` / `IntrinsicFormatDeltaUint64`
columns, `BlockRefs` is emitted in row order during decode (`appendVariableWidthRefs`). Within a
single block `RowIdx` rises monotonically and `BlockIdx` is constant, so the packed key
(`BlockIdx<<16 | RowIdx`) is already in ascending order for the dominant single-block decode case
(`span:start`, `trace:id`, `span:id` on M1/M4). Multi-block merged columns keep `BlockIdx` in the
high 16 bits, so they too stay ascending whenever blocks are appended in index order.

**Fix:** build the `RefIndexEntry` keys and detect ascending order in the **same pass** (one extra
`p < prev` comparison per entry, no allocation). When the keys are already monotonic, skip
`slices.SortFunc` entirely — the result is identical because the input is already the sorted output.
When not monotonic (e.g. interleaved dict entries, out-of-order block merges), fall back to the
existing `slices.SortFunc`, so correctness is unchanged for every input.

**Why correct:** the slice content (`{Packed, Pos}` pairs) is identical to the old code; only the
sort is conditionally skipped, and only when a single O(N) scan has proven the slice is already in
the exact order `slices.SortFunc` would produce. Binary-search consumers (`lookupRefIdx`,
`BlockRefRange`, `LookupRefFast*`) see the same sorted index in both branches.

Back-ref: `internal/modules/blockio/shared/intrinsic_ref_index.go:EnsureRefIndex`

## NOTE-174: EnsureRefIndex — LSD radix sort for the unsorted fallback

*Added: 2026-06-10*

NOTE-168 made `EnsureRefIndex` skip the sort when the packed-ref keys are already ascending
(the single-block flat/delta case). The residual cost is the *fallback* path taken when the
keys are **not** monotonic — interleaved multi-value dict columns (the dominant case for
`rate() by (...)` / `histogram_over_time(...) by (...)` group-by queries, where each dict
entry's row-sorted `BlockRefs` are concatenated across entries) and out-of-order block
merges. That fallback called
`slices.SortFunc(idx, func(a,b){ cmp.Compare(a.Packed,b.Packed) })`.

**Problem:** a fresh querier CPU profile (2026-06-10, 30m) attributed ~5.1% of total querier
CPU to `EnsureRefIndex.func1` reaching `slices.pdqsortCmpFunc` / `slices.partitionCmpFunc`.
The cost is the per-comparison comparator **closure indirection** of `SortFunc` on top of the
O(N log N) comparison count — `pdqsortCmpFunc` was measurably more expensive than the
closure-free `pdqsortOrdered` in the same profile.

**Fix:** `radixSortRefIndex` — an LSD radix sort over the fixed 32-bit `Packed` key (4
counting passes of 256 buckets, least-significant byte first). O(N) with no comparator
indirection, so it removes the comparison-sort cost entirely. One scratch buffer is allocated
per call; since the index is built at most once per column under `sync.Once`, the allocation
is amortized away and far cheaper than the repeated closure calls it replaces.

**Why correct:** both the radix sort and `slices.SortFunc` order solely by `Packed`. Four
(even) passes leave the result back in the original slice (no final copy). Ties on `Packed`
keep an arbitrary-but-consistent order; the binary-search consumers (`lookupRefIdx`,
`BlockRefRange`, `LookupRefFast*`) locate by `Packed` only and never depend on tie order.
Verified Packed-order-identical to `slices.SortFunc` over 2000 random trials plus edge cases
(empty, single, all-equal, sorted, reverse-sorted, extreme 0/0x80000000/0xFFFFFFFF keys);
`go test -race ./blockio/shared` and `./executor` green. Microbench (20k interleaved entries):
1680µs SortFunc → 489µs radix = 3.4x faster.

Back-ref: `internal/modules/blockio/shared/intrinsic_ref_index.go:radixSortRefIndex`

## NOTE-190: radixSortRefIndex — bound the pass count by key magnitude (skip leading-zero bytes)

*Added: 2026-06-11*

NOTE-174's `radixSortRefIndex` always ran all four byte passes over the 32-bit `Packed` key
(`BlockIdx<<16 | RowIdx`). A querier CPU profile (2026-06-11, 30m) still showed it at ~0.71%
self time — the top blockpack symbol on the group-by histogram/rate path. Most of the upper
two byte passes (shift 16/24) are degenerate: `RowIdx` is always 16-bit and `BlockIdx` is
typically small, so the high bytes are frequently all-zero. A zero-valued byte position is an
*identity* radix pass (every key lands in bucket 0) that still costs a full O(N) count plus an
O(N) scatter.

**Fix:** OR all keys in one O(N) prescan, find the highest non-zero byte, and run radix passes
only up to that byte. Keys confined to the low 16 bits (the dominant single-block / RowIdx-only
case) now run 2 passes instead of 4. When all keys are 0 the slice is already trivially sorted
and zero passes run.

**Why correct:** skipping a leading-zero byte position is a no-op permutation, so the remaining
passes produce the identical ascending order on `Packed`. The variable pass count changes the
output buffer parity, so after an odd number of passes the sorted data (now in `buf`) is copied
back into `idx`; after an even count it is already in `idx`. Verified Packed-order-identical to
`slices.SortFunc` across the existing 2000-trial random test, the edge-case table, and a new
byte-skip test sweeping max key magnitudes of 1/2/3/4 significant bytes (500 trials each);
`go test -race ./blockio/shared` and `./executor` green. Microbench (50k entries): full-32bit
549µs, low-24bit 430µs (−22%), low-16bit 310µs (−43%).

Back-ref: `internal/modules/blockio/shared/intrinsic_ref_index.go:radixSortRefIndex`

## NOTE-169: appendDeltaUint64Page — single-byte uvarint fast path, index-based store

*Added: 2026-06-10*

`appendDeltaUint64Page` decodes a delta-uint64 page (span:start and other sorted-uint64 intrinsic
columns) by accumulating per-row uvarint deltas. After NOTE-168 retired the EnsureRefIndex sort,
the querier CPU profile flagged `appendDeltaUint64Page` (~2.1% self time) as the residual decode
cost of the unfiltered rate path (M1/M4 — `{} | rate()` and `{} | rate() by (...)`), which scans
every block's span:start column.

**Problem:** the loop called `binary.Uvarint(raw[pos:])` per row. Even inlined, that re-creates a
slice header each iteration and runs its generic continuation-bit shift loop, and the result was
appended one value at a time. Delta-sorted columns overwhelmingly produce deltas `< 128` (one byte),
so the generic decoder did far more work than the common case needs.

**Fix:**
- **Single-byte fast path:** test `raw[pos] < 0x80` first; if so the delta is the byte value itself
  — a plain load + add + `pos++`, no re-slice, no loop. Only when the continuation bit is set do we
  fall back to `binary.Uvarint(raw[pos:])` for the multi-byte case (identical decode).
- **Index-based store:** pre-extend `dst.Uint64Values` by `rowCount` once (within existing capacity
  — callers pre-size to `totalRows`, NOTE-145/150 — else a single `append(make(...))` grow) and
  write each value by index, removing the per-row append cap check. The pre-extend respects the
  three-index capacity-capped slots used by `decodePagesParallel` (cap == rc, base == 0), so it
  fills exactly the worker's disjoint `[off:off+rc)` region and never reallocates or aliases
  another worker's slot.

**Why correct:** the single-byte branch is exactly `binary.Uvarint`'s output when the leading byte
has no continuation bit (`acc += b`); the multi-byte branch delegates to `binary.Uvarint` unchanged.
The truncation error (`pos >= len(raw)`) is raised before any out-of-range read. Refs decode is
unchanged (`appendVariableWidthRefs`). Verified bit-identical to the old append/Uvarint form over
2000 random trials mixing small (1-byte) and large (multi-byte) deltas across both branches and
correct ref reconstruction; `go test -race ./blockio/shared` and `./executor` green.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:appendDeltaUint64Page`

---

## NOTE-171: decodeDictPagesArena — single page decode via budgeted page retention
*Added: 2026-06-10*

**Problem:** `decodeDictPagesArena` (NOTE-152) snappy-decompresses every page **twice** — once
in the sizing pass (pass 1) and once in the fill pass (pass 2). NOTE-152 deliberately accepted the
redundant second decompress to keep memory bounded, under the then-current assumption that the
querier was I/O-latency bound with CPU headroom (the old "33% CPU" profile). The 2026-06-10 querier
CPU profile shows the opposite: queriers peg ~5 cores on heavy metrics queries, and `snappy.Decode`
inside `decodeDictPagesArena` is on the M4 (`rate() by (resource.service.name)`) group-by hot path.
The second decode is now wasted CPU on a CPU-bound path.

**Decision:** Retain each page's decompressed bytes from pass 1 in a pooled `IntrinsicBuf` and reuse
it in pass 2, decoding each page **once** in the common case. Retention is capped by a 4 MiB total
budget (`retainBudget = intrinsicBufMaxCap`): once the sum of retained decompressed bytes would
exceed the budget, that page's buffer is released and the page is re-decoded in pass 2 into a single
shared scratch buffer (the old behavior). `retained[i] == nil` is the sentinel for "re-decode page
i". This preserves the bounded-memory guarantee under `GOMEMLIMIT=13GiB` — at most ~4 MiB of
decompressed pages plus one scratch buffer are held at once — while eliminating the second snappy
decode for the overwhelmingly common case of small dict columns that fit the budget.

**Why correct / byte-identical:** Output is independent of whether a page was retained or re-decoded
— the decompressed bytes are identical either way, and `forEachDictPageValue` / `decodeRef` produce
the same entries and refs. `decodeRef` returns a `BlockRef` **value** (no aliasing into the page
buffer), and `DictEntry.Value` strings are created via `string(valBytes)` (a copy), so reusing or
releasing page buffers after each pass cannot corrupt live column data. Entry order, per-entry ref
order, and the exact-capacity arena contract from NOTE-152 are all unchanged. All retained buffers
are returned to the pool via a deferred cleanup.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:decodeDictPagesArena`

---

## NOTE-186: index-based ref store into pre-extended slices (two ref-decode sites)
*Added: 2026-06-11*

Both per-page ref-decode loops appended `BlockRef`s one at a time into a slice whose backing
capacity the caller already guarantees, paying a bounds-vs-cap check and a length update per ref.
A querier CPU profile (2026-06-09) attributed ~1.4% self time to `appendVariableWidthRefs` (the
delta/xor/flat ref section) and ~1.35% to the `decodeDictPagesArena` pass-2 ref closure (the dict
group-by ref fill, on the M4 `rate() by (...)` hot path) — together ~2.7% of the ref-decode surface.

**Fix:** mirror the NOTE-169 value-side store discipline at both sites. Pre-extend the destination
slice by the number of refs about to be written, then write each ref by index.

- **`appendVariableWidthRefs`:** callers always guarantee capacity — the serial path pre-sizes
  `BlockRefs` to `totalRows` (`decodePagedColumnBlob`, NOTE-145) and the parallel path hands each
  page a capacity-capped sub-slice `[off:off:off+rc]` (`decodePagesParallel`, NOTE-150), so
  `cap-len >= count` always holds and the slice is never reallocated. A `make`-backed growing append
  guards the (currently unreachable) short-capacity caller.
- **`decodeDictPagesArena` pass 2:** each entry's `BlockRefs` is carved with exact capacity
  `refTotals[j]` (`arena[off:off:off+c]`), and the summed `refCount` across all page occurrences of
  an entry equals that capacity (the same sum computed in pass 1). Extending by `refCount` per
  occurrence therefore never exceeds cap and never reallocates, preserving the exact-capacity arena
  contract from NOTE-152 (a stray future append still reallocates rather than clobbering a neighbor).

**Why correct:** the index-written `BlockRef` values are byte-identical to the appended ones — only
the store mechanism changed, not the decode of `BlockIdx`/`RowIdx`. Page order (hence per-entry ref
order) is unchanged. The `end > len(raw)` truncation check in `appendVariableWidthRefs` is unchanged
and still runs before any read. Verified bit-identical across all four ref-width combinations, the
parallel sub-slice path, and dict ref reconstruction; `go test -race ./blockio/shared` and
`./executor` green, including the paged-column equivalence suites.

Back-ref: `internal/modules/blockio/shared/intrinsic_codec.go:appendVariableWidthRefs`,
`internal/modules/blockio/shared/intrinsic_codec.go:decodeDictPagesArena`

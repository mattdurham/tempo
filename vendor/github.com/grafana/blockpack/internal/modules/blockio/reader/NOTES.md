# Reader Module — Design Notes

## NOTE-001: Lazy Column Decode (Compressed-First, On-Demand Full Decode)
*Added: 2026-03-05*
*Updated: 2026-04-14 — V14 two-stage model: compressedEncoding → ensureDecompressed → decodeNow*

**Problem:** `ParseBlockFromBytes` with a `wantColumns` filter decoded only the predicate
columns eagerly, leaving all remaining columns absent from the block. Callers (executor)
had to issue a second pass via `AddColumnsToBlock` to decode those remaining columns eagerly
before the row loop — decoding ~90 columns per block even when only ~10-15 were accessed.

**Solution (V14):** Replace the two-pass decode with a single-pass lazy model:
1. **Eager pass** — decode `wantColumns` fully (snappy decompress + presence + values).
   Mark `decoded=true`. Unchanged.
2. **Lazy registration** — for all other columns, store `compressedEncoding` as a zero-copy
   sub-slice of rawBytes. No snappy decompression occurs. `rawEncoding` stays nil.
   (SPEC-V14-002)
3. **On-demand full decode** — the first call to any value accessor OR `IsPresent()` calls
   `decodeNow()` (via `decodeOnce`), which first calls `ensureDecompressed()` (via
   `decompressOnce`) to snappy-decode into `rawEncoding`, then runs `readColumnEncoding`
   to populate all typed fields.

**`Column.IsPresent()` behavior:** Triggers full decode on a lazy column (same path as
any value accessor). After decode, if `c.Present` is nil, all spans are present (no
bitmap). If non-nil, the bitset is consulted.

**Savings estimate (T9/Q66, 1997 blocks, ~90 non-predicate columns accessed ~15 times):**
- Old: 1997 × 90 × (snappy + full_decode) ≈ 870ms
- New: 1997 × ~15 × (snappy + full_decode) ≈ 145ms (non-accessed columns: 0 cost)
- Expected saving: ~725ms per query (improved from prior estimate by eliminating presence-only decode overhead)

Back-ref: `internal/modules/blockio/reader/column.go:ensureDecompressed`,
`internal/modules/blockio/reader/column.go:decodeNow`,
`internal/modules/blockio/reader/block.go:Column`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## NOTE-002: rawEncoding Lifetime Safety and Concurrency Model
*Added: 2026-03-05*
*Updated: 2026-04-14 — single-goroutine claim removed; decodeOnce/decompressOnce added for concurrent safety*

**rawEncoding lifetime:** `rawEncoding` is populated by `ensureDecompressed()` as a
freshly-allocated snappy-decoded buffer. It is consumed by `decodeNow()` (which clears
it on completion). Both operations are guarded by their respective `sync.Once` fields
(`decompressOnce`, `decodeOnce`). All lazy decodes complete before the BlockWithBytes
goes out of scope (query row loop).

**compressedEncoding lifetime:** `compressedEncoding` is a zero-copy sub-slice of
`BlockWithBytes.RawBytes`, valid for the lifetime of the owning BlockWithBytes.
It is cleared inside `decompressOnce.Do` after snappy decoding.

**internMap safety:** Each `ParseBlockFromBytes` and `AddColumnsToBlock` call creates its
own fresh `make(map[string]string)` intern map local to that call. `ResetInternStrings()`
is a no-op retained for call-site compatibility.

**Concurrency:** Column decode is safe for concurrent callers. `decompressOnce` and
`decodeOnce` serialize concurrent access. `decoded atomic.Bool` provides a fast-path
atomic check before entering `decodeOnce.Do` (NOTE-CONC-001). The scan path itself is
typically single-goroutine, but `IsPresent()` and value accessors on the same Column
may be called concurrently (e.g. when multiple executor goroutines share a block).

Back-ref: `internal/modules/blockio/reader/column.go:decodeNow`,
`internal/modules/blockio/reader/column.go:ensureDecompressed`

---

## NOTE-CONC-001: decoded atomic.Bool — Cross-Goroutine Signal for Column Decode
*Added: 2026-04-14*

**Problem:** With two separate `sync.Once` fields (`presenceOnce` and `decodeOnce`),
concurrent goroutines calling `IsPresent()` could read `rawEncoding` outside of any
Once closure, racing with `decodeOnce.Do` writing `rawEncoding`. The two Onces
operated on overlapping state, creating an undetected data race.

**Solution:** Eliminate `presenceOnce`. Merge all decode into a single `decodeOnce`.
Introduce `decoded atomic.Bool` as the ONLY cross-goroutine signal:

- `decoded.Store(true)` is called in ALL exit paths of `decodeOnce.Do` (success, error,
  and the nil-rawEncoding branch).
- `decoded.Store(false)` is called in `resetColumn()` when a Column is reused for a
  new block.
- `needsDecode()` reads `decoded.Load()` atomically — this is the ONLY safe outer check.
- `rawEncoding` and `compressedEncoding` MUST only be read or written inside their
  respective Once closures (`decodeOnce.Do` and `decompressOnce.Do`). Reading them
  outside (even as a nil-check) races with concurrent Once execution.

**Why not rely solely on `decodeOnce.Do`?** `sync.Once.Do` is idempotent and cheap
after the first call (one atomic load internally). The `decoded atomic.Bool` provides
the same fast-path cost while making the "already decoded" state explicit and readable
without relying on `sync.Once` internals. It also enables `IsDecoded()` to be a clean
public API without exposing the Once.

**Applied at:** `block.go:IsPresent`, `block.go:needsDecode`, `column.go:decodeNow`,
`block_parser.go:parseBlockColumnsReuse` (eager path sets decoded=true immediately),
`block_parser.go:resetColumn` (resets decoded=false for reused columns),
`reader.go:AddColumnsToBlock` (sets decoded=true after eager decode).

Back-ref: `internal/modules/blockio/reader/block.go:needsDecode`,
`internal/modules/blockio/reader/block.go:decoded`,
`internal/modules/blockio/reader/column.go:decodeNow`

---

## NOTE-003: objectcache Migration — Process-Level Caches with Strong References
*Added: 2026-03-23*
*Updated: 2026-03-29*

**Problem:** The three `sync.Map` process-level caches in `parser.go` held strong
`*T` pointers, causing parsed file metadata (~45 MB per file after snappy decode),
sketch indexes, and decoded intrinsic columns to accumulate without bound. In Tempo
deployments scanning hundreds of blockpack files, these caches grew until OOM or
process restart. Additionally, the intrinsic TOC (`r.intrinsicIndex` map) had no
process-level cache at all — it was re-decoded from bbolt bytes on every
`NewReaderFromProvider` call even when the raw blob was already cached.

**Solution:** Replace all three `sync.Map` globals with `objectcache.Cache[T]`
instances (new `internal/modules/objectcache/` module). Add a fourth cache for
the intrinsic TOC. `objectcache.Cache[T]` stores strong `*T` references:

- Entries are retained for the process lifetime; no GC reclamation occurs.
- Memory is bounded by `GOMEMLIMIT` at the process level.
- `ClearCaches()` updated to call `.Clear()` on all four instances.

*Addendum (2026-03-29):* An intermediate design used `weak.Pointer[T]` for
GC-cooperative eviction. Profiling revealed a 50x regression — weak entries were
reclaimed between block scans, forcing constant re-decode from file cache. The
implementation was reverted to strong references. See objectcache NOTE-OC-001 for
the full rationale.

**`metadataBytes` safety:** `*Reader` copies `pm.metadataBytes` at construction,
establishing a strong ref chain `Reader → metadataBytes` independent of the cache.
Range index offsets sub-slice into this copied pointer, remaining valid for the
entire reader lifetime.

**Concurrent double-parse:** Two goroutines opening the same file simultaneously
may both miss the cache and both parse. The second `Put` overwrites with an
equivalent object (immutable data). This is the same race as the prior `sync.Map`
code. `filecache.GetOrFetch` deduplicates the underlying I/O via singleflight, so
at most one raw-bytes read occurs even if two parses run.

Back-ref: `internal/modules/blockio/reader/parser.go`,
`internal/modules/blockio/reader/intrinsic_reader.go`,
`internal/modules/objectcache/cache.go`

---

## NOTE-004: BUG-07 — compareRangeKey float64 NaN safety via cmp.Compare
*Added: 2026-03-23*

**Problem:** The `ColumnTypeRangeFloat64` branch of `compareRangeKey` used manual `<` / `>`
comparisons. IEEE 754 defines NaN as unordered: `NaN < x`, `NaN > x`, and `NaN == x` are
all false. Both branches failed for any NaN operand, causing the function to fall through to
`return 0` (equal). Binary search then placed NaN at an unpredictable position, corrupting
`BlocksForRange` / `BlocksForRangeInterval` results silently.

**Fix:** Replace manual comparisons with `cmp.Compare(va, vb)` (Go 1.21+). `cmp.Compare`
implements a stable total order: NaN is treated as less than any non-NaN value (including
`-Inf`). This matches the contract required by `slices.SortFunc` / `sort.Search` callers.

**Why cmp.Compare and not NaN guard:** A NaN guard (`if math.IsNaN → return 0`) would still
return 0 for `NaN vs NaN` (acceptable) but could return 0 for `NaN vs -Inf` (also NaN ==
-Inf, wrong). `cmp.Compare` gives a consistent total order without special-casing.

Back-ref: `internal/modules/blockio/reader/range_index.go:compareRangeKey`

---

## NOTE-006: Intern Map Pool — per-call map replaced by sync.Pool
*Added: 2026-03-25*

**Problem:** `ParseBlockFromBytes` at `reader.go:481` allocated a fresh `make(map[string]string)`
on every call. `scanBlocks` calls `ParseBlockFromBytes` twice per matching block (first pass for
predicate evaluation, second pass to decode result columns), producing hundreds of map allocations
per query over many blocks.

**Why not pool inside ParseBlockFromBytes:** The returned `*BlockWithBytes` outlives the call.
Lazy columns (registered during first-pass parsing) store an `internMap` reference and call
`decodeNow()` during the row-emission loop after `ParseBlockFromBytes` returns. Clearing and
returning the map inside `ParseBlockFromBytes` would corrupt lazy decodes in progress.

**Solution:** Pool at the `scanBlocks` block-loop level. `scanBlocks` acquires one pooled map
per block iteration, passes it to both first-pass and second-pass `ParseBlockFromBytesWithIntern`
calls, then releases it **after** `streamSortedRows` completes (all lazy decodes done). This
guarantees the map is alive for the full block lifetime.

**Safety invariants:**
- Strings interned during parsing are copied into heap-allocated `Column.StringDict` entries
  before the intern map is cleared. Clearing the map only removes key→value references in the
  map; the underlying string data in column dicts is unaffected.
- The scan path is single-goroutine (NOTE-002). No concurrent access to the pooled map occurs.
- `ParseBlockFromBytes` (the original public method) is unchanged — it still allocates a fresh
  map for callers outside `scanBlocks`.

**NOTE-002 addendum:** Per-call intern maps (introduced for race-safety) are now pooled rather
than heap-allocated for the `scanBlocks` hot path. The race-safety guarantee is preserved: the
pooled map is held exclusively by one goroutine's block iteration at a time (single-goroutine
scan invariant), and is cleared before returning to the pool.

**Related:** The companion clone elimination in `scanBlocks` (`executor` package) is documented in executor NOTE-049.

Back-ref: `internal/modules/blockio/reader/column.go:internMapPool`,
          `internal/modules/blockio/reader/column.go:AcquireInternMap`,
          `internal/modules/blockio/reader/reader.go:ParseBlockFromBytesWithIntern`,
          `internal/modules/executor/stream.go:scanBlocks`

---

## NOTE-007: Present-Rows Scratch Pool — collectPresentRowsInto
*Added: 2026-03-25*

**Problem:** `collectPresentRows` at `column.go` allocated a fresh `make([]int, 0, presentCount)`
on every call. Both `decodeXORBytes` and `decodePrefixBytes` called this function, producing one
allocation per XOR/prefix column per block parse. With many such columns per block this added up
to hundreds of `[]int` allocations per query.

**Solution:** Replaced `collectPresentRows` with `collectPresentRowsInto`, which accepts a
caller-supplied `*[]int` buffer from `presentRowsScratchPool`. Callers (`decodeXORBytes`,
`decodePrefixBytes`) acquire a scratch before the loop and release after. The buffer is reset
to `[:0]` inside `collectPresentRowsInto` before each use.

**Cap guard:** If the pooled slice grows beyond 65536 entries (due to a very large block), it is
replaced with a fresh 2048-entry slice before pool return, preventing large backing arrays from
being retained indefinitely.

**Lifetime:** The scratch is valid for the duration of the per-row decode loop and released
immediately after. The `presentRows` slice returned by `collectPresentRowsInto` is `*buf` — it
is only iterated in the same stack frame and not stored. The loop variable `presentRow` is a
copy. No aliasing issues.

Back-ref: `internal/modules/blockio/reader/column.go:presentRowsScratchPool`,
          `internal/modules/blockio/reader/column.go:collectPresentRowsInto`,
          `internal/modules/blockio/reader/column.go:decodeXORBytes`,
          `internal/modules/blockio/reader/column.go:decodePrefixBytes`

---

## NOTE-005: BUG-08 — decode*Key sentinel values for malformed short keys
*Added: 2026-03-23*

**Problem:** `decodeInt64Key`, `decodeUint64Key`, and `decodeFloat64Key` all returned `0`
for keys shorter than 8 bytes. Zero is a valid encoded value for all three types, so
malformed (short) keys were silently treated as valid zero-values. This corrupted binary
search comparisons in `compareRangeKey` and `BlocksForRange`/`BlocksForRangeInterval`.

**Fix:** Apply type-appropriate sentinels for short-key fallback:
- `decodeInt64Key`: return `math.MinInt64` — sorts below all valid int64 values.
- `decodeUint64Key`: return `0` — already the minimum uint64 sentinel; no change needed.
- `decodeFloat64Key`: return `math.NaN()` — `cmp.Compare` (NOTE-004/BUG-07) treats NaN as
  less than any non-NaN, giving a consistent total order without corrupting search.

**Shared helper:** `readLE8(key string) (uint64, bool)` extracts the 8-byte LE uint64 and
returns `false` for short keys. Each decode function applies its own sentinel on `!ok`.

Back-ref: `internal/modules/blockio/reader/range_index.go:readLE8`,
          `internal/modules/blockio/reader/range_index.go:decodeInt64Key`,
          `internal/modules/blockio/reader/range_index.go:decodeFloat64Key`
Test: `internal/modules/blockio/reader/range_index_bugs_test.go:TestDecodeFloat64Key_ShortKey_ReturnsNaN`,
      `internal/modules/blockio/reader/range_index_bugs_test.go:TestDecodeInt64Key_ShortKey_ReturnsSentinel`

## NOTE-008: Span Identity Fields in Intrinsic Section Only (Not in Block Columns)
*Added: 2026-03-25*

*Addendum (2026-03-25): Original entry claimed dual-storage (block columns AND intrinsic
section). That was incorrect. Span identity fields are written ONLY to the intrinsic TOC
section; `addPresent` calls for these columns were removed from all write paths.*

**Decision:** Span identity fields (trace:id, span:id, span:parent_id, span:name, span:kind,
span:start, span:duration, span:status, span:status_message, resource.service.name) are
stored exclusively in the intrinsic TOC section. `Block.GetColumn()` returns nil for
these names — this is expected and handled by the executor's `nilIntrinsicScan` mechanism.

**For consumers needing these fields:** Use `Reader.GetIntrinsicColumn(name)` or the
executor's `lookupIntrinsicFields` helper. Do NOT call `Block.GetColumn()` for intrinsic
field names and expect a non-nil result.

**Back-ref:** `internal/modules/blockio/writer/writer_block.go:newBlockBuilder`,
`internal/modules/executor/column_provider.go:nilIntrinsicScan`,
`internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`,
`internal/modules/executor/executor.go:SpanMatchFromRow`

---

## NOTE-009: FileLayout() V12-Only Simplification and Full-Byte-Detail Enhancement
*Added: 2026-03-31*

**Decision:** Remove V10/V11 code paths from `layoutMetadata()`. The codebase writes only
V12 files (snappy-compressed metadata); V10/V11 paths were dead code that complicated the
section model and prevented adding logical sub-sections for V12 metadata components.

**Changes bundled in this decision:**
1. **V12-only metadata:** `layoutMetadata()` emits one physical `metadata.compressed`
   section plus logical sub-sections for range index columns. The old uncompressed
   `metadata.block_index`, `metadata.column_index`, `metadata.trace_index` per-column
   `metadata.range_index.column[*]` sections are removed.
2. **Intrinsic paged breakdown:** For paged (v2) intrinsic columns, emit one physical
   `intrinsic.column[name].page[N]` section per page plus a `intrinsic.column[name].page_toc`
   section for the TOC header bytes, instead of a single aggregate section. Each page section
   carries `RowCount`, `MinValue`, and `MaxValue` from its `PageMeta`.
3. **Sketch actual bytes:** `SketchIndexInfo.EstimatedBytes` replaced by `TotalBytes`
   (actual computed uncompressed sketch section size) and `HeaderBytes` (fixed 12 bytes).
   `ColumnSketchStat` gains `CMSBytes` and `TopKBytes` for per-entry byte accounting.
4. **KLL bucket boundaries:** `RangeIndexColumn` gains `BucketMin`/`BucketMax` (global
   min/max from wire format). `RangeIndexBucket` gains `End` (upper boundary: next
   bucket's Start for interior buckets, BucketMax for the last bucket).
5. **FileBloom logical section:** `FileLayoutReport.FileBloom` (*FileBloomInfo) describes
   the FBLM section: total bytes and per-column name + fuse filter size. Logical — not
   a physical section, so no impact on the byte invariant.

**Byte invariant preserved:** All new sections that describe content inside
`metadata.compressed` or `intrinsic.toc` use `IsLogical: true`. The invariant
`sum(physical CompressedSize) == FileSize` continues to hold.

**Back-ref:** `internal/modules/blockio/reader/layout.go:FileLayout`,
`internal/modules/blockio/reader/layout.go:layoutMetadata`,
`internal/modules/blockio/reader/layout.go:buildSketchIndexInfo`,
`internal/modules/blockio/reader/layout.go:buildRangeIndex`,
`internal/modules/blockio/reader/layout.go:buildFileBloomInfo`

---

## NOTE-010: CMS Removal — skipColumnCMS Zero-Alloc Backward Compat for SKTC/SKTD Files
*Added: 2026-04-02*

**Decision:** Remove CMS data from the sketch parse path. Legacy SKTC (`0x534B5443`) and
SKTD (`0x534B5444`) files are still readable by `parseSketchIndexSection` via the
`skipColumnCMS` helper, which reads and discards the CMS bytes with zero allocations.

**Rationale:**
- CMS contributed ~70% of sketch section size and caused OOM during compaction at scale.
- Keeping the skip path (rather than rejecting old files outright) avoids a forced re-write
  of all existing blockpack files when upgrading to SKTE. Legacy files remain queryable.

**skipColumnCMS mechanics:**
- Reads `cms_depth` (uint8) and `cms_width` (uint16 LE) from the file header.
- Computes skip distance: `cms_depth × cms_width × 2 × presentCount` bytes.
- Advances `pos` without allocating any heap objects.
- Handles any depth/width values in old files; not limited to CMSDepth=4/CMSWidth=64 defaults.

**`fileSketchSummaryMagic` bump:**
- Changed from `0x46534B54` ("FSKT") to `0x46534B55` ("FSKU").
- Invalidates any externally cached `FileSketchSummary` blobs that embedded CMS data.
- Old-magic summaries return a bad-magic error on unmarshal (safe rejection, no silent corruption).

**Back-ref:**
- `internal/modules/blockio/reader/sketch_index.go:skipColumnCMS`
- `internal/modules/blockio/reader/sketch_index.go:parseSketchIndexSection`
- `internal/modules/blockio/shared/constants.go:fileSketchSummaryMagic`

---

## NOTE-011: V5 Footer Detection and VectorIndex Lazy Load (2026-04-02)
*Added: 2026-04-02*

**Decision:** The `readFooter()` method attempts V5 (46 bytes) detection before V4 (34 bytes).
If `fileSize >= 46`, it reads a single 46-byte buffer from `fileSize-46`. The first two bytes
determine the version:
- `buf[0:2] == 5 (FooterV5Version)`: parse V5; extract `vectorIndexOffset` and `vectorIndexLen`.
- `buf[12:14] == 4 (FooterV4Version)`: V4 footer is embedded at offset 12 of the V5 buffer; parse V4 from that slice with no extra I/O.

`VectorIndex()` and `VectorIndexRaw()` are lazy: `vectorIndexOffset`/`vectorIndexLen` are stored at footer-parse time but the section bytes are NOT fetched. The section is read on first call to `VectorIndex()` or `VectorIndexRaw()`, guarded by `vectorIndexOnce`. For V3/V4 files, both methods return `nil, nil` immediately.

**Rationale:** The vector index section can be large (codebook ≈ 768 KB for 768-dim PQ + PQ codes 96 bytes/vector). Eager loading for every file open would inflate memory on readers that never issue semantic queries. Lazy loading ensures non-vector query paths pay zero vector I/O cost. The single-buffer V5/V4 detection preserves the 3-I/O budget (`TestLeanReader_ThreeIO`) — V4 files large enough to trigger the V5 read pay no extra I/O penalty.

**Consequence:** Writers with `VectorDimension > 0` emit a V5 footer. Writers with `VectorDimension == 0` continue to emit V4 footers with no behavioral change.

Back-ref: `internal/modules/blockio/reader/parser.go:readFooter`,
`internal/modules/blockio/reader/reader.go:VectorIndex`,
`internal/modules/blockio/reader/reader.go:VectorIndexRaw`,
`internal/modules/blockio/reader/vector_index.go:parseVectorIndexSection`

---

## NOTE-012: V7 Footer (FooterV7Version=7) — V14 Section Directory (2026-04-10)
*Added: 2026-04-10*

**Decision:** Redesign the write/read path for V14 format using a new FooterV7 (18 bytes):
1. Replace the single snappy-compressed metadata blob with a section directory footer.
   Footer wire format: `magic[4]+version[2]=7+dir_offset[8]+dir_len[4]` = 18 bytes.
2. Add `readSectionDirectory` to decode the snappy-compressed section directory into
   `map[uint8]shared.DirEntryType` and `map[string]shared.DirEntryName` for O(1) lookup.
   File-level intrinsic columns use name-keyed entries (`DirEntryName`); fixed sections
   use type-keyed entries (`DirEntryType`). No separate intrinsic TOC read needed.
3. Replace `parseV5MetadataLazy` (single blob) with `parseSectionsLazy`: each section is
   read on demand via `ReadAt(entry.Offset, entry.CompressedLen)` + `snappy.Decode`.
4. Remove `zstdDec`, `decompScratchPool`, and `decompressZstdScratch` from `column.go`.
5. Add snappy decode per column in the block-parse path: each column blob is
   `snappy.Decode(compressedBlob)` before parsing encoding-specific bytes.

**Version number rationale:** Agentic uses FooterV5Version=5 (46-byte vector footer) and
FooterV6Version=6 (58-byte compact-traces footer). Our section-directory footer uses version 7
to avoid the collision with both agentic footer versions.

**Why section directory instead of footer offsets:**
V3/V4/V5/V6 footers stored fixed offset fields for specific sections (header_offset,
compact_offset, intrinsic_offset, vector_offset, etc.). Adding a new section required a footer
format bump. The V7 section directory is extensible: new section types are added by writing a
new entry without any footer wire-format change. Reader code performs a map lookup by
section type rather than accessing a named struct field.

**Why per-section lazy parse:**
The block index section must be read eagerly (needed to populate `r.blockMetas` for all
subsequent operations). All other sections are used only conditionally:
- Range index: only when a range predicate needs pruning.
- Trace index: only for `GetTraceByID`/`TraceEntries`.
- TS index: only for time-range pruning.
- Sketch index: only for TopK/HLL queries.
- File bloom: only for bloom filter checks at query start.


Lazy parse means opening a reader costs one `ReadAt` (footer) + one `ReadAt`+decode
(section directory) + one `ReadAt`+decode (block index) + one `ReadAt` per intrinsic
column name-keyed entry (to peek format/type/count via `PeekIntrinsicBlobHeader`,
populating `IntrinsicColMeta` for executor fast-path dispatch). All other sections incur
I/O only when their data is actually needed.

**Why remove decompScratchPool:**
The scratch pool held output buffers for zstd decompression. With zstd removed from
column parsing entirely (all internal sub-segments are now raw bytes), there is no
decompression within the column decode path. The outer snappy decode per column uses
`snappy.Decode(nil, compressedBlob)`, which allocates its own output buffer — a single
allocation per column decode, retained for the duration of the block scan. No pool is
needed for this allocation pattern; it is too short-lived to benefit from pooling and the
allocation count is proportional to `wantColumns` (typically small).

**Why keep NewLeanReaderFromProvider:**
`NewLeanReaderFromProvider` still exists and is retained for the trace-ID lookup path.
For V14 files it delegates directly to `parseSectionsLazyV14` (same as
`NewReaderFromProvider`), so both entry points are functionally equivalent on V14 files.
For legacy V3/V4 files, `NewLeanReaderFromProvider` continues its original optimized path:
reading only the footer and compact trace index, falling back to `NewReaderFromProvider`
when no compact index is present.

In V14 the section directory makes every section independently addressable, so the
distinction between "lean" and "full" reader is less meaningful — but
`NewLeanReaderFromProvider` is preserved for API compatibility.


**Alternatives considered:**
- *Keep decompScratchPool, use it for snappy decode output*: Rejected — column decode
  output buffers are referenced by `rawEncoding` in `Column` structs and outlive the
  decode call.
- *Store section directory at file start (offset 0)*: Rejected — blocks are written at
  offset 0 and there is no fixed-size prefix before block data. The footer-pointer pattern
  is already established.

Back-ref: `internal/modules/blockio/reader/parser.go:readFooter`,
`internal/modules/blockio/reader/parser.go:readSectionDirectory`,
`internal/modules/blockio/reader/parser.go:parseSectionsLazy`,
`internal/modules/blockio/reader/column.go` (decompScratchPool removed),
`internal/modules/blockio/reader/columnar_read.go` (snappy decode per column),
`internal/modules/blockio/reader/reader.go:NewReaderFromProvider`

---

## NOTE-013: V14 Two-Phase Trace Index Loading
*Added: 2026-04-12*

**Problem:** `ensureV14TraceSection` previously called `readV14Section(SectionTraceIndex)` on
every `BlocksForTraceID` call, reading and decompressing the entire ~50 MB trace section per
block. With 59 blocks, a single FindTraceByID lookup incurred ~3 GB of reads — even with disk
cache, this took ~6 seconds.

**Solution:** Apply the same two-phase approach that V3/V4 lean readers already use:

- **Phase 1 (eager, ~KB):** `ensureV14TraceSection` calls `readV14Section`, passes the result
  to `splitV14CompactSection` to extract just the header (magic + version + block_count +
  bloom + block_table), and stores it via `parseCompactIndexBytesV14Header`. The header bytes
  are cached under `fileID+"/v14/compact-header"`.

- **Phase 2 (lazy, ~50 MB, only on bloom hit):** `ensureTraceIndexRaw` detects
  `compactParsed.isV14TraceSection == true` and re-reads the full V14 section, calls
  `splitV14CompactSection` again to extract the trace index bytes, caches them under
  `fileID+"/compact-trace-index"`, and stores them in `compactParsed.traceIndexRaw`.

**Why re-read the full section in phase 2 instead of caching just the trace index part:**
The V14 section is snappy-compressed as a single blob; there is no direct file offset for
the trace index portion. The full blob must be fetched to decompress, then split. The
`readV14Section` cache (`fileID+"/v14/sec/03/dec"`) already holds the decompressed blob
for full readers; for lean readers the re-read on bloom hit is the correct trade-off.

**Key functions:**
- `splitV14CompactSection(data)` — stateless splitter; returns header and trace-index
  sub-slices with no copy.
- `parseCompactIndexBytesV14Header(header)` — parses header into `compactTraceIndex` with
  `isV14TraceSection: true` and `traceIndexRaw: nil`.
- `ensureV14TraceSection` — rewritten to use two-phase path.
- `ensureTraceIndexRaw` — extended with V14 branch guarded by `isV14TraceSection`.

**`parseCompactIndexBytesV14` unchanged:** The full-parse function (used by full readers
and tests) remains intact. The new header-only path is additive.

**`isV14TraceSection` field on `compactTraceIndex`:** Signals which fetch strategy
`ensureTraceIndexRaw` should use. V3/V4 lean readers set `traceIndexOffset`/`traceIndexLen`
and leave `isV14TraceSection: false`. V14 lean readers set `isV14TraceSection: true` and
leave those fields zero.

Back-ref: `internal/modules/blockio/reader/trace_index.go:splitV14CompactSection`,
`internal/modules/blockio/reader/trace_index.go:parseCompactIndexBytesV14Header`,
`internal/modules/blockio/reader/trace_index.go:ensureTraceIndexRaw`,
`internal/modules/blockio/reader/parser.go:ensureV14TraceSection`,
`internal/modules/blockio/reader/reader.go:compactTraceIndex`

---

## NOTE-PERF-TS: Raw Byte Storage for TS Index — Zero-Alloc Parse
*Added: 2026-04-14*

**Decision:** The parsed TS index is stored as a raw `[]byte` sub-slice of the metadata
buffer rather than a materialized `[]tsIndexEntry` slice. `BlocksInTimeRange` scans the
20-byte-stride buffer in-place (minTS[8] + maxTS[8] + blockID[4] per entry).

**Rationale:** Parsing the TS index into a `[]tsIndexEntry` at open time would allocate
one large slice of N structs plus one entry per block. For a file with 10,000 blocks,
this is eliminated entirely by the raw-byte approach. The in-place scan is O(N) sequential
memory reads, which is cache-friendly and avoids any per-entry allocation.

**How to apply:** Do NOT add a parsed `[]tsIndexEntry` field to the Reader or parser state.
If new consumers of the TS index need per-entry access, extend `BlocksInTimeRange` or add
a parallel scan function that reads from the raw byte slice.

Back-ref: `internal/modules/blockio/reader/ts_index.go:parseTSIndex`,
          `internal/modules/blockio/reader/ts_index.go:BlocksInTimeRange`,
          `internal/modules/blockio/reader/reader.go:tsRaw`

---

## NOTE-PERF-1: Sparse Dict Columns — Deferred Dense Expansion
*Added: 2026-04-14*

**Decision:** Sparse dict columns (encoding kinds 2 and 7 RLE) defer the O(spanCount)
`expandSparseIndexes` allocation until the column is first accessed. The raw sparse index
bytes are stored in `sparseDictIdx` at decode time; `expandDenseIdx` runs at most once
(via `sync.Once`) on the first value access.

**Rationale:** Many queries decode columns for predicate evaluation but then exit early
(e.g., the block does not match). If the dense `Idx` expansion ran eagerly at decode time,
the O(spanCount) allocation would occur even for blocks where no row is ever read.

**How to apply:** When adding new column encoding types with a sparse/dense split, follow
the same pattern: store the raw sparse data at decode time, expand lazily at first access
using `sync.Once`. Do NOT call `expandDenseIdx` from the decode path.

Back-ref: `internal/modules/blockio/reader/block.go:expandDenseIdx`,
          `internal/modules/blockio/reader/block_parser.go:sparseDictIdx`,
          `internal/modules/blockio/reader/column.go:decodeDictKind2Sparse`

---

## NOTE-PERF-RANGE: Raw Byte Storage for Range Index Float64 Bounds
*Added: 2026-04-14*

**Decision:** Float64 range bounds in the range index are stored as a raw `[]byte` sub-slice
of the metadata buffer (`float64BoundsRaw`). `RangeColumnBoundaries` decodes them on demand
rather than materializing a `[]float64` slice at parse time.

**Rationale:** Float64 bounds are only needed when a query contains a float64 range predicate.
For files with many columns and mostly non-float64 queries, materializing all float64 bounds
eagerly would waste O(numBuckets × numColumns) memory. The zero-copy approach keeps parse
time and memory constant regardless of how many float64 columns exist.

**How to apply:** Do NOT eagerly decode float64 bounds in `parseRangeIndex`. Any new
per-bucket bound type should follow the same deferred-decode pattern.

Back-ref: `internal/modules/blockio/reader/range_index.go:float64BoundsRaw`,
          `internal/modules/blockio/reader/reader.go:RangeColumnBoundaries`

---

## NOTE-PERF-COMPACT: Raw Byte Storage for Compact Trace Index
*Added: 2026-04-14*

**Decision:** The compact trace index is stored as raw bytes (`traceIndexRaw`) in the
Reader rather than parsed into a `map[[16]byte][]uint16` at load time. Lookups scan the
sorted trace ID table in-place via binary search.

**Rationale:** Parsing the compact trace index into a Go map would allocate one entry
plus the `[]uint16` block list per trace. For a file with 100,000 distinct traces, this
is ~100,000 heap allocations on every open call. The raw-byte approach defers all
allocation to lookup time, allocating only for the matched trace's block list.

**How to apply:** Do NOT add a parsed map for the compact trace index. New readers should
extend `BlocksForTraceIDCompact` or `TraceEntries` to scan from the raw bytes.

Back-ref: `internal/modules/blockio/reader/trace_index.go:BlocksForTraceIDCompact`,
          `internal/modules/blockio/reader/trace_index.go:TraceEntries`,
          `internal/modules/blockio/reader/reader.go:traceIndexRaw`

---

## NOTE-PERF-SKETCH: Zero-Copy Distinct Count Storage in Sketch Index
*Added: 2026-04-14*

**Decision:** Per-block distinct counts in the sketch index are stored as raw 4-byte-per-block
LE uint32s in `distinctRaw` (a zero-copy sub-slice of the metadata buffer). `Distinct()`
decodes on demand, and `distinctAt(i)` provides single-block access.

**Rationale:** Materializing `make([]uint32, numBlocks)` per column at parse time adds
O(numColumns × numBlocks) allocation to every reader open. For a file with 500 columns
and 1,000 blocks, this is 500,000 uint32 values (~2 MB) that may never be read. The
zero-copy approach stores only a slice header per column at parse time.

**How to apply:** Do NOT allocate a `[]uint32` slice for distinct counts in `parseSketchIndex`.
If adding new per-block numeric arrays, follow the same zero-copy sub-slice pattern.

Back-ref: `internal/modules/blockio/reader/sketch_index.go:distinctRaw`,
          `internal/modules/blockio/reader/sketch_index.go:Distinct`,
          `internal/modules/blockio/reader/sketch_index.go:distinctAt`

---

## NOTE-014: V14 Phase-1 traceIndexRaw Pre-Population (Cache-Hit Fix)
*Added: 2026-04-14*

**Problem:** In `ensureV14TraceSection`, `traceIdxBytes` was captured inside the
`GetOrFetch` closure, which only runs on a cache miss. On a cache hit, the closure
was skipped, `traceIdxBytes` remained nil, and `r.compactParsed.traceIndexRaw` was
never set. Consequently, `ensureTraceIndexRaw` (phase 2) always issued a second
`readV14Section` call, even though the decompressed blob was already in the section
cache (`fileID+"/v14/sec/03/dec"`).

**Decision:** After `GetOrFetch` returns, check whether `traceIdxBytes` is nil (cache
hit path). If so, call `readV14Section(SectionTraceIndex)` and run
`splitV14CompactSection` again. Because the decompressed blob is already held by the
section cache, this incurs zero provider I/O — it is a pure in-memory slice. The
resulting trace index bytes are stored in `r.compactParsed.traceIndexRaw`.

**Rationale:** Eliminates one redundant cache lookup per bloom-hit lookup on warm readers.
The fix stays inside the `v14TraceOnce.Do` block so it runs at most once per Reader, and
errors from the re-read are silently swallowed (phase 2 will retry normally if needed).

**Consequence:** `SPEC-010a` updated — `traceIndexRaw` may be non-nil after phase 1.
`ensureTraceIndexRaw` already short-circuits when `traceIndexRaw != nil`, so phase 2
is a no-op for bloom hits after a warm phase 1.

Back-ref: `internal/modules/blockio/reader/parser.go:ensureV14TraceSection`

**Superseded by NOTE-015 (warm-hit eviction guard):** The Decision above describes an intermediate
design. The shipped implementation uses `r.cache.Get` instead of `readV14Section` on the warm-hit
path, avoiding provider I/O when the section blob has been LRU-evicted. See NOTE-015.

---

## NOTE-015: ensureV14TraceSection Cache-Eviction Guard
*Added: 2026-04-14*

**Problem:** The warm-hit path in `ensureV14TraceSection` (when the compact-header cache
entry exists but the GetOrFetch closure did not run, leaving `traceIdxBytes` nil)
unconditionally called `r.readV14Section(SectionTraceIndex)`. The comment claimed "zero
provider I/O" — true only while the large ~50 MB decompressed blob
(`fileID+"/v14/sec/03/dec"`) remained in the in-memory cache. Under LRU pressure the
blob can be evicted, causing `readV14Section` to re-issue a ~50 MB network read. A
second problem: `traceIndexRaw` was pre-populated on every phase-1 call, even for
bloom-miss queries where the trace index is never accessed, holding ~50 MB per Reader.

**Decision:** Replace the unconditional `readV14Section` call with `r.cache.Get`. If the
section blob is hot in the cache, re-split it in-memory (zero I/O). If it was evicted,
leave `traceIndexRaw` nil — `ensureTraceIndexRaw` handles the nil case and does the
fetch on the first bloom hit. The guard `r.fileID != ""` skips the lookup for NopCache
readers (which always run the GetOrFetch closure and never reach this branch), and also
defensively skips any reader with an empty fileID — which should not occur in production
but would otherwise produce a lookup against the NopCache or an incorrect cache key.

**Rationale:** Phase 1 may opportunistically pre-populate `traceIndexRaw` when the
section blob is already in-memory, but must not issue provider I/O or allocate large
buffers when the blob is absent. This keeps the two-phase design valid under eviction.

**Consequence:** SPEC-010a updated — `traceIndexRaw` is only pre-populated when the
section blob is in the in-memory cache. Bloom-hit paths always work because
`ensureTraceIndexRaw` handles the nil case. `ensureV14TraceSection` never issues a
provider read on the warm-hit path.

Back-ref: `internal/modules/blockio/reader/parser.go:ensureV14TraceSection`

---

## NOTE-016: parseSectionsLazyV14 — Deferred Intrinsic Blob Reads (2026-04-16)
*Added: 2026-04-16*

**Problem:** `parseSectionsLazyV14` previously called `r.cache.GetOrFetch` for every
name-keyed intrinsic column blob to peek `Format/Type/Count` from each blob header at
reader-open time. With N intrinsic columns per file and M blockpack files per Tempo shard
query, this produced N×M GCS reads just to open readers — even for metrics queries that
only need one or two columns (e.g. `span:start` for `{} | rate()`). On a cold cache with
107 blocks × ~10 columns per block, this was ~1070 GCS reads before any useful work.

**Decision:** Remove the eager blob reads from `parseSectionsLazyV14`. The intrinsic index
is now populated with only `Name/Offset/Length` from the section directory (zero I/O).
`Format/Type/Count` remain zero until the first `IntrinsicColumnMeta(name)` call for that
column, which triggers the existing lazy-peek path (one blob read per column, cached).

Alongside this, `HasIntrinsicColumn(name string) bool` was added to `Reader` as a
pure map-lookup that never triggers any I/O. `metricsColumnsAreIntrinsic` now calls
`HasIntrinsicColumn` instead of `IntrinsicColumnMeta` to check column existence without
issuing a blob read per column per file.

**Effect on I/O at open time:** Reader open for a V14 file is now exactly 3 reads:
footer + section directory + block_index. All other sections (intrinsic blobs, trace
index, range index, TS index, sketch index, file bloom) are deferred to first access.

**Effect on predicate evaluation:** `predicates.go` calls `IntrinsicColumnMeta` which
still triggers the lazy peek on first access per column. The blob is immediately cached
in `r.cache` (same key as `GetIntrinsicColumnBlob`), so subsequent `GetIntrinsicColumnBlob`
calls for the same column-file pair are cache hits.

**Consequence:** The reader_test.go `TestLeanReader_ThreeIO` assertion was tightened from
`>= 3` to `== 3` since reader open is now deterministically 3 reads for V14 files.

Back-ref: `internal/modules/blockio/reader/parser.go:parseSectionsLazyV14`,
`internal/modules/blockio/reader/intrinsic_reader.go:HasIntrinsicColumn`

---

## NOTE-V8-003: V8 selective fetch and per-column I/O

**Context:** V8 files use per-column range/sketch blobs. `ensureRangeColumnParsed` and
`ColumnSketch` detect `footerVersion == FooterV8Version` and call `fetchToCSection` with
`ToCKey{ToCTypeMetadata, ToCSubTypeRange/Sketch, colName}`. Only the requested column's
blob is read — no other column's data is fetched.

**fetchToCSection caching:** Cache keys use null-byte separators
(`fileID\x00v8\x00type\x00subtype\x00name`) because OTEL attribute names can contain
slashes but not null bytes. This avoids cache key collisions between columns like
`"http/method"` and `"http"` + `"/method"`.

**Footer detection:** `tryReadFooterMagic18` reads the last 18 bytes once, shared between
V8 (version=8) and V7 (version=7) detection. This preserves the `TestLeanReader_ThreeIO`
invariant (exactly 3 I/Os: footer+dir+block_index for V7 files).

**V8 sketch concurrency:** `ColumnSketch` acquires `sketchIdxMu` (sync.Mutex) to guard
concurrent per-column fetch+store. V14 uses `sync.Once` inside `ensureV14SketchSection`
which serializes all sketch accesses. V8 uses a per-Reader mutex to allow concurrent
fetches of different columns while preventing double-write.

**Intrinsic access (V8):** `parseSectionsV8` populates `r.intrinsicIndex` from
`ToCSubTypeIntrinsic` ToCEntries at open time (zero I/O — offsets only). The existing
`GetIntrinsicColumnBlob` path reads `r.intrinsicIndex[name].Offset/Length` and calls
`r.provider.ReadAt` then `snappy.Decode` — unchanged for V8.

Back-ref: `internal/modules/blockio/reader/parser.go:parseSectionsV8`,
          `internal/modules/blockio/reader/parser.go:fetchToCSection`,
          `internal/modules/blockio/reader/parser.go:tryReadFooterMagic18`,
          `internal/modules/blockio/reader/range_index.go:ensureRangeColumnParsed`,
          `internal/modules/blockio/reader/range_index.go:parseRangeColumnBlobV8`,
          `internal/modules/blockio/reader/sketch_index.go:parseOneColumnSketchBlob`,
          `internal/modules/blockio/reader/reader.go:ColumnSketch`

---

## NOTE-022-reader: DistinctAt, TopKMatchAt, FuseContainsAt — Zero-Allocation Sketch Accessors
*Added: 2026-05-15*

**Decision:** Add `DistinctAt`, `TopKMatchAt`, and `FuseContainsAt` to `columnSketchData` to
satisfy the updated `queryplanner.ColumnSketch` interface (NOTE-022 in queryplanner/NOTES.md).

- `DistinctAt` delegates to the existing private `distinctAt` method (reads `distinctRaw` directly).
- `TopKMatchAt` scans `topkFP[presentIdx]` for the block's entry — O(K) where K ≤ 20.
- `FuseContainsAt` scans `presentMap` to find the block's bloom slice and calls `sketch.BloomContains`.

Both `TopKMatchAt` and `FuseContainsAt` use an early-exit `break` on `bIdx > blockIdx` because
`presentMap` is always sorted ascending (built in blockIdx order by `parseColumnPresence`).

**Back-ref:** `internal/modules/blockio/reader/sketch_index.go:DistinctAt,TopKMatchAt,FuseContainsAt`

---

## NOTE-153: pool the WantOnly lazy-column arena (lazyColumnStore) — 2026-06-09

**Decision:** `parseBlockColumnsReuse` registers every *non-wanted* column of a block into a
per-block `[]Column` arena (`lazyColumnStore`, NOTE-002) so a caller *could* later lazily decode
one. On the WantOnly path this arena is `make([]Column, 0, len(metas))` on every parse. A querier
`alloc_space` profile (2026-06-09, dev-test-03) showed this single allocation as the largest
allocator on the metrics block-scan path — **~21% / 29 GB**, `block_parser.go` line for the
`make([]Column…)` — dominated by attribute group-by queries (e.g. M6
`{span.kind=server} | rate() by (span.http.request.method)`), whose group-by column is a V14
block column (not intrinsic) so they go through `ParseBlockFromBytes` + `traceAccumulateRow`.

The arena's entries are pure overhead for that path: the two-pass WantOnly scan only ever touches
*wanted* columns (predicate pass → predicateCols, output pass → outputCols), plus a possible lazy
`span:start` on v3 files which is decoded **inside** the row loop. So the arena can be returned to
a `sync.Pool` once the block is fully scanned.

**Mechanism:**
- `lazyColumnStorePool` (`sync.Pool` of `*[]Column`) + `acquireLazyColumnStore(n)` (len 0, cap ≥ n).
- `Block.lazyStorePtr` holds the pool handle; nil when the arena was not pooled (WantAll path
  registers no lazy columns, so `wantColumns == nil` skips the block entirely).
- `(*Block).ReleaseLazyColumnStore()` zeroes every `Column` in the arena before `Put`, so the pool
  retains no reference to `rawBytes` (via `compressedEncoding` sub-slices) nor to any
  lazily-decoded dict/idx slices. Idempotent and nil-safe.
- Callers: `metrics_trace.go` releases after each block in both the single-pass (`predicateCols==nil`)
  and two-pass paths (first/predicate block after `releaseBlockColumnProvider`; second/output block
  after the accumulate loop; rejected `Size()==0` and error paths too).

**Safety:** release happens only after the block is fully consumed and after the column provider
(which references the block's eager columns) is released. Other WantOnly callers that do not release
simply fall back to `Pool.New` (a fresh alloc) — no correctness dependency on releasing.

**Verification:** `BenchmarkParseBlockWantOnly_LazyStorePool` (200-span block): NoRelease
40.5 KB/op, 35 allocs/op → WithRelease 7.7 KB/op, 33 allocs/op (**−81% bytes/op**, ~2× faster).
`TestNOTE153_LazyStorePoolReuseCorrect` parses two files with different lazy-column values through
the WantOnly path, releases the first arena, then parses the second (reusing the pooled arena) and
asserts each decodes to its own values — catches stale `compressedEncoding`/dict aliasing across
pool reuse. Full reader + executor suites pass under `-race`.

## NOTE-154: adaptive Phase-1 ToC read — stop the full-block fallback — 2026-06-09

**Decision:** `readBlockColumnarWithCache` (SPEC-005 columnar Phase-1) read a fixed `tocHintBytes`
(4 KiB) to obtain the block header + column-metadata array, then parsed the metadata to learn each
wanted column's `(dataOffset, compressedLen)` for the targeted Phase-2 reads. 4 KiB covers only
~120 columns. Real OTel trace blocks carry **hundreds** of attribute columns, so for essentially
every block `parseColumnMetadataArray` returned a "short for …" truncation error and the code fell
back to `make([]byte, blockLen)` + a **full block read** (`columnar_read.go:214`). A querier
`alloc_space` profile (2026-06-09, dev-test-03) showed that single fallback `make` as the **#1
allocator: ~546 MB / 18.5%** of alloc_space — versus only ~91 MB on the proper assembled-buffer
line (`:242`), i.e. the columnar cache was being bypassed on the common path.

**Mechanism:** `readSufficientToC(blockOff, blockLen)` reads the ToC starting at `tocHintBytes` and
**grows the read geometrically** (`tocGrowthFactor = 4`) until `parseColumnMetadataArray` succeeds
or the whole block has been read, then returns that buffer. It runs *inside* the
`GetOrFetchV8Section` fetch closure, so the **correctly-sized ToC is what gets cached**: warm queries
pay one cache hit + one successful parse — no growth loop, no full-block read. The outer header- and
metadata-error fallbacks remain as final safety nets for genuine corruption (now rare).

**Safety:** byte-for-byte identical assembled buffer to before (Phase-2 logic unchanged); the only
change is that Phase 1 now reliably delivers the full metadata array. On cold miss the metadata is
parsed twice (once in the closure to size the read, once by the caller) — CPU-only, negligible, and
the querier is I/O-bound with 33% CPU headroom. `min(blockLen, …)` caps every read at the block
size, so small blocks still read once and the growth loop always terminates.

**Verification:** `TestNOTE154_AdaptiveToCReadsAllColumns` builds a block with enough columns to
overflow `tocHintBytes`, reads it through `ReadGroupColumnar`, and asserts the wanted columns decode
correctly (i.e. the columnar Phase-2 path is taken, not the full-block fallback) and that a
provider read-byte counter stays far below the full block size. Full reader + executor suites pass
under `-race`.

## NOTE-170: single-block inline fast path in ReadGroupColumnarCached — 2026-06-10

**Decision:** `ReadGroupColumnarCached` (SPEC-005 columnar read) always set up parallel
machinery — a `results` slice, a `sync.WaitGroup`, a `runtime.NumCPU()`-sized semaphore
channel, and one goroutine per block — before reading the wanted column bytes for each block
in `cr.BlockIDs`. But query-frontend shards **one block per querier call** (confirmed
2026-06-09: `blockGroupPipeline` always receives a single group, its worker concurrency never
fires), so `cr.BlockIDs` almost always has length 1. For that dominant case the parallel setup
is pure per-read overhead — a goroutine spawn, a channel send/recv pair, and a `wg.Wait` futex —
on the **universal** block-read path hit by every query (Q1–Q10, M1–M9). A 2026-06-10 querier
CPU profile showed kernel scheduler/lock cost (`_raw_spin_lock` + `queued_spin_lock_slowpath`
≈ 6% combined, plus `runtime.futex`/`startm`/`wakep` traffic) as a top non-blockpack sink,
consistent with per-block goroutine churn.

**Mechanism:** when `len(cr.BlockIDs) == 1`, read that single block inline on the calling
goroutine via `readBlockColumnarWithCache` and return `map[int][]byte{blockIdx: data}`. No
goroutine, no channel, no WaitGroup, no results slice. The multi-block parallel path is
unchanged and still used whenever a coalesced read genuinely spans multiple blocks.

**Safety:** identical error semantics — `readBlockColumnarWithCache` returns an error (it does
not panic on bad input); the per-goroutine `recover()` in the parallel path existed only to keep
sibling blocks' results from being lost when one block panicked, which is moot with a single
block (a genuine panic now propagates up the call stack exactly as it would have when only one
block was being read). Output map shape is byte-for-byte identical to the parallel path's output
for a one-element `cr.BlockIDs`.

## NOTE-173: coalesced Phase-2 cold column reads in readBlockColumnarWithCache — 2026-06-10

**Decision:** `readBlockColumnarWithCache` Phase 2 (SPEC-005) issued one `r.provider.ReadAt`
per wanted column that missed the section cache — i.e. one ranged backend (S3) GET per cold
column. A heavy metrics query touches dozens of small columns across hundreds of cold blocks,
so cold blocks produced dozens of tiny reads each. A querier CPU profile (2026-06-10,
dev-test-03) is dominated by *connection establishment*, not transferred bytes: kernel
`__inet_hash_connect` (~6.5%), `__inet_check_established` (~12.6%), `tcp_twsk_unique`, plus TLS
handshake crypto (`bigmod`/`mlkem`/`edwards25519`/`gcmAesDec`) and `Syscall6` (~19%). That is
the per-read round-trip / connection cost — the classic "fewer fetches, not cheaper compute"
lever the prior status.log entries kept identifying as the only thing that moves wall-clock.

**Mechanism:** `planColdRuns` gathers the `(start,end)` ranges of every wanted column whose
data lies past the already-cached ToC, sorts them by offset, and merges ranges separated by at
most `colCoalesceMaxGap` (64 KiB) unwanted bytes into a small set of coalesced runs. Each run is
read from the provider exactly once, lazily, on the first cold-miss fetch closure that lands in
it (`coldRuns.ensure`); every column in that run then slices its compressed blob out of the
shared buffer. The cap stops two distant columns from pulling a huge span of unrelated data.

**Safety / cache semantics unchanged:** per-column section-cache granularity is preserved — the
`GetOrFetchV8Section` key is still `blockIdx/colName`, so warm queries still pay one cache hit
per column and **never** trigger a coalesced provider read (a fully-warm block issues zero
reads, exactly as before). Only the cold path is affected: N small `ReadAt`s become a handful of
larger ones. The assembled buffer is byte-for-byte identical — each column's bytes still land at
their original absolute offset. Sorting the ranges makes run construction independent of the
metadata array's column ordering, so `ensure` always finds a run that fully covers a column.

**Verification:** existing reader + executor suites pass under `-race`, including
`TestReadGroupColumnar`, `TestReadGroup_IOFailure`, and `TestNOTE154_AdaptiveToCReadsAllColumns`
(which asserts the columnar Phase-2 path is taken and provider read-bytes stay far below the full
block size — still true: coalescing reads the wanted-column span plus small gaps, not the block).
`make precommit` fully green (gofumpt, golines, golangci-lint incl gocyclo + fieldalignment,
deadcode, staticcheck).

## NOTE-177: concurrent Phase-2 column fetches in readBlockColumnarWithCache — 2026-06-10

**Problem:** Phase 2 of `readBlockColumnarWithCache` resolved each wanted column serially via
its own `GetOrFetchV8Section(blockIdx/colName)`. On the warm steady-state path every column is
a cache hit, but each hit is an independent round-trip (the querier CPU profile attributes ~21%
to `MemCache.Get` and a similar slice to `FileCache` disk read+copy). A heavy metrics query
(e.g. `{...} | rate() by (...)`) touches many small columns per block, so these N round-trips
stack their latency end-to-end. query-frontend shards one block per querier call, so the
block-level pipeline never parallelises this — the serial per-column fetch is the warm-path
wall-clock cost, not per-row decode/compute (the recurring status.log conclusion: VOLUME/RTT,
not cheaper compute, is what moves wall-clock).

**Mechanism:** the wanted columns are collected once, then fetched concurrently with a bounded
worker fan-out (`min(numCols, NumCPU)`). Each column writes its compressed blob into a DISJOINT
region of the pre-allocated `assembled` buffer — column extents never overlap — so the per-column
work is embarrassingly parallel and data-race-free. Fanning the N cache round-trips out collapses
their stacked latency into ~1 RTT of wall-clock per block. A single (or zero) wanted column skips
the fan-out machinery and resolves inline (`fetchColumnInto` with a nil runMu).

**Safety:** the cold path (`coldRuns.ensure`) lazily populates shared run buffers and is therefore
serialized under `runMu`; both the `ensure` call and the subsequent `copy` out of `rn.buf` happen
inside the locked region of the fetch closure, so no cold buffer is read before it is fully
populated. Warm queries never reach the cold path, so `runMu` is uncontended in steady state. The
section-cache key (`blockIdx/colName`) and per-column granularity are unchanged, and the assembled
buffer is byte-for-byte identical to the serial version (each column lands at its absolute offset).

**Verification:** reader + executor suites pass under `-race` (including `TestReadGroupColumnar`,
`TestReadGroup_IOFailure`, `TestNOTE154_AdaptiveToCReadsAllColumns`). The disjoint-region writes
and per-column `cp` allocations give the race detector nothing to flag.

## NOTE-179: batched per-column section fetch (GetMultiV8Section) — 2026-06-10

**Problem:** Phase 2 of `readBlockColumnarWithCache` resolved each wanted column through its
own `GetOrFetchV8Section(blockIdx/colName)`. NOTE-177 fanned those out concurrently to collapse
their stacked round-trip latency, but a fresh querier CPU profile (gcx, 30m, quiet cluster)
showed the dominant cost was NOT per-row compute (all blockpack scan/decode frames < 0.65%
self-CPU) — it was `gomemcache.(*Client).dial` at **28.5% cumulative** (`getConn` 28.8%, with
`getFreeConn` only 0.08%). The concurrent per-column fan-out issued N simultaneous memcache Gets;
each needs its own connection, and the burst exhausted the idle pool so almost every Get *dialed*
a fresh connection (plus the kernel `__inet_check_established` / `__inet_hash_connect` / TCP
TIME-WAIT-reuse churn that dominated the rest of the profile). The fan-out optimised latency but
multiplied the connection-establishment CPU that is the real warm-path bottleneck.

**Mechanism:** when the section cache implements `sectionBatchFetcher`, all wanted columns for a
block are fetched in ONE `GetMultiV8Section` call. `gomemcache.GetMulti` groups the (hashed) keys
per server and pipelines them over a SINGLE connection per server, collapsing N connection
acquisitions into one while keeping the single-RTT latency the fan-out provided.
`tieredcache.TypedTieredCache.GetMultiV8Section` routes the batch to the sub-cache for the
section type; `chaincache.ChainedCache.GetMulti` probes the fast in-process / local-disk tiers
per key (no connection cost) and batches the remaining misses into the memcache tier's
`MemCache.GetMulti`. Columns that miss the batch are read from the cached ToC or a coalesced
cold run (NOTE-173), written back via `PutV8Section`, and copied into `assembled` — rare on the
warm steady-state path. The assembled buffer is byte-for-byte identical to the fan-out version
(each column lands at its absolute offset). When the cache lacks batch support the code falls
back to the NOTE-177 concurrent fan-out unchanged.

**Safety:** `fetchColumnsBatched` runs on a single goroutine, so the cold path (`coldRuns.ensure`,
which mutates shared run buffers) needs no lock. All returned blobs are independent copies
(MemCache copies on the way out; the cold/ToC fallback copies into a fresh `cp`), so no buffer
aliases the cache. Per-key hit/miss accounting at the batch tier is approximate (the section
metrics are coarse counters); correctness is unaffected.

**Verification:** reader + executor suites pass under `-race`. New unit tests cover
`MemCache.GetMulti` (hits/misses/copy-safety/transient-error-as-miss/nil-receiver),
`ChainedCache.GetMulti` (single batched round-trip to the batch tier + per-key fallback +
writeback + no input mutation), and `TypedTieredCache.GetMultiV8Section` (batch hit + PutV8Section
writeback + no-batch-support fallback).

---

## NOTE-185: Combined ToC + Columns Single Round-Trip on the Warm Block Read
*Added: 2026-06-11*

**Problem:** Even after NOTE-179 collapsed the per-column fan-out into one `GetMultiV8Section`,
each warm block read still paid TWO sequential memcache round-trips: Phase-1 fetched the block's
ToC via `GetOrFetchV8Section` (round-trip #1), and only *then* — once the column offsets were
known — Phase-2 batched the columns via `GetMultiV8Section` (round-trip #2). query-frontend shards
ONE block per querier call (NOTE-170), so a heavy metrics query over hundreds of blocks pays
hundreds of *extra* sequential round-trips that exist purely because the column fetch was thought
to depend on the ToC. The dominant querier cost on the warm path is round-trip count, not bytes
or compute (NOTE-173/179 profiling: `(*Client).dial`, TLS/TCP setup, `Syscall6`).

**Key insight:** a wanted column's section key is `blockIdx/<column-name>`, and the caller already
holds every wanted column NAME (the `wantColumns` set) *before* the ToC is decoded. The ToC is only
needed to learn each column's byte OFFSET — i.e. *where* to place an already-fetched blob in the
assembled buffer, not *which* keys to request. So the ToC key and all wanted column keys can be
requested together in ONE pipelined `GetMulti`.

**Mechanism:** `readBlockColumnarWithCache` first calls `fetchTocAndColumnsCombined`, which issues a
single `GetMultiV8SectionMixed` for the ToC key (tocType `sectionTypeBlockToc`) plus one key per
wanted column (tocType `sectionTypeBlockCol`). All these keys route to the same `toc` sub-cache, so
one `MemCache.GetMulti` over a single pipelined connection serves them. The ToC blob from the result
drives header/metadata parsing exactly as before; column blobs that hit are copied straight into
their disjoint region of `assembled` and excluded from Phase-2's `cols` list, so the warm path issues
NO second memcache round-trip. `shared.V8SectionKey` (in the shared package, to avoid a
reader→tieredcache import cycle) carries the per-key `(TocType, SubType, Name)` routing;
`TypedTieredCache.GetMultiV8SectionMixed` batches keys spanning multiple tocTypes that all route to
one tier.

**Fallbacks (byte-identical):** if the ToC misses the combined batch we cannot place columns, so we
fall back to the existing Phase-1 `GetOrFetchV8Section` (which also resolves a cold ToC) and Phase-2
column resolution unchanged. Individual column misses simply remain in `cols` and are resolved by the
existing batched / inline / fan-out paths (cold runs + writeback). A blob whose length does not match
the ToC-reported `compressedLen` is treated as a miss (defensive). When the cache implements neither
`sectionMixedFetcher` nor `sectionBatchFetcher` the original two-phase code runs verbatim. Every blob
copied from the combined batch is the cache's own copy (`MemCache` copies on the way out), so no buffer
aliases the cache, and the assembled bytes are byte-for-byte identical to the two-phase output.

**Verification:** reader + tieredcache + executor suites pass under `-race`. New
`TestReader_CombinedTocColumnFetch_WarmIdentical` builds a multi-column block, reads it cold (populating
the cache), then reads it warm and asserts (a) ZERO provider I/O on the warm read — proving the combined
batch fully satisfied both phases — and (b) the warm assembled bytes are byte-identical to the cold
two-phase bytes and decode each wanted column correctly.

Back-ref: `internal/modules/blockio/reader/columnar_read.go:readBlockColumnarWithCache`,
`internal/modules/blockio/reader/columnar_read.go:fetchTocAndColumnsCombined`,
`internal/modules/tieredcache/typed.go:GetMultiV8SectionMixed`,
`internal/modules/blockio/shared/v8sectionkey.go:V8SectionKey`

## NOTE-187: Lazily Plan Cold Runs Only When a Cold Miss Exists
*Added: 2026-06-11*

**Problem:** `readBlockColumnarWithCache` called `r.planColdRuns(metas, wantColumns, …)`
*eagerly* on every block read, immediately after sizing the assembled buffer and BEFORE the
`cols` (cold-miss) list was built. `planColdRuns` does an O(wanted-columns) scan, allocates a
`ranges` slice, and runs a `slices.SortFunc` over it to coalesce the columns that extend past the
cached ToC into a few merged byte ranges. But after NOTE-185 the warm steady-state path — the
production state we optimise for — satisfies *every* wanted column from the combined ToC+columns
`GetMulti`, so the `cols` list comes out **empty** and the resulting `runs` plan is never consulted:
the single/zero-column branch iterates an empty `cols`, and the batched branch is only reached when
`len(cols) > 1`. The cold-run plan was pure wasted CPU + GC pressure on the universal warm hot path
(query-frontend shards one block per querier call, NOTE-170, so this runs once per block per query
over hundreds of blocks).

**Fix:** move the `planColdRuns` call to *after* the `cols` list is built and gate it behind
`len(cols) > 0`. `runs` is declared as a nil `coldRuns` (a slice type, so nil is a valid empty plan)
and only populated when there is at least one genuine cold miss to resolve. On the warm path `cols`
is empty, so `runs` stays nil and neither `fetchColumnInto` nor `fetchColumnsBatched` is ever invoked
with it — the plan is never built. On the cold path `len(cols) > 0` guarantees `runs` is the exact
same coalesced plan as before (computed over the full `metas`/`wantColumns`, unchanged), so the
round-trip-collapsing coalesce of NOTE-173 still applies identically.

**Correctness:** `runs` is consumed only by `fetchColumnInto` / `fetchColumnsBatched`, both of which
are reached only inside `for _, m := range cols { … }` (single/zero-column branch) or under
`len(cols) > 1` (batched branch) — every consumption site is dominated by `len(cols) > 0`, so a nil
`runs` is never dereferenced. The assembled bytes are byte-for-byte identical to the eager version.

**Verification:** `go build ./...` clean; reader suite green
(`TestReader_CombinedTocColumnFetch_WarmIdentical` exercises the warm zero-cold-miss path and the
cold multi-column path); executor suite green under `-race`.

Back-ref: `internal/modules/blockio/reader/columnar_read.go:readBlockColumnarWithCache`,
`internal/modules/blockio/reader/columnar_read.go:planColdRuns`

## NOTE-188: Index-Aligned Column Re-Keying in fetchTocAndColumnsCombined
*Added: 2026-06-11*

**Problem:** `fetchTocAndColumnsCombined` built a `colName` map (V8SectionKey -> bare column
name) solely to translate the combined ToC+columns batch response back into the `colHits`
(name -> blob) map the caller probes by `m.name`. This combined batch runs once per block per
query on the warm read path (NOTE-185: query-frontend shards one block per querier call), so the
per-block `colName` map allocation + hashing is wasted GC pressure on the universal hot path.

**Fix:** the request slice is built deterministically as `[toc, col0, col1, ...]`, so keeping a
parallel `colNames []string` index-aligned with `reqs[1:]` lets the response be re-keyed by bare
name via `hits[reqs[i+1]]` with no reverse map. The ToC is `hits[reqs[0]]` directly (dropping the
separately-reconstructed `tocReq`). A single small slice replaces a map; one allocation removed
per warm block read.

**Correctness:** `reqs[i+1]` is the column request whose name is `colNames[i]` by construction
(both appended in lockstep in the same loop), so the (request, name) pairing is exact. Columns
that miss the batch are absent from `hits` and skipped, identical to the old `isCol` guard. The
returned `colHits` map is byte-identical to before. ToC-miss / batch-unsupported fallbacks
unchanged (still return nil,nil so the caller's two-phase path resolves them).

**Verification:** `go build ./...` clean; reader suite green under `-race`
(`TestReader_CombinedTocColumnFetch_WarmIdentical` exercises warm + cold paths).

Back-ref: `internal/modules/blockio/reader/columnar_read.go:fetchTocAndColumnsCombined`

## NOTE-189: Fast Per-Column Section Name Build — No Throwaway fmt.Sprintf
*Added: 2026-06-11*

**Problem:** the warm block read built each wanted column's section name with
`fmt.Sprintf("%d/%s", blockIdx, name)` at three sites (`fetchTocAndColumnsCombined`,
`fetchColumnsBatched`, `fetchColumnInto`) and the ToC key with `fmt.Sprintf("%d", blockIdx)`.
This intermediate "blockIdx/name" string is only an input to the full V8 cache key built
downstream (see NOTE-189 in tieredcache), so it is a pure throwaway allocation — and it is
built once per wanted column per block per query on the universal warm path. `fmt.Sprintf`
also boxes `blockIdx` into `interface{}` and runs the format scanner.

**Fix:** `strconv.Itoa(blockIdx)` once per block (reused across the column loop), and a
trivial `colSectionName(blockIdxStr, name) = blockIdxStr + "/" + name` concatenation per
column. `fetchTocAndColumnsCombined` now takes the precomputed `blockIdxStr` directly.

**Correctness:** `strconv.Itoa` matches `%d` for the non-negative `blockIdx`; the "/" and
the resulting "blockIdx/name" string are byte-identical to the prior `fmt.Sprintf`, so the
cache keys (and warm hits) are unchanged. fmt is still used for error wrapping elsewhere.

**Verification:** `go build ./...` clean; reader + tieredcache + executor suites green
under `-race`.

Back-ref: `internal/modules/blockio/reader/columnar_read.go:colSectionName`,
`readBlockColumnarWithCache`, `fetchTocAndColumnsCombined`, `fetchColumnsBatched`,
`fetchColumnInto`.

## NOTE-197 — PrefetchIntrinsicColumns warms the per-file intrinsic working set in one round-trip

`PrefetchIntrinsicColumns(names)` batch-fetches the named intrinsic column blobs via the
optional `intrinsicBatchFetcher` (TypedTieredCache.GetMultiIntrinsic), decodes each hit, and
populates both the per-Reader `intrinsicDecoded` map and the process-level `parsedIntrinsicCache`.
Subsequent `GetIntrinsicColumn` calls for those names then return from the per-Reader cache with
zero memcache traffic, and `GetIntrinsicColumnBlob` (raw-scan predicate path) hits the in-process
chaincache tier the batch wrote back into. Names not present in the file or already decoded are
skipped; names that miss the batch fall through to the normal per-name path. Decode/cache errors
on an individual column are ignored here (the per-name path surfaces them on access). NOT safe
for concurrent use with GetIntrinsicColumn on the same Reader (callers invoke it synchronously
on the same goroutine that then scans).

**Verification:** reader + tieredcache + executor suites green under `-race`.

Back-ref: `internal/modules/blockio/reader/intrinsic_reader.go:PrefetchIntrinsicColumns`,
`internal/modules/tieredcache/typed.go:GetMultiIntrinsic`,
`internal/modules/executor/metrics_trace_intrinsic.go:prefetchIntrinsicWorkingSet`.

## NOTE-199 — PrefetchIntrinsicColumns consults the process cache before fetch + decode

The NOTE-197 prefetch collected its `want` set by skipping only columns already in the
per-Reader `intrinsicDecoded` map. But a Reader is created fresh per query (per block per
querier call, NOTE-170/185), so that map is always empty at prefetch time — even when a
prior query's Reader on the same file already decoded the working-set columns into the
strong-reference process-level `parsedIntrinsicCache`. The result was a redundant
`GetMultiIntrinsic` round-trip AND a redundant `DecodeIntrinsicColumnBlob` on every warm
query for columns that were already decoded process-wide. For high-cardinality group-by
dicts (the dominant cost of `rate()`/`histogram_over_time` by a large attribute column —
`DecodeIntrinsicColumnBlob` is ~27% of querier alloc_space) this re-decoded the single most
expensive column on every request.

The fix consults `parsedIntrinsicCache.Get` during `want` collection: a process-cache hit
hydrates the per-Reader `intrinsicDecoded` map directly and drops the name from `want`, so
the column is neither re-fetched nor re-decoded; the subsequent `GetIntrinsicColumn` returns
the shared decoded value. The collection loop takes the write lock (not the read lock) since
it now mutates `intrinsicDecoded`; this is safe because `PrefetchIntrinsicColumns` is invoked
synchronously on the scanning goroutine and is documented as not concurrency-safe with
`GetIntrinsicColumn` on the same Reader. The duplicate `useProcessCache` local further down
the function is hoisted to the collection loop. Byte-identical results: process-cache values
are immutable decoded columns; a miss falls through to the unchanged batch path.

**Verification:** reader + tieredcache + executor suites green under `-race`; `go build ./...`.

Back-ref: `internal/modules/blockio/reader/intrinsic_reader.go:PrefetchIntrinsicColumns`.

## NOTE-200 — Process-level cache of decoded V8 block columns
*Added: 2026-06-11*

**Problem:** NOTE-199 eliminated redundant re-decode of *intrinsic* columns on the warm
path by consulting the process-level `parsedIntrinsicCache`. The analogous V8 *block*
columns had no such cache. A Reader is created fresh per query (per block per querier call,
NOTE-170/185), and `parseBlockColumnsReuse` re-ran the full per-column decode —
snappy decompress + `readColumnEncoding` (dict/idx build, RLE/delta page expansion, radix
sort) — for every wanted block column on every warm query, even when a prior query already
decoded the exact same column from the exact same on-disk block. A 2026-06-11 querier CPU
profile attributed ~8% of self-time to this decode path (`decodeDictPagesArena`,
`appendDeltaUint64Page`, `appendVariableWidthRefs`, snappy decode, `radixSortRefIndex`),
re-paid on every request that scans block-level attribute columns.

**Fix:** add `parsedV8ColumnCache objectcache.Cache[Column]`, keyed by
`fileID + "/v8col/" + blockOffset + "/" + colName + "/" + colType`. The block's byte offset
within the file (`meta.Offset`) uniquely identifies an immutable block (files are immutable
once written; compaction creates new fileIDs), so the key resolves to exactly one decoded
column. In `parseBlockColumnsReuse`'s eager-decode loop:
- a cache HIT copies the immutable decoded slices into the per-query `Column` via
  `copyDecodedColumnInto` and skips decompress + decode entirely;
- a cache MISS decodes as before, then stores an immutable snapshot
  (`snapshotDecodedColumn`) sharing the freshly-decoded slices.

`fileID` is threaded into `parseBlockColumnsReuse` from the two `Reader` parse entry points
(`ParseBlockFromBytes`, `ParseBlockFromBytesWithIntern`); the cache is bypassed when
`fileID == ""` (test callers, mirroring the other process caches). `SetIntrinsicCacheBytes`
sizes it on the same budget as the intrinsic cache, and `ClearCaches` clears it.

**Correctness:** the snapshot holds only the immutable decoded slices (`StringDict`/`Idx`,
`Int64Dict`/`Idx`, …, `Present`, `sparseDictIdx`). These are never mutated in place: the
per-query `Column` keeps its own zero-valued `sync.Once`/`atomic.Bool`, so the lazy dense
expansion of NOTE-PERF-1 (`expandDenseIdx`) runs per query, reads the shared `sparseDictIdx`
read-only, and assigns a *new* dense `Idx` slice on the per-query column — it never writes
into the shared snapshot. Interned strings are independently heap-allocated and safe to
share across queries. The production parse path always passes `prevBlock == nil`, so the
column-reuse (`resetColumn`) branch is never reached with a cached snapshot. `Column.SizeBytes`
estimates the snapshot's footprint for LRU budgeting.

**Verification:** `go build ./...` clean; reader + executor suites green under `-race`. New
`TestParsedV8ColumnCache_WarmEqualsCold` asserts a warm parse hits the cache (no new entries)
and produces per-row values byte-identical to a cold decode across all four block-column
types; `TestParsedV8ColumnCache_DifferentFileIDs` asserts cross-file isolation.

Back-ref: `internal/modules/blockio/reader/parser.go:parsedV8ColumnCache`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`v8ColumnCacheKey`, `snapshotDecodedColumn`, `copyDecodedColumnInto`,
`internal/modules/blockio/reader/block.go:Column.SizeBytes`.

## NOTE-201 — Extend V8 decoded-column cache to the lazy (deferred) decode path
*Added: 2026-06-11*

**Problem:** NOTE-200 cached decoded V8 block columns, but only the *eager* loop in
`parseBlockColumnsReuse` consulted and populated `parsedV8ColumnCache`. Columns NOT in the
query's `wantColumns` set are registered lazily (NOTE-001) with `compressedEncoding` pointing
into `rawBytes` and no immediate decode; their snappy decompress + `readColumnEncoding` is
deferred to first access via `Column.decodeNow`. That deferred decode never touched the cache,
so any column reached lazily on the warm path — e.g. a predicate-filtered block column outside
the eager want set, or a column accessed only during row emission — was re-decoded from scratch
on every warm query, exactly the redundant-decode cost NOTE-200 eliminated for the eager path.

**Fix:** carry the same `v8ColumnCacheKey` on the lazy `Column` and have `decodeNow` use it:
- the lazy-registration loop computes `v8CacheKey` (empty when `fileID == ""`) and stores it on
  the registered `Column` alongside `compressedEncoding`;
- `decodeNow` consults `parsedV8ColumnCache` on `v8CacheKey` *before* `ensureDecompressed`; on a
  HIT it copies the immutable decoded slices into the per-query column and skips decompress +
  decode entirely; on a MISS it decodes as before, then stores a snapshot
  (`snapshotDecodedColumn`, with the column's own `Present`/`SpanCount`).
- `resetColumn` clears `v8CacheKey` so a reused `Column` (the `prevBlock` column-reuse branch)
  never carries a stale key.

**Correctness:** identical sharing model to NOTE-200 — the snapshot holds only immutable decoded
slices; the per-query column keeps its own zero-valued `denseOnce`/`sparseDictIdx`, so
NOTE-PERF-1 dense expansion runs per query and never mutates the shared snapshot. The cache
copy runs inside `decodeOnce.Do`, preserving the single-decode guarantee for concurrent lazy
accessors (NOTE-CONC-001). The lazy `Column` literal leaves `Present == nil`, so the
`if c.Present == nil` copy matches the original `decodeNow` semantics. Lazy columns sourced from
the `lazyColumnStorePool` are zeroed on release and freshly re-initialised per parse via the
struct literal, so `v8CacheKey` is never stale on the pooled path.

**Verification:** `go build ./...` clean; reader + executor suites green under `-race`. New
`TestParsedV8ColumnCache_LazyWarmEqualsCold` parses with an eager want set that excludes two
columns, forces their lazy decode via `EnsureDecoded`, asserts the cache grows (lazy MISS
populated it), then asserts a warm reader's lazy access HITS the cache (no new entries) and
yields per-row values byte-identical to the cold lazy decode.

Back-ref: `internal/modules/blockio/reader/column.go:Column.decodeNow`,
`Column.v8CacheKey`, `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`
(lazy-registration loop), `resetColumn`.

## NOTE-208: pool the assembled columnar read buffer — 2026-06-11

**Decision:** `readBlockColumnarWithCache` (SPEC-005 columnar Phase-2) builds an "assembled"
sparse buffer with `make([]byte, bufSize)` — one per block per query, sized to span the ToC
prefix plus the furthest wanted column's extent (often most of the block). It is the largest
single allocation on the warm metrics block-scan path (M-queries), and a 2026-06-11 querier
CPU profile showed heavy GC/runtime traffic (`runtime.growslice`/`roundupsize`/`wbBufFlush`/
`findObject`, futex/scheduler churn) consistent with churning these large buffers each block.
Prior alloc profiles also attributed the columnar assembled-buffer line as a top allocator.

**Mechanism:** `assembledBufPool` (`sync.Pool` of `*[]byte`) recycles the backing arrays.
- `acquireAssembledBuffer(n)` returns a slice of exactly length `n` from a pooled array of
  sufficient capacity, else a fresh `make`. The returned bytes are **not zeroed**.
- `(*Reader).ReleaseRawBuffer(buf)` returns the backing array to the pool once the block
  parsed from it is fully consumed. Buffers larger than `assembledBufMaxPooledCap` (16 MiB)
  are dropped so a rare oversized block cannot pin a huge array in the pool.
- Callers: `metrics_trace.go` and `metrics_log.go` release after each block is fully scanned
  (single-pass success, two-pass success, and predicate-reject early-exit). Other callers
  that do not release simply let the buffer be GC'd — no correctness dependency on releasing
  (identical contract to NOTE-153's `ReleaseLazyColumnStore`).

**Safety (why a dirty reused buffer is correct):** `parseBlockColumnsReuse` reads bytes ONLY
inside (a) the ToC prefix `[0, tocEnd)` and (b) each WANTED column's exact
`[dataOffset, dataOffset+compressedLen)` extent — both of which `readBlockColumnarWithCache`
fully overwrites via `copy` before returning. It never reads the gaps between columns, so
stale bytes left there by a prior block are never observed. The original `make` zero-filled
the gaps, but the gaps are dead — that zeroing was pure overhead.

**Lifetime:** the buffer is sub-sliced zero-copy into `Column.compressedEncoding` (lazy
decode), so it must stay live until the block is fully consumed — the same lifetime point as
NOTE-153's lazyColumnStore. In the trace two-pass path the buffer is shared by both passes
(`ParseBlockFromBytes(bwb.RawBytes, …)`), so release happens only after both passes complete
and after `ReleaseLazyColumnStore` has zeroed every `compressedEncoding` reference.

Back-ref: `columnar_read.go:readBlockColumnarWithCache` (assembled buffer),
`metrics_trace.go` / `metrics_log.go` (release call sites), NOTE-153 (parallel lifetime hook).

## NOTE-209: pool the lazy-column decompression buffer — 2026-06-11

**Decision:** A lazily-registered V14 column (SPEC-V14-002) defers snappy decompression to
first access via `Column.ensureDecompressed`, which called `decompressV14ColumnData` →
`snappy.Decode(nil, data)`. That allocated a **fresh** decompressed buffer per lazy column
decode, stored it in `c.rawEncoding`, and `decodeNow` nilled it after `readColumnEncoding`.
This fires on every warm query that touches a deferred column (predicate-filtered block
columns not in the eager wantColumns set, high-cardinality intrinsic group-bys, search
second-pass output columns). A 2026-06-11 querier CPU profile showed `runtime.memmove` (2.84%)
and `runtime.memclrNoHeapPointers` (2.37%) high on the warm path — the eager parse path
(`parseBlockColumnsReuse`) already reuses a pooled `decompBuf` across columns, but the lazy
path did not, so its per-column decompress was pure GC churn.

**Mechanism:** `ensureDecompressed` now draws the decompression destination from the existing
`decompBufPool` via `decompressV14ColumnDataInto((*bp)[:0], …)` (the same pooled-grow helper
the eager path uses), stores the pool handle in a new `Column.decompPooledPtr` field, and
`decodeNow` calls `releaseDecompPooled()` after `readColumnEncoding` to return the backing
array to the pool. On a decompression error the handle is `Put` back immediately so a failed
decode never leaks a pooled buffer.

**Safety (why recycling is correct):** identical to the eager-path invariant documented in
`parseBlockColumnsReuse`: every column decoder (`decodeDictionary`, `decodeInlineBytes`,
`decodeXORBytes`, …) **copies all decoded data out** — `Present` bitmap, `Dict` values, `Idx`
arrays, and `BytesInline`/`BytesDict` are fresh `make`+`copy`d slices that never alias the
input `data`. So once `readColumnEncoding` returns, `rawEncoding` (which aliases the pooled
buffer) is dead and the buffer can be recycled. `releaseDecompPooled` clears `rawEncoding`
before/with the `Put` so the alias is never read after recycling.

**Concurrency:** `decompPooledPtr` is written exactly once, inside `decompressOnce.Do`
(happens-before all callers via `sync.Once`), and read/cleared exactly once, inside the
single goroutine that wins `decodeOnce.Do`. `decodeNow` always runs `ensureDecompressed()`
(which blocks until decompression completes) before entering `decodeOnce.Do`, so the field is
fully visible. `decompPooledPtr != nil` implies `rawEncoding != nil` (it is only set on the
success branch), so the early `rawEncoding == nil` decode branch has nothing to release.
Verified race-free under `go test -race` (incl. the concurrent-IsPresent stress test, x5).

Back-ref: `column.go:ensureDecompressed`/`decodeNow`/`releaseDecompPooled`,
`block_parser.go:decompressV14ColumnDataInto` (pooled-grow helper) + the "decoders copy data
out" invariant comment, NOTE-208 (assembled-buffer pool, sibling alloc reduction).

## NOTE-212: skip the assembled-buffer copy for already-decoded columns — 2026-06-11

**Decision:** On the warm columnar read path, `readBlockColumnarWithCache` fetched every
wanted column's *compressed* blob from the memcache section cache (`preHits`) and
`copy`-ed it into the per-block assembled buffer at the column's absolute ToC offset, so
`parseBlockColumnsReuse` could read `rawBytes[start:end]`. But that parser FIRST consults
the process-level `parsedV8ColumnCache` (NOTE-200), which holds the fully-DECODED column
snapshot keyed by `(fileID, blockOffset, name, type)`. On the fully-warm steady state that
decoded cache hits for the wanted columns, so the parser `continue`s and NEVER reads the
compressed bytes — making the per-column `copy` into the assembled buffer pure dead work.
On a wide metrics query (e.g. `rate() by (...)`, predicate-filtered `rate by`) that warm-path
per-column memmove (plus the assembled-buffer alloc/churn it feeds) is the dominant remaining
warm CPU/GC cost — prior CPU profiles attributed `runtime.memmove` + `runtime.memclrNoHeapPointers`
~5-7% combined and the cache/assembled-buffer path as the top sink (status.log carries this as
the standing "reduce decode/copy VOLUME, not per-row compute" target across NOTE-208/209/210).

**Mechanism:** In the assembled-buffer fill loop, before copying a wanted column's `preHits`
blob, probe `parsedV8ColumnCache.Get(v8ColumnCacheKey(fileID, blockOff, name, type))`. On a
hit, stash the **live** snapshot pointer in a new per-Reader `preDecodedColumns`
map (keyed by `preDecodedKey{blockOffset,name,colType}`) and `continue` — skipping both the
memcache→assembled copy AND queuing the column for cold resolution. `parseBlockColumnsReuse`
gains a `preDecodedLookup func(preDecodedKey) *Column` parameter (built by both
`ParseBlockFromBytes` and `ParseBlockFromBytesWithIntern` via `r.preDecodedLookup()`); it
checks that lookup FIRST, before the existing `parsedV8ColumnCache.Get`, and
`copyDecodedColumnInto`s the snapshot when present.

**Concurrency:** `ReadGroupColumnar` (which populates `preDecodedColumns`) is called from
`blockGroupPipeline` worker goroutines CONCURRENTLY on the same `*Reader`, while the parse runs
on the sequential consumer goroutine — and a read of group N+1 can run while group N is parsed.
A `preDecodedMu sync.Mutex` guards every map access (the stash write and the lookup read).
`preDecodedLookup()` returns `nil` when the map is empty so the common path (no pre-resolved
columns) pays no lock or per-column call; the lookup closure itself locks per call but the map
is tiny and accessed at most once per wanted column per block, so it is effectively uncontended
off the heavy fan-out path. (In production query-frontend shards 1 block/querier-call, so the
fan-out and thus contention never actually fire — but the lock keeps the general multi-group
path race-free under `go test -race`.)

**Safety (why this is race-free, not a probe-then-skip TOCTOU):** the naive form — reader
probes the cache, skips the copy, parser re-probes — opens an eviction race: the snapshot can
be LRU-evicted between the two probes, leaving the parser to decompress stale assembled-buffer
bytes (garbage → snappy error or, worse, wrong data). We close that window by storing the LIVE
`*Column` the reader observed. `objectcache.Cache.Get` returns a strong pointer; the Reader
(one per querier call) retains it in `preDecodedColumns` for its whole lifetime, so the parse
consumes the exact snapshot the reader saw — independent of any concurrent LRU eviction. The
key uses the column's TRUE `(name, type)` because the cache/parser key on type and one block
can carry the same name with different types. `blockOff` (from `cr.BlockOffsets[j]`) equals
`r.BlockMeta(blockIdx).Offset` (both flow from `metas[bi].Offset` via `CoalesceBlocks`), so the
reader's stash key and the parser's lookup key are identical. The assembled buffer still always
holds the ToC prefix (header + metadata) the parser reads to locate columns; only the
already-decoded column DATA extents are left unwritten, and those are never read.

**Verified:** `TestReader_PreDecodedColumns_SkipCopyStillCorrect` warms both caches, confirms a
fresh Reader pre-resolves every wanted column (`preDecodedColumns` fully populated), then
CORRUPTS every column-data byte of the assembled buffer (restoring only the ToC prefix) and
asserts the parse still returns the cold ground-truth per-row values — proving the skipped
extents are never read and the snapshots drive the result. `go test -race` green for
`./blockio/reader` + `./executor`, NOTE-200 warm==cold and NOTE-185 combined-fetch tests
still pass.

Back-ref: `columnar_read.go:readBlockColumnarWithCache` (the skip), `reader.go:Reader.preDecodedColumns`,
`block_parser.go:parseBlockColumnsReuse` (`preDecoded` param + `preDecodedKey`), NOTE-200
(decoded-column cache this reads), NOTE-208 (assembled-buffer pool this avoids touching),
NOTE-185 (combined ToC+columns fetch whose copy this elides).

## NOTE-213: size the assembled buffer to span only NON-pre-decoded columns — 2026-06-11

**Decision:** `readBlockColumnarWithCache` previously computed the assembled-buffer size
(`bufSize`) in a first pass over `metas` that spanned the furthest WANTED column, then ran a
SECOND pass (NOTE-212) that skipped columns already present in `parsedV8ColumnCache`. On the
fully-warm wide path (e.g. `rate() by (...)`, predicate-filtered `rate by` — M4/M9) every wanted
column's decoded snapshot is cached, so the parser never reads any column extent — yet the
buffer was still sized to the furthest wanted column (often most of the block), acquired from
the NOTE-208 pool, and had only its ToC prefix written into it. That over-sized acquire is the
dominant remaining assembled-buffer allocation/copy cost the standing "reduce copy/decode
VOLUME" target (NOTE-208/209/210/212) points at.

**Mechanism:** fold the NOTE-212 skip detection into the sizing pass. A single loop over `metas`
now: validates the column extent against `blockLen` (full-block fallback unchanged), calls
`stashPreDecodedColumn` and `continue`s on a decoded-cache hit (excluding the column from both
the buffer size and the copy list), and otherwise appends to `keepCols` and grows `bufSize` to
that column's end. The buffer is then acquired at this reduced `bufSize`; on the fully-warm path
where every wanted column is pre-decoded, `bufSize == tocEnd` and only the ToC prefix is
allocated/copied. The Phase-2 copy loop iterates `keepCols` (no second `stashPreDecodedColumn`
call, no redundant `wantColumns`/extent re-check) and `cols` is the subset that also missed the
combined GetMulti.

**Correctness:** `fetchColumnInto` writes only `assembled[colStart:colEnd]` for a column drawn
from `cols ⊆ keepCols`, and `bufSize` spans every `keepCols` extent, so no cold-path write ever
lands beyond the (possibly shrunk) buffer. `planColdRuns` still plans over the full
`metas`/`wantColumns`; a pre-decoded column is by definition a decoded-cache hit, which on a
COLD block never happens, so on the cold path `keepCols` equals the full wanted set and the
buffer/plan are byte-for-byte identical to before — the fold only changes the warm path where
there are no cold misses. The buffer is never zeroed; the parser reads only the ToC prefix and
each kept column extent, all fully overwritten (NOTE-208 safety contract preserved).

**Verified:** `go test -race ./blockio/reader ./executor` green, incl.
`TestReader_PreDecodedColumns_SkipCopyStillCorrect` (corrupts every column-data byte, asserts
the parse still returns cold ground-truth), `TestParsedV8ColumnCache_WarmEqualsCold`,
`TestReader_CombinedTocColumnFetch_WarmIdentical`.

Back-ref: `columnar_read.go:readBlockColumnarWithCache`, NOTE-212 (the per-column skip this
folds into sizing), NOTE-208 (the pool whose acquire this shrinks), NOTE-185 (combined fetch).

## NOTE-214: prune already-decoded columns from the combined fetch via a cached name->type map — 2026-06-11

**Decision:** The combined ToC+columns GetMulti (NOTE-185) requested the compressed blob of
EVERY wanted column, but on the warm path many of those columns already have a decoded snapshot
in `parsedV8ColumnCache` (NOTE-200) and the fetched blob is discarded — NOTE-212/213 skip copying
it into the assembled buffer. That blob fetch is pure wasted memcache `Get` traffic (the
`MemCache.Get` CPU sink the standing target — carried as the NEXT TARGET across NOTE-212/213 —
points at). A blob can only be probed against `parsedV8ColumnCache` by its `(name, type)` key, and
the type is not known until the ToC is decoded, which happens AFTER the GetMulti — so the fetch
could not be pruned upfront.

**Mechanism:** a process-level `blockColTypesCache objectcache.Cache[blockColTypes]` keyed by
`fileID/v8coltypes/blockOffset` records each block's `name -> []colType` mapping. It is populated
once per block read in `readBlockColumnarWithCache` right after `parseColumnMetadataArray` succeeds
(`cacheBlockColTypes`). Before building the combined fetch, `prunePreDecodedFromFetch` consults this
mapping: for each wanted column whose type is known AND whose decoded snapshot is present in
`parsedV8ColumnCache`, it stashes the LIVE snapshot via the existing `stashPreDecodedColumn`
mechanism and drops the column from the fetch set. The pruned set is passed to
`fetchTocAndColumnsCombined`, so the pruned columns' compressed blobs are never requested. The
sizing pass (NOTE-213) then finds them already stashed and excludes them from the buffer/copy, and
the parser serves them from `r.preDecodedColumns` (NOTE-212) without reading the now-unfetched bytes.

**Correctness:** identical to the NOTE-212 sizing-pass skip — the LIVE snapshot pointer is held on
the Reader so the parser consumes it without a re-probe (no LRU-eviction race), keyed on the TRUE
`(name, type)`. `prunePreDecodedFromFetch` clones `wantColumns` only when at least one column is
pruned (no allocation on the cold/cache-miss path) and returns it unchanged otherwise; the ORIGINAL
`wantColumns` still flows to the sizing pass and `planColdRunsLazy`, so a pruned column is detected
and skipped there exactly as a sizing-pass-skipped column would be. On the first query against a
block the colTypes cache misses, `fetchCols == wantColumns`, and everything is fetched as before —
the mapping is populated after the ToC parse for subsequent queries, so the change only affects the
warm path. `blockColTypesCache` is thread-safe (process-level) and `stashPreDecodedColumn` locks
`preDecodedMu`, so the prune is race-free across concurrent `blockGroupPipeline` workers.

**Verified:** `go test -race ./blockio/reader ./executor` green, incl. new
`TestReader_PrunePreDecodedFromFetch_NoBlobFetch` (warms the process caches, then asserts a warm
read prunes every wanted column — zero provider I/O — and returns the cold ground-truth values),
plus `TestReader_PreDecodedColumns_SkipCopyStillCorrect`, `TestParsedV8ColumnCache_WarmEqualsCold`,
`TestReader_CombinedTocColumnFetch_WarmIdentical` unchanged-green.

Back-ref: `columnar_read.go:prunePreDecodedFromFetch`/`cacheBlockColTypes`,
`parser.go:blockColTypesCache`, `block_parser.go:blockColTypesCacheKey`, NOTE-185 (the combined
fetch this prunes), NOTE-200 (the decoded cache it probes), NOTE-212/213 (the copy/sizing skip it
extends to the fetch itself).

## NOTE-AP-001: AllPresent encoding kinds — free presence on the decode path

The writer (writer NOTE-AP-001) emits AllPresent encoding kinds (15–21) for fully-present dense
columns. Each is wire-identical to its base dense kind except the
`presence_rle_len[4] + presence_rle_data` segment is omitted.

`readColumnEncoding` maps an AllPresent kind back to its base kind via `shared.BaseKindFor` and
passes an `allPresent bool` to the relevant `decode*` function. The shared
`decodePresenceMaybe(data, pos, nBits, allPresent)` helper short-circuits the presence read: when
`allPresent` is true it returns `shared.AllPresentBitset(nBits)` (an all-ones bitset) at the
unchanged position, consuming zero bytes. All downstream index/value logic is unchanged because the
base dense decoders already read a full `rowCount`-length index array independent of presence.

Old files (and files written with `DisableAllPresentEncoding`) continue to use the base kinds and
the existing `decodePresenceRLEFromSlice` path; both forms decode to identical columns.

Back-ref: `reader/column.go:readColumnEncoding`/`decodePresenceMaybe`,
          `shared/presence_rle.go:AllPresentBitset`, `shared/constants.go:BaseKindFor`,
          writer NOTE-AP-001.

## NOTE-215: decode bit-packed DeltaUint64 (kinds 22/23)

`decodeDeltaUint64BitPacked` (`column.go`) decodes the bit-packed delta variant added by writer
NOTE-215 (SPECS §9.4.1). It mirrors `decodeDeltaUint64` (kind 5) but reads a single `bit_width`
(0–64) and unpacks each present offset from an LSB-first bit stream via `readBitsLE` (the inverse
of the writer's `writeBitsLE`). `readColumnEncoding` dispatches kind 22 here; the AllPresent
variant (kind 23) maps back via `shared.BaseKindFor` and synthesizes a fully-present presence
vector with `decodePresenceMaybe` (no presence bytes read).

The packed array is always length-prefixed (`readRawSegment`), zero-length when `bit_width == 0`
(every present value equals base). The decoder validates `bit_width ≤ 64` and that the packed
payload holds at least `ceil(present_count * bit_width / 8)` bytes before unpacking.

Back-ref: `reader/column.go:decodeDeltaUint64BitPacked`/`readBitsLE`, `shared/constants.go`,
          writer NOTE-215, SPECS §9.4.1.

## NOTE-216: hoist per-entry bounds check out of fixed-width dict decode loops

Issue #330 proposed new `KindDictionaryFixed`/`KindSparseDictionaryFixed` kinds to remove
"redundant per-entry length framing" from numeric/bool dictionary payloads. On inspection the
premise did not hold: the Int64/Uint64/Float64/Bool dict payloads are **already** fixed-width and
unframed on the wire — `count[4] + N×width`, with no per-entry length prefix (only the
String/Bytes/UUID payloads carry a per-entry `len[4]`). A new fixed-width kind would therefore be
byte-for-byte identical to the existing `KindDictionary` payload for those types: zero wire
savings, a redundant kind, and a dead writer branch. We did not add the kinds.

What was genuinely improvable was the decode hot loop the issue points at (memory cbe72ea1). The
old `decodeDictBody` loop performed a per-entry bounds check (`if pos+8 > len(dictBytes)`) and a
per-entry `append` for every fixed-width slot. NOTE-216 hoists the bounds check out of the loop:
validate the full payload length once up front (`pos + entryCnt*width <= len(dictBytes)`), allocate
the destination slice at exact length, then run a tight strided read with no per-iteration branch.
The bool case collapses to a single `copy` of the contiguous entry run. The up-front check happens
before the `make`, so a corrupt/oversized `entryCnt` is rejected without over-allocating or reading
out of bounds (covered by TestDictFixedWidth_TruncatedPayloadRejected).

No wire format change, no enc_version bump, no new kind: the decode is a pure read-path refactor.
String/Bytes/UUID variable-width payloads keep their per-entry length read (genuinely variable, no
hoist possible).

Back-ref: `reader/column.go:decodeDictBody`, `reader/dict_fixed_width_test.go`, issue #330,
          memory cbe72ea1 (warm-path decode hotspot), skill 814c1630 (boundary tests).

## NOTE-217: decode uniform-length byte columns (kinds 24/25/26/27/28)

`readColumnEncoding` dispatches the uniform-length byte kinds to `decodeXORBytesUniform`
(24/25/28) and `decodeInlineBytesUniform` (26/27), the read side of writer NOTE-217 (SPECS
§9.3.1, §9.5.1). Both read a single `uniform_len[4]` after the presence segment, then slice the
packed `present_count × uniform_len` payload — no per-row length read. The XOR decoder applies
the standard XOR-against-previous reconstruction over the fixed-width slices; the Inline decoder
copies each `uniform_len`-byte slice directly. AllPresent (kind 28) maps back via
`shared.BaseKindFor`, so presence is synthesized rather than read. Sparse kinds carry no
`present_count` field — presence is taken entirely from the bitset (matching kinds 8/9).

InlineBytes uniform (26/27) is reader-only — the writer never emits the InlineBytes family — but
remains decodable for forward compatibility and any external producer.

Back-ref: `reader/column.go:decodeXORBytesUniform,decodeInlineBytesUniform`,
          `reader/layout.go:encodingKindNames`, writer NOTE-217, SPECS §9.3.1, §9.5.1.

## NOTE-218: decode per-page DeltaUint64 (kind 39)

`decodeDeltaUint64Paged` (`column.go`) decodes the per-page delta variant added by writer
NOTE-218 (SPECS §9.4.2). After the presence segment it reads `page_count[2]`, then a page index
of `page_count × (page_first_row[4] + page_base[8] + page_bit_width[1] + page_payload_bytes[4])`,
then the concatenated per-page payloads. Each page holds up to `deltaPageSizeReader` (1024) present
rows; the last page holds the remainder. The per-page row count is **not** on the wire — it is
derived from the global present-row ordering and the fixed page size, which is why
`deltaPageSizeReader` MUST equal the writer's `deltaPageSize`. Each page's offsets are unpacked
via `readBitsLE` (shared with kind 22) and rebased on that page's `page_base`.

`page_first_row` is a redundant integrity field: the decoder validates it against the actual first
present row of the page (computed from the presence bitset) and rejects a mismatch. `page_bit_width`
is validated `≤ 64` and each page's payload length is bounds-checked against
`ceil(page_rows × bit_width / 8)`. A page with `bit_width == 0` (all values equal its base) carries
no payload bytes. There is no AllPresent variant — kind 39 always carries the presence-RLE segment
— so `readColumnEncoding` dispatches it directly without `shared.BaseKindFor` remapping.

Back-ref: `reader/column.go:decodeDeltaUint64Paged`/`deltaPageSizeReader`/`readBitsLE`,
          `reader/layout.go:encodingKindNames`, `shared/constants.go` (kind 39),
          writer NOTE-218, SPECS §9.4.2.

## NOTE-219: decode Gorilla-XOR Float64 (kinds 40/41)

`decodeGorillaFloat64` (`column.go`) decodes the Gorilla-XOR float variant added by writer
NOTE-219 (SPECS §9.8). After the presence segment it reads `stream_bit_len[8]` then the
`stream_len[4] + stream_bytes` raw segment, and unpacks the bit stream via `decodeGorillaStream`
(the exact inverse of the writer loop). The decoded present values are stored as a flat
`Float64Dict` (one entry per present row) with an identity-by-present `Float64Idx` — the same
dense layout `decodeDeltaUint64BitPacked` uses for uint64, so the existing `Float64Value`
accessor works unchanged.

The stream is read LSB-first with `readBitsLE` (shared with kind 22). `stream_bit_len` bounds
every read: `decodeGorillaStream` refuses to read past it, so trailing zero padding in the final
byte is never misinterpreted as a control bit, and a truncated stream is rejected rather than read
out of bounds. New windows store `leading[5]` (clamped to 31) and `meaningful_len-1[6]`; the
decoder reconstructs `trailing = 64 - leading - meaningful_len` and validates `leading +
meaningful_len ≤ 64`. Each value is reconstructed by XOR-folding against the running predecessor
on the raw 64-bit word, so NaN payloads, ±Inf, ±0.0, and denormals round-trip exactly.

Kind 41 is the AllPresent variant: `readColumnEncoding` maps it back via `shared.BaseKindFor` and
presence is synthesized rather than read. There is no sparse variant.

Back-ref: `reader/column.go:decodeGorillaFloat64,decodeGorillaStream,readBitsLE`,
          `reader/layout.go:encodingKindNames`, `shared/constants.go` (kinds 40/41),
          writer NOTE-219, SPECS §9.8.

---

## NOTE-220 — V15 inline tiny columns (reader side)

V15 (`VersionBlockV15` = 15) keeps the V14 block header unchanged but adds a per-column `flags[1]`
byte after `col_type` in the TOC entry. When `ColFlagInline` (0x01) is set the column's raw
(un-snappy) blob is stored inline as `inline_len[1] + inline_data` directly in the TOC entry — no
`data_offset`/`compressed_len` and no data-section blob (writer NOTE-220, SPECS §12.2.1).

`parseBlockHeader` now accepts both V14 and V15. `parseColumnMetadataArray` takes the block version
and branches: for V15 it reads the flags byte, and on inline it slices `inline_data` straight out
of the passed-in TOC bytes into `colMetaEntry.inlineData`. Both `compressedLen` and `dataOffset`
are zero for an inline entry, so the "trace-level column" skip (`compressedLen == 0`) is widened to
`compressedLen == 0 && inlineData == nil` everywhere it appears (eager loop, lazy-registration
loop, `AddColumnsToBlock`).

Inline columns bypass two expensive operations: the offset chase into `rawBytes` and the per-column
snappy decompress. The eager-decode path uses `inlineData` directly as the decompressed
`readColumnEncoding` input. The lazy-registration path sets `Column.rawEncoding = m.inlineData`
directly (no `compressedEncoding`, no `v8CacheKey`) so `ensureDecompressed` is a no-op and
`decodeNow` decodes straight from the inline bytes on first access.

`readSufficientToC` grows the cold ToC read until `parseColumnMetadataArray` succeeds — which now
requires the inline bytes present — so the cached ToC always covers inline data. The columnar
assembled-buffer paths copy the full ToC prefix (`raw[:tocEnd]`, inline data included) and skip
`compressedLen == 0` columns from the per-column blob fetch, so a wanted inline column is served
from the copied prefix with zero extra fetch.

Back-ref: `reader/colmetaentry.go:inlineData`,
          `reader/block_parser.go:parseBlockHeader,parseColumnMetadataArray,parseBlockColumnsReuse`,
          `reader/reader.go:AddColumnsToBlock`, `reader/columnar_read.go:readSufficientToC`,
          `reader/column.go:ensureDecompressed,decodeNow`, `shared/constants.go` (VersionBlockV15,
          ColFlagInline, ColInlineMaxLen), writer NOTE-220, SPECS §12.2.1, NOTE-39.

## NOTE-222: PresenceView — hoist the per-row IsPresent atomic out of scan loops
*Added: 2026-06-12*

**Problem:** `Column.IsPresent(idx)` performs an atomic `decoded.Load()` (via `needsDecode`)
on EVERY call. The atomic is load-bearing for the cross-goroutine happens-before chain from
`decodeNow`'s `sync.Once` write to a reader of `c.Present` (NOTE-CONC-001). But in a scan over
`SpanCount` rows the column is decoded exactly once — on the first `IsPresent` — so every
subsequent row paid an atomic load purely to re-confirm a state that cannot change for the
duration of the scan. A querier CPU profile (2026-06-12) showed `Column.IsPresent` at ~7% of
blockpack self-time, dominated by the per-row presence checks in the executor's
`column_provider.go` stream-scan loops (dict-mask, regex, !=, is-null, is-not-null).

**Solution:** `PresenceView()` establishes the decode happens-before chain ONCE (it goes
through `needsDecode`/`decodeNow` exactly like `IsPresent`) and returns the column's stable,
immutable `Present` bitmap (`nil` = all spans present). Scan loops call it once before the
loop and then bit-test inline via `shared.IsPresent` (executor helper `presentAt`), eliminating
the per-row atomic. After `decodeNow` returns, `Present` is part of the shared decoded snapshot
and never mutates, so repeated non-atomic reads within one scan are race-free.

Semantically identical to a per-row `IsPresent`: nil bitmap → present, else the bit. Single
non-loop callsites (metrics_trace, stream_log_topk) keep `IsPresent` — there the atomic is
amortized over the whole call and hoisting buys nothing.

Back-ref: `reader/block.go:PresenceView`, `executor/column_provider.go:presentAt` and the
converted stream-scan loops.

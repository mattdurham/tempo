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

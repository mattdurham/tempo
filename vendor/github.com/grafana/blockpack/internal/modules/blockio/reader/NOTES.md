# Reader Module — Design Notes

## NOTE-001: Lazy Column Decode (Presence-First, On-Demand Full Decode)
*Added: 2026-03-05*

**Problem:** `ParseBlockFromBytes` with a `wantColumns` filter decoded only the predicate
columns eagerly, leaving all remaining columns absent from the block. Callers (executor)
had to issue a second pass via `AddColumnsToBlock` to decode those remaining columns eagerly
before the row loop — decoding ~90 columns per block even when only ~10-15 were accessed.

**Solution:** Replace the two-pass decode with a single-pass lazy model:
1. **Eager pass** — decode `wantColumns` fully (presence + values). Unchanged.
2. **Lazy registration** — for all other columns, call `decodePresenceOnly` (no zstd
   decompression) and store a `rawEncoding` sub-slice into the block's raw bytes. The
   `Column.Present` bitset is populated immediately; value data is deferred.
3. **On-demand full decode** — the first call to any value accessor (`StringValue`,
   `Uint64Value`, etc.) checks `rawEncoding != nil` and calls `decodeNow()`, which performs
   the full zstd decompression and populates all typed fields.

**`decodePresenceOnly` complexity:** O(M/8) where M is span count — scans past compressed
blob length prefixes to reach the uncompressed presence RLE. No zstd involved.

**`Column.IsPresent()` contract:** Always available after `ParseBlockFromBytes` without
triggering `decodeNow()`. Presence is decoded during lazy registration.

**Savings estimate (T9/Q66, 1997 blocks, ~90 non-predicate columns):**
- Old: 1997 × 90 × zstd_decompress ≈ 870ms
- New: 1997 × 90 × presence_only + 1997 × ~15 × zstd_decompress ≈ 40ms + 150ms = 190ms
- Expected saving: ~680ms per query

Back-ref: `internal/modules/blockio/reader/column.go:decodePresenceOnly`,
`internal/modules/blockio/reader/column.go:decodeNow`,
`internal/modules/blockio/reader/block.go:Column`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## NOTE-002: rawEncoding Lifetime Safety
*Added: 2026-03-05*

**Invariant:** `rawEncoding` is a sub-slice into `BlockWithBytes.RawBytes`. It is valid
for the lifetime of the owning `BlockWithBytes`. All lazy decodes (`decodeNow()`) complete
within the same block's row loop before `bwb` goes out of scope.

**internMap safety:** Each `ParseBlockFromBytes` and `AddColumnsToBlock` call creates its
own fresh `make(map[string]string)` intern map local to that call. Strings interned during
parsing and lazy decodes do not persist across calls. `ResetInternStrings()` is now a no-op
retained for call-site compatibility — callers still invoke it before each block, but it has
no effect. Cross-call intern reuse no longer occurs; this trade-off is accepted for
race-safety (per-call maps eliminate any shared-map data race between concurrent readers).

*Addendum (2026-03-17):* The original description (internMap borrowed from
`Reader.internStrings`, bounded by `ResetInternStrings`) reflected an earlier design that
was superseded when per-call intern maps were introduced for race-safety.

**Single-goroutine guarantee:** The scan path is single-goroutine. No locking is required
for `decodeNow()`.

Back-ref: `internal/modules/blockio/reader/column.go:decodeNow`

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

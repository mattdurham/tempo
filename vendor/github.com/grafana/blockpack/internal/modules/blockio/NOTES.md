# Blockpack blockio — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
writer and reader packages. These notes complement SPECS.md and are intended to prevent
re-introducing decisions that were deliberately reversed.

---

## 1. Object Storage I/O Model
*Added: 2026-02-10*

**Decision:** Read entire blocks in a single I/O operation. Never add per-column I/O.

**Rationale:** The target deployment is object storage (S3, GCS, Azure Blob) where request
latency is 50–100 ms regardless of payload size. Per-column selective reads were tried in
earlier versions and produced 10–120× more API calls with no meaningful bandwidth savings.
The cost model is: minimize round-trips, not bytes transferred.

**Metric thresholds:**
- `io_ops` per query: <500 good, 500–1000 warning, >1000 critical
- `bytes/io`: >100 KB good, 10–100 KB warning, <10 KB critical

**Implication for readers:** `GetBlockWithBytes` must always call `readRange` exactly once
per block. Column filtering happens in `parseBlockColumnsReuse` (in-memory), not at I/O
time. Any future optimization that adds per-column I/O will regress performance on the
target infrastructure.

---

## 2. Sort-Then-Write Span Ordering
*Added: 2026-02-10*

**Decision:** Buffer all spans during `AddSpan`/`AddTracesData`, then sort during `Flush`.
Sort key: `(service.name, MinHashSig, TraceID)`.

**Rationale:**
- **Service name** clusters spans from the same service into the same blocks, improving
  the effectiveness of the range index (service name is a common query predicate).
- **MinHash signature** clusters spans with similar attribute sets. This improves dictionary
  compression ratios (similar values in the same block) and block-level pruning (query
  predicates eliminate more blocks).
- **TraceID** as the tertiary key ensures all spans from a trace land in as few blocks as
  possible, reducing the number of I/O operations for `FindTraceByID`.

**Consequence:** Spans cannot be flushed incrementally as they arrive. The writer must hold
all spans in memory until `Flush` is called. `CurrentSize()` estimates ~2 KB per span.

---

## 3. Incremental Range Index Construction
*Added: 2026-02-10, updated: 2026-02-27*

**Decision:** During block building, `blockBuilder` tracks only the per-column minimum and
maximum encoded key observed within the block (`b.colMinMax map[string]*blockColMinMax`).
After each block is written, `buildAndWriteBlock` calls `addBlockRangeToColumn` once per
column, which feeds the min and max into the column's KLL sketch (embedded in
`rangeColumnData`) and appends a `blockRange` entry recording the [min, max] interval.

**Rationale:** The previous approach appended one `blockRangeValue` per span per column
(O(spans × columns) per block), requiring pre-allocation of `spanHint * 32` capacity to
avoid growslice. The per-column min/max approach:
1. Reduces per-block range index work from O(spans × columns) to O(columns) — a 100-2000x
   reduction for typical blocks with 2000 spans and 30 attributes.
2. Eliminates the large `rangeVals` slice (was up to `2000 * 32 * ~40 bytes ≈ 2.5 MB` per
   block), reducing peak memory during block building.
3. Eliminates `buildKLLFromRangeIndex` (the full re-scan pass at Flush time): KLL sketches
   are built incrementally in `addBlockRangeToColumn` alongside the `blockRange` entries.

**Implication:** The range index (`w.rangeIdx`) holds per-block [min, max] ranges and
embedded KLL sketches. At `Flush()` time, `applyRangeBuckets` calls `kll.Boundaries` to
get bucket boundaries, then iterates `cd.blocks` to assign each block to all overlapping
buckets via `appendUniqueBlockID`. No full re-scan of the range index is needed.

---

## 4. KLL Sketches for Range Index Buckets
*Added: 2026-02-10, updated: 2026-02-26*

**Decision:** All range index columns use a correct multi-level KLL compaction sketch
(Karnin, Lang, Liberty, FOCS 2016) to compute approximate quantile bucket boundaries.

**Implementation:** `KLL[T cmp.Ordered]` and `KLLBytes` in `kll.go` implement true KLL:
items accumulate in an unsorted level-0 buffer; when a level fills it is sorted, randomly
halved (keeping every other item at a random offset), and survivors are merge-sorted into
level h+1. Items at level h have weight 2^h. Memory: O(k log n) retained items. Quantile
error: O(1/k) with high probability, regardless of input order.

**Rationale:** Computing exact quantiles requires O(n) memory. KLL provides approximate
quantiles with O(k log n) memory. The resulting boundaries are written to the range index
section (§5.2 in SPECS) and used at query time to map a predicate value to a bucket range.

**Consequence:** Bucket boundaries are approximate — two nearby values may land in different
buckets. This only affects pruning efficiency, never correctness.

**Incremental KLL construction (see §17):** KLL sketches are embedded in `rangeColumnData`
and fed two samples per block (block min and max) in `addBlockRangeToColumn`. No separate
full-scan pass at `Flush()` time is needed. See §17 for the full rationale.

---

## 5. Per-Writer zstd Encoder
*Added: 2026-02-10*

**Decision:** A single `*zstd.Encoder` is created at writer construction and reused for all
blocks across all Flush calls.

**Rationale:** Early versions used `sync.Pool` for encoder pooling. In production workloads
with large batch conversions (hundreds of blocks), `sync.Pool` accumulated encoder instances
that were never released until GC ran. Each encoder uses ~128 MB of internal state. With
hundreds of pooled encoders this caused OOM kills. A single reused encoder limits memory
to one encoder per writer instance regardless of block count.

**Implication:** `compressBuf` (the intermediate compression buffer) is reused across calls.
Callers must copy the result before the next compression call — the writer handles this
internally by writing to the output stream immediately after compression.

---

## 6. Encoding Selection at Build Time
*Added: 2026-02-10*

**Decision:** `columnBuilder.buildData` chooses the encoding kind based on column type,
data characteristics, and column name heuristics evaluated once at block finalization.

Selection logic:
- **Uint64 columns:** analyze `(max - min)` range and cardinality via `shouldUseDeltaEncoding`.
  Delta wins when range ≤ 65535 (fits uint16), or range ≤ uint32 and cardinality > 3, or
  cardinality > 2 for larger ranges.
- **Bytes/ID columns** (`isIDColumn` heuristic): use XOR encoding. Exception: `trace:id`
  uses DeltaDictionary (trace IDs are sorted and sequential in a sorted-spans file).
- **Bytes/URL columns** (`isURLColumn` heuristic): use prefix encoding.
- **Array columns** (`isArrayColumn`): always use dictionary encoding.
- **All other types:** use dictionary encoding. Choose sparse variant when null ratio > 50%.
  Choose RLE variant when cardinality is very low (writer detects this during column build).

**Note:** The `isIDColumn` list is checked by column name suffix (`.id`, `_id`, `-id`, etc.)
and exact name (`span:id`, `span:parent_id`). The explicit exclusion of `trace:id` is
intentional — trace IDs appear identically across all spans of a trace, so dictionary
encoding with delta indexes gives better compression.

---

## 7. UUID Auto-Detection
*Added: 2026-02-10, updated: 2026-02-26*

**Decision:** String columns whose sampled values all match UUID format are transparently
stored as 16-byte binary (Bytes type).

**Rationale:** UUID strings are 36 characters (32 hex + 4 dashes). Storing them as raw
bytes saves ~55% space. The writer samples the first `uuidSampleCount` (8) values per
string column and detects UUID format. If all samples are UUIDs, the full conversion loop
verifies all values. Only if every present value converts successfully is the column stored
as Bytes with XOR encoding (since UUIDs are binary IDs).

**Implication:** A column that starts as Bytes (UUID) may not be queried as String in the
same block. The type is determined per-block at finalization time. If any present value
fails full UUID conversion, the entire column falls back to string encoding for that block
(see §22 for the full rationale and the fix history).

---

## 8. Block Size Targeting
*Added: 2026-02-10*

**Decision:** Default block target is 512 KB compressed. Writer closes a block when adding
the next span would exceed this target.

**Rationale:** 512 KB is a sweet spot between:
- Enough data for dictionary compression to achieve good ratios (>1000 spans typically).
- Small enough that a single block read (I/O) stays under 1 MB, even accounting for
  variance.
- `MaxBlockSpans` is 65535 (uint16 span index limit in trace block index). Default
  `MaxBlockSpans` in `WriterConfig` is 2000.

A block is closed when `CurrentSize() >= defaultBlockTargetBytes` OR when span count would
exceed `MaxBlockSpans`.

---

## 9. Concurrent Access Detection
*Added: 2026-02-10*

**Decision:** Writer uses `atomic.Bool` to detect concurrent calls to `AddSpan` or `Flush`.
On detection it panics immediately.

**Rationale:** Writer is NOT thread-safe by design. Silent data corruption is worse than a
panic. The `atomic.Bool` sentinel catches races on the hot write path with zero overhead in
the non-concurrent case (atomic store of 0→1 on entry, 1→0 on exit).

---

## 10. Arena Allocation in Reader
*Added: 2026-02-10*

**Decision:** Column decoding supports `*arena.Arena` for reduced GC pressure. Dictionaries
always use heap allocation.

**Rationale:** In the hot query path, thousands of blocks may be decoded per second. Arena
allocation eliminates per-column GC overhead by providing a bump-pointer allocator that is
reset between queries. Dictionaries (string slices, bytes slices) are explicitly excluded
from arena allocation because the Column struct is returned to callers who may cache it
beyond the arena's lifetime. Arena-allocated dictionaries cause memory corruption when the
arena is reset.

**The rule:** Any data that lives in a `Column` struct's dictionary fields (`StringDict`,
`BytesDict`, etc.) MUST use `make()` (heap allocation). Data in index arrays and presence
bitsets MAY use `makeSlice[T](a, n)` for arena allocation.

---

## 11. Lazy Dedicated Index Parsing
*Added: 2026-02-10*

**Decision:** `parseV5MetadataLazy` records byte offsets for each dedicated column index
entry but does not parse the values. Parsing is deferred to first access via
`ensureDedicatedColumnParsed`.

**Rationale:** A file may have hundreds of dedicated column indexes. Most queries only
reference a handful of columns. Parsing all indexes at reader construction time wastes CPU
and memory. The lazy approach parses only the columns actually queried.

**Implication:** `Reader.dedicatedOffsets` is a `map[string]dedicatedIndexMeta` where
`dedicatedIndexMeta` stores `{typ, offset, length}` into the metadata byte slice. The
parsed result is cached in `Reader.dedicatedIndex[column]` after first parse.

---

## 12. Block Reuse in Query Path
*Added: 2026-02-10*

**Decision:** `parseBlockColumnsReuse` accepts an optional `*Block` to reuse its allocation.
If the reusable block has the same column set, Column objects from the previous decode are
also reused.

**Rationale:** Each block decode allocates a `*Block`, a `map[string]*Column`, and N
`*Column` structs. In a streaming query over thousands of blocks, this creates significant
GC pressure. Reusing allocations from the previous block decode reduces allocations by ~60%
on typical query workloads.

**Rule:** Reuse is safe only within a single query execution. Never cache a `*Block` across
query calls.

---

## 13. Coalescing Strategy
*Added: 2026-02-10*

**Decision:** `CoalesceBlocks` merges adjacent block reads using
`AggressiveCoalesceConfig`: 100% waste ratio tolerance, 4 MB maximum gap.

**Rationale:** On object storage, two sequential reads of 100 KB each cost the same latency
as one read of 200 KB. The aggressive config will always merge adjacent blocks even if the
gap between them contains data not needed for the current query. The waste ratio of 100%
means "even if half the bytes read are not used, merge". The 4 MB gap means "even if there
is 4 MB of other data between two needed blocks, read it all in one request".

**Addendum (2026-02-25):** The old `ModulesBlockReader.CoalesceBlocks` (adapter.go) did
not actually coalesce — it returned one `CoalescedRead` per block regardless of adjacency.
Block reads went 1:1 with I/O operations. As of the queryplanner migration,
`queryplanner.FetchBlocks` calls `BlockIndexer.ReadBlocks`, which routes through
`reader.ReadBlocks` → `CoalesceBlocks(r.blockMetas, indices, AggressiveCoalesceConfig)` →
`ReadCoalescedBlocks`. Adjacent blocks are now merged into bulk reads. This is the first
real coalescing in the modules execution path and reduces I/O ops on multi-block queries.

**Addendum (2026-03-03):** `AggressiveCoalesceConfig` now includes `MaxReadBytes = 8 MB`.
Without a cap, a query touching many adjacent blocks could produce a single unbounded range
request — risking timeouts, memory exhaustion, and object-storage limits on services like
S3. The 8 MB cap splits the merged run whenever adding the next block would exceed it. A
single block larger than 8 MB is still read whole (the cap cannot split an individual
block); it simply prevents further merging beyond it.

---

## 14. Format Versioning
*Added: 2026-02-10*

Two version spaces exist and must not be confused:

- **File format version** (`versionV10 = 10`, `versionV11 = 11`): Stored in the File Header
  and in each Block Header. Controls block index entry layout (v11 adds Kind byte) and
  which features are active.
- **Footer version** (`FooterV2Version = 2`, `FooterV3Version = 3`): Stored in the Footer.
  Controls the footer size and the presence of the compact trace index fields.

A single file uses the same block version throughout. The footer version is independent
and may evolve independently.

V12 (`VersionV12 = 12`) is a container-only version: block encoding is unchanged from
V11, but the metadata section is snappy-compressed. See §28 for rationale.

---

## 15. Column Name Bloom Filter
*Added: 2026-02-10*

Each block's `ColumnNameBloom` is a 32-byte (256-bit) Golomb-style bloom filter tested
with two independent hash functions (FNV1a and MurmurHash3, both mod 256). It is used
during block pruning to skip blocks that cannot possibly contain a queried column.

False positives are possible (a block may not contain the column even if the bloom says it
does). False negatives are impossible by construction.

The bloom is built by the writer calling `SetBit(bloom[:], hash1 % 256)` and
`SetBit(bloom[:], hash2 % 256)` for each column name added to a block.

---

## 16. Sparse Column Backfill in addColumn
*Added: 2026-02-10*

**Invariant:** Every column in a block must have exactly `spanCount` rows.

**Why it was broken:** `addColumn` in `blockBuilder` creates a new `columnBuilder` lazily
on first use. If a column first appears at span row N (e.g., an optional attribute absent
from the first N spans), the column builder starts with zero rows. Without backfill, when
the block is finalized, the column's `rowCount()` is `(spanCount - N)` rather than
`spanCount`. The reader's `parseColumnData` enforces `row_count == spanCount` and returns a
parse error for any mismatch.

**Fix (writer_block.go `addColumn`):** After creating the new `columnBuilder` and before
registering it in `b.columns`, loop `b.spanCount` times and add a null row for the
appropriate type. This fills rows 0..N-1 so that the first real value lands at row N and
the total row count equals `spanCount` at finalization.

**Why this is safe:** `b.spanCount` is incremented at the very end of `addRow`, so when
`addColumn` is called from within `addRow`, `b.spanCount` equals the number of rows already
finalized — exactly the number of null rows needed.

**Columns always present (no backfill needed in practice):** `trace:id`, `span:id`,
`span:name`, `span:kind`, `span:start`, `span:end`, `span:duration` appear in every span.
`resource.service.name` appears in nearly every span. Backfill is only triggered for
optional attributes like `span.http.method`, `span.db.statement`, etc.

---

## 17. Incremental KLL Construction for Range Buckets
*Added: 2026-02-25, updated: 2026-02-27*

**Decision:** KLL quantile sketches are built incrementally alongside block writing.
The two-pass `buildKLLFromRangeIndex` approach has been replaced with per-block min/max
accumulation (see §3).

**History:** The original KLL implementation used `idx = count % maxSamples` (deterministic
modulo cycling), which is not reservoir sampling — it is deterministic rotation. For sorted
input (spans are sorted by service_name before block building), each slot ended up holding
only the last value that cycled through it, so the reservoir contained only the
alphabetically-last ~10% of values. This silently broke bucket boundaries: `remapStringKeys`
stored lower bounds lexicographically greater than real values, breaking the binary search
at query time and returning zero results for valid queries.

**Current state:** The KLL implementation is a correct multi-level compaction sketch (see
§4). It handles sorted input correctly via random halving. The deterministic-modulo bug no
longer exists.

**Incremental approach (`addBlockRangeToColumn`):**
1. For each block, `blockBuilder` computes the per-column min and max encoded key.
2. `buildAndWriteBlock` calls `addBlockRangeToColumn` for each column, which:
   a. Appends a `blockRange{minKey, maxKey, blockID}` entry to `cd.blocks`.
   b. Feeds `minKey` and `maxKey` (decoded to typed values) into `cd.kll*`.
3. At `Flush()`, `applyRangeBuckets` calls `kll.Boundaries` to get bucket boundaries and
   iterates `cd.blocks` to assign blocks to all overlapping buckets via range-overlap.

**Bucket key rule:** All `applyOverlap*` functions use `bounds[i]` (the KLL quantile
boundary) as the bucket key. For string/bytes types, the key is truncated to
`rangeBucketKeyMaxLen` (50 bytes). Using `bounds[i]` directly ensures consistent bucket
keys across all blocks — a prior `min(bounds[i], br.minKey)` clamp was incorrect because
blocks spanning multiple buckets would store their min key in higher buckets, creating
phantom entries that broke the reader's binary search.

**Bucket distribution invariant:** Range buckets MUST be approximately uniformly populated.
With `defaultRangeBuckets = 1000`, each bucket should cover roughly 0.1% of the value
space. The incremental approach feeds only two samples per block per column into the KLL
sketch (block min and max). For datasets with many blocks, this provides adequate quantile
estimation. For datasets with very few blocks, `Boundaries` may return fewer than 2 entries,
in which case `applyOverlap*` returns early and the column has no bucket index (all blocks
are returned for any predicate on that column — correct, conservative behavior).

---

## 18. executor_test.go Uses New Executor Directly
*Added: 2026-02-25*

**As of the modules executor migration:** `internal/modules/blockio/executor_test.go`
no longer routes through `adapter.go` + `internal/executor`. It calls
`modules_executor.Collect(r, program, modules_executor.CollectOptions{})` directly,
with `r` being a `*modules_reader.Reader`.

**What this means:**
- EX-01 through EX-07 in `executor_test.go` are now integration tests of the real
  new execution path (writer → reader → queryplanner → executor).
- The `BuildPredicates` function (via `planBlocks`) in `internal/modules/executor` is exercised by every
  test that uses a named-column predicate (EX-01, EX-03, EX-04, EX-06).
- `adapter.go` and `NewModulesBlockReader` are still present and exported; they are
  retained for `comparison_test.go`, which runs the same TraceQL query through both
  the old blockpack format and the modules format via the same `executor.BlockReader`
  interface. That cross-format comparison is the adapter's explicit purpose.

---

## 19. addPresent Promoted from Closure to Method
*Added: 2026-02-26*

**Decision:** `addPresent` is an unexported method on `*blockBuilder`, not a local closure
inside `addRow`.

**Rationale:** In Go, a function literal that captures variables from the enclosing scope
allocates a heap object to hold the captured variables. The original `addPresent` closure
captured `b` (the `*blockBuilder` receiver). This closure was heap-allocated once per
`addRow` call — once per span. At production scale (tens of millions of spans per ingest
batch), pprof attributed ~108 GB of allocations to `addRow.func1` (the closure object).

Since `b` is already the receiver of `addRow`, the closure existed solely to give `addPresent`
access to `b`. Promoting it to an unexported method (`func (b *blockBuilder) addPresent(...)`)
is a purely mechanical refactor — zero behavior change — that eliminates the per-span heap
allocation entirely.

**Implication:** Any future helper that needs per-attribute logic inside `addRow` should
be written as an unexported method on `*blockBuilder`, not as a local closure.

---

## 20. addColumn Uses Remaining-Capacity Hint for Dynamic Columns
*Added: 2026-02-26*

**Decision:** `addColumn` passes `max(b.spanHint-b.spanCount, 0)` (not `b.spanHint`) as
the `initCap` argument to `newColumnBuilder` for dynamically-created attribute columns.

**Rationale:** A dynamic column first encountered at span row N has at most
`(spanHint - N)` rows remaining in the block. Pre-allocating `spanHint` capacity wastes
the first N slots. Pprof attributed ~24 GB to `stringColumnBuilder.addString` at scale;
the dominant source was the `make([]string, 0, spanHint)` call in `newColumnBuilder`
for columns first appearing late in a block (N close to `spanHint`).

Using `max(b.spanHint-b.spanCount, 0)` as the hint:
- Columns first seen at row 0 get full `spanHint` capacity (unchanged, no regression).
- Columns first seen at row 1900 of a 2000-span block get capacity 100 (correct).
- `max(..., 0)` protects against the degenerate case where `b.spanCount >= b.spanHint`.

**Why `b.spanCount` is correct at call time:** `b.spanCount` is incremented at the very
end of `addRow`, after all columns for the current span have been written. When `addColumn`
is called from within `addRow`, `b.spanCount` equals the number of rows already finalized —
exactly the number of null rows that will be backfilled for the new column (see §16).

**Consequence:** Dynamic columns appearing very late in a block may incur one growslice
event if the actual remaining row count exceeds the hint. This is rare and cheaper than
the systematic over-allocation it replaces.

---

## 22. UUID Auto-Detection Graceful Fallback
*Added: 2026-02-26*

**Decision:** `stringColumnBuilder.buildData` falls through to standard string encoding when
the full UUID conversion loop encounters a non-UUID value, rather than returning an error.

**Background:** `shouldStoreAsUUID` samples only the first `uuidSampleCount` (8) values for
performance. The full conversion loop that follows checks ALL values. Before this fix, if any
value in positions 8+ failed `uuidToBytes`, `buildData` returned a `fmt.Errorf` which
propagated up through `finalize → buildAndWriteBlock → flushBlocks`. `flushBlocks` did NOT
clean up on error (see §23), causing an infinite retry loop on every subsequent `Flush()` call.

**Real-world trigger:** `resource.service.instance.id` in Kubernetes deployments contains
both proper UUIDs (e.g., `550e8400-e29b-41d4-a716-446655440000`) and pod names
(e.g., `cortex-dev-01.cortex-gw-zone-b-ff5d8ddc-54qtw.cortex-gw`). The first 8 values
sampled would be UUIDs (nodes); later values would be pod names, causing conversion failures.

**Fix:** If any present value fails `uuidToBytes`, set `uuidOK = false` and break. If
`uuidOK` is false after the loop, skip the bytes-column path and continue to standard string
encoding. No error is returned; the column is stored as a string dictionary. The comment
in the source explains the dual role of `shouldStoreAsUUID` (performance gate) vs. the full
loop (correctness verification).

**Implication:** A column detected as UUID in one block may fall back to string encoding in
a subsequent block if a non-UUID value appears. Column type is determined per-block, not
globally. The removed `uuidColumns map[string]bool` field (which was initialized but never
read) was intended to persist detection across blocks but was never wired up; it remains
dead code and is harmless.

---

## 23. flushBlocks Error-Path Cleanup
*Added: 2026-02-26; updated 2026-02-26 (arena removed — see §24)*

**Decision:** `flushBlocks()` cleans up `w.spans` in the `buildAndWriteBlock` error path.

**Before this fix:** When `buildAndWriteBlock` failed, `flushBlocks` returned immediately
without cleanup. On the next `Flush()` call, `blockStart` reset to 0, but
`blockID = len(w.blockMetas)` was non-zero (e.g., 151 if 151 blocks had been written).
This caused the same block-ID to be retried indefinitely with stale spans, producing 50+
identical error lines in logs and leaving the writer in an inconsistent state.

**Current implementation (post §24):** `clear(w.spans)` + truncate on the error path.
No arena or protoSpanRoots to clean up (those were removed in §24).

**Consequence:** After a failed `flushBlocks`, any blocks written before the failure are
permanently recorded in `w.blockMetas`. A subsequent `Flush()` call will not retry them —
it will only flush any new spans added after the error. This is the correct behaviour:
partially-written blocks cannot be un-written.

---

## 24. Arena Removed from Writer (GC Crash Fix)
*Added: 2026-02-26*

**Problem:** Production Tempo reported `fatal error: found bad pointer in Go heap` under the
GC. Investigation showed the crash originated from the `w.spans` backing array. After a large
flush cycle, elements between the new `len` and the old `cap` retained stale
`SpanAttrs/ResourceAttrs/ScopeAttrs` `[]AttrKV` slice headers. Those headers held interior
pointers into freed `internal/arena` chunks. After `attrArena.Free()`, the arena's largest
block is kept but smaller blocks are GC-eligible; the OS may reclaim the physical pages.
When the GC later scanned the capacity tail of `w.spans`, it found these stale pointers and
fired `runtime.badPointer`.

Two complementary bugs contributed:
1. `clear(w.spans)` only clears up to `len`, not `cap`. Capacity-tail elements are not
   zeroed, so stale `[]AttrKV` headers survive arena.Free().
2. `attrArena.Free()` calls `xunsafe.Clear` on smaller blocks. The OS may subsequently
   reclaim those physical pages, turning the stale pointers into truly invalid addresses.

**Fix:** Remove `attrArena arena.Arena`, `protoSpanRoots`, `parseOTLPAttrsArena`,
`convertAnyMapArena`, and `otlpValueToAttrArena` from the writer entirely. All attribute
parsing now uses regular heap allocations (`parseOTLPAttrs`, `convertAnyMap`,
`otlpValueToAttr`). With heap-only allocations, all pointers in `w.spans` are normal GC-
visible heap pointers. Stale capacity-tail elements point to old `[]AttrKV` backing arrays
that remain valid heap objects — the GC can trace them without crashing. The `found bad
pointer` class of crash is impossible when no unsafe interior pointers exist.

**Trade-off:** Each span now allocates up to three `[]AttrKV` backing arrays on the heap
(span attrs, resource attrs, scope attrs) instead of one bump-pointer allocation from the
arena. This increases per-span GC pressure. Given the frequency and severity of the crash,
correctness is the right trade-off.

---

## 21. AddSpan Uses Arena-Backed Attribute Parsing
*Added: 2026-02-26; superseded by §24 (arena removed 2026-02-26)*

Arena-backed attribute parsing (`parseOTLPAttrsArena`, `convertAnyMapArena`) was introduced
to reduce `[]AttrKV` heap allocations in the `AddSpan` hot path. This optimisation was
reverted in §24 after arena pointer-lifetime bugs caused `found bad pointer in Go heap` GC
crashes in production. `parseOTLPAttrs`, `convertAnyMap`, and `otlpValueToAttr` are now the
single heap-allocating variants used by all code paths.

---

## 25. Column Index (§5.3) and Column Stats (§8.3) Removed
*Added: 2026-02-27*

**Decision:** Two format sections previously identified as unused have been removed:
- **Column Index (§5.3):** `r.columnIndexes` was parsed at reader open time but never read
  by any query execution path. Column offsets are always taken from `block.column_metadata`
  (§8.2) during block parsing. Writer now writes `col_count=0` per block; reader skips the
  section without allocating (`skipColumnIndex`).
- **Column Stats Section (§8.3):** `Column.Stats` was populated on every block read but
  never accessed by the executor, query planner, or any other caller. Stats fields
  (`statsOffset`, `statsLen`) still occupy 16 reserved bytes in each `ColumnMetadataEntry`
  (always zero) for format compatibility with existing files; the reader skips them.

**Size impact:** Based on pre-removal benchmarks on real Tempo trace data:
- Column Index: ~4.1% of total file size (~1.79 bytes/span across 29M spans).
- Column Stats: negligible in new files (zero bytes); significant savings for old files
  that had written stats.

**Format compatibility:**
- New files: column index section bytes `block_count × uint32(0)` are written for
  structural compatibility with old readers that expect exactly this section to be present.
- Old files (with non-zero stats): still readable — `parseColumnMetadataArray` skips the
  16 reserved bytes regardless of their value; the stats data at the old offsets is simply
  ignored.

**Code removed:**
- `writer/stats.go` — all stats types (`stringStats`, `int64Stats`, `uint64Stats`,
  `float64Stats`, `boolStats`, `bytesStats`), all accumulate functions, all encode functions.
- `writer/column_builder.go` — `buildStats() []byte` removed from the `columnBuilder`
  interface.
- `writer/metadata.go` — `columnIndexBlock`, `columnIndexEntry` types, `writeColumnIndexSection`.
- `reader/parser.go` — `columnIndexBlock`, `columnIndexEntry` types, `parseColumnIndex`;
  replaced with `skipColumnIndex`.
- `reader/reader.go` — `columnIndexes []columnIndexBlock` field removed.
- `reader/block_parser.go` — `parseColumnStats`, `parseColumnStatsSection` functions,
  `statsOffset`/`statsLen` fields from `colMetaEntry`.
- `reader/block.go` — `Stats shared.ColumnStats` field removed from `Column`.
- `shared/types.go` — `ColumnStats` struct removed.

**Encoding decision preserved:** `uint64ColumnBuilder.buildData` uses `shouldUseDeltaEncoding`
to choose between delta and dictionary encoding based on value range and cardinality. This
logic previously consumed `b.stats.min`/`b.stats.max` from the now-deleted `uint64Stats`
struct; it now uses explicit `minVal`, `maxVal`, `hasVals` fields on the builder, preserving
identical behaviour with no intermediate stats allocation.

---

## 26. Block Header Trace Table and Block Index Value Statistics Removed
*Added: 2026-02-27*

Two additional structures were removed from the format as part of the filesize reduction
work on this branch:

### Block Header: Trace Table (bytes 16–23 → reserved2)

**What was removed:** Bytes 16–23 of the 24-byte block header formerly stored
`trace_count[4]` + `trace_table_len[4]`, pointing to a per-block trace table appended
after the column data. The trace table listed the set of trace IDs present in each block,
supporting fast trace-to-block lookup at read time.

**Why removed:** Trace-to-block lookup is handled by the Trace Block Index in the metadata
section (§5.3), which was added in v8 and is the authoritative index used by all query
paths. The per-block trace table duplicated this information at significant cost (~20% of
total file size across real Tempo trace data).

**Format compatibility:** Bytes 16–23 are now `reserved2`, always written as zero.
Old readers that parsed the trace table will read `trace_table_len=0` and skip the section.
New readers skip bytes 16–23 unconditionally.

**Code removed:** `writer/writer_block.go` — trace table serialization loop removed;
`reader/block_parser.go` — `blockHeader.reserved2` comment added; `reader/block.go` —
trace table parsing removed.

### Block Index: Value Statistics Section

**What was removed:** Each block index entry previously appended a variable-length Value
Statistics section after the 98-byte fixed header. This section stored per-attribute bloom
filters (256 bits), min/max ranges, and approximate distinct-value counts for query-time
block pruning and selectivity estimation.

**Why removed:** The block index value statistics were not consumed by any active query
execution path. Block-level pruning uses the min/max time ranges (MinStart/MaxStart), trace
ID range (MinTraceID/MaxTraceID), and the column name bloom filter — all in the fixed
header. The variable-length stats section imposed parse overhead on every file open.

**Format compatibility:** Old readers that expected a variable-length stats section after
the 98-byte header will now read zero bytes of stats (the section is simply absent). The
`block_count` and per-block fixed-header fields are unchanged; readers that handle a
zero-length stats section parse correctly.

**Code removed:** `shared/types.go` — `BlockMeta.ValueStats` field removed;
`writer/metadata.go` — `writeBlockIndexSection` no longer appends value stats payload;
`reader/parser.go` — value stats parsing removed from `parseV5MetadataLazy`.

---

## 27. Log Signal Write Path — Parallel Code Paths on Writer
*Added: 2026-02-27*

**Decision:** Add OTEL log record support using parallel code paths on the existing `Writer`
struct, controlled by a `signalType uint8` field set on the first `AddLogsData` call.

**Key choices:**

1. **Separate files, not mixed.** Log blockpack files and trace blockpack files are distinct.
   A single `Writer` instance produces one signal type only; attempting to mix returns an
   error. This matches the "separate files" design decision from the brainstorm.

2. **Parallel code paths, not union structs.** `pendingLogRecord` and `logBlockBuilder`
   mirror `pendingSpan` and `blockBuilder` exactly, with different intrinsic fields.
   The trace hot paths (`addRowFromProto`, `sortPending`, `buildBlock`, `flushBlocks`)
   are untouched. Log paths are: `addLogRecordFromProto`, `sortPendingLogs`, `buildLogBlock`,
   `flushLogBlocks`, `buildAndWriteLogBlock`. This preserves trace performance and provides
   clean test isolation.

3. **Sort key for log records.** `(svcName ASC, minHashSig ASC, timestamp ASC)`. The
   tertiary key is timestamp rather than traceID. This clusters log records temporally
   within each service+attribute-similarity group, which is the dominant query pattern
   for logs (time-range queries). TraceID is not used because log records may not have one.

4. **VersionV12 file header.** A new format version (12) extends the 21-byte file header
   to 22 bytes by appending a `signal_type` byte at offset 21. The writer always emits
   V12 headers for both trace and log blockpack files; when no explicit signal type is
   set it defaults `signal_type` to `SignalTypeTrace`. The reader reads this byte for V12
   files and exposes it as `SignalType()`. Older readers that do not understand V12 reject
   the file cleanly (version check).

5. **No trace block index for log files.** `writeTraceBlockIndexSection` is called with
   an empty map (0 traces), producing a valid 5-byte trace index section. No semantic
   change to the format is needed.

6. **logBlockBuilder.finalize delegates to blockBuilder.** Rather than duplicating the
   column encoding and serialization logic, `logBlockBuilder.finalize` constructs a minimal
   `blockBuilder` shell with the same `columns` map and `spanCount` (=recordCount) and
   calls `blockBuilder.finalize`. All column encoding infrastructure is reused.

**Scope:** Writer + reader signal type detection only. Query engine, compaction, Tempo API,
LogQL, and metrics signal type are explicitly out of scope.

---

## 28. WithArena Removed — Dead Code Cleanup
*Added: 2026-02-27*

**Decision:** Remove `WithArena(a *arena.Arena) Option` and the `readerOptions.arena`
field from `internal/modules/blockio/reader/options.go`.

**Rationale:** The `readerOptions.arena` field was set by `WithArena` but was never
read by any code path in the reader (`reader.go`, `block.go`, `block_parser.go`,
`column.go`, `parser.go`). The option was accepted and silently discarded. Since
`internal/arena/` has been deleted (see §24 for the GC-crash history), removing this
dead import and dead code is required for the project to compile.

**Consequence:** `WithArena` is no longer available. Since it was a no-op, callers
receive a compile error but no change in runtime behaviour. As this is `internal/`
code, there are no external callers. §10 (Arena Allocation in Reader) documents the
original intent of the option; that design was never implemented and is now superseded.
Future reader arena allocation, if desired, should use `internal/modules/arena/`.

---

## 29. Snappy Metadata Compression (V12)
*Added: 2026-02-27*

**Decision:** Compress the entire metadata section as a single snappy blob. Signal via
file version V12 (file header `version = 12`). V10/V11 files remain fully readable.

**Approach:** In `Flush()` and `writeEmptyFile()` (writer.go), call
`snappy.Encode(nil, metaBytes)` immediately after `buildMetadataSectionBytes` returns.
Write the compressed bytes and record `len(compressed)` as `metadataLen`. Pass
`shared.VersionV12` to `writeFileHeader`.

In `parseV5MetadataLazy()` (parser.go), when `r.fileVersion == shared.VersionV12`,
call `snappy.DecodedLen(data)` to guard against decompression bombs, then
`snappy.Decode(nil, data)` to recover the uncompressed metadata. The decompressed slice
is assigned to `data` before `r.metadataBytes = data`, so the lazy range index offsets
(absolute byte positions into `r.metadataBytes`) continue to work correctly.

**Why snappy and not zstd:** Snappy's decompression speed (~1.5 GB/s) is the key
property. Metadata is read once at reader open time; CPU overhead matters more than
compression ratio. `github.com/golang/snappy v1.0.0` is already present in go.mod as
an indirect dependency (pulled in by Tempo), so importing it costs zero version resolution.

**Why V12 version bump and not a magic prefix:** The file version already gates feature
detection at `readHeader`. Adding `VersionV12` requires one constant and one line in the
version check. Old readers fail cleanly with "unsupported version 12". A magic prefix
would require changes inside the metadata section body and adds ambiguity risk.

**Why entire section and not sub-sections:** The lazy range index records byte offsets
into the full decompressed `r.metadataBytes` slice. Compressing each sub-section
independently would require parsing section boundaries twice (before and after
decompression) and break the lazy offset strategy. A single decompression call preserves
all existing parsing logic unchanged.

**Block headers remain V11:** Block encoding is unchanged in V12. The `version` field in
block headers describes the block payload format, not the file container format.
`buildBlock` in `writer_block.go` hardcodes `shared.VersionV11` (line 625). V12 files
contain V11 block payloads. `parseBlockIndexEntry` uses `version >= shared.VersionV11`
to read the kind byte, covering both V11 and V12 file versions.

**Decompression bomb guard:** `snappy.DecodedLen(data)` is checked against
`MaxMetadataSize` (100 MiB) before allocating the decode buffer. This prevents a
maliciously crafted small compressed blob from forcing a huge allocation.

**go.mod change:** `github.com/golang/snappy` moves from `// indirect` to a direct
dependency. Run `go mod tidy` after adding the import.

**FileLayout impact:** The `FileLayout()` reader method reports the entire compressed
metadata blob as a single `"metadata.compressed"` section for V12 files (with
`CompressedSize = metadataLen` and `UncompressedSize = len(metadataBytes)`). Sub-section
breakdown is not available at physical byte granularity when metadata is compressed.

---

## 30. BlockMeta.MinStart / MaxStart Used for Time-Range Pruning in Log Queries
*Added: 2026-03-02*

**Context:** `queryplanner.Plan()` now accepts a `TimeRange` parameter (MinNano, MaxNano)
and prunes blocks whose `[BlockMeta.MinStart, BlockMeta.MaxStart]` window does not overlap
the time range before any bloom or range-index pruning.

**For log files:** `MinStart` and `MaxStart` in `BlockMeta` record the minimum and maximum
`log:timestamp` (TimeUnixNano) values across all records in the block. The log writer sets
these via `bb.minStart = min(bb.minStart, record.TimeUnixNano)` and `bb.maxStart = max(...)`.
The temporal sort key `(svcName, minHashSig, timestamp)` ensures blocks are roughly
chronological within a service group, making time-range pruning highly effective for
recent-N-minutes queries.

**For trace files:** `MinStart` and `MaxStart` record `span.start` (StartTimeUnixNano).
The same time-range pruning applies and is useful for trace queries with time windows.

**Invariant:** Callers must pass `queryplanner.TimeRange{}` to disable time-range pruning
when no time bounds are known (e.g. metrics aggregations, structural join queries). Passing
a non-zero `TimeRange` with `MinStart = MaxStart = 0` blocks would incorrectly eliminate
blocks with zero timestamps (written before timestamp tracking was introduced), but in
practice all blocks have non-zero timestamps.

---

## 31. Native Column Compaction Path — Zero OTLP Allocations
*Added: 2026-02-28*

**Decision:** Compaction reads raw column values directly into `pendingSpan` and flushes
them via `addRowFromBlock`. No OTLP proto objects (`*tracev1.Span`, `*commonv1.KeyValue`,
`*commonv1.AnyValue`) are created during the compaction hot path.

**Background:** The original compaction path routed through full OTLP proto materialization:
```
Block columns → *tracev1.Span + map[string]any (spanFromRow)
             → *KV + *AnyValue per attribute (anyToKV / columnAny)
             → synthesizeResourceSpans (more proto wrappers)
             → AddSpan → pendingSpan
             → addRowFromBlock → Block columns
```
Profiling showed this generated ~85 billion unnecessary object allocations per ingest
window (`compaction.spanFromRow` 14.5B, `compaction.columnAny` 24.6B, `compaction.anyToKV`
12.9B, plus downstream proto allocs) — ~24% of total lifetime allocations and a primary
driver of GC-induced CPU spikes.

**Implementation:**

1. **`pendingSpan` dual-path:** Two new fields were added to `pendingSpan` in
   `writer_block.go`:
   - `srcBlock *modules_reader.Block` — non-nil for the columnar path; nil for the proto path
   - `srcRowIdx int` — source row index within `srcBlock`
   When `srcBlock != nil`, `buildBlock` dispatches to `addRowFromBlock` instead of
   `addRowFromProto`.

2. **`addRowFromBlock` method on `blockBuilder`:** Iterates `srcBlock.Columns()`, reads
   typed values, normalizes Range types to base types via `baseColumnType()` (Range types
   are encoding metadata, not logical type differences), clones byte slices with
   `slices.Clone` to avoid aliasing into block memory, and calls `addPresent` for all
   columns. Special-cases `trace:id` and `span:start` to update `traceRows`, `minStart`,
   `maxStart`, `minTraceID`, `maxTraceID`, `spanCount` — identical bookkeeping to
   `addRowFromProto`.

3. **`AddRow(block *reader.Block, rowIdx int) error` on `Writer`:** New public method that
   builds a `pendingSpan` with `srcBlock`/`srcRowIdx` set, calls
   `computeMinHashSigFromBlock`, and appends to `w.pending`. No `protoRoots` anchoring is
   needed since the block is caller-owned.

4. **`computeMinHashSigFromBlock` in `writer_sort.go`:** Hashes attribute key names (with
   namespace prefix stripped: `span.` → 5 chars, `resource.` → 9 chars, `scope.` → 6 chars)
   using the same FNV-1a min-heap logic as `computeMinHashSigFromProto`. Only present rows
   are hashed (checked via `col.IsPresent(rowIdx)`).

5. **Compaction rewrite:** `addSpanFromBlock` in `compaction.go` now uses `dedupeKey`
   (reads `trace:id` + `span:id` directly from block columns to build the 24-byte dedup
   key) and calls `AddRow`. Removed entirely: `spanFromRow`, `columnAny`, `anyToKV`,
   `cloneBytes`, and all OTLP imports (`commonv1`, `tracev1`).

**Type normalization:** `baseColumnType()` maps Range variants to their base type before
calling `addPresent`. Range types are an encoding selection detail in the block metadata
section (§5.2); the logical value type is the base type. This matches `addRowFromProto`,
which only uses base types.

**Correctness:** `TestCompactBlocks_NativeColumns` (15 spans across 3 blocks, all column
types verified) and `TestCompactBlocks_NativeColumns_Dedup` (deduplication of identical
spans across blocks) validate the native path end-to-end.

---

## 31. FileLayout UncompressedSize for Column Data Sections
*Added: 2026-02-27*

**Decision:** `FileLayout()` now populates `UncompressedSize` on column data sections by
walking each encoding's wire format and streaming zstd chunks to `io.Discard` to measure
their inflated size without materializing the full decompressed payload.

**Definition:** `UncompressedSize` is the total byte count of the column data blob if all
zstd-compressed sub-sections were replaced by their decompressed contents. Non-compressed
parts (encoding headers, presence RLE, index arrays) contribute their on-disk size as-is.

**Implementation:** `columnDataUncompressedSize(data []byte)` dispatches on the encoding
kind byte to per-kind inflated-size functions (`inflatedDictKind`, `inflatedDeltaUint64`,
`inflatedXORBytes`, `inflatedPrefixBytes`, `inflatedDeltaDict`). Each function uses
`inflatedZstdChunk` to stream `len[4]+zstd_data[len]` chunks to `io.Discard` and count
the decompressed bytes — no full output buffer is allocated.

**Edge case:** For small dictionary columns (few unique values), zstd frame overhead
(~13 bytes for magic, frame header, block header) can make the compressed output larger
than the raw data. In this case `UncompressedSize < CompressedSize` is expected and
correct — it accurately reflects that compression was counterproductive for that chunk.

**Performance:** Since `FileLayout()` is an analysis/debugging tool, streaming to
`io.Discard` is acceptable; the output is never materialized. For a 50,000-span file
(~25 blocks, ~500 column data sections) the total analysis time is ~250ms.

**Sections without UncompressedSize:** Structural sections (footer, file_header, block
headers, column_metadata) and metadata sections (block_index, range_index, column_index,
trace_index) do not set `UncompressedSize` — they are stored uncompressed on disk, so
`CompressedSize` already represents the true size. The `omitempty` JSON tag ensures these
sections omit the field in serialized output.

## 15. Per-File TS Index for Direction-Aware Block Selection
*Added: 2026-03-02*

**Decision:** Write a TS index section (sorted minTS/maxTS/blockID tuples) into the
metadata section at Flush() time.

**Rationale:** Log queries are almost always time-bounded ("last 1 hour"). The existing
BlockMeta metadata provides MinStart/MaxStart per block, but finding overlapping blocks
requires scanning all N BlockMeta entries (O(N)). The TS index pre-sorts entries by minTS
so the reader can binary-search (O(log N)) to skip blocks before the query window and stop
early once past the window.

For BACKWARD queries with a Limit, this means the planner can reverse the sorted block list
and the executor stops after collecting Limit entries — making tail-log queries O(limit)
instead of O(all_blocks).

**Wire location:** appended after the trace block index in the snappy-compressed metadata
section. Size impact: N × 20 bytes raw; compresses to ~10-12 KB for 1000 blocks.

---

## 32. Log MinHash Hashes key=value Pairs; Trace MinHash Hashes Keys Only
*Added: 2026-03-02*

**Decision:** `computeMinHashSigFromLog` hashes `"key=value"` pairs (via FNV-1a over key
bytes + '=' separator + string value bytes) rather than just key names.
`computeMinHashSigFromProto` (the trace path) continues to hash key names only.

**Rationale:** The two signal types have fundamentally different attribute distributions:

- **Traces:** Different span types have different attribute schemas (e.g. HTTP spans have
  `http.method`, DB spans have `db.system`). Hashing key names clusters spans by _schema
  similarity_, which is what improves compression and block-level pruning for traces.

- **Logs (Loki-style):** Log streams within a service share an identical attribute schema
  (every stream has the same set of label keys, e.g. `{cluster, namespace, service_name}`).
  If only key names are hashed, all streams within a service collapse to the same MinHash
  signature, defeating the clustering benefit. Hashing `"key=stringValue"` produces a
  distinct signature per stream (since streams differ in label values), enabling the writer
  to cluster records from the same stream together in the same blocks.

**Implementation detail:** For non-string attribute values (int, double, bool, bytes), the
function falls back to hashing the key name only (via `addHashToMinHeap`) to avoid
allocating a string representation. This is a minor precision trade-off: non-string
attributes have less impact on log stream identity than the string-valued label attributes
used by Loki.

**Back-ref:** `internal/modules/blockio/writer/writer_log.go:computeMinHashSigFromLog`,
`internal/modules/blockio/writer/writer_sort.go:addKVHashToMinHeap`

## 33. Column Maps Keyed by (Name, Type) to Prevent Silent Data Loss

**Date:** 2026-03-03

**Decision:** All column maps in the writer (`blockBuilder.columns`, `blockBuilder.builderCache`,
`logBlockBuilder.columns`) and reader (`Block.columns`) are keyed by `shared.ColumnKey{Name, Type}`
instead of by column name alone.

**Problem:** OTLP permits the same attribute key to appear with different types across spans
(e.g. `span.foo` as `string` on one span and `int64` on another). When column maps were keyed
by name only, `addColumn` detected the second type as a "conflict" and returned `nil`, causing
the conflicting span's value to be silently dropped. This produced incorrect query results for
mixed-type attributes and was reported by Tempo.

**Fix:** `ColumnKey{Name string; Type ColumnType}` is a comparable struct and a valid Go map key.
Using it as the map key stores both `span.foo/string` and `span.foo/int64` as independent columns
in the same block. The writer's `addColumn` and `addLogColumn` functions no longer need a
type-conflict guard — different types simply get different map entries. The `finalize` function
sorts columns by `(name, type)` for deterministic output.

**Reader side:** `Block.Columns()` returns `map[shared.ColumnKey]*Column`. New accessor methods
`GetColumnByType(name, type)` and `GetAllColumns(name)` allow callers to retrieve all type
variants for a given attribute name. `GetColumn(name)` retains backward compatibility by
returning the first match.

**Bloom filter stays name-only:** The block-level column name bloom filter tracks names (not
types) because block-level pruning only needs to know whether _any_ column with that name
exists. Having both `span.foo/string` and `span.foo/int64` is correctly represented by a
single bloom entry for `"span.foo"`.

**Back-ref:** `internal/modules/blockio/shared/types.go:ColumnKey`,
`internal/modules/blockio/writer/writer_block.go:addColumn`,
`internal/modules/blockio/reader/block.go:GetColumnByType`

## 36. Trace ID Bloom Filter in Compact Trace Index

*Added: 2026-03-03*

**Decision:** The Compact Trace Index (§6) now carries a variable-size bloom filter for all
trace IDs in the file, encoded using Kirsch-Mitzenmacher double-hashing (k=7) with the raw
trace ID bytes as entropy. The compact index version is bumped to 2.

**Rationale:** `FindTraceByID` must check many files to locate a trace. Even with the compact
index (2 I/Os: footer + compact section), each file's compact index is parsed eagerly and its
hash map built up front. For files that do not contain the target trace — the common case
across most files in a large trace store — the bloom filter provides an O(1) in-memory
rejection before any hash map lookup or trace-block scan. This avoids per-query work on the
"definitely not present" path; avoiding the initial map allocation would require a future
change to lazily parse the compact index.

**Why raw bytes, not FNV/murmur?** OTLP trace IDs are random 128-bit values (RFC 9546).
They are already uniformly distributed and serve as their own entropy source. Applying an
additional hash function (FNV, murmur) adds CPU cost without meaningfully improving
distribution. The Kirsch-Mitzenmacher technique (`h_i = (h1 + i*h2) mod m` with h1/h2 from
the raw bytes) is theoretically sound and practically sufficient for random inputs.

**Why variable size?** A fixed bloom size would be either wasteful for small files (few traces,
large filter) or ineffective for large files (many traces, tiny filter). Sizing at 10 bits per
trace (TraceIDBloomBitsPerTrace=10) yields ≈0.8% FP with k=7 across all file sizes, clamped
to [128B, 1MB].

**Backward compatibility:** Version-1 compact indexes (no bloom) continue to work. The reader
treats a nil bloom as vacuous (always returns true), preserving no-false-negatives. Older
writers automatically get version-1 indexes; new readers support both versions.

**Back-ref:** `internal/modules/blockio/shared/bloom.go:AddTraceIDToBloom`,
`internal/modules/blockio/writer/metadata.go:writeCompactTraceIndex`,
`internal/modules/blockio/reader/trace_index.go:BlocksForTraceIDCompact`

## 37. Log Body Auto-Parse at Ingest

*Added: 2026-03-04*

**Decision:** Parse log record bodies (JSON or logfmt) at write time and store all
extracted top-level fields as sparse `log.{key}` `ColumnTypeRangeString` columns.

**Rationale:** Loki/LogQL queries frequently filter on structured body fields
(`| level="error"`, `| duration>100`). Without body parsing at ingest, these filters
cannot be pushed down to block-level predicates — every block must be read and every
row must be pipeline-evaluated. By storing extracted fields as dedicated range columns,
the executor can use bloom filter + range index pruning identically to how it prunes on
resource attributes, reducing I/O by skipping blocks that cannot match.

**Why at ingest, not at query time:** Block pruning requires per-block metadata
(bloom filters, min/max range index) written at ingest time. Query-time parsing cannot
retroactively update block metadata. Ingest-time parsing is a one-time cost amortized
across all queries over the block's lifetime.

**No field cap decision:** Fields are not capped per record. Log bodies rarely exceed
20-30 fields in practice; imposing a cap would silently drop high-cardinality fields
and create subtle query correctness bugs. Column count has no structural limit in the
blockpack format.

**Silent failure policy (extends NOTE-007):** If body parse fails (malformed JSON/logfmt,
plain text), no columns are added and no error is surfaced. The `log:body` intrinsic
is always written, preserving full-text search capability.

**Performance note:** `parseLogBody` allocates one `map[string]string` per record when a
structured body is detected. For unstructured bodies, allocation is zero. The per-record
cost is acceptable given that body parsing enables block-level skip for filtered queries.

Back-ref: `internal/modules/blockio/writer/writer_log_body.go:parseLogBody`,
          `internal/modules/blockio/writer/writer_log.go:addLogRecordFromProto`

---

## 38. Trace Index Format v2 — Block IDs Only, Scan In-Block

*Added: 2026-03-04*

**Decision:** The Trace Block Index (§5.3) and Compact Trace Index now use format
version 2, which stores only a list of block IDs per trace — no per-block span indices.
At `GetTraceByID` time, the reader scans the `trace:id` column in the already-loaded
block to find matching row indices.

**Rationale:** The old format stored `span_count[2] + span_indices[N×2]` per
(trace, block) pair. For a trace with S spans spread across B blocks, the index cost
is `16 + 2 + B×(4 + S×2)` bytes on disk plus `B` separate heap allocations of `[]uint16`
in memory. The new format costs `16 + 2 + B×2` bytes — constant per block regardless
of span count. A trace with 50 spans in 3 blocks drops from ~322 bytes to 24 bytes
(13× reduction); the ratio grows with span count.

**Why scanning is acceptable:** Blocks are always fetched in a single full I/O
(SPEC-007, NOTE-24). By the time any row is accessed, the entire block is already in
memory. Scanning the `trace:id` bytes column for up to `MaxBlockSpans` rows (~2000)
is O(N) sequential memory reads — a few microseconds — with no additional I/O.
The prior span index existed only to skip this scan; removing it trades negligible CPU
for substantial index size and allocation reduction.

**Backward compatibility:** Format v1 files are still readable. `parseTraceBlockIndex`
detects the fmt_version byte (0x01 vs 0x02), parses v1 span indices but discards them,
and builds the same `map[[16]byte][]uint16` (block IDs only) in both cases.

**Back-ref:** `reader/parser.go:parseTraceBlockIndex`,
`writer/metadata.go:writeTraceBlockIndexSection`, `api.go:GetTraceByID`,
`shared/constants.go:TraceIndexFmtVersion2`

---

## 39. Lazy Column Decode — Presence-Only Registration + On-Demand Full Decode
*Added: 2026-03-05*

**Decision:** `parseBlockColumnsReuse` now performs lazy registration for all columns
not in `wantColumns`. Instead of skipping them (old behavior), it decodes presence-only
(reads the uncompressed RLE bitset, skips compressed dict/value blobs) and stores a
`rawEncoding` sub-slice pointing into the block's raw bytes. Full decode is deferred
to the first value access (`Column.decodeNow()`).

**Why presence-only is cheap:** Each column's compressed blobs have a 4-byte length
prefix in the wire format. `decodePresenceOnly` reads the header fields and skips
forward — no zstd decompression, O(M/8) per column where M is span count.

**rawEncoding lifetime:** `rawEncoding` is a sub-slice of `BlockWithBytes.RawBytes`.
`bwb.RawBytes` is retained for the lifetime of `bwb`. All lazy decodes complete within
the block's row loop (before `bwb` goes out of scope). This guarantee holds because the
scan is single-goroutine and sequential.

**internMap validity:** Each `ParseBlockFromBytes` and `AddColumnsToBlock` call creates its
own fresh `make(map[string]string)` intern map local to that call. Strings do not persist
across calls. `ResetInternStrings` is now a no-op retained for call-site compatibility.
Cross-call intern reuse no longer occurs; the trade-off is accepted for race-safety.
*(Addendum 2026-03-17: supersedes the original "borrowed from Reader.internStrings" design.)*

**Performance impact:** For T9/Q66 (1997 blocks, ~90 non-predicate columns):
eagerly decoding all columns cost ~0.87s in zstd decompression. Lazy decode replaces
this with ~0.04s presence-only reads + ~0.15s for the ~15 columns actually accessed.

**NOTE-001** in `reader/block.go` and `reader/column.go` tags the relevant implementation.

**Back-ref:** `reader/block_parser.go:parseBlockColumnsReuse`,
`reader/column.go:decodePresenceOnly`, `reader/block.go:Column.decodeNow`

---

## 40. Numeric String Range Index Promotion
*Added: 2026-03-05*

**Decision:** At block build time, `logBlockBuilder` attempts to parse all non-empty
string attribute values for each column as int64 (preferred) or float64 (fallback).
If every non-empty value in a column parses as the same numeric type, the string
`blockColMinMax` entry is replaced with an 8-byte LE numerically-encoded entry before
the flush loop. The resulting range index column type is `ColumnTypeRangeInt64` or
`ColumnTypeRangeFloat64` instead of `ColumnTypeRangeString`.

**Rationale:** String attributes like `latency_ms = "4500"` are common in OTLP data.
Without this promotion, the KLL sketch uses lexicographic ordering ("1000" < "100" is
false lexicographically), causing incorrect block pruning for `latency_ms > 3500`-style
queries. With numeric promotion, the KLL computes numerically correct quantile
boundaries and pruning works as expected.

**Invariants:**
- A single non-parseable non-empty value marks the column as non-numeric for the
  entire block. All-or-nothing per block.
- Empty strings (present=true, val.Str="") are ignored — they are null fills from
  `addLogColumn` paths and must not prevent numeric promotion.
- The substitution guard (`mm.colType == ColumnTypeString || ColumnTypeRangeString`)
  ensures native int64 columns are never overwritten by the parsed-string version.
- int64 is preferred over float64 (exact representation; common for counters, latencies).
- Negative float values are excluded from float promotion (negative IEEE-754 bit patterns
  sort in reverse under LE comparison, producing incorrect range index boundaries).
- The flush loop in `writer.go` is unchanged. Substitution happens inside `buildLogBlock`
  before `builtBlock` is returned.

**Performance:** `colNonNumeric` and `colNonFloat` short-circuit failed columns after the
first failure. For mixed-value columns, overhead is O(1) after the first non-parseable
value.

**Back-ref:** `internal/modules/blockio/writer/writer_log.go:buildLogBlock`,
`internal/modules/blockio/writer/writer_log.go:addLogPresent`

---

## 41. IterateFields Must Skip ColumnTypeRangeString (Body-Parsed Range Columns)
*Added: 2026-03-05*

**Decision:** `modulesSpanFieldsAdapter.IterateFields` skips columns of type
`ColumnTypeRangeString` via an early `continue` guard before `modulesGetValue`.

**Rationale:** The writer (`writer_log.go:SPEC-11.5`) auto-parses JSON/logfmt log bodies
and stores parsed fields as `log.{key}` columns of type `ColumnTypeRangeString`. These
exist solely for range-index support and block-level pruning — they are not raw
LogRecord attributes (StructuredMetadata). The chunk store never exposes body-parsed
fields as SM.

`LokiConverter.extractStructuredMetadata` calls `IterateFields` and collects all
`log.*` string-valued columns as Loki StructuredMetadata. Before this fix, it would
collect body-parsed `log.level`, `log.msg`, `log.duration_seconds`, etc. as SM. This
caused the Loki Pipeline (`| json`, `| logfmt`) to see pre-populated SM fields
matching the same keys it was about to extract from the body — producing `_extracted`
suffix collisions, different label sets, and entry-count mismatches vs the chunk store.

Real LogRecord attributes (e.g. `detected_level` from SeverityText, SM from
`entry.StructuredMetadata`) are stored as `ColumnTypeString`. They are correctly
included in IterateFields output and must not be skipped.

**Invariants:**
- `ColumnTypeRangeString` columns are always body-parsed; they are never written as raw
  LogRecord attributes.
- `ColumnTypeString` log.* columns are always real SM; they must pass through.
- `GetField` (direct column lookup by name) is unaffected — it calls `modulesGetValue`
  directly without the IterateFields loop.
- `SpanMatch.Clone()` also uses `IterateFields`; omitting `ColumnTypeRangeString` there
  is correct since those are internal index columns not useful to Clone callers.

**Back-ref:** `internal/modules/blockio/span_fields.go:IterateFields`,
`benchmark/lokibench/converter.go:extractStructuredMetadata`

---

## 42. tryApplyExactValues Must Bail Out for ColumnTypeRangeString Blocks Spanning Multiple Values
*Added: 2026-03-05*

**Decision:** In `tryApplyExactValues`, when `cd.colType == ColumnTypeRangeString` and any
block has `minKey != maxKey`, return `false` immediately to fall through to the KLL overlap
path.

**Rationale:** The exact-value index stores each block only under its `minKey` and `maxKey`
encoded string values. The reader looks up blocks via `BlocksForRange`, which is a point
lookup (binary search to find the bucket whose lower bound ≤ queryValue, returns that single
bucket). For a block spanning multiple string values (e.g., `minKey="auth-service"`,
`maxKey="web-server"`), any intermediate value (e.g., `"grafana"`) matches neither key in
a point lookup — the lookup finds the `"auth-service"` bucket but does not include the block
when querying `"grafana"`, because that bucket's lower bound is ≤ `"grafana"` but the block
was not added to that bucket. This produces false-negative pruning: blocks that should be
returned are silently omitted, causing query results to differ from the chunk store.

Numeric range types (RangeInt64, RangeUint64, RangeFloat64) are safe with min != max in the
exact-value path because their predicates use `BlocksForRangeInterval` (interval lookup),
which unions all buckets between minKey and maxKey. String types use point lookup only.

The KLL overlap path (`applyOverlapString`) correctly handles string intervals by spanning
all KLL buckets between `findBucketString(minKey)` and `findBucketString(maxKey)`, giving
correct no-false-negatives semantics.

**Invariants:**
- When all blocks for a `ColumnTypeRangeString` column have `minKey == maxKey` (homogeneous
  blocks, one distinct string per block), the exact-value path is safe and produces zero
  false positives.
- When any block has `minKey != maxKey`, the KLL path is used regardless of cardinality.

**Back-ref:** `internal/modules/blockio/writer/range_index.go:tryApplyExactValues`

---

## NOTE-BLOOM-REMOVAL: Block Index Wire Format Change — ColumnNameBloom Removed (2026-03-07)
*Added: 2026-03-07*

**Change:** `ColumnNameBloom [32]byte` (32 bytes per block index entry) has been removed
from the block index wire format.

**Wire format impact:** Block index entries are now 32 bytes smaller per entry. The parser
(`reader/parser.go:parseBlockIndexEntry`) no longer reads these 32 bytes. Files written
before this change are unreadable with this version (breaking change, accepted for internal
format).

**Files changed:** `writer/metadata.go` (stop writing), `reader/parser.go` (stop reading),
`shared/types.go` (removed field), `shared/constants.go` (removed constants).

**Rationale:** CMS (Count-Min Sketch) subsumes column-name bloom. If a column was never
written to a block, `BlockCMS(b, col)` returns nil and the planner passes conservatively —
identical behavior to a bloom miss. CMS additionally provides value-level pruning.
See `internal/modules/blockio/shared/NOTES.md:NOTE-BLOOM-REMOVAL` for full rationale.

---

## 43. MaxBlocks Corrected to 65,535
*Added: 2026-03-11*

**Change:** `MaxBlocks` constant corrected from 100,000 to 65,535 in `shared/constants.go`
and both SPECS.md files.

**Rationale:** The trace index wire format encodes block IDs as uint16. Block IDs are
0-based, so the maximum valid ID is 65534, allowing at most 65,535 blocks per file
(IDs 0–65534). The writer already enforced this limit (`writer.go:buildAndWriteBlock`
returns an error when `blockID >= 65535`), but the spec and constant were stale at 100,000.

**Files changed:** `shared/constants.go`, `SPECS.md` §1.1, `shared/SPECS.md` §8.

---

## 44. MaxMetadataSize Spec Corrected to 256 MiB
*Added: 2026-03-11*

**Change:** SPECS.md §1.1 and §5 corrected from 100 MiB to 256 MiB to match the code.

**Rationale:** The constant was raised from 100 MiB to 256 MiB in `shared/constants.go`
to accommodate sketch section data at scale. The spec was not updated at the time.
`shared/SPECS.md` §8 was already correct (268,435,456).

---

## 45. KLL Bucket Boundary Parsing and File-Level Fast Reject
*Added: 2026-03-14*

**Decision:** Parse `bucket_min`, `bucket_max`, and typed boundary arrays from the range
index wire format (previously skipped). Expose them via `Reader.RangeColumnBoundaries`.
Use these in `executor.planBlocks` for file-level fast reject.

**Rationale:** Before this change, the reader skipped all bucket metadata (min, max,
boundaries) during `parseRangeColumnEntry` — only the value entries were used. This meant
that the executor had no way to quickly discard entire files when the query value is
completely outside the file's observed range. For example, a query `span:duration > 1h`
against a file whose longest span was 10 seconds would still read and evaluate all blocks.

**File-level fast reject algorithm:**
1. After `PlanWithOptions`, if `program.Predicates` is non-nil, call `fileLevelReject`.
2. `fileLevelReject` walks AND-combined leaf `RangeNode` predicates.
3. For each leaf with a `Min` or `Max` bound, fetch `RangeColumnBoundaries` for the column.
4. If the predicate interval is entirely disjoint from `[BucketMin, BucketMax]`, return
   early with empty `SelectedBlocks` (no block reads needed).

**Scope:** Only numeric range columns (RangeInt64, RangeUint64, RangeDuration, RangeFloat64)
support file-level reject; RangeString/RangeBytes are excluded (their BucketMin/BucketMax
are 0 in the wire format and not meaningful for interval comparison).

**Back-refs:**
- `internal/modules/blockio/reader/range_index.go:parseTypedBoundaries`
- `internal/modules/blockio/reader/reader.go:RangeColumnBoundaries`
- `internal/modules/executor/plan_blocks.go:fileLevelReject`

---

## 46. VersionV13 and VersionBlockV12 — Wire Format Space Reduction
*Added: 2026-03-14*

**Change:** Two new format versions eliminate dead wire fields:

1. **VersionV13 (file version):** Block index entries no longer include
   `MinTraceID[16] + MaxTraceID[16]` (32 bytes per block). These fields were tracked
   in `BlockMeta` but were never used for pruning — the compact trace index handles
   trace lookup. Saves 32 bytes per block per file.

2. **VersionBlockV12 (block-content version):** Column metadata entries no longer
   include `stats_offset[8] + stats_len[8]` (16 bytes per column per block). These
   stub fields were always written as zero and never read. Saves 16 bytes per column
   per block.

**Reader backward compatibility:** The reader (`parser.go:parseBlockIndexEntry`)
conditionally includes/skips `MinTraceID+MaxTraceID` based on `version < VersionV13`.
The block parser (`block_parser.go:parseColumnMetadataArray`) conditionally
includes/skips stats fields based on `blockVersion < VersionBlockV12`.

**Writer:** All new files use `VersionV13` as the file header version and
`VersionBlockV12` as the block content version. The `buildBlock` function accepts
`blockVersion` to allow future versions to add new block formats.

**Files changed:** `shared/constants.go`, `writer/metadata.go`, `writer/writer.go`,
`writer/writer_block.go`, `writer/writer_log.go`, `reader/parser.go`,
`reader/block_parser.go`, `reader/layout.go`.

**Back-refs:**
- `internal/modules/blockio/shared/constants.go:VersionV13,VersionBlockV12`
- `internal/modules/blockio/writer/metadata.go:writeBlockIndexSection`
- `internal/modules/blockio/writer/writer_block.go:finalize`
- `internal/modules/blockio/reader/parser.go:parseBlockIndexEntry`
- `internal/modules/blockio/reader/block_parser.go:parseColumnMetadataArray`

---

## 47. MaxCompactSectionSize — Defined But Unenforced (GAP-8)
*Added: 2026-03-18*

**Status: aspirational constant — no enforcement exists.**

`MaxCompactSectionSize = 52_428_800` (50 MiB) is defined in `shared/constants.go` and
referenced in two comments in `writer/writer.go` (lines ~384 and ~460). However, the
constant is **never checked in any code path**:

- `writeCompactTraceIndex` in `writer/metadata.go` builds the compact trace index blob
  and returns its byte length, but does not check `buf.Len() > MaxCompactSectionSize`.
- The reader (`parser.go`) reads `compactLen` bytes unconditionally with no size guard.
- The two `writer.go` comments cite the constant to justify a `uint32` cast being safe,
  but that safety argument is circular — the size is never actually bounded.

**Why it was not enforced:** The compact index grows as `O(unique_traces × avg_blocks_per_trace)`.
For typical workloads this is well under 1 MiB. The 50 MiB limit was chosen as a generous
cap that would never be hit in practice, so enforcement was deferred.

**Risk:** A pathological file with millions of unique traces across many blocks could
produce a compact index exceeding 50 MiB, causing a silent `uint32` truncation in the
`compactLen` field of the v4 footer. The reader would then read a wrong (truncated) byte
count and corrupt the parse.

**TODO:** If enforcement is added, the recommended approach is:
1. Change `MaxCompactSectionSize` from `const` to `var` (to allow test override).
2. Add a size check in `writeCompactTraceIndex` after building the blob: return an error
   if `len(blob) > shared.MaxCompactSectionSize`.
3. Write a test that lowers the limit to a small value, writes enough traces to exceed it,
   and asserts the writer returns an error.

**Back-refs:**
- `internal/modules/blockio/shared/constants.go:MaxCompactSectionSize`
- `internal/modules/blockio/writer/metadata.go:writeCompactTraceIndex`
- `internal/modules/blockio/writer/writer.go` (comments on lines ~384, ~460)

---

## 48. FileBloom Section — Cacheable Fuse8 for File-Level Service Name Rejection
*Added: 2026-03-20*

**Decision:** Add an optional `FileBloom` section (magic `FBLM`) as the last entry in the
metadata blob, storing a BinaryFuse8 filter for `resource.service.name` values.

**Why Fuse8 (not CMS):** CMS is already available via `FileSketchSummary` but requires
keeping the full metadata blob alive if cached. Fuse8 bytes are cloned to a standalone
small buffer (~100–300 bytes for typical files), making them safe to cache by (path, size)
without retaining the entire metadata blob.

**Why service.name only (not trace:id):** trace:id has high cardinality (unique per trace),
making a Fuse8 bloom large for big files (~8.8 bits/trace). The existing compact trace index
bloom (Kirsch-Mitzenmacher, ~10 bits/trace) already covers trace:id; it is now exposed via
`TraceBloomRaw()` and `MayContainTraceID()` for the same caching pattern.

**Caching pattern:**
```go
svcBloom   := r.FileBloomRaw()   // clone, ~100–300 bytes
traceBloom := r.TraceBloomRaw()  // clone, existing compact bloom
// Later, without reopening the file:
fb, _ := reader.ParseFileBloom(svcBloom)
fb.MayContainString("resource.service.name", "grafana") // false → skip file
shared.TestTraceIDBloom(traceBloom, traceID)            // false → skip file
```

Back-ref: `internal/modules/blockio/writer/file_bloom.go:writeFileBloomSection`,
          `internal/modules/blockio/reader/file_bloom.go:FileBloom`,
          `internal/modules/executor/plan_blocks.go:fileLevelBloomReject`


## 49. Pre-Computed Column Iteration List on Block (iterFields)
*Added: 2026-03-23*

**Decision:** `Block.BuildIterFields()` pre-computes a deduplicated `[]ColIterEntry` slice
at parse time. `modulesSpanFieldsAdapter.IterateFields` uses this slice directly when
present, falling back to the original `seen` map path when it is nil.

**Why:** pprof on BenchmarkRealWorldQueries (25K traces × 10 spans) attributed ~9%
of all allocations and ~15% of bytes to `(*modulesSpanFieldsAdapter).IterateFields`. The
dominant cost was `make(map[string]struct{})` called once per matched span — 250K+ map
allocations per benchmark run. Deduplication is invariant for a given block (column set is
fixed after parse), so it can safely be moved to parse time.

**How to apply:** `BuildIterFields()` is called immediately after `buildNameIndex()` in
every block parse path (`block_parser.go:parseBlockColumnsReuse` and
`reader.go:AddColumnsToBlock`). `IterFields()` returns the slice; `IterateFields` uses it
when non-nil. Blocks created without a full parse (unit tests using `NewBlockForParsing`)
have `iterFields` nil and fall back to the seen-map path — functionally correct, just not
zero-alloc.

**Fallback path:** When `iterFields` is nil (block created without calling BuildIterFields,
e.g., unit tests that call `NewBlockForParsing` without full parse), IterateFields falls
back to the original seen-map path to avoid a nil-slice panic.

Back-ref: `internal/modules/blockio/reader/block.go:BuildIterFields`,
          `internal/modules/blockio/span_fields.go:IterateFields`

## 50. Pooled modulesSpanFieldsAdapter (sync.Pool)
*Added: 2026-03-23*

**Decision:** `getSpanFieldsAdapter` / `putSpanFieldsAdapter` (unexported) wrap a `sync.Pool` for
`*modulesSpanFieldsAdapter`. `NewSpanFieldsAdapter` delegates to `getSpanFieldsAdapter`.
Callers must call `ReleaseSpanFieldsAdapter` (exported) after the adapter's last use (after Clone or
after the span callback returns).

**Why:** pprof attributed ~2% of all allocations to `blockio.NewSpanFieldsAdapter`.
The struct is 2 fields (a pointer and an int) — 16 bytes — but is allocated once per
matched span per block scan at high frequency. sync.Pool eliminates repeated small-object
allocation/GC cycles.

**Release discipline:** The adapter MUST be released after `SpanMatch.Clone()` completes
(Clone materializes values out of the adapter into `materializedSpanFields`), or after the
span match callback returns without cloning. In `api.go`, the release call appears
immediately after the `fn(match)` callback returns, before the next iteration. Because Clone
replaces `out.Fields` with a `materializedSpanFields` (not the original adapter), the
original adapter pointer is not reachable from any returned `SpanMatch`.

**Do not release inside the callback:** If the caller's `fn` stores the `SpanMatch` without
cloning, it will hold a stale (pooled) adapter. The API contract is: clone if you need to
retain. This is unchanged from before; Clone already existed for this purpose.

Back-ref: `internal/modules/blockio/span_fields.go:getSpanFieldsAdapter`,
          `api.go:streamFilterProgram`,
          `api.go:streamLogProgram`

## 51. BUG-02 — tryApplyExactValues sorted uint64 boundaries as int64
*Added: 2026-03-23*

**Problem:** The `ColumnTypeRangeUint64` case in `tryApplyExactValues` copied the
`ColumnTypeRangeInt64` case verbatim: it built `[]int64` by casting uint64 bits to int64,
then sorted with `slices.Sort([]int64)`. For values with the high bit set (>= 2^63), signed
sort treated them as negative, placing them before small positive values. This produced
inverted `cd.boundaries` which the query planner uses for binary-search block pruning,
causing false-positive or false-negative block selection for uint64 columns with high-bit
values (e.g. durations near math.MaxUint64 or span:kind=5).

**Fix:** Sort as `[]uint64` (unsigned order), then reinterpret each element's bits as int64
for the wire format, matching the pattern already used in `applyOverlapUint64`.

Back-ref: `internal/modules/blockio/writer/range_index.go:tryApplyExactValues` (ColumnTypeRangeUint64 case)
Test: `internal/modules/blockio/writer/range_index_bug02_test.go:TestTryApplyExactValues_Uint64_HighBit`

## 52. BUG-07 — compareRangeKey float64 NaN total order (reader-side)
*Added: 2026-03-23*

See `internal/modules/blockio/reader/NOTES.md:NOTE-004` for full details.
**Summary:** Replaced manual `<`/`>` comparisons in `compareRangeKey` (ColumnTypeRangeFloat64)
with `cmp.Compare`, which provides a stable total order for NaN inputs.

## 53. BUG-08 — decode*Key sentinel values for malformed short keys (reader-side)
*Added: 2026-03-23*

See `internal/modules/blockio/reader/NOTES.md:NOTE-005` for full details.
**Summary:** `decodeInt64Key` now returns `math.MinInt64` and `decodeFloat64Key` returns
`math.NaN()` for short keys, instead of `0` which is a valid value that silently corrupts
binary search comparisons.

## 54. DUP-01 — Generic findBucket Replaces Four Identical Binary-Search Functions
*Added: 2026-03-23*

**Decision:** Consolidated four identical binary-search helper functions
(`findBucketInt64`, `findBucketUint64`, `findBucketFloat64`, `findBucketString`) into a
single generic `findBucket[T cmp.Ordered]` using Go 1.21+ type constraints.

**Rationale:** All four functions had byte-for-byte identical bodies; only the element type
differed. Maintaining four copies introduced risk that a bug fix applied to one copy would
not be propagated to the others (DUP class). The generic version is behaviorally identical
to all four originals — `cmp.Ordered` covers `int64`, `uint64`, `float64`, and `string`
safely without any semantic change.

**Consequence:** Callers pass a typed slice; the compiler instantiates the appropriate
version at compile time. No runtime overhead. Any future change to binary-search semantics
needs to be made in exactly one place.

Back-ref: `internal/modules/blockio/writer/range_index.go:findBucket`

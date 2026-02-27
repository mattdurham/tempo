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
*Added: 2026-02-10, updated: 2026-02-26*

**Decision:** Per-span range index values are appended to a per-block flat log
(`b.rangeVals []blockRangeValue`) during block building. After all spans in a block are
processed, `buildAndWriteBlock` consumes `b.rangeVals` and updates `w.rangeIdx` directly
(O(n) map inserts, deduplicating consecutive identical block IDs).

**Rationale:** Building the nested `rangeIndex` map directly on the per-span hot path caused
millions of map allocations per ingest batch. The per-block log approach:
1. Defers map work from the per-span loop to the post-block step (called once per 2000 spans).
2. Allows pre-allocation of `b.rangeVals` to `spanHint * 32` capacity, eliminating growslice
   during the per-span append loop.
3. Avoids any sort: the deduplication check `blockIDs[last] != bid` is O(1) because block IDs
   are written in strictly ascending order.

**Implication:** The range index (`w.rangeIdx`) is populated incrementally as blocks are
written. KLL sketches and bucket boundaries are computed from it at `Flush()` time via
`buildKLLFromRangeIndex`, after which `applyRangeBuckets` remaps exact value keys to bucket
lower-bound keys before serialization.

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

**Two-pass KLL construction (see §17):** KLL is built from the deduped range index keys at
`Flush()` time via `buildKLLFromRangeIndex`, not from per-span values during block building.
See §17 for the full rationale.

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

## 17. Two-Pass KLL Construction for Range Buckets
*Added: 2026-02-25, updated: 2026-02-26*

**Decision:** KLL quantile sketches are built from the unique keys in the deduped range
index (after all blocks are written), not from per-span values during block construction.

**History:** The original KLL implementation used `idx = count % maxSamples` (deterministic
modulo cycling), which is not reservoir sampling — it is deterministic rotation. For sorted
input (spans are sorted by service_name before block building), each slot ended up holding
only the last value that cycled through it, so the reservoir contained only the
alphabetically-last ~10% of values. This silently broke bucket boundaries: `remapStringKeys`
stored lower bounds lexicographically greater than real values, breaking the binary search
at query time and returning zero results for valid queries.

**Current state:** The KLL implementation is now a correct multi-level compaction sketch
(see §4). It handles sorted input correctly via random halving. The deterministic-modulo bug
no longer exists.

**Why the two-pass approach is still kept:**
The range index is built from unique value keys (one entry per distinct value per column
across all blocks). Feeding these to KLL weights each distinct value equally — a value that
appears in every span does not dominate bucket boundaries more than a rare value. This is
semantically correct for block pruning: bucket boundaries should divide the *value space*
uniformly, not be skewed by span frequency. Per-span streaming would overweight high-frequency
values and cluster boundaries around them.

**Two-pass approach (`buildKLLFromRangeIndex`):**
1. All blocks are written; `w.rangeIdx` accumulates unique value → block-ID mappings.
2. At `Flush()`, `buildKLLFromRangeIndex(rangeIdx)` feeds each unique key once into the
   per-column KLL sketch. Go map iteration is randomized, so there is no systematic ordering
   bias regardless of how the index was populated.
3. `applyRangeBuckets` finalizes boundaries and remaps exact value keys to bucket-range keys.

**Safety net (`remapStringKeys` / `remapBytesKeys`):** A safety clamp ensures every stored
bucket key is ≤ the actual value it covers: the lower bound is `min(bounds[bid], key)`
before truncation. This prevents any regression from silently breaking the binary search
invariant at query time.

**Bucket distribution invariant:** Range buckets MUST be approximately uniformly populated.
With `defaultRangeBuckets = 1000`, each bucket should cover roughly 0.1% of the distinct
value space. A single bucket covering 99% of block IDs provides no pruning benefit. This
invariant is guaranteed by the equal-weight-per-distinct-value property of two-pass
construction combined with the correct KLL compaction algorithm.

---

## 18. executor_test.go Uses New Executor Directly
*Added: 2026-02-25*

**As of the modules executor migration:** `internal/modules/blockio/executor_test.go`
no longer routes through `adapter.go` + `internal/executor`. It calls
`modules_executor.New().Execute(r, program, modules_executor.Options{})` directly,
with `r` being a `*modules_reader.Reader`.

**What this means:**
- EX-01 through EX-07 in `executor_test.go` are now integration tests of the real
  new execution path (writer → reader → queryplanner → executor).
- The `bloomPredicates` function in `internal/modules/executor` is exercised by every
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

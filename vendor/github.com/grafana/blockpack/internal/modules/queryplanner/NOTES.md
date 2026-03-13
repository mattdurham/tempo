# queryplanner — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`queryplanner` package. These notes complement SPECS.md and are intended to prevent
re-introducing decisions that were deliberately reversed.

---

## 1. Why a Separate Package?
*Added: 2026-02-10*

**Decision:** Block selection logic lives in `queryplanner`, not in `blockio/reader` or the
executor.

**Rationale:** The `blockio/reader` package is responsible for *how* to read blocks
(coalescing, wire parsing, caching). Deciding *which* blocks to read is a distinct concern.
Mixing them would couple the storage layer to query semantics and prevent alternative
executors from reusing the same pruning logic.

---

## 2. BlockIndexer Interface
*Added: 2026-02-10*

**Decision:** `Planner` depends on the `BlockIndexer` interface, not on `*reader.Reader`
directly.

**Rationale:** Go structural typing means `*reader.Reader` satisfies `BlockIndexer` without
any changes to the reader package. The interface:
- Allows the planner to be tested with a lightweight in-memory stub.
- Allows alternative storage backends to be plugged in.
- Makes the dependency explicit: the planner only needs `BlockCount`, `BlockMeta`, and
  `ReadBlocks`.

**Consequence:** `selection.go` must not import `blockio/reader` — it must operate on
`BlockIndexer` only.

---

## 3. Bloom Filter Was the Only Pruning Stage (Superseded)
*Added: 2026-02-10; superseded by §NOTE-QP-BLOOM-REMOVAL (2026-03-07)*

**Original decision:** The planner performs exactly one pruning stage: column-name bloom filter.

**Original rationale:** A bloom filter miss is a cheap, reliable "definitely absent" signal — no I/O,
O(1) per block. Any block whose bloom filter reports a queried column as absent cannot
contain matching spans and is safely dropped. This is the highest-leverage pruning available
without touching index structures.

**Superseded:** See §NOTE-QP-BLOOM-REMOVAL below. The column-name bloom filter has been
removed. Range-index pruning is now the primary pruning stage.

---

## 4. FetchBlocks Is a Thin Delegation
*Added: 2026-02-10*

**Decision:** `FetchBlocks` does not implement coalescing or I/O itself. It delegates
directly to `BlockIndexer.ReadBlocks`.

**Rationale:** Coalescing strategy (gap size, waste ratio) belongs to the storage layer, not
the planner. The `reader.Reader` implementation uses `AggressiveCoalesceConfig` (4 MB gap,
100% waste ratio), which is appropriate for object storage. If a different backend requires
different coalescing, it can implement `ReadBlocks` accordingly.

**Addendum (2026-02-25):** `FetchBlocks` → `BlockIndexer.ReadBlocks` is now the canonical
call site for coalesced block I/O in the modules execution path. Previously, the old
`ModulesBlockReader` adapter's `CoalesceBlocks` returned one read per block (no actual
merging). The new path through `reader.ReadBlocks` uses `AggressiveCoalesceConfig`,
merging adjacent blocks into bulk reads. See also blockio/NOTES.md §13 addendum.

---

## 5. Sorted SelectedBlocks
*Added: 2026-02-10*

**Decision:** `Plan.SelectedBlocks` is always sorted in ascending order.

**Rationale:**
- The `ReadBlocks` coalescing algorithm requires sorted input to identify adjacent ranges.
- Sorted output is predictable and easier to verify in tests.
- `setToSortedSlice` converts the `map[int]struct{}` candidate set to a sorted slice in
  O(n log n), which is negligible compared to the subsequent I/O.

---

## 6. No Caching in the Planner
*Added: 2026-02-10*

**Decision:** The planner holds no mutable state beyond a reference to `BlockIndexer`. It
performs no caching of plan results.

**Rationale:** The `BlockIndexer` implementation (`reader.Reader`) handles caching at the
storage layer. Adding a second cache layer in the planner would complicate invalidation
without meaningful benefit. Each `Plan` call is stateless and safe for concurrent use if
the underlying `BlockIndexer` is.

---

## 7. Removed: Range Column Index
*Added: 2026-02-25*

**Decision:** The cross-block range index was removed from the queryplanner.

**What was removed:**
- `RangeColumnType(col string) (shared.ColumnType, bool)` from `BlockIndexer`
- `BlocksForRange(col string, key RangeValueKey) ([]int, error)` from `BlockIndexer`
- `Values []string` and `ColType shared.ColumnType` from `Predicate`
- `PrunedByIndex int` from `Plan`
- The two-stage pruning loop in `Plan()` (bloom → range index) — now removed entirely
- The entire `applyPredicate` function and `pruneResult` struct from `selection.go`
- The `predicates.go` file in the executor package (which encoded vm.Values to wire format)

**Rationale:** The range index added significant interface surface, correctness
constraints (wire encoding, OR-query safety, bucket boundary invariants), and tight coupling
between the planner and the blockio format's internal range-bucket representation. The
Range index pruning was the secondary stage. Span-level evaluation in the executor handles all remaining filtering correctly.
Dedicated index pruning can be reintroduced as a separate, optional layer without
modifying the planner's core interface.

**Addendum (2026-02-25):** Dedicated index pruning was re-added as described in §8.
`RangeColumnType` and `BlocksForRange` are back in `BlockIndexer`.
`Predicate.Values`, `Predicate.ColType`, and `Plan.PrunedByIndex` are back.
`predicates.go` in the executor encodes `vm.Values` to wire format again.
The rationale for removal stands: the range index is now an optional second stage
that is only used when `Predicate.Values` is non-empty — the planner passes all
blocks conservatively when no equality predicates are present.

---

## 8. Range-Index Pruning Design
*Added: 2026-02-25; updated 2026-03-07 (bloom removal)*

**Decision:** Range-index pruning is the primary block-pruning stage. It operates on the
candidate set after time-range pruning (Stage 0).

**Design:**
1. `Predicate` carries `Values []string` (wire-encoded query values) and
   `ColType shared.ColumnType` (informational; used by callers to encode `Values`).
2. `pruneByIndexAll` in `selection.go` evaluates the predicate tree recursively via
   `blockSetForPred`. For leaf nodes, `leafBlockSet` calls `r.BlocksForRange(col, val)` for
   each value in `pred.Values`, unions the resulting block sets.
3. `Plan.PrunedByIndex` tracks the blocks eliminated by index lookup.
4. Range pruning is skipped when `len(pred.Values) == 0` or `len(pred.Columns) != 1`
   (OR/AND composites use `Children`; range lookup requires exactly one column per leaf).

**Concrete impact:** `{ resource.service.name = "auth" }` on 100 blocks each with 2000
distinct service names:
- Without range pruning: all 100 blocks pass (service.name is present everywhere)
- Range pruning: only the ~1-2 blocks whose range bucket includes "auth" are selected

**OR composite pruning (updated 2026-03-06):** OR composites now carry per-child
`Values` and use nil-skip semantics in `blockSetForPred`. Unconstrained (unindexed)
children are skipped rather than making the entire OR unconstrained. This is safe
because unindexed columns represent scopes with no data in the file — the row-level
`ColumnPredicate` remains authoritative. See NOTE-012 for the full safety proof.

---

## 9. Time-Range Pruning Stage
*Added: 2026-03-02*

**Decision:** `Plan()` accepts a second parameter `timeRange TimeRange` and eliminates
blocks whose `[BlockMeta.MinStart, BlockMeta.MaxStart]` window does not overlap the
query's time range before range-index pruning.

**Design:**
- `TimeRange` has `MinNano uint64` (inclusive lower bound) and `MaxNano uint64` (inclusive
  upper bound). A zero value for either field means no bound on that side.
- Stage 0 runs before the range-index stage (§8). Pruned blocks are removed from
  the candidate set before predicates are evaluated.
- `Plan.PrunedByTime int` counts eliminated blocks for observability.
- A zero `TimeRange{}` completely disables this stage — no `BlockMeta` calls are made.

**Rationale:** Log files use `MinStart`/`MaxStart` in `BlockMeta` to record the min/max
`log:timestamp` per block (see blockio/SPECS.md §11). Time-range queries like "last 1 hour"
can eliminate entire blocks cheaply using only metadata, before any I/O or range-index
evaluation. This is the highest-leverage pruning for time-bounded log queries.

**Invariant:** `BlockMeta.MinStart` and `MaxStart` must be populated by the writer. For
log files, they track `log:timestamp` (TimeUnixNano). A block with `MinStart = MaxStart = 0`
is treated as matching all time ranges (zero means "unknown", not "epoch").

**Caller contract:** Pass `queryplanner.TimeRange{}` to disable pruning (e.g. metrics
queries, structural span queries). Pass a non-zero `TimeRange` when the query has a time
filter that can be applied at the block level.

## 10. Direction via PlanWithOptions
*Added: 2026-03-02*

**Decision:** Add `Direction` (Forward/Backward) and `Limit` to `Plan` via a new
`PlanWithOptions` overload rather than extending `Plan()` with additional parameters.

**Rationale:** `Plan()` has a stable two-parameter signature. Adding a third parameter would
require updating all callers. `PlanWithOptions` is an additive overload: existing callers
of `Plan()` are unaffected, and new callers that need direction/limit use `PlanWithOptions`.

**Ordering mechanism:** The planner reverses `plan.SelectedBlocks` in-place for Backward
queries (O(N), N blocks, no allocations). The executor iterates `plan.SelectedBlocks` in
order, so the ordering is transparent to the executor — it needs no knowledge of Direction.

**BACKWARD queries with Limit:** The executor already stops early when
`len(result.Matches) >= opts.Limit`. With BACKWARD ordering, the most recent blocks are
first in `SelectedBlocks`, so the executor's early stop naturally collects the most
recent `Limit` entries without scanning all blocks. This is the key performance win for
tail-log queries (e.g. "show last 100 log lines").

---

## 11. Tree-Based AND/OR Predicate Combining
*Added: 2026-03-06*

**Decision:** Replace the flat `CombineOp` field (LogicalAND / LogicalOR per predicate)
with a recursive tree structure: `Predicate.Children []Predicate` and `Predicate.Op LogicalOp`.

**Problem with the flat approach:** The previous `CombineOp` model grouped all predicates
into an AND group and an OR group, then combined them as `AND_result ∩ OR_result`. This
supported `A && (B || C)` as a flat list but could not express `(A || B) && (C || D)` or
deeper nesting. The semantics were also subtle: an unindexed OR predicate had to mark
the entire OR group as unconstrained, and the bloom mixed-case logic had a bug where
the condition was `!AND && !OR` (union semantics) instead of `!AND || !OR` (intersection).

**Tree design:**
- A leaf (`len(Children) == 0`) has `Columns`/`Values`/`IntervalMatch` for range-index pruning.
- A composite node (`len(Children) > 0`) has `Op` specifying how children combine:
  - `LogicalAND`: block must satisfy all children (intersection of block sets).
  - `LogicalOR`: block must satisfy at least one child (union of block sets).
- The top-level `[]Predicate` passed to `Plan` is implicitly AND-combined.

**Correctness properties preserved:**
- OR nil-skip semantics: unconstrained OR children (no range index) are skipped rather
  than making the whole OR unconstrained. Only when ALL children are unconstrained does
  the OR node return nil. See NOTE-012 for the full rationale and safety proof.
- AND-conservative: unconstrained AND children are skipped; the AND node only prunes
  based on what the range index can actually determine.
- No false negatives: range index is conservative by construction.

**nil vs empty map distinction (`leafBlockSet`):**
- `nil` means "predicate not indexable" (no values, single-column requirement not met, or
  column has no range index). Callers skip this conservatively.
- Non-nil empty map means "index was consulted and found no blocks" — all candidates should
  be pruned for an AND predicate.

**Back-ref:** `internal/modules/queryplanner/selection.go:blockSetForPred`

---

## NOTE-012: OR Node Nil-Skip Semantics — Skip Unconstrained Children, Not Whole OR
*Added: 2026-03-06*

**Decision:** In `blockSetForPred`, the `LogicalOR` branch skips nil (unconstrained)
children rather than returning nil for the entire OR node when any child is unconstrained.

**Previous behaviour:** If any OR child had no range index (returned nil), the entire OR
node returned nil — treating the OR as fully unconstrained. This was correct for OR queries
where each arm is a distinct user-written condition (e.g. `{ .env = "prod" || .region = "us" }`),
because a block satisfying the unconstrained arm might match anything.

**Problem with the RangeNode redesign:** The TraceQL/LogQL compilers now expand every
unscoped attribute predicate (e.g. `.service.name = "auth"`) into an OR composite:
`{IsOR:true, Children:[resource.service.name, span.service.name, log.service.name]}`.
For log-signal files, `span.service.name` has no range index (logs don't have spans), so
the old logic returned nil for the entire OR — nullifying the range-index pruning that
`resource.service.name` could provide.

**New behaviour:** OR children that return nil are skipped. The OR node returns nil only
when ALL children are unconstrained. When at least one child IS constrained, its block set
forms the basis of the union. This is still sound:
- The pruned blocks are a superset of those the constrained child alone would keep.
- No block that satisfies ANY OR arm is pruned (the union includes all constrained arms'
  blocks; unconstrained arms contribute no pruning, so their blocks remain in candidates).

**Safety proof:** Let `S_c` = blocks returned by constrained child c. A block B is pruned
iff it's absent from the union ∪S_c. If B could satisfy an unconstrained child, B was
never in the constrained children's sets and thus cannot be pruned by them. If B is in ∪S_c,
it's kept. No false negatives.

**Unstated invariant that makes this sound:** The writer stores every `ColumnTypeString`
attribute as a `ColumnTypeRangeString` range column. A column with no range index is
therefore absent from the file entirely — no row can match via that scope. This means an
unconstrained OR child (nil from `leafBlockSet`) truly represents "no data in this scope",
not "data exists but is unindexed". Without this invariant, skipping nil children would
risk false negatives (pruning a block that has unindexed matching data).

**Back-ref:** `internal/modules/queryplanner/selection.go:blockSetForPred` (LogicalOR branch)

---

## NOTE-QP-BLOOM-REMOVAL: Column-Name Bloom Filter Removed
*Added: 2026-03-07*

**Decision:** The per-block column-name bloom filter (`BlockMeta.ColumnNameBloom [32]byte`)
and all associated queryplanner code have been removed from the pruning pipeline.

**What was removed:**
- `BlockMeta.ColumnNameBloom [32]byte` field (32 bytes per block in the wire format)
- `pruneByBloomAll`, `blockPassesBloom`, `anyColumnPresent` from `selection.go`
- `Plan.PrunedByBloom int` from the `Plan` struct
- Column-name bloom construction (`AddToBloom`) from writer block builders
- `ColumnNameBloomBits = 256`, `ColumnNameBloomBytes = 32` constants
- `AddToBloom`, `TestBloom`, `BloomHash1`, `BloomHash2`, `murmur32`, `SetBit`, `IsBitSet`
  from `blockio/shared/bloom.go` (TraceID bloom functions retained)
- `TestPlanBloomPruning`, `TestPlanORBloomPruning`, `TestPlanMixedANDORBloom`,
  `TestPlanNestedAndOrBloom` test functions
- All `assert.Equal(t, X, plan.PrunedByBloom, ...)` assertions

**Rationale:** The column-name bloom filter tested only for *column presence*, not *value
presence*. With modern blockio where almost every block contains every common column
(resource attributes are ubiquitous), the bloom filter provided near-zero pruning benefit
while occupying 32 bytes per block in the index. CMS (Count-Min Sketch) and BinaryFuse8
(value membership filter) supersede the column-name bloom for value-level pruning with
much better discrimination.

**Range-index remains:** `pruneByIndexAll` and `blockSetForPred` are retained. The
range-index stage (now Stage 1, was Stage 2) is the primary block-pruning mechanism.

**Back-ref:** `internal/modules/queryplanner/selection.go:pruneByIndexAll`

---

## NOTE-013: CMS Zero Is a Hard Prune — No False Negatives for Zero
*Added: 2026-03-07*

**Decision:** A `CMSEstimate(val) == 0` for a block is treated as a definitive "absent"
signal and prunes that block.

**Rationale:** Count-Min Sketch never under-counts (it can only over-count due to hash
collisions). A zero estimate therefore means the value was never added to the sketch for
that block — it is definitively absent. This is a safe hard prune with no false negatives.
Only non-zero estimates are ambiguous (they may be inflated by collisions).

**Contrast with FPR:** BinaryFuse8 has ~0.39% false positive rate — it can report "present"
for a value that isn't there. CMS has NO false negatives for zero — if estimate==0, the
value is definitely absent.

**Back-ref:** `internal/modules/queryplanner/scoring.go:pruneByCMSAll`

---

## NOTE-014: Score = freq / max(cardinality, 1) — Selectivity Metric
*Added: 2026-03-07*

**Decision:** Block selectivity score = `sum_preds(freq_i / max(card_i, 1))`. Higher =
more selective.

**Rationale:** A block where a queried value is frequent relative to the total number of
distinct values is a "hot" block — it concentrates the data of interest. The score is
used to rank surviving blocks (after pruning) for the executor's early-termination logic:
high-scoring blocks are processed first when direction+limit permit early exit.

- `cardinality` = `cs.Distinct()[blockIdx]` (HLL estimate of distinct values in the block)
- `freq` = `cs.TopKMatch(valFP)[blockIdx]` (exact count if in top-K) else `cs.CMSEstimate(val)[blockIdx]`
- TopK count is preferred when available because it is an exact count via FP lookup; CMS is
  used as fallback for values not in the top-K.

**Back-ref:** `internal/modules/queryplanner/scoring.go:scoreBlocks`

---

## NOTE-015: Fuse Runs Before CMS — Hard Exclusion First
*Added: 2026-03-07*

**Decision:** BinaryFuse8 pruning (Stage 2) runs before Count-Min Sketch pruning (Stage 3).

**Rationale:** Fuse gives hard binary exclusion at ~0.39% FPR: for most absent values it
immediately eliminates the block with a single hash lookup. CMS requires comparing the
frequency estimate to zero, which involves a small computation per block. Running fuse first
means CMS only sees blocks that fuse couldn't eliminate — typically the blocks that
actually contain the value. For dense datasets where most blocks contain most columns, fuse
eliminates more blocks than CMS for typical point-value queries.

**Back-ref:** `internal/modules/queryplanner/planner.go:Plan` (Stage 2 before Stage 3)

---

## NOTE-016: Column-Major Pivot — blockSet Candidates + ColumnSketch Bulk Interface
*Added: 2026-03-07; updated: 2026-03-08*

**Decision:** Three coordinated changes form the column-major pivot:
1. `blockSet = []uint64` dense bitset replaces `map[int]struct{}` for tracking candidate blocks.
2. `ColumnSketch(col string) ColumnSketch` bulk interface replaces four per-block sketch
   methods (`BlockHLL`, `BlockCMS`, `BlockContainsValue`, `BlockTopK`) on `BlockIndexer`.
3. The sketch wire format is column-major (all blocks contiguous per column) rather than
   block-major (all columns contiguous per block).

**Rationale:**

*Cache locality:* Query-time access patterns are column-major — a single predicate accesses
one column across all N blocks. Column-major wire layout makes parsing into flat arrays
straightforward: `distinct[0..N]`, `cms[0..N]`, `fuse[0..N]` are contiguous in memory.
Block-major layout requires gathering from N scattered positions for a single column query.

*Reduced allocations:* With per-block methods, each `BlockHLL(blockIdx, col)` call either
allocates a new result or accesses a cached block-level map. The bulk `ColumnSketch(col)`
call returns a pre-parsed struct allocated once at file parse time. Pruning loops read from
pre-allocated `[]uint32` / `[]bool` slices with no per-call allocation.

*Bulk array operations:* `FuseContains(hash)` returns `[]bool` of length `BlockCount()`.
`CMSEstimate(val)` returns `[]uint32`. These are fetched once per predicate/value and scanned
over the candidate set — O(1) fetches × O(numBlocks) scan, not O(numBlocks) fetches.

*blockSet efficiency:* `blockSet = []uint64` for N=256 blocks occupies 32 bytes (L1 cache
line). A Go map with 256 entries costs ~2-4KB in header + bucket overhead plus pointer
indirection. `iter` uses `bits.TrailingZeros64` to skip empty 64-bit words in O(1) per word.

**Wire format summary (writer/sketch_index.go):**
```
magic[4 LE] = 0x534B5443   // "SKTC"
num_blocks[4 LE]
num_columns[4 LE]
per column (sorted by name):
  name_len[2 LE] + name[N]
  presence[ceil(numBlocks/8)]       // 1 bit per block
  distinct[numBlocks×4 LE]          // uint32 HLL cardinality per block, 0 for absent
  topk_k[1]                         // = 20
  per present block: topk_entry_count[1] + entries (fp[8 LE] + count[2 LE])
  cms_depth[1] + cms_width[2 LE]    // = 4, 256
  per present block: cms_counters[depth×width×2 LE]
  per present block: fuse_len[4 LE] + fuse_data[fuse_len]
```

**Reader data structures:**
```go
type columnSketchData struct {
    presence  []uint64           // bitset, 1 bit per block
    distinct  []uint32           // one per block (0 for absent)
    topkFP    [][]uint64         // [presentIdx][entries] fingerprints
    topkCount [][]uint16         // [presentIdx][entries] counts
    cms       []*CountMinSketch  // [presentIdx]
    fuse      []*BinaryFuse8     // [presentIdx]
    presentMap []int             // presentMap[i] = blockIdx of i-th present block
    numBlocks  int
}
type sketchIndex struct {
    columns   map[string]*columnSketchData
    numBlocks int
}
```

**Back-ref:** `internal/modules/blockio/writer/sketch_index.go:writeSketchIndexSection`,
`internal/modules/blockio/reader/sketch_index.go:parseSketchIndexSection`,
`internal/modules/queryplanner/blockset.go`,
`internal/modules/queryplanner/column_sketch.go`

---

## NOTE-017: blockSet Replaces map[int]struct{} — Dense Bitset for Candidates
*Added: 2026-03-07*

**Decision:** The candidate block set is a `blockSet = []uint64` dense bitset rather than
`map[int]struct{}`.

**Rationale:**
- For typical file sizes (64-4096 blocks), the bitset occupies 8-512 bytes — fits in L1/L2
  cache. A map with 64 entries costs ~1-2KB in Go overhead plus pointer chasing.
- `iter` over a bitset uses `bits.TrailingZeros64` to skip empty words in O(words/64),
  not O(numBlocks).
- `and` / `or` operations on two bitsets are word-parallel — a single 64-bit AND eliminates
  64 candidate checks at once.
- `clear` during iteration is safe (we iterate a snapshot or use forward-only scanning).

**Back-ref:** `internal/modules/queryplanner/blockset.go`

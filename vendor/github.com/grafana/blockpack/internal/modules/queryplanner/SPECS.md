# queryplanner — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`queryplanner` package. It complements NOTES.md (design rationale) and TESTS.md (test plan).

---

## 1. Responsibility Boundary

The queryplanner **decides which blocks to fetch** for a query. It does not perform I/O itself
and does not parse block contents.

| Concern | Owner |
|---------|-------|
| Which blocks to read | **queryplanner** |
| How to read blocks (coalescing, wire parsing) | `blockio/reader` |
| Span-level predicate evaluation | caller (executor) |

---

## 2. BlockIndexer Interface

```go
type BlockIndexer interface {
    BlockCount() int
    BlockMeta(blockIdx int) shared.BlockMeta
    ReadBlocks(blockIndices []int) (map[int][]byte, error)
    RangeColumnType(col string) (shared.ColumnType, bool)
    BlocksForRange(col string, key shared.RangeValueKey) ([]int, error)
    BlocksForRangeInterval(col string, minKey, maxKey shared.RangeValueKey) ([]int, error)
    BlocksInTimeRange(minNano, maxNano uint64) []int
    ColumnSketch(col string) ColumnSketch
}
```

### 2.8 ColumnSketch

Returns bulk per-block sketch data for the named column, or nil when no sketch data is
available (old files or column not sketched). The returned `ColumnSketch` has methods that
return slices indexed by block number. Used by Stage 2 (Fuse pruning) and Stage 3 (block scoring).

---

## 2a. ColumnSketch Interface

```go
type ColumnSketch interface {
    // Bulk methods — return slices indexed by block number (length = BlockCount()).
    Presence() []uint64                  // bitset: 1 bit per block (1 = column present)
    Distinct() []uint32                  // HLL cardinality per block (0 for absent blocks)
    TopKMatch(valFP uint64) []uint16     // TopK count per block (0 if not in top-K or absent)
    FuseContains(valHash uint64) []bool  // fuse membership per block (true = may be present)

    // Scalar accessors — zero-allocation per-block lookups for scoring/pruning hot paths.
    // NOTE-022: eliminates per-call slice allocation from bulk methods in candidates.iter loops.
    DistinctAt(blockIdx int) uint32                       // HLL cardinality for blockIdx; 0 if absent or blockIdx < 0. O(log presentCount).
    TopKMatchAt(valFP uint64, blockIdx int) uint16        // TopK count for valFP at blockIdx; 0 if absent, blockIdx < 0, or no match. O(log presentCount + K).
    FuseContainsAt(valHash uint64, blockIdx int) bool     // bloom membership for blockIdx; false if absent or blockIdx < 0; true (conservative) if no bloom data. O(log presentCount).
}
```

All bulk methods return slices of length `BlockCount()`. For blocks where the column is absent,
`Distinct` returns 0, `TopKMatch` returns 0, and `FuseContains` returns true (conservative —
no false negatives).

### Scalar accessors

`DistinctAt`, `TopKMatchAt`, and `FuseContainsAt` provide the same data as the bulk methods
without allocating a slice. They are intended for hot-path use inside `candidates.iter` loops
where the bulk methods would allocate a fresh slice per predicate evaluation.

- **`DistinctAt(blockIdx int) uint32`**: returns `Distinct()[blockIdx]`. Returns 0 when
  `blockIdx < 0`, `blockIdx >= BlockCount()`, or the column is absent from that block.
- **`TopKMatchAt(valFP uint64, blockIdx int) uint16`**: returns `TopKMatch(valFP)[blockIdx]`.
  Returns 0 when `blockIdx < 0`, out of range, or `valFP` is not in the top-K for that block.
- **`FuseContainsAt(valHash uint64, blockIdx int) bool`**: returns `FuseContains(valHash)[blockIdx]`.
  Returns false when `blockIdx < 0` or the column is absent from that block. Returns true
  (conservative) when the block has no bloom data.

All three use binary search over `presentMap` — O(log presentCount) lookup, O(K) additional
for `TopKMatchAt` where K is the number of top-K entries per block (≤ 20).

---

## 2b. blockSet Type

```go
type blockSet []uint64
```

Dense bitset tracking candidate blocks during pruning. One bit per block, stored in
`ceil(BlockCount/64)` uint64 words. Methods:
- `allBlocks(n int) blockSet` — initialize with all N blocks set
- `test(i int) bool` — is block i set?
- `set(i int)` / `clear(i int)` — set or clear bit i
- `and(other blockSet)` / `or(other blockSet)` — bitwise AND/OR in-place
- `count() int` — number of set bits
- `iter(fn func(int))` — iterate over set bits in ascending order
- `numBlocks() int` — total capacity (len*64)

### 2.1 BlockCount

Returns the total number of blocks in the file. A return value of 0 means the file is empty;
`Plan` returns an empty plan without further lookups.

### 2.2 BlockMeta

Returns metadata for the block at `blockIdx`, including time bounds (`MinStart`/`MaxStart`)
used by the time-range pruning stage (§5.0).

**Invariant:** `blockIdx` is always in `[0, BlockCount())`. The planner never calls
`BlockMeta` with an out-of-range index.

### 2.3 ReadBlocks

Reads raw block bytes for the given sorted indices. Callers may pass the result directly to
`reader.ParseBlockFromBytes`. Implementations are expected to coalesce adjacent reads.

**Invariant:** An empty input slice returns an empty map with a `nil` error.

### 2.4 RangeColumnType

Returns the `shared.ColumnType` for a column that has a range index, and whether
the column is indexed. Returns `(0, false)` if the column has no range index.

### 2.5 BlocksForRange

Returns the sorted block indices whose range bucket covers the given wire-encoded value for
a range column. Returns an empty slice (not an error) when no blocks match. The
`key` parameter must be wire-encoded per the `Predicate.Values` contract (see §3).

### 2.6 BlocksForRangeInterval

Returns block indices from all buckets whose range overlaps [minKey, maxKey]. The
implementation includes the bucket containing minKey (largest lower ≤ minKey) through
the last bucket whose lower ≤ maxKey. Unions block IDs across all matching buckets
and returns a sorted slice.
NOTE-011: Used for case-insensitive regex prefix lookups where the query spans a
lexicographic range (e.g., all case variants from "DEBUG" to "debug").

### 2.7 BlocksInTimeRange

Returns sorted block indices whose timestamp window overlaps [minNano, maxNano] using the
per-file TS index (O(log N) binary search). Returns nil when the TS index is absent (old
files); the planner falls back to a full O(N) `BlockMeta` scan in that case.

Back-ref: `internal/modules/blockio/reader/ts_index.go:BlocksInTimeRange`
See also: blockio/SPECS.md §12 (TS index wire format).

---

## 3. Predicate

`Predicate` is a tree node for block-level pruning. It is either a **leaf** or a **composite**:

```go
type Predicate struct {
    // Leaf fields (len(Children) == 0):
    Columns       []string           // column names for range-index lookup
    Values        []string           // range OR: kept if range index contains any value
    ColType       shared.ColumnType  // informational: type used by callers to encode Values
    IntervalMatch bool               // when true, Values[0..1] = [min, max] interval

    // Composite fields (len(Children) > 0):
    Op       LogicalOp   // how Children are combined (LogicalAND or LogicalOR)
    Children []Predicate // sub-predicates; when set, leaf fields are ignored
}
```

### 3.1 Leaf nodes (len(Children) == 0)

`Columns` lists the qualified column names using the blockio convention:
- Resource attributes: `"resource.<attr>"` (e.g. `"resource.service.name"`)
- Span attributes: `"span.<attr>"`
- Intrinsics: `"span:name"`, `"trace:id"`, `"span:duration"`, etc.

Leaf pruning uses the range-index stage:

**Range stage (optional):** when `len(Values) > 0` and `len(Columns) == 1`, the planner
looks up the range index. Point lookups union `BlocksForRange` results across all values.
Interval lookups (`IntervalMatch == true`) call `BlocksForRangeInterval(Values[0], Values[1])`.
The planner returns `(blockSet, true)` with an all-zero blockSet when the index was
consulted but found no matches (so AND-combined callers prune all candidates); `(nil, false)`
means the column is not indexed.

`Values` must be wire-encoded per the range-index format:
- String / RangeString: raw string bytes
- Int64 / RangeDuration: 8-byte little-endian int64
- Uint64 / RangeUint64: 8-byte little-endian uint64
- Float64 / RangeFloat64: 8-byte little-endian IEEE-754 float64

`ColType` is informational only — callers use it when encoding `Values`. The planner
itself does not inspect `ColType`; it uses `RangeColumnType` to determine the indexed type.

### 3.2 Composite nodes (len(Children) > 0)

When `Children` is non-empty, `Op` specifies how the children's block sets combine:

- `LogicalAND`: block must satisfy **all** children (intersection).
- `LogicalOR`: block must satisfy **at least one** child (union).

Leaf fields (`Columns`, `Values`, `IntervalMatch`, `ColType`) are ignored for composite nodes.

**OR skip-unconstrained semantics:** unconstrained (`bool=false`) OR children are skipped —
they represent columns with no range index. The OR node returns `(nil, false)` (unconstrained)
only when ALL children are unconstrained. This enables pruning on scopes that DO have range
indexes, e.g. unscoped `.attr` expanded to `OR(resource.attr, span.attr, log.attr)` where
only `resource.attr` has an index still prunes using that index. See NOTE-012.

**AND conservatism:** unconstrained (`bool=false`) AND children are skipped; the intersection
is taken only over indexed children via `intersectBySelectivity`. If no AND children are
indexed, the AND node is unconstrained (`bool=false`).

### 3.3 Top-level predicate list

The `[]Predicate` passed to `Plan` is treated as a root `LogicalAND` node: every top-level
predicate must be satisfied for a block to survive. This is equivalent to:

```
{Op: LogicalAND, Children: predicates}
```

### 3.4 Expressive patterns

```
A && B                → []Predicate{{Columns:[A]}, {Columns:[B]}}
A || B (diff cols)    → []Predicate{{Op:OR, Children:[{Columns:[A]},{Columns:[B]}]}}
A && (B || C)         → []Predicate{{Columns:[A]}, {Op:OR, Children:[{B},{C}]}}
(A||B) && (C||D)      → []Predicate{{Op:OR, Children:[A,B]}, {Op:OR, Children:[C,D]}}
```

---

## 4. Plan

```go
type Plan struct {
    SelectedBlocks []int
    TotalBlocks    int
    PrunedByIndex  int
    PrunedByTime   int
    PrunedByFuse   int
    BlockScores    []float64
    Explain        string
    Direction      Direction  // ordering of SelectedBlocks; set by PlanWithOptions (default Forward)
    Limit          int        // early-termination hint (0 = no limit); set by PlanWithOptions
}
```

### 4.1 SelectedBlocks

Sorted slice of block indices to fetch. Always a subset of `[0, TotalBlocks)`.

**Invariant:** When `Direction == Forward`, `SelectedBlocks` is sorted ascending.
When `Direction == Backward`, `SelectedBlocks` is sorted descending.
Calling `Plan()` directly always returns `Direction == Forward`.

### 4.2 TotalBlocks

Equals `BlockIndexer.BlockCount()` at the time `Plan` was called.

### 4.3 PrunedByIndex

Cumulative count of blocks removed by range-index lookup across all predicates. A block
eliminated by one predicate is counted once and does not appear in subsequent predicate
evaluations. `PrunedByIndex` is 0 when no predicates carry `Values`, or when no queried
column has a range index.

### 4.4 PrunedByTime

Count of blocks eliminated by time-range comparison before the range-index stage.
`PrunedByTime` is 0 when `TimeRange` is zero.

### 4.5 PrunedByFuse

Count of blocks eliminated by BinaryFuse8 membership checks (Stage 2). A block is pruned
when the fuse filter definitively excludes all queried values for every AND-combined
top-level predicate. `PrunedByFuse` is 0 when no sketch data is available.

### 4.6 BlockScores

`[]float64` slice of length `TotalBlocks`, indexed by block number. Each element holds the
per-block selectivity score: `freq / max(cardinality, 1)`. Higher scores indicate more
selective blocks (fewer distinct values relative to query frequency).

- `0.0` means the block was not scored (no sketch data matched for that block, or block was
  pruned before scoring). Because the score formula uses `freq/max(card,1)` and `freq > 0`
  whenever a TopK match exists, any scored block has score strictly `> 0`; `0.0` reliably
  signals "unscored".
- Nil when no sketch data is available for any queried column, or no blocks remain after
  pruning.

### 4.7 Explain

ASCII trace of how the predicate tree resolved to block sets. Empty string by default.
Only populated when `PlanOptions.EnableExplain == true` is passed to `PlanWithOptions`.
File-level reject explanations (set in `plan_blocks.go`) are always populated regardless
of `EnableExplain`. See NOTE-020. Format:
```
(resource.service.name=[0,1,2] || span.service.name=nil) => [0,1,2]
AND resource.env=[1,2,3]
=> [1,2]
```

---

## 4a. TimeRange

```go
type TimeRange struct {
    MinNano uint64 // inclusive lower bound (Unix nanoseconds); 0 = no lower bound
    MaxNano uint64 // inclusive upper bound (Unix nanoseconds); 0 = no upper bound
}
```

A zero `TimeRange{}` disables time-range pruning entirely. When either field is non-zero,
blocks whose `[BlockMeta.MinStart, BlockMeta.MaxStart]` window does not overlap
`[MinNano, MaxNano]` are eliminated before the range-index stage.

---

## 5. Plan Semantics

`Plan(predicates []Predicate, timeRange TimeRange) *Plan`

### 5.0 Time-range stage (Stage 0)

When `timeRange.MinNano > 0` or `timeRange.MaxNano > 0`, for each candidate block the
planner calls `BlockMeta(blockIdx)` and checks:
- `tooOld`: `timeRange.MaxNano > 0 && meta.MinStart > timeRange.MaxNano`
- `tooNew`: `timeRange.MinNano > 0 && meta.MaxStart < timeRange.MinNano`

A block is removed when `tooOld || tooNew`. `PrunedByTime` counts removed blocks.

### 5.1 No predicates

When `predicates` is `nil` or empty, all surviving blocks (after time-range pruning) are
returned in `SelectedBlocks`. `PrunedByIndex` is 0.

### 5.2 Empty file

When `TotalBlocks == 0`, `SelectedBlocks` is `nil` and `PrunedByIndex` is 0.

### 5.3 Range-index stage (Stage 1)

`pruneByIndexAll` evaluates the top-level predicate list (AND-combined) via range index.
Each predicate's block set is computed recursively (`blockSetForPred`):

- **Leaf:** returns `(blockSet, bool)` from `leafBlockSet` — `bool=true` when the range
  index was consulted (constrained), `bool=false` when the column has no range index or no
  values are provided (unconstrained). An all-zero `blockSet` with `bool=true` means the
  index was consulted but found no matching blocks.
- **OR node:** unions children's block sets, skipping unconstrained (`bool=false`) children.
  Returns `(nil, false)` (unconstrained) only when ALL children are unconstrained. This lets
  partially-indexed OR composites (e.g. unscoped `.attr` expanded to resource/span/log scopes
  where only some scopes have a range index) still prune using the indexed children.
- **AND node:** intersects children's block sets, skipping unconstrained (`bool=false`)
  children via `intersectBySelectivity`.

`pruneByIndexAll` collects all constrained block sets from the top-level AND predicates and
intersects them. Candidates are then pruned to this intersection. `PrunedByIndex` counts the
eliminated blocks. When no predicate returns a constrained set, `pruneByIndexAll` returns
`(0, nil)` and no index pruning is applied.

**Empty vs unconstrained (`bool=false`):** an all-zero `blockSet` with `bool=true` means
"index consulted, no blocks matched" and prunes all candidates (for AND). `bool=false` means
"no index coverage — skip this predicate conservatively".

### 5.3b BinaryFuse8 pruning stage (Stage 2)

`pruneByFuseAll` uses the per-column `ColumnSketch.FuseContains` bulk method to eliminate
blocks where the fuse filter definitively excludes all queried values for every AND-combined
top-level predicate.

- For each predicate leaf, `cs.FuseContainsAt(hash, blockIdx)` is called once per block per
  value — zero allocation, O(log presentCount) per call.
- A block is pruned if it passes no predicate's fuse check (AND semantics).
- Interval predicates and non-single-column predicates are skipped (conservative pass).
- Returns `PrunedByFuse` count.

**No false negatives:** BinaryFuse8 has ~0.39% FPR (false positives only). A block that
does contain the queried value will always pass the fuse check (SPEC-SK-12).

### 5.3c Block scoring stage (Stage 3)

`scoreBlocks` computes a selectivity score for each surviving block:
`score = sum_over_predicates(freq / max(cardinality, 1))`

- `cardinality` = `cs.DistinctAt(blockIdx)` (HLL estimate; zero allocation)
- `freq` = `cs.TopKMatchAt(valFP, blockIdx)` (TopK count; zero allocation; 0 if value is not in top-K for that block)

Higher score = more selective (fewer distinct values relative to query frequency). Stored in
`Plan.BlockScores`. Nil when no sketch data is available.

### 5.4 Multiple predicates

The top-level `[]Predicate` is AND-combined: a block must satisfy every top-level predicate's
check across all stages. Use composite nodes (`Op`/`Children`) to express OR relationships —
see §3.4.

### 5.5 Safety: no false negatives

The planner only removes blocks that are **definitely** free of a queried value:
- Stage 1 (range index): range bucket is conservative — a block is only pruned when the
  value is outside all stored bucket ranges.
- Stage 2 (fuse): BinaryFuse8 has no false negatives (SPEC-SK-12).

No matching span is ever missed.

---

## 6. FetchBlocks

```go
func (p *Planner) FetchBlocks(plan *Plan) (map[int][]byte, error)
```

Delegates directly to `BlockIndexer.ReadBlocks(plan.SelectedBlocks)`. Returns a map from
block index to raw byte slice ready for `reader.ParseBlockFromBytes`.

---

## 7. Re-added: Range Column Index

The cross-block range index was previously removed (see NOTES.md §7) and has
since been re-added as an optional pruning stage (see NOTES.md §8).

`RangeColumnType`, `BlocksForRange`, `Predicate.Values`, `Predicate.ColType`, and
`Plan.PrunedByIndex` are all present in the current interface. Range pruning is skipped
when `Predicate.Values` is empty — no behaviour change for callers that do not supply values.

## 8. Direction and PlanOptions

```go
type Direction uint8
const (
    Forward  Direction = 0
    Backward Direction = 1
)

type PlanOptions struct {
    Direction     Direction
    Limit         int
    EnableExplain bool // when true, Plan.Explain is populated; false (default) = empty string
}
```

Both `Plan(predicates, timeRange)` and `PlanWithOptions(predicates, timeRange, opts)` delegate
to the internal `planInternal(predicates, timeRange, enableExplain bool)` function. `Plan()` calls
`planInternal` with `enableExplain=false` (no explain).
`PlanWithOptions` passes `opts.EnableExplain` to `planInternal`. After `planInternal` returns,
`PlanWithOptions`:
- Sets `plan.Direction = opts.Direction` and `plan.Limit = opts.Limit`.
- When `opts.Direction == Backward`, reverses `plan.SelectedBlocks` in-place.

Inside `planInternal`:
- When `enableExplain == true`, populates `plan.Explain` with the predicate trace.
- Direction, Limit, and other PlanOptions fields are NOT consulted inside planInternal.

**Invariant:** When `EnableExplain` is false (the default), `plan.Explain` is always the
empty string for predicate-level explain output. File-level reject strings (set in
`plan_blocks.go`) are unconditional. See NOTE-020.

The `Plan` struct has these fields (see §4 for full documentation):

```go
type Plan struct {
    SelectedBlocks []int
    TotalBlocks    int
    PrunedByIndex  int
    PrunedByTime   int
    PrunedByFuse   int
    BlockScores    []float64
    Explain        string
    Direction      Direction  // ordering of SelectedBlocks; set by PlanWithOptions
    Limit          int        // early-termination hint (0 = no limit)
}
```

**Invariant:** When `Direction == Forward`, `SelectedBlocks` is sorted ascending.
When `Direction == Backward`, `SelectedBlocks` is sorted descending.
Calling `Plan()` directly always returns `Direction == Forward`.

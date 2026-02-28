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
| Bloom filter pruning | **queryplanner** |
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
}
```

### 2.1 BlockCount

Returns the total number of blocks in the file. A return value of 0 means the file is empty;
`Plan` returns an empty plan without further lookups.

### 2.2 BlockMeta

Returns metadata for the block at `blockIdx`, including the column-name bloom filter
(`BlockMeta.ColumnNameBloom`). The bloom filter is a 256-bit Bloom filter (32-byte array)
over all column names present in the block.

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

---

## 3. Predicate

```go
type Predicate struct {
    Columns []string           // bloom OR: kept if any column present
    Values  []string           // range OR: kept if range index contains any value
    ColType shared.ColumnType  // column type for range lookup; 0 = bloom-only
}
```

`Columns` lists the qualified column names using the blockio convention:
- Resource attributes: `"resource.<attr>"` (e.g. `"resource.service.name"`)
- Span attributes: `"span.<attr>"`
- Intrinsics: `"span:name"`, `"trace:id"`, `"span:duration"`, etc.

A `Predicate` is used for two-stage pruning:

1. **Bloom stage:** blocks whose bloom filter reports all `Columns` as definitely absent are
   dropped. A block with any one of the columns present survives bloom pruning.

2. **Range stage (optional):** when `len(Values) > 0` and `len(Columns) == 1`, the planner
   calls `BlocksForRange` for each value, unions the returned block sets, then removes
   candidates not in the union. This stage is skipped when `Values` is empty or when
   `len(Columns) != 1` (OR bloom predicates always have `len(Columns) > 1` and skip range).

`Values` must be wire-encoded per the range-index format (SPECS §5.2.1):
- String / RangeString: raw string bytes
- Int64 / RangeDuration: 8-byte little-endian int64
- Uint64 / RangeUint64: 8-byte little-endian uint64
- Float64 / RangeFloat64: 8-byte little-endian IEEE-754 float64

---

## 4. Plan

```go
type Plan struct {
    SelectedBlocks []int
    TotalBlocks    int
    PrunedByBloom  int
    PrunedByIndex  int
}
```

### 4.1 SelectedBlocks

Sorted slice of block indices to fetch. Always a subset of `[0, TotalBlocks)`.

**Invariant:** `SelectedBlocks` is sorted in ascending order.

### 4.2 TotalBlocks

Equals `BlockIndexer.BlockCount()` at the time `Plan` was called.

### 4.3 PrunedByBloom

Cumulative count of blocks removed by bloom filter checks across all predicates. A block
eliminated by predicate 1 is counted once and does not appear in subsequent predicate
evaluations.

### 4.4 PrunedByIndex

Cumulative count of blocks additionally removed by range-index lookup across all predicates.
A block pruned by bloom is not re-counted here. `PrunedByIndex` is 0 when no predicates
carry `Values`, or when no queried column has a range index.

---

## 5. Plan Semantics

### 5.1 No predicates

When `predicates` is `nil` or empty, all blocks are returned in `SelectedBlocks`.
`PrunedByBloom` is 0.

### 5.2 Empty file

When `TotalBlocks == 0`, `SelectedBlocks` is `nil` and `PrunedByBloom` is 0.

### 5.3 Bloom filter stage

For each predicate, the planner calls `BlockMeta(blockIdx).ColumnNameBloom` for every
remaining candidate block. A block is removed from the candidate set if and only if the
bloom filter reports all `Columns` as **definitely absent** (`TestBloom` returns `false`
for every entry in `pred.Columns`).

False positives (bloom says "present" but column is actually absent) are allowed and cause
no correctness issue — the block is fetched and the caller evaluates spans at runtime.

### 5.4 Range-index stage

For each predicate with `len(Values) > 0` and `len(Columns) == 1`, after bloom pruning,
the planner unions `BlocksForRange(col, val)` results across all values in
`pred.Values`, then removes candidates not in that union. Blocks not covered by any
value's range bucket are pruned. `PrunedByIndex` accumulates the count.

This stage is skipped entirely when `pred.Values` is empty or the column has no dedicated
index (`RangeColumnType` returns `false`).

### 5.5 Multiple predicates

Multiple predicates are independent checks applied sequentially. For each predicate: bloom
stage first, then range stage. A block is removed when any predicate eliminates it. Blocks
survive only if they pass all predicate checks — equivalent to AND semantics across predicates.

### 5.6 Safety: no false negatives

The planner only removes blocks that are **definitely** free of a queried value:
- Bloom filters cannot produce false negatives.
- The range index is a range bucket: if a value falls within a bucket, the block is kept.
  Blocks are only pruned when the value is outside all stored bucket ranges.

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
since been re-added as an optional second pruning stage (see NOTES.md §8).

`RangeColumnType`, `BlocksForRange`, `Predicate.Values`, `Predicate.ColType`, and
`Plan.PrunedByIndex` are all present in the current interface. The bloom-only path remains
the default when `Predicate.Values` is empty — no behaviour change for callers that do not
supply values.

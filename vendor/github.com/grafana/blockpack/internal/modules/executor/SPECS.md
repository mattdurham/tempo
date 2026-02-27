# executor — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/executor` package.

---

## 1. Responsibility Boundary

| Concern | Owner |
|---------|-------|
| Which blocks to fetch | `queryplanner` (bloom filter) |
| Bulk block I/O | `blockio/reader` (coalescing) |
| Span-level predicate evaluation | **executor** |
| Result collection (TraceID, SpanID, limits) | **executor** |

---

## 2. Executor

```go
type Executor struct{}

func New() *Executor
func (e *Executor) Execute(r *reader.Reader, program *vm.Program, opts Options) (*Result, error)
```

`Executor` is stateless and safe for concurrent use. `New()` returns a zero-value `Executor`.

---

## 3. Execute

### 3.1 Parameters

- `r *reader.Reader` — the modules blockpack reader. A nil `r` returns an empty result
  with no error.
- `program *vm.Program` — compiled TraceQL filter (from `vm.CompileTraceQLFilter`). A nil
  `program` panics when any block contains spans (the executor calls
  `program.ColumnPredicate` for every selected block).
- `opts Options` — execution hints (see §5).

### 3.2 Execution Steps

1. Create a `queryplanner.Planner` backed by `r`.
2. Call `planner.Plan(nil)` — bloom-only pruning using column-presence predicates extracted
   from `program.Predicates.AttributesAccessed`. Currently passes `nil` (all blocks
   selected); bloom pruning happens when the caller provides explicit `Predicate` values.
3. Call `planner.FetchBlocks(plan)` — bulk coalesced I/O for all selected blocks.
4. For each selected block:
   a. `r.ParseBlockFromBytes(raw, nil, meta)` — decode all columns.
   b. `program.ColumnPredicate(provider)` — evaluate the filter closure against the block,
      returning a `RowSet` of matching row indices.
   c. For each matching row, call `spanMatchFromBlock` to extract `TraceID` and `SpanID`.
   d. Append the `SpanMatch` to `result.Matches`.
   e. If `opts.Limit > 0` and `len(result.Matches) >= opts.Limit`, return early.
5. Return `result`.

### 3.3 Error Conditions

- `FetchBlocks` error → returned as `("FetchBlocks: ...")` with nil result.
- `ParseBlockFromBytes` error → returned as `("ParseBlockFromBytes block N: ...")`.
- `program.ColumnPredicate` error → returned as `("ColumnPredicate block N: ...")`.
- Block index missing from `rawBlocks` map → silently skipped (not an error).

---

## 4. Types

### 4.1 SpanMatch

```go
type SpanMatch struct {
    SpanID   []byte
    BlockIdx int
    RowIdx   int
    TraceID  [16]byte
}
```

- `TraceID` — 16-byte trace ID, copied from the `"trace:id"` column at `RowIdx`.
- `SpanID` — 8-byte span ID, copied from the `"span:id"` column at `RowIdx`.
- `BlockIdx` — index of the block within the file.
- `RowIdx` — index of the span within the block.

If `"trace:id"` or `"span:id"` columns are absent or the row is null, the corresponding
field is left at its zero value.

### 4.2 Result

```go
type Result struct {
    Plan          *queryplanner.Plan
    Matches       []SpanMatch
    BytesRead     int64
    BlocksScanned int
}
```

- `Plan` — block selection output (TotalBlocks, SelectedBlocks, PrunedByBloom).
- `Matches` — all spans that matched `program`, in block-then-row order.
- `BytesRead` — sum of raw block byte lengths for all scanned blocks.
- `BlocksScanned` — number of blocks that were parsed and evaluated (≤ `Plan.TotalBlocks`).

### 4.3 Options

```go
type Options struct {
    Limit int
}
```

- `Limit` — maximum number of matches to return. `0` means no limit. When reached, the
  executor returns immediately without scanning remaining blocks.

---

## 5. Removed: Predicate Extraction

The executor no longer performs dedicated-index predicate extraction. See NOTES.md §6.

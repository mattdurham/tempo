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
func (e *Executor) Collect(r *modules_reader.Reader, program *vm.Program, opts CollectOptions) ([]MatchedRow, error)
func (e *Executor) CollectTopK(r *modules_reader.Reader, program *vm.Program, opts CollectOptions) ([]MatchedRow, error)
func SpanMatchFromRow(row MatchedRow, signalType uint8) SpanMatch
```

`Executor` is stateless and safe for concurrent use. `New()` returns a zero-value `Executor`.

`Collect` is the primary query entry point — it replaces the earlier `Execute` method.
`CollectTopK` is the globally-ordered variant using a heap (see §6, SPEC-STREAM-7).
`SpanMatchFromRow` extracts identity fields from a `MatchedRow` after collection.

---

## 3. Collect

### 3.1 Parameters

- `r *modules_reader.Reader` — the modules blockpack reader. A nil `r` returns `(nil, nil)`.
- `program *vm.Program` — compiled TraceQL filter (from `vm.CompileTraceQLFilter`). A nil
  `program` returns an error (`"executor.Collect: program must not be nil"`).
- `opts CollectOptions` — execution hints (see §4.3).

### 3.2 Execution Steps

1. Compute `wantColumns` from `program` and `secondPassCols` from `opts.AllColumns` (NOTE-028).
2. Create a `queryplanner.Planner` backed by `r`.
3. Call `planner.PlanWithOptions(buildPredicates(r, program), opts.TimeRange, ...)` —
   range-index, fuse, and CMS pruning based on extracted column predicates, further
   narrowed by `opts.TimeRange` if non-zero.
4. Apply sub-file sharding filter if `opts.BlockCount > 0` (see §3.4).
5. Fetch blocks lazily via `r.CoalescedGroups(plan.SelectedBlocks)` / `r.ReadGroup(group)`.
6. For each selected block:
   a. `r.ParseBlockFromBytes(raw, wantColumns, meta)` — eagerly decode predicate columns;
      lazily register all other columns (presence-only decode, full decode deferred to
      first value access). See NOTE-025.
   b. `program.ColumnPredicate(provider)` — evaluate the filter closure against the block,
      returning a `RowSet` of matching row indices.
   c. Per-row time filtering via `TimestampColumn` if set (SPEC-STREAM-4).
   d. Append `MatchedRow{Block, BlockIdx, RowIdx}` to results.
   e. If `opts.Limit > 0` and `len(results) >= opts.Limit`, return early.
7. If `opts.OnStats != nil`, invoke the deferred callback with `CollectStats`.
8. Return `[]MatchedRow`.

### 3.3 Error Conditions

- `ReadGroup` error → returned as `("ReadGroup: ...")` with nil result.
- `ParseBlockFromBytes` error → returned as `("ParseBlockFromBytes block N: ...")`.
- `program.ColumnPredicate` error → returned as `("ColumnPredicate block N: ...")`.
- Block index missing from fetched group → silently skipped (not an error).

### 3.4 Sub-File Sharding

When `opts.BlockCount > 0`, the planner's selected blocks are filtered to only include
block indices in `[opts.StartBlock, opts.StartBlock + opts.BlockCount)`. This lets the
frontend sharder partition a single blockpack file across multiple parallel jobs.

- `StartBlock` — first internal block index to include (0-based, inclusive).
- `BlockCount` — number of blocks to include. `0` means no sub-file sharding (scan all
  blocks selected by the planner).

Sub-file sharding is applied *after* planner pruning but *before* coalesced group
construction. The planner still operates on the full file, so pruning statistics
(`TotalBlocks`, `PrunedByTime`, etc.) reflect the whole file; only `SelectedBlocks` and
`FetchedBlocks` reflect the shard.

Back-ref: `internal/modules/executor/stream.go:Collect` (lines 109-121),
`internal/modules/executor/stream_topk.go:CollectTopK`

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

- `TraceID` — 16-byte trace ID, copied from `"trace:id"` (trace files) or `"log:trace_id"`
  (log files) at `RowIdx`.
- `SpanID` — span ID bytes, copied from `"span:id"` (trace files) or `"log:span_id"`
  (log files) at `RowIdx`.
- `BlockIdx` — index of the block within the file.
- `RowIdx` — index of the span/record within the block.

The column names used depend on `r.SignalType()`: `SignalTypeLog` uses `log:trace_id` /
`log:span_id`; all other signal types use `trace:id` / `span:id`.

If the relevant column is absent or the row is null, the field is left at its zero value.

### 4.2 MatchedRow

```go
type MatchedRow struct {
    Block    *modules_reader.Block
    BlockIdx int
    RowIdx   int
}
```

`Collect` and `CollectTopK` return `[]MatchedRow`. Each row holds a reference to the
parsed block and the row index within it. Use `SpanMatchFromRow(row, signalType)` to
extract identity fields (`TraceID`, `SpanID`) after collection.

Matches are in block-then-row order (no randomization). For `CollectTopK`, matches are
in globally-sorted timestamp order.

### 4.3 CollectOptions

```go
type CollectOptions struct {
    OnStats         func(CollectStats)
    TimestampColumn string
    TimeRange       queryplanner.TimeRange
    Limit           int
    Direction       queryplanner.Direction
    AllColumns      bool
    StartBlock      int
    BlockCount      int
}
```

| Field | Description |
|-------|-------------|
| `TimeRange` | Block-level time window. Zero value disables time-range pruning. |
| `Limit` | Max matches. `0` = no limit. When reached, executor returns early. |
| `Direction` | Forward (default, ascending) or Backward (descending). |
| `TimestampColumn` | `""` disables per-row time filtering. `"log:timestamp"` or `"span:start"` enables it. |
| `OnStats` | Optional; called (deferred) after execution with `CollectStats`. |
| `AllColumns` | `false` (default): second pass decodes predicate + search columns. `true`: decode all columns. Only needed for `IterateFields()` callbacks (NOTE-028). |
| `StartBlock` | First block index for sub-file sharding. See §3.4. |
| `BlockCount` | Number of blocks for sub-file sharding. `0` = scan all. See §3.4. |

### 4.4 CollectStats

```go
type CollectStats struct {
    Explain        string
    TotalBlocks    int
    PrunedByTime   int
    PrunedByIndex  int
    PrunedByFuse   int
    PrunedByCMS    int
    SelectedBlocks int
    FetchedBlocks  int
}
```

- `Explain` — ASCII trace of how the predicate tree resolved to block sets.
- `FetchedBlocks` — blocks actually read from storage (≤ `SelectedBlocks`).

### 4.5 Result (Legacy)

```go
type Result struct {
    Plan          *queryplanner.Plan
    Matches       []SpanMatch
    BytesRead     int64
    BlocksScanned int
}
```

`Result` and `Options` are retained for `ExecuteStructural` (§11) which still uses the
original return shape.


---

## 5. Predicate Extraction (buildPredicates)

`buildPredicates(r, program)` converts a compiled `vm.Program` into
`[]queryplanner.Predicate` for bloom-filter and range-index pruning.

The query compiler populates `program.Predicates` as a `*vm.QueryPredicates` containing:
- `Nodes []vm.RangeNode` — a tree of pruning predicates (AND-combined at the top level).
- `Columns []string` — additional columns needed for row-level decode (negations, `log:body`, etc.).

`buildPredicates` iterates `Nodes` and calls `translateNode` to convert each `RangeNode`
into a `queryplanner.Predicate`, preserving the tree structure (composites → Children,
leaves → Columns/Values/IntervalMatch). The resulting predicates are AND-combined by the
planner's top-level loop.

**Leaf node translation:**
- `Values` non-empty → point-lookup predicate (bloom + range-index, `IntervalMatch: false`).
- `Min` or `Max` non-nil → interval predicate (bloom + range-index, `IntervalMatch: true`).
- `Pattern` non-empty → regex prefix analysis via `vm.AnalyzeRegex`; see §5a.
- Column without range index → bloom-only predicate (no `Values`).

**Composite node translation:**
- `IsOR: true` → `queryplanner.Predicate{Op: LogicalOR, Children: ...}`
- `IsOR: false` → `queryplanner.Predicate{Op: LogicalAND, Children: ...}`

**Unscoped attributes** (e.g. `.http.method = "GET"`) are expanded at compile time into
OR composites covering `resource.*`, `span.*`, and `log.*` scoped children.

**Negation predicates** (`!=`, `!~`) produce no `Nodes` (they cannot safely prune blocks)
but do add column names to `Columns` so the two-pass decode loads the needed data.

### 5a. Regex Prefix Optimization

When a `RangeNode` carries a `Pattern`, `buildPredicates` calls
`vm.AnalyzeRegex(pattern)` to determine whether the pattern can be converted to
range-index prefix lookups.

**Optimizable patterns:** literal prefixes (`foo.*`, `^error`, `^error.*$`),
alternations of literals (`error|warn|info`). For these, the extracted prefix strings
are used for range-index pruning (see below for single vs. multi-prefix semantics).

**Case-sensitive single-prefix patterns** (`foo.*`, `^error`, or alternations with a
common prefix like `cluster-0|cluster-1`): use **interval matching** `[prefix, prefix+"\xff"]`.
Go's regex parser factors out common prefixes at parse time — `cluster-0|cluster-1`
becomes `Concat(Literal("cluster-"), CharClass([01]))`, so `AnalyzeRegex` extracts
`"cluster-"` as the single prefix. A point lookup for `"cluster-"` returns nil because
`'-'` (0x2D) < `'0'` (0x30), placing it below all actual bucket lower boundaries.
Interval matching `[prefix, prefix+"\xff"]` correctly finds all blocks whose values
start with the prefix. The `Predicate` has `IntervalMatch: true` and
`Values: [prefix, prefix+"\xff"]`.

**Case-sensitive multi-prefix patterns** (`error|warn|info`): produce multiple distinct
string prefixes via `Alternate([error, warn, info])` (no common-prefix factoring).
These use **union point lookups** (`BlocksForRange` per prefix) with `IntervalMatch: false`.

**Case-insensitive patterns** (`(?i)debug`) with a **single prefix**: use **interval
matching** for range-index pruning. The all-uppercase prefix is the min key and the
all-lowercase prefix + `"\xff"` is the max key. The `\xff` suffix ensures buckets with
lower boundaries extending beyond the prefix (e.g., `"debug-service"`) are included.
All range-index buckets whose lower boundary falls within `[UPPER, lower\xff]` are
included via `BlocksForRangeInterval`. The `Predicate` has `IntervalMatch: true` and
`Values: [upperPrefix, lowerPrefix\xff]`.

**Case-insensitive alternations** (`(?i)(error|warn)`): fall back to **bloom-only**
predicates. Each prefix would need a separate interval, and different prefixes may span
non-overlapping lexicographic ranges.

**Non-optimizable patterns** (`.*foo`, `[a-z]+`): fall through to bloom-only predicates.

Back-ref: `internal/modules/executor/predicates.go:translateRegexNode`,
`internal/vm/regex_optimize.go:AnalyzeRegex`

See NOTES.md §10 for encoding details and rationale.
See NOTES.md §11 for regex optimization design decisions.

---

## 6. Collect / CollectTopK Invariants

The SPEC-STREAM invariants apply to `Collect` and `CollectTopK`. The earlier `Stream`
callback-based method has been removed; `Collect` now uses the same lazy coalesced-group
I/O path (see NOTE-035).

### 6.1 Invariants

- **SPEC-STREAM-1:** Nil reader returns `(nil, nil)`. Nil `program` returns an error.
- **SPEC-STREAM-2:** Blocks are fetched lazily via `CoalescedGroups`/`ReadGroup` (~8 MB per I/O). Never per-column or per-block individual reads.
- **SPEC-STREAM-3:** `FetchedBlocks <= SelectedBlocks`. `FetchedBlocks` is incremented at `ReadGroup` time by the count of blocks in the fetched group (actual I/O). Groups that are skipped due to early stop are not counted.
- **SPEC-STREAM-4:** `TimestampColumn == ""` disables per-row time filtering (trace mode). `TimestampColumn == "log:timestamp"` enables per-row `[MinNano, MaxNano]` checks.
- **SPEC-STREAM-5:** Direction is applied at plan time (`PlanWithOptions`). Within each block, when `TimestampColumn` is set, rows are sorted by per-row timestamp: ascending for Forward (oldest first), descending for Backward (newest first). When `TimestampColumn` is empty (trace mode), rows are reversed for Backward direction.
- **SPEC-STREAM-6:** `OnStats` is deferred; `FetchedBlocks` reflects actual I/O at the time execution completes (including early-stop paths).
- **SPEC-STREAM-7:** `CollectTopK` guarantees the returned rows are the globally top-`Limit` entries by per-row timestamp. A heap of size `Limit` is maintained: min-heap for Backward (evict oldest), max-heap for Forward (evict newest). Requires `TimestampColumn` to be set. When `Limit == 0`, delegates to `Collect`. Block-level early termination: blocks with `MaxStart <= heap.min` (Backward) or `MinStart >= heap.max` (Forward) are skipped entirely (no I/O). Results are delivered in sort order after the scan completes (not lazily).
- **SPEC-STREAM-8:** `QueryOptions.MostRecent` in the public API (`api.go`) maps to `Direction: Backward` + `TimestampColumn: "span:start"` in `CollectOptions`. When `true` with no `Limit`, blocks are traversed in reverse `BlockMeta.MinStart` order and rows within each block are sorted by `span:start` descending — this is locally newest-first but does not guarantee global ordering when block time ranges overlap. When `true` with a `Limit`, `CollectTopK` is used instead, which guarantees the returned spans are the globally top-`Limit` by `span:start`. `span:start` is always in `searchMetaColumns` so no extra I/O is needed. Default (`false`) is forward with no timestamp sort. Only applies to filter queries; structural queries collect all blocks regardless.
  Back-ref: `api.go:streamFilterProgram`

Back-ref: `internal/modules/executor/stream.go:Collect`, `internal/modules/executor/stream.go:CollectOptions`, `internal/modules/executor/stream_topk.go:CollectTopK`

---

## 7. StreamLogs

### 7.1 Signature

```go
func StreamLogs(
    r *modules_reader.Reader,
    program *vm.Program,
    pipeline *logqlparser.Pipeline,
    fn LogEntryCallback,
) error
```

### 7.2 Types

```go
type LogEntry struct {
    Labels         logqlparser.LabelSet
    LogAttrs       map[string]string
    Line           string
    TimestampNanos uint64
}

type LogEntryCallback func(entry *LogEntry) bool
```

`LogAttrs` holds `log.*` `ColumnTypeString` column values keyed by their full column name
(e.g. `"log.detected_level"`). These are original LogRecord attributes, distinct from
body-auto-parsed fields (`ColumnTypeRangeString`). `LogAttrs` is nil when no such columns
are present. Callers (e.g. `logEntryFields`) must emit these with the `"log."` prefix intact
so that downstream SM extraction can identify them by prefix.

### 7.3 Invariants

- **SPEC-SL-1:** Nil reader returns nil error; `fn` is never called.
- **SPEC-SL-2:** Nil pipeline is valid; rows are delivered without pipeline transformation.
- **SPEC-SL-3:** Labels are built lazily from `resource.*` and `log.*` columns with prefixes
  stripped. On the pre-parsed block path, a `blockLabelSet` backed by block columns is
  used — no map allocation until `Materialize()` is called.
  `resource.service.name` → label key `service.name`; `log.level` → label key `level`.
  Callers that need a `map[string]string` call `Labels.Materialize()`. This must be done
  inside the `LogEntryCallback` — the `LabelSet` backing is released to the pool after
  the callback returns.
- **SPEC-SL-4:** Returning `false` from `fn` stops iteration; no further blocks are read.
- **SPEC-SL-5:** `Pipeline.Process` is called per matched row; it may mutate the `LabelSet`.
  The `LabelSet` passed to the pipeline is backed by block columns on the hot path.

Back-ref: `internal/modules/executor/stream_log.go:StreamLogs`

---

## 8. StreamLogsTopK

### 8.1 Signature

```go
func StreamLogsTopK(
    r *modules_reader.Reader,
    program *vm.Program,
    pipeline *logqlparser.Pipeline,
    opts StreamOptions,
    fn LogEntryCallback,
) error
```

### 8.2 Behaviour

StreamLogsTopK collects the globally top `opts.Limit` log rows by per-row timestamp,
applying the pipeline per row before inserting into the heap. Results are delivered via
`fn` in sort order after the full scan completes. When `opts.Limit == 0`, all
pipeline-passing rows are collected, sorted, and delivered.

This is the primary log streaming entry point for `api.go`'s pipeline path.

### 8.3 Invariants

- **SPEC-SLK-1:** The heap stores `*LogEntry` (post-pipeline) not block/row references.
  The pipeline may mutate labels in-place; results must be captured at scan time.
- **SPEC-SLK-2:** Block-level pruning: once the heap is full, blocks whose timestamp
  range cannot improve the heap root are skipped (no I/O, no parsing). For Backward:
  blocks with `MaxStart <= heap.root.ts` are skipped. For Forward: blocks with
  `MinStart >= heap.root.ts` are skipped. Guards: `MaxStart == 0` / `MinStart == 0`
  are treated as unknown and never skipped.
- **SPEC-SLK-3:** Uses `planner.Plan` with empty `TimeRange` (no planner-level time
  pruning). Per-row time filtering is applied manually via `opts.TimeRange.MinNano` and
  `opts.TimeRange.MaxNano`. This avoids false-negative regressions when block-level time
  pruning is combined with pipeline row filtering.
- **SPEC-SLK-4:** Per-row time filtering is applied before the pipeline (fast path: no
  allocation for rows outside the time window). Nil pipeline is valid; rows pass through
  without transformation.
- **SPEC-SLK-5:** Results are delivered in sort order after the full scan completes (not
  lazily). Backward: newest first. Forward: oldest first.
- **SPEC-SLK-6:** `OnStats` is deferred; `FetchedBlocks` reflects actual I/O at the time
  execution completes. `FetchedBlocks` counts per-group, not per-block.

Back-ref: `internal/modules/executor/stream_log_topk.go:StreamLogsTopK`

---

## 9. ExecuteLogMetrics

### 9.1 Signature

```go
func ExecuteLogMetrics(
    r *modules_reader.Reader,
    program *vm.Program,
    pipeline *logqlparser.Pipeline,
    querySpec *vm.QuerySpec,
    funcName string,
    groupBy []string,
) (*LogMetricsResult, error)
```

### 9.2 Types

```go
type LogMetricsResult struct {
    Rows          []LogMetricsRow
    BytesRead     int64
    BlocksScanned int
}

type LogMetricsRow struct {
    Values   map[string]float64 // bucket-key → aggregated value
    GroupKey []string           // group-by label values
}
```

### 9.3 Supported Functions

| `funcName` | Description |
|---|---|
| `count_over_time` | Count of matching log lines per bucket |
| `rate` | count_over_time / step_seconds |
| `bytes_over_time` | Sum of log line byte lengths per bucket |
| `bytes_rate` | bytes_over_time / step_seconds |
| `sum_over_time` | Sum of unwrap values per bucket |
| `avg_over_time` | Average of unwrap values per bucket |
| `min_over_time` | Minimum of unwrap values per bucket |
| `max_over_time` | Maximum of unwrap values per bucket |
| `quantile_over_time` | q-th percentile of unwrap values per bucket (nearest-rank method) |

### 9.4 Invariants

- **SPEC-ELM-1:** Nil reader returns empty `LogMetricsResult` with no error.
- **SPEC-ELM-2:** Nil `querySpec` returns an error (`"ExecuteLogMetrics: querySpec cannot be nil"`). Callers must always provide a non-nil `querySpec`.
- **SPEC-ELM-3:** Time bucketing uses `vm.QuerySpec.Step`, `Start`, `End` (nanoseconds).
  Rows outside `[Start, End]` are ignored.
- **SPEC-ELM-4:** `groupBy` is the list of label names for group aggregation. An empty list
  produces a single group containing all rows.
- **SPEC-ELM-5:** Unwrap functions (`sum_over_time`, `avg_over_time`, `min_over_time`,
  `max_over_time`) read the numeric value from `logqlparser.UnwrapValueKey` in the labels
  map (set by `StageUnwrap`). Rows without this key are skipped.
- **SPEC-ELM-6:** `quantile_over_time` requires accumulating all numeric values per bucket
  during the scan phase (`aggBucketState.values []float64`), then computing the q-th
  percentile post-scan using the nearest-rank method. The quantile parameter `q` comes from
  `vm.QuerySpec.Aggregate.Quantile` (0 ≤ q ≤ 1). Rows without `logqlparser.UnwrapValueKey`
  are skipped. Returns 0 for empty buckets.
  Back-ref: `internal/modules/executor/metrics_log.go:aggBucketState`,
  `internal/modules/executor/metrics_log.go:logRowValues`,
  `internal/modules/executor/metrics_log.go:logComputeQuantile`

Back-ref: `internal/modules/executor/metrics_log.go:ExecuteLogMetrics`

---

## 10. ExecuteTraceMetrics

### 10.1 Signature

```go
func ExecuteTraceMetrics(
    r *modules_reader.Reader,
    program *vm.Program,
    querySpec *vm.QuerySpec,
) (*TraceMetricsResult, error)
```

### 10.2 Types

```go
type TraceMetricLabel struct {
    Name  string
    Value string
}

type TraceTimeSeries struct {
    Labels []TraceMetricLabel
    Values []float64
}

type TraceMetricsResult struct {
    Series        []TraceTimeSeries
    BytesRead     int64
    BlocksScanned int
}
```

### 10.3 Supported Functions

| `AggregateSpec.Function` | Description |
|---|---|
| `COUNT` | Count of matching spans per bucket |
| `RATE` | count / step_seconds per bucket |
| `SUM` | Sum of field values per bucket |
| `AVG` | Average of field values per bucket |
| `MIN` | Minimum field value per bucket |
| `MAX` | Maximum field value per bucket |
| `HISTOGRAM` | Log2-bucketed histogram; each series has a `__bucket` label (log2 lower boundary). Span is assigned to bucket `pow(2, floor(log2(v)))`. For `span:duration` the value is first converted from nanoseconds to seconds. Returns 0 (not NaN) for empty histogram cells. |
| `QUANTILE` | q-th percentile of field values per time bucket (nearest-rank method). `q` from `querySpec.Aggregate.Quantile` (0 ≤ q ≤ 1). NaN for empty buckets. Reuses `logComputeQuantile`. |
| `STDDEV` | Sample standard deviation of field values per time bucket (Welford online algorithm). NaN when bucket count < 2 (sample stddev undefined). |

### 10.4 Invariants

- **SPEC-ETM-1:** Labels are an ordered slice; the same label name will not appear twice.
- **SPEC-ETM-2:** `Values[i]` is NaN when no data exists for bucket i (except COUNT/RATE which use 0).
- **SPEC-ETM-3:** `len(Values)` == numBuckets where `numBuckets = ceil((EndTime-StartTime)/StepSizeNanos)`.
- **SPEC-ETM-4:** Nil reader returns empty `TraceMetricsResult` with no error. Note: the public
  wrapper `ExecuteMetricsTraceQL` (api.go) returns an error for a nil reader.
- **SPEC-ETM-5:** Nil `querySpec` returns an error (`"ExecuteTraceMetrics: querySpec cannot be nil"`).
- **SPEC-ETM-6:** Time bucketing uses `vm.QuerySpec.TimeBucketing.StartTime`, `EndTime`,
  `StepSizeNanos` (nanoseconds). Spans outside `[StartTime, EndTime)` are skipped.
  Bucket index = `(spanStart - StartTime) / StepSizeNanos`.
- **SPEC-ETM-7:** `span:start` is used for time bucketing. Rows where `span:start` is absent
  or the column is nil are silently skipped (not counted).
- **SPEC-ETM-8:** `GroupBy` is the list of attribute paths from `querySpec.Aggregate.GroupBy`
  (already normalized by `spec.Normalize()`). Missing attributes produce an empty string label.
  An empty GroupBy list produces a single series covering all matching spans.
- **SPEC-ETM-9:** `wantColumns` passed to `ParseBlockFromBytes` always includes `span:start`,
  the aggregate field (if any), and all `GroupBy` columns in addition to predicate columns.
  Failure to include these columns in wantColumns silently returns 0 results (data loss).
- **SPEC-ETM-10:** Aggregate field columns of non-numeric type (string, bool, bytes) are silently
  skipped per row — they do not increment `count` and do not affect the bucket value.
- **SPEC-ETM-11:** Series in the output are sorted by their label string for deterministic output.
  Within a series, Values[i] corresponds to bucket i (0-indexed).

Back-ref: `internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`

---

## 11. ExecuteStructural

```go
type StructuralResult struct {
    Matches []SpanMatch
}

func (e *Executor) ExecuteStructural(
    r *reader.Reader,
    q *traceqlparser.StructuralQuery,
    opts Options,
) (*StructuralResult, error)
```

### 11.1 Overview

`ExecuteStructural` executes a TraceQL structural query (e.g. `{ A } >> { B }`) against a
modules blockpack Reader. It is the counterpart to `Execute` for structural operators.

### 11.2 Three-Phase Algorithm

1. **Collect** — Scan all blocks. For each span, record spanID, parentID, and whether it
   matches the left and/or right filter. Results are keyed by `[16]byte` trace ID.
2. **Resolve** — For each trace, build a `spanID→index` map and set each span's `parentIdx`
   field. `parentIdx = -1` means root (no known parent in this file).
3. **Evaluate** — Apply the structural operator to each trace's span list. Emit matching
   right-side spans as `SpanMatch` entries.

Back-ref: `internal/modules/executor/stream_structural.go:ExecuteStructural`

### 11.3 Invariants

- **SPEC-STRUCT-1:** Nil reader returns `&StructuralResult{}` with no error.
- **SPEC-STRUCT-2:** All blocks are scanned — structural queries bypass bloom and range
  pruning because any block may contain spans from either side of the operator.
- **SPEC-STRUCT-3:** A nil left or right `FilterExpression` in `StructuralQuery` compiles to
  a nil program, which matches all rows (wildcard `{}`).
- **SPEC-STRUCT-4:** `Options.Limit > 0` caps the number of entries in `StructuralResult.Matches`.
  Matches are deduplicated per trace (a span may only appear once per trace result).
- **SPEC-STRUCT-5:** `SpanMatch.SpanID` is an 8-byte raw slice. `SpanMatch.TraceID` is a
  `[16]byte` value. `BlockIdx` and `RowIdx` are zero (structural queries aggregate across
  blocks; per-block row positions are not meaningful in the result).

### 11.4 Structural Operators

| Operator | Symbol | Semantics |
|----------|--------|-----------|
| OpDescendant  | `>>`  | R is any descendant of L (walk R's ancestor chain) |
| OpChild       | `>`   | R's direct parent is L |
| OpSibling     | `~`   | R shares a parent with a leftMatch span, R ≠ L |
| OpAncestor    | `<<`  | R is any ancestor of L (walk L's parent chain) |
| OpParent      | `<`   | R is the direct parent of L |
| OpNotSibling  | `!~`  | R is rightMatch with no leftMatch sibling at the same parent |

Back-ref: `internal/modules/executor/stream_structural.go:applyStructuralOp`

---

## 12. Intrinsic-Guided Block Pruning (Phase 3)

The intrinsic-guided pruning subsystem uses the file-level intrinsic columns section
(present in v4 footer files) to narrow block selection before the standard planner pass.

### 12.1 ProgramIsIntrinsicOnly

**SPEC-IC-1:** `ProgramIsIntrinsicOnly(program *vm.Program) bool`

Returns `true` when all column references in the program can be served from the intrinsic
columns section without reading any block data. Returns `false` if:
- `program` is nil
- The program has no predicates (match-all query)
- Any referenced column is not in `traceIntrinsicColumns` or `logIntrinsicColumns`

Intrinsic trace columns: `trace:id`, `span:id`, `span:parent_id`, `span:name`, `span:kind`,
`span:start`, `span:end`, `span:duration`, `span:status`, `span:status_message`,
`resource.service.name`

Intrinsic log columns: `log:timestamp`, `log:observed_timestamp`, `log:severity_number`,
`log:severity_text`, `log:trace_id`, `log:span_id`, `log:flags`, `resource.service.name`

Back-ref: `internal/modules/executor/predicates.go:ProgramIsIntrinsicOnly`

### 12.2 BlocksFromIntrinsicTOC

**SPEC-IC-2:** `BlocksFromIntrinsicTOC(r *Reader, program *vm.Program) []int`

Returns the set of block indices that **may** contain rows matching the given predicate
program, using only the intrinsic column TOC (min/max per column). Conservative filter:
returns nil (no pruning) when:
- `r.HasIntrinsicSection()` is false (v3 footer or empty TOC)
- `ProgramIsIntrinsicOnly(program)` is false

**Phase 3 (current):** Stub — always returns nil when conditions above are met (the
infrastructure is in place but TOC-based min/max pruning is not yet implemented).

**Phase 3b (future):** Use per-column min/max from the TOC to skip blocks whose entire
range does not overlap the query range, without loading column blobs.

Back-ref: `internal/modules/executor/predicates.go:BlocksFromIntrinsicTOC`

### 12.3 Column Sets

`traceIntrinsicColumns` and `logIntrinsicColumns` are package-level maps in `predicates.go`
that enumerate which column names are covered by the intrinsic section. They are used
exclusively by `ProgramIsIntrinsicOnly`. `resource.service.name` is included in both sets
as a "practically intrinsic" column (written for every span/log record).

### 12.4 Phase 3b Note

Full intrinsic-guided pruning (loading column blobs and doing exact row-level filtering
without reading blocks) is deferred to Phase 3b. The stub in `BlocksFromIntrinsicTOC`
ensures the code path exists and is tested. The API is stable; Phase 3b only fills in the
implementation body.

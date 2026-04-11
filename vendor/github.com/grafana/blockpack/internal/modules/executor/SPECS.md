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
func Collect(r *modules_reader.Reader, program *vm.Program, opts CollectOptions) ([]MatchedRow, QueryStats, error)
func SpanMatchFromRow(row MatchedRow, signalType uint8, r *modules_reader.Reader) SpanMatch
```

`Collect` is the primary query entry point — it replaces the earlier `Execute` method.
When `TimestampColumn` and `Limit` are both set, `Collect` uses a heap-based scan
to guarantee globally correct top-K results (see §6, SPEC-STREAM-7).
`SpanMatchFromRow` extracts identity fields from a `MatchedRow` after collection.
`QueryStats` is always returned (never nil); callers may ignore it with `_`.

---

## 3. Collect

### 3.1 Parameters

- `r *modules_reader.Reader` — the modules blockpack reader. A nil `r` returns `(nil, QueryStats{}, nil)`.
- `program *vm.Program` — compiled TraceQL filter (from `vm.CompileTraceQLFilter`). A nil
  `program` returns an error (`"executor.Collect: program must not be nil"`).
- `opts CollectOptions` — execution hints (see §4.3).

### 3.2 Execution Steps

1. Compute `wantColumns` from `program` and `opts.SelectColumns` (via `ProgramWantColumns`), and `secondPassCols` from `opts.AllColumns` (NOTE-028). When `opts.SelectColumns` is non-empty and the program has no predicate columns, `wantColumns` is derived from `SelectColumns` alone, enabling storage-level column projection.
2. Create a `queryplanner.Planner` backed by `r`.
3. Call `planner.PlanWithOptions(BuildPredicates(r, program), opts.TimeRange, ...)` —
   range-index and membership pruning (SketchBloom) based on extracted column predicates, further
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
7. Populate and return `QueryStats` with `ExecutionPath`, `TotalDuration`, and per-step `Steps` (see §4.4).
8. Return `([]MatchedRow, QueryStats, nil)`.

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

Back-ref: `internal/modules/executor/stream.go:Collect` (lines 109-121)

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
    Block           *modules_reader.Block
    // IntrinsicFields is set when the result was produced without reading full block
    // data — either Case B (pure intrinsic + sort) or Case A with a range predicate
    // (pure intrinsic + no sort + hasRangePredicate=true, see NOTE-047). Block is nil
    // when IntrinsicFields is set. Case A with equality predicates populates Block via
    // forEachBlockInGroups and leaves IntrinsicFields nil. The caller should use
    // IntrinsicFields for field lookups when Block is nil.
    IntrinsicFields modules_shared.SpanFieldsProvider
    BlockIdx        int
    RowIdx          int
}
```

`Collect` returns `[]MatchedRow`. Each row holds a reference to the
parsed block and the row index within it. Use `SpanMatchFromRow(row, signalType, r)` to
extract identity fields (`TraceID`, `SpanID`) after collection.

Matches are in block-then-row order (no randomization). When `TimestampColumn` and
`Limit` are both set, matches are in globally-sorted timestamp order.

### 4.3 CollectOptions

```go
type CollectOptions struct {
    TimestampColumn string
    TimeRange       queryplanner.TimeRange
    Limit           int
    Direction       queryplanner.Direction
    AllColumns      bool
    SelectColumns   []string
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
| `AllColumns` | `false` (default): second pass decodes predicate + search columns. `true`: decode all columns. Only needed for `IterateFields()` callbacks (NOTE-028). |
| `SelectColumns` | Storage-level column projection. When non-empty, these column names are merged into `wantColumns` so that only the requested columns are decoded from block blobs. For queries with no predicate columns (e.g. `{}`), this prevents decoding all columns. Predicate columns are always included regardless of this list. |
| `StartBlock` | First block index for sub-file sharding. See §3.4. |
| `BlockCount` | Number of blocks for sub-file sharding. `0` = scan all. See §3.4. |

### 4.4 QueryStats and StepStats

`Collect` and `CollectLogs` return a `QueryStats` value as their second return instead of
calling an `OnStats` callback. `QueryStats` is always non-zero; callers may discard it
with `_`.

```go
type QueryStats struct {
    ExecutionPath string
    TotalDuration time.Duration
    Steps         []StepStats
}

type StepStats struct {
    Name      string
    Duration  time.Duration
    BytesRead int64
    IOOps     int
    Metadata  map[string]any
}
```

**`ExecutionPath`** — one of the path constants identifying which code branch ran (see §6.1 SPEC-STREAM-6). NOTE-044: "intrinsic-topk-sort" renamed to "intrinsic-topk-kll".

**`Steps`** — ordered list of per-phase stats. Only phases that actually ran appear in the slice. Step `Name` values and their `Metadata` keys:

| Step name | Appears when | Metadata keys |
|-----------|-------------|---------------|
| `"plan"` | Block-scan path (not intrinsic fast path) | `total_blocks`, `selected_blocks`, `pruned_by_time`, `pruned_by_index`, `pruned_by_fuse`, `explain` |
| `"intrinsic"` | Cases A, B, C, D fast paths | Case A plain: `selected_blocks`. Case B KLL: `ref_count`, `scan_count` (=0). Case B scan: `ref_count`, `scan_count`. |
| `"mixed-prefilter"` | Cases C, D mixed paths | `candidate_blocks` |
| `"block-scan"` | Block-scan path | `fetched_blocks`, `matched_rows` |

**`QueryStats.Explain()`** — convenience method that returns the `"explain"` string from
the `"plan"` step's Metadata, or `""` if the plan step is absent or has no explain value.
Back-ref: `internal/modules/executor/query_stats.go:QueryStats.Explain`

**`QueryStats.SelectedBlocks()`** — convenience method that returns the `"selected_blocks"` int
from the `"plan"` step's Metadata, or `0` if the plan step is absent.
Back-ref: `internal/modules/executor/query_stats.go:QueryStats.SelectedBlocks`

### 4.5 Result (Legacy)

```go
type Result struct {
    Plan          *queryplanner.Plan
    Matches       []SpanMatch
    BytesRead     int64
    BlocksScanned int
}
```

`Options` is shared with `ExecuteStructural` (§11). `ExecuteStructural` returns `*StructuralResult` (not `*Result`) — see §11 for its distinct return type.


---

## 5. Predicate Extraction (BuildPredicates)

`BuildPredicates(r, program)` converts a compiled `vm.Program` into
`[]queryplanner.Predicate` for bloom-filter and range-index pruning.

The query compiler populates `program.Predicates` as a `*vm.QueryPredicates` containing:
- `Nodes []vm.RangeNode` — a tree of pruning predicates (AND-combined at the top level).
- `Columns []string` — additional columns needed for row-level decode (negations, `log:body`, etc.).

`BuildPredicates` iterates `Nodes` and calls `translateNode` to convert each `RangeNode`
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

When a `RangeNode` carries a `Pattern`, `BuildPredicates` calls
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

## 6. Collect Invariants

The SPEC-STREAM invariants apply to `Collect`. The earlier `Stream`
callback-based method has been removed; `Collect` now uses the same lazy coalesced-group
I/O path (see NOTE-035).

### 6.1 Invariants

- **SPEC-STREAM-1:** Nil reader returns `(nil, nil)`. Nil `program` returns an error.
- **SPEC-STREAM-2:** Blocks are fetched lazily via `CoalescedGroups`/`ReadGroup`. When
  `wantColumns` is set, the reader SHOULD use sub-block column I/O (SPEC-005) to transfer
  only the bytes for the needed columns rather than the full block. When `wantColumns` is
  nil (match-all), the full block is fetched as before. The unit of lazy fetch remains the
  coalesced group, but each group's read volume is bounded by the sum of the wanted
  columns' byte ranges across the group's blocks rather than the full block sizes.
- **SPEC-STREAM-3:** `StepStats.IOOps` counts `ReadGroup` calls (one per coalesced group fetched). `"fetched_blocks"` in step Metadata counts individual blocks fetched. Both values exclude groups skipped due to early stop.
- **SPEC-STREAM-4:** `TimestampColumn == ""` disables per-row time filtering (trace mode). `TimestampColumn == "log:timestamp"` enables per-row `[MinNano, MaxNano]` checks.
- **SPEC-STREAM-5:** Direction is applied at plan time (`PlanWithOptions`). Within each block, when `TimestampColumn` is set, rows are sorted by per-row timestamp: ascending for Forward (oldest first), descending for Backward (newest first). When `TimestampColumn` is empty (trace mode), rows are reversed for Backward direction.
- **SPEC-STREAM-6:** `Collect` always returns a non-zero `QueryStats` as its second value.
  `QueryStats.ExecutionPath` identifies the code path: one of `"intrinsic-plain"`,
  `"intrinsic-topk-kll"`, `"intrinsic-topk-scan"`, `"mixed-plain"`, `"mixed-topk"`,
  `"intrinsic-need-block-scan"`, `"block-plain"`, `"block-topk"`, `"block-pruned"`, or
  `"bloom-rejected"`.
  `QueryStats.TotalDuration` is the wall-clock duration of the full `Collect` call.
  `QueryStats.Steps` contains one entry per phase that ran; see §4.4 for step names and
  metadata keys. The `"plan"` step is absent for intrinsic fast paths (Cases A–D) since
  `planBlocks` is not called on those paths. When the intrinsic path falls through to the
  full block scan (errNeedBlockScan), `slog.Warn` is emitted with the fast-path's
  `ExecutionPath` and `total_blocks` fields. See NOTE-043, NOTE-044.
- **SPEC-STREAM-7:** When `TimestampColumn` and `Limit` are both set, `Collect` guarantees the returned rows are the globally top-`Limit` entries by per-row timestamp. A heap of size `Limit` is maintained: min-heap for Backward (evict oldest), max-heap for Forward (evict newest). Block-level early termination: blocks with `MaxStart <= heap.min` (Backward) or `MinStart >= heap.max` (Forward) are skipped entirely (no I/O). Results are delivered in sort order after the scan completes (not lazily). Back-ref: `internal/modules/executor/stream_topk.go:topKScanBlocks`
- **SPEC-STREAM-8:** `QueryOptions.MostRecent` in the public API (`api.go`) maps to `Direction: Backward` + `TimestampColumn: "span:start"` in `CollectOptions`. When `true` with no `Limit`, blocks are traversed in reverse `BlockMeta.MinStart` order and rows within each block are sorted by `span:start` descending — this is locally newest-first but does not guarantee global ordering when block time ranges overlap. When `true` with a `Limit`, `Collect` is used with `TimestampColumn` set, which guarantees the returned spans are the globally top-`Limit` by `span:start` (SPEC-STREAM-7). Default (`false`) is forward with no timestamp sort. Only applies to filter queries; structural queries collect all blocks regardless.
  *Addendum (2026-03-25):* The original spec stated "`span:start` is always in `searchMetaColumns` so no extra I/O is needed." This is no longer accurate. `span:start` is now served via `traceIntrinsicColumns` injection into `secondPassCols` in `Collect` (see NOTE-050). When `wantColumns != nil && !opts.AllColumns`, `span:start` enters `secondPassCols` via the `traceIntrinsicColumns` loop, ensuring it is decoded for the per-row sort. When `opts.AllColumns = true`, `secondPassCols` is nil (decode all columns), which also covers `span:start`. Both paths guarantee `span:start` is available for sorting; no extra I/O is required.
  Back-ref: `api.go:streamFilterProgram`, `internal/modules/executor/stream.go:Collect`
- **SPEC-STREAM-9:** When `opts.Limit > 0` and the program contains at least one
  intrinsic predicate (`hasSomeIntrinsicPredicates` returns true), `Collect` runs an
  intrinsic pre-filter before the full block scan. The pre-filter dispatches on
  `(ProgramIsIntrinsicOnly × TimestampColumn != "")`:
  - Pure intrinsic + no sort (Case A): adaptive dispatch on `hasRangePredicate(program)`:
      - Range predicates (Min or Max set, e.g. `duration>100ms`): `lookupIntrinsicFields`
        reads field values from cached intrinsic blobs — zero internal block reads.
        `MatchedRow.IntrinsicFields` is populated; `MatchedRow.Block` is nil.
      - Equality predicates (Values set, e.g. `status=error`, `svc=X`): `forEachBlockInGroups`
        reads only the internal blocks containing matched refs — targeted I/O.
        `MatchedRow.Block` is populated; `MatchedRow.IntrinsicFields` is nil.
      The range path is faster when refs are spread across many blocks (sorted flat column
      scan scatters matches). The equality path is faster when refs cluster in 1-3 blocks
      (dict-equality matches are sparse and highly localised). See NOTE-047.
  - Pure intrinsic + sort: group refs by block, order blocks by BlockMeta.MaxStart DESC
    (KLL path NOTE-044), build packed-key→timestamp map from tsCol (O(N)), look up each
    ref's timestamp, sort M pairs, return IntrinsicFields rows (zero block reads).
  - Mixed + no sort: fetch candidate blocks only, re-evaluate with ColumnPredicate,
    collect up to limit.
  - Mixed + sort: fetch candidate blocks only, re-evaluate with ColumnPredicate,
    topKScanRows → topKDeliver (global top-K preserved by superset invariant).
  The pre-filter falls through to the full block scan when no usable intrinsic
  constraint exists (e.g., all top-level nodes are OR with non-intrinsic children).
  Back-ref: `internal/modules/executor/stream.go:collectFromIntrinsicRefs`,
            `internal/modules/executor/stream.go:collectIntrinsicTopK` (Case B KLL path:
            group refs by block, MaxStart DESC sort, O(N) map build, O(M log M) sort)

Back-ref: `internal/modules/executor/stream.go:Collect`, `internal/modules/executor/stream.go:CollectOptions`, `internal/modules/executor/stream.go:collectFromIntrinsicRefs`

---

## SPEC-STREAM-10: Intrinsic Column Separation Invariants

*Updated 2026-03-26: PR #172 (exclusive-intrinsic storage) was rolled back — see writer
NOTE-002 and executor NOTE-052. Intrinsic columns are now written to BOTH block column
payloads AND the intrinsic TOC section (dual storage). The three mechanisms below remain
correct and are now conservative no-ops for block-scan (block columns are populated, so
nilIntrinsicScan is never triggered). They still protect against future format versions
that omit intrinsic columns from block payloads, and the intrinsic fast paths (Cases A–D)
continue to rely on the intrinsic TOC section.*

Three mechanisms implement the dual-storage intrinsic model (NOTE-050, NOTE-051, NOTE-052).
They work together so that block-column ColumnPredicate and intrinsic post-filtering
never interfere with each other.

### SPEC-STREAM-10.1: nilIntrinsicScan — FullScan on absent intrinsic column

When `Block.GetColumn(name)` returns nil **and** `name` is an intrinsic column name
(e.g. `resource.service.name`, `span:name`, `span:kind`, `trace:id`, `span:id`), the
column scan returns a **FullScan** (all rows pass) rather than an empty row set.

This is required so that AND intersection with user-attribute predicates is not
short-circuited to empty — the intrinsic filter is applied later by
`filterRowSetByIntrinsicNodes`. Without this invariant, every query against a v4 file
with an intrinsic predicate would return 0 results.

Contract: absent intrinsic column in block payload → FullScan, not empty.
Back-ref: `internal/modules/executor/column_provider.go:nilIntrinsicScan`

### SPEC-STREAM-10.2: userAttrProgram — strip intrinsic RangeNodes for block-column scan

`userAttrProgram` returns a shallow copy of the Program with all RangeNode leaves whose
`Column` field is an intrinsic column name removed. This copy is used during block
column ColumnPredicate evaluation so that intrinsic predicates are not evaluated against
nil block columns (which would cause false-negative results).

Contract: `userAttrProgram` returns nil when **all** predicates are intrinsic, and the
caller treats nil as match-all (proceeds to `filterRowSetByIntrinsicNodes` for the full
row set). Composite nodes with no remaining user-attribute children are also omitted
(empty OR/AND nodes must not be emitted into the filtered node list).
Back-ref: `internal/modules/executor/predicates.go:userAttrProgram`,
          `internal/modules/executor/predicates.go:filterIntrinsicNodes`

### SPEC-STREAM-10.3: filterRowSetByIntrinsicNodes — post-filter via intrinsic section

After ColumnPredicate, `filterRowSetByIntrinsicNodes` enforces intrinsic predicates
against the file-level intrinsic section. It calls `lookupIntrinsicFields` to materialise
field values for each candidate row, then evaluates each row against the intrinsic
RangeNodes.

Contract: rows absent from intrinsic data (field not present in the intrinsic section)
**fail** the predicate — absent value does not match any equality or range predicate.

Mixed-OR limitation (NOTE-051): when a non-intrinsic leaf appears inside an OR node
alongside intrinsic leaves, `rowSatisfiesIntrinsicNodes` evaluates the non-intrinsic
leaf as false (it has no intrinsic value). This is a known limitation — such queries
fall through to the full block-scan path instead.
Back-ref: `internal/modules/executor/stream.go:filterRowSetByIntrinsicNodes`,
          `internal/modules/executor/stream.go:lookupIntrinsicFields`

---

## 7. StreamLogs

### 7.1 Signature

```go
func StreamLogs(
    r *modules_reader.Reader,
    program *vm.Program,
    pipeline *logqlparser.Pipeline,
) ([]LogEntry, error)
```

### 7.2 Types

```go
type LogAttrs struct {
    Names  []string
    Values []string
}

type LogEntry struct {
    LokiLabels     string
    Line           string
    LogAttrs       LogAttrs
    TimestampNanos uint64
}
```

`LokiLabels` holds the raw value of the `resource.__loki_labels__` column for the row
(e.g. `{service_name="api", env="prod"}`). `LogAttrs` holds `log.*` `ColumnTypeString`
column values as parallel `Names`/`Values` slices (e.g. `Names=["log.detected_level"]`,
`Values=["info"]`). These are original LogRecord attributes, distinct from body-auto-parsed
fields (`ColumnTypeRangeString`). Both slices are nil when no such columns are present.
Callers (e.g. `logEntryFields`) must emit these with the `"log."` prefix intact so that
downstream SM extraction can identify them by prefix.

### 7.3 Invariants

- **SPEC-SL-1:** Nil reader returns nil, nil; result is empty.
- **SPEC-SL-2:** Nil pipeline is valid; rows are delivered without pipeline transformation.
- **SPEC-SL-3:** `LokiLabels` holds the value of `resource.__loki_labels__` for the row.
  This is the only resource-label field returned; building a full `map[string]string` per
  row was wasteful because consumers only ever read this one key.
- **SPEC-SL-4:** `Pipeline.Process` is called per matched row; it may drop or modify the row.
- **SPEC-SL-5:** `Pipeline.Process` is called per matched row; it may mutate labels in-place.

Back-ref: `internal/modules/executor/stream_log.go:StreamLogs`

---

## 8. CollectLogs

### 8.1 Signature

```go
func CollectLogs(
    r *modules_reader.Reader,
    program *vm.Program,
    pipeline *logqlparser.Pipeline,
    opts CollectOptions,
) ([]LogEntry, QueryStats, error)
```

### 8.2 Behaviour

CollectLogs collects the globally top `opts.Limit` log rows by per-row timestamp,
applying the pipeline per row before inserting into the heap. Results are returned as a
`[]LogEntry` slice in sort order after the full scan completes. When `opts.Limit == 0`, all
pipeline-passing rows are collected, sorted, and returned.

This is the primary log collection entry point for `api.go`'s pipeline path.

### 8.3 Invariants

- **SPEC-SLK-1:** The heap stores `LogEntry` by value (post-pipeline) not block/row references.
  Value storage eliminates per-row heap allocation. The pipeline may mutate labels in-place;
  results must be captured at scan time.
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
- **SPEC-SLK-5:** Results are returned in sort order after the full scan completes (not
  lazily). Backward: newest first. Forward: oldest first.
- **SPEC-SLK-6:** `CollectLogs` returns a `QueryStats` as its second value. The `"plan"`
  step records `total_blocks` and `selected_blocks`; the `"block-scan"` step records
  `fetched_blocks` (number of individual blocks fetched across all groups), `IOOps`
  (number of `ReadGroup` calls, i.e. coalesced group fetches), and `BytesRead` (sum of
  `BlockMeta.Length` for all blocks in fetched groups, including blocks skipped by
  block-level pruning within the group). `TotalDuration` is wall-clock for the full call.
  `ExecutionPath` is `"block-topk"` when `opts.Limit > 0`, else `"block-plain"`.

Back-ref: `internal/modules/executor/stream_log_topk.go:CollectLogs`

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
  Note: when called via `ExecuteMetricsTraceQL` (api.go), `StartTime` and `EndTime` are the
  step-aligned values (`alignedStart`, `alignedEnd`), not the raw caller-supplied `StartNano`/`EndNano`.
- **SPEC-ETM-4:** Nil reader returns empty `TraceMetricsResult` with no error. Note: the public
  wrapper `ExecuteMetricsTraceQL` (api.go) returns an error for a nil reader.
- **SPEC-ETM-5:** Nil `querySpec` returns an error (`"ExecuteTraceMetrics: querySpec cannot be nil"`).
- **SPEC-ETM-6:** Time bucketing uses `vm.QuerySpec.TimeBucketing.StartTime`, `EndTime`,
  `StepSizeNanos` (nanoseconds). Intervals are **right-closed**: spans outside `(StartTime, EndTime]`
  are skipped (start exclusive, end inclusive). This matches Tempo `IntervalMapperQueryRange` semantics.
  Bucket index: `offset = spanStart - StartTime; idx = offset / StepSizeNanos;`
  `if offset % StepSizeNanos == 0 { idx-- }` (spans at exact step boundaries belong to the previous bucket).
  Back-ref: `internal/modules/executor/metrics_trace.go:traceAccumulateRow`,
  `internal/modules/executor/metrics_trace_intrinsic.go:executeTraceMetricsIntrinsic`.
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

func ExecuteStructural(
    r *modules_reader.Reader,
    q *traceqlparser.StructuralQuery,
    opts Options,
) (*StructuralResult, error)
```

### 11.1 Overview

`ExecuteStructural` executes a TraceQL structural query (e.g. `{ A } >> { B }`) against a
modules blockpack Reader. It is the counterpart to `Collect` for structural operators.

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

- **SPEC-STRUCT-6:** `OpNotDescendant (!>>)` — a rightMatch span R passes if NO span in
  R's ancestor chain is a leftMatch. Root spans (parentIdx == -1) have an empty ancestor
  chain and always pass. Contrast with `>>` (OpDescendant) which requires at least one
  ancestor to be leftMatch. All blocks are still scanned (SPEC-STRUCT-2 applies).
  Back-ref: `internal/modules/executor/stream_structural.go:evalOpNotDescendantStruct`

- **SPEC-STRUCT-7:** `OpNotChild (!>)` — a rightMatch span R passes if R has no parent
  (parentIdx == -1) or R's direct parent is not a leftMatch. Root spans trivially pass.
  Contrast with `>` (OpChild) which requires the direct parent to be leftMatch.
  All blocks are still scanned (SPEC-STRUCT-2 applies).
  Back-ref: `internal/modules/executor/stream_structural.go:evalOpNotChildStruct`

### 11.4 Structural Operators

| Operator | Symbol | Semantics |
|----------|--------|-----------|
| OpDescendant  | `>>`  | R is any descendant of L (walk R's ancestor chain) |
| OpChild       | `>`   | R's direct parent is L |
| OpSibling     | `~`   | R shares a parent with a leftMatch span, R ≠ L |
| OpAncestor    | `<<`  | R is any ancestor of L (walk L's parent chain) |
| OpParent      | `<`   | R is the direct parent of L |
| OpNotSibling  | `!~`  | R is rightMatch with no leftMatch sibling at the same parent |
| OpNotDescendant | `!>>` | R is rightMatch but NO ancestor of R is leftMatch |
| OpNotChild      | `!>`  | R is rightMatch and R's direct parent is NOT leftMatch (or R has no parent) |

Back-ref: `internal/modules/executor/stream_structural.go:applyStructuralOp`

---

## 12. Pipeline Aggregate Queries (streamPipelineQuery)

Invoked when `QueryTraceQL` receives a `*traceqlparser.MetricsQuery` (e.g.
`{ filter } | count() > N` or `{ filter } | avg(span.latency_ms)`).

### 12.1 Algorithm

1. Compile and run the filter expression with `Limit: 0` to collect all matching spans.
2. If `pipeline == nil` or `pipeline.Aggregate.Name == ""`, emit all spans directly
   (respecting `opts.Limit`). No grouping or aggregation is performed.
3. Group spans into spansets keyed by `SpanMatch.TraceID` string.
4. For each spanset in insertion order, compute the aggregate value.
5. If `pipeline.HasThreshold == true`, discard spansets whose aggregate does not satisfy
   `aggVal <op> thresholdVal`.
6. Emit all spans from passing spansets, respecting `opts.Limit`.

### 12.2 Supported Aggregates

| Name | Field required | Description |
|---|---|---|
| `count` / `count_over_time` | No | Number of spans in the spanset |
| `avg` | Yes | Arithmetic mean of numeric field values |
| `min` | Yes | Minimum of numeric field values |
| `max` | Yes | Maximum of numeric field values |
| `sum` | Yes | Sum of numeric field values |

### 12.3 Invariants

- **SPEC-PA-1:** The aggregate field is looked up via `getSpanFieldNumeric`, which matches
  `int64`, `uint64`, `float64`, and `int` field values. String and bool fields are
  silently skipped per span. The field name supports scope-prefix tolerance:
  `"span:duration"` matches `"duration"`; `"span.latency_ms"` matches `"latency_ms"`.
  The aggregate field MUST appear in the filter expression for its column to be
  included in `wantColumns`. Queries of the form `{ } | avg(span.latency_ms)` where
  the field does not appear in the filter will silently return 0 results because
  `streamPipelineQuery` does not set `AllColumns: true`. Use `{ span.latency_ms > 0 }`
  or equivalent to ensure the field is loaded.
  Back-ref: `api.go:getSpanFieldNumeric`

- **SPEC-PA-2:** When `avg/min/max/sum` finds no numeric field values in a spanset, the
  aggregate returns `(0, false)` and the spanset is silently skipped — it is not emitted
  even when `HasThreshold == false`.
  Back-ref: `api.go:computeSpansetAggregate`

- **SPEC-PA-3:** `count` / `count_over_time` always returns `(float64(len(spans)), true)`
  regardless of field names. It cannot return `ok == false`.
  Back-ref: `api.go:computeSpansetAggregate`

- **SPEC-PA-4:** If total matched spans exceeds `maxPipelineSpans` (1,000,000), the query
  returns an error immediately before grouping. Callers must narrow their filter expression.
  Back-ref: `api.go:streamPipelineQuery`

- **SPEC-PA-5:** Spans are grouped by `SpanMatch.TraceID` string. Spans with an empty
  TraceID are grouped under the sentinel key `"__no_trace_id__"`.
  Back-ref: `api.go:streamPipelineQuery`

- **SPEC-PA-6:** When `pipeline.HasThreshold == false`, all spansets with a successful
  aggregate (`ok == true`) are emitted. This covers queries like `{ } | count()` with no
  comparison operator.
  Back-ref: `api.go:streamPipelineQuery`

- **SPEC-PA-7:** When `pipeline == nil` or `pipeline.Aggregate.Name == ""` (pipeline with
  no aggregate, e.g. only `by()`/`select()` clauses), all filtered spans are emitted
  directly, respecting `opts.Limit`. No grouping or aggregation is performed.
  Back-ref: `api.go:streamPipelineQuery`

Back-ref: `api.go:streamPipelineQuery`, `api.go:computeSpansetAggregate`,
`api.go:getSpanFieldNumeric`, `api.go:compareThreshold`

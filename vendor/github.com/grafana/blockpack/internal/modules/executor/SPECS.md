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
func Collect(ctx context.Context, r *modules_reader.Reader, program *vm.Program, opts CollectOptions) ([]MatchedRow, QueryStats, error)
func SpanMatchFromRow(row MatchedRow, signalType uint8, r *modules_reader.Reader) (SpanMatch, error)
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

1. Compute `wantColumns` via `ProgramWantColumns(program)`. Apply `opts.SelectColumns` via a two-path strategy: (a) **no predicate columns** (`wantColumns == nil`, e.g. `{}`): promote `SelectColumns` to the sole first-pass filter so only the requested output columns are decoded from every block; (b) **predicate columns present**: do NOT add `SelectColumns` to `wantColumns` — output-only columns are deferred to `secondPassCols` so they are decoded only for blocks that match the filter, not every scanned block. Compute `secondPassCols` via `computeColumnFilters` (NOTE-028): when `opts.AllColumns=true`, `computeColumnFilters` returns early after setting `wantColumns`, leaving `secondPassCols=nil`; the second parse pass then decodes all columns unconditionally.
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

Back-ref: `internal/modules/executor/stream.go:Collect` (lines 125+); sub-file sharding filter at lines 248-256

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
| `AllColumns` | `false` (default): second pass decodes predicate + search columns. `true`: `computeColumnFilters` returns early, leaving `secondPassCols=nil`, so the second pass decodes all columns — all columns available after parsing. Only needed for `IterateFields()` callbacks (NOTE-028). For queries with predicate columns, `SelectColumns` is NOT in `wantColumns` (it would normally be deferred to `secondPassCols`); `SelectColumns` has no effect when `AllColumns=true`. |
| `SelectColumns` | Storage-level column projection. When non-empty, these column names are merged into `wantColumns` so that only the requested columns are decoded from block blobs. For queries with no predicate columns (e.g. `{}`), this prevents decoding all columns. Predicate columns are always included regardless of this list. Has no effect when `AllColumns=true`. |
| `StartBlock` | First block index for sub-file sharding. See §3.4. |
| `BlockCount` | Number of blocks for sub-file sharding. `0` = scan all. See §3.4. |

### 4.4 QueryStats and StepStats

`Collect` returns a `QueryStats` value as its second return instead of
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

Contract addendum (NOTE-076): when `filterRowSetByIntrinsicNodes` evaluates an OR composite
node whose ALL children are non-intrinsic leaves, `rowSatisfiesIntrinsicNodesOR` returns
`true` (pass-through), not `false`. Non-intrinsic children within an OR have already been
validated by `ColumnPredicate`; the post-filter has no intrinsic constraint to enforce for
them. Composite children (AND or OR sub-groups) within an OR are treated as constrained —
their recursive evaluation result is authoritative.
Back-ref: `internal/modules/executor/predicates.go:rowSatisfiesIntrinsicNodesOR`

---

## SPEC-STREAM-11: Unified Bounded Sliding-Window Block Pipeline

*Added: 2026-04-15*

All block-scan paths (scanBlocks, forEachBlockInGroups, ExecuteTraceMetrics,
topKScanBlocks) use `blockGroupPipeline` for I/O.
NOTE-449 resolved NOTE-058: all callers now propagate a real `context.Context`; the
`context.Background()` placeholders have been removed.
Back-ref: `internal/modules/executor/stream_topk.go:topKScanBlocks`

**Invariants:**

- Dispatcher is semaphore-gated: at most `workerCount` groups are dispatched ahead of
  `nextExpected` at any time. The semaphore token is released only AFTER `processGroup`
  returns, ensuring the dispatcher cannot enqueue a new group while the current group's
  data is still held in memory. Therefore the pending reorder map holds at most
  `workerCount-1` out-of-order groups. Total peak in-memory = at most `workerCount`
  groups (`workerCount-1` pending + 1 being processed in `processGroup`).
  Back-ref: `block_group_pipeline.go:blockGroupPipeline` (sem channel)
- At most `workerCount` (constant `defaultPipelineWorkers = 8`; see NOTE-058 for the
  I/O-latency-hiding rationale and why GOMEMLIMIT scaling was rejected) coalesced
  groups are dispatched ahead of the consumer at any time.
- I/O (ReadGroup) is concurrent across W goroutines. Parse+process is sequential on the
  consumer goroutine. `Reader.ParseBlockFromBytes` is not goroutine-safe.
- Early exit: `processGroup` returning `errLimitReached` cancels further dispatch.
  In-flight ReadGroup calls complete normally; their results are drained and discarded
  without calling `processGroup`.
- Group delivery order: the consumer reorders completions by groupIdx so `processGroup`
  is always called in the same group sequence as `CoalescedGroups` returns.
- Stats: `fetchedGroups` counts ReadGroup completions; `fetchedBlocks` sums BlockIDs
  across fetched groups; `bytesRead` sums `BlockMeta.Length` per fetched block.
  Groups whose ReadGroup was cancelled before dispatch are NOT counted. Groups that
  completed ReadGroup but were buffered in the reorder map when errLimitReached fires
  ARE counted in fetchedGroups/fetchedBlocks/bytesRead.
- `ExecuteStructural` is exempt: it requires all blocks pre-fetched for parent-resolve
  phase (NOTE-034) and continues to use `ReadBlocks` / `queryplanner.FetchBlocks`.

Back-ref: `internal/modules/executor/block_group_pipeline.go:blockGroupPipeline`

---

## SPEC-STREAM-12: Second-pass decode gate in forEachBlockInGroups

*Added: 2026-04-15*

`forEachBlockInGroups` accepts an optional `preFn func(pb parsedBlock, candidates []int) bool`
parameter. When non-nil, `preFn` is called after the first-pass column decode with the
first-pass parsed block and the candidate row indices. If `preFn` returns false, the
second-pass decode and `fn` are both skipped for that block. When `preFn` is nil it is not
called and the second-pass decode proceeds as usual.

**Invariants:**

- Callers with a column predicate (collectMixedPlain, collectMixedTopK) pass a preFn that
  evaluates `program.ColumnPredicate`. On success it returns `rowSet.Size() > 0`; on
  evaluation error it returns `true` (conservative passthrough) so fn can surface the
  error with full block context via the shared preFnErr captured variable.
- Intrinsic-only callers (collectIntrinsicPlain, collectIntrinsicTopK) pass nil — no
  column predicate evaluation is required in those paths.
- When `preFn` returns false, `fn` is never called for that block; no second-pass I/O or
  allocation occurs.
- `preFn` and `fn` are called sequentially on the same goroutine (processGroup is
  sequential per SPEC-STREAM-11); closures may capture shared state without locks.

Back-ref: `internal/modules/executor/stream.go:forEachBlockInGroups`
Back-ref: `internal/modules/executor/stream_prefn_test.go:TestForEachBlockInGroupsPreFnNilPassthrough`
Back-ref: `internal/modules/executor/stream_prefn_test.go:TestForEachBlockInGroupsPreFnFalseSkips`
Back-ref: `internal/modules/executor/stream_prefn_test.go:TestForEachBlockInGroupsPreFnTruePassthrough`

---

## SPEC-STREAM-13: Bounded, Pointed Newest-First Block Collection (`RecentFirstBudget`)
*Added: 2026-07-08 (issue #481 parts 2-3, F-2, team-lead ruling R14/R14-AMENDED)*

**Contract:** when `CollectOptions.RecentFirstBudget != nil`, `Collect`'s block-selection/fetch
path enforces three independent, optional caps — `MaxBlocks`, `MaxBytes`, `MaxDuration` (a zero
field means no cap on that dimension) — stopping block collection at the FIRST one reached, on top
of the existing `Direction=Backward`/`WantSort=false` newest-first read order this strategy
requires (see `NOTE-VI-099` for why `WantSort` must be false: `RecentFirstBudget` never routes
through `shouldUseTopKPath`'s globally-correct-but-exhaustive heap scan, which reads ALL selected
blocks regardless of budget).

**`MaxBlocks` is enforced EXACTLY, at plan time.** Before coalescing, `scanBlocks` truncates the
already newest-first-reversed `SelectedBlocks` list to at most `MaxBlocks` entries
(`stream.go`, ~line 321-329) — this is a hard, concurrency-independent bound: at most `MaxBlocks`
blocks are ever fetched, regardless of coalescing group boundaries or pipeline concurrency.

**`MaxBytes`/`MaxDuration` are checked once per coalesced I/O GROUP, not per block or per row**
(`stream.go`, ~line 471-603): `bytesSoFar` accumulates cumulative bytes read across processed
groups; `scanStart`/`time.Since` tracks elapsed wall-clock time. Both checks are cheap to compute
unconditionally (a no-op when `RecentFirstBudget` is nil, the normal unbounded path).

**R14-AMENDED — the concurrent-prefetch overshoot bound.** Because `blockGroupPipeline` dispatches
up to `defaultPipelineWorkers` (8) groups' I/O CONCURRENTLY via a pre-filled semaphore — it does
not wait for a per-group budget decision before issuing the next group's I/O — the actual I/O
overshoot bound is `min(remaining groups, defaultPipelineWorkers)` groups' worth of wasted
prefetch, not a flat "one group," for a file with ≤8 total groups every group may already be in
flight before any check can act. **This is an accepted tradeoff, not a correctness gap**: the caps
exist to prevent the catastrophic unbounded-read class (multi-hundred-second/OOM queries), and a
≤8-group file is inherently small (≤~64 MB of coalesced reads). **The RESULT SET is unaffected by
this overshoot and remains an exact, concurrency-independent bound** — results only ever reflect
groups whose `processGroup` call completed before a stop fired (`processGroup` runs strictly in
ascending group order), so a group whose I/O was wastefully prefetched but never reached
`processGroup` never contributes rows to the answer. Callers must report ACTUAL bytes/blocks read
from `QueryStats` (never the configured budget values), so any overshoot is visible to the user,
never hidden behind the cap number.

**Never routes through the top-K path.** `shouldUseTopKPath` gates purely on
`WantSort`/`TimestampColumn`/`Limit` — `RecentFirstBudget`'s callers always set `WantSort=false`,
so the gate is false regardless of `Limit`, pinned directly at the gate function
(`recentfirst_test.go` in `internal/modules/executor`), independent of how any particular caller
constructs `CollectOptions`.

**Design rationale (why truncate-before-coalesce for an exact bound; why the concurrent-prefetch
overshoot is acceptable):** `NOTE-VI-099`.

Back-ref: `internal/modules/executor/stream.go` (`scanBlocks` truncation, `bytesSoFar`/`scanStart`
setup, per-group budget check), `internal/modules/executor/recentfirst.go:RecentFirstBudget`
(`SPEC-ROOT-023`'s executor-local mirror), `internal/modules/executor/collectoptions.go`. Tests:
`recentfirst_test.go` (root, real-write-path suite: `MaxBlocks`/`MaxBytes`/`MaxDuration` stopping,
newest-first ordering, never-routes-through-topK, including the `Limit>0`+`MaxBytes`/`MaxDuration`-only
regression coverage added by issue #71's post-mortem), `internal/modules/executor/recentfirst_test.go`
(`shouldUseTopKPath` gate pin). Issue #481.

**RETIRED (2026-07-12, plan-scan-fallback.md Phase 7, task #190).** `CollectOptions.RecentFirstBudget`
and its consumer code in `stream.go` (`scanBlocks`' truncation, the per-group `bytesSoFar`/
`scanStart` budget checks) have been REMOVED, along with `internal/modules/executor/recentfirst.go`
(file deleted outright — confirmed, no longer present in the tree) and its dedicated test suite.
`Collect` no longer has a bounded, pointed newest-first read mode distinct from the ordinary
`Direction`/`TimestampColumn`/`Limit`-driven paths (`SPEC-STREAM-5`/`7`/`8`). See `SPEC-ROOT-023`
for the full removal rationale (this mechanism's replacement, `SPEC-VI-12`'s early-stopping index
resolution, serves the case this budget existed for via a categorically different mechanism — a
resolution-time index walk, not a capped raw-block scan — so there is no successor entry within
THIS file for the removed mechanism itself).

Back-ref (current): confirmed absent — `internal/modules/executor/recentfirst.go` and
`recentfirst_test.go` no longer exist in the tree.

---

## SPEC-STREAM-14: `topKScanBlocks` Group-Level Early Stop and Direction-Aware Group Order
*Added: 2026-07-12 (issue #197)*

**Problem this closes:** `topKScanBlocks` (the `shouldUseTopKPath` heap scan — SPEC-STREAM-7)
previously fetched EVERY coalesced I/O group selected by the planner regardless of `Limit`,
even once the heap was already provably saturated with unbeatable entries. Unlike
`scanBlocks`' `streamSortedRows` (SPEC-STREAM-11's `errLimitReached` signal), `processGroup`
inside `topKScanBlocks` never returned `errLimitReached`, so `blockGroupPipeline`'s
dispatch-cancellation mechanism never engaged. A `Backward`+`Limit` query over an
intrinsic-only predicate (no attribute-leaf column for the value index to prune on, e.g.
`{ duration > 1ms }`) therefore paid for the full file's I/O regardless of `Limit`.

**Fix has two required, cooperating parts — neither alone is sufficient:**

1. **Direction-aware group traversal order.** `CoalesceBlocks`/`CoalescedGroups`
   (`reader/coalesce.go`) always returns groups in ascending file-offset order, independent of
   the caller's block order (see `reader/reader_test.go`'s
   `TestCoalescedGroups_PreservesCallerBlockOrder`, which locks this in — `CoalesceBlocks`
   itself is intentionally NOT changed by this fix). `topKScanBlocks` now re-slices (not
   re-coalesces) the group list it was given: for `backward=true` it reverses the slice so the
   highest-offset (newest) groups are dispatched and processed first. This is pure in-memory
   re-slicing with no I/O and no change to `CoalesceBlocks`'/`CoalescedGroups`' contract.
   Without this, the early-stop below could never fire for a Backward query, because the true
   top-`Limit` answer is only known after the newest (in ascending order, LAST) group is fetched.
2. **Heap-saturation early stop.** `topKGroupBound` aggregates the direction-relevant timestamp
   extreme (`MaxStart` for backward, `MinStart` for forward) across every block in one coalesced
   group, purely from already-resident `BlockMeta` (no I/O) — the group-granularity analogue of
   `topKSkipBlock`'s per-block bound check (SPEC-STREAM-7). A block with an unset (`0`) bound
   field poisons its whole group's bound (`unknown=true`), mirroring `topKSkipBlock`'s
   contract that an unset bound is never skippable. `topKScanBlocks` precomputes a suffix
   aggregate (`suffixBound[i]` = the merged bound of every group at local index `>= i`) once,
   in O(groups) time, before dispatch begins. After each group finishes processing, if the heap
   already holds `Limit` entries and `topKSkipGroupBound(suffixBound[groupIdx+1], backward,
   heap.worstTS())` is true — i.e. no not-yet-fetched group can possibly improve the heap —
   `processGroup` returns `errLimitReached`, which `blockGroupPipeline` (SPEC-STREAM-11)
   translates into immediate dispatch cancellation (in-flight reads are drained, never
   processed, and excluded from `fetchedGroups`/`fetchedBlocks`/`bytesRead` per SPEC-STREAM-11's
   existing stats contract).

**Result:** for a `Backward`+`Limit` query, `topKScanBlocks` fetches only the newest-offset
prefix of coalesced groups needed to prove the heap can no longer improve — not the whole file.
`StepStats.IOOps`/`BytesRead` on the `"block-scan"` step reflect this bounded prefix.

**Qualification (2026-07-13, task #215): the bounded prefix has a scheduling-dependent floor
of `min(remaining groups, defaultPipelineWorkers)`, not "1 group."** This early stop shares
`blockGroupPipeline` (SPEC-STREAM-11) with every other block-scan path, and inherits the same
concurrent-prefetch-overshoot tradeoff SPEC-STREAM-13's R14-AMENDED note documents for
`RecentFirstBudget` (now retired, but the underlying `blockGroupPipeline` mechanics it described
are unchanged): the dispatcher's semaphore is pre-filled with `defaultPipelineWorkers` (8,
NOTE-058) tokens and provides no synchronous per-group check of `topKScanBlocks`' own
heap-saturation signal before dispatch — that check only runs, sequentially, inside
`processGroup` once a group's own I/O has already completed. So for a file with at most
`defaultPipelineWorkers` total coalesced groups, every group may already be dispatched before
the first group's early-stop signal can fire; for a larger file, up to `defaultPipelineWorkers`
groups are fetched "for free" before the semaphore's backpressure engages. This is an accepted
tradeoff, not a correctness gap (identical reasoning to R14-AMENDED: results are unaffected,
since `processGroup` still runs in ascending group order and a group whose I/O was wastefully
prefetched but never reached `processGroup` never contributes rows). Task #215 confirmed the
`min(remaining, defaultPipelineWorkers)` ceiling empirically (~80+ stress-repro attempts under
artificial CPU contention: observed `IOOps` ranged from 1 up to but never beyond 8) and updated
`stream_topk_intrinsic_unbounded_test.go`'s regression guard to assert against this ceiling
(`executor.DefaultPipelineWorkers`, a `predicates_export_test.go` export) with a fixture sized
well past it, rather than a strict `IOOps < totalGroups` bound that races on this ceiling for
small files.

**Correctness, not just performance.** The suffix-bound check is derived independently per
remaining group from real `BlockMeta` bounds, not from an assumption that offset order exactly
tracks timestamp order — a group is only ever proven "cannot improve" when its own aggregate
bound (or an unset/`unknown` bound, conservatively) says so. The reversed traversal order is
what makes this check useful in practice (SPEC-STREAM-8's existing "descending blockID order
for time-sorted files" assumption is what makes it fire quickly), but even if that assumption
were violated for some file, the fix would never incorrectly drop a result — it would simply
stop skipping less aggressively.

Back-ref: `internal/modules/executor/stream_topk.go:topKScanBlocks,topKGroupBound,
groupBoundFor,mergeGroupBound,topKSkipGroupBound`. Tests:
`stream_topk_intrinsic_unbounded_test.go:TestCollect_IntrinsicOnlyPredicate_MostRecentLimit_ScansEntireFile`
(issue #197 reproduction). Issue #197.

---

## SPEC-INTRINSIC-004: File-level bloom pre-check before intrinsic scan

*Added: 2026-04-14*

Before dispatching to the intrinsic fast path (`collectFromIntrinsicRefs`), the executor
runs a file-level bloom filter check via `fileLevelBloomReject`. If the bloom filter
indicates that no span in the file can satisfy the predicate nodes, the file is rejected
in O(1) without reading any blocks or intrinsic sections.

**Invariant:** When `fileLevelBloomReject` returns true, the result is an empty
`MatchedRow` slice with `ExecutionPath = ExecPathBloomRejected`. No block I/O occurs.

**Invariant:** The bloom pre-check is applied only when `program.Predicates != nil`.
A nil predicate set (match-all program) bypasses the bloom check and proceeds directly
to `collectFromIntrinsicRefs`.

**Invariant:** The bloom check is a conservative filter — a false result (not rejected)
does not guarantee any span matches; it only certifies the file *may* contain a match.
False positives are expected and handled by subsequent predicate evaluation.

Back-ref: `internal/modules/executor/stream.go:collectWithBloomCheck`

---

## 7. ExecuteTraceMetrics `[SUPERSEDED — ExecuteTraceMetrics deleted outright, issue #481 Task F-4, 2026-07-08]`

**This entire section documents `ExecuteTraceMetrics`, the full-block-scan metrics engine, which
was DELETED OUTRIGHT (not deprecated) as part of Phase F (issue #481 parts 3-4) — there is no
scan path left to answer to this contract.** Kept below verbatim for history, per `SPEC-ROOT-009`'s
"mark superseded, never delete" rule (each `SPEC-ETM-N` ID below remains valid and addressable,
none are reused). The CURRENT metrics-answering contract (the VI-only `ExecuteTraceMetricsFromVI`,
with an unconditional typed-error decline — no scan fallback of any kind) is `SPEC-VIS-2` (revised
2026-07-08). §10.1 (Signature) and §10.3 (Supported Functions) below describe a function and a
function-support table that no longer exist in any form — `ExecuteTraceMetricsFromVI` only ever
answers `count_over_time()`/`rate()` without group-by (the VI-answerable subset), never the full
SUM/AVG/MIN/MAX/HISTOGRAM/QUANTILE/STDDEV/group-by surface below. **§10.2 (Types) is the ONE
subsection that SURVIVES unchanged** — `TraceMetricLabel`/`TraceTimeSeries`/`TraceMetricsResult`
are still declared and used by `ExecuteTraceMetricsFromVI` and `api.go` today.

### 10.1 Signature `[SUPERSEDED — no live function has this signature]`

```go
func ExecuteTraceMetrics(
    r *modules_reader.Reader,
    program *vm.Program,
    querySpec *vm.QuerySpec,
) (*TraceMetricsResult, error)
```

### 10.2 Types (SURVIVES, field renamed — used by `ExecuteTraceMetricsFromVI`/`api.go`)

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
    Series         []TraceTimeSeries
    IndexBytesRead int64
    BlocksScanned  int
}
```

**`BytesRead` renamed to `IndexBytesRead` (issue #218, 2026-07-13):** the field was dead under
`ExecuteTraceMetrics` (deleted, see §7 above) and remained dead/always-zero under
`ExecuteTraceMetricsFromVI` until this change. `ExecuteTraceMetricsFromVI` now populates it from
the queried `*SliceValueIndexSource`'s `ValueIndexBuildStats.BytesRead` (`metrics_trace.go`,
back-ref: `RecordFileIO`/`Stats`) — the total byte size of value-index files downloaded
resolving the query. Zero for a non-production `ValueIndexSource` test fake that doesn't track
file I/O. No caller referenced the old `BytesRead` name (verified repo-wide); this is a rename,
not an additive field.
```

### 10.3 Supported Functions `[SUPERSEDED — see SPEC-VIS-2: only count_over_time()/rate() without group-by survive, via ExecuteTraceMetricsFromVI]`

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

### 10.4 Invariants `[SUPERSEDED — SPEC-ETM-1 through SPEC-ETM-14 (including the two standalone entries at the end of this file) all describe ExecuteTraceMetrics/metrics_trace_intrinsic.go behavior that no longer exists; kept below for history only, IDs never reused]`

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
- **SPEC-ETM-12:** Blocks are fetched concurrently via `blockGroupPipeline` (SPEC-STREAM-11):
  up to `defaultPipelineWorkers` (W=8) coalesced groups (~8 MB each) are dispatched
  concurrently (semaphore-gated). Peak memory is O(W × group_size): at most W groups
  in memory at once (at most W-1 in pending reorder map + 1 being processed) ≈ 64 MB
  for W=8, not one group at a time. Parse and accumulation remain sequential on the
  consumer goroutine (see NOTE-058).
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

1. **Collect** — Scan all blocks. For each span, evaluate all N filter programs and record
   a `nodeMatch uint8` bitmask (bit i set = span matches program[i]). Results are keyed by
   `[16]byte` trace ID. See NOTE-080 for the bitmask design (max 8 nodes).
2. **Resolve** — For each trace, build a `spanID→index` map and set each span's `parentIdx`
   field. `parentIdx = -1` means root (no known parent in this file).
3. **Evaluate** — Apply the structural operators to each trace's span list via
   `applyStructuralOps`: 2-node queries route through the per-operator functions
   (`evalOpDescendantStruct` etc.); N>2 chains route through `evalOpChain`, which uses
   an intermediate match-set approach (left-to-right pairwise evaluation). Emit matching
   terminal-node spans as `SpanMatch` entries.

Back-ref: `internal/modules/executor/stream_structural.go:ExecuteStructural`

### 11.3 Invariants

- **SPEC-STRUCT-1:** Nil reader returns `&StructuralResult{}` with no error.
- **SPEC-STRUCT-2:** Block selection uses `planBlocks(r, prog, tr, opts)` per structural
  node — the same bloom, range-index, intrinsic-TOC, and TS-index pruning as plain filter
  queries (NOTE-091). The selected block sets are unioned across all programs. Any block
  absent from every program's set has no spans matching any structural node and is skipped.
  Safety: `shouldRejectFileForProgram` gates per-program pruning — LHS of negation ops
  (`!>>`, `!>`, `!~`) use `planBlocks(nil, tr, opts)` (time-range only, no predicate pruning).
  Known limitation: intermediate ancestor spans in pruned blocks can cause false negatives
  for multi-block traces; acceptable for typical workloads where traces fit in one block.
  For `{} >> {}` (nil predicates), `planBlocks(nil, tr, opts)` is called per program,
  selecting all time-range-matching blocks — behavior identical to the pre-NOTE-091 path.
- **SPEC-STRUCT-3:** A nil filter in any chain slot of `StructuralQuery` (left or any right
  `Expr` node) compiles to a nil program, which matches all rows (wildcard `{}`).
- **SPEC-STRUCT-4:** `Options.Limit > 0` caps the number of entries in `StructuralResult.Matches`.
  Matches are deduplicated per trace (a span may only appear once per trace result).
- **SPEC-STRUCT-5:** `SpanMatch.SpanID` is an 8-byte raw slice. `SpanMatch.TraceID` is a
  `[16]byte` value. `BlockIdx` and `RowIdx` are populated with the originating block index
  and row index within that block for the matched terminal span. These values are used by
  the conversion layer (`api.go`) to construct a `SpanFieldsAdapter` backed by the reader's
  intrinsic section. `BlockIdx` and `RowIdx` are both zero only for spans that happened to
  be in block 0 at row 0; callers must not assume zero means "not set".

- **SPEC-STRUCT-6:** `OpNotDescendant (!>>)` — a rightMatch span R passes if NO span in
  R's ancestor chain is a leftMatch. Root spans (parentIdx == -1) have an empty ancestor
  chain and always pass. Contrast with `>>` (OpDescendant) which requires at least one
  ancestor to be leftMatch. Block selection uses planBlocks per program with the union of selected blocks (SPEC-STRUCT-2).
  Back-ref: `internal/modules/executor/stream_structural.go:evalOpNotDescendantStruct`

- **SPEC-STRUCT-7:** `OpNotChild (!>)` — a rightMatch span R passes if R has no parent
  (parentIdx == -1) or R's direct parent is not a leftMatch. Root spans trivially pass.
  Contrast with `>` (OpChild) which requires the direct parent to be leftMatch.
  Block selection uses planBlocks per program with the union of selected blocks (SPEC-STRUCT-2).
  Back-ref: `internal/modules/executor/stream_structural.go:evalOpNotChildStruct`

- **SPEC-STRUCT-8:** N-node chain support — `ExecuteStructural` supports chains of N nodes
  (e.g. `A >> B >> C`) via `FlattenChain` + `evalOpChain`. Two constraints are enforced at
  runtime in `ExecuteStructural`:
  1. **Max 8 nodes**: chains with more than 8 filter nodes return an error
     (`"structural chain too long: N nodes (max 8)"`). This is required by the `nodeMatch
     uint8` bitmask: `uint8(1) << i` overflows silently at i=8.
  2. **Negation ops disallowed in chains**: if any op in a multi-step chain is
     `OpNotSibling`, `OpNotDescendant`, or `OpNotChild`, `ExecuteStructural` returns an
     error (`"negation operator X is not supported in multi-node chains"`). Single-step
     queries (2-node, 1-op) continue to support negation operators unchanged.
  Back-ref: `internal/modules/executor/stream_structural.go:ExecuteStructural`,
            `internal/modules/executor/stream_structural.go:evalOpChain`

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

### 11.5 Index-Driven Structural Query Engine (issue #489, plan-d.md D2-D6)

A second dispatch strategy alongside the scan-based `ExecuteStructural` (§11.1-11.4): for 2-node
chains only, candidate discovery is driven by the value index and whole-trace resolution by
TraceGroup materialization, with no full-block scan. Chains that don't flatten to exactly 2 nodes
(`compileStructuralPair`'s decline, NOTE-VI-087) always fall back to `ExecuteStructural`, which
retains full N-ary chain support (SPEC-STRUCT-8) unchanged.

## SPEC-STRUCT-9: `ExecuteStructuralFromIndex` — 2-node positive-operator index-driven structural queries
*Added: 2026-07-08*

**Contract:**
```go
func ExecuteStructuralFromIndex(
    ctx context.Context,
    q *traceqlparser.StructuralQuery,
    leftSource, rightSource ValueIndexSource,
    isSelective StructuralSelectivityClassifier,
    traceGroupStore valueindex.LookupStore,
    tenant, indexPrefix string,
    readerFor StructuralReaderProvider,
    minTS, maxTS uint64,
    indexOnly bool,
    opts Options,
) (*StructuralResult, bool, error)
```

Answers a 2-node structural query using value-index-driven candidate discovery
(`FindTraceGroupInCandidates`, SPEC-VIS-5) plus TraceGroup materialization
(`MaterializeTraceGroupMultiFile`, SPEC-VIS-3), with no full-block scan. Positive operators only
(`>>`, `>`, `~`, `<<`, `<`) — negated operators decline (routed to SPEC-STRUCT-10 instead).

`ok=false, err=nil` (routine decline, caller falls back to `ExecuteStructural`):
- `q` does not flatten to exactly a 2-node chain (`compileStructuralPair`'s own decline, NOTE-VI-087).
- `op` is a negated operator — this function is never responsible for negated operators.
- `leftSource` has no VI coverage for `[minTS, maxTS)`, and `indexOnly` is false.
- the trace-by-id index has zero candidate files for the window, and `indexOnly` is false.

`ok=false, err!=nil`:
- index/data inconsistency (skew) surfaces exactly as `MaterializeTraceGroupMultiFile`/`GetTraceByID` already do (SPEC-VIS-3/4/5) — never silently masked.
- `ErrStructuralIndexCoverageGap` (SPEC-STRUCT-11) when `indexOnly` is true and either coverage gap above occurs, or when any candidate's assembled TraceGroup is `Partial`.

**Invariant: bit-role occupancy never swaps, regardless of which side seeds discovery.**
`nodeMatch` bit0 is ALWAYS the walk-anchor (L)'s real VI-match bit; bit1 is ALWAYS R's bit
(provisional-match-all pre-D3B-confirmation, then real post-confirmation). This role assignment
is fixed by `resolvedSpansToStructuralRecs` unconditionally, independent of `chooseCandidateTraceIDs`'
seed choice (NOTE-VI-092) — seeding discovery from R's exact matches (when R is Selective and L
is not) changes WHICH traces are even considered, never WHICH bit represents which side. This is
the property validator-d2's adversarial check confirmed holds for all three structural-operator
edge cases (including `~`'s self-exclusion case): the provisional-R-then-confirm walk is
algebraically identical regardless of seed choice specifically because bit-role occupancy is
invariant.

**Join-key correctness (task #12):** all internal joins between value-index matches and
`TraceGroup`/`ResolvedSpan` data key on `(SourceRef, BlockPage, RowIdx)`, never `SpanID` — see
NOTE-VI-092 for why `VILookupResult.SpanID` is not a valid join key for this purpose.

Design rationale (walk-anchor/discovery-seed split, selectivity injection, intersection prefilter): see NOTE-VI-092.

Back-ref: `internal/modules/executor/structural_index.go:ExecuteStructuralFromIndex`. Tests: `structural_index_test.go`, `structural_index_golden_test.go`, `structural_index_seed_test.go`, `structural_index_realvi_test.go`. Issue #489.

---

## SPEC-STRUCT-10: `ExecuteNegatedStructuralFromIndex` — 2-node negated-operator index-driven structural queries
*Added: 2026-07-08*

**Contract:**
```go
func ExecuteNegatedStructuralFromIndex(
    ctx context.Context,
    q *traceqlparser.StructuralQuery,
    rightSource ValueIndexSource,
    traceGroupStore valueindex.LookupStore,
    tenant, indexPrefix string,
    readerFor StructuralReaderProvider,
    minTS, maxTS uint64,
    indexOnly bool,
    opts Options,
) (*StructuralResult, bool, error)
```

Answers a 2-node NEGATED structural query (`!>>`, `!>`, `!~`) using the value index for RIGHT-side
candidate discovery only (no `leftSource` parameter exists — the negated side is never
VI-resolvable, see NOTE-VI-093) plus TraceGroup materialization. Mirrors SPEC-STRUCT-9's
decline/error contract with the operator polarity reversed:

`ok=false, err=nil`:
- `q` does not flatten to exactly a 2-node chain.
- `op` is a positive operator — routed to SPEC-STRUCT-9 instead.
- `rightSource` has no VI coverage for the window, and `indexOnly` is false.
- the trace-by-id index has zero candidate files for the window, and `indexOnly` is false.

`ok=false, err!=nil`: same shape as SPEC-STRUCT-9 — index/data skew, or `ErrStructuralIndexCoverageGap` (SPEC-STRUCT-11) under `indexOnly` or a `Partial` TraceGroup.

**Invariant: the negated side is structurally unresolvable via VI, enforced by the function's own
signature, not by a runtime convention a caller could violate.** `ExecuteNegatedStructuralFromIndex`
has NO `leftSource` parameter at all — contrast `ExecuteStructuralFromIndex` (SPEC-STRUCT-9), which
takes both `leftSource` and `rightSource`. There is no way for any caller, by mistake or otherwise,
to wire a left-side `ValueIndexSource` into this function; candidate-trace discovery is driven
EXCLUSIVELY by `rightSource`. This is a compile-time-enforced invariant, not a
documented-but-checkable-at-runtime one — the strongest form the codebase can express for "this
side is never VI-resolvable" (NOTE-VI-093).

**Cost model is NOT interchangeable with SPEC-STRUCT-9's** — see NOTE-VI-093: confirmation of the negated side is unconditional (whole tree, every candidate), not selectivity-gated, so cost is proportional to trace size rather than to VI selectivity. Callers must not assume a shared per-candidate cost model between this function and `ExecuteStructuralFromIndex`.

**Join-key correctness (task #12, D6 audit confirmed):** the right-side VI match join keys on
`(SourceRef, BlockPage, RowIdx)`, never `SpanID` — see NOTE-VI-093. The left side's D3B-confirmed
match set is safely keyed by SpanID (both operands are real per-row data, not raw VI results).

Back-ref: `internal/modules/executor/structural_index_negated.go:ExecuteNegatedStructuralFromIndex`. Tests: `structural_index_negated_test.go`, `structural_index_negated_realvi_test.go`. Issue #489.

---

## SPEC-STRUCT-11: `ErrStructuralIndexCoverageGap` — shared coverage-gap/Partial-tree contract for both index-driven structural engines
*Added: 2026-07-08*

**Contract:** `ErrStructuralIndexCoverageGap` (`errors.Is`-comparable sentinel, `internal/modules/executor/structural_errors.go`) is returned by both `ExecuteStructuralFromIndex` (SPEC-STRUCT-9) and `ExecuteNegatedStructuralFromIndex` (SPEC-STRUCT-10) in exactly two situations:
1. `indexOnly` is true and either the search VI (candidate discovery) or the trace-by-id VI/TraceGroup index has no coverage for the query window — mirrors #487's `IndexOnly`/`ErrSliceIndexCoverageGap` pattern (SPEC-VIS-2) and `ErrValueIndexNoCoverage`'s precedent: a time-sliced structural job has no safe scan fallback across slice boundaries, so it must fail loudly rather than silently narrow.
2. ANY candidate trace's assembled TraceGroup (`valueindex.AssembleTrace`) is `Partial` — regardless of `indexOnly`. Ruling (team-lead, 2026-07-07): this is treated as a coverage gap for STRUCTURAL evaluation specifically, unlike `GetTraceByID`'s plain, non-structural `Partial` handling (an orphan span, `SpanEntry.IsRoot`/`ParentSpanID` semantics, `valueindex/NOTES.md`), which is NOT itself an error condition there. A partial tree can produce false NEGATIVES for `ExecuteStructuralFromIndex`'s positive operators and false POSITIVES for `ExecuteNegatedStructuralFromIndex`'s negated operators (NOTE-VI-093) — both directions make labeling a narrower-than-true answer "authoritatively successful" unsafe, so both engines fail the whole candidate-trace evaluation rather than silently returning a partial match set.

Back-ref: `internal/modules/executor/structural_errors.go:ErrStructuralIndexCoverageGap`. Issue #489.

---

## SPEC-STRUCT-12: `StructuralSelectivityClassifier` — injected selectivity classifier (import-cycle workaround)
*Added: 2026-07-08*

**Contract:** `type StructuralSelectivityClassifier func(prog *vm.Program) bool` (`internal/modules/executor/structural_index.go`) reports whether `prog` classifies as `queryplan.Selective` over the caller's already-decoded VCNT section and query window. `ExecuteStructuralFromIndex` (SPEC-STRUCT-9) accepts this as an injected dependency rather than importing `internal/modules/queryplan` directly, because `queryplan` → `vibuilder` → `executor` is a real Go import cycle (verified by build attempt). Root's own caller wraps `queryplan.ClassifyProgramVCNT(prog, vcntData, vcntDir, minTS, maxTS) == queryplan.Selective` to construct the classifier it passes in. A `nil` classifier (or a `nil` `rightSource`) is treated as "not Selective" for both sides — `ExecuteStructuralFromIndex` always falls back to L as the discovery seed in that case (NOTE-VI-092).

Back-ref: `internal/modules/executor/structural_index.go:StructuralSelectivityClassifier`. Issue #489.

---

## SPEC-STRUCT-13: Bounded, Pointed Newest-First Execution for the Structural Scan Engine (`Options.Direction`/`RecentFirstBudget`)
*Added: 2026-07-08 (issue #481 parts 2-3, F-3)*

**Contract:** `Options.Direction`/`Options.RecentFirstBudget` (`options.go`) activate a bounded,
newest-first execution path for `ExecuteStructural`'s own 1e/1f decline categories — structural
chains that flatten to other than exactly 2 nodes, any polarity, which never reach the
index-driven engines (`SPEC-STRUCT-9`/`10`) and always ran the full scan engine before this phase.
`RecentFirstBudget != nil` implies the caller has also set `Direction == queryplanner.Backward` —
`ExecuteStructural` does not infer `Direction` from budget presence, mirroring
`executor.CollectOptions`' explicit-field convention (`SPEC-STREAM-13`). When
`RecentFirstBudget` is `nil`, both fields are ignored and `ExecuteStructural` behaves exactly as
before this phase: the full predicate-selected block set, no early stop, no incomplete-trace
exclusion.

**Mechanism:** `fetchStructuralBlocksBounded` fetches the (already Direction-ordered)
`selectedBlocks` ONE BLOCK AT A TIME — not the single coalesced batch call the unbounded path
uses — stopping at the FIRST `RecentFirstBudget` cap reached: `MaxBlocks`/`MaxBytes` are checked
BEFORE each fetch (zero-I/O, from `BlockMeta.Length`), `MaxDuration` is checked AFTER each fetch
(the only way to bound actual wall-clock I/O time spent). Trading per-block-call coalescing
efficiency for a genuine, checkable stop condition is the explicit, accepted tradeoff. Under
`RecentFirstBudget`, the unbounded path's `expandStructuralBlocksForTraces` step (the unconditional
full-file cross-block-trace-completion scan) is SKIPPED ENTIRELY — see `SPEC-STRUCT-14` for the
correctness consequence this has for parent resolution.

**Result reporting:** `StructuralResult.BudgetStopped`/`BlocksRead` (`structuralresult.go`) are
populated ONLY when `Options.RecentFirstBudget` was set; both are zero-value for the unbounded
path (unchanged behavior). `BudgetStopped` is `true` when a `MaxBlocks`/`MaxBytes`/`MaxDuration`
cap stopped block collection before the full predicate-selected block set was read.

**Relationship to `SPEC-STRUCT-8`:** this bounded path is orthogonal to `SPEC-STRUCT-8`'s own,
pre-existing, independent constraints (max-8-node chains; negation operators disallowed in any
multi-node chain, unconditionally, regardless of budget mode). A chain that would already hard-error
under `SPEC-STRUCT-8` continues to hard-error identically whether or not `RecentFirstBudget` is
set — the bounded path changes HOW blocks are read for chains `SPEC-STRUCT-8` already allows to
execute, never WHETHER a given chain is allowed to execute at all. See `NOTE-VI-098` for the
practical consequence this has for negated multi-node chains specifically (R15-AMENDED).

Back-ref: `internal/modules/executor/options.go:Options`,
`internal/modules/executor/structuralresult.go:StructuralResult`,
`internal/modules/executor/stream_structural.go:fetchStructuralBlocksBounded,ExecuteStructural`. Tests:
`stream_structural_test.go` (bounded-path suite, chronology/newest-first regression coverage).
Issue #481.

`BudgetStopped`/`BlocksRead` are now also attached as `blockpack.structural.budget_stopped`/
`blockpack.structural.blocks_read` attributes on the `blockpack.query` span (guarded by
`IsRecording()`) — visibility only, no semantic change to this rule's own contract above
(NOTE-480, issue #493).

**RETIRED (2026-07-12, plan-scan-fallback.md Phase 7, task #190).** `Options.Direction`/
`Options.RecentFirstBudget`, `fetchStructuralBlocksBounded`, and
`StructuralResult.BudgetStopped`/`BlocksRead` (the budget-populated fields — `BlocksRead` itself
survives as a field, see below) have been REMOVED. `StructuralResult` is now just `{Matches
[]SpanMatch, BlocksRead int}` — `BudgetStopped` is gone entirely; `BlocksRead` remains but is no
longer budget-gated (it is populated on every path now, not "ONLY when `Options.RecentFirstBudget`
was set" as this entry originally specified). `ExecuteStructural`'s 1e/1f decline-category chains
now always run the full, unbounded scan engine this entry's own Contract paragraph describes as
the pre-Phase-7-#481 default — there is no bounded alternative left to select. See `SPEC-ROOT-023`
for why this was removed rather than left as a permanent additive alternative, and `SPEC-STRUCT-14`
for the corresponding retirement of this entry's own downstream parent-resolution consequence.

Back-ref (current): `internal/modules/executor/structuralresult.go:StructuralResult` (now
`{Matches, BlocksRead}` only), `stream_structural.go:ExecuteStructural` (unconditional unbounded
path).

---

## SPEC-STRUCT-14: Incompleteness-Exclusion Contract for Bounded Structural Parent Resolution
*Added: 2026-07-08 (issue #481 part 2, F-3, team-lead ruling R15)*

**Contract:** `resolveStructuralParentIndices`'s `budgetMode` parameter (true whenever
`Options.RecentFirstBudget != nil`) excludes a trace WHOLESALE — never partially evaluated — when
any of its spans carries a non-empty parent reference (`ParentSpanId` was non-empty at write time)
that fails to resolve within that trace's assembled span set. `StructuralResult.IncompleteTraceCount`
is this rule's own observable output: the count of traces excluded this way, zero-value for the
unbounded path.

**Why wholesale exclusion, not partial evaluation or the unbounded path's orphan treatment.**
Under the unbounded path (`budgetMode == false`), an unresolved-but-present parent reference is a
genuine orphan: the caller already fetched every block in the file
(`expandStructuralBlocksForTraces`), so "not found" means "does not exist," and the existing
`parentIdx = -1` root-like treatment is correct and UNCHANGED by this rule. Under
`RecentFirstBudget`, that expansion is SKIPPED (`SPEC-STRUCT-13`), so "not found" is genuinely
AMBIGUOUS — the ancestor may exist in a block the bounded read never reached. R15 resolves that
ambiguity conservatively: treat it as "may exist, unread," not "confirmed absent," because a
wrongly-confirmed absence would flip a negated operator (`!>>`, `!>`, `!~`) into a FALSE POSITIVE —
a materially different, and worse, failure mode than the honest partial answer (a false negative)
that a positive operator's incompleteness already produces under a tight budget. Excluding the
whole trace, uniformly for BOTH polarities (no operator-specific special-casing), is R15's explicit
requirement: a positive-operator's honest degradation from omission is the same shape of tradeoff
either way, so one uniform rule safely covers both directions.

**R15-AMENDED — interaction with `SPEC-STRUCT-8`'s pre-existing negation/chain-length
constraints.** Because `SPEC-STRUCT-8` already hard-errors any multi-node (>2) chain containing a
negation operator — unconditionally, independent of this phase or of budget mode — a "3+-node
negated chain" can never actually reach `resolveStructuralParentIndices` (or any structural
execution path) at all. This narrows the practical scope of this entry's false-positive concern to
2-node negated chains only (the only shape in which a negation operator and a multi-block,
budget-truncated trace can co-occur). No spec-text change to `SPEC-STRUCT-8` itself was made or is
needed — it already, correctly, forecloses the 3+-node-negated case; this entry cross-references it
rather than duplicating or amending it. See `NOTE-VI-098` for the fuller design-decision history,
including why the ruling table that originally scoped this phase's work briefly (and incorrectly)
treated "3+-node negated" as a distinct bucket needing its own execution path.

Back-ref: `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices,ExecuteStructural`,
`internal/modules/executor/structuralresult.go:StructuralResult.IncompleteTraceCount`. Tests:
`stream_structural_test.go` (incompleteness-exclusion suite, negated-operator false-positive
regression coverage). Issue #481.

`IncompleteTraceCount` is now also attached as `blockpack.structural.incomplete_trace_count`
on the `blockpack.query` span (guarded by `IsRecording()`) — visibility only, no semantic
change to this rule's own contract above (NOTE-480, issue #493).

**RETIRED (2026-07-12, plan-scan-fallback.md Phase 7, task #190).** `resolveStructuralParentIndices`'s
`budgetMode` parameter described above has been REMOVED from the function's signature entirely
(confirmed by direct read: current signature is `resolveStructuralParentIndices(traceSpans
[][]structuralSpanRec, ops []traceqlparser.StructuralOp) [][]structuralSpanRec` — two parameters,
no boolean). `StructuralResult.IncompleteTraceCount` (and `BudgetStopped`, `SPEC-STRUCT-13`) are
likewise removed from `StructuralResult`, which is now just `{Matches []SpanMatch, BlocksRead
int}`. The function now ALWAYS takes the unbounded path's own treatment this entry described as
already-correct-and-unchanged: an unresolved-but-present parent reference is always treated as a
genuine orphan (`parentIdx = -1`, root-like), because `SPEC-STRUCT-13`'s bounded path (the only
caller that ever needed the wholesale-exclusion behavior this entry specifies) no longer exists —
see that entry's own retirement note for why. This entry's reasoning (why wholesale exclusion,
not partial evaluation, was the correct choice UNDER a bounded budget) is retained verbatim for
history; it no longer describes a reachable code path.

Back-ref (current): `internal/modules/executor/stream_structural.go:resolveStructuralParentIndices`
(now unconditional), `structuralresult.go:StructuralResult` (now `{Matches, BlocksRead}` only).

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

---

## SPEC-ETM-13: Dict-ID Group Map Invariants `[SUPERSEDED — metrics_trace_intrinsic.go deleted outright, issue #481 Task F-4, 2026-07-08]`

*Added: 2026-04-17*

**Kept below verbatim for history; every back-ref function in this entry (`buildGroupIDMap`,
`accumulateIntrinsicBuckets`, `intrinsicInt64ColToString`, `scanIntrinsicColDictIDs`,
`scanIntrinsicColVals`) was deleted along with `metrics_trace_intrinsic.go` and no longer exists
anywhere in the codebase (confirmed by repo-wide grep at tombstone time). No successor entry
exists — the VI-only `ExecuteTraceMetricsFromVI` (`SPEC-VIS-2`) has no group-by dict-ID fast path
of its own; it only ever answers `count_over_time()`/`rate()` without group-by.**

- **SPEC-ETM-13.1:** When `len(agg.GroupBy) <= maxGroupByDimsFastPath (8)`, the intrinsic
  fast path uses `buildGroupIDMap` to construct a `map[uint32]groupIDKey` instead of
  `buildGroupKeyMap`. The dict-ID path is transparent to callers: `buckets` keys and series
  labels are byte-identical to the string-keyed path.
- **SPEC-ETM-13.2:** When `len(agg.GroupBy) > maxGroupByDimsFastPath`, the intrinsic fast
  path falls back to `buildGroupKeyMap` and the string-keyed accumulation functions.
- **SPEC-ETM-13.3:** Dict index 0 is always the empty-string sentinel in every dimension's
  dict slice. Pks absent from a group-by column are assigned index 0, producing an empty
  string label for that dimension — identical to the string-keyed path behavior.
- **SPEC-ETM-13.4:** Histogram boundary values stored in `histGroupIDKey.boundary` and
  `histSingleGroupIDKey.boundary` are always powers-of-2 or 0 (computed via
  `intrinsicHistogramBoundary`). Float64 map key comparison is therefore safe — NaN cannot appear.
- **SPEC-ETM-13.5:** For metrics queries with group-by on intrinsic enum columns (span:kind,
  span:status), the series label values emitted must be OTel string names ("server", "client",
  "ok", "error", etc.), never integer enum values (1, 2, 3, etc.). This applies to all code
  paths: dict-ID fast path (N≤8), string-keyed fallback (N>8), and histogram accumulation.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:buildGroupIDMap`,
          `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBuckets`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:intrinsicInt64ColToString`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanIntrinsicColDictIDs`
Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:scanIntrinsicColVals`

## SPEC-ETM-14: N=0/N=1 No-Predicate Direct Accumulation `[SUPERSEDED — metrics_trace_intrinsic.go deleted outright, issue #481 Task F-4, 2026-07-08]`

*Added: 2026-04-22*

**Kept below verbatim for history; every back-ref function in this entry
(`accumulateIntrinsicBucketsDirect`, `accumulateHistogramDirect`, `accumulateAggDirect`,
`accumulateCountRateDirect`, `accumulateHistogramDirectN0`) was deleted along with
`metrics_trace_intrinsic.go` and no longer exists anywhere in the codebase. No successor entry
exists — see SPEC-ETM-13's tombstone note for why.**

**N=1 no-predicate direct accumulation** — when a metrics query has exactly one group-by key,
no span-level predicate filter, and the block is a dict-encoded intrinsic block, the executor
dispatches through `accumulateIntrinsicBucketsDirect` which eliminates `inRangeRefs` and hash
maps entirely.

Dispatch conditions (all must hold):

- `len(agg.GroupBy) == 1`
- `len(predicates) == 0` (no TraceQL filter)
- block is intrinsic-format (dict-encoded)

Behavioral invariants:

- **Byte-equivalence guarantee:** output is byte-identical to the hash-map-based path for all
  supported function types (count, rate, sum, min, max, histogram).
- **`inRangeCount == 0` early exit:** returns `(true, nil)` before any dict or agg column work
  when no spans fall within the time range; the returned empty `buckets` map is correct.
- **`numSteps <= 0` guard:** returns `(true, nil)` for degenerate time specs.
- **Flat-format group-by fallback:** when the group-by column is flat-format (not dict-encoded),
  returns `(false, nil)` to fall through to the general `executeTraceMetricsIntrinsicN` path.
- **N>1 fallback:** queries with more than one group-by key reach the `default:` branch and
  fall through to the full hash map accumulation path.
- **N=0 histogram:** when `filteredRefs == nil`, `len(agg.GroupBy) == 0`, and `agg.Function == HISTOGRAM`,
  the switch dispatches to `accumulateHistogramDirectN0`, which scans the histogram column without
  materializing `inRangeRefs` or any group-key map — output is byte-identical to the reference no-group-by path.

Fallback: any condition not met falls through to the general `executeTraceMetricsIntrinsicN`
path with full hash map accumulation.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic.go:accumulateIntrinsicBucketsDirect`,
          `internal/modules/executor/metrics_trace_intrinsic.go:accumulateHistogramDirect`,
          `internal/modules/executor/metrics_trace_intrinsic.go:accumulateAggDirect`,
          `internal/modules/executor/metrics_trace_intrinsic.go:accumulateCountRateDirect`,
          `internal/modules/executor/metrics_trace_intrinsic.go:accumulateHistogramDirectN0`

## SPEC-SCAN-1: StreamScanEqualAny Dict-Mask Fast Path Invariants

*Added: 2026-04-17*

- **SPEC-SCAN-1.1:** `StreamScanEqualAny` fires the dict-mask fast path when all values in
  the query have the same kind (all string, all int64, or all float64) AND the column type
  is String, Int64, Uint64, or Float64. Bool columns always use the generic rowEqual path
  (NOTE-077: D=2 max dict, overhead exceeds savings). Mixed-kind value sets also fall through.
- **SPEC-SCAN-1.2:** Results from the fast path are byte-identical to the generic
  `scanWith`+`rowEqual` fallback for every (col-type, value-kind) combo. The fast path is
  a pure performance optimization — it never changes which rows match.
- **SPEC-SCAN-1.3:** String+float64 queries against string columns return 0 matches (fast path
  returns immediately without scanning). This matches rowEqual's type-strict veto at line 84-89.
- **SPEC-SCAN-1.4:** String+int64 queries use `strconv.ParseInt(s, 10, 64)` per dict entry,
  identical to rowCompareString. Dict entries that fail ParseInt produce false (no match).
- **SPEC-SCAN-1.5:** Uint64+int64 queries cast the query int64 to uint64 (`uint64(t)`)
  before comparing. Negative int64 inputs produce large uint64 values — this matches
  rowCompare line 117 exactly.
- **SPEC-SCAN-1.6:** `col.EnsureDecoded()` must be called before accessing any XxxDict/XxxIdx
  slice directly. Skipping this returns empty slices for lazily-registered columns (NOTE-077).
- **SPEC-SCAN-1.7:** nilIntrinsicScan (SPEC-STREAM-10.1) runs before any fast-path dispatch.
  The col==nil guard is unchanged; fast-path dispatch only runs after col != nil is confirmed.

Back-ref: `internal/modules/executor/column_provider.go:StreamScanEqualAny`,
          `internal/modules/executor/column_provider.go:dispatchDictFastPath`

## SPEC-SCAN-2: StreamScanNotEqual — Absent Column Semantics

*Added: 2026-04-17*

- **SPEC-SCAN-2.1:** When `StreamScanNotEqual` is called for a column that is absent from
  the block (`lookupColumn` returns nil) AND the column is not an intrinsic column, the
  function returns `(0, nil)` immediately. An absent user-attribute column matches no rows
  for any `!=` predicate.
- **SPEC-SCAN-2.2:** When the absent column is an intrinsic column, `nilIntrinsicScan`
  runs first (SPEC-STREAM-10.1). The result depends on whether the intrinsic is recognized:
  a recognized absent intrinsic returns FullScan; an unrecognized name falls through to
  `return 0, nil`.
- **SPEC-SCAN-2.3:** When the column IS present (`col != nil`) but a specific row has no
  value (`!col.IsPresent(i)`), that row IS included in the `!=` result (per-row null is
  treated as "not equal to anything"). This is the existing per-row behavior; SPEC-SCAN-2
  governs only the whole-column-absent case.

Back-ref: `internal/modules/executor/column_provider.go:StreamScanNotEqual`

---

## SPEC-CSP-01: ComputeSecondPassCols — Column Set for Result Materialization

*Added: 2026-04-30*

```go
func ComputeSecondPassCols(program *vm.Program, selectColumns []string) map[string]struct{}
```

`ComputeSecondPassCols` returns the set of column names needed for result materialization
at `NewSpanFieldsAdapterWithReader` call sites. It is the exported complement of the
internal `computeColumnFilters` second-pass output.

### Inputs

- `program *vm.Program` — compiled TraceQL filter program. May be nil (structural path,
  log path). When non-nil and has predicates, predicate columns are included in the result.
- `selectColumns []string` — caller-requested output columns (`opts.SelectColumns`). May be
  nil or empty. When non-empty, these columns are included in the result alongside
  traceIntrinsicColumns.

### Output

- Returns `map[string]struct{}` (`secondPassCols`) or nil.
- Non-nil when either `program` has predicates or `selectColumns` is non-empty.
- Nil when both `program` is nil (or has no predicates) and `selectColumns` is empty;
  nil is valid and means "load all intrinsic columns" at the call site.

### Invariants

- **traceIntrinsicColumns always present in non-nil result:** when the return value is
  non-nil, it always includes all entries from `traceIntrinsicColumns` (which contains
  `span:start`, `span:duration`, `trace:id`, `span:id`, and others). This satisfies
  SPEC-ROOT-017's span:end synthesis requirement — both `span:start` and `span:duration`
  are always available for synthesis when `wantCols` is non-nil.
- **searchMetaCols always present in non-nil result:** `searchMetaCols` entries are
  included in every non-nil result alongside predicate and select columns.
- **Pure function:** no I/O, no side effects. Safe to call before a row loop and reuse
  the result for every row in the loop.
- **Correct wantCols for stream paths:** the return value is the correct `wantCols`
  argument for `NewSpanFieldsAdapterWithReader` at `streamFilterProgram`,
  and the `QueryTraceQL` structural result path (SPEC-ROOT-017).

### Call-site pattern

```go
// SPEC-ROOT-017: pass secondPassCols as wantCols to restrict intrinsic decoding
wantCols := executor.ComputeSecondPassCols(program, opts.SelectColumns)
for _, row := range rows {
    adapter := blockio.NewSpanFieldsAdapterWithReader(..., wantCols)
    ...
}
```

Back-ref: `internal/modules/executor/predicates.go:ComputeSecondPassCols`

## SPEC-IS-01: IntrinsicScanner — Mandatory Sequential-Scan Access (NOTE-443, issue #362)

`reader.IntrinsicScanner` is the canonical way to read an intrinsic column with a
sequential forward scan. It centralizes the streaming-vs-eager decision that callers
previously hand-rolled (try `Reader.ScanIntrinsicColumn`, fall back to
`Reader.GetIntrinsicColumn` on `streamed=false`).

### Policy

- **Sequential-scan access to intrinsic columns MUST use `IntrinsicScanner`.** A caller
  that walks every row of a value-decoupled column once (boundary pre-count, bucket
  accumulation, rank-scatter) writes its logic as a single `Scan(visit)` callback over
  `*shared.DecodedPage` and lets the scanner pick streaming (paged Flat/XOR/Delta, O(one
  page)) or the eager single-synthetic-page fallback (legacy v1 / non-paged) transparently.
- **Direct `GetIntrinsicColumn` calls in the executor are reserved for random-access
  patterns** — predicate evaluation, reverse `refIndex` lookups by rank, multi-pass
  random algorithms — where a materialized full column is genuinely required.

### Scope and limits

- `IntrinsicScanner` serves **value-decoupled columns only** (uint64 Flat/XOR/Delta and
  per-row bytes), whose values live in `Uint64Values`/`BytesValues` parallel to
  `BlockRefs`. `DecodedPage` is a flat value-per-row model with no `DictEntries` field.
- **Dict columns are out of scope.** Their values live in a cross-row `DictEntries` arena
  with no flat `DecodedPage` representation. `Scan` returns an error (rather than silently
  dropping data) if the eager-fallback column is Dict-format. Dict sequential scans use
  `Reader.ScanDictGroupByColumn` (NOTE-407) instead.
- **Absent columns** (no intrinsic section, or name not present) cause zero `visit` calls
  and a `nil` return — identical to streaming a column with zero pages.

### Lifetime contract

The `*shared.DecodedPage` and every slice it references are valid ONLY for the duration of
a single `visit` call. The streaming path reuses page buffers across pages; a visitor that
retains any value or ref beyond its call MUST copy it. The eager-fallback path happens to
alias the cached column's immutable slices, but callers MUST NOT rely on that — which path
runs is intentionally opaque.

Back-ref: `internal/modules/blockio/reader/intrinsic_scanner.go`,
`internal/modules/blockio/reader/intrinsic_reader.go:ScanIntrinsicColumn,GetIntrinsicColumn`,
NOTE-406/407/410/442 (the streaming-discipline predecessors).

---

## SPEC-OBS-001: Context Propagation (see root SPEC.md)

All executor entry points (`Collect`) MUST accept `ctx context.Context`
as first parameter. Nil is normalized to `context.Background()` at the boundary. See root
SPEC-OBS-001 for the full invariant. NOTE-449 resolved NOTE-058.

Back-ref: `internal/modules/executor/stream.go:Collect`.

---

## SPEC-OBS-002: Mandatory Spans Per Query Type (see root SPEC.md)

`Collect` on the `planBlocks → scanBlocks` path MUST produce:
`blockpack.query` → `blockpack.planner` (child) → `blockpack.block` (children, one per block).

Every query path MUST emit exactly one `blockpack.planner` span — including the intrinsic
pre-filter fast paths (search and metrics) that resolve the query before reaching the
`planBlocks → scanBlocks` site. A successfully two-phase-pruned query (NOTE-464, issue #383)
MUST NOT be invisible in a distributed trace.

`queryplanner.PlanOptions.EnableExplain` MUST be gated on `querySpan.IsRecording()` — checked
once per `Collect`/`ExecuteStructural` call and threaded down explicitly (never re-derived from
context, never polled per block/structural node) — so `plan.Explain` string-building only
happens when the span will actually record it (NOTE-478, issue #493).

Back-ref: `internal/modules/executor/stream.go:Collect,scanBlocks,emitFastPathPlannerSpan`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`,
`internal/modules/executor/otel_spans.go:emitPlannerSpan,startBlockSpan`,
`internal/modules/executor/stream_structural.go:collectAllStructuralSpans,ExecuteStructural`.

---

## SPEC-OBS-005: Two-Phase Execution Observables (NOTE-464, issue #383)

The `blockpack.planner` span MUST carry a `full_fetch_skipped` attribute describing whether the
query avoided fetching full block payloads:

- `blockpack.planner.full_fetch_skipped` (bool, ALWAYS present): true when the query was
  answered with zero full block payload fetches — the strongest form of the issue #383 win.
  True on: the metrics-intrinsic decline path when all blocks were block-pruned. False on the
  full block-scan path and structural nodes.

NOTE-440: `blockpack.planner.bitmap_selectivity` was removed. It was computed from the
candidate-row bitmap produced by the intrinsic-TOC pre-filter, which the executor rewrite (#440)
deleted along with the IntrinsicTOC (#433/#436). No code computed candidate_rows / total_spans
after that removal, so the attribute was permanently omitted; the dead `PlannerSpanStats` fields
and `bitmapSelectivity()` helper were deleted.

Back-ref: `internal/modules/executor/otel_spans.go:PlannerSpanStats,emitPlannerSpan`.

---

## SPEC-OBS-003: IsRecording() Guard (see root SPEC.md)

Every `span.SetAttributes(...)` on the hot path MUST be wrapped in `if span.IsRecording()`.

Back-ref: `internal/modules/executor/otel_spans.go`.

---

## SPEC-OBS-004: Block Span Cache Attributes (see root SPEC.md)

Cache hit/miss counts on `blockpack.block` spans are aggregated via `CacheStats`, never
via per-fetch child spans.

Back-ref: `internal/modules/executor/otel_spans.go:attachCacheStats`,
`internal/modules/blockio/reader/cache_stats.go`.

---

## SPEC-OBS-006: StructuralFunnelStats Contract — Structural Index-Driven Funnel

*Added: 2026-07-09 (issue #493, Task 6)*

`StructuralFunnelStats` (`structural_funnel_stats.go`) carries per-stage candidate counts for
the index-driven structural funnel (`ExecuteStructuralFromIndex`/
`ExecuteNegatedStructuralFromIndex`, `structural_index.go`/`structural_index_negated.go`).
Threaded ALONGSIDE `*StructuralResult`, never added to it — `StructuralResult` stays scoped to
`SPEC-STRUCT-13`/`14`'s orthogonal `RecentFirstBudget` concern.

**Fields:** `LeadMatches`, `LeadTraces`, `CandidateTraces`, `TraceGroupHits`, `TraceGroupMisses`,
`TraceGroupPartial` (bool), `TreeWalkSurvivors`, `VerifiedSurvivors`.

**Contract:** `nil` means "span not recording, skip all counting work" — mirrors
`PlannerSpanStats`' nil-means-skip convention (`otel_spans.go`). Every increment site in both
engines is guarded by `stats != nil`. Emitted as span attributes on the existing
`blockpack.query` span (`attachStructuralFunnelStats`), alongside `blockpack.query.engine`
(values: `"scan"`, `"structural_scan"`, `"structural_index"` — set unconditionally at
span-start on all three query engines, including on error-returning decline paths that never
reach a stats-attach call).

Back-ref: `internal/modules/executor/structural_funnel_stats.go:StructuralFunnelStats,
attachStructuralFunnelStats`; call sites in
`internal/modules/executor/structural_index.go:ExecuteStructuralFromIndex,
evalOneStructuralCandidateTrace`,
`internal/modules/executor/structural_index_negated.go:ExecuteNegatedStructuralFromIndex,
evalOneNegatedStructuralCandidateTrace`. Also retrofits `blockpack.query.engine` onto
`stream.go:Collect` ("scan") and `stream_structural.go:ExecuteStructural` ("structural_scan").
See NOTE-479 for design rationale. Issue #493.

---

## SPEC-VIS-1: SliceValueIndexSource concurrency safety

*Added: 2026-07-02*

**Invariant:** `SliceValueIndexSource`'s `Add`, `RecordFileIO`, `Stats`, `LookupResults`, and
`AllResults` are safe for concurrent callers. All five methods acquire an internal `sync.Mutex`
for their full body (map/field read or write), so concurrent `Add`/`RecordFileIO` calls from
multiple goroutines cannot race on the underlying `data` map or `stats` counters, and a
concurrent `Stats`/`LookupResults`/`AllResults` reader always observes a consistent snapshot
(never a partially-written map bucket).

This is required by `vibuilder.BuildSource`'s leaf-loop parallelization (tempo issue #465,
timeout incident): multiple leaf-predicate goroutines call `Add`/`RecordFileIO` on the same
`SliceValueIndexSource` concurrently once the leaf loop is bounded-concurrency rather than
serial. See NOTE-VI-048 for the rationale and `vibuilder/NOTES.md`'s corresponding entry for
the call-site change that makes this concurrency real.

The mutex adds no behavioral change to any method's return semantics — `Stats()`'s
Hits-derived-at-read-time design (NOTE-VI-039) was already written to tolerate any `Add`
ordering; the mutex only makes that tolerance safe under the Go memory model when the calls are
truly concurrent, not just reordered on a single goroutine.

Back-ref: `internal/modules/executor/metrics_trace.go:SliceValueIndexSource`.
Tests: `internal/modules/executor/metrics_trace_vi_test.go:TestSliceValueIndexSource_ConcurrentAddAndRecordFileIO_NoRace` (EX-VIS-01).

---

## SPEC-VIS-2: `TraceMetricOptions.IndexOnly` — forbidding `ExecuteMetricsTraceQL`'s internal scan fallback on ANY VI non-answer
*Added: 2026-07-07 (issue #487, holistic-review Issue 1/fix A)*

**Contract:** `TraceMetricOptions.IndexOnly bool` (root `tracemetricoptions.go`), when `true`,
forbids `ExecuteMetricsTraceQL`'s (root `api.go`) full-block-scan fallback: on ANY value-index
non-answer — `opts.ValueIndex == nil` (no index source at all, the OUTER decline) OR
`ExecuteTraceMetricsFromVI` returning `ok=false` for any of its own decline reasons (unsupported
shape, no coverage for a leaf, legacy `TimeSec == 0` block, canceled context — the INNER
decline) — `ExecuteMetricsTraceQL` returns `blockpack.ErrValueIndexNoCoverage`
(`errors.Is`-comparable) instead of falling through to `executor.ExecuteTraceMetrics` (the full
scan).

**Why blockpack owns this, unlike the search path.** ~~The search path's analogous decline
(`executor.QueryTraceQLFromIndex`, `NOTE-VI-035`/`096`/`078`) leaves the decline-to-scan
decision to its CALLER~~ **(superseded below — see Revised 2026-07-08)** ~~(tempo's `tryIndexFetch`, which converts a routine decline into its own
`ErrSliceIndexCoverageGap`) — blockpack's search-path function just returns `(nil, false, nil)`
and lets the caller decide.~~ The metrics path is architecturally different:
`ExecuteMetricsTraceQL` owns its VI-decline-to-scan fallback INTERNALLY ~~(it calls
`ExecuteTraceMetricsFromVI` itself, then falls through to `ExecuteTraceMetrics` itself)~~ — there
is no external caller decision point to hook into. ~~`IndexOnly` exists to give an external caller
(tempo's #487 time-slice job dispatch) a way to forbid that internal fallback from outside,
mirroring the search path's OUTCOME (typed decline signal, no unsafe scan) via a different
MECHANISM (a boolean flag) required by the different ownership shape.~~

**Why a narrowed-window job cannot safely fall back to a full scan (historical rationale, still
correct as a description of WHY a scan was unsafe — the scan itself no longer exists, see
below).** A `#487` per-slice job runs the same query over `[sliceStart, sliceEnd)` only.
`ExecuteTraceMetrics`'s full block scan was un-windowed at the per-span level relative to the
slice boundary — it would have ignored `[sliceStart, sliceEnd)` and could have double-counted or
over-fetched spans across multiple overlapping slice jobs dispatched for the same block. This is
exactly why removing the scan outright (below), rather than merely gating it behind `IndexOnly`,
is strictly safer, not a regression.

~~**`ErrValueIndexNoCoverage`** (root `tracemetricoptions.go`) is the sentinel, `errors.Is`-
comparable, distinct per its own doc comment from the search path's tempo-owned
`ErrSliceIndexCoverageGap` (different repos, different ownership, same underlying design
intent).~~ **(superseded below.)**

**Revised 2026-07-08 (issue #481 parts 3-4, Phase F) — the fallback this entry originally
describes is GONE, not merely forbidden by a flag.** `ExecuteTraceMetrics` (the full-block-scan
engine referenced throughout the original text above) was **deleted outright**
(`internal/modules/executor/metrics_trace.go`) — there is no scan left for `IndexOnly` to forbid,
so `IndexOnly` no longer changes `ExecuteMetricsTraceQL`'s decline behavior at all (it is retained
on `TraceMetricOptions` only so tempo's callers can keep describing their own job's dispatch shape,
per its own updated field doc comment). Every non-answer — the OUTER decline
(`opts.ValueIndex == nil`) and every INNER decline `ExecuteTraceMetricsFromVI` can produce — is now
the SAME unconditional, production-default typed-error contract, for every caller, always.

**`ErrValueIndexNoCoverage` is replaced outright (no back-compat alias) by FOUR
`errors.Is`-comparable sentinels** (`internal/modules/executor/decline_errors.go`), mirroring
`ErrStructuralIndexCoverageGap`'s plain-sentinel style rather than an enum+struct: `ErrMetricsShapeNotAnswerable`
(an aggregate/group-by shape the VI path cannot answer — SUM/AVG/MIN/MAX/HISTOGRAM/QUANTILE/STDDEV
and any group-by, deterministic per query text), `ErrMetricsNoCoverage` (a specific leaf/column in
the predicate has no VI coverage — a per-query-SHAPE limitation), `ErrMetricsLegacyTimeSecZero` (a
matched span's block predates per-span timestamps — a per-block-DATA limitation, heterogeneous
across blocks), and `ErrMetricsValueIndexDisabled` (team-lead ruling R8: no `ValueIndexSource` was
supplied for this call at all — a zeroth, operator-CONFIGURATION-level condition, distinct from
`ErrMetricsNoCoverage`'s narrower "this one column isn't covered" scope; an earlier version of this
fix conflated the two, losing R8's distinct category, fixed before landing). Because the fallback
scan this entry originally protected against is now categorically gone, "why blockpack owns this
decision internally" is moot — there is no fallback left to own or forbid, only a typed error to
return.

Back-ref: `api.go:ExecuteMetricsTraceQL`, `tracemetricoptions.go:TraceMetricOptions.IndexOnly`,
`internal/modules/executor/decline_errors.go` (all four sentinels),
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetricsFromVI`. See `NOTES.md`
NOTE-VI-086, NOTE-VI-096 (the search-path equivalent this mirrors in outcome, not mechanism — ID
corrected 2026-07-08, see NOTE-VI-096 for why the citation changed from NOTE-VI-047). Tests:
`tracemetricoptions_test.go` (original `IndexOnly` suite, now exercising the unconditional
contract), plus the F-4 real-write-path suite for the four sentinels. Issues #487, #481.

---

## SPEC-VIS-3: `MaterializeTraceGroupMultiFile` / `StructuralReaderProvider` / `ResolvedSpan` / `ErrStructuralMultiFileCoverageGap` — multi-file trace materialization for structural queries
*Added: 2026-07-08 (issue #489, plan-d.md §D3, relocated root→executor per D3B checkpoint ruling)*

**Contract:** `MaterializeTraceGroupMultiFile(ctx, readerFor StructuralReaderProvider, group valueindex.TraceGroup, traceID [16]byte, maxConcurrentReaderOpens int) ([]ResolvedSpan, error)` (`internal/modules/executor/structural_multifile.go`) resolves EVERY distinct `SourceRef` present in a `TraceGroup` against its own reader, with bounded concurrency (`errgroup.SetLimit`, default 8 when `maxConcurrentReaderOpens <= 0`). Returns `[]ResolvedSpan{Reader, SourceRef, Span, BlockIdx, RowIdx}` in first-seen `SourceRef` order (deterministic regardless of goroutine completion order).

Either of two failure classes fails the WHOLE call via `errgroup.Wait()` (never a partial per-file result):
- `ErrStructuralMultiFileCoverageGap` (`errors.Is`-comparable): `readerFor` could not open some `SourceRef` at all.
- `ResolveTraceGroupSourceRef`'s (SPEC-VIS-4) own index/data skew error, wrapped with sourceRef context.

Additive only — never changes `GetTraceByID`'s single-file `materializeTraceGroup` behavior (NOTE-VI-076, unchanged).

Back-ref: `internal/modules/executor/structural_multifile.go:MaterializeTraceGroupMultiFile, StructuralReaderProvider, ResolvedSpan, ErrStructuralMultiFileCoverageGap`. Tests: `structural_multifile_test.go`. See NOTE-VI-088 for full design rationale. Issue #489.

---

## SPEC-VIS-4: `ResolveTraceGroupSourceRef` — single shared resolve/skew-detection primitive for single-file and multi-file trace-by-id resolution
*Added: 2026-07-08 (issue #489, plan-d.md D3B checkpoint ruling)*

**Contract:** `ResolveTraceGroupSourceRef(reader *modules_reader.Reader, group valueindex.TraceGroup, traceID [16]byte, sourceRef string) ([]ResolvedTraceRow, error)` (`internal/modules/executor/structural_traceresolve.go`) filters `group.Spans` to `sourceRef` (empty disables the filter, NOTE-VI-076 v1 back-compat), resolves each surviving span's `BlockRef`, reads/parses the needed blocks, and defensively re-verifies each resolved row's own `trace:id` column against `traceID`. Every failure mode (an index-named page that doesn't resolve, a read/parse failure, or a `trace:id` mismatch) is index/data skew and returns an error — never a silent drop. Zero surviving spans after a non-empty `sourceRef` filter is an authoritative "not found in THIS file" (`(nil, nil)`), not skew.

This is the literal single shared implementation for both `GetTraceByID`'s single-file path (root `materializeTraceGroup` is now a thin delegate over it) and `MaterializeTraceGroupMultiFile`'s (SPEC-VIS-3) multi-file path — extracted verbatim from the original inline algorithm in root's `materializeTraceGroup` so the two callers can never disagree on what counts as "resolved" vs. "skew." All 19 pre-existing `GetTraceByID` tests pass unchanged, confirming behavioral parity with the pre-extraction implementation.

Back-ref: `internal/modules/executor/structural_traceresolve.go:ResolveTraceGroupSourceRef, ResolvedTraceRow`, root `reader.go:materializeTraceGroup` (thin delegate). Tests: `gettracebyid_index_test.go` (19, unchanged), `structural_multifile_test.go` (6). See NOTE-VI-089 for full design rationale. Issue #489.

---

## SPEC-VIS-5: `FindTraceGroupInCandidates` — shared candidate-TraceGroup merge/lookup primitive for GetTraceByID and index-driven structural discovery
*Added: 2026-07-08 (issue #489, plan-d.md §D4, team-lead ruling 2026-07-07)*

**Contract:** `FindTraceGroupInCandidates(ctx, lister valueindex.LookupStore, keys []string, traceID [16]byte, queryMinSec, queryMaxSec uint64) (valueindex.TraceGroup, bool, error)` (`internal/modules/executor/structural_tracegroup.go`) fetches and decodes every candidate trace-by-id index file in `keys`, merging every matching group found for `traceID` across all of them (SpanID-deduplicated, first occurrence wins; `TimeSec` is the minimum across matches). Candidates are never short-circuited on first match — disjoint-span L0 files for the same TraceID are a legitimate pre-compaction state. A fetch/decode failure on any candidate is index/data inconsistency and returns an ERROR (NOTE-VI-071's authoritative-index contract), never silently skipped.

**Concurrency (task #199, NOTE-VI-106, 2026-07-12):** every candidate's `valueindex.LookupTraceGroupPartial` call is fanned out CONCURRENTLY via `errgroup`, bounded by `candidateFetchConcurrency` (4, mirroring `vibuilder.downloadConcurrency`'s convention and value) — wall-clock latency is bounded by the slowest single candidate instead of scaling linearly with the candidate count. The merge itself remains deterministic: results are collected into a slice indexed by each candidate's original position in `keys` and folded back in that same order once every candidate has resolved, so the SpanID-dedup/minimum-`TimeSec` merge semantics are byte-identical to the prior strictly-sequential loop regardless of goroutine completion order. A real (non-404-class) error on any candidate still fails the whole call, mirroring `vibuilder.queryKeysRanged`'s sibling-abort guard.

Shared, canonical implementation for both `GetTraceByID`'s single-trace lookup (root `findTraceGroupInCandidates` is a thin delegate) and `ExecuteStructuralFromIndex`'s (SPEC-STRUCT-9) many-candidate-trace discovery loop.

Back-ref: `internal/modules/executor/structural_tracegroup.go:FindTraceGroupInCandidates`, root `reader.go:findTraceGroupInCandidates` (thin delegate). See NOTE-VI-091 for original design rationale and NOTE-VI-106 for the concurrency fix. Issue #489, task #199.

---

## SPEC-VIS-6: `SliceValueIndexSource.AddLeaf`/`LookupLeaf` — leaf-aware same-column disambiguation and decline propagation
*Added: 2026-07-12 (issues #206, #207, task #207); corrected 2026-07-12 (task #213, CRITICAL regression fix)*

**Contract:** `AddLeaf(leafIdx int, colName string, colType modules_shared.ColumnType, results []VILookupResult)` records `results` into both the existing column-level aggregate bucket (`Add`'s behavior, unchanged) and, when `leafIdx >= 0`, a per-leaf bucket keyed on `leafIdx` — the leaf's own structural DFS leaf-slot position (assigned identically at build time by `vibuilder.collectLeaves` and at eval time by `viEvalNode`'s running counter; see `AddLeaf`'s own doc comment for why a structural position, not a `*vm.RangeNode` pointer, is the only cross-compile-safe identity). `LookupLeaf(leafIdx int, colName string, colType modules_shared.ColumnType) ([]VILookupResult, bool)` resolves a specific leaf:

1. `leafIdx >= 0` and a per-leaf entry exists for it → that leaf's own results, `true`.
2. `leafIdx >= 0`, no per-leaf entry, and this source has **never** had `AddLeaf` called with a valid (`>= 0`) index for ANY leaf (`leafAware == false`) → falls back to `LookupResults(colName, colType)` (the column-level aggregate) — the correct, original behavior for a genuinely legacy caller that never used leaf-aware keying at all.
3. `leafIdx >= 0`, no per-leaf entry, `leafAware == true`, and `leafIdx` was marked via `MarkRequirePresentLeaf` (task #212, corrected task #213) with a non-empty set of paired sibling leaf indices, AND every one of those paired sibling indices has its own per-leaf entry in `leafData` → falls back to the UNION of those paired siblings' own per-leaf results (never the bare column-level aggregate) — safe specifically because those exact siblings are the SAME rewrite call's own value-bearing range-OR, whose union already implies "column X is present"; see the task #213 correction paragraph below for why this must be keyed by leaf INDEX, not by column name.
4. `leafIdx >= 0`, no per-leaf entry, and this source **has** had `AddLeaf` called with a valid index for at least one OTHER leaf (`leafAware == true`), and case 3's carve-out does not apply (not marked at all, or marked but at least one paired sibling lacks its own per-leaf entry) → `(nil, false)`, unconditionally. Never falls back to the column aggregate.

**Why case 4 must not fall back (issue #207):** once any leaf of a query has been keyed positionally, a same-column sibling leaf's own results already live in the shared column-aggregate bucket (every `AddLeaf` call writes there too, for `AllResults`/`Stats`). A missing per-leaf entry in a leaf-aware source means THIS leaf was declined at build time (e.g. task #204's millisecond-decidability gate on `span:duration`, or a same-column multi-value-equality decline) — never "this source predates leaf-aware keying," because it demonstrably does not (some other leaf just used it). Falling back in this case would silently substitute an unrelated sibling leaf's own match set for the declined leaf, reintroducing issue #206's union-instead-of-intersection bug through a different path (confirmed by `value_index_oracle_comparison_test.go`'s Shape 9a/9b, which failed with exactly this wrong-answer signature before the `leafAware` gate existed). `viEvalNode`/`viEvalAND`/`viEvalOR`/`viEvalNodes` (same file) already treat any leaf's `(nil, false)` as "decline the whole enclosing AND/OR," so case 4 correctly forces `QueryTraceQLFromIndex`/`ExecuteTraceMetricsFromVI` to report no-coverage for the whole query rather than silently answering a different (over- or under-inclusive) one.

**Case 3 (task #212, CORRECTED task #213): why a `RequirePresent` leaf from the SCOPED `!=` rewrite is the one exception that MAY fall back — and why the UNSCOPED rewrite must NEVER take this branch.** `attr != V` (`extractNeqNode`/`extractNeqNumericNode`) has two distinct rewrite shapes. The SCOPED form (`span.attr != V` / `resource.attr != V`) compiles to `[RequirePresent(attr), OR{attr>V, attr<V}]` — `vibuilder.buildPredicate` NEVER builds a value predicate for the `RequirePresent` leaf (existence-only, no `Values`/`Min`/`Max`/`Pattern`), so it is deliberately never `AddLeaf`'d, yet its column always has a REAL sibling — the adjacent range-OR, from the SAME rewrite call — that IS `AddLeaf`'d whenever the query has any coverage at all. The UNSCOPED form (bare `.attr != V`) compiles to `OR{RequirePresent(resource.attr), RequirePresent(span.attr)}` — presence-only, with NO value-bearing sibling at all for either scope.

Task #212's original fix keyed the carve-out by bare COLUMN NAME (`s.data[colName]` existence), which is safe for the scoped shape (nothing else on that column exists except the rewrite's own pairing) but UNSAFE for the unscoped shape: `unscopedCols` produces the identical expanded column names (`resource.<name>`, `span.<name>`) an entirely unrelated, independently-authored leaf elsewhere in the SAME query might reference (e.g. `{ .score != 100 && resource.score > 5 }`). Case 3 as originally specified would then substitute that UNRELATED leaf's own match set as "proof of presence" — neither a superset nor an exact match of true presence, producing wrong answers (false positives via AND, false negatives via OR) that nothing else in the tree could correct, since the unscoped shape's `OR{RequirePresent, RequirePresent}` IS the complete representation for that condition. Task #213 corrects this: the carve-out is now keyed by the SPECIFIC paired sibling leaf-slot INDEX (`vm.RangeNode.NeqPairedRange`, set ONLY on the scoped rewrite's `RequirePresent` leaf, never the unscoped one), never a bare column name. `vibuilder.BuildSource`/`BuildSourceBounded` call `MarkRequirePresentLeaf(leafIdx, pairedLeafIdxs []int)` — SIGNATURE CHANGED — only when a declined leaf's `RangeNode.RequirePresent && RangeNode.NeqPairedRange` is true and `collectLeaves` successfully validated the adjacent sibling's shape; the unscoped rewrite's `RequirePresent` leaves never satisfy this and are never marked, so they always fall through to case 4's decline — exactly the pre-#212 (#207-only) safe behavior.

**Blast radius confined to same-column sibling shapes:** a declined leaf whose column has no OTHER leaf anywhere in the query (case where `LookupResults(colName)` would legitimately return `(nil, false)` on its own) behaves identically regardless of `leafAware`/marking — see Shape 9c. `leafAware` (and, since task #212/#213, the paired-sibling mark) only changes behavior for the specific case case 2 vs. case 3/4 disambiguates: a declined leaf whose SAME rewrite call has a covered sibling leaf in the same leaf-aware source.

**New/changed public API surface:** `MarkRequirePresentLeaf` is a new exported method on `SliceValueIndexSource` (task #212), alongside the pre-existing `AddLeaf`/`LookupLeaf`/`MarkNewestFirst` leaf-identity-bookkeeping family. Its signature CHANGED (task #213) from `MarkRequirePresentLeaf(leafIdx int, colName string)` to `MarkRequirePresentLeaf(leafIdx int, pairedLeafIdxs []int)`.

Back-ref: `internal/modules/executor/metrics_trace.go:SliceValueIndexSource.AddLeaf, SliceValueIndexSource.LookupLeaf, SliceValueIndexSource.MarkRequirePresentLeaf, SliceValueIndexSource.leafAware, SliceValueIndexSource.requirePresentLeaves`, `internal/modules/vibuilder/builder.go:collectLeaves, neqRangeSiblingLeaves`, `internal/vm/rangenode.go:RangeNode.NeqPairedRange`. Tests: `value_index_oracle_comparison_test.go` (`TestOracle_Shape09a_SameColumnAND_MixedCoverageDecline`, `TestOracle_Shape09b_SameColumnOR_MixedCoverageDecline`, `TestOracle_Shape09c_DifferentColumnAND_MixedCoverageDecline`, `TestOracle_Shape11a_NotEquals_AND_ResolvesCorrectly`, `TestOracle_Shape11c_NotEquals_OR_ResolvesCorrectly`), `neq_metrics_realvi_test.go`, `neq_unscoped_collision_realvi_test.go`, `structural_oracle_comparison_test.go` Shape7. Issues #206, #207, #212, #213.

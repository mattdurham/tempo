# executor — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/executor` package.

---

## 1. Responsibility Boundary
*Added: 2026-02-10*

The executor owns span-level evaluation. Block selection (which blocks to read) is
delegated to `queryplanner`; raw I/O (coalescing, wire parsing) is delegated to
`blockio/reader`. The executor only decides which spans within a selected block match the
query.

---

## 2. Collect Takes *reader.Reader, Not BlockIndexer
*Added: 2026-02-10 (updated 2026-03-11: renamed Execute → Collect per NOTE-035)*

**Decision:** `Collect` takes `*reader.Reader` directly rather than `queryplanner.BlockIndexer`.

**Rationale:** After block selection, the executor needs `ParseBlockFromBytes` and
`BlockMeta` — methods that are on `*reader.Reader` but not part of `BlockIndexer`.
Extending `BlockIndexer` with parsing methods would conflate two distinct concerns:
index lookup (what the planner needs) and block decoding (what the executor needs).
Using `*reader.Reader` directly is explicit and avoids unnecessary interface widening.

`*reader.Reader` still satisfies `BlockIndexer` via Go structural typing, so the planner
works without a type assertion.

---

## 3. ColumnDataProvider for Modules Block
*Added: 2026-02-10*

**Decision:** The executor implements `vm.ColumnDataProvider` directly against
`*reader.Block` (modules format) in `column_provider.go`, rather than converting to the
old `blockpackio.Block` type via the adapter in `internal/modules/blockio/adapter.go`.

**Rationale:** The adapter conversion copies every column dictionary, which allocates
significantly more memory than necessary. The modules `Block` type exposes its columns
directly via `GetColumn`, making a direct implementation straightforward and allocation-free.

---

## 4. RowSet Is Sorted by Construction
*Added: 2026-02-10*

**Decision:** `rowSet` maintains rows in ascending order. Stream scans iterate rows
0..n-1 sequentially, so `Add` is called in ascending order. `ToSlice` returns the
backing slice directly without re-sorting.

**Rationale:** Sorted row indices are required for:
- `Contains` (binary search, O(log n))
- `Union/Intersect/Complement` (merge-style algorithms, O(n+m))
- Deterministic output ordering

---

## 5. SpanMatch Contains Block and Row Indices
*Added: 2026-02-10*

**Decision:** `SpanMatch` includes `BlockIdx` and `RowIdx` in addition to `TraceID` and
`SpanID`.

**Rationale:** The caller (e.g. a Tempo query handler) may want to fetch additional column
values for matched spans without re-scanning the block. Having `BlockIdx` and `RowIdx`
allows targeted column access via `ParseBlockFromBytes` + `GetColumn(name).StringValue(rowIdx)`.

---

## 6. Predicate Extraction and Dedicated Index Pruning
*Added: 2026-02-25*

**Decision:** The executor calls `buildPredicates(r, program)` (in `predicates.go`),
which extracts column names and encoded values from `program.Predicates` and builds
`[]queryplanner.Predicate` for bloom-filter and range-index block pruning.

**Design (superseded 2026-03-06 — see NOTE-030):**
The original design used `HasOROperations` flag and flat maps (`DedicatedColumns`,
`DedicatedRanges`, `DedicatedColumnsRegex`, `UnscopedColumnNames`) to determine
per-column predicates. The current design uses a `RangeNode` tree that is recursively
translated to `queryplanner.Predicate` composites, enabling per-child OR pruning
rather than per-query. See NOTE-030 for the complete redesign rationale.

This gives two-stage block-level pruning. See queryplanner/NOTES.md §8 for
range-pruning design.

**History:** An earlier version of this note documented a period when `predicates.go` was
removed and the executor passed `nil` predicates to `planner.Plan`. That design was
superseded and predicate extraction was reinstated with the full two-stage pruning
described above.

---

## 7. Integration Coverage via blockio/executor_test.go
*Added: 2026-02-25*

**As of the modules executor migration:** the primary integration tests for this
executor live in `internal/modules/blockio/executor_test.go` (EX-01 through EX-07),
not only in `internal/modules/executor/executor_test.go`.

`internal/modules/blockio/executor_test.go` exercises the full round-trip:
writer (`modules_blockio.NewWriterWithConfig`) → reader
(`modules_reader.NewReaderFromProvider`) → this executor (`New().Execute`).

**Coverage provided by the blockio integration tests:**
- `bloomPredicates` with real data (AND and OR query paths, EX-01, EX-03, EX-04, EX-06)
- `Execute` empty-file short-circuit (EX-05)
- Multi-block scanning with `BlocksScanned >= 2` assertion (EX-04)
- Zero-match result (EX-02)

`internal/modules/executor/executor_test.go` additionally covers:
- `Options.Limit` early-exit (EX-08)
- `SpanMatch.TraceID` / `SpanMatch.SpanID` field population (EX-09)
- `result.Plan.TotalBlocks` populated (EX-10)

---

## 8. Signal-Aware Column Selection in spanMatchFromBlock
*Added: 2026-03-02*

**Decision:** `spanMatchFromBlock` receives the file's `SignalType` and uses different
column names depending on whether the file holds trace or log data.

**Trace files (`SignalTypeTrace`):** `trace:id` and `span:id`.
**Log files (`SignalTypeLog`):** `log:trace_id` and `log:span_id`.

**Rationale:** Log files store trace and span correlation IDs under `log:trace_id` and
`log:span_id` (the OpenTelemetry log record fields), not under the `trace:id` / `span:id`
columns used by trace files. Without signal-aware lookup, `TraceID` and `SpanID` would
always be zero/nil for log queries. The signal type is read once via `r.SignalType()` and
passed to `spanMatchFromBlock` to avoid repeated calls.

---

## 9. log. Column Scope in Unscoped Attribute Expansion
*Added: 2026-03-02*

**Decision:** Unscoped attribute expansion (e.g. `.level`) now expands to three column
names: `resource.{attr}`, `span.{attr}`, and `log.{attr}`.

**Rationale:** Log files store user attributes under the `log.{key}` prefix. Without the
`log.` expansion, unscoped queries like `{ .level = "error" }` would never match log records
even when the attribute is present. The bloom-OR predicate across all three column names is
safe: a block is only pruned when none of the three columns is present.

---

## NOTE-010: buildPredicates Consumes DedicatedRanges; encodeValue Handles Plain Column Types
*Added: 2026-03-02 — superseded by NOTE-030 (2026-03-06)*

**Superseded:** The `DedicatedRanges` map no longer exists. Range predicates are now
represented as `RangeNode{Min, Max}` in the `Nodes` tree and translated by `translateNode`.
The `encodeValue` function and its handling of plain vs `Range*` column types is unchanged.

**Original decision (historical):** `buildPredicates` included a loop over
`program.Predicates.DedicatedRanges` that encoded range bounds as point queries for
range-index pruning. `encodeValue` accepted both the `Range*` column type variants and
their non-range counterparts (`ColumnTypeUint64`, `ColumnTypeInt64`, `ColumnTypeFloat64`).

**Rationale:** Log intrinsics like `log:timestamp` are stored with `ColumnTypeUint64` in the
range index (not `ColumnTypeRangeUint64`). The writer's `updateLogMinMax` uses
`ColumnTypeUint64` because the raw column type is non-range. Without handling plain column
types, `encodeValue` would return `("", false)` for `log:timestamp` range predicates,
leaving the `Values` slice empty and silently skipping range-index pruning.

**Encoding rules (unchanged):** 8-byte little-endian for numeric types; raw string for
string types. The `Range*` and plain variants produce identical wire encoding.

---

## NOTE-011: Regex Prefix Optimization for Range-Index Pruning
*Added: 2026-03-03 — updated 2026-03-06 (NOTE-030)*

**Decision:** `translateRegexNode` (formerly a loop over `DedicatedColumnsRegex` in
`buildPredicates`) analyzes regex patterns using `vm.AnalyzeRegex` and produces
range-index predicates when the pattern has an extractable literal prefix.

**Design:**
1. `vm.AnalyzeRegex(pattern)` parses the regex syntax tree (via `regexp/syntax`) and
   extracts literal prefixes from optimizable patterns: `foo.*`, `^error`, `error|warn`.
2. For optimizable patterns, `buildPredicates` encodes each prefix as a `RangeString`
   value in `Predicate.Values`. The planner's `pruneByIndex` uses `BlocksForRange` to
   eliminate blocks whose string range buckets don't overlap the prefix.
3. The column type defaults to `ColumnTypeRangeString` when no range index exists (the
   planner's `pruneByIndex` gracefully skips when `RangeColumnType` returns false).
4. The full regex is still evaluated per-span by the VM — range-index pruning only
   eliminates blocks, never individual spans.

**Case-sensitive single-prefix patterns use interval matching for range-index pruning.**
Go's regex parser factors out common prefixes from alternations at parse time (before
`Simplify()`). For example, `cluster-0|cluster-1` becomes
`Concat(Literal("cluster-"), CharClass([01]))` with the common prefix `"cluster-"`.
A point lookup via `BlocksForRange("col", "cluster-")` returns nil because `'-'` (0x2D)
is less than `'0'` (0x30) — `"cluster-"` falls below all actual bucket lower boundaries
(which start at values like `"cluster-0"`).

Fix: for any case-sensitive regex with a **single extracted prefix**, `buildPredicates`
now uses interval matching `[prefix, prefix+"\xff"]` instead of a point lookup.
`BlocksForRangeInterval("col", "cluster-", "cluster-\xff")` correctly finds all buckets
with lower boundaries in that range (e.g., `"cluster-0"` through `"cluster-4"`).

The `\xff` suffix also ensures blocks in buckets where the lower boundary extends
beyond the bare prefix (e.g., `"foo-service"` for prefix `"foo"`) are included.

**Multi-prefix patterns** (e.g., `error|warn|info`, which parses as
`Alternate([OpLiteral("error"), OpLiteral("warn"), OpLiteral("info")])` without
common-prefix factoring) continue to use union point lookups per prefix, which
correctly handle full-string values.

**Case-insensitive patterns (`(?i)...`) use interval matching for range-index pruning.**
The range index stores original-case values and uses lexicographic comparison.
`"DEBUG"` and `"debug"` occupy different bucket positions, so a single point lookup
would miss blocks containing other case variants (false negatives).

Instead, `buildCaseInsensitiveRegexPredicate` generates an interval query:
- Min key: `strings.ToUpper(prefix)` (e.g., `"DEBUG"`)
- Max key: `strings.ToLower(prefix) + "\xff"` (e.g., `"debug\xff"`)

The `\xff` suffix ensures the interval captures buckets whose lower boundary extends
beyond the exact prefix (e.g., a bucket with lower `"debug-service"` is > `"debug"` but
< `"debug\xff"`). Without the suffix, such buckets would be pruned even though the regex
`(?i)debug.*` matches values in those buckets.

The predicate has `IntervalMatch: true`. The planner calls `BlocksForRangeInterval`
which finds all buckets whose lower boundary falls within [min, max]. This works
because uppercase ASCII/UTF-8 bytes sort before lowercase — the interval covers
every case variant (e.g., `"Debug"`, `"dEBUG"`, `"DeBuG"` all fall within
`["DEBUG", "debug\xff"]`).

**Case-insensitive alternations** (`(?i)(error|warn)`) fall back to bloom-only predicates.
Each prefix needs a separate interval, and different prefixes may span non-overlapping
lexicographic ranges — a single interval cannot safely cover all branches.

**Rationale:** Log workloads heavily use regex patterns like `(?i)debug`, `error.*`,
`^GET /api`. Block pruning is the primary performance lever in this system (I/O ops
reduction). Converting prefix patterns to range-index lookups enables block pruning
without any writer-side changes.

Back-ref: `internal/modules/executor/predicates.go:buildPredicates`,
`internal/vm/regex_optimize.go:AnalyzeRegex`

---

## NOTE-012: Stream vs Execute — Lazy Callback vs Eager Batch
*Added: 2026-03-03*

**Decision:** `Stream` is a separate method from `Execute` rather than a flag on `Options`.

**Rationale:**
- `Execute` returns `*Result` (batch `[]SpanMatch`). Callers like `tempoapi` expect a
  fully-materialized slice — adding callback plumbing to `Execute` would complicate its
  contract without benefit for those callers.
- `Stream` uses lazy `CoalescedGroups`/`ReadGroup` I/O instead of eager `FetchBlocks`.
  This is the correct strategy for streaming with a Limit: I/O is proportional to results
  returned, not to total selected blocks.
- Separating the two methods keeps each one simple and makes their different I/O strategies
  explicit at the call site.

**Signal unification:** `Stream` accepts `StreamOptions.TimestampColumn` to distinguish
trace mode (no per-row filter) from log mode (`"log:timestamp"` filter). The executor no
longer needs two divergent code paths — the difference is parameterized rather than
duplicated.

**Callers after this task:** `api.go:streamFilterProgram` and `api.go:streamLogProgram`
are both thin wrappers over `executor.Stream`. No inline block-scan loops remain in api.go.

**Addendum (2026-03-03):** The "out of scope" logql/engine.go:StreamLogs work was completed
as part of the logql→executor unification. StreamLogs and ExecuteLogMetrics now live in
executor/stream_log.go and executor/metrics_log.go, both using the same lazy coalesced-group
I/O pattern. The internal/modules/logql package has been deleted. See NOTE-013 for the
new executor→logqlparser import edge.

---

## Migration: Content migrated from internal/modules/logql/NOTES.md
*Migrated: 2026-03-03 — package internal/modules/logql deleted; execution logic moved to executor*

## NOTE-001: Why a New Module Instead of Extending logqlparser
*Added: 2026-03-03*

**Decision:** The LogQL execution engine lived in `internal/modules/logql/`, not in
`internal/logqlparser/`. It has now been merged into executor.

**Rationale:** `internal/logqlparser/` is a parser — it converts LogQL strings into AST
nodes and compiles them to `vm.Program` values. Adding execution logic (pipeline stage
evaluation, metric aggregation, block scanning) would conflate two distinct concerns and
break the separation that exists between `internal/traceqlparser/` (parse only) and
`internal/modules/executor/` (execute only).

The architecture now mirrors the established TraceQL architecture:
- TraceQL: `traceqlparser` (parse + compile) + `executor` (execute)
- LogQL: `logqlparser` (parse + compile + Pipeline types) + `executor` (execute)

---

## NOTE-002: Per-Row Pipeline Execution, Not Column-Level
*Added: 2026-03-03*

**Decision:** Pipeline stages operate per matched row with signature
`func(ts, line, labels) (line, labels, bool)`, not over entire columns.

**Rationale:** LogQL pipeline stages are inherently row-oriented: `| json` parses each
log body independently; `| label_filter` tests per-row label values; `| line_format`
templates use per-row label state. There is no vectorized equivalent that would safely
apply to blockpack's columnar layout.

Column-level operations (e.g., selecting which column to read for `log:body`) remain in
the block scan path. The per-row pipeline runs only after the block scan returns matching
row indices — keeping the hot path in the columnar scan and the post-filter in the pipeline.

---

## NOTE-003: Reuse vm.QuerySpec for Metrics, Not a New Bucketing System
*Added: 2026-03-03*

**Decision:** Metric aggregation in `metrics_log.go` reuses `vm.QuerySpec` for time
bucketing. It does NOT call `executor.ExecuteMetrics`.

**Rationale:** `vm.QuerySpec` (time bucketing: step, start, end) is generic — it was
designed for TraceQL metrics but is not TraceQL-specific. Reusing it avoids creating a
parallel bucketing system.

The engine does NOT delegate to `ExecuteMetrics` because that function is tightly coupled
to TraceQL span match fields and does not provide a hook for per-row pipeline execution.
LogQL metrics needs to interpose the pipeline between block scan and bucket accumulation.

---

## NOTE-004: Label Map Mutability and Caller Responsibility
*Added: 2026-03-03*

**Decision:** `Pipeline.Process` may mutate the `labels` map in place. The caller is
responsible for copying the map if the original state is needed after the call.

**Rationale:** Allocating a new map per row for every pipeline call would generate
significant GC pressure at high log volumes. Mutation in place is safe because:
1. Pipeline stages chain sequentially (no concurrent access to the same map).
2. The engine constructs a fresh labels map per row from block columns.
3. Callers that need the pre-pipeline state (unusual) can copy before calling Process.

---

## NOTE-005: Template Pre-Compilation for line_format
*Added: 2026-03-03*

**Decision:** `text/template` parsing for `| line_format "tmpl"` happens once at query
compile time (`logqlparser/compile.go`), not per-row.

**Rationale:** `text/template.Parse` is expensive (it involves regex compilation and
AST building). Parsing the same template string for every log row is unnecessary and
would dominate pipeline overhead. The compiled `*template.Template` is embedded in the
`PipelineStageFunc` closure and reused for every row.

---

## NOTE-006: UnwrapValueKey as Special Label
*Added: 2026-03-03*

**Decision:** `UnwrapStage` stores the extracted numeric value in the labels map under
the key `"__unwrap_value__"` (constant `logqlparser.UnwrapValueKey`).

**Rationale:** Passing a separate numeric value alongside the labels map would require
changing the `PipelineStageFunc` signature, breaking all existing stage implementations.
Using a reserved label key is a pragmatic convention that preserves the uniform stage
interface.

---

## NOTE-007: Silent Failure Policy for Parse Errors in Stages
*Added: 2026-03-03*

**Decision:** `JSONStage` and `LogfmtStage` do NOT drop rows when the log body is not
valid JSON/logfmt. They keep the row with unmodified labels.

**Rationale:** This matches Loki's behavior. In a mixed-format log stream, some records
may not conform to the expected format. Silently keeping them allows downstream label
filters to handle the case explicitly.

---

## NOTE-008: Engine Does Not Modify the Reader
*Added: 2026-03-03*

**Decision:** `StreamLogs` and `ExecuteLogMetrics` take `*reader.Reader` as a parameter
but never call writer-side methods. They are pure read paths.

---

## NOTE-009: logqlparser Produces Everything Needed to Execute
*Added: 2026-03-03*

**Decision:** `logqlparser.CompilePipeline` produces `*logqlparser.Pipeline` alongside
`*vm.Program`. The executor receives both and does not re-parse the query.

---

## NOTE-010: Free Functions Instead of Engine Struct
*Added: 2026-03-03*

**Decision:** `StreamLogs` and `ExecuteLogMetrics` are package-level free functions, not
methods on an `Engine` struct.

**Rationale:** The execution logic carries no state. Free functions are simpler to call,
test, and reason about.

---

## NOTE-011: Label Column Scoping — resource.* and log.* Prefixes Stripped
*Added: 2026-03-03*

**Decision:** `logReadLabels` strips the `resource.` and `log.` column name prefixes when
building the per-row labels map. The label `resource.service.name` becomes `service.name`.

**Rationale:** In LogQL, stream labels do NOT carry the `resource.` prefix. This matches
Loki semantics where resource-level attributes are exposed as first-class labels.

---

## NOTE-014: Reuse NewColumnProvider, Not a Reimplemented Interface
*Added: 2026-03-03 (migrated from internal/modules/logql/NOTES.md NOTE-012)*

**Decision:** `ExecuteLogMetrics` and `StreamLogs` call `NewColumnProvider(block)` (now
in the same package) to satisfy `vm.ColumnDataProvider`, rather than implementing the
interface again.

---

## NOTE-013: executor now imports logqlparser — new dependency edge
*Added: 2026-03-03*

**Decision:** `stream_log.go` and `metrics_log.go` import `logqlparser` for the
`*logqlparser.Pipeline` type.

**Rationale:** Pipeline types were moved from `internal/modules/logql` into
`internal/logqlparser` as part of unifying the logql package (all execution moves to
executor, all parsing/compilation stays in logqlparser). executor → logqlparser introduces
no import cycle: logqlparser imports only vm, standard library packages, and
github.com/go-logfmt/logfmt — none of which import executor.

This mirrors the established TraceQL architecture: traceqlparser (parse + compile) +
executor (execute). For LogQL: logqlparser (parse + compile + Pipeline types) + executor
(execute).

---

## NOTE-015: blockHasBodyParsed — parser-skip optimization for log queries
*Added: 2026-03-04*

**Decision:** `blockHasBodyParsed(block)` scans `block.Columns()` looking for any
`log.*` key with `Type == ColumnTypeRangeString`. When the block has body-auto-parsed
columns, `StreamLogs` / `StreamLogsTopK` acquire a `blockLabelSet` for the block and, in
the per-row scan loop, invoke `pipeline.ProcessSkipParsers` instead of `pipeline.Process`,
skipping the logfmt/JSON parse stage.

**Rationale:** The blockpack writer stores body-auto-parsed fields as `log.{key}`
`ColumnTypeRangeString` sparse columns (see logqlparser NOTE-9). `blockLabelSet` exposes
these fields lazily via column reads before the pipeline runs. Running `| logfmt` or
`| json` after the label set already contains those fields is redundant — these stages
call `logfmt.NewDecoder(bytes.NewBufferString(line))` per row, allocating ~15 objects
per row. On T7-T9 queries (env=prod + component filter), this produced ~18M allocs
for a 100 MB dataset.

**Column type discriminator:** Body-auto-parsed columns use `ColumnTypeRangeString`;
explicit OTLP LogRecord attributes (e.g. `log.level`, `log.detected_level`) use
`ColumnTypeString`. The discriminator is the column type byte stored in the block
metadata. `blockHasBodyParsed` uses this to determine which code path to take.

**Bug fixed:** `stringColumnBuilder.colType()` previously hardcoded `ColumnTypeString`
regardless of the type passed to `newColumnBuilder`. This caused body-parsed columns to
be written with type=0 (ColumnTypeString) instead of type=11 (ColumnTypeRangeString),
making `blockHasBodyParsed` always return false. Fixed by storing `typ` in the struct.
See blockio writer SPECS §8 for the column type encoding.

Back-ref: `internal/modules/executor/stream_log.go:blockHasBodyParsed`
Back-ref: `internal/modules/blockio/writer/column_types.go:stringColumnBuilder`

---

## NOTE-016: Three-Layer Regex Scan Optimization
*Added: 2026-03-04*

**Decision:** `ScanRegexFast` / `ScanRegexNotMatchFast` implement three compounding
optimizations over the original `StreamScanRegex` path:

1. **Pre-compiled regex** — the `*regexp.Regexp` is compiled once at query-compile time
   (in `logqlparser.Compile` or `traceql_compiler.compileColumnPredicateComparison`) and
   passed directly to the scan. `regexp.Compile` is never called inside the hot per-block
   scan loop.

2. **Literal prefix pre-filter** — `vm.RegexPrefixes(pattern)` extracts literal substrings
   that *must* appear in any matching string. `containsAnySubstring` checks these with
   `strings.Contains` before invoking the DFA/NFA engine. For patterns like `"error|warn"`,
   this eliminates the regex call entirely for the majority of rows that match neither
   prefix. Case-insensitive patterns (`(?i)`) are excluded from the pre-filter because
   `strings.Contains` is case-sensitive and would produce false negatives.

3. **Flat batch string extraction** — `col.StringValues()` builds a `[]string` of length
   `SpanCount` in a single pass over `StringIdx/StringDict`. The scan loop then indexes
   into this flat slice, eliminating the two-level dictionary dereference (`StringIdx[i]
   → StringDict[di]`) on every iteration.

4. **CI literal bypass (fold-contains)** — pure case-insensitive literal patterns like
   `(?i)error`, `(?i)error|warn` are detected at query-compile time via
   `vm.AnalyzeRegex(...).IsLiteralContains && .CaseInsensitive`. When true, the caller
   passes `nil` for `re` and pre-lowercased prefixes. The scan path then uses
   `strings.ToLower(v)` + `containsAnySubstring` instead of the regex engine entirely.
   This eliminates Go's NFA backtracking path (`regexp.(*Regexp).tryBacktrack`), which
   dominates CPU for `(?i)` patterns and accounted for ~60% of CPU in T3/Q20 profiling.
   Anchored patterns (`^`, `$`) are excluded — `IsLiteralContains` is false when any anchor
   is present, ensuring correctness.

**Correctness:** The prefix pre-filter is safe — it is a superset filter. Any row skipped
by `containsAnySubstring` cannot be matched by the regex (a regex can only match a string
that contains its required literal substrings). No false negatives are possible.

**CI bypass correctness:** `re == nil` is used only when `IsLiteralContains = true`, meaning
`strings.Contains(v, prefix)` is exactly equivalent to `re.MatchString(v)`. The anchored
pattern exclusion (`hasAnchor = false` required) ensures no false positives.

**Not-match path:** For the regex path, `ScanRegexNotMatchFast` uses optimizations 1 and 3
only. For the CI bypass path, it uses optimization 4 (fold-contains negated).

Back-ref: `internal/modules/executor/column_provider.go:ScanRegexFast`,
`internal/modules/executor/column_provider.go:ScanRegexNotMatchFast`,
`internal/modules/executor/column_provider.go:streamScanRegexFast`,
`internal/modules/blockio/reader/block.go:StringValues`,
`internal/vm/regex_optimize.go:RegexAnalysis.IsLiteralContains`

---

## NOTE-SL-017: blockLabelSet and sync.Pool for Zero-Alloc Hot Path
*Added: 2026-03-04*

**Decision:** `StreamLogs`, `StreamLogsTopK`, and `ExecuteLogMetrics` use a `sync.Pool`
of `*blockLabelSet` instead of building a `map[string]string` per row.

**Rationale:** On the pre-parsed block path (where `blockHasBodyParsed` returns true),
label values already live in block column dictionaries. `blockLabelSet.Get(key)` calls
`col.StringValue(rowIdx)` directly — one array index lookup, zero allocations. The overlay
map is allocated only when `Set` or `Delete` is called. For rows dropped by label
filters before any mutation, the per-row allocation cost is exactly zero (one pool
Get/Put, which is amortised across goroutines via the sync.Pool shard mechanism).

`buildBlockColMaps` is called once per block (not per row) to build the label-name →
original-column-name index. The `colNames` slice and `colMap` map are shared across all
rows in the block. The `*blockLabelSet` itself is reset per-row via `resetForRow`, which
clears the overlay and deleted maps in-place to reuse their backing arrays.

For `StreamLogsTopK`, entries stored in the heap call `Materialize()` immediately after
pipeline processing and before pool release. This ensures the heap holds stable
`map[string]string`-backed `LabelSet` values regardless of pool reuse.

**colName conflict resolution:** `buildBlockColMaps` uses a two-pass approach: `resource.*`
columns are registered first (explicit OTLP attributes take priority), then `log.*`
columns fill in gaps. Within each scope, `ColumnTypeString` wins over `ColumnTypeRangeString`
for the same stripped label name (explicit attribute beats auto-parsed body field).

Back-ref: `internal/modules/executor/block_label_set.go:blockLabelSet`,
`internal/modules/executor/block_label_set.go:buildBlockColMaps`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-018: Two-Pass Column Decode for Non-Matching Block Elimination
*Added: 2026-03-04*

**Decision:** All executor code paths use a two-pass block parse strategy:
1. **First pass** — `ParseBlockFromBytes` with `ProgramWantColumns(program)`, which decodes only the columns referenced by the query predicate (leaf columns from `Predicates.Nodes` tree, plus `Predicates.Columns` for negations/pushdown/log:body).
2. **Predicate evaluation** — `program.ColumnPredicate` runs against the minimal block.
3. **Early exit** — if `rowSet.Size() == 0`, skip this block entirely. No second parse.
4. **Second pass** (only for blocks with matches) — re-parse `bwb.RawBytes` with `nil` wantColumns to decode all columns for output delivery.

**Rationale:** Object-storage blocks commonly have 50–200 columns. A selective query
(e.g. `{span.http.method = "GET"}`) touches 1–3 columns for the predicate but would
previously decompress all columns in every candidate block. For queries where most
candidate blocks have no matches (the common case), this eliminates the bulk of
decompression work. For blocks with matches, the cost is a second parse, but those
are a small fraction of candidate blocks in typical trace/log queries.

**Exception — `Execute` and structural join:** These paths include identity columns
(`trace:id`, `span:id`, etc.) in `ProgramWantColumns(program, ...)` so output can be
built from the first-pass block directly. No second parse is ever issued.

**`ProgramWantColumns` must include ALL predicate column sources (updated 2026-03-06):**
- Leaf `Column` values from the `Nodes` RangeNode tree (via `collectNodeColumns`)
- `Columns []string` — negations, `log:body`, pushdown label-filter columns, and any
  column that needs row-level decode but produces no pruning node
Omitting any source causes `ParseBlockFromBytes` to skip the column, resulting in
false-empty predicate results (regression). See NOTE-030.

**When `wantColumns` is nil** (no predicates / match-all program), the first pass
decodes all columns and no second pass is issued.

Back-ref: `internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/modules/executor/executor.go:Execute`,
`internal/modules/executor/stream.go:Stream`,
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/metrics.go:ExecuteMetrics`,
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/stream_topk.go:topKScanBlocks`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`

---

## NOTE-019: GetColumn O(1) via colCols Slice in blockLabelSet
*Added: 2026-03-05*

**Decision:** `buildBlockColMaps` now resolves `*Column` pointers once per block into a
parallel `colCols []*Column` slice alongside `colNames`. All four `block.GetColumn(colNames[idx])`
calls in `blockLabelSet.Get`, `Has`, `Keys`, and `Materialize` are replaced with direct
`colCols[idx]` slice indexing. The `log:timestamp` and `log:body` column pointers are lifted
out of the per-row loop in `logTopKScan`, `logCollectAll`, and `StreamLogs`.

**Rationale:** `Block.GetColumn(name)` iterates the full `map[ColumnKey]*Column` to find a
column by name (O(M) where M = total columns in block, typically 50-200). For a block with
100 columns and 1000 rows passing the pipeline, each label access costs up to 100 map
iterations. The column set is fixed for a block's lifetime; resolving `*Column` pointers
once per block converts all per-row accesses to O(1) slice indexing.

**Invariant:** `colCols[i]` and `colNames[i]` always refer to the same column. Both slices
are built in lockstep in `buildBlockColMaps` — every append to `colNames` is immediately
followed by an append to `colCols`. `colCols` is nil'd in `releaseBlockLabelSet` before
pool return to prevent stale pointer retention.

Back-ref: `internal/modules/executor/block_label_set.go:buildBlockColMaps`,
`internal/modules/executor/block_label_set.go:blockLabelSet`

---

## NOTE-020: internStrings Reset Per Block to Bound Map Size
*Added: 2026-03-05*

**Decision:** `Reader.ResetInternStrings()` is called before the first-pass
`ParseBlockFromBytes` in each log scan block loop (`logTopKScan`, `logCollectAll`,
`StreamLogs`). It is NOT called before the second-pass parse.

**Rationale:** `r.internStrings` was originally scoped to the `*Reader` lifetime, meaning
it accumulated every unique string from every block across the scan. At 1GB scale this
grows to millions of entries, causing slow map lookups due to long probe chains. String
interning is only beneficial within a single block parse (deduplication of dictionary
entries); cross-block deduplication provides no benefit for streaming log queries.

Resetting before the first pass (via the for-range delete idiom, which preserves the
backing hash table) bounds the map to the unique strings in one block (hundreds, not
millions). Not resetting before the second pass is intentional: the second pass parses the
same raw bytes (same dictionary), so all strings from the first pass are already interned;
second-pass intern lookups are near-free cache hits.

**Thread safety:** `r.internStrings` is single-goroutine use only (documented invariant
on the field). The reset is safe because reset and parse are sequential on the same goroutine.

Back-ref: `internal/modules/blockio/reader/reader.go:ResetInternStrings`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-021: Time Pre-Filter Before Second-Pass Decode
*Added: 2026-03-05*

**Decision:** `logTopKScan` and `logCollectAll` now apply the `opts.TimeRange` filter
using the first-pass block BEFORE triggering the second-pass full decode.
`"log:timestamp"` is explicitly injected into the `wantColumns` copy at the start of
`StreamLogsTopK` to guarantee the timestamp column is always present in the first-pass block.

**Rationale:** The second-pass decode (NOTE-018) was firing for every block with any
`ColumnPredicate` match, even when every row was subsequently dropped by the time range
filter. For topK and time-range queries, many candidate blocks have no rows within the
requested time window. Before this fix, a full 50-200 column decode fired for each such
block (measured at 1.72s of 3.64s total for T7/Q55 with a 1GB dataset).

Pre-filtering with a cheap uint64 comparison (`ts < MinNano || ts > MaxNano`) on the
already-decoded timestamp column eliminates the full second-pass decode for blocks where
all predicate-matching rows fall outside the time window.

**wantColumns injection:** `"log:timestamp"` is added to a copy of `wantColumns` once per
`StreamLogsTopK` call (not per block). When `wantColumns` is nil (match-all program), no
copy is needed — the first-pass block already decodes all columns including the timestamp.

**Row loop simplification:** The per-row time filter that previously ran inside the main
row loop (after the second pass) is replaced by the pre-filter. The main row loop iterates
`keptByTime []int` (time-passing row indices) instead of `rowSet.ToSlice()`.

**`StreamLogs` scope:** Fix 3 does not apply to `StreamLogs` because `StreamLogs` has no
`opts.TimeRange` parameter; the pre-filter concept requires an external time range to
screen against.

Back-ref: `internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-022: AddColumnsToBlock Replaces Second-Pass ParseBlockFromBytes
*Added: 2026-03-05*

**Decision:** The second-pass `r.ParseBlockFromBytes(bwb.RawBytes, nil, meta)` in
`logTopKScan`, `logCollectAll`, and `StreamLogs` has been replaced with
`r.AddColumnsToBlock(bwb, nil)`.

**Rationale:** `parseBlockColumnsReuse` (the core of `ParseBlockFromBytes`) iterates
raw bytes from scratch on every call. When `wantColumns = nil` (second pass), it
decompresses ALL columns — including those already decoded in the first pass. This
was re-doing decompression work for predicate columns that were already present in
`bwb.Block.columns`, accounting for ~1.1-1.2s out of ~1.6-2.1s total for
`logTopKScan` in T7/Q55 and T12/Q86 profiling.

`AddColumnsToBlock(bwb, nil)` re-reads the raw block bytes to parse column metadata,
then for each column checks whether it already exists in `bwb.Block.columns`. Columns
present from the first pass are skipped (zero decompression cost). Only newly-encountered
columns are decompressed and added. The result is a fully-populated block with zero
redundant decompression work.

**Nil guard in AddColumnsToBlock:** A nil guard was added to the `addColumns` parameter
check. Previously, calling `AddColumnsToBlock(bwb, nil)` was a no-op (nil map read in
Go returns false, so the name filter always fired and no columns were added). The nil
guard changes semantics: `addColumns == nil` now means "add all missing columns".
This is the correct semantics for the second-pass use case.

**In-place mutation:** `AddColumnsToBlock` mutates `bwb.Block.columns` directly.
The `bwb` pointer is not reassigned at the call site. All code below the call that
accesses `bwb.Block` sees the fully-populated block.

**internStrings interaction (NOTE-020):** NOTE-020 states we do not reset
`r.internStrings` before the second pass because the second pass re-parses the same
dictionary (all first-pass strings are already interned, giving near-free cache hits).
With `AddColumnsToBlock`, only new columns are decoded. Their dictionary strings may
not be interned yet, but are looked up in the same bounded intern map (reset before the
first pass). New strings are added on miss. The intern map remains bounded to one
block's unique strings. NOTE-020's invariant is preserved.

Back-ref: `internal/modules/blockio/reader/reader.go:AddColumnsToBlock`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`

---

## NOTE-023: opts.TimeRange Forwarded to planner.Plan in StreamLogsTopK
*Added: 2026-03-05*

**Decision:** `StreamLogsTopK` previously passed `queryplanner.TimeRange{}` to
`planner.Plan`, disabling block-level time pruning entirely. It now passes
`queryplanner.TimeRange{MinNano: opts.TimeRange.MinNano, MaxNano: opts.TimeRange.MaxNano}`.

**Rationale:** For queries with an explicit time window, the planner can eliminate
blocks whose `[MinStart, MaxStart]` range lies entirely outside `[MinNano, MaxNano]`
before any I/O is issued. When `opts.TimeRange` is zero, `queryplanner.TimeRange{}`
is passed as before — no behavioral change.

**Relationship to NOTE-018:** NOTE-018 documented that `ObjectCache` is not at the
executor layer; it does not address time pruning. The original comment at the call
site referred to a concern about combining block-level time pruning with pipeline
row filtering causing false negatives. That concern is addressed by the two-level
filtering design: block pruning eliminates whole blocks only when they are entirely
outside the window (the planner's standard interval overlap test). Per-row time
filtering (NOTE-021) then handles sub-block granularity. These two levels are
complementary and do not cause false negatives.

**When opts.TimeRange is zero:** Zero is the identity value for `queryplanner.TimeRange`;
the planner treats zero MinNano/MaxNano as "no time constraint" and skips time pruning.
This ensures a zero `opts.TimeRange` produces identical behavior to the previous
hardcoded `queryplanner.TimeRange{}`.

Back-ref: `internal/modules/executor/stream_log_topk.go:StreamLogsTopK`

---

## NOTE-024: Literal Alternation Detection Fixes Overly Wide Regex Interval
*Added: 2026-03-05*

**Problem:** Go's `regexp/syntax` parser factors common prefixes from alternations before
`AnalyzeRegex` receives the parsed tree. For `"cluster-0|cluster-1"`, the parser produces
`Concat(Literal("cluster-"), CharClass([01]))`, so `AnalyzeRegex` returns a single prefix
`"cluster-"`. `buildPredicates` emits interval `["cluster-", "cluster-\xff"]`, matching
ALL cluster-X blocks — zero pruning for non-matching clusters like cluster-2, cluster-3.

**Fix:** `extractLiteralAlternatives(pattern string) []string` checks whether the raw
pattern string is a pure OR of complete literal strings (splitting on `|`, verifying each
part has no regex metacharacters: `` .*+?[]{}()^$\ ``). Called in the `len(analysis.Prefixes) == 1`
branch before emitting the interval. When it returns 2+ literals, those literals are used
as point lookups (same as the existing multi-prefix path for patterns like `"error|warn|info"`
which parse without common-prefix factoring). When it returns nil or a single literal, the
existing interval path is used unchanged.

**Why `len(lits) > 1`:** A single-literal pattern like `"debug"` splits to `["debug"]`.
Keeping it on the interval path is correct: a point lookup for `"debug"` would miss blocks
in range-index buckets whose lower boundary extends beyond the bare prefix (e.g.,
`"debug-service"` for prefix `"debug"`). The interval `["debug", "debug\xff"]` covers all such buckets.

**Correctness:** Point lookups are correct for full-string literals because the range index
stores the actual string value. A lookup for `"cluster-0"` finds all blocks containing at
least one span with that exact attribute value.

**Relationship to existing paths:**
- `"prod|staging"` (no common prefix) → already uses the multi-prefix path
  (`len(analysis.Prefixes) == 2`) — unaffected by this change.
- `"cluster-0|cluster-1"` (common prefix) → previously: wrong interval; now: point lookups.
- `"debug.*"` (single prefix with trailing wildcard) → still uses interval — unchanged.
- `"(?i)cluster-0|cluster-1"` → case-insensitive branch fires first — unchanged.

Back-ref: `internal/modules/executor/predicates.go:buildPredicates`,
`internal/modules/executor/predicates.go:extractLiteralAlternatives`

---

## NOTE-025: AddColumnsToBlock Removed — Lazy Column Decode Handles All Columns
*Added: 2026-03-05*

**Decision:** Removed `r.AddColumnsToBlock(bwb, nil)` calls from `StreamLogs`,
`StreamLogsTopK`, `logCollectAll`, and the second `ParseBlockFromBytes(raw, nil, meta)` call
from `MetricsLogsQuery`. These were the "second pass" that decoded all remaining columns
after the predicate filter.

**Rationale:** `ParseBlockFromBytes` now performs lazy registration for all columns not in
`wantColumns`. Columns not needed for predicate evaluation are registered with presence-only
decode (no zstd decompression). Their full decode is deferred to the first value access
(`StringValue`, `Uint64Value`, etc.), which calls `Column.decodeNow()` internally.
This eliminates the 90+ eager zstd decompressions per block for non-predicate columns,
reducing per-block decode cost from O(all columns) to O(predicate columns + accessed columns).

**Estimated savings:** For T9/Q66 (1997 blocks, ~90 non-predicate columns per block):
- Old: 1997 × 90 × zstd_decompress ≈ 0.87s
- New: 1997 × 90 × presence_only + 1997 × ~15 × zstd_decompress ≈ 0.04s + 0.15s = 0.19s

**Safety:** The `rawEncoding` slice points into `bwb.RawBytes`, which is valid for the
lifetime of `bwb`. All lazy decodes complete within the block's row loop iteration,
before `bwb` goes out of scope. Single-goroutine scan path — no locking needed.

Back-ref: `internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/metrics_log.go:MetricsLogsQuery`,
`internal/modules/blockio/reader/block.go:Column.decodeNow`,
`internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`

---

## NOTE-026: Dictionary-Level Float Parse for Numeric String Column Scans
*Added: 2026-03-05*

**Decision:** `scanStringDictFloat` pre-parses the string dictionary of a `ColumnTypeRangeString`
or `ColumnTypeString` column once per block, then uses boolean array lookups per row for numeric
threshold comparisons. It is invoked by `StreamScanGreaterThan`, `StreamScanGreaterThanOrEqual`,
`StreamScanLessThan`, and `StreamScanLessThanOrEqual` when the column is string-typed and the
comparison value is `float64`.

**Context:** T12 benchmark queries (`| latency_ms > 4500`) use numeric comparisons against
string-typed log attribute columns (e.g. `log.latency_ms` stored as `ColumnTypeRangeString` with
values "0"–"4999" as string OTLP attributes, not numeric). The previous hot path in `rowCompare`
called `strconv.ParseFloat(col.StringValue(rowIdx), 64)` for every row — approximately
`blockSpanCount × blocksScanned` ParseFloat calls per query (e.g. ~2M calls for a 1002-block scan
with 2000 rows/block).

**Optimization:** String columns use dictionary encoding (`StringDict []string` + `StringIdx []uint32`).
The dictionary typically has O(hundreds) unique values even when `SpanCount` is in the thousands.
`scanStringDictFloat`:
1. Iterates `col.StringDict` once (O(dictSize), typically ~700 entries).
2. Calls `strconv.ParseFloat` per dictionary entry — only those that survive are marked `matches[i] = true`.
3. Scans rows using `col.StringIdx[i]` → `matches[di]` — O(1) bool lookup per row, zero ParseFloat calls.

**Measured impact** (8-iteration benchmark, 1002-block scan):
- Q87 (`env+gt4000`, 507 blocks): −22% CPU (308→240 cpuMs)
- Q88 (`cluster+gt4000`, 482 blocks): −29% CPU (323→228 cpuMs)
- Q86 (`env+gt4500`, 1002 blocks): −12% CPU (486→428 cpuMs)

**Range pruning note:** For uniformly distributed string-numeric values (e.g. random 0–4999 across all
blocks), the range index assigns every block to every bucket (every block's min≈0, max≈4999). No range
index pruning is possible for this workload. The dictionary-level float parse is the primary lever.

Back-ref: `internal/modules/executor/column_provider.go:scanStringDictFloat`,
`internal/modules/executor/column_provider.go:StreamScanGreaterThan`,
`internal/modules/executor/column_provider.go:StreamScanGreaterThanOrEqual`,
`internal/modules/executor/column_provider.go:StreamScanLessThan`,
`internal/modules/executor/column_provider.go:StreamScanLessThanOrEqual`

---

## NOTE-027: encodeValue TypeString Cross-Encoding for Numeric Range Index
*Added: 2026-03-05*

**Decision:** `encodeValue` now handles `vm.TypeString` for `ColumnTypeRangeInt64` and
`ColumnTypeRangeFloat64`. When the string parses as the appropriate numeric type, the
function encodes it as 8-byte LE and returns true. Otherwise it returns false (falls back
to bloom-only pruning).

**Rationale:** When the writer promotes a string column to a numeric range index
(NOTE-040 in blockio NOTES.md), `r.RangeColumnType("log.latency_ms")` returns
`ColumnTypeRangeInt64`. A query `| latency_ms > 4500` where the VM produces
`{TypeInt, 4500}` already works via the existing TypeInt path. However, a query
`| latency_ms > "4500"` (string comparison) produces `{TypeString, "4500"}`. Without
this change, `encodeValue` returned `("", false)` → bloom-only pruning. With this
change, the string "4500" is parsed as int64 and encoded correctly.

**Safety:** Returns false (no pruning) when ParseInt/ParseFloat fails. No false negatives.
Only improves pruning; never removes correct blocks.

**Back-ref:** `internal/modules/executor/predicates.go:encodeValue`

---

## NOTE-029: Multi-Prefix Regex Path Must Use Full Literals, Not Go-Factored Partial Prefixes
*Added: 2026-03-05*

**Problem (T6/Q45 and T6/Q50 false-negative pruning):**

In `buildPredicates`, when `len(analysis.Prefixes) > 1`, the code previously encoded
the Go-extracted prefixes as point lookups. This is safe for patterns like `"prod|staging|dev"`
where Go does NOT factor a common prefix (3 independent literal branches). But for patterns
like `"us-east-1|us-west-2|eu-west-1"`, Go's regex parser factors the common prefix:

```
us-east-1|us-west-2|eu-west-1
→ Alternate(Concat(Literal("us-"), Alternate("east-1","west-2")), Literal("eu-west-1"))
→ analysis.Prefixes = ["us-", "eu-west-1"]
```

The partial prefix `"us-"` is NOT a valid range-index key — the index stores full values like
`"us-east-1"` and `"us-west-2"`. Using `"us-"` as a point lookup returns 0 blocks →
all blocks with `region=us-east-1` or `region=us-west-2` get pruned → **false negatives / data loss**.

**Fix:** In the multi-prefix branch, call `extractLiteralAlternatives(pattern)` on the raw
pattern string first. If all alternatives are pure literals (no metacharacters), use those
full literals as point lookups. If not (or if the call returns nil), fall back to bloom-only
to prevent false negatives.

**Why this is safe:** `extractLiteralAlternatives` checks the original pattern string before
Go's regex parser can factor it, so it recovers the original full literals regardless of
what Go's internal representation looks like.

**Scope of impact:** This bug affected any LogQL stream selector OR TraceQL predicate using
a regex alternation pattern with 2+ distinct prefix groups — typically patterns like
`region=~"us-east-1|us-west-2|eu-west-1"` where some alternatives share a prefix.
Patterns with no common prefix factoring (`"prod|staging|dev"`) were unaffected.

**Relationship to NOTE-024:** NOTE-024 fixed the single-prefix case (Go factors ALL alternatives
into a common prefix — enters the `len == 1` branch). NOTE-029 fixes the multi-prefix case
(some alternatives share a prefix, others don't — enters the `len > 1` branch).

**Back-ref:** `internal/modules/executor/predicates.go:buildPredicates` (multi-prefix branch)

---

## NOTE-028: SearchMetaColumns — Scoped Second-Pass Decode for Trace Search
*Added: 2026-03-05*

**Decision:** `StreamOptions.AllColumns` controls the second-pass decode in `Stream()`.
By default (`false`), the second pass decodes only `searchMetaColumns() ∪ wantColumns`
(predicate columns) instead of all columns. Callers that require full column access set
`AllColumns = true`. `streamFilterProgram` in `api.go` leaves `AllColumns` at its default.

**Rationale:** Mirrors Tempo's two-pass design (pkg/traceql/engine.go `ExecuteSearch`):
first pass fetches only filter predicate columns; second pass fetches a fixed set of
search-meta intrinsics plus any predicate columns already in scope. Tempo's parquet format
stores `RootSpanName`, `RootServiceName`, `TraceDuration`, and `TraceStartTime` as
pre-computed trace-level columns. Blockpack stores everything per-span, so the equivalent
set is 8 columns: `trace:id`, `span:id`, `span:start`, `span:end`, `span:duration`,
`span:name`, `span:parent_id` (root detection), `resource.service.name`.

`FindTraceByID` (`GetTraceByID` in api.go) uses `GetBlockWithBytes` directly and always
reads all columns — unaffected by this change. Only `QueryTraceQL` filter queries benefit.

**Back-ref:** `internal/modules/executor/stream.go:Stream`,
`internal/modules/executor/predicates.go:searchMetaColumns`

---

## NOTE-030: RangeNode Tree Replaces Flat QueryPredicates Maps
*Added: 2026-03-06*

**Decision:** `vm.QueryPredicates` was redesigned from a collection of flat maps
(`DedicatedColumns`, `DedicatedRanges`, `UnscopedColumnNames`, `DedicatedColumnsRegex`,
`AttributesAccessed`, `HasOROperations`) to a unified tree:

```go
type RangeNode struct {
    Min, Max *Value   // range interval bounds
    Column   string   // leaf: fully-scoped column name
    Pattern  string   // leaf: regex pattern
    Values   []Value  // leaf: equality values (OR'd)
    Children []RangeNode
    IsOR     bool     // composite: OR vs AND semantics
}

type QueryPredicates struct {
    Nodes   []RangeNode // AND-combined top-level pruning tree
    Columns []string    // columns for row-level decode only (negations, log:body)
}
```

**Why the flat maps were removed:**

1. `HasOROperations` was a coarse flag that disabled range-index pruning for the entire
   query when any OR was present — even when the OR was between scoped columns that could
   each be individually indexed. The tree allows OR composites to be built per-attribute
   rather than per-query.

2. `UnscopedColumnNames` was a separate bloom-only list for unscoped attributes. This
   prevented range-index pruning for unscoped equality predicates (`.service.name = "auth"`).
   With the tree, unscoped attributes expand to `{IsOR:true, Children:[resource.X, span.X, log.X]}`
   at compile time, enabling per-scope range-index lookup.

3. `DedicatedColumns` / `DedicatedRanges` / `DedicatedColumnsRegex` were separate maps
   that `buildPredicates` iterated independently. The tree unifies all predicate types under
   a single recursive `translateNode` function, eliminating the impedance mismatch between
   compile-time structure and runtime translation.

4. `AttributesAccessed` was used by `ProgramWantColumns` to include columns in the
   first-pass decode. `Columns []string` serves the same purpose more directly: only columns
   that need decode-but-not-pruning (negations, `log:body`, pushdown label filters) appear
   in `Columns`; columns that drive pruning nodes are collected from `Nodes` via
   `collectNodeColumns`.

**Backward-incompatible changes:** all callers of `buildPredicates`, `ProgramWantColumns`,
and the logql/traceql compilers were updated in the same commit. No old field names remain
in production code.

**Negation invariant:** `!=` and `!~` predicates produce no `Nodes`. A bloom filter
says "column possibly present" — removing a block because `service.name != "prod"` would
risk removing blocks where some spans DO satisfy the predicate. Negations only go to
`Columns` for the row-level decode.

**Back-ref:** `internal/modules/executor/predicates.go:buildPredicates`,
`internal/modules/executor/predicates.go:translateNode`,
`internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/vm/bytecode.go:RangeNode`,
`internal/vm/traceql_compiler.go:extractTraceQLPredicates`

---

## NOTE-031: Early-Skip Materialization in logTopKScan When Heap Is Full
*Added: 2026-03-06*

**Problem:** In `logTopKScan`, for every row that passes the pipeline filter, we previously
called `labels.Materialize()` (creates `map[string]string`), `collectLogStringAttrs()`
(creates another `map[string]string`), and `&LogEntry{}` (heap alloc) before trying to insert
into the top-K heap. For `limit=1000` queries with 200K+ matching rows, ~99.5% of these
allocations were immediately wasted — the row was inserted but immediately evicted, or never
even challenged the heap root.

**Fix:** After pipeline processing (which must still run for keep/drop decisions), check
whether the heap is already full and whether this row's timestamp can possibly displace the
current worst entry:

```go
if buf.Len() >= opts.Limit {
    worst := buf.entries[0].ts
    if (backward && ts <= worst) || (!backward && ts >= worst) {
        releaseBlockLabelSet(bls)
        continue
    }
}
```

For backward queries (newest-first), `ts <= worst` means this row is older than the oldest
entry currently in the heap, so it cannot improve the result. For forward queries (oldest-first),
`ts >= worst` means this row is newer than the newest entry in the heap. In either case, skip
materialization.

**Correctness:** The pipeline runs before the check, so any `| keep` / `| drop` / `| label_format`
mutations have already been applied to `line` and `labels`. The check only gates map allocation,
not pipeline evaluation. The heap invariant is maintained: we only skip rows that genuinely
cannot enter the top-K.

**Impact:** Eliminates ~99% of `Materialize()` and `collectLogStringAttrs()` calls in the
top-K path for queries where matching rows >> limit (e.g., T12/Q86 with 200K+ matching rows
and limit=1000). Expected ~30-40% CPU reduction for logfmt/numeric queries on large datasets.

**Back-ref:** `internal/modules/executor/stream_log_topk.go:logTopKScan`

---

## NOTE-032: aggBucketState Shared Between Log and Trace Metrics
*Added: 2026-03-08*

**Decision:** `aggBucketState` (defined in `metrics_log.go`) is reused by
`metrics_trace.go` rather than duplicating the struct.

**Rationale:** Both log and trace metric execution accumulate the same per-bucket state:
`sum`, `count`, `min`, `max`, and a `values []float64` slice for quantile. Keeping a
single struct definition in `metrics_log.go` avoids drift between the two
implementations. Any change to `aggBucketState` affects both callers, which is
intentional — they share the same aggregation semantics.

**Consequence:** `metrics_trace.go` imports nothing extra; it simply references
`aggBucketState` from the same package. The struct is unexported, which is correct —
it is an internal accumulator, not part of the public API.

**Back-ref:** `internal/modules/executor/metrics_log.go:aggBucketState`,
`internal/modules/executor/metrics_trace.go:traceUpdateBucket`

---

## NOTE-033: HISTOGRAM, QUANTILE, and STDDEV in ExecuteTraceMetrics
*Added: 2026-03-08*

**Decision:** Three previously unimplemented aggregate functions now work in
`ExecuteTraceMetrics`. Each uses the existing block scan + `traceUpdateBucket` +
`traceRowValue` pipeline with targeted extensions.

**HISTOGRAM — composite key with 3rd segment:**
`traceAccumulateRow` detects `FuncNameHISTOGRAM` and embeds the log2 bucket boundary
as a 3rd `"\x00"`-delimited segment in the composite key:
`"bucketIdx\x00attrGroupKey\x00bucketBoundary"`
For `span:duration` (stored as nanoseconds), the boundary is computed in seconds:
`pow(2, floor(log2(nanos/1e9)))`. For other fields: `pow(2, floor(log2(|v|)))`.
A dedicated `traceHistogramSeries` function reads back the 3rd segment to build
`__bucket` labels and counts. `ExecuteTraceMetrics` dispatches to it when HISTOGRAM.
Non-HISTOGRAM functions continue to use the 2-segment key format unchanged.

**Why separate series builder:** The 2-segment key produces one series per `attrGroupKey`.
The 3-segment key produces one series per `(attrGroupKey, bucketBoundary)`. These
are structurally different — `traceBuildDenseSeries` cannot handle 3-segment keys
without special-casing that would obfuscate the non-histogram path.

**QUANTILE — values slice + logComputeQuantile reuse:**
`traceUpdateBucket` adds a `FuncNameQUANTILE` case that appends each span's field
value to `bucket.values` (same as `logFuncQuantileOverTime` in `metrics_log.go`).
`traceRowValue` calls `logComputeQuantile(bucket.values, quantile)` — both functions
are in the same package, so no import is needed. The `quantile float64` parameter is
threaded through `traceRowValue`'s signature (mirroring `logRowValues`'s pattern).

**STDDEV — Welford online algorithm with `aggBucketState.mean` and `.m2`:**
Two new fields (`mean`, `m2`) are added to `aggBucketState` in `metrics_log.go`.
They are zero-initialized for log-metric buckets (no behavioral change for any log path).
`traceUpdateBucket` applies Welford's recurrence:
  delta = v - mean; count++; mean += delta/count; m2 += delta*(v-mean)
`traceRowValue` emits `sqrt(m2/(count-1))` for sample stddev, or NaN when `count < 2`.

**Back-ref:** `internal/modules/executor/metrics_trace.go:traceUpdateBucket`,
`internal/modules/executor/metrics_trace.go:traceRowValue`,
`internal/modules/executor/metrics_trace.go:traceHistogramSeries`,
`internal/modules/executor/metrics_trace.go:traceHistogramBucket`,
`internal/modules/executor/metrics_log.go:aggBucketState`

---

## NOTE-034: ExecuteStructural — Three-Phase Structural Query Algorithm
*Added: 2026-03-08*

`ExecuteStructural` ports the three-phase structural query algorithm from `api.go`
(`streamStructuralQuery`) into the modules executor package.

**Phase 1 (Collect):** All blocks are scanned via `queryplanner.Plan(nil, TimeRange{})` —
no pruning is applied. For each span, we record spanID, parentID, leftMatch, rightMatch keyed
by `[16]byte` trace ID. Using a fixed-size array key avoids the `fmt.Sprintf("%x", ...)` hex
allocation per span that the api.go version performs.

**Phase 2 (Resolve):** Parent indices are resolved with a `string(spanID)→index` map. Using
`string([]byte)` as a map key avoids allocating a persistent string; Go optimises this pattern.

**Phase 3 (Evaluate):** The structural operator is applied per trace. Matching right-side span
indices are deduplicated and emitted as `SpanMatch` entries.

**No lazy coalesced groups / early-stop:** Structural queries still rely on
`queryplanner.FetchBlocks` / `Reader.ReadBlocks` for aggressive I/O coalescing, but they do
not use the lazy coalesced-group / bloom-filter early-stop machinery. All candidate blocks
from `Plan(nil, TimeRange{})` are fetched up front (`FetchBlocks` is used directly, same as
`Execute` in `executor.go`) and then fully scanned.

**allMatchSet:** When a left or right filter is nil (empty `{}`), a local `allMatchSet` rowSet
is returned instead of calling `ColumnPredicate`. This avoids compiling a trivial program.

Back-ref: `internal/modules/executor/stream_structural.go`

---

## NOTE-035: Execute → Collect Migration and Sub-File Sharding
*Added: 2026-03-11*

**Decision:** The original `Execute` method (returning `*Result` with `[]SpanMatch`) has been
replaced by `Collect` (returning `[]MatchedRow`). The `Stream` method has been merged into
`Collect` — both now use the same lazy coalesced-group I/O path.

**Rationale:**
- `MatchedRow` carries a reference to the parsed `*Block`, allowing callers to extract
  arbitrary columns after collection (e.g., `SpanMatchFromRow`, `IterateFields`). The old
  `SpanMatch` required the executor to eagerly extract TraceID/SpanID during scan.
- Lazy coalesced-group I/O (previously Stream-only) is now the single I/O strategy for all
  filter queries. `FetchBlocks` (eager bulk fetch) is retained only for `ExecuteStructural`.

**Nil program semantics change:** The old `Execute` panicked on nil `program` when blocks
contained spans. `Collect` returns an error instead. This is safer for callers and consistent
with `CollectTopK`, `StreamLogs`, and `StreamLogsTopK` which all return errors for nil program.

**Sub-file sharding (added 2026-03-11):** `CollectOptions.StartBlock` and `BlockCount` allow
the frontend sharder to partition a single blockpack file across multiple parallel jobs.
Sharding is applied post-planner (after pruning) so all statistics remain file-wide; only
the block iteration window is narrowed. Both `Collect` and `CollectTopK` support sharding.

Back-ref: `internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/stream_topk.go:CollectTopK`

---

## NOTE-036: Intrinsic-Guided Block Pruning Design (Phase 3)
*Added: 2026-03-11*

**Change:** Added `ProgramIsIntrinsicOnly` and `BlocksFromIntrinsicTOC` to `predicates.go`,
plus a test file `intrinsic_pruning_test.go`.

**Why intrinsic columns in the executor?**

The executor is the natural owner of block selection logic. Adding intrinsic-guided pruning
here, rather than in the reader or planner, keeps the selection policy in one place: the
executor knows both the program (query predicates) and the reader (file contents).

**Why `ProgramIsIntrinsicOnly` instead of per-column checks?**

Block pruning is only useful when ALL predicates can be evaluated against the intrinsic
section. If even one predicate references a dynamic attribute (`span.http.method`), the
executor must read blocks anyway. Checking "intrinsic-only" is a single gate that avoids
partial evaluation.

**Why is Phase 3b deferred?**

Phase 3b (exact row-level pruning via column blobs) requires: (a) reading column blobs from
S3, (b) binary search or dict lookup for predicate matching, and (c) mapping matched rows
back to block indices. This is non-trivial and should be benchmarked against the existing
planner before being enabled. The Phase 3 stub establishes the API without blocking the
rest of the feature.

**`resource.service.name` as "practically intrinsic":**

The service name is stored as an OTLP resource attribute, but in practice every span has
one. Including it in `traceIntrinsicColumns` enables common "filter by service" queries
to be classified as intrinsic-only, making them eligible for Phase 3b pruning.

**Files changed:** `internal/modules/executor/predicates.go` (new functions + maps),
`internal/modules/executor/intrinsic_pruning_test.go` (new tests).

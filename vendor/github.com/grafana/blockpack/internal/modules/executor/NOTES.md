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

**Decision:** The executor calls `BuildPredicates(r, program)` (in `predicates.go`),
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
(`modules_reader.NewReaderFromProvider`) → this executor (`executor.Collect`).

**Coverage provided by the blockio integration tests:**
- `bloomPredicates` with real data (AND and OR query paths, EX-01, EX-03, EX-04, EX-06)
- `Collect` empty-file short-circuit (EX-05)
- Multi-block scanning with `BlocksScanned >= 2` assertion (EX-04)
- Zero-match result (EX-02)

`internal/modules/executor/executor_test.go` additionally covers:
- `Options.Limit` early-exit (EX-08)
- `SpanMatch.TraceID` / `SpanMatch.SpanID` field population (EX-09)
- `statsOut.TotalBlocks` populated via CollectStats (EX-10)

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

## NOTE-010: BuildPredicates Consumes DedicatedRanges; encodeValue Handles Plain Column Types
*Added: 2026-03-02 — superseded by NOTE-030 (2026-03-06)*

**Superseded:** The `DedicatedRanges` map no longer exists. Range predicates are now
represented as `RangeNode{Min, Max}` in the `Nodes` tree and translated by `translateNode`.
The `encodeValue` function and its handling of plain vs `Range*` column types is unchanged.

**Original decision (historical):** `BuildPredicates` included a loop over
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
`BuildPredicates`) analyzes regex patterns using `vm.AnalyzeRegex` and produces
range-index predicates when the pattern has an extractable literal prefix.

**Design:**
1. `vm.AnalyzeRegex(pattern)` parses the regex syntax tree (via `regexp/syntax`) and
   extracts literal prefixes from optimizable patterns: `foo.*`, `^error`, `error|warn`.
2. For optimizable patterns, `BuildPredicates` encodes each prefix as a `RangeString`
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

Fix: for any case-sensitive regex with a **single extracted prefix**, `BuildPredicates`
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

Back-ref: `internal/modules/executor/predicates.go:BuildPredicates`,
`internal/vm/regex_optimize.go:AnalyzeRegex`

---

## NOTE-012: Stream vs Execute — Lazy Callback vs Eager Batch
*Added: 2026-03-03* | *Historical — both `Stream` and `Execute` have been removed; see addendum below.*

**Decision:** `Stream` was a separate method from `Execute` rather than a flag on `Options`.

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

**Callers after this task:** `api.go:streamFilterProgram` calls `executor.Collect`;
`api.go:streamLogProgram` calls `executor.CollectLogs`. The `Stream` method has been
removed — see NOTE-035. No inline block-scan loops remain in api.go.

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

`buildBlockColMapsWithLogCache` is called once per block (not per row) to build the label-name →
original-column-name index. The `colNames` slice and `colMap` map are shared across all
rows in the block. The `*blockLabelSet` itself is reset per-row via `resetForRow`, which
clears the overlay and deleted maps in-place to reuse their backing arrays.

For `StreamLogsTopK`, entries stored in the heap call `Materialize()` immediately after
pipeline processing and before pool release. This ensures the heap holds stable
`map[string]string`-backed `LabelSet` values regardless of pool reuse.

**colName conflict resolution:** `buildBlockColMapsWithLogCache` uses a two-pass approach: `resource.*`
columns are registered first (explicit OTLP attributes take priority), then `log.*`
columns fill in gaps. Within each scope, `ColumnTypeString` wins over `ColumnTypeRangeString`
for the same stripped label name (explicit attribute beats auto-parsed body field).

Back-ref: `internal/modules/executor/block_label_set.go:blockLabelSet`,
`internal/modules/executor/block_label_set.go:buildBlockColMapsWithLogCache`,
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

**Exception — `ExecuteStructural`:** This path includes identity columns
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
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`,
`internal/modules/executor/stream_topk.go:topKScanBlocks`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`

---

## NOTE-019: GetColumn O(1) via colCols Slice in blockLabelSet
*Added: 2026-03-05*

**Decision:** `buildBlockColMapsWithLogCache` resolves `*Column` pointers once per block into a
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
are built in lockstep in `buildBlockColMapsWithLogCache` — every append to `colNames` is immediately
followed by an append to `colCols`. `colCols` is nil'd in `releaseBlockLabelSet` before
pool return to prevent stale pointer retention.

**NOTE-038:** `buildBlockColMaps` was removed (2026-03-16) — it was a strict subset of
`buildBlockColMapsWithLogCache`. Non-log callers now call `buildBlockColMapsWithLogCache`
and discard the `logStrNames`/`logStrCols` returns with `_, _`. This eliminates ~55 lines
of duplicated column-selection logic.

Back-ref: `internal/modules/executor/block_label_set.go:buildBlockColMapsWithLogCache`,
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
`"cluster-"`. `BuildPredicates` emits interval `["cluster-", "cluster-\xff"]`, matching
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

Back-ref: `internal/modules/executor/predicates.go:BuildPredicates`,
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

In `BuildPredicates`, when `len(analysis.Prefixes) > 1`, the code previously encoded
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

**Back-ref:** `internal/modules/executor/predicates.go:BuildPredicates` (multi-prefix branch)

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

**Back-ref:** `internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/predicates.go:searchMetaColumns`

**Addendum (2026-03-25):** `searchMetaCols` was shrunk as part of the intrinsic-section
migration (NOTE-050). Trace-signal identity columns (trace:id, span:id, span:start, span:end,
span:duration, span:name, span:parent_id, resource.service.name, span:status,
span:status_message, span:kind) were removed from `searchMetaCols` because they are now
stored exclusively in the intrinsic TOC section and injected directly into `secondPassCols`
via the `traceIntrinsicColumns` loop (all 11 columns). These columns are absent from block
payloads; identity values are fetched via `lookupIntrinsicFields`.

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
   that `BuildPredicates` iterated independently. The tree unifies all predicate types under
   a single recursive `translateNode` function, eliminating the impedance mismatch between
   compile-time structure and runtime translation.

4. `AttributesAccessed` was used by `ProgramWantColumns` to include columns in the
   first-pass decode. `Columns []string` serves the same purpose more directly: only columns
   that need decode-but-not-pruning (negations, `log:body`, pushdown label filters) appear
   in `Columns`; columns that drive pruning nodes are collected from `Nodes` via
   `collectNodeColumns`.

**Backward-incompatible changes:** all callers of `BuildPredicates`, `ProgramWantColumns`,
and the logql/traceql compilers were updated in the same commit. No old field names remain
in production code.

**Negation invariant:** `!=` and `!~` predicates produce no `Nodes`. A bloom filter
says "column possibly present" — removing a block because `service.name != "prod"` would
risk removing blocks where some spans DO satisfy the predicate. Negations only go to
`Columns` for the row-level decode.

**Back-ref:** `internal/modules/executor/predicates.go:BuildPredicates`,
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
with `StreamLogs`, and `StreamLogsTopK` which all return errors for nil program.

**Sub-file sharding (added 2026-03-11):** `CollectOptions.StartBlock` and `BlockCount` allow
the frontend sharder to partition a single blockpack file across multiple parallel jobs.
Sharding is applied post-planner (after pruning) so all statistics remain file-wide; only
the block iteration window is narrowed. `Collect` supports sharding.

Back-ref: `internal/modules/executor/stream.go:Collect`

---

## NOTE-036: planBlocks Unification — Intrinsic TOC Pruning in All Query Paths
*Added: 2026-03-14*

**Decision:** All five query paths (Collect, ExecuteTraceMetrics, ExecuteLogMetrics,
StreamLogs, CollectLogs) now use a shared `planBlocks` helper that runs:
1. `BuildPredicates` — converts vm.Program predicates into planner predicates
2. `PlanWithOptions` — range-index/fuse/CMS pruning and time range filtering
3. `fileLevelReject` — O(1) file-level fast reject using KLL bucketMin/bucketMax boundaries
4. `BlocksFromIntrinsicTOC` intersection — intrinsic-column fast reject

**Rationale:** Before this change, intrinsic TOC pruning was only active in `Collect`.
The four other paths called `planner.Plan` or `planner.PlanWithOptions` directly without
the `BlocksFromIntrinsicTOC` intersection step. Queries involving intrinsic columns (e.g.
trace:id, span:id) on the metrics or log paths could therefore scan blocks that would have
been pruned on the trace search path — wasting I/O.

**Why safe:** `BlocksFromIntrinsicTOC` returns nil when no pruning is possible (no intrinsic
section, no intrinsic predicates, or all blocks survive). The intersection is only applied
when it actually shrinks the selected set, so correctness is preserved and there is no
performance regression for queries with no intrinsic predicates.

Back-ref: `internal/modules/executor/plan_blocks.go:planBlocks`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`,
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/stream_log_topk.go:CollectLogs`

---

## NOTE-037: LogAttrs — Flat Slice Struct Instead of map[string]string
*Added: 2026-03-15*

**Decision:** `LogEntry.LogAttrs` field changed from `map[string]string` to a new
`LogAttrs` struct with parallel `Names []string` and `Values []string` slices.
`collectLogStringAttrs` uses slice append instead of map insertion.

**Rationale:** Per-row map allocation was a hot path. Most rows have zero or one
log.* attribute (log.level, log.detected_level). A flat slice struct avoids the
map header allocation (~8 bytes overhead) and internal hash-table bucket
allocations entirely. For the common zero-attribute case, both slices remain nil.

**Impact on callers:**
- `logEntryFields.GetField`: linear scan over Names slice (typically 0–2 entries;
  faster than map lookup for such small N due to cache locality).
- `logEntryFields.IterateFields`: iterate parallel slice indices.
- `converter.logAttrsToLabelAdapters`: iterate `.Names`/`.Values` directly.
- Tests: no assertions on LogAttrs type; no changes needed.

Back-ref: `internal/modules/executor/stream_log.go:LogAttrs`,
`internal/modules/executor/stream_log_topk.go:collectLogStringAttrs`,
`api.go:logEntryFields`,
`benchmark/lokibench/converter.go:logAttrsToLabelAdapters`

---

## NOTE-039: cmp3 Generic Helper Reduces rowCompare Cyclomatic Complexity
*Added: 2026-03-16*

**Decision:** Introduced `cmp3[T cmp.Ordered](a, b T) (int, bool)` — a one-line wrapper
around `cmp.Compare` — in `column_provider.go`. All repeated three-way `switch { case v < t:
return -1, true; case v > t: return 1, true; default: return 0, true }` blocks in
`rowCompare` are replaced with `return cmp3(v, t)`.

**Rationale:** `rowCompare` had cyclomatic complexity 40 (CRITICAL tier) with the same
comparison pattern repeated 8+ times across all numeric and string types. Using the
stdlib `cmp.Compare` (available since Go 1.21) reduces line count from ~120 to ~50 and
cyclomatic complexity from 40 to ~20 with no behavior change.

**Float64 parse path:** The `ColumnTypeString` + `float64` branch (numeric threshold against
string column, used by `| latency_ms > 100` pipeline filters) retains its `strconv.ParseFloat`
call — `cmp3(parsed, f)` replaces only the comparison switch, not the parse step.

Back-ref: `internal/modules/executor/column_provider.go:rowCompare`,
`internal/modules/executor/column_provider.go:cmp3`

---

## NOTE-040: scanIntrinsicLeafRefs leaf.Values Loop Merged From Two Passes to One
*Added: 2026-03-16*

**Decision:** In `scanIntrinsicLeafRefs` (dict-format + exact-values branch), two sequential
`for _, v := range leaf.Values` loops were merged into one. Previously:
1. First loop: built `wantStr map[string]struct{}` and `wantInt map[int64]struct{}`
2. Second loop: built `bloomKeys [][]byte`

Both loops iterated the same `leaf.Values` slice with an identical `switch v.Type { case
TypeString/TypeInt/TypeDuration }` structure, with no ordering dependency between the
match-set population and bloom-key construction.

**Rationale:** The merge is a strict reduction — both outputs are constructed in a single
pass. The identical switch structure makes the merge mechanical. `bloomKeys` is appended
inline with the map insertions. No behavioral change.

**Impact:** Reduces `scanIntrinsicLeafRefs` by ~20 lines and cyclomatic complexity from
~35 to ~28.

Back-ref: `internal/modules/executor/predicates.go:scanIntrinsicLeafRefs`

---

## NOTE-041: logTopKEntry.ts Field Removed — Use entry.TimestampNanos Directly
*Added: 2026-03-16*

**Decision:** `logTopKEntry.ts uint64` has been removed. All heap comparisons
(`logTopKHeap.Less`, `logTopKCanSkipBlock`, `logTopKInsert`, early-skip guard, sort closures
in `logDeliverAll`) now read `entry.TimestampNanos` directly.

**Rationale:** `ts` was always set to the same value as `entry.TimestampNanos` at every
construction site. The redundant field added 8 bytes per entry × limit (e.g., 1000 entries =
8KB overhead), plus one extra assignment per row. There was no case where `ts != entry.TimestampNanos`.
Removing the field eliminates the class of bug where one is updated but the other is not.

Back-ref: `internal/modules/executor/stream_log_topk.go:logTopKEntry`,
`internal/modules/executor/stream_log_topk.go:logTopKHeap`

---

## NOTE-042: iterateLogRows Extracts Shared Block-Iteration Boilerplate
*Added: 2026-03-16*

**Decision:** The ~130 lines of shared block-iteration boilerplate that was duplicated between
`logTopKScan` and `logCollectAll` has been extracted into `iterateLogRows`. Both callers
are now ~15-line wrappers that pass their differing logic as callbacks:

- `canSkipBlock func(meta shared.BlockMeta) bool` — `logTopKScan` passes heap-based block pruning;
  `logCollectAll` passes nil (never skip).
- `fn func(ts uint64, entry LogEntry) bool` — `logTopKScan` passes the NOTE-031 early-skip guard
  + heap insertion; `logCollectAll` passes a slice append.

**Rationale:** Both functions shared: `CoalescedGroups` + `blockToGroup`, `ReadGroup` loop,
`ResetInternStrings` + `ParseBlockFromBytes`, `ColumnPredicate` evaluation, NOTE-021 time
pre-filter, column cache setup, and the per-row pipeline loop. Any fix to the shared boilerplate
(e.g., NOTE-021, NOTE-001, NOTE-SL-016) previously had to be applied twice. The extraction
eliminates this duplication class.

**Complexity impact:** Combined cyclomatic complexity drops from (36+30)=66 to one ~30
(`iterateLogRows`) + two trivial callers.

Back-ref: `internal/modules/executor/stream_log_topk.go:iterateLogRows`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`,
`internal/modules/executor/stream_log.go:StreamLogs`
## NOTE-038: Unified Intrinsic Pre-Filter — Partial-AND for Mixed Queries
*Added: 2026-03-16*

**Decision:** `collectFromIntrinsicRefs` is rewritten as a unified 4-case dispatcher
replacing the separate `collectFromIntrinsicRefs` (plain) and `collectTopKFromIntrinsicRefs`
(topK) functions. The `Collect` entry gate changes from `ProgramIsIntrinsicOnly` to
`hasSomeIntrinsicPredicates`, extending the intrinsic pre-filter to mixed queries.

**Problem with the old design:**
Mixed queries like `{ resource.service.name = "svc" && span.http.method = "GET" }` bypassed
the intrinsic pre-filter entirely because `ProgramIsIntrinsicOnly` returned false. The pre-filter
was an all-or-nothing gate: if any non-intrinsic column appeared, the full block scan ran.

**New design — partial-AND semantics:**
`evalNodeBlockRefsPartialAND` is a new variant of `evalNodeBlockRefs` where AND nodes skip
unevaluable (non-intrinsic) children rather than failing. The result is a superset of the
true matching rows. `blockRefsFromIntrinsicPartial` uses this variant to build candidate refs
for mixed queries.

**Why partial-AND is safe for AND nodes:**
The pre-filter is a superset: it may return rows that do NOT satisfy non-intrinsic conditions.
After fetching the candidate blocks, `program.ColumnPredicate` re-evaluates the full predicate,
eliminating false positives. This is the same principle used by bloom-filter + range-index block
pruning in `planBlocks`: the planner produces a superset, the VM corrects it.

**Why OR nodes remain fail-fast:**
An unevaluable OR child would produce an unbounded result — potentially the entire file's row
set. There is no useful partial OR pre-filter: you cannot narrow candidates when any alternative
is unconstrained. Keeping OR fail-fast means `blockRefsFromIntrinsicPartial` falls through to
the full block scan when an evaluable-but-unevaluable-OR is encountered, which is conservative
but correct.

**4-case dispatch:**

| ProgramIsIntrinsicOnly | TimestampColumn | Case | Block reads |
|---|---|---|---|
| true | empty | A | minimal (candidate blocks) |
| true | set | B | zero (IntrinsicFields rows) |
| false | empty | C | minimal (candidate blocks + ColumnPredicate) |
| false | set | D | minimal (candidate blocks + ColumnPredicate + topK heap) |

**Global top-K correctness for Case D:**
The partial-AND pre-filter is a superset — it never excludes a row that satisfies all intrinsic
predicates. Therefore all true matching rows (those satisfying both intrinsic and non-intrinsic
conditions) are present among the candidate blocks. ColumnPredicate eliminates non-intrinsic
false positives. topKScanRows then finds the globally correct top-K timestamp order within the
true matching rows. SPEC-STREAM-7 is preserved.

**Dead code removed:**
`buildPredicateMatchSet`, `evalNodeMatchKeys`, `collectTopKFromIntrinsicRefs`,
`unionSortedKeys`, `intersectSortedKeys` — all deleted. `collectTopKFromIntrinsicRefs`
was a parallel implementation of `evalNodeBlockRefs` in packed-key representation; that
duplication is eliminated by inlining the packed-key construction into `collectIntrinsicTopK`.

**Relationship to NOTE-039 (in code comments referencing the old design):**
NOTE-039 stated that both OR and AND must fail fast on unevaluable children because refs are
returned directly without VM re-evaluation. This remains true for `evalNodeBlockRefs` (used
on the pure-intrinsic path). The new `evalNodeBlockRefsPartialAND` explicitly relaxes the AND
constraint for the mixed-query path, where VM re-evaluation is mandatory.

Back-ref: `internal/modules/executor/predicates.go:hasSomeIntrinsicPredicates`,
`internal/modules/executor/predicates.go:evalNodeBlockRefsPartialAND`,
`internal/modules/executor/predicates.go:blockRefsFromIntrinsicPartial`,
`internal/modules/executor/stream.go:collectFromIntrinsicRefs`

---

## NOTE-039: EX-INT-06 Comment Updated — Case C, Not True Fallback
*Added: 2026-03-16*

The comment in `TestIntrinsicFastPath_FallbackToBlockScan` previously said "fast path is
not applicable" for the query `{ resource.service.name =~ "loki-.*" && span.http.method = "GET" }`.
After the gate change from `ProgramIsIntrinsicOnly` to `hasSomeIntrinsicPredicates` (NOTE-038),
this query now takes Case C (mixed + no sort): the intrinsic pre-filter narrows candidates
by service name, then VM re-evaluation eliminates all rows because no spans have http.method
set. The comment was updated to accurately describe Case C behaviour.

---

## NOTE-040: EX-INT-13 — True Non-Intrinsic-Only Fallback Path Test
*Added: 2026-03-16*

Added `TestCollect_NonIntrinsicOnly_FallsBackToBlockScan` (EX-INT-13) to exercise the true
fallback path: a query with zero intrinsic leaves (e.g., `{ span.http.method = "GET" }`)
causes `hasSomeIntrinsicPredicates` to return false, and the executor falls through to the
full block scan. The test confirms no error and no panic, and returns 0 results because none
of the test spans have `http.method` set.

---

## NOTE-041: SPECS.md §4.2 MatchedRow — IntrinsicFields Field Added
*Added: 2026-03-16*

The `IntrinsicFields modules_shared.SpanFieldsProvider` field added to `MatchedRow` in the
unified pre-filter implementation (NOTE-038) was absent from the §4.2 struct definition in
SPECS.md. The definition now matches the actual struct in `stream.go`. `SPEC-STREAM-9`
already described the semantics (IntrinsicFields rows for Case A/B zero-block-read paths);
this change ensures the struct definition itself is also complete.

---

## NOTE-042: collectIntrinsicTopK Sort Path — Map Lookup Replaces O(N) Scan of Timestamp Blob
*Added: 2026-03-16*

**Decision:** In `collectIntrinsicTopK`'s sort path (M < sortScanThreshold), the O(N) scan
of the timestamp blob via `ScanFlatColumnRefsFiltered` is replaced for small M by a
map-then-sort approach: build a `packed-key → timestamp` map from the full decoded
`IntrinsicColumn`, look up each matching ref's timestamp in O(1), sort the M pairs by
timestamp, and take top K.

**Problem with the previous stash approach:**
The stash tried to use `planBlocks` block ordering (KLL sketch, newest-first for MostRecent)
to iterate matching refs. Block ordering only provides inter-block ordering — it says nothing
about per-row timestamp ordering within a single block. When all matching refs live in a
single block (common in tests and for rare-service queries), block ordering provided zero
information, violating the descending-timestamp assertion in EX-INT-10.

**Key observation — flat column layout:**
The flat timestamp column (`span:start`, `log:timestamp`) stores `BlockRefs` in ascending
timestamp order, NOT in ascending packed-key order. Binary search by packed key is therefore
incorrect. The correct lookup is: build a `map[uint32]uint64` (packed-key → timestamp) from
the decoded column's parallel `BlockRefs` and `Uint64Values` arrays, then look up each ref.

**Performance characteristics:**
- Map build: O(N) — same cost as the scan path but paid once per query.
- Per-ref lookup: O(1) hash map (vs O(N/M) amortized for scan).
- Sort: O(M log M) for M matching refs.
- Effective for M << N (rare-service queries); scan path remains for M >= sortScanThreshold.
- For Q35 (M=15, N=130K): map lookup + sort ≈ 130K insertions + 15 log(15) comparisons.
  The dominant cost is the O(N) map build, but this avoids decompressing the blob again
  since `GetIntrinsicColumn` returns the already-decoded column from cache.

**Test fix:**
`TestCollect_PureIntrinsicWithSort_ZeroBlockRead` (EX-INT-10) was failing because the stash
used block-level ordering which does not provide per-row timestamp order within a single
block. Map-then-sort correctly retrieves per-row timestamps regardless of block/row layout.

Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopK`

---

## NOTE-043: CollectStats ExecutionPath Telemetry and Fast-Path OnStats Wiring
*Added: 2026-03-17*

**Decision:** `CollectStats` gains four new fields to identify which of the eight
execution paths ran for a given `Collect` call:

- `ExecutionPath string`: one of "intrinsic-plain" (Case A), "intrinsic-topk-sort"
  (Case B map path), "intrinsic-topk-scan" (Case B scan path), "mixed-plain" (Case C),
  "mixed-topk" (Case D), "block-plain" (block-scan no sort), "block-topk" (block-scan
  with topK heap), "intrinsic-need-block-scan" (fast path tried but fell through).
- `IntrinsicRefCount int`: number of refs from `BlockRefsFromIntrinsicTOC` (M for Case B).
- `IntrinsicScanCount int`: entries visited by `ScanFlatColumnRefsFiltered` in Case B
  scan path. Zero for the map path and all other paths.
- `MixedCandidateBlocks int`: number of unique candidate blocks from
  `blockRefsFromIntrinsicPartial` for Cases C and D. Zero for all other paths.

**Problem solved:** The `OnStats` defer in `Collect` (SPEC-STREAM-6) fires only for the
block-scan fallback. Fast-path returns from `collectFromIntrinsicRefs` exited before the
defer was registered, giving callers no stats for the most common query case (intrinsic
fast path). This was an observable gap: setting `OnStats` on a service-name filter query
produced no callback at all.

**Fix:** A `*CollectStats` pointer is passed to `collectFromIntrinsicRefs` and all four
sub-functions. Each path populates its fields. `Collect` calls `opts.OnStats(fastStats)`
synchronously on successful fast-path return (`err != errNeedBlockScan`). For
`errNeedBlockScan`, the partial stats are discarded and the block-scan defer provides the
final stats as before.

**`SortScanThreshold` exported:** `sortScanThreshold` (unexported const) renamed to
`SortScanThreshold` (exported var) to allow test overrides that force the scan path for
small M. Production code does not modify this variable.

**No double callback:** Fast-path and block-scan callbacks are mutually exclusive.
`opts.OnStats(fastStats)` is called only when `err != errNeedBlockScan`, which means
`return rows, err` follows immediately — the block-scan defer is registered AFTER this
return and therefore never fires for the fast-path case.

Back-ref: `internal/modules/executor/stream.go:CollectStats`,
          `internal/modules/executor/stream.go:Collect`,
          `internal/modules/executor/stream.go:collectFromIntrinsicRefs`,
          `internal/modules/executor/stream.go:SortScanThreshold`

---

## NOTE-044: collectIntrinsicTopK KLL Path — Block-Level MaxStart Ordering
*Added: 2026-03-17*

**Decision:** In `collectIntrinsicTopK`'s small-M path (M < SortScanThreshold), replace the
flat map-then-sort approach ("intrinsic-topk-sort") with a block-aware KLL path
("intrinsic-topk-kll") that groups matching refs by BlockIdx and orders blocks by
`BlockMeta.MaxStart` DESC before collecting per-row timestamps.

**What changed:**
- M refs are grouped into `blockRefs[BlockIdx]` and unique block indices collected into
  `blockOrder []int`.
- `blockOrder` is sorted by `r.BlockMeta(bi).MaxStart` DESC (largest MaxStart first — newest
  block first). `MaxStart` is the KLL-sketch upper bound on `span:start` within a block.
- For each block (newest first), each ref's timestamp is looked up in the packed-key →
  timestamp map (same O(N) map build as before).
- (ref,ts) pairs are collected in block-descending order, then sorted globally by timestamp
  DESC (or ASC for forward) and top-K selected.
- `ExecutionPath` is set to `"intrinsic-topk-kll"` to distinguish from the old sort path.

**Why block ordering by MaxStart:**
Block-level ordering by MaxStart biases collection toward newer blocks first. For the common
case where matching refs are clustered in recent blocks, this provides a natural pre-sort
benefit before the final O(M log M) sort. Future extensions can use early termination here
(once the heap is full and the next block's MaxStart is below the heap min, all remaining
blocks are pruned) without correctness risk — the current implementation always sorts all M
pairs to ensure globally correct top-K.

**Correctness:**
The final sort over all M (ref,ts) pairs preserves the global ordering invariant. Block-level
MaxStart ordering is an optimization hint, not a correctness dependency.

**ExecutionPath telemetry:**
The old path "intrinsic-topk-sort" is retired. The new path "intrinsic-topk-kll" replaces it
for M < SortScanThreshold. The scan path "intrinsic-topk-scan" (M >= SortScanThreshold) is
unchanged. `SPEC-STREAM-9` back-ref and SPECS.md §4.4 and §6.1 updated accordingly.

Back-ref: `internal/modules/executor/stream.go:collectIntrinsicTopK`,
          `internal/modules/executor/stream_perf_test.go:TestCollect_IntrinsicTopK_KLLPath`

**Correction to NOTE-043 bullet list:** NOTE-043's `ExecutionPath` constant list
(`intrinsic-topk-sort`) is retired by this note; the correct value is `intrinsic-topk-kll`
for all non-scan small-M paths. The NOTE-043 bullet `"intrinsic-topk-sort" (Case B map path)`
should be read as `"intrinsic-topk-kll" (Case B KLL path, M < SortScanThreshold)`.

---

## NOTE-045: File-Level CMS Reject — Merged Count-Min Sketch for Equality Pruning
*Added: 2026-03-20*

`fileLevelCMSReject` in `plan_blocks.go` uses the `FileSketchSummary` (a merged
Count-Min Sketch across all blocks in the file) to reject a file entirely when a queried
equality value is definitely absent from every block.

**Why CMS, not Fuse8, at file level?**
- Block-level per-block Fuse8 filters are already wired into `PlanWithOptions` for per-block pruning.
- File-level `FileBloom` (NOTE-045) uses Fuse8 but only covers `resource.service.name`.
- The merged CMS covers all **string-valued** equality predicates for columns tracked by the sketch index.
- CMS `Estimate(v)==0` is a zero-false-negative guarantee: if the merged CMS says 0, the value
  never appeared in any block in the file.

**Why NOT a file-level Fuse8 for all columns?**
- Fuse8 filters are not mergeable across blocks without reprocessing the original key set.
- CMS is mergeable (element-wise sum, saturating). One merge pass produces the file-level summary.
- For file-level pruning, CMS absence detection (Estimate==0) is sufficient — false positives
  only mean we don't prune the file, which is conservative and safe.

**Integration**:
- `Reader.FileSketchSummary()` computes lazily (sync.Once) from the per-block sketch index.
- `Reader.FileSketchSummaryRaw()` serializes the summary for external caching (keyed by
  file path + size), allowing callers to avoid re-reading the file on subsequent queries.
- `cmsRejectByNodes` is the inner logic, accepting `*FileSketchSummary` directly for testability.
- Only string-valued equality predicates are checked (Values non-empty, no Min/Max/Pattern).
  Non-string types (bytes, numerics, durations) are skipped — the sketch uses raw wire-encoding
  for those and decoding them requires column-type context not available here.
- AND semantics: reject if ANY leaf rejects. OR semantics: reject only if ALL children reject.

**Limitations**:
- CMS has false positives: `Estimate(v) > 0` does NOT mean v is present.
- For small files or low-cardinality columns, false positives are common and pruning is unreliable.
- Only string-typed predicate values are evaluated. Bytes, numeric, and duration predicates pass
  through conservatively — extending to those types would require replicating the wire-encoding
  logic from `encodeValue` in predicates.go.
- This is an optimization only; correctness is maintained by the block-level scan that follows.

Back-ref: `internal/modules/executor/plan_blocks.go:fileLevelCMSReject`,
          `internal/modules/executor/plan_blocks.go:cmsRejectByNodes`,
          `internal/modules/blockio/reader/reader.go:FileSketchSummaryRaw`

## NOTE-046: Zero-Block-Read Fast Path for ExecuteTraceMetrics
**Date:** 2026-03-20

`ExecuteTraceMetrics` previously called `r.ReadBlocks()` unconditionally — reading full block
bytes even for queries like `{ } | count_over_time()` where only `span:start` is needed.

**Fast path condition:** all `wantColumns` (span:start + aggregate field + group-by + filter columns)
must be in `traceIntrinsicColumns`. When true, the query is answered entirely from the intrinsic
section — which is read once at `NewReaderFromProvider` time and cached — with zero full block reads.

**Eligible queries include:** `count_over_time()`, `rate()`, `histogram_over_time(span.duration)`,
any aggregate grouped by `resource.service.name` or `span:status`, `span:kind`, etc.

**Not eligible:** any query referencing non-intrinsic columns (e.g. `sum(span.latency_ms)`,
`count by (span.http.status_code)`) — these fall through to the existing block-scan path.

**Implementation:** `executeTraceMetricsIntrinsic` in `metrics_trace_intrinsic.go`.
- No predicates (`{ }`) with group-by or aggregate field: builds `keyToBucket` (packKey → bucketIdx) by iterating `span:start` flat column. For count/rate with no group-by, span:start is streamed inline without allocating `keyToBucket`.
- Intrinsic predicates: `BlockRefsFromIntrinsicTOC(r, program, 0)` filters refs before building `keyToBucket`.
- count/rate, no group-by: streams `span:start` inline without `keyToBucket` (zero intermediate maps).
- Group-by: `buildGroupKeyMap` iterates dict/flat column entries directly to build one composite key map.
- Aggregate field + group-by: `buildAggValsMap` (one map) combined with `groupKeyMap` in final pass.
- Aggregate field, no group-by: `streamAggColumnNoGroupBy` iterates the aggregate column directly.

**Back-refs:** `metrics_trace_intrinsic.go`, `metrics_trace.go:executeTraceMetricsIntrinsic` call site.

---

## NOTE-047: Unified Field Population in collectIntrinsicPlain
**Date:** 2026-03-23
*Updated: 2026-03-29*

Case A (pure intrinsic + no sort) originally dispatched on `hasRangePredicate(program)` to
choose between two sub-paths:

- **Range path:** `lookupIntrinsicFields` populated `MatchedRow.IntrinsicFields` directly from
  cached intrinsic blobs (zero additional I/Os). `MatchedRow.Block` was nil.
- **Equality path:** `forEachBlockInGroups` fetched only the matching blocks; field population
  was deferred until block decode. `MatchedRow.IntrinsicFields` was nil.

**Current state (2026-03-29):** The two-sub-path dispatch has been unified.
`collectIntrinsicPlain` now always uses `lookupIntrinsicFields` for field population regardless
of predicate type. The `hasRangePredicate` helper and the equality-specific block-fetch path
have been removed. All matched rows have `MatchedRow.IntrinsicFields` populated at collection
time; `MatchedRow.Block` is nil for the intrinsic-only case.

**Rationale:** The equality block-fetch path added I/O complexity without a measurable latency
advantage in production workloads. `lookupIntrinsicFields` reads from already-cached intrinsic
blobs for both range and equality predicates, keeping the code path uniform and eliminating the
`hasRangePredicate` branch.

**Back-refs:** `stream.go:collectIntrinsicPlain`,
`stream.go:collectFromIntrinsicRefs` (call site).

---

## NOTE-048: Parallel Phase 1 Fetch in forEachBlockInGroups
**Date:** 2026-03-24

`forEachBlockInGroups` is split into two phases:

**Phase 1 (parallel I/O):** All coalesced groups are fetched concurrently via goroutines.
`r.ReadGroup` only calls `r.provider.ReadAt` which is stateless and safe for concurrent use.
Parallelism overlaps S3/disk latency across groups — at 36ms per request, reading 3 sequential
groups takes 108ms; in parallel it takes 36ms.

**Phase 2 (sequential decode):** `Reader.ParseBlockFromBytes` is not safe for concurrent use
on the same `*Reader` instance; Phase 2 remains single-goroutine.

**Early-stop trade-off:** All groups are pre-fetched in Phase 1 before Phase 2 begins. If
`fn` returns early (e.g. limit reached), the remaining group data is discarded — the S3 reads
have already been issued. In practice `forEachBlockInGroups` is only called for Case A
(pure intrinsic, equality predicates) where refs cluster in 1–3 blocks, so at most 1–2
groups are ever pre-fetched. The over-fetch cost is negligible.

**Buffer lifecycle:** Each group's `map[int][]byte` is released entry-by-entry via
`delete(fetched[i].data, blockIdx)` immediately after `ParseBlockFromBytes` returns,
letting the GC reclaim block bytes before the next group is processed. The local `bwb`
variable holds `bwb.RawBytes` alive through the `fn` call (NOTE-001 lazy decode safety).

**Back-refs:** `stream.go:forEachBlockInGroups`

---

## NOTE-049: scanBlocks Intern Map Pool and Clone Elimination
*Added: 2026-03-25*

**Problem:** `scanBlocks` called `r.ParseBlockFromBytes` twice per matching block (first pass
for predicate evaluation + second pass to decode result columns). Each call allocated a fresh
`make(map[string]string)` intern map inside `ParseBlockFromBytes`. With hundreds of blocks
per query, this produced hundreds of map allocations and corresponding GC pressure.

Additionally, `rows := slices.Clone(rowSet.ToSlice())` allocated a redundant copy of the
matched-row index slice. The clone comment stated it was needed because "ToSlice returns the
backing slice and must not be modified", but `rowSet` is never accessed again after line 382
in `scanBlocks` — no `Contains` calls occur on the `scanBlocks` code path after `ToSlice()`.

**Fix 1 — Intern map pool:** `scanBlocks` now acquires a pooled `map[string]string` from
`modules_reader.AcquireInternMap()` at the start of each block iteration and passes it to
`r.ParseBlockFromBytesWithIntern()` for both parse passes. The map is released via
`modules_reader.ReleaseInternMap()` after `streamSortedRows` completes — this is the correct
release point because lazy columns registered during the first parse may call `decodeNow()`
during row emission, and they reference the intern map directly. Releasing before
`streamSortedRows` would corrupt lazy string decodes.

**Fix 2 — Clone elimination:** Changed `rows := slices.Clone(rowSet.ToSlice())` to
`rows := rowSet.ToSlice()`. The clone was conservative but unnecessary on this code path.
`streamSortedRows` sorts `rows` in-place, but since `rowSet` is not used after `ToSlice()`,
sorting the backing slice directly is safe.

**Lifetime contract for pooled intern maps:**
1. Acquire: at block-iteration start in `scanBlocks`.
2. Keep alive: through both `ParseBlockFromBytesWithIntern` calls AND all of `streamSortedRows`
   (where lazy `decodeNow()` calls may occur).
3. Release: immediately after `streamSortedRows` returns (or on any early-exit error path).

**Back-refs:** `stream.go:scanBlocks`,
              `internal/modules/blockio/reader/column.go:AcquireInternMap`,
              `internal/modules/blockio/reader/reader.go:ParseBlockFromBytesWithIntern`,
              `internal/modules/blockio/reader/NOTES.md:NOTE-006`

## NOTE-050: Intrinsic Columns — Stored Exclusively in Intrinsic TOC Section
*Added: 2026-03-25*

*Addendum (2026-03-25): Original entry claimed dual-storage (block columns AND intrinsic
section). That was incorrect. Intrinsic columns are written ONLY to the intrinsic TOC
section; `addPresent` calls for these columns were removed. This addendum corrects the record.*

**Decision:** Intrinsic columns (trace:id, span:id, span:parent_id, span:name, span:kind,
span:start, span:duration, span:status, span:status_message, resource.service.name) are
stored exclusively in the intrinsic TOC section. They are NOT written to block column
payloads. `ParseBlockFromBytes` returns nil columns for these names; this is handled by
`nilIntrinsicScan` which produces FullScan results for AND intersection.

**Rationale:**
- The intrinsic section enables fast pre-filtering (bloom, min/max) and O(1) identity
  lookup via `lookupIntrinsicFields` without full block decodes.
- Removing dual-storage eliminates redundant data in block payloads.

**Consequences for executor:**
- `searchMetaCols` no longer lists trace-signal intrinsic column names because those
  columns are served via the intrinsic section path (`lookupIntrinsicFields`) rather
  than through the `wantColumns` second-pass decode. Log-signal identity columns remain
  in `searchMetaCols` because log blocks use different identity column names.
- `secondPassCols` injects all `traceIntrinsicColumns` (trace:id, span:id, span:start,
  etc.) so that `lookupIntrinsicFields` populates `IntrinsicFields` correctly. The
  names are passed to `ParseBlockFromBytes` as `wantColumns`; they return nil columns
  since the data is absent from block payloads. Identity values come from `lookupIntrinsicFields`.
- `SpanMatchFromRow` accepts a `*Reader` parameter and calls `lookupIntrinsicFields`
  to populate identity fields from the intrinsic section for the block-scan path.
- `RangeNode.MinInclusive`/`MaxInclusive` fields distinguish `>` vs `>=` and `<` vs `<=`
  for the flat-column intrinsic scan, ensuring correctness at exact boundaries.

**Back-ref:** `internal/modules/executor/predicates.go:searchMetaCols`,
`internal/modules/executor/stream_structural.go:collectBlockStructuralSpanRecs`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/executor.go:SpanMatchFromRow`,
`internal/vm/bytecode.go:RangeNode`,
`internal/modules/blockio/writer/writer_block.go:newBlockBuilder`

## NOTE-051: Mixed-OR Predicates — False-Negative Limitation for OR(intrinsic, non-intrinsic)
*Added: 2026-03-25*

**Decision:** The block-scan post-filter (`filterRowSetByIntrinsicNodes` /
`rowSatisfiesIntrinsicNodesOR`) uses a conservative skip for non-intrinsic leaves in an
OR context: when a leaf references a non-intrinsic column it returns `false` (no match)
rather than `true` (pass-through). This can produce false negatives for OR queries such as
`{span:name="foo" || span.http.url="bar"}` where a row matches only the non-intrinsic branch.

**Rationale:** Returning `true` for non-intrinsic leaves in OR would cause false positives
because `nilIntrinsicScan` returns `FullScan` for nil intrinsic columns (columns absent from
block payloads). In an AND context `FullScan` is an identity element (safe); in an OR context
it would admit every row regardless of the non-intrinsic predicate value, producing incorrect
results. Fixing this correctly requires a two-pass approach that evaluates non-intrinsic
columns before composing the OR — a more invasive architectural change that is deferred.

**Consequences:**
- Queries combining an intrinsic predicate with a non-intrinsic predicate under OR may drop
  rows that satisfy only the non-intrinsic branch during block-level scanning.
- Impact is low: OR between intrinsic columns and user-attribute columns is uncommon in
  practice. Queries with `Limit > 0` are unaffected because they use the intrinsic fast path
  which does not go through `filterRowSetByIntrinsicNodes`.
- Full-table scans (no limit, no intrinsic index pruning) are the only affected path.

**Back-ref:** `internal/modules/executor/predicates.go:rowSatisfiesIntrinsicNodesOR`

## NOTE-052: Dual Storage Coexistence — Block Columns and Intrinsic Section
*Added: 2026-03-26*

**Decision:** After the rollback of PR #172 (see writer NOTE-002), dual storage is in effect:
intrinsic columns are present in BOTH block column payloads AND the intrinsic TOC section.
Both fast paths and block-scan paths remain valid and coexist.

**Two access patterns, two storage layers:**

1. **Block column payloads** (via `ParseBlockFromBytes` + `GetColumn`) — used by the block-scan
   path for predicate evaluation and result materialization. O(1) row access by (blockIdx, rowIdx)
   once the block is in memory.

2. **Intrinsic TOC section** (via `BlockRefsFromIntrinsicTOC`, `ScanFlatColumnRefsFiltered`,
   `lookupIntrinsicFields`) — used by the intrinsic fast paths (Cases A–D from
   `collectFromIntrinsicRefs`) for zero-block-read query execution, TOC-level bloom pruning,
   and metrics aggregation over intrinsic columns.

**Executor workarounds remain but are now conservative no-ops for block-scan:** The
`nilIntrinsicScan` (SPEC-STREAM-10.1), `userAttrProgram` (SPEC-STREAM-10.2), and
`filterRowSetByIntrinsicNodes` (SPEC-STREAM-10.3) mechanisms introduced for the
exclusive-intrinsic model still compile and run. With dual storage, `Block.GetColumn` returns
a non-nil column for all intrinsic names, so `nilIntrinsicScan` is never triggered during
block-scan. These mechanisms add no overhead in the dual-storage regime and provide
defence-in-depth should a future format version again omit intrinsic columns from block payloads.

**Back-ref:** `internal/modules/executor/stream.go:collectFromIntrinsicRefs`,
`internal/modules/executor/column_provider.go:nilIntrinsicScan`,
`internal/modules/executor/predicates.go:userAttrProgram`,
`internal/modules/blockio/writer/NOTES.md:NOTE-002`

# logql — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/logql` package.

---

## NOTE-001: Why a New Module Instead of Extending logqlparser
*Added: 2026-03-03*

**Decision:** The LogQL execution engine lives in `internal/modules/logql/`, not in
`internal/logqlparser/`.

**Rationale:** `internal/logqlparser/` is a parser — it converts LogQL strings into AST
nodes and compiles them to `vm.Program` values. Adding execution logic (pipeline stage
evaluation, metric aggregation, block scanning) would conflate two distinct concerns and
break the separation that exists between `internal/traceqlparser/` (parse only) and
`internal/modules/executor/` (execute only).

The new module mirrors the established TraceQL architecture:
- TraceQL: `traceqlparser` (parse + compile) + `executor` (execute)
- LogQL: `logqlparser` (parse + compile) + `modules/logql` (execute)

A dedicated spec-driven module also enforces documentation discipline for a non-trivial
new subsystem with its own invariants (label scoping, pipeline short-circuit, unwrap).

*Addendum (2026-03-17):* This module was merged into `internal/modules/executor/`.
See internal/modules/executor/NOTES.md NOTE-012 Addendum for the migration details.

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

**Decision:** Metric aggregation in `metrics.go` reuses `vm.QuerySpec` and `vm.AggBucket`
for time bucketing and aggregation state. It does NOT call `executor.ExecuteMetrics`.

*Addendum (2026-03-17):* `metrics.go` was merged into `executor/metrics_log.go:ExecuteLogMetrics`.

**Rationale:** `vm.QuerySpec` (time bucketing: step, start, end) and `vm.AggBucket`
(Sum, Count, Min, Max) are generic — they were designed for TraceQL metrics but are not
TraceQL-specific. Reusing them avoids creating a parallel bucketing system.

The engine does NOT delegate to `executor.ExecuteMetrics` because that function is tightly
coupled to TraceQL span match fields (`SpanMatch.SpanID`, `SpanMatch.TraceID`) and does
not provide a hook for per-row pipeline execution. LogQL metrics needs to interpose the
pipeline between block scan and bucket accumulation. Rather than adding a hook to the
executor (which would pollute a focused module), the logql engine replicates the small
bucketing loop with the pipeline step inserted.

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

`text/template` execution (as opposed to parsing) is safe for concurrent use with
distinct `*bytes.Buffer` instances. The pipeline stage creates a new buffer per call.

---

## NOTE-006: UnwrapValueKey as Special Label
*Added: 2026-03-03*

**Decision:** `UnwrapStage` stores the extracted numeric value in the labels map under
the key `"__unwrap_value__"` (constant `UnwrapValueKey`).

**Rationale:** Passing a separate numeric value alongside the labels map would require
changing the `PipelineStageFunc` signature, breaking all existing stage implementations.
Using a reserved label key is a pragmatic convention that preserves the uniform stage
interface. The double-underscore prefix follows the Loki convention for internal/synthetic
labels and avoids collision with user-defined labels.

Metric aggregation reads `UnwrapValueKey` after pipeline execution to obtain the unwrapped
value. If the key is absent (no unwrap stage), numeric aggregations (`sum_over_time`, etc.)
are not applicable and should return an error at compile time.

---

## NOTE-007: Silent Failure Policy for Parse Errors in Stages
*Added: 2026-03-03*

**Decision:** `JSONStage` and `LogfmtStage` do NOT drop rows when the log body is not
valid JSON/logfmt. They keep the row with unmodified labels.

**Rationale:** This matches Loki's behavior. In a mixed-format log stream, some records
may not conform to the expected format. Silently keeping them (without extracted labels)
allows downstream label filters to handle the case explicitly (e.g., `| level != ""`
would filter out non-parsed records). Dropping them unconditionally would silently hide
data.

Contrast with `LabelFilterStage` and `UnwrapStage`: these explicitly test or extract a
specific value and a missing or non-numeric value means the filter/unwrap condition is
not satisfiable — dropping is the correct semantic.

---

## NOTE-008: Engine Does Not Modify the Reader
*Added: 2026-03-03*

**Decision:** `Engine` takes `*reader.Reader` as a parameter but never calls writer-side
methods. It is a pure read path.

**Rationale:** Log queries are read-only operations. The engine orchestrates block
selection via `queryplanner.Planner`, block parsing via `reader.ParseBlockFromBytes`, and
predicate evaluation via `vm.Program.ColumnPredicate`. No write operations are needed or
performed. Documenting this explicitly prevents future additions from accidentally
introducing write-path calls in the query hot path.

---

## NOTE-009: Mirrors TraceQL Pattern — logqlparser Produces Everything Needed to Execute
*Added: 2026-03-03*

**Decision:** `logqlparser.CompilePipeline` produces `*Pipeline` (the executable pipeline)
alongside `*vm.Program`. The engine receives both and does not re-parse the query.

**Rationale:** Keeping the engine thin (it only drives execution) and the parser/compiler
rich (it knows how to compile all AST node types to executable closures) mirrors the
TraceQL architecture. The engine is not coupled to AST types — it only knows about
`vm.Program` (block-level predicates) and `*Pipeline` (row-level stages). This makes the
engine easier to test independently of the parser.

---

## NOTE-010: Free Functions Instead of Engine Struct
*Added: 2026-03-03*

**Decision:** `StreamLogs` and `ExecuteLogMetrics` are package-level free functions, not
methods on an `Engine` struct.

**Rationale:** The engine carries no state — it does not cache readers, maintain pools, or
track query history. An `Engine` struct would be a zero-value wrapper adding ceremony
without benefit. Free functions are simpler to call, test, and reason about. This matches
`executor.Collect()` being a stateless free function with no constructor ceremony; the engine struct
is a vestigial convention from earlier designs.

---

## NOTE-012: Reuse executor.NewColumnProvider, Not a Reimplemented Interface
*Added: 2026-03-03*

**Decision:** `ExecuteLogMetrics` and `StreamLogs` call `executor.NewColumnProvider(block)` to satisfy
`vm.ColumnDataProvider`, rather than implementing the interface again inside the logql package.

**Rationale:** `vm.ColumnDataProvider` is a 28-method interface. Reimplementing it in the logql package
would create a maintenance burden — any future column type additions would require updating both the
executor and logql implementations. `executor.NewColumnProvider` is the canonical, well-tested
implementation of this interface.

There is no circular dependency: `executor` does not import `modules/logql`. The import graph is:
`modules/logql` → `executor` → `vm`, `blockio/reader`, `blockio/shared`.

Back-ref: `internal/modules/logql/metrics.go:ExecuteLogMetrics`, `internal/modules/logql/engine.go:StreamLogs`

---

## NOTE-011: Label Column Scoping — resource.* and log.* Prefixes Stripped
*Added: 2026-03-03*

**Decision:** `logReadLabels` strips the `resource.` and `log.` column name prefixes when
building the per-row labels map. The label `resource.service.name` becomes `service.name`.

**Rationale:** In LogQL, stream labels do NOT carry the `resource.` prefix. When a user
writes `{service.name="svc"}`, the compiled predicate scans `resource.service.name`. The
pipeline's label map should use the same un-prefixed names so that `| label_format` and
`| keep service.name` refer to `service.name`, not `resource.service.name`. This matches
Loki semantics where resource-level attributes are exposed as first-class labels.

---

## NOTE-013: Structured Metadata Predicate Pushdown — Position-Based Eligibility
*Added: 2026-03-04*

**Decision:** `CompileAll` pushes down `StageLabelFilter` stages into the `vm.Program` when
they appear before any parser stage in the pipeline, using position-based eligibility.

**Rationale:** Structured metadata fields (`detected_level`, `service_name`, etc.) are stored
as `log.*` columns in blockpack. Without pushdown, the `LokiConverter` bridge strips all
pipeline stages (including label filters on these columns), forcing blockpack to decode every
matching row — then Loki's Pipeline discards 70-90% post-scan.

Position-based detection was chosen over alternatives:
- **Column-existence check** (querying the reader at compile time) would couple the compiler
  to the reader, breaking the clean parse→compile→execute separation.
- **Whitelist approach** (hardcoded field names) would be brittle and require maintenance.
- **Position-based** is safe because before any parser stage, a label filter can only reference
  stream labels (`resource.*`) or structured metadata (`log.*`) — never parsed fields.

The column name mapping follows the same prefix convention as label matchers: label filter
`detected_level="error"` maps to column `log.detected_level`, just as stream matcher
`cluster="dev"` maps to column `resource.cluster`.

Back-ref: `internal/logqlparser/compile.go:CompileAll`, `internal/logqlparser/compile.go:identifyPushdownStages`

---

## NOTE-014: LokiConverter Pipeline Label Filter Pushdown
*Added: 2026-03-05*

**Decision:** `benchmark/lokibench/converter.go:buildPushdownQuery` (which replaced
`stripPipelineStages`) re-emits simple `StageLabelFilter` stages as native StreamLogQL
predicates instead of stripping all pipeline stages. This allows blockpack's executor to
evaluate `| detected_level="error"`, `| level="error"`, `| instance_id="X"`, and
`| component="api"` via column scans rather than deferring to the Loki pipeline.

**Rationale:** `stripPipelineStages` dropped ALL pipeline stages to avoid triggering
StreamLogQL's native pipeline path (which previously stripped `log.*` columns from
`SpanMatch.Fields`). This caused 100% of pipeline label filters to be evaluated row-by-row
by the Loki pipeline AFTER blockpack returned all matching rows. For filters on
high-selectivity columns (`instance_id`: 200 unique values, `detected_level`: 4 values),
blockpack was fetching 10-60x more rows than necessary.

`buildPushdownQuery` reconstructs the query with eligible label filter stages preserved.
Parser stages (logfmt, json) are dropped because native `log.*` columns exist at write time
without parsing. Mutating stages (label_format, line_format, drop, keep) stop reconstruction.

**Negation exclusion:** Negation filters (`!=`, `!~`) are intentionally excluded. A block
that lacks the column entirely satisfies a negation semantically, but StreamLogQL's
block-level bloom pruning treats column absence as "column not present" and might prune
the block — producing false negatives. Negations remain Loki-pipeline-only.

**OR filter support:** OR-chained label filters (`| detected_level="error" or detected_level="warn"`)
are serialized with `or` between fragments, which is valid blockpack LogQL syntax.

Back-ref: `benchmark/lokibench/converter.go:buildPushdownQuery`, `benchmark/lokibench/converter.go:labelFilterFragment`

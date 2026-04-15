# logqlparser — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/logqlparser` package.

---

## 1. Direct Compilation to ColumnPredicate Closures
*Added: 2026-03-03*

**Decision:** LogQL compiles directly to `vm.ColumnPredicate` closures rather than converting
to TraceQL AST and reusing `vm.CompileTraceQLFilter`.

**Rationale:** LogQL line filters (`|= "text"`) require substring matching (`ScanContains`),
but the TraceQL compiler maps equality operators to `ScanEqual`. Converting LogQL `|= "text"`
to a TraceQL `OpEq` on `log:body` would produce exact-match semantics instead of substring
semantics. Rather than adding special-case logic to the TraceQL compiler, we compile directly
to the correct scan methods.

The ColumnPredicate closures are lightweight (one closure per matcher/filter) and compose
naturally via Intersect for AND semantics.

---

## 2. Not-Contains via Complement
*Added: 2026-03-03*

**Decision:** `!= "text"` (FilterNotContains) is implemented as
`Complement(ScanContains(col, text))` rather than adding a `ScanNotContains` method to
`ColumnDataProvider`.

**Rationale:** Adding a method to the `ColumnDataProvider` interface would require updating
every implementation (executor column_provider, any mocks). The Complement approach is
semantically correct and reuses existing infrastructure. Performance is acceptable because
Complement operates on RowSet bitmaps, not raw data.

---

## 3. Label Matchers Map to resource.{name} Columns
*Added: 2026-03-03*

**Decision:** LogQL label matchers `{key="val"}` compile to predicates on `resource.{key}`
columns in blockpack.

**Rationale:** Loki stores stream labels as OpenTelemetry resource attributes. The blockpack
writer stores these in `resource.*` columns. This mapping is consistent with how the
lokibench write path (`store_blockpack.go`) encodes labels.

---

## 4. Parser Stops at Pipeline Stages
*Added: 2026-03-03*

**Decision:** The parser stops at pipeline stages (`| identifier`) without returning an
error, treating the rest of the query as opaque.

**Rationale:** The lokibench adapter handles Loki-specific pipeline stages (| json, | logfmt,
| label_format, etc.) using Loki's own `Pipeline.ForStream()`. The parser only needs to
extract the label matchers and line filters that blockpack can evaluate natively as column
predicates. Attempting to parse pipeline stages would be wasted effort and would couple the
parser to Loki internals.

---

## 5. extractPredicates for Block Pruning
*Added: 2026-03-03*

**Decision:** `Compile` populates `vm.QueryPredicates` from label matchers for bloom filter
and range index pruning. Line filters do not contribute to block pruning.

**Rationale:** Block-level pruning operates on per-block metadata (bloom filters, min/max
stats for dedicated columns). Label matchers target resource columns which have dedicated
column storage and bloom entries. Line filters target `log:body` which is stored as a
variable-length string column without dedicated bloom or range metadata — there's nothing
to prune against at the block level.

Negation matchers (`!=`, `!~`) are NOT added to `AttributesAccessed`, `DedicatedColumns`,
or `DedicatedColumnsRegex`. A missing column matches negation semantics (`!=`, `!~`), so
bloom-pruning on column absence would produce false negatives by dropping blocks that
legitimately match. Selectors with only negation matchers produce nil predicates (no block
pruning possible).

---

## 6. CompileAll — Predicate Pushdown for Structured Metadata
*Added: 2026-03-04*

**Decision:** `CompileAll` pushes `StageLabelFilter` stages that appear before any parser
into the `vm.Program` as `ColumnPredicate` closures on `"log." + labelName`, and into
`QueryPredicates` for block-level bloom/range pruning.

**Rationale:** Structured metadata fields (`detected_level`, `service_name`, etc.) are stored
as `log.*` columns. Without pushdown, the bridge layer strips all pipeline stages, and the
native engine evaluates label filters post-scan in the pipeline. This wastes work: all
matching rows are decoded, then the pipeline discards 70-90%.

Position-based eligibility (before any parser) ensures safety: before `| json` or `| logfmt`,
a label can only reference stream labels or structured metadata, never parsed fields.

**Two-phase evaluation:** `CompileAll` separates predicates into two evaluation phases
within the combined `ColumnPredicate`:
1. Phase 1 (cheap): label matchers (`resource.*`) + pushed-down structured metadata (`log.*`)
   run as bulk column scans. These columns are small, often RLE/dictionary-encoded.
2. Phase 2 (expensive): line filters (`|=`, `|~`, etc.) evaluate per-row using
   `GetValue("log:body", rowIdx)` only on rows surviving phase 1. This avoids a full-column
   scan over the large `log:body` column when phase 1 eliminates most rows.

When no cheap predicates exist, line filters fall back to bulk scans (no regression).

**Scope restrictions:**
- OR filters are not pushed down (union semantics complicate block pruning)
- Numeric comparison ops (`>`, `<`, `>=`, `<=`) are not pushed down (type-aware column scanning
  is not consistently supported for structured metadata columns)
- Pushed-down stages are removed from the pipeline to avoid double evaluation

---

## 7. Pre-Compile Regex at Closure Creation
*Added: 2026-03-04*

**Decision:** `compileMatcherPredicate` and `compileLineFilterPredicate` compile the regex
pattern exactly once — at `Compile()` time — and capture the `*regexp.Regexp` in the
returned `ColumnPredicate` closure. `vm.RegexPrefixes` is called at the same time to
extract literal prefix candidates for the scan-layer pre-filter.

**Rationale:** The previous implementation delegated regex compilation to
`provider.ScanRegex(col, pattern)`, which called `regexp.Compile` inside
`StreamScanRegex` — once per block per query. With 12–15 blocks per query, each
query incurred 12–15 redundant compilations of the same pattern. Hoisting compilation
to closure-creation time reduces this to exactly one compilation per query.

`ScanRegexFast(col, re, prefixes)` / `ScanRegexNotMatchFast` accept the pre-compiled
`*regexp.Regexp` directly. `ScanRegex` / `ScanRegexNotMatch` (string-based) remain on
the interface for callers outside the logqlparser/traceql_compiler paths.

Back-ref: `internal/logqlparser/compile.go:compileMatcherPredicate`,
`internal/logqlparser/compile.go:compileLineFilterPredicate`

---

## 8. Predicate Pushdown for Pipeline StageLabelFilter (body-parsed fields)
*Added: 2026-03-04*

**Decision:** `extractPredicates` walks `sel.Pipeline` for `StageLabelFilter` stages
and pushes positive ops (`OpEqual`, `OpRegex`) into `UnscopedColumnNames` (bloom-OR
across `log.{name}` and `resource.{name}`), and numeric ops (`OpGT/GTE/LT/LTE`) into
`DedicatedRanges` on `log.{name}` columns. Only a contiguous leading run of
`StageLabelFilter` stages is considered — any other stage type acts as a barrier.

**Rationale:** Log body fields parsed at ingest are stored as `log.{key}` range string
columns. The existing block-pruning machinery already supports
min/max range index lookups and bloom filter checks for these columns. By generating
`QueryPredicates` entries from pipeline label filters, the executor gains block-level
skip for structured-body queries (`| level="error"`, `| component="api"`) at zero
additional executor cost — it reuses the existing predicate evaluation path.

**Why UnscopedColumnNames (not DedicatedColumns["log.*"]):** A pipeline label filter
binds to whichever scope provides the label at query time — it may match `resource.level`
or `log.level`. Pruning only on `log.*` would drop blocks where the value lives in
`resource.*` (false negatives). Bloom-OR across both scopes is safe.

**Why negations are excluded:** A block that contains no `log.level` column at all
satisfies `level!="error"` (the column is absent, so no rows have `level="error"`,
and none need to be excluded). Bloom-pruning on column absence would misidentify such
blocks as "definitely no match" and drop them — producing false negatives.
This is the same reasoning applied to stream selector negations (NOTE-5).

**Numeric range ops use DedicatedRanges (not AttributeRanges):** Range ops are written
to `preds.DedicatedRanges`, which is the map read by `buildPredicates` in the logql
executor path. `AttributeRanges` is for TraceQL span attributes and is not consulted
for log queries.

Back-ref: `internal/logqlparser/compile.go:extractPredicates`

---

## 9. parserFreeStages and ProcessSkipParsers for body-auto-parsed blocks
*Added: 2026-03-04*

**Decision:** `Pipeline` carries a `parserFreeStages []PipelineStageFunc` field built
at compile time by excluding `StageJSON` and `StageLogfmt` from the stage list.
`ProcessSkipParsers` runs `parserFreeStages` instead of `Stages`, skipping body
re-parsing when body fields are already stored as `log.*` columns.

**Rationale:** When the blockpack writer's `parseLogBody` detects a JSON or logfmt body
at ingest time, it stores extracted fields as `log.{key}` `ColumnTypeRangeString` sparse
columns. At query time, `logReadLabels` then populates those fields into the labels map
before the pipeline runs. Running `| logfmt` or `| json` afterwards is a pure no-op
that wastes CPU decoding the body string redundantly — measured at ~15 allocs/row
before this fix (logfmt.NewDecoder + bytes.NewBufferString per row).

`compilePipelineSkipping` (called by both `CompileAll` and `CompilePipeline`) builds
both `Stages` (full pipeline) and `parserFreeStages` (same minus JSON/logfmt). The
executor calls `blockHasBodyParsed(block)` once per block, then routes per-row
processing through `ProcessSkipParsers` or `Process` accordingly.

**CompilePipeline now delegates to compilePipelineSkipping:** Before this change,
`CompilePipeline` returned `Pipeline{Stages: funcs}` without setting `parserFreeStages`.
If `ProcessSkipParsers` was called on such a pipeline, it would return `true` (keep all
rows) because `len(parserFreeStages) == 0`, silently bypassing label filter stages.
`CompilePipeline` now calls `compilePipelineSkipping(stages, nil)` to ensure
`parserFreeStages` is always populated.

**Performance impact:** T7-T9 logfmt queries on 100 MB OTLP data: allocs reduced from
~18M to ~1.8M per query (10x). Remaining allocs (~6/row) are from `logReadLabels` map
creation and `LogEntry` construction — unavoidable with the current per-row labels model.
Queries where cluster/datacenter pruning fires (59 blocks vs 198) are at parity or
faster than Loki chunks.

Back-ref: `internal/logqlparser/pipeline.go:ProcessSkipParsers`
Back-ref: `internal/logqlparser/compile.go:compilePipelineSkipping`
Back-ref: `internal/modules/executor/stream_log.go:blockHasBodyParsed`

---

## 10. LabelSet Interface for Lazy Label Access
*Added: 2026-03-04*

**Decision:** `PipelineStageFunc` was changed from `map[string]string` to `LabelSet` to
enable lazy label reading in the executor hot path.

**Rationale:** Building a `map[string]string` from ~20 block columns per row allocates
~6 heap objects. When a query like `{env="prod"} | level="error"` filters 90% of rows,
90% of those allocations are wasted. The `LabelSet` interface allows `blockLabelSet`
(in the executor package) to defer column reads and map allocation until a label is
actually accessed or until `Materialize()` is called at the delivery boundary.

`mapLabelSet` (in this package) provides the fallback implementation for the
non-pre-parsed block path and for tests.

`LineFormatStage` calls `labels.Materialize()` to pass a `map[string]string` to
`text/template`. This is acceptable since `LineFormatStage` is a mutation stage, not
a filter — the primary allocation savings come from dropped rows at `LabelFilterStage`.

Back-ref: `internal/logqlparser/labelset.go:LabelSet`,
`internal/modules/executor/block_label_set.go:blockLabelSet`

---

## 11. RangeNode Compile — OR Composites and Negation Semantics
*Added: 2026-03-06*

**Decision:** `extractPredicates` and `addPushdownQueryPredicates` in `compile.go` emit
`vm.RangeNode` trees rather than flat maps.

**Stream matchers** (e.g. `{service.name="auth"}`) map to **leaf** `RangeNode` values on the
`resource.*` column — one node per positive matcher. Negations (`!=`, `!~`) produce no
`Nodes`; their column is added to `Columns` so `ProgramWantColumns` loads the data for the
row-level predicate closure.

**Pipeline filters** (e.g. `| detected_level="error"`) can match either `log.detected_level`
OR `resource.detected_level` depending on how the block was produced. Each positive op
becomes an `{IsOR:true, Children:[log.X leaf, resource.X leaf]}` composite. Negations are
skipped from `Nodes` (see NOTE-6 / NOTE-030 negation invariant) but both column variants
are added to `Columns`.

**Why OR composites for pipeline filters, not a single resource.* leaf:** A `| level="info"`
filter on logs can match `log.level` (Loki-style structured metadata) or `resource.level`
(OTLP-style resource attribute). Using a single scope would produce false negatives for
blocks written with the other scope. The OR composite keeps a block if EITHER scoped column
contains the value in its range index.

**Why negations are in `Columns` but not `Nodes`:** Block-level bloom and range-index
pruning can only KEEP blocks that definitely contain a value. A negation predicate like
`!= "prod"` means "rows where service.name is absent OR is not prod". A block where
service.name is absent PASSES the negation — if we pruned it because the bloom says
service.name is absent, we'd produce false negatives. So negations only go to `Columns`
to ensure the column is decoded for the row-level closure.

Back-ref: `internal/logqlparser/compile.go:extractPredicates`,
`internal/logqlparser/compile.go:addPushdownQueryPredicates`

---

## 12. HasLive — Distinguishing Live Labels from Undecoded Block Columns
*Added: 2026-03-09*

**Decision:** `LabelSet` gains a `HasLive(key string) bool` method. `LogfmtStage` and
`JSONStage` switch from `Has(k)` to `HasLive(k)` as the skip guard for "already stored"
keys.

**Problem:** `blockLabelSet.Has(k)` returns true for columns that are present in the
block but not yet decoded (rawEncoding != nil). LogfmtStage used `Has` to skip such
columns (interpreting them as "already stored at ingest"). But `Materialize()` also
skips undecoded columns (NOTE-SL-017 performance optimization). So when a query runs
`line_format "{{.caller}}: {{.msg}}"`, neither logfmt extraction nor Materialize provides
the values — the output is `": "`.

**Semantics of `HasLive`:**
- `mapLabelSet`: `HasLive` == `Has` (map lookup; all values are immediately accessible).
- `blockLabelSet`: `HasLive` returns true only if the key is in the overlay (not deleted)
  OR the backing column is `IsDecoded() && IsPresent(rowIdx)`. Undecoded columns
  (lazily registered with rawEncoding != nil) return false.

**Why the existing "stored columns win" tests (LQP-TEST-127, LQP-TEST-128) still pass:**
Those tests use `mapLabelSet` (via `NewMapLabelSet`). For `mapLabelSet`, `HasLive == Has`,
so pre-populated map keys have HasLive=true and the skip guard still fires. The invariant
"decoded/overlay labels win over pipeline extraction" is preserved; only undecoded block
columns are now treated as absent by the parser stages.

**`__error__` in `LogfmtStage`:**
Blockpack uses Loki's default non-strict logfmt mode: `__error__` is always set to `""`
regardless of whether the logfmt decoder reports an error. This matches Loki's behaviour
where partial-parse bodies (e.g. access logs with quoted strings mid-body) are kept
without an error label. `__error__` is set unconditionally and is not guarded by
`HasLive`. Strict mode (`"LogfmtParserErr"` on failure) is not implemented.

**`HideBodyParsedColumns` in LogfmtStage/JSONStage:**
Parser stages call `labels.HideBodyParsedColumns()` immediately before parsing the body.
This marks all ingest-time `log.*` columns as deleted in the `blockLabelSet` so that
`Get()` returns `""` for them unless the stage explicitly sets the value via `Set()`.
This ensures first-wins semantics from body re-parsing and prevents ingest-time last-wins
column values from shadowing re-parsed results. The call is placed inside the stage
function (not the executor loop) so that non-parser stages that execute before `| logfmt`
(e.g. `| label_format`) operate on the full label set unaffected.

---

## 13. Numeric pipeline filters skipped when no parser seen (NOTE-012)
*Added: 2026-03-19*

**Decision:** `identifyPushdownStages` now adds parseable numeric ops (`OpGT/GTE/LT/LTE`)
to `skip` when `!seenParser`, matching the behaviour of string ops.

**Rationale:** Previously numeric ops were always kept in the pipeline even when pushed
into a `ColumnPredicate` (both `skip` and `columnPred` were populated for string ops;
only `columnPred` was populated for numeric). This caused double evaluation:
1. `ScanGreaterThan`/`ScanLessThan` (column predicate) — correct per-row comparison.
2. `LabelFilterStage` (pipeline) — same comparison via `blockLabelSet.Get` + `strconv.ParseFloat`.

`ScanGreaterThan` already handles `ColumnTypeRangeString` columns via a dict-level
float pre-parse fast path (NOTE-022 in executor), so the pipeline stage was genuinely
redundant for the pre-parser case.

**When numeric ops are kept in pipeline:** when `seenParser=true` (a `| json` or
`| logfmt` stage precedes the numeric filter in the full query). In that path the
pipeline may run on blocks without body-parsed columns, where the parser stage extracts
values the column predicate cannot see.

**Scope note — numeric range predicates are log-scoped:** `compileSinglePushdownPredicate`
for numeric ops scans only `log.{name}`, not `resource.{name}` (matching the comment
at `compile.go:compileSinglePushdownPredicate` — "range predicates are log-scoped").
String ops union both scopes. This means `| latency_ms > 200` will not match an
attribute stored under `resource.latency_ms`. This was true before this change —
the prior pipeline backstop (`LabelFilterStage` via `blockLabelSet.Get`) may have
matched resource-scoped attributes as a side effect, but the column predicate never
did. The correct mental model: numeric range filters target body-parsed log fields
(`log.*`), not resource/stream labels. This case is unchanged.

Back-ref: `internal/logqlparser/compile.go:identifyPushdownStages`

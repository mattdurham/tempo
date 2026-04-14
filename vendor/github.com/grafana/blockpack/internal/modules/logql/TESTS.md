# logql — Test Specifications

This document defines the required tests for the LogQL execution pipeline.
Pipeline stage unit tests live in `internal/logqlparser/pipeline_test.go`.
Integration tests (StreamLogs, ExecuteLogMetrics) live in
`internal/modules/executor/stream_log_test.go` and `internal/modules/executor/metrics_log_test.go`.

---

## Pipeline Stage Unit Tests

### LQL-TEST-001: TestPipeline_Empty

**Scenario:** A nil or empty Pipeline is a no-op.

**Input:** `ts=1000, line="hello", labels={app: "foo"}`

**Assertions:** Returns `("hello", {app: "foo"}, true)`.

---

### LQL-TEST-002: TestPipeline_ShortCircuit

**Scenario:** A stage returning `false` stops subsequent stages from running.

**Setup:** Pipeline with stage-1 (drops all rows) followed by stage-2 (panics if called).

**Assertions:** Returns `(_, _, false)` without panic.

---

### LQL-TEST-003: TestJSONStage_ValidJSON

**Scenario:** Valid JSON object extracts top-level fields as labels.

**Input:** `line={"level":"info","msg":"started","count":42}`

**Assertions:** `labels["level"] == "info"`, `labels["msg"] == "started"`, `labels["count"] == "42"`.

---

### LQL-TEST-004: TestJSONStage_InvalidJSON

**Scenario:** Non-JSON input does not drop the row.

**Input:** `line="not json"`, `labels={app: "foo"}`

**Assertions:** Returns `("not json", {app: "foo"}, true)`. Labels unchanged.

---

### LQL-TEST-005: TestJSONStage_NestedValues

**Scenario:** Nested JSON objects are serialized as strings, not recursively expanded.

**Input:** `line={"outer": {"inner": "val"}}`

**Assertions:** `labels["outer"]` is a non-empty string (the JSON encoding of the nested object).

---

### LQL-TEST-006: TestLogfmtStage_Valid

**Scenario:** Valid logfmt input extracts key-value pairs.

**Input:** `line="level=info msg=started latency=12.5ms"`

**Assertions:** `labels["level"] == "info"`, `labels["msg"] == "started"`, `labels["latency"] == "12.5ms"`.

---

### LQL-TEST-007: TestLogfmtStage_Invalid

**Scenario:** Invalid logfmt does not drop the row.

**Input:** `line="=novalue"` (malformed logfmt).

**Assertions:** Row is kept, returns `true`.

---

### LQL-TEST-008: TestLabelFormatStage_Rename

**Scenario:** Rename label `src` to `dst`.

**Setup:** `mappings = {"dst": "src"}`, `labels = {src: "value", other: "x"}`

**Assertions:** `labels["dst"] == "value"`, `labels["src"]` absent, `labels["other"] == "x"`.

---

### LQL-TEST-009: TestLabelFormatStage_MissingSource

**Scenario:** Source label absent — destination set to empty string.

**Setup:** `mappings = {"dst": "missing"}`, `labels = {other: "x"}`

**Assertions:** `labels["dst"] == ""`.

---

### LQL-TEST-010: TestLineFormatStage_Basic

**Scenario:** Template replaces line using label values.

**Setup:** Template `"level={{ .level }} app={{ .app }}"`, `labels = {level: "warn", app: "svc"}`

**Assertions:** Returns line `"level=warn app=svc"`.

---

### LQL-TEST-011: TestLineFormatStage_MissingKey

**Scenario:** Template references absent label — produces empty string for that field.

**Setup:** Template `"{{ .missing }}"`, `labels = {}`

**Assertions:** Returns `""` as line; row NOT dropped.

---

### LQL-TEST-012: TestDropStage

**Scenario:** Named fields are removed from labels.

**Setup:** `fields = ["level", "ts"]`, `labels = {level: "info", ts: "123", msg: "hello"}`

**Assertions:** `labels["level"]` absent, `labels["ts"]` absent, `labels["msg"] == "hello"`.

---

### LQL-TEST-013: TestKeepStage

**Scenario:** Only named fields are kept in labels.

**Setup:** `fields = ["level"]`, `labels = {level: "info", ts: "123", msg: "hello"}`

**Assertions:** `labels["level"] == "info"`, `labels["ts"]` absent, `labels["msg"]` absent.

---

### LQL-TEST-014: TestLabelFilterStage_Equal

**Scenario:** `=` filter keeps rows where label matches.

**Setup:** `LabelFilterStage("level", "error", OpEqual)`, `labels = {level: "error"}`

**Assertions:** Returns `true`.

---

### LQL-TEST-015: TestLabelFilterStage_NotEqual

**Scenario:** `!=` filter drops rows where label matches.

**Setup:** `LabelFilterStage("level", "error", OpNotEqual)`, `labels = {level: "error"}`

**Assertions:** Returns `false`.

---

### LQL-TEST-016: TestLabelFilterStage_Regex

**Scenario:** `=~` filter matches by regex.

**Setup:** `LabelFilterStage("level", "err.*", OpRegex)`, `labels = {level: "error"}`

**Assertions:** Returns `true`.

---

### LQL-TEST-017: TestLabelFilterStage_NumericGT

**Scenario:** `>` filter compares parsed numeric label.

**Setup:** `LabelFilterStage("latency_ms", "100", OpGT)`, `labels = {latency_ms: "150"}`

**Assertions:** Returns `true`.

---

### LQL-TEST-018: TestLabelFilterStage_NumericNonParseable

**Scenario:** Non-numeric label value with numeric op drops the row.

**Setup:** `LabelFilterStage("latency_ms", "100", OpGT)`, `labels = {latency_ms: "not-a-number"}`

**Assertions:** Returns `false`.

---

### LQL-TEST-019: TestUnwrapStage_Valid

**Scenario:** Valid numeric label is stored in UnwrapValueKey.

**Setup:** `UnwrapStage("duration_ms")`, `labels = {duration_ms: "42.5"}`

**Assertions:** `labels[UnwrapValueKey] == "42.5"`, returns `true`.

---

### LQL-TEST-020: TestUnwrapStage_MissingLabel

**Scenario:** Absent label causes row drop.

**Setup:** `UnwrapStage("duration_ms")`, `labels = {other: "x"}`

**Assertions:** Returns `false`.

---

### LQL-TEST-021: TestUnwrapStage_NonNumeric

**Scenario:** Non-numeric label causes row drop.

**Setup:** `UnwrapStage("duration_ms")`, `labels = {duration_ms: "not-a-number"}`

**Assertions:** Returns `false`.

---

## Pipeline Integration Tests

### LQL-TEST-022: TestPipeline_JSONThenLabelFilter

**Scenario:** `| json | level = "error"` — only rows with JSON level=error pass.

**Input rows:**
- `line={"level":"error","svc":"api"}` → passes
- `line={"level":"info","svc":"api"}` → dropped
- `line="plain text"` → dropped (level absent after json)

**Assertions:** Only first row passes.

---

### LQL-TEST-023: TestPipeline_LogfmtThenLabelFormat

**Scenario:** `| logfmt | label_format svc=service` — extract then rename.

**Input:** `line="service=web level=info"`

**Assertions:** `labels["svc"] == "web"`, `labels["service"]` absent.

---

### LQL-TEST-024: TestPipeline_JSONThenDropKeep

**Scenario:** `| json | drop ts | keep level` — extract, drop ts, keep only level.

**Input:** `line={"level":"warn","ts":"123","msg":"hello"}`

**Assertions:** Only `labels["level"] == "warn"` remains; `ts` and `msg` absent.

---

## Metrics Tests

### LQL-TEST-025: TestMetrics_CountOverTime

**Scenario:** `count_over_time({app="svc"}[1m])` — count rows per bucket.

**Setup:** Write 6 log records, 2 per minute across 3 minutes.

**Assertions:** 3 buckets, each with value 2.

---

### LQL-TEST-026: TestMetrics_Rate

**Scenario:** `rate({app="svc"}[1m])` — count/stepSeconds per bucket.

**Setup:** Same as LQL-TEST-025.

**Assertions:** Each bucket value equals `2 / 60.0`.

---

### LQL-TEST-027: TestMetrics_BytesOverTime

**Scenario:** `bytes_over_time({app="svc"}[1m])` — sum byte lengths per bucket.

**Setup:** 3 log records with bodies "hello" (5 bytes), "world!" (6 bytes), "x" (1 byte) all in one bucket.

**Assertions:** Bucket value == 12.

---

### LQL-TEST-028: TestMetrics_SumOverTime

**Scenario:** `sum_over_time({app="svc"} | logfmt | unwrap duration [1m])` — sum unwrapped values.

**Setup:** 3 log records: `duration=10`, `duration=20`, `duration=30` in one bucket.

**Assertions:** Bucket value == 60.

---

### LQL-TEST-029: TestMetrics_VectorAggSum

**Scenario:** `sum by (level) (count_over_time(...[1m]))` — aggregate across label.

**Setup:** 4 records: 2 with `level=error`, 2 with `level=info`.

**Assertions:** Series for `level=error` has value 2, series for `level=info` has value 2.

---

### LQL-TEST-030: TestMetrics_Topk

**Scenario:** `topk(2, count_over_time(...[1m]))` — top 2 series by count.

**Setup:** 5 series with counts 10, 5, 8, 3, 1.

**Assertions:** Result contains exactly 2 series with values 10 and 8.

---

## Engine Integration Tests

### LQL-TEST-031: TestEngine_StreamLogs_Basic

**Scenario:** Basic log stream query with selector and pipeline.

**Setup:** Write 5 log records with `app=svc`, 3 with `level=error` (via logfmt), 2 with `level=info`.
Query: `{app="svc"} | logfmt | level = "error"`.

**Assertions:** Callback called 3 times; all delivered rows have `level=error`.

---

### LQL-TEST-032: TestEngine_StreamLogs_Limit

**Scenario:** StreamOptions.Limit caps delivered rows.

**Setup:** 10 log records. Query: `{}`. Limit: 3.

**Assertions:** Callback called exactly 3 times.

---

### LQL-TEST-033: TestEngine_StreamLogs_EmptyFile

**Scenario:** Empty file returns no rows without error.

**Setup:** Flush with no records. Query: `{}`.

**Assertions:** Callback never called; no error.

---

### LQL-TEST-034: TestEngine_ExecuteMetrics_Integration

**Scenario:** Full metric query on real blockpack log file.

**Setup:** Write 10 log records across 2 minutes with logfmt bodies.
Query: `count_over_time({app="svc"} | logfmt | level = "error" [1m])`.

**Assertions:** MetricsResult.Series non-empty; total count matches expected error rows.

---

## StreamLogs Engine Tests

### LQL-TEST-035: TestStreamLogs_BasicFilter

**Scenario:** `StreamLogs` with nil pipeline returns all rows matching selector.

**Setup:** 3 log records: 2 for "svc", 1 for "other". Selector: `{service.name = "svc"}`.

**Assertions:** Callback called 2 times; both entries have correct Line values.

---

### LQL-TEST-036: TestStreamLogs_WithPipeline

**Scenario:** JSON pipeline stage extracts labels from log body.

**Setup:** 2 records with JSON bodies. Selector + `| json`.

**Assertions:** Both entries have `level` label extracted from JSON body.

---

### LQL-TEST-037: TestStreamLogs_PipelineDropsRows

**Scenario:** `| json | level = "error"` drops non-error rows.

**Setup:** 3 records: 1 error, 2 info.

**Assertions:** Only 1 entry delivered.

---

### LQL-TEST-038: TestStreamLogs_NilReader

**Scenario:** nil reader returns nil without error.

**Assertions:** No error; no callback calls.

---

### LQL-TEST-039: TestStreamLogs_EarlyStop

**Scenario:** Callback returning `false` stops iteration.

**Setup:** 3 records. Callback stops after 1.

**Assertions:** Callback called exactly 2 times (stops on second false return).

---

## Predicate Pushdown Tests (CompileAll)

### LQL-TEST-040: TestCompileAll_LabelFilterBeforeParser_PushedDown

**Scenario:** `{app="foo"} | detected_level="error"` — label filter before any parser.

**Assertions:** Pipeline is nil (stage removed). Program includes `ScanEqual(log.detected_level, error)`.
QueryPredicates includes `DedicatedColumns["log.detected_level"]` entry.

---

### LQL-TEST-041: TestCompileAll_LabelFilterAfterParser_NotPushedDown

**Scenario:** `{app="foo"} | json | level="error"` — label filter after parser.

**Assertions:** Pipeline has 2 stages (json + label filter). Program does NOT include `ScanEqual(log.level, ...)`.

---

### LQL-TEST-042: TestCompileAll_RegexFilterPushedDown

**Scenario:** `{app="foo"} | detected_level=~"err.*"` — regex filter before parser.

**Assertions:** Pipeline is nil. Program includes `ScanRegex(log.detected_level, err.*)`.
QueryPredicates includes `DedicatedColumnsRegex["log.detected_level"]`.

---

### LQL-TEST-043: TestCompileAll_NegationFilterPushedDown

**Scenario:** `{app="foo"} | detected_level!="debug"` — negation before parser.

**Assertions:** Pipeline is nil. Program includes `ScanNotEqual(log.detected_level, debug)`.
Negation NOT added to `AttributesAccessed` (bloom safety).

---

### LQL-TEST-044: TestCompileAll_NumericFilterNotPushedDown

**Scenario:** `{app="foo"} | status > 500` — numeric comparison.

**Assertions:** Pipeline has 1 stage (filter remains). Not pushed down.

---

### LQL-TEST-045: TestCompileAll_OrFilterNotPushedDown

**Scenario:** `{app="foo"} | level="error" or level="warn"` — OR alternatives.

**Assertions:** Pipeline has 1 stage (OR filter remains). Not pushed down.

---

### LQL-TEST-046: TestCompileAll_MixedPushdownAndPipeline

**Scenario:** `{app="foo"} | detected_level="error" | json | msg="failed"`.

**Assertions:** Pipeline has 2 stages (json + msg filter). detected_level pushed into program.

---

## Coverage Requirements

- All `PipelineStageFunc` constructors must be exercised.
- Both happy path and error/drop path must be tested for each stage.
- `Pipeline.Process` short-circuit behavior must be verified (LQL-TEST-002).
- `StreamLogs` must be exercised with real in-memory blockpack files (LQL-TEST-035 to LQL-TEST-039).
- `ExecuteLogMetrics` must be exercised with real in-memory blockpack files (LQL-TEST-025 to LQL-TEST-029+).
- Unwrap integration with metrics (LQL-TEST-028) must pass.
- GroupBy aggregation (LQL-TEST-029) must work with correct label group keys.
- All 8 metric functions must have explicit tests.

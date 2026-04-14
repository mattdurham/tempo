# logql — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
LogQL execution pipeline. The code lives in `internal/logqlparser/` (parse, compile, pipeline
stages) and `internal/modules/executor/` (block scanning, log streaming, metrics).

---

## 1. Responsibility Boundary

| Concern | Owner |
|---------|-------|
| LogQL string → AST | `logqlparser` (`Parse`) |
| AST → `vm.Program` + `Pipeline` | `logqlparser` (`Compile`, `CompileAll`) |
| Pipeline stage execution (per row) | `logqlparser` (`Pipeline.Process`) |
| Log stream query execution | `executor` (`StreamLogs`, `CollectLogs`) |
| Metric aggregation execution | `executor` (`ExecuteLogMetrics`) |
| Block selection and bulk I/O | `queryplanner` + `blockio/reader` |

---

## 2. Core Types

### SPEC-001: PipelineStageFunc

```go
type PipelineStageFunc func(ts uint64, line string, labels LabelSet) (string, LabelSet, bool)
```

- Receives the row timestamp (`ts`), log body (`line`), and a mutable `LabelSet`.
- Returns the (possibly transformed) line, `LabelSet`, and a bool indicating keep (`true`) or drop (`false`).
- A `false` return signals that no further stages should run for this row.
- Implementations MUST NOT retain references to the input `LabelSet` beyond the call.
- `LabelSet` is an interface (see `internal/logqlparser/labelset.go`) that may be backed by block columns
  (lazy) or a plain `map[string]string`. Use `LabelSet.Materialize()` to obtain a concrete map when needed.
- Back-ref: `internal/logqlparser/pipeline.go:PipelineStageFunc`, `internal/logqlparser/labelset.go:LabelSet`

### SPEC-002: Pipeline

```go
type Pipeline struct {
    Stages []PipelineStageFunc
}

func (p *Pipeline) Process(ts uint64, line string, labels LabelSet) (string, LabelSet, bool)
```

- `Process` chains all stages in order. On the first stage returning `false`, processing stops immediately (short-circuit drop).
- A nil or empty `Pipeline` is a no-op: returns `(line, labels, true)`.
- `Process` is safe to call concurrently on the same `Pipeline` value (stages are pure functions; no shared mutable state).
- Back-ref: `internal/logqlparser/pipeline.go:Pipeline`

---

## 3. Pipeline Stages

### SPEC-003: JSONStage

```go
func JSONStage() PipelineStageFunc
```

- Parses `line` as a JSON object. Extracts all top-level key-value pairs as labels.
- Non-string JSON values are converted to string via `fmt.Sprint`.
- If `line` is not valid JSON or not a JSON object, the row is NOT dropped — labels remain unchanged.
- Back-ref: `internal/logqlparser/pipeline.go:JSONStage`

### SPEC-004: LogfmtStage

```go
func LogfmtStage() PipelineStageFunc
```

- Parses `line` as logfmt (`key=value` pairs separated by spaces).
- Uses `github.com/go-logfmt/logfmt` decoder.
- If `line` is not valid logfmt, labels remain unchanged; row is NOT dropped.
- Back-ref: `internal/logqlparser/pipeline.go:LogfmtStage`

### SPEC-005: LabelFormatStage

```go
func LabelFormatStage(mappings map[string]string) PipelineStageFunc
```

- For each `dst → src` mapping: copies `labels[src]` to `labels[dst]`.
- After copying, if `dst != src`, deletes `labels[src]`.
- If `src` is absent, `dst` is set to `""`.
- Never drops rows.
- Back-ref: `internal/logqlparser/pipeline.go:LabelFormatStage`

### SPEC-006: LineFormatStage

```go
func LineFormatStage(tmpl *template.Template) PipelineStageFunc
```

- Executes the pre-compiled `text/template` against the `labels` map as data.
- Replaces `line` with the template output.
- Template references `{{ .label_name }}` to access label values.
- If template execution fails, the original `line` is kept; row is NOT dropped.
- Template must be pre-compiled at query compile time (not per-row).
- Back-ref: `internal/logqlparser/pipeline.go:LineFormatStage`

### SPEC-007: DropStage

```go
func DropStage(fields []string) PipelineStageFunc
```

- Removes each named field from the `labels` map.
- Fields not present in `labels` are silently skipped.
- Never drops rows.
- Back-ref: `internal/logqlparser/pipeline.go:DropStage`

### SPEC-008: KeepStage

```go
func KeepStage(fields []string) PipelineStageFunc
```

- Removes all labels NOT in `fields` from the `labels` map.
- Fields in `fields` that are absent in `labels` are silently skipped.
- Never drops rows.
- Back-ref: `internal/logqlparser/pipeline.go:KeepStage`

### SPEC-009: LabelFilterStage

```go
func LabelFilterStage(name, value string, op logqlparser.FilterOp) PipelineStageFunc
```

- Evaluates `labels[name]` against `value` using the given `op`.
- String ops: `OpEqual` (exact), `OpNotEqual`, `OpRegex`, `OpNotRegex`.
- Numeric ops: `OpGT`, `OpLT`, `OpGTE`, `OpLTE` — parse `labels[name]` as `float64`; if unparseable, the row is dropped.
- Returns `false` (drop) if the filter condition is NOT satisfied.
- Back-ref: `internal/logqlparser/pipeline.go:LabelFilterStage`

### SPEC-010: UnwrapStage

```go
const UnwrapValueKey = "__unwrap_value__"

func UnwrapStage(field string) PipelineStageFunc
```

- Reads `labels[field]`, parses it as `float64`, and stores the result in `labels[UnwrapValueKey]`.
- If the label is absent or not parseable as float64, the row is dropped.
- Subsequent pipeline stages (and metric aggregation) read `UnwrapValueKey` for the numeric value.
- Back-ref: `internal/logqlparser/pipeline.go:UnwrapStage`

---

## 4. Label Scoping Rules

### SPEC-011: Label Lifecycle

- Stream labels (from the log selector, e.g., `{app="foo"}`) are present as pre-populated entries in the `labels` map passed to `Process`.
- Extracted labels (from `| json`, `| logfmt`) are added to the map during pipeline execution.
- Extracted labels are ephemeral — they exist only within the pipeline and metric accumulation phase.
- Labels are NEVER written back to blockpack column storage.
- Earlier pipeline stages take precedence: if `| json` extracts `level=info` and a later `| label_format` renames it, the final state reflects all transformations in order.

### SPEC-012: Label Map Ownership

- The `labels` map passed to `Process` is owned by the caller. The pipeline may mutate it in place.
- Callers that need to preserve the original labels must copy the map before calling `Process`.

---

## 5. Engine

### SPEC-013: LogEntry Type

```go
type LogEntry struct {
    LokiLabels     string
    Line           string
    LogAttrs       LogAttrs
    TimestampNanos uint64
}
```

- Holds one matched, pipeline-processed log row.
- `LokiLabels` is the raw value of `resource.__loki_labels__` for the row (Loki label set serialized as a string).
- `Line` is the (possibly transformed) log body after pipeline execution.
- `LogAttrs` holds `log.*` ColumnTypeString column values as parallel name/value slices.
- Back-ref: `internal/modules/executor/stream_log.go:LogEntry`

### SPEC-014: LogEntryCallback

`StreamLogs` no longer uses a callback type — it returns `([]LogEntry, error)` directly.

- `StreamLogs` collects all matching rows and returns them as a slice.
- Use `CollectLogs` for timestamp-sorted top-K collection with a limit.

### SPEC-015: StreamLogs Semantics

```go
func StreamLogs(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline) ([]LogEntry, error)
```

- Returns all matched, pipeline-passing rows as a slice.
- Uses `queryplanner` for block selection and column-predicate evaluation.
- For each matched row: reads `log:body`, `log:timestamp`, and stream labels from the block, then applies `pipeline.Process`.
- Rows where `Process` returns `false` are silently dropped.
- If `r` is nil, returns `(nil, nil)` immediately.
- Block I/O errors and predicate evaluation errors are returned immediately.
- Back-ref: `internal/modules/executor/stream_log.go:StreamLogs`

### SPEC-016: ExecuteLogMetrics Semantics

```go
func ExecuteLogMetrics(
    r *reader.Reader,
    program *vm.Program,
    pipeline *Pipeline,
    querySpec *vm.QuerySpec,
    funcName string,
    groupBy []string,
) (*LogMetricsResult, error)
```

- Runs block scan + predicate evaluation (same filter path as StreamLogs).
- For each matched row: reads timestamp, body, and labels; applies `pipeline.Process`.
- Rows passing the pipeline are accumulated into time-series buckets using `querySpec.TimeBucketing`.
- `querySpec.TimeBucketing.Enabled` must be `true` and `StepSizeNanos > 0`; otherwise rows are silently skipped.
- If `pipeline` contains an `UnwrapStage`, `UnwrapValueKey` label is used as the numeric value for aggregation.
- Returns a dense `LogMetricsResult` with one row per (bucket × group-by-key) pair.
- Back-ref: `internal/modules/executor/metrics_log.go:ExecuteLogMetrics`

---

## 6. Metric Aggregation

### SPEC-017: MetricsResult

```go
type LogMetricsResult struct {
    Rows          []LogMetricsRow
    BytesRead     int64
    BlocksScanned int
}

type LogMetricsRow struct {
    Values   map[string]float64
    GroupKey []string
}
```

- `GroupKey[0]` is the 0-indexed time-bucket number (as decimal string).
- `GroupKey[1..]` are the group-by label values corresponding to `groupBy` parameter.
- `Values` maps the function name (e.g., `"count_over_time"`) to the computed float64 value.
- Empty buckets (no matching rows) have `Values[funcName] = 0`.
- Back-ref: `internal/modules/executor/metrics_log.go:LogMetricsResult`

### SPEC-018: Supported Metric Functions

| Function | Behavior |
|----------|----------|
| `count_over_time` | `bucket.Count++` per matching row |
| `rate` | `bucket.Count / stepSeconds` |
| `bytes_over_time` | `sum(len(line))` per bucket |
| `bytes_rate` | `sum(len(line)) / stepSeconds` |
| `sum_over_time` | `sum(unwrapValue)` — requires unwrap |
| `avg_over_time` | `sum(unwrapValue) / count` — requires unwrap |
| `min_over_time` | `min(unwrapValue)` — requires unwrap |
| `max_over_time` | `max(unwrapValue)` — requires unwrap |
| `quantile_over_time` | percentile of collected values — requires unwrap |

### SPEC-019: Vector Aggregation Operators

| Operator | Behavior |
|----------|----------|
| `sum` | Sum values across series sharing GROUP BY labels |
| `avg` | Average across series |
| `min` | Minimum across series |
| `max` | Maximum across series |
| `topk(n, ...)` | Top N series by total value |
| `bottomk(n, ...)` | Bottom N series by total value |

---

## 7. Block Pruning Invariant

### SPEC-020: Structured Metadata Predicate Pushdown

Pipeline stages generally operate post-scan. However, `StageLabelFilter` stages that
reference structured metadata columns (`log.*`) can be pushed down into the `vm.Program`
when they appear **before** any parser stage (`| json`, `| logfmt`) in the pipeline.

**Pushdown eligibility rules (position-based):**
- The filter must be a `StageLabelFilter` in a contiguous run at the start of the pipeline, before any other stage type (parsers `| json`, `| logfmt`, and label-mutating stages `| label_format`, `| drop`, `| keep`, `| line_format` all act as barriers)
- The filter must NOT have OR alternatives (`OrFilters` must be empty)
- The filter must use a string comparison op (`=`, `!=`, `=~`, `!~`), not numeric (`>`, `<`, `>=`, `<=`)

**When pushed down, the filter contributes to:**
1. **Row-level pruning**: A `ColumnPredicate` closure that scans both `"log." + labelName` (structured metadata) and `"resource." + labelName` (stream labels). Positive ops (`=`, `=~`) union the results from both columns; negative ops (`!=`, `!~`) intersect them. This preserves the pipeline's unprefixed label map semantics where both structured metadata and stream labels are accessible by the same name.
2. **Block-level pruning**: `QueryPredicates` entries for bloom filter and range index pruning on both `"log." + labelName` and `"resource." + labelName` columns (equality and regex ops only; negations excluded from `AttributesAccessed` for bloom safety)

**Pushed-down stages are removed from the pipeline** to avoid double evaluation.

Filters after a parser stage cannot be pushed down because the label may reference a parsed
field (extracted from the log body at runtime), not a stored column.

**Two-phase evaluation order:**

`CompileAll` separates predicates into two evaluation phases within the `ColumnPredicate`:

1. **Phase 1 (cheap):** Label matchers (`resource.*`) and pushed-down structured metadata
   (`log.*`) run as bulk column scans. These columns are small, often RLE/dictionary-encoded.
2. **Phase 2 (expensive):** Line filters (`|=`, `|~`, etc.) evaluate per-row using
   `GetValue("log:body", rowIdx)` only on rows surviving phase 1. This avoids a full-column
   scan over the large `log:body` column when phase 1 eliminates most rows.

When no cheap predicates exist (no label matchers, no pushed-down metadata filters), line
filters fall back to bulk `ScanContains`/`ScanRegex` scans (original behavior).

Back-ref: `internal/logqlparser/compile.go:CompileAll`, `internal/logqlparser/compile.go:buildTwoPhasePredicate`

---

## 8. Error Handling

### SPEC-021: Stage Error Policy

- Parse errors in `JSONStage` and `LogfmtStage` are silent: the row is kept with unmodified labels.
- Parse errors in `LabelFilterStage` (numeric ops on non-numeric values) cause row drop (`false` return), not error propagation.
- Parse errors in `UnwrapStage` cause row drop (`false` return).
- Template execution errors in `LineFormatStage` keep the original line; row is NOT dropped.

### SPEC-022: Engine Error Propagation

- `StreamLogs`: block I/O errors and predicate evaluation errors are returned immediately.
- `ExecuteLogMetrics`: same as StreamLogs for I/O errors; bucket accumulation silently skips rows with missing/invalid timestamps.
- `StreamLogs` returns all matching rows; use `CollectLogs` with `opts.Limit` for early-stop behavior.
- `ExecuteLogMetrics` with `querySpec == nil` returns an error immediately.

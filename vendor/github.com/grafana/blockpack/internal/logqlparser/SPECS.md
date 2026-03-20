# logqlparser — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/logqlparser` package.

---

## 1. Responsibility Boundary

| Concern | Owner |
|---------|-------|
| LogQL string → AST | **logqlparser** (`Parse`, `ParseQuery`) |
| AST → `vm.Program` | **logqlparser** (`Compile`) |
| AST → `QueryPredicates` (block pruning) | **logqlparser** (`extractPredicates`) |
| AST → `*logqlparser.Pipeline` (executable stage closures) | **logqlparser** (`CompilePipeline`) |
| Executing the program against blocks | `executor` / `queryplanner` |
| Pipeline stage execution (per row) | `internal/modules/executor` (`StreamLogs`, `ExecuteLogMetrics`) |

---

## 2. Supported LogQL Subset

### LQP-SPEC-001: Label Matchers

```
{label="val"}       → MatchEqual
{label!="val"}      → MatchNotEqual
{label=~"regex"}    → MatchRegex
{label!~"regex"}    → MatchNotRegex
```

Multiple matchers are comma-separated within `{}` and ANDed.

### LQP-SPEC-002: Line Filters

```
|= "text"           → FilterContains
!= "text"           → FilterNotContains
|~ "regex"          → FilterRegex
!~ "regex"          → FilterNotRegex
```

Multiple line filters are chained after the selector and ANDed.

### LQP-SPEC-003: Pipeline Stages

Pipeline stages are parsed by `Parse` and stored in `LogSelector.Pipeline`:

```
| json                        → StageJSON
| logfmt                      → StageLogfmt
| label_format dst=src        → StageLabelFormat, Params=["dst=src"]
| line_format "tmpl"          → StageLineFormat, Params=["tmpl"]
| drop field1, field2         → StageDrop, Params=["field1", "field2"]
| keep field1                 → StageKeep, Params=["field1"]
| fieldname = "value"         → StageLabelFilter, LabelFilter.Op=OpEqual
| fieldname != "value"        → StageLabelFilter, LabelFilter.Op=OpNotEqual
| fieldname =~ "regex"        → StageLabelFilter, LabelFilter.Op=OpRegex
| fieldname !~ "regex"        → StageLabelFilter, LabelFilter.Op=OpNotRegex
| fieldname > 42              → StageLabelFilter, LabelFilter.Op=OpGT
| fieldname < 100             → StageLabelFilter, LabelFilter.Op=OpLT
| fieldname >= 10             → StageLabelFilter, LabelFilter.Op=OpGTE
| fieldname <= 50             → StageLabelFilter, LabelFilter.Op=OpLTE
| unwrap fieldname            → StageUnwrap, Params=["fieldname"]
```

### LQP-SPEC-003a: Metric Expressions

`ParseQuery` parses any LogQL query including metric expressions:

```
count_over_time({sel} | pipeline [5m])   → LogQuery.Metric
rate({sel} [1m])                         → LogQuery.Metric
sum by (label) (count_over_time(...))    → LogQuery.VectorAgg
topk(10, rate(...))                      → LogQuery.VectorAgg
```

---

## 3. Parse

```go
func Parse(query string) (*LogSelector, error)
```

### LQP-SPEC-004: Parse Semantics

- Empty string → `&LogSelector{}` with zero matchers and zero filters.
- `{}` → same as empty string.
- `{matchers} filters` → populated `LogSelector`.
- `{matchers} filters | pipeline stages` → `LogSelector` with `Pipeline` populated.
- Unexpected trailing tokens → descriptive error with position context.
- Malformed input → descriptive error with position context.

`ParseQuery` dispatches to `Parse` for log selectors, or parses `MetricExpr`/`VectorAggExpr`
when a metric function or vector aggregation operator is detected at the start.

Note: `!=` is context-dependent in LogQL — inside `{}` it is a not-equal label matcher;
after `{}` followed by a quoted string it is a not-contains line filter.

### LQP-SPEC-005: String Literals

Both double-quoted (`"value"`) and backtick (`` `value` ``) strings are supported.
Double-quoted strings support `\"` and `\\` escape sequences.

### LQP-SPEC-006: Label Names

Label names may contain letters, digits, underscores, and dots (e.g., `service.name`),
and may start with any of these characters.

---

## 4. Compile

```go
func Compile(sel *LogSelector) (*vm.Program, error)
```

### LQP-SPEC-007: Compile Semantics

- `nil` or empty selector → match-all program (`FullScan`).
- Label matchers compile to predicates on `resource.{name}` columns.
- Line filters compile to predicates on the `log:body` column.
- Multiple predicates are ANDed via `Intersect`.

### LQP-SPEC-008: Column Mapping

| LogQL | Column | Scan Method |
|-------|--------|-------------|
| `{key="val"}` | `resource.{key}` | `ScanEqual` |
| `{key!="val"}` | `resource.{key}` | `ScanNotEqual` |
| `{key=~"re"}` | `resource.{key}` | `ScanRegexFast` (pre-compiled) |
| `{key!~"re"}` | `resource.{key}` | `ScanRegexNotMatchFast` (pre-compiled) |
| `\|= "text"` | `log:body` | `ScanContains` |
| `!= "text"` | `log:body` | `Complement(ScanContains)` |
| `\|~ "regex"` | `log:body` | `ScanRegexFast` (pre-compiled) |
| `!~ "regex"` | `log:body` | `ScanRegexNotMatchFast` (pre-compiled) |

### LQP-SPEC-009: Not-Contains via Complement

There is no `ScanNotContains` in `ColumnDataProvider`. The `!= "text"` line filter
is implemented as `Complement(ScanContains(col, text))`. This is a deliberate design
choice — see NOTES.md §2.

### LQP-SPEC-010: QueryPredicates for Block Pruning

`Compile` populates `vm.Program.Predicates` (`*vm.QueryPredicates`) for block-level pruning
from two sources. The struct has two fields:
- `Nodes []vm.RangeNode` — AND-combined top-level pruning tree (leaf and composite nodes).
- `Columns []string` — additional columns for row-level decode only (negations, `log:body`).

**Stream selector matchers:**
- `MatchEqual` → leaf `RangeNode{Column:"resource.{name}", Values:[value]}`
- `MatchRegex` → leaf `RangeNode{Column:"resource.{name}", Pattern:pattern}`
- `MatchNotEqual` / `MatchNotRegex` → no `Nodes`; column added to `Columns` for row-level decode (bloom-pruning on absence would produce false negatives — see NOTE-11)

**Pipeline `StageLabelFilter` stages (contiguous leading run only; stops at first barrier stage):**

Only `StageLabelFilter` stages that appear before any parser/mutation stage (json, logfmt, label_format, drop, keep, line_format) are eligible. Any other stage type terminates the pushdown scan.

- `OpEqual`  → OR composite `{IsOR:true, Children:[log.{name} leaf, resource.{name} leaf]}` — safe, no false negatives; added to `Columns` for both scopes
- `OpRegex`  → OR composite with pattern leaf children
- `OpGT`     → OR composite with `Min:value` in both `log.{name}` and `resource.{name}` children
- `OpGTE`    → OR composite with `Min:value` (inclusive equivalent; range index handles exact boundary)
- `OpLT`     → OR composite with `Max:value` in both children
- `OpLTE`    → OR composite with `Max:value`
- `OpNotEqual` / `OpNotRegex` → no `Nodes`; both `log.{name}` and `resource.{name}` added to `Columns`

**Why OR composites for pipeline filters:** A pipeline label filter binds to whichever scope
provides the label at query time. Pruning only on `log.*` would drop blocks where the value
lives in `resource.*` (false negatives). An OR composite keeps a block if EITHER scoped column
satisfies the predicate in its range index. See NOTE-11 for full rationale.

**Nil predicates:** Selectors and pipelines whose positive-match predicates are all negations
produce nil `Predicates.Nodes` (no block pruning possible). A non-nil `*QueryPredicates` with
empty `Nodes` but non-empty `Columns` is valid (line filters / negation-only queries).

Back-ref: `internal/logqlparser/compile.go:extractPredicates`

### LQP-SPEC-018: CompileAll — Predicate Pushdown for Structured Metadata

```go
func CompileAll(sel *LogSelector) (*vm.Program, *Pipeline, error)
```

`CompileAll` compiles a LogSelector into both a `vm.Program` and a `*Pipeline`,
pushing eligible structured metadata label filters into the program. A `StageLabelFilter`
is pushed down when it is in a contiguous leading run before any other stage type
(parsers `| json`, `| logfmt`, and label-mutating stages `| label_format`, `| drop`,
`| keep`, `| line_format` all act as barriers), has no OR alternatives, and uses a
string comparison op (`=`, `!=`, `=~`, `!~`).

Pushed-down filters compile to:
- `ColumnPredicate` closure scanning both `"log." + labelName` and `"resource." + labelName`. Positive ops (`=`, `=~`) union the results; negative ops (`!=`, `!~`) intersect them. This preserves the pipeline's unprefixed label map semantics where both structured metadata and stream labels share the same name.
- `QueryPredicates.Nodes` OR composite covering both `"log." + labelName` and `"resource." + labelName` (block-level bloom/range pruning); both column names also added to `Columns` for first-pass decode.

Pushed-down stages are removed from the returned pipeline. See SPEC-020 in
`internal/modules/logql/SPECS.md` for the full pushdown specification.

Back-ref: `internal/logqlparser/compile.go:CompileAll`

---

## 5. AST Types

### LQP-SPEC-011: LogSelector

```go
type LogSelector struct {
    Matchers    []LabelMatcher
    LineFilters []LineFilter
    Pipeline    []PipelineStage // parsed pipeline stages (| json, | level="error", etc.)
}
```

### LQP-SPEC-012: LabelMatcher

```go
type LabelMatcher struct {
    Name  string    // label name (without "resource." prefix)
    Value string    // match value or regex pattern
    Type  MatchType // =, !=, =~, !~
}
```

### LQP-SPEC-013: LineFilter

```go
type LineFilter struct {
    Pattern string     // substring or regex pattern
    Type    FilterType // |=, !=, |~, !~
}
```

### LQP-SPEC-014: PipelineStage

```go
type PipelineStage struct {
    LabelFilter *LabelFilter      // set only for StageLabelFilter
    Params      []string          // stage-specific params
    Type        PipelineStageType // StageJSON, StageLogfmt, ...
}
```

### LQP-SPEC-015: MetricExpr

```go
type MetricExpr struct {
    Selector      *LogSelector
    Function      string          // count_over_time, rate, bytes_rate, etc.
    Unwrap        string          // optional unwrap field name
    Pipeline      []PipelineStage // pipeline stages (unwrap stage removed, stored in Unwrap)
    RangeDuration time.Duration   // range interval [5m] etc.
}
```

### LQP-SPEC-016: VectorAggExpr

```go
type VectorAggExpr struct {
    Inner   *MetricExpr // inner metric expression
    Op      string      // sum, avg, min, max, topk, bottomk
    GroupBy []string    // by (l1, l2)
    Without []string    // without (l1, l2)
    Param   int         // k for topk/bottomk
}
```

### LQP-SPEC-017: LogQuery

```go
type LogQuery struct {
    Selector  *LogSelector   // log stream query
    Metric    *MetricExpr    // metric query
    VectorAgg *VectorAggExpr // vector aggregation
}
```

Exactly one field is non-nil.

---

## 6. Pipeline Types and Stage Constructors

### SPEC-001: PipelineStageFunc

```go
type PipelineStageFunc func(ts uint64, line string, labels LabelSet) (string, LabelSet, bool)
```

Receives a nanosecond timestamp, the log line, and a mutable `LabelSet`. Returns the
(possibly transformed) line, `LabelSet`, and a keep boolean. `false` means drop the row.
Implementations MUST NOT retain references to the input `LabelSet` beyond the call.

Back-ref: `internal/logqlparser/pipeline.go:PipelineStageFunc`

### SPEC-001a: LabelSet

```go
type LabelSet interface {
    Get(key string) string
    Has(key string) bool
    HasLive(key string) bool
    Set(key, val string)
    Delete(key string)
    Keys() []string
    Materialize() map[string]string
}
```

- `Get` returns the value for `key`, or `""` if absent or deleted.
- `Has` reports whether `key` is present anywhere — in the overlay, a decoded column, OR an undecoded block column. Used for block-level column presence checks. For per-row parser stages, prefer `HasLive`.
- `HasLive` reports whether `key` has a live (immediately accessible) value — in the overlay (set by a previous stage) or in a decoded block column. Undecoded block columns (present in storage but not yet decompressed) return `false`. For `mapLabelSet`, `HasLive` is identical to `Has`. Used by `JSONStage` and `LogfmtStage` to skip only keys that are truly accessible, not merely present in block metadata.
- `Set` adds or overwrites `key` with `val`. A subsequent `Has(key)` MUST return `true`.
- `Delete` removes `key`. A subsequent `Has(key)` MUST return `false`.
- `Materialize` copies all present labels into a new `map[string]string`.

Two implementations exist: `mapLabelSet` (map-backed, tests and non-pre-parsed blocks) and `blockLabelSet` (column-backed, hot path — `internal/modules/executor/block_label_set.go`).

Back-ref: `internal/logqlparser/labelset.go:LabelSet`

### SPEC-002: Pipeline

```go
type Pipeline struct {
    Stages []PipelineStageFunc
}
```

`Process` chains all stages in order. Short-circuits on the first stage that returns
`false`. A nil or empty `Pipeline` is a no-op: returns `(line, labels, true)`.
`Process` and `ProcessSkipParsers` accept and return `LabelSet`.
`Process` is safe for concurrent use on the same `Pipeline` value.

Back-ref: `internal/logqlparser/pipeline.go:Pipeline.Process`

### SPEC-003: JSONStage

`JSONStage()` parses the log line as a JSON object and extracts all top-level key-value
pairs as labels. If the line is not valid JSON or not a JSON object, labels are unchanged
and the row is kept (silent failure policy — NOTE-007). When iterating JSON keys, keys
where `labels.HasLive(k)` returns true are skipped (live labels win). Keys present in
block storage but not yet decoded (HasLive=false) are populated into the overlay so that
subsequent stages such as `line_format` can access them.

Back-ref: `internal/logqlparser/pipeline.go:JSONStage`

### SPEC-004: LogfmtStage

`LogfmtStage()` parses the log line as logfmt key=value pairs and extracts them as labels.
Uses `github.com/go-logfmt/logfmt`. Invalid logfmt keeps labels unchanged; row is NOT
dropped (NOTE-007). Keys where `labels.HasLive(k)` returns true are skipped (live labels
win). Undecoded block columns (HasLive=false) are populated into the overlay so that
subsequent stages such as `line_format` can access them. Before parsing the body,
`HideBodyParsedColumns()` is called on the label set so that ingest-time last-wins
column values cannot shadow re-parsed first-wins values. After parsing, `__error__` is
always set to `""` unconditionally (non-strict mode — parse errors do not set
`"LogfmtParserErr"`). `__error__` is not guarded by `HasLive`.

Back-ref: `internal/logqlparser/pipeline.go:LogfmtStage`

### SPEC-005: LabelFormatStage

`LabelFormatStage(mappings map[string]string)` renames labels. For each dst→src mapping,
copies `labels[src]` to `labels[dst]` then deletes `labels[src]` (unless dst == src).
If src is absent, dst is set to `""`. Never drops rows.

Back-ref: `internal/logqlparser/pipeline.go:LabelFormatStage`

### SPEC-006: LineFormatStage

`LineFormatStage(tmpl *template.Template)` replaces the log line using the pre-compiled
`text/template` executed against the labels map. Template is pre-compiled at query compile
time (NOTE-005). If template execution fails, the original line is kept; row is NOT dropped.

Back-ref: `internal/logqlparser/pipeline.go:LineFormatStage`

### SPEC-007: DropStage

`DropStage(fields []string)` removes the named fields from labels. Fields not present in
labels are silently skipped. Never drops rows.

Back-ref: `internal/logqlparser/pipeline.go:DropStage`

### SPEC-008: KeepStage

`KeepStage(fields []string)` removes all labels NOT in the given fields list. Fields in
`fields` not present in labels are silently skipped. Never drops rows.

Back-ref: `internal/logqlparser/pipeline.go:KeepStage`

### SPEC-009: LabelFilterStage

`LabelFilterStage(name, value string, op FilterOp)` filters rows based on a label
comparison. String ops: `OpEqual`, `OpNotEqual`, `OpRegex`, `OpNotRegex`. Numeric ops:
`OpGT`, `OpLT`, `OpGTE`, `OpLTE` — parse label value as `float64`; if unparseable, drop.
Returns `false` (drop) if the filter condition is NOT satisfied.

```go
type FilterOp int

const (
    OpEqual    FilterOp = iota // ==
    OpNotEqual                 // !=
    OpRegex                    // =~
    OpNotRegex                 // !~
    OpGT                       // >
    OpLT                       // <
    OpGTE                      // >=
    OpLTE                      // <=
)
```

Back-ref: `internal/logqlparser/pipeline.go:LabelFilterStage`

### SPEC-010: UnwrapStage and UnwrapValueKey

```go
const UnwrapValueKey = "__unwrap_value__"
```

`UnwrapStage(field string)` reads `labels[field]`, parses it as `float64`, and stores the
result in `labels[UnwrapValueKey]`. If the label is absent or not parseable as `float64`,
the row is dropped. Subsequent stages and metric aggregation read `UnwrapValueKey` for the
unwrapped value (NOTE-006).

Back-ref: `internal/logqlparser/pipeline.go:UnwrapStage`, `internal/logqlparser/pipeline.go:UnwrapValueKey`

# executor — Test Specifications

This document defines the required tests for the `internal/modules/executor` package.
All tests use an in-memory write/read round-trip to exercise the full pipeline.

---

## EX-01: TestExecute_BasicQuery

**Scenario:** A service-name filter returns only matching spans.

**Setup:** 3 spans (svc-alpha, svc-beta, svc-alpha). Query: `{ resource.service.name = "svc-alpha" }`.

**Assertions:** `len(result.Matches) == 2`.

---

## EX-02: TestExecute_NoMatches

**Scenario:** A query for a non-existent service returns zero matches.

**Setup:** 1 span (real-svc). Query: `{ resource.service.name = "ghost-svc" }`.

**Assertions:** `result.Matches` is empty.

---

## EX-03: TestExecute_SpanAttributeFilter

**Scenario:** A span-attribute filter returns only spans with the matching value.

**Setup:** 5 spans with http.method = GET/POST/GET/DELETE/GET. Query: `{ span.http.method = "GET" }`.

**Assertions:** `len(result.Matches) == 3`.

---

## EX-04: TestExecute_MultiBlock

**Scenario:** Queries span all blocks when multiple blocks are present.

**Setup:** 12 spans, `MaxBlockSpans=5` (≥2 blocks), all with `batch.id = "b1"`.
Query: `{ span.batch.id = "b1" }`.

**Assertions:** `len(result.Matches) == 12`, `result.BlocksScanned >= 2`.

---

## EX-05: TestExecute_EmptyFile

**Scenario:** Querying an empty file returns an empty result without error.

**Setup:** Flush with no spans. Query: `{ resource.service.name = "any" }`.

**Assertions:** `result.Matches` is empty, `result.BlocksScanned == 0`.

---

## EX-06: TestExecute_ANDPredicate

**Scenario:** AND of two conditions returns only spans matching both.

**Setup:** 4 spans: (svc-a/GET), (svc-a/POST), (svc-b/GET), (svc-b/POST).
Query: `{ resource.service.name = "svc-a" && span.http.method = "GET" }`.

**Assertions:** `len(result.Matches) == 1`.

---

## EX-07: TestExecute_MatchAll

**Scenario:** The `{}` wildcard returns all spans.

**Setup:** 5 spans. Query: `{}`.

**Assertions:** `len(result.Matches) == 5`.

---

## EX-08: TestExecute_Limit

**Scenario:** `Options.Limit` caps the number of returned matches.

**Setup:** 10 spans. Query: `{}`. `Options{Limit: 3}`.

**Assertions:** `len(rows) == 3`.

---

## EX-09: TestExecute_SpanMatchFields

**Scenario:** `SpanMatch.TraceID` and `SpanMatch.SpanID` are populated from block columns.

**Setup:** 1 span with known TraceID `{0xAB, 0xCD, ...}`. Query: `{}`.

**Assertions:** `SpanMatchFromRow(rows[0], r.SignalType(), r).TraceID == [16]byte{0xAB, 0xCD}`,
`len(SpanMatchFromRow(rows[0], r.SignalType(), r).SpanID) > 0`.

---

## EX-10: TestExecute_PlanPopulated

**Scenario:** `QueryStats` is populated with execution path and step info for a query that
hits the intrinsic fast path.

**Setup:** 5 spans. Query: `{ resource.service.name = "svc" }`.

**Assertions:** `qs.ExecutionPath != ""` and at least one step present in `qs.Steps`
(returned as second value from `Collect`).

---

## EX-QS-01: TestQueryStats_ZeroValue

**Scenario:** Zero-value `QueryStats` is safe to read without initialization.

**Assertions:** `qs.ExecutionPath == ""`, `qs.TotalDuration == 0`, `qs.Steps == nil`.

---

## EX-QS-02: TestStepStats_NilMetadata

**Scenario:** `StepStats.Metadata` nil map is safe for missing-key lookups.

**Assertions:** `s.Metadata["total_blocks"]` returns zero-value `nil, false` without panic.

---

## EX-QS-03: TestCollect_ReturnsQueryStats_BlockScan

**Scenario:** `Collect` returns non-empty `QueryStats` for a block-scan query.

**Setup:** 1 span with user attribute `http.method="GET"` → forces block-scan path.
Query: `{ span.http.method = "GET" }`. `CollectOptions{Limit: 10}`.

**Assertions:**
- `len(rows) == 1`
- `qs.ExecutionPath != ""`
- `qs.TotalDuration > 0`
- `planStep(qs).Metadata["total_blocks"].(int) >= 1`

---

## EX-QS-04: TestCollectLogs_ReturnsQueryStats

**Scenario:** `CollectLogs` returns `QueryStats` with correct execution path for limited queries.

**Setup:** Log writer with 3 records. `CollectOptions{Limit: 10}`.

**Assertions:**
- `qs.ExecutionPath == "block-topk"`
- `qs.TotalDuration > 0`

---

## ~~EX-QS-05: TestCollect_WarnOnIntrinsicBlockScanFallback~~ (removed)

*Removed: 2026-03-30*

The `errNeedBlockScan` fallback path is triggered only when the intrinsic pre-filter finds
no usable intrinsic constraint (all top-level nodes are OR with non-intrinsic children).
Constructing a test that reliably hits this path without coupling to internal dispatch
logic proved fragile. The `slog.Warn` call is covered by code inspection and integration;
a unit test would require either exporting internal state or using global slog interception,
neither of which is worth the maintenance cost.

---

## Coverage Requirements

- All public entry points must be exercised: `Collect`, `ExecuteStructural`, `StreamLogs`, `CollectLogs`, `ExecuteLogMetrics`, `ExecuteTraceMetrics`.
- Both the empty-file short-circuit and the multi-block scan path must be covered.
- The `Options.Limit` early-exit path must be exercised (EX-08).
- `SpanMatch` field population must be verified (EX-09).
- Unscoped attribute expansion to resource, span, and log scopes must be covered (EX-11–EX-13).
- `QueryStats` zero-value, nil Metadata safety, and block-scan stats coverage (EX-QS-01–EX-QS-04).
- Intrinsic fast-path cases (SPEC-STREAM-9 Cases A–D) must be covered by EX-INT-01 through EX-INT-10 (see below).

---

## Integration Tests in blockio Package
*Added: 2026-02-25*

EX-01 through EX-07 are also exercised as integration tests in
`internal/modules/blockio/executor_test.go`. Those tests use the same TraceQL queries
and assertions but build trace data with `modules_blockio.Writer` (the full write path)
rather than relying solely on in-package writer helpers. This provides a full round-trip
validation: write → flush → read → query → assert.

EX-08 (Limit), EX-09 (SpanMatch fields), EX-10 (Plan populated), and EX-11 through
EX-13 (unscoped attribute expansion) are specific to
`internal/modules/executor/executor_test.go` and are not duplicated in the blockio
integration tests.

## EX-11: TestExecute_UnscopedAttr_MatchesResourceScope

**Scenario:** An unscoped attribute query (e.g. `.service.name = "svc-a"`) matches when
the attribute is present in the resource scope.

**Setup:** 2 spans; span 0 has `resource.service.name = "svc-a"`, span 1 has
`resource.service.name = "svc-b"`. Query: `{ .service.name = "svc-a" }`.

**Assertions:** `len(result.Matches) == 1`.

---

## EX-12: TestExecute_UnscopedAttr_MatchesSpanScope

**Scenario:** An unscoped attribute query matches when the attribute is present in the
span scope only (not resource scope).

**Setup:** 2 spans; span 0 has `span.custom.attr = "yes"`, span 1 has no `custom.attr`.
Query: `{ .custom.attr = "yes" }`.

**Assertions:** `len(result.Matches) == 1`.

---

## EX-13: TestExecute_UnscopedAttr_NoMatchWhenAbsent

**Scenario:** An unscoped attribute query returns zero matches when the attribute is
absent from all scopes.

**Setup:** 1 span with `span.other.attr = "val"`. Query: `{ .custom.attr = "yes" }`.

**Assertions:** `result.Matches` is empty.

---

## EX-14: TestBuildPredicates_RegexPrefix

**Scenario:** A regex pattern with a literal prefix produces a range-index predicate.

**Setup:** 1 span with `span.http.url = "https://api.example.com/foo"`.
Query: `{ span.http.url =~ "https://api.*" }`.

**Assertions:** Predicate for `span.http.url` has `Values` containing `"https://api"`.

---

## EX-15: TestBuildPredicates_RegexAlternation

**Scenario:** An alternation regex produces multiple values in the range-index predicate.

**Setup:** 1 span with `resource.service.name = "error-svc"`.
Query: `{ resource.service.name =~ "error|warn|info" }`.

**Assertions:** Predicate for `resource.service.name` has `Values` containing
`"error"`, `"warn"`, and `"info"`.

---

## EX-16: TestBuildPredicates_RegexComplex

**Scenario:** A complex regex that cannot be optimized produces a bloom-only predicate.

**Setup:** 1 span with `resource.service.name = "web-svc"`.
Query: `{ resource.service.name =~ ".*complex[a-z]+" }`.

**Assertions:** Predicate for `resource.service.name` has empty `Values`.

---

## EX-17: TestBuildPredicates_LogRegexPrefix

**Scenario:** Regex prefix optimization works for log files (not just traces).

**Setup:** 1 log record with `resource.service.name = "debug-service"`.
Query: `{ resource.service.name =~ "debug.*" }`.

**Assertions:** Predicate for `resource.service.name` has `Values` containing `"debug"`.

---

## EX-18: TestBuildPredicates_LogRegexCaseInsensitive

**Scenario:** Case-insensitive regex produces interval-match predicate with [UPPER, lower]
bounds covering all case variants via `BlocksForRangeInterval`. NOTE-011.

**Setup:** 1 log record with `resource.service.name = "my-service"`.
Query: `{ resource.service.name =~ "(?i)DEBUG.*" }`.

**Assertions:** Predicate for `resource.service.name` has `IntervalMatch: true`,
`Values[0] == "DEBUG"` (min/uppercase), `Values[1] == "debug\xff"` (max/lowercase + \xff suffix).

---

## EX-19: TestBuildPredicates_LogRegexCommonPrefix

**Scenario:** Case-sensitive alternation whose values share a common prefix produces
point-lookup predicates for each literal alternative. NOTE-024.

Go's regex parser factors `"cluster-0|cluster-1"` into
`Concat(Literal("cluster-"), CharClass([01]))`, so `AnalyzeRegex` returns the single
prefix `"cluster-"`. The interval `["cluster-", "cluster-\xff"]` is overly wide — it
matches ALL cluster-X blocks. `extractLiteralAlternatives` detects the raw pattern as a
pure literal OR and uses exact point lookups instead.

**Setup:** 1 log record with `resource.service.name = "cluster-0-service"`.
Query: `{ resource.service.name =~ "cluster-0|cluster-1" }`.

**Assertions:** Predicate for `resource.service.name` has `IntervalMatch: false`,
`Values` contains `"cluster-0"` and `"cluster-1"` (exact point lookups, not an interval).

---

## EX-S-01: TestStream_TracePath

**Scenario:** Stream with no timestamp column fires callback only for matching trace spans.

**Setup:** 3 spans (svc-alpha, svc-beta, svc-alpha). Query: `{ resource.service.name = "svc-alpha" }`.
`StreamOptions{TimestampColumn: ""}`.

**Assertions:** callback fires exactly twice; both invocations carry svc-alpha rows.
`StreamStats.SelectedBlocks >= 1`.

---

## EX-S-02: TestStream_LogPath_TimeFilter

**Scenario:** Stream with TimestampColumn applies per-row time filter.

**Setup:** 3 log records with `log:timestamp` T1 < T2 < T3.
`StreamOptions{TimestampColumn: "log:timestamp", TimeRange: {MinNano: T2, MaxNano: T2}}`.

**Assertions:** callback fires exactly once (T2 record only).

---

## EX-S-03: TestStream_Direction_Backward

**Scenario:** Backward direction delivers newest block first.

**Setup:** 4 log records across 2 blocks (MaxBlockSpans=2). Block 0 is older, block 1 newer.
`StreamOptions{Direction: Backward, TimestampColumn: "log:timestamp"}`.

**Assertions:** first callback invocation has `blockIdx == 1` (higher-numbered block).

---

## EX-S-04: TestStream_EarlyStop_FetchedLessThanSelected

**Scenario:** Limit causes early stop; FetchedBlocks <= SelectedBlocks.

**Setup:** 10 records across >= 3 blocks (use small MaxBlockSpans).
`StreamOptions{Limit: 2}`.

**Assertions:** callback fires exactly twice. `StreamStats.FetchedBlocks <= StreamStats.SelectedBlocks`.

**Note:** With small test data that fits in one coalesced group, FetchedBlocks == SelectedBlocks; strict < only holds when multiple groups exist.

---

## EX-S-05: TestStream_NilReader

**Scenario:** Nil reader returns nil without calling callback.

**Setup:** call `Collect(nil, program, CollectOptions{})`.

**Assertions:** error is nil; callback is never invoked.

---

## StreamLogs Tests (stream_log_test.go)

### EX-SL-01: TestStreamLogs_BasicFilter

**Scenario:** StreamLogs returns only rows matching the label selector.

**Setup:** 3 log records (svc×2, other×1). Query: `{service.name = "svc"}`, no pipeline.

**Assertions:** callback fires twice; both entries have `Line` from the matching records.

---

### EX-SL-02: TestStreamLogs_WithPipeline

**Scenario:** JSON pipeline stage extracts labels from body.

**Setup:** 2 records with JSON bodies containing `"level"` field.

**Assertions:** each `LogEntry.Labels["level"]` is populated.

---

### EX-SL-03: TestStreamLogs_PipelineDropsRows

**Scenario:** Label filter pipeline stage drops non-matching rows.

**Setup:** 3 records: 2×info, 1×error. Pipeline: `| json | level="error"`.

**Assertions:** only 1 entry delivered; `entry.Labels["level"] == "error"`.

---

### EX-SL-04: TestStreamLogs_NilReader

**Scenario:** Nil reader returns nil without calling callback.

**Assertions:** error is nil.

---

### EX-SL-05: TestStreamLogs_EarlyStop

**Scenario:** Returning false from callback stops iteration.

**Setup:** 3 records. Callback returns false after second call.

**Assertions:** callback invoked exactly twice.

---

## StreamLogsTopK Tests (stream_log_topk_test.go)

### EX-SLK-01: TestStreamLogsTopK_GlobalOrder_Backward

**Scenario:** Top-K by timestamp returns the globally correct K entries.

**Setup:** 6 log records across 2 blocks (MaxBlockSpans=3) with distinct timestamps T1…T6.
`opts.Limit=3`, `Direction=Backward`.

**Assertions:** callback fires 3 times; entries are T6, T5, T4 (newest 3).

---

### EX-SLK-02: TestStreamLogsTopK_PipelineFilters

**Scenario:** Pipeline dropping rows reduces the heap candidates.

**Setup:** 4 records with logfmt `level=info` or `level=error`. Pipeline: `| logfmt | level="error"`. `opts.Limit=10`.

**Assertions:** only error-level entries delivered; count matches error-level count.

---

### EX-SLK-03: TestStreamLogsTopK_BlockSkip

**Scenario:** Block-level skip triggered once heap is full.

**Setup:** 4 records across 2 blocks (MaxBlockSpans=2); block 0 has older timestamps,
block 1 has newer timestamps. `opts.Limit=2`, `Direction=Backward`.

**Assertions:** `StreamStats.FetchedBlocks < StreamStats.SelectedBlocks` after scan —
confirms block 0 was skipped because its MaxStart cannot improve the full heap.

---

### EX-SLK-04: TestStreamLogsTopK_TimeRange

**Scenario:** Per-row time filter excludes rows outside opts.TimeRange.

**Setup:** 3 records at T1, T2, T3. `opts.TimeRange={MinNano: T2, MaxNano: T2}`.

**Assertions:** exactly 1 entry delivered (T2 only).

---

### EX-SLK-05: TestStreamLogsTopK_LimitZeroDeliversAll

**Scenario:** `opts.Limit == 0` delivers all pipeline-passing rows sorted.

**Setup:** 5 records with distinct timestamps. No pipeline. `Direction=Forward`.

**Assertions:** all 5 entries delivered in ascending timestamp order.

---

### EX-SLK-06: TestStreamLogsTopK_NilReader

**Scenario:** Nil reader returns nil without calling callback.

**Assertions:** error is nil; callback never invoked.

---

### EX-SLK-07: TestStreamLogsTopK_NilPipeline

**Scenario:** Nil pipeline passes all rows through without transformation.

**Note:** No separate test function. Covered by `TestStreamLogsTopK_LimitZeroDeliversAll`
(limit=0, no pipeline, all rows delivered).

---

### EX-SLK-08: TestStreamLogsTopK_GlobalOrder_Forward

**Scenario:** Forward direction delivers oldest K entries in ascending order.

**Setup:** 4 records at T1 < T2 < T3 < T4. `opts.Limit=2`, `Direction=Forward`.

**Assertions:** 2 entries delivered; first has T1, second has T2.

---

## ExecuteLogMetrics Tests (metrics_log_test.go)

### EX-ELM-01: TestMetrics_CountOverTime

**Scenario:** count_over_time buckets log records by time step.

**Setup:** 2 records per bucket, 3 buckets (1-minute steps).

**Assertions:** 3 rows; each row has count == 2; `BytesRead > 0`.

---

### EX-ELM-02: TestMetrics_Rate

**Scenario:** rate divides count by step duration in seconds.

**Setup:** 2 records per bucket, 2 buckets (1-minute steps).

**Assertions:** rate == 2 / 60.0 per bucket.

---

### EX-ELM-03: TestMetrics_BytesOverTime

**Scenario:** bytes_over_time sums log line lengths per bucket.

**Setup:** 3 records with bodies "hello", "world!", "x" in one bucket.

**Assertions:** bucket value equals sum of `len(line)` for all records.

---

### EX-ELM-04: TestMetrics_SumOverTime

**Scenario:** sum_over_time sums unwrap values.

**Setup:** 3 records with logfmt bodies `duration=10`, `duration=20`, `duration=30`;
pipeline `| logfmt | unwrap duration`.

**Assertions:** bucket value == 60.

---

### EX-ELM-05: TestMetrics_GroupBy

**Scenario:** groupBy partitions aggregation by label values.

**Setup:** 2 error + 3 info records with logfmt level field; `groupBy: ["level"]`.

**Assertions:** two result rows; error count == 2, info count == 3.

---

### EX-ELM-06: TestMetrics_NilReader

**Scenario:** Nil reader returns empty result.

**Assertions:** `len(result.Rows) == 0`, no error.

---

### EX-ELM-07: TestMetrics_NilQuerySpec

**Scenario:** Nil querySpec returns an error (SPEC-ELM-2).

**Assertions:** error is non-nil.

---

### EX-ELM-08: TestMetrics_BytesRate

**Scenario:** bytes_rate divides total bytes by step duration in seconds.

**Setup:** 2 records with 5-byte bodies ("hello") in one 1-minute bucket; total = 10 bytes.

**Assertions:** bytes_rate == 10.0 / 60.0.

---

### EX-ELM-09: TestMetrics_AvgOverTime

**Scenario:** avg_over_time computes average of unwrap values per bucket.

**Setup:** 3 records with logfmt `duration=10`, `duration=20`, `duration=30`;
pipeline `| logfmt | unwrap duration`.

**Assertions:** avg == 20.0.

---

### EX-ELM-10: TestMetrics_MinMaxOverTime

**Scenario:** min_over_time and max_over_time return extremes of unwrap values.

**Setup:** 5 records with val = 5, 15, 3, 99, 42; pipeline `| logfmt | unwrap val`.

**Assertions:** min == 3.0; max == 99.0 (tested in separate ExecuteLogMetrics calls).

---

### EX-ELM-11: TestMetrics_RowsOutsideTimeRange

**Scenario:** Rows with timestamps outside [Start, End) are excluded from bucket counts.

**Setup:** 1 record inside the range; 1 before the range; 1 exactly at End (exclusive).

**Assertions:** 1 result row with count == 1.

---

## EX-20: TestExtractLiteralAlternatives

**Scenario:** `extractLiteralAlternatives` correctly classifies patterns.

**Setup:** Table-driven white-box unit test in `predicates_helper_test.go`
(package `executor`).

**Cases:**
| Pattern | Expected |
|---|---|
| `"cluster-0\|cluster-1"` | `["cluster-0", "cluster-1"]` |
| `"prod\|staging"` | `["prod", "staging"]` |
| `"error\|warn\|info"` | `["error", "warn", "info"]` |
| `"debug"` | `["debug"]` (single literal) |
| `"debug.*"` | nil (wildcard) |
| `"cluster-0.*\|cluster-1"` | nil (mixed) |
| `"(?i)error\|warn"` | nil (metachar) |
| `""` | nil (empty) |
| `"foo\|bar\|baz.qux"` | nil (dot metachar in third part) |
| `"foo\|"` | nil (empty alternative) |
| `` `cluster\-0\|cluster\-1` `` | nil (backslash metachar) |

**Assertions:** Return value equals expected for each case; single-literal case returns
a one-element slice (not nil) so the `len(lits) > 1` guard keeps it on the interval path.

---

## EX-21: TestBuildPredicates_LogRegexCaseInsensitiveAlternation

**Scenario:** Case-insensitive alternation (`(?i)(error|warn)`) falls back to bloom-only
predicate because each alternative requires a separate interval. NOTE-011.

**Setup:** 1 log record with `resource.service.name = "my-service"`.
Query: `{ resource.service.name =~ "(?i)(error|warn)" }`.

**Assertions:** Predicate for `resource.service.name` has `IntervalMatch: false` and
empty `Values` (bloom-only; no point lookups or interval bounds emitted for multi-prefix
case-insensitive alternations).

---

## ExecuteTraceMetrics Tests (metrics_trace_test.go)

### EX-ETM-01: TestTraceMetrics_Count

**Scenario:** count_over_time() with no group-by buckets spans across time buckets.

**Setup:** 3 buckets, 2 spans each. Query: `{ } | count_over_time()`.

**Assertions:** 1 series, 3 values, each == 2.0. BytesRead > 0.

---

### EX-ETM-02: TestTraceMetrics_Rate

**Scenario:** rate() divides count by step_seconds.

**Setup:** 2 spans per bucket. step = 60s.

**Assertions:** Values[0] == 2.0 / 60.0.

---

### EX-ETM-03: TestTraceMetrics_Sum

**Scenario:** sum(span.latency_ms) sums field values per bucket.

**Setup:** 3 spans with span.latency_ms = 10, 20, 30.

**Assertions:** Values[0] == 60.0.

---

### EX-ETM-04: TestTraceMetrics_Avg

**Scenario:** avg(span.latency_ms) computes average.

**Assertions:** Values[0] == 20.0.

---

### EX-ETM-05: TestTraceMetrics_MinMax

**Scenario:** min and max return extremes. Separate ExecuteTraceMetrics calls.

**Setup:** span.latency_ms = 5, 15, 3, 99.

**Assertions:** min Values[0] == 3.0; max Values[0] == 99.0.

---

### EX-ETM-06: TestTraceMetrics_GroupBy

**Scenario:** count_over_time() grouped by resource.service.name partitions series.

**Setup:** 2 spans svc-a, 3 spans svc-b.

**Assertions:** 2 series; svc-a count == 2, svc-b count == 3.

---

### EX-ETM-07: TestTraceMetrics_NaNMissingBuckets

**Scenario:** SUM/AVG/MIN/MAX emit NaN for empty buckets (SPEC-ETM-2).

**Setup:** 3 buckets; spans only in bucket 0 for sum(span.latency_ms).

**Assertions:** Values[0] not NaN; Values[1] and Values[2] are NaN.

---

### EX-ETM-08: TestTraceMetrics_CountZeroMissingBuckets

**Scenario:** count_over_time() emits 0 for empty buckets (SPEC-ETM-2).

**Setup:** 3 buckets; spans only in bucket 0.

**Assertions:** Values[1] == 0.0 and !math.IsNaN(Values[1]).

---

### EX-ETM-09: TestTraceMetrics_NilReader

**Scenario:** Nil reader returns empty result (SPEC-ETM-4).

**Assertions:** result != nil, len(result.Series) == 0, no error.

---

### EX-ETM-10: TestTraceMetrics_NilQuerySpec

**Scenario:** Nil querySpec returns an error (SPEC-ETM-5).

**Assertions:** err != nil.

---

### EX-ETM-11: TestTraceMetrics_OutOfRangeSpansSkipped

**Scenario:** Spans outside [StartTime, EndTime) are excluded (SPEC-ETM-6).

**Setup:** 1 in-range span, 1 before start, 1 at exact EndTime (exclusive).

**Assertions:** Values[0] == 1.0.

---

### EX-ETM-12: TestTraceMetrics_Histogram

**Scenario:** histogram_over_time(span.duration) produces time-series with `__bucket` labels.

**Setup:** 6 spans with `span:duration` = 1ms, 2ms, 5ms, 10ms, 50ms, 100ms (as nanoseconds).
Single time bucket. Query: `{ } | histogram_over_time(span.duration)`.

**Assertions:**
- `len(result.Series) > 0`
- Every series has a label `Name == "__bucket"` with a non-empty value
- All non-NaN values are >= 0.0

---

### EX-ETM-13: TestTraceMetrics_Quantile

**Scenario:** quantile_over_time(span.latency_ms, 0.9) returns 90th percentile of field values.

**Setup:** 10 spans with `span.latency_ms` = 1 through 10. Single time bucket.
Query: `{ } | quantile_over_time(span.latency_ms, 0.9)`.

**Assertions:** `Values[0] == 9.0` (nearest-rank 90th percentile: ceil(0.9×10)−1 = index 8 = value 9).

---

### EX-ETM-14: TestTraceMetrics_Stddev

**Scenario:** stddev(span.latency_ms) returns sample standard deviation.

**Setup:** 4 spans with `span.latency_ms` = 2, 4, 4, 4. Single time bucket.
Query: `{ } | stddev(span.latency_ms)`.

**Assertions:** `Values[0] == 1.0` (sample stddev of [2,4,4,4]: m2=3.0, sqrt(3/3)=1.0).

---

### EX-ETM-15: TestTraceMetrics_StddevNaN

**Scenario:** stddev with a single span returns NaN (sample stddev undefined for n < 2).

**Setup:** 1 span with `span.latency_ms` = 42. Single time bucket.
Query: `{ } | stddev(span.latency_ms)`.

**Assertions:** `math.IsNaN(Values[0]) == true`.

### EX-ETM-16: TestTraceMetrics_HistogramMultiGroupBy

**Scenario:** histogram_over_time with two GroupBy attributes produces correctly labeled series
(exercises multi-GroupBy composite key parsing via `strings.LastIndexByte`).

**Setup:** 2 spans with `(span.env, span.region)` = `(prod, us-east)` and `(staging, eu-west)`.
Duration 10ms. Single time bucket.
Query: `{ } | histogram_over_time(span.duration) by (span.env, span.region)`.

**Assertions:** Every series has `span.env`, `span.region`, and `__bucket` labels present.

---

## EX-ST-01: TestExecuteStructural_NilReader

**Scenario:** Nil reader returns empty result with no error.

**Setup:** Query `{ name = "x" } >> { name = "y" }`.

**Assertions:** `result.Matches` is empty; `err` is nil.

---

## EX-ST-02: TestExecuteStructural_Descendant

**Scenario:** `>>` returns the grandchild as a descendant of root.

**Setup:** 4-span trace (root→child1→grandchild, root→child2). Query: `{ resource.service.name = "svc-root" } >> { resource.service.name = "svc-leaf" }`.

**Assertions:** Exactly 1 match — the grandchild span.

---

## EX-ST-03: TestExecuteStructural_Child

**Scenario:** `>` returns only direct children, not grandchildren.

**Setup:** Same 4-span trace. Query: `{ resource.service.name = "svc-root" } > { resource.service.name = "svc-child" }`.

**Assertions:** 2 matches (child1, child2); grandchild is absent.

---

## EX-ST-04: TestExecuteStructural_Sibling

**Scenario:** `~` returns the sibling-op span as sibling of child-op.

**Setup:** Same 4-span trace. Query: `{ name = "child-op" } ~ { name = "sibling-op" }`.

**Assertions:** 1 match (child2/sibling-op); child1 does not match itself.

---

## EX-ST-05: TestExecuteStructural_Ancestor

**Scenario:** `<<` returns root and child1 as ancestors of grandchild.

**Setup:** Same 4-span trace. Query: `{ resource.service.name = "svc-leaf" } << { resource.service.name != "svc-leaf" }`.

**Assertions:** 2 matches (root, child1).

---

## EX-ST-06: TestExecuteStructural_Parent

**Scenario:** `<` returns only the direct parent of grandchild.

**Setup:** Same 4-span trace. Query: `{ resource.service.name = "svc-leaf" } < { resource.service.name != "svc-leaf" }`.

**Assertions:** 1 match (child1); root is not matched (it is grandparent).

---

## EX-ST-07: TestExecuteStructural_NotSibling

**Scenario:** `!~` returns right-side spans with no left-match sibling.

**Setup:** Same 4-span trace. Query: `{ name = "child-op" } !~ { name = "leaf-op" }`.

**Assertions:** 1 match (grandchild/leaf-op), whose parent (child1) has no sibling named child-op at the same level.

---

## EX-ST-08: TestExecuteStructural_Limit

**Scenario:** `Options.Limit` caps the number of returned matches.

**Setup:** Same 4-span trace. Query: `{ name = "root-op" } >> {}` (3 descendants). `Limit: 1`.

**Assertions:** `len(result.Matches) == 1`.

---

## EX-ST-09: TestExecuteStructural_MultiBlock

**Scenario:** Spans split across multiple blocks are correctly resolved.

**Setup:** `maxSpansPerBlock=1` forces each span into its own block. Same descendant query as EX-ST-02.

**Assertions:** Grandchild is still found; `len(result.Matches) == 1`.

---

## EX-ST-10: TestExecuteStructural_NotDescendant

**Scenario:** `!>>` returns only rightMatch spans with no leftMatch ancestor (SPEC-STRUCT-6).

**Setup:** Standard 4-span trace (root→child1→grandchild, root→child2).
Query: `{ name = "root-op" } !>> {}`.
All 4 spans are rightMatch (wildcard). Root has no ancestors → passes.
child1, child2, grandchild all have root in their ancestor chain → excluded.

**Assertions:**
- `len(result.Matches) == 1`
- root (spanID 0xAA) is present
- child1 (0xBB), child2 (0xDD), grandchild (0xCC) are absent

---

## EX-ST-11: TestExecuteStructural_NotChild

**Scenario:** `!>` returns rightMatch spans whose direct parent is NOT leftMatch, or spans
with no parent (SPEC-STRUCT-7).

**Setup:** Standard 4-span trace. Query: `{ name = "root-op" } !> {}`.
root has no parent → passes. child1/child2 have parent=root (leftMatch) → excluded.
grandchild has parent=child1 (not leftMatch) → passes.

**Assertions:**
- `len(result.Matches) == 2`
- root (0xAA) and grandchild (0xCC) are present
- child1 (0xBB) and child2 (0xDD) are absent

---

## EX-PA-01: TestQueryTraceQL_PipelineCount

**Scenario:** `| count()` with no threshold emits all spans from every spanset (SPEC-PA-3, SPEC-PA-6).

**Setup:** 3 spans in the same trace, `resource.service.name = "test-svc"`.
Query: `{ resource.service.name = "test-svc" } | count()`.

**Assertions:** 3 spans returned.

---

## EX-PA-02: TestQueryTraceQL_PipelineAvg

**Scenario:** `| avg(span.latency_ms)` without threshold emits all spans (SPEC-PA-1, SPEC-PA-6).

**Setup:** 3 spans with latency_ms = 10, 20, 30 (int64). Same trace.
Query: `{ span.latency_ms > 0 } | avg(span.latency_ms)`.
Note: aggregate field must appear in filter predicate to be loaded into wantColumns.

**Assertions:** 3 spans returned (avg=20.0, no threshold → passes).

---

## EX-PA-03: TestQueryTraceQL_PipelineMin

**Scenario:** `| min(span.latency_ms) > 15` keeps only qualifying spansets (SPEC-PA-1).

**Setup:** trace-A: latency_ms=5,25 (min=5 → fails); trace-B: latency_ms=20,30 (min=20 → passes).
Query: `{ span.latency_ms > 0 } | min(span.latency_ms) > 15`.

**Assertions:** 2 spans returned (trace-B only).

---

## EX-PA-04: TestQueryTraceQL_PipelineMax

**Scenario:** `| max(span.latency_ms) < 50` keeps only qualifying spansets (SPEC-PA-1).

**Setup:** trace-A: latency_ms=10,80 (max=80 → fails); trace-B: latency_ms=10,40 (max=40 → passes).
Query: `{ span.latency_ms > 0 } | max(span.latency_ms) < 50`.

**Assertions:** 2 spans returned (trace-B only).

---

## EX-PA-05: TestQueryTraceQL_PipelineThreshold

**Scenario:** `| count() > N` filters traces by span count (SPEC-PA-3).

**Setup:** trace-A: 2 spans; trace-B: 5 spans. Query: `{ resource.service.name = "test-svc" } | count() > 3`.

**Assertions:** 5 spans returned (trace-B only).

---

## EX-PA-06: TestQueryTraceQL_PipelineNoMatchingField

**Scenario:** avg over non-existent field → all spansets skipped (SPEC-PA-2).

**Setup:** 3 spans with `resource.service.name = "test-svc"` and NO `latency_ms` attribute.
Query: `{ resource.service.name = "test-svc" } | avg(span.latency_ms)`.

**Note:** Two invariants combine to produce 0 results here: (1) SPEC-PA-1 — because
`span.latency_ms` does not appear in the filter predicate, it is not loaded into
`wantColumns` and `getSpanFieldNumeric` finds no values; (2) SPEC-PA-2 — even if the
column were loaded, it is absent from the data. Both invariants contribute to the zero
result. This test primarily demonstrates the interaction between SPEC-PA-1 and SPEC-PA-2.

**Assertions:** 0 spans returned.

---

## EX-PA-07: TestQueryTraceQL_PipelineNoAggregate

**Scenario:** Pipeline with no aggregate emits all filtered spans (SPEC-PA-7).

**Setup:** 3 spans. Query: `{ resource.service.name = "test-svc" } | by(resource.service.name)`.

**Assertions:** 3 spans returned.

---

## EX-PA-08: TestQueryTraceQL_PipelineSum

**Scenario:** `| sum(span.latency_ms)` without threshold emits all spans (SPEC-PA-1, SPEC-PA-6).

**Setup:** 3 spans with latency_ms = 10, 20, 30 (int64). Same trace.
Query: `{ span.latency_ms > 0 } | sum(span.latency_ms)`.

**Assertions:** 3 spans returned (sum=60.0, HasThreshold=false, spanset passes).

---

## EX-INT-06: TestIntrinsicFastPath_MixedPredicateBlockScan

**Scenario:** Mixed predicates — intrinsic pre-filter (Case C) narrows candidates by service
name; VM re-evaluation eliminates all rows because no spans have the non-intrinsic attribute.

**Setup:** 6 spans — 3 for "loki-querier", 2 for "grafana", 1 for "tempo-distributor".
Query: `{ resource.service.name =~ "loki-.*" && span.http.method = "GET" }`.
No spans have `http.method` set.

**Assertions:**
- `len(rows) == 0` (VM re-evaluation eliminates all intrinsic candidates).
- No error, no panic.

Back-ref: `intrinsic_correctness_test.go:TestIntrinsicFastPath_MixedPredicateBlockScan`

---

## EX-INT-07: TestCollect_MixedPredicateNoSort

**Scenario:** Mixed query (intrinsic + span attribute, no sort) uses intrinsic pre-filter
then VM re-evaluation; returns only rows matching both conditions.

**Setup:** 10 spans: 5 with `resource.service.name="svc-a"` + `span.http.method="GET"`,
5 with `resource.service.name="svc-a"` + `span.http.method="POST"`.
Query: `{ resource.service.name = "svc-a" && span.http.method = "GET" }`.
`CollectOptions{Limit: 10}`.

**Assertions:** `len(results) == 5`. All results have `span.http.method == "GET"`.

---

## EX-INT-08: TestCollect_MixedPredicateWithSort

**Scenario:** Mixed query with timestamp sort uses intrinsic pre-filter + topKScanRows;
returns globally correct top-K.

**Setup:** 6 spans across 2 blocks (MaxBlockSpans=3): 3 with `svc="target"` + `http.method="GET"`
at timestamps T0, T2, T4; 3 with `svc="other"` at T1, T3, T5.
Query: `{ resource.service.name = "target" && span.http.method = "GET" }`.
`CollectOptions{Limit: 2, TimestampColumn: "span:start", Direction: Backward}`.

**Assertions:** `len(results) == 2`. Both results have `resource.service.name == "target"`.
Results are in descending timestamp order.

---

## EX-INT-09: TestCollect_PureIntrinsicNoSort_RegressionGuard

**Scenario:** Pure intrinsic query with no sort still returns correct results after gate
change from `ProgramIsIntrinsicOnly` to `hasSomeIntrinsicPredicates`. Also verifies
adaptive dispatch: equality predicates populate `MatchedRow.Block` (forEachBlockInGroups),
range predicates populate `MatchedRow.IntrinsicFields` (lookupIntrinsicFields).

**Setup:** 10 spans: 5 with `svc="svc-a"`, 5 with `svc="svc-b"`.
Query: `{ resource.service.name = "svc-a" }` (equality). `CollectOptions{Limit: 5}`.

**Assertions:**
- `len(results) == 5`
- For each row: `row.Block != nil` and `row.IntrinsicFields == nil` (equality → Block path)
- `row.Block.GetColumn("resource.service.name")` is present and value is `"svc-a"`

---

## EX-INT-10: TestCollect_PureIntrinsicWithSort_ZeroBlockRead

**Scenario:** Pure intrinsic query with timestamp sort returns IntrinsicFields rows
(zero block reads). Regression guard for Case B path.

**Setup:** 6 spans: 3 with `svc="target"` at timestamps T0, T2, T4; 3 with `svc="other"`.
Query: `{ resource.service.name = "target" }`.
`CollectOptions{Limit: 2, TimestampColumn: "span:start", Direction: Backward}`.

**Assertions:** `len(results) == 2`. Both results have `IntrinsicFields != nil`
(confirms zero-block-read path). Results are in descending timestamp order.

---

## EX-INT-11: TestHasSomeIntrinsicPredicates

**Scenario:** Unit test for the `hasSomeIntrinsicPredicates` helper.

**Setup:** Table-driven, package-internal test in `predicates_helper_test.go`.

**Cases:**
| Query | Expected |
|---|---|
| `{ resource.service.name = "svc" }` | `true` |
| `{ span:duration > 1000 }` | `true` |
| `{ span.http.method = "GET" }` | `false` |
| `{ resource.service.name = "svc" && span.http.method = "GET" }` | `true` |
| `{ span.http.method = "GET" \|\| resource.service.name = "svc" }` | `true` |
| `{}` | `false` |
| nil program | `false` |

**Assertions:** Return value equals expected for each case.

---

## EX-INT-12: TestCollect_MixedPredicatePartialAND_SupersetSafety

**Scenario:** Partial-AND pre-filter returns a superset; VM re-evaluation eliminates
false positives. Verifies the superset safety invariant.

**Setup:** 4 spans: span-0 `(svc-a, GET)`, span-1 `(svc-a, POST)`,
span-2 `(svc-b, GET)`, span-3 `(svc-b, POST)`.
Query: `{ resource.service.name = "svc-a" && span.http.method = "GET" }`.
`CollectOptions{Limit: 10}`.

**Assertions:** `len(results) == 1`. The single result corresponds to span-0 only.

---

## EX-INT-13: TestCollect_NonIntrinsicOnly_FallsBackToBlockScan

**Scenario:** True fallback to full block scan when query has ONLY non-intrinsic predicates.
`hasSomeIntrinsicPredicates` returns false and the executor runs the full block scan path.

**Setup:** Use `buildIntrinsicTestReader` data (6 spans across 3 services, none with
`span.http.method` set). Query: `{ span.http.method = "GET" }`. `CollectOptions{Limit: 100}`.

**Assertions:** `len(results) == 0` (no spans have http.method); no error; no panic.
Confirms the full block scan is correctly invoked when the intrinsic fast path is gated off.


---

## EX-PERF-01: TestCollect_IntrinsicTopK_MapPath_Stats

**Scenario:** Case B KLL path (M < SortScanThreshold) produces QueryStats with
ExecutionPath="intrinsic-topk-kll", intrinsic step ref_count=M, scan_count=0. NOTE-044.

**Setup:** 3 spans with svc="rare-svc" at explicit timestamps T0 < T1 < T2. 5 spans
with svc="other-svc". CollectOptions{Limit: 2, TimestampColumn: "span:start",
Direction: Backward}.

**Assertions:**
- len(rows) == 2
- qs.ExecutionPath == "intrinsic-topk-kll"
- intrinsicStep(qs).Metadata["ref_count"] == 3
- intrinsicStep(qs).Metadata["scan_count"] == 0
- rows[0] is newer than rows[1] (correct descending timestamp order)

---

## EX-PERF-02: TestCollect_IntrinsicTopK_ScanPath_Stats

**Scenario:** Case B scan path forced via executor.SortScanThreshold=2 override.
Confirms ExecutionPath="intrinsic-topk-scan" and scan_count > 0.

**Setup:** Set executor.SortScanThreshold = 2; restore in t.Cleanup. Do NOT t.Parallel().
3 spans svc="target", 3 spans svc="other". CollectOptions{Limit: 2,
TimestampColumn: "span:start", Direction: Backward}.

**Assertions:**
- qs.ExecutionPath == "intrinsic-topk-scan"
- intrinsicStep(qs).Metadata["scan_count"].(int) > 0
- len(rows) == 2

---

## EX-PERF-03: TestCollect_ExecutionPath_AllPaths

**Scenario:** Table-driven sub-tests confirm each of the seven primary ExecutionPath
values is produced by the correct query shape.

**Sub-tests:** "intrinsic-plain", "intrinsic-topk-kll" (NOTE-044), "intrinsic-topk-scan"
(override SortScanThreshold in sub-test, no t.Parallel), "mixed-plain", "mixed-topk",
"block-plain", "block-topk".

**Each sub-test assertions:** qs.ExecutionPath equals expected string; count fields
(ref_count, candidate_blocks in step Metadata) are non-zero where applicable.

---

## EX-PERF-04: TestCollect_IntrinsicTopK_KLLPath

**Scenario:** Case B KLL path (M < SortScanThreshold) groups refs by block, orders blocks
by BlockMeta.MaxStart DESC, and returns globally correct top-K by timestamp. NOTE-044.

**Setup:** 3 spans with svc="rare-svc" at distinct timestamps; 5 spans with svc="other-svc".
CollectOptions{Limit: 2, TimestampColumn: "span:start", Direction: Backward}.

**Assertions:**
- len(rows) == 2
- qs.ExecutionPath == "intrinsic-topk-kll"
- intrinsicStep(qs).Metadata["ref_count"] == 3
- intrinsicStep(qs).Metadata["scan_count"] == 0
- rows[0].IntrinsicFields != nil (zero block reads)
- rows[1].IntrinsicFields != nil
- rows[0] span:start > rows[1] span:start (descending order)



---

## EP-01: TestExecutionPath_RangePredicate_IntrinsicFields

**Scenario:** Range predicates on intrinsic columns (duration>X) route to "intrinsic-plain"
and populate `MatchedRow.IntrinsicFields` via `lookupIntrinsicFields` (zero block decode).
Regression guard for adaptive `collectIntrinsicPlain` dispatch (hasRangePredicate=true path).

**Setup:** 5 spans with varying duration (2ms, 50ms, 200ms, 300ms, 2ms), status, kind,
and service name. `CollectOptions{Limit: 10000}`.

**Cases:**
- `{ duration > 100ms }` → 2 results (200ms, 300ms spans)
- `{ duration > 10ms }` → 3 results (50ms, 200ms, 300ms spans)

**Assertions per case:**
- `qs.ExecutionPath == "intrinsic-plain"`
- `blockScanStep(qs)` is nil (intrinsic path — no block-scan step)
- Each row: `row.IntrinsicFields != nil`, `row.Block == nil`

---

## EP-02: TestExecutionPath_EqualityPredicate_BlockPopulated

**Scenario:** Equality predicates on intrinsic columns (status=error, kind=server, svc=X)
route to "intrinsic-plain" and populate `MatchedRow.Block` via `forEachBlockInGroups`.
Regression guard for adaptive `collectIntrinsicPlain` dispatch (hasRangePredicate=false path).

**Setup:** Same 5-span dataset as EP-01. `CollectOptions{Limit: 10000}`.

**Cases:**
- `{ status = error }` → 2 results
- `{ kind = server }` → 3 results
- `{ resource.service.name = "svc-a" }` → 2 results
- `{ resource.service.name = "svc-b" }` → 2 results

**Assertions per case:**
- `qs.ExecutionPath == "intrinsic-plain"`
- Each row: `row.Block != nil`, `row.IntrinsicFields == nil`

---

## EP-03: TestExecutionPath_RangeAndEquality_IntrinsicFields

**Scenario:** Combined range+equality intrinsic query uses the range (IntrinsicFields) path.
When ANY predicate is a range, hasRangePredicate=true drives the whole group to
`lookupIntrinsicFields`, including the equality parts.

**Setup:** Same 5-span dataset. `CollectOptions{Limit: 10000}`.
Query: `{ duration > 100ms && status = error }`.

**Assertions:**
- `len(results) == 1` (only span with 300ms duration AND error status)
- `qs.ExecutionPath == "intrinsic-plain"`
- Each row: `row.IntrinsicFields != nil`, `row.Block == nil`

---

## EP-04: TestExecutionPath_UserAttribute_UsesBlockScan

**Scenario:** User attribute predicate (span.http.method) bypasses intrinsic fast path
entirely — no intrinsic predicate → hasSomeIntrinsicPredicates=false → block-plain path.

**Setup:** Same 5-span dataset; one span has `span.http.method="GET"`.
`CollectOptions{Limit: 10000}`.
Query: `{ span.http.method = "GET" }`.

**Assertions:**
- `len(results) == 1`
- `qs.ExecutionPath == "block-plain"`
- `blockScanStep(qs).IOOps > 0`

---

## EP-05: TestExecutionPath_Correctness

**Scenario:** Result count correctness across all query types — range, equality, OR, AND,
user attribute. Guards against path optimizations silently changing observable results.

**Setup:** Same 5-span dataset. Table-driven. `CollectOptions{Limit: 10000}`.

**Cases:**
| Query | Expected count |
|---|---|
| `{ duration > 100ms }` | 2 |
| `{ duration > 10ms }` | 3 |
| `{ duration > 1s }` | 0 |
| `{ status = error }` | 2 |
| `{ kind = server }` | 3 |
| `{ kind = client }` | 2 |
| `{ resource.service.name = "svc-a" }` | 2 |
| `{ resource.service.name = "svc-b" }` | 2 |
| `{ span.http.method = "GET" }` | 1 |
| `{ duration > 100ms && status = error }` | 1 |
| `{ status = error && kind = server }` | 0 |
| `{ status = error \|\| kind = server }` | 5 |
| `{ resource.service.name = "svc-a" && status = error }` | 1 |

**Assertions:** `len(results) == expected` for every case; no errors.

---

**Scenario:** `HasLive` returns true when the key is in the overlay.

**Setup:** Zero-column `blockLabelSet`; call `Set("level", "info")`.

**Assertions:** `bls.HasLive("level") == true`.

---

## EXEC-TEST-BLS-012: TestBlockLabelSet_HasLive_MissingKey

**Scenario:** `HasLive` returns false when the key is absent entirely.

**Setup:** Zero-column `blockLabelSet`; no Set called.

**Assertions:** `bls.HasLive("missing") == false`.

---

## EXEC-TEST-BLS-013: TestBlockLabelSet_HasLive_DeletedKey

**Scenario:** `HasLive` returns false for a key that was Set then Deleted.

**Setup:** Zero-column `blockLabelSet`; `Set("app", "svc")` then `Delete("app")`.

**Assertions:** `bls.HasLive("app") == false`.

---

## Intrinsic Fast-Path Tests (intrinsic_correctness_test.go, intrinsic_pruning_test.go, metrics_trace_intrinsic_test.go)

These tests verify the SPEC-STREAM-9 intrinsic execution path (Cases A–D) and the
SPEC-STREAM-10 invariants (nilIntrinsicScan, userAttrProgram, filterRowSetByIntrinsicNodes).

---

### EX-INT-01: TestIntrinsicFastPath_RegexOnServiceName

**Scenario:** A regex predicate on `resource.service.name` uses the intrinsic fast path
(no full block read required when the service name column exists in the intrinsic TOC).

**Setup:** v4 file with 3 spans of different `resource.service.name` values.
Query: `{ resource.service.name =~ "svc.*" }`.

**Assertions:** only matching spans returned; block read count is zero (intrinsic section only).

Back-ref: `internal/modules/executor/intrinsic_correctness_test.go:TestIntrinsicFastPath_RegexOnServiceName`

---

### EX-INT-02: TestIntrinsicFastPath_OROnServiceName

**Scenario:** An OR predicate with `resource.service.name` uses the intrinsic fast path.

**Setup:** v4 file with spans from services A, B, C.
Query: `{ resource.service.name = "A" OR resource.service.name = "B" }`.

**Assertions:** spans from A and B returned; no block I/O.

Back-ref: `internal/modules/executor/intrinsic_correctness_test.go:TestIntrinsicFastPath_OROnServiceName`

---

### EX-INT-03: TestCollect_PureIntrinsicNoSort_RegressionGuard

**Scenario:** Pure intrinsic equality query (SPEC-STREAM-9 Case A, equality path) returns
correct results with zero block reads. Regression guard for the intrinsic-section migration.

**Setup:** v4 file. Query: `{ resource.service.name = "svc-alpha" }`.

**Assertions:** only svc-alpha spans returned; `blockScanStep(qs)` is nil (zero block reads via intrinsic path).

Back-ref: `internal/modules/executor/intrinsic_correctness_test.go:TestCollect_PureIntrinsicNoSort_RegressionGuard`

---

### EX-INT-04: TestCollect_PureIntrinsicNoSort_RangePredicate

**Scenario:** Pure intrinsic range predicate (SPEC-STREAM-9 Case A range path) returns
correct results with zero block reads.

**Setup:** v4 file with spans whose `span:duration` values straddle a threshold.
Query: `{ span:duration > X }`.

**Assertions:** only spans with duration > X returned; `blockScanStep(qs)` is nil (zero block reads via intrinsic path).

Back-ref: `internal/modules/executor/intrinsic_correctness_test.go:TestCollect_PureIntrinsicNoSort_RangePredicate`

---

### EX-INT-05: TestCollect_PureIntrinsicWithSort_ZeroBlockRead

**Scenario:** Pure intrinsic query with top-K sort (SPEC-STREAM-9 Case B) returns
globally correct top-K results with zero block reads.

**Setup:** v4 file with N spans across multiple blocks. `CollectOptions{Limit: K, TimestampColumn: "span:start"}`.

**Assertions:** K spans returned in timestamp order; `blockScanStep(qs)` is nil (zero block reads via intrinsic path).

Back-ref: `internal/modules/executor/intrinsic_correctness_test.go:TestCollect_PureIntrinsicWithSort_ZeroBlockRead`

---

### EX-INT-06: TestCollect_NonIntrinsicOnly_FallsBackToBlockScan

**Scenario:** A non-intrinsic-only query (SPEC-STREAM-9 Case C/D) falls through to the
full block scan path and still returns correct results.

**Setup:** v4 file. Query: `{ span.http.status_code = 200 }` (non-intrinsic column).

**Assertions:** correct spans returned; `blockScanStep(qs).IOOps >= 1`.

Back-ref: `internal/modules/executor/intrinsic_correctness_test.go:TestCollect_NonIntrinsicOnly_FallsBackToBlockScan`

---

### EX-INT-07: TestIntrinsicOnlyQueryPrunesBlocks

**Scenario:** An intrinsic-only predicate prunes blocks via the TOC before any block read.
Verifies that block selection using the intrinsic index skips non-matching blocks.

**Setup:** v4 file with multiple blocks; only one block contains the target service name.
Query: `{ resource.service.name = "target-svc" }`.

**Assertions:** `planStep(qs).Metadata["selected_blocks"].(int) < total blocks`; correct spans returned.

Back-ref: `internal/modules/executor/intrinsic_pruning_test.go:TestIntrinsicOnlyQueryPrunesBlocks`

---

### EX-INT-08: TestBlocksFromIntrinsicTOC_FlatDurationRange

**Scenario:** A `span:duration` range predicate selects only the blocks whose duration
range overlaps the predicate interval.

**Setup:** v4 file with 2 blocks: one with short-duration spans, one with long-duration spans.
Query: `{ span:duration > threshold }` where threshold is between the two groups.

**Assertions:** only the block with matching spans is selected; no false-negative pruning.

Back-ref: `internal/modules/executor/intrinsic_pruning_test.go:TestBlocksFromIntrinsicTOC_FlatDurationRange`

---

### EX-INT-09: TestProgramIsIntrinsicOnly

**Scenario:** `programIsIntrinsicOnly` correctly identifies programs that reference only
intrinsic columns (enabling the zero-block-read fast path).

**Setup:** programs with (a) only intrinsic predicates, (b) mixed predicates,
(c) only non-intrinsic predicates.

**Assertions:** returns true only for (a); false for (b) and (c).

Back-ref: `internal/modules/executor/intrinsic_pruning_test.go:TestProgramIsIntrinsicOnly`

---

### EX-INT-10: TestTraceMetrics_Intrinsic_CountAll_ZeroBlockReads

**Scenario:** A trace metrics query that references only intrinsic columns executes
with zero block reads (uses intrinsic section exclusively).

**Setup:** v4 file. Metrics query: count all spans grouped by `resource.service.name`.

**Assertions:** correct per-service counts; `stats.BlocksRead == 0`.

Back-ref: `internal/modules/executor/metrics_trace_intrinsic_test.go:TestTraceMetrics_Intrinsic_CountAll_ZeroBlockReads`

---

### EX-INT-11: TestLogBuildDenseRows_CapPrealloc

**Scenario:** `logBuildDenseRows` with a realistic time window and multiple attr group keys
does not panic and returns the correct row count.

**Setup:** 10 time buckets, 3 attr group keys; each bucket populated in the input map.

**Assertions:** `len(rows) == 30` (10 * 3); no panic; result is non-nil.

Back-ref: `internal/modules/executor/metrics_log_overflow_test.go:TestLogBuildDenseRows_CapPrealloc`

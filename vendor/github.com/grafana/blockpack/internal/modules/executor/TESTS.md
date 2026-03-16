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

**Assertions:** `len(result.Matches) == 3`.

---

## EX-09: TestExecute_SpanMatchFields

**Scenario:** `SpanMatch.TraceID` and `SpanMatch.SpanID` are populated from block columns.

**Setup:** 1 span with known TraceID `{0xAB, 0xCD, ...}`. Query: `{}`.

**Assertions:** `result.Matches[0].TraceID == [16]byte{0xAB, 0xCD}`,
`len(result.Matches[0].SpanID) > 0`.

---

## EX-10: TestExecute_PlanPopulated

**Scenario:** `result.Plan` is populated with block count information.

**Setup:** 5 spans. Query: `{ resource.service.name = "svc" }`.

**Assertions:** `result.Plan != nil`, `result.Plan.TotalBlocks > 0`.

---

## Coverage Requirements

- All public functions (`New`, `Execute`, `Stream`) must be exercised.
- Both the empty-file short-circuit and the multi-block scan path must be covered.
- The `Options.Limit` early-exit path must be exercised (EX-08).
- `SpanMatch` field population must be verified (EX-09).
- Unscoped attribute expansion to resource, span, and log scopes must be covered (EX-11–EX-13).

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

## EX-17: TestBuildPredicates_LogRegexCommonPrefix

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

**Setup:** call `Stream(nil, program, StreamOptions{}, callback)`.

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

### EX-SLK-01: TestStreamLogsTopK_BasicTopK

**Scenario:** Top-K by timestamp returns the globally correct K entries.

**Setup:** 6 log records across 2 blocks (MaxBlockSpans=3) with distinct timestamps T1…T6.
`opts.Limit=3`, `Direction=Backward`.

**Assertions:** callback fires 3 times; entries are T6, T5, T4 (newest 3).

---

### EX-SLK-02: TestStreamLogsTopK_PipelineFiltering

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

### EX-SLK-05: TestStreamLogsTopK_UnlimitedCollectAll

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

**Setup:** 3 records. No pipeline.

**Assertions:** all 3 entries delivered.

---

### EX-SLK-08: TestStreamLogsTopK_DirectionForward

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

## EX-18: TestExtractLiteralAlternatives

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

## EX-CL-01: TestClassifyCollect_IntrinsicTopK

**Scenario:** Intrinsic-only predicate + limit + TimestampColumn set + HasIntrinsicSection
→ `modeIntrinsicTopK`.

**Setup:** In-memory reader with intrinsic section. Program with intrinsic-only predicate
(resource.service.name). `opts.Limit=5, opts.TimestampColumn="span:start"`.

**Assertions:** `classifyCollect(r, program, opts) == modeIntrinsicTopK`.

---

## EX-CL-02: TestClassifyCollect_IntrinsicPlain

**Scenario:** Intrinsic-only predicate + limit + no TimestampColumn → `modeIntrinsicPlain`.

**Setup:** Same as EX-CL-01 but `opts.TimestampColumn=""`.

**Assertions:** `classifyCollect(r, program, opts) == modeIntrinsicPlain`.

---

## EX-CL-03: TestClassifyCollect_BlockFallback_NoIntrinsicSection

**Scenario:** Intrinsic-only predicate but file has no intrinsic section → `modeBlockTopK`
(when TimestampColumn set) or `modeBlockPlain`.

**Setup:** Reader without intrinsic section. Intrinsic-only program.
`opts.Limit=5, opts.TimestampColumn="span:start"`.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockTopK`.

---

## EX-CL-04: TestClassifyCollect_BlockTopK

**Scenario:** Non-intrinsic predicate + TimestampColumn + Limit → `modeBlockTopK`.

**Setup:** Any reader. Non-intrinsic program (e.g. span attribute filter).
`opts.Limit=10, opts.TimestampColumn="span:start"`.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockTopK`.

---

## EX-CL-05: TestClassifyCollect_BlockPlain

**Scenario:** Default case → `modeBlockPlain`.

**Setup:** Any reader. Any program. `opts.Limit=0, opts.TimestampColumn=""`.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockPlain`.

---

## EX-CL-06: TestClassifyCollect_ZeroLimitNotIntrinsic

**Scenario:** `opts.Limit == 0` with intrinsic-only program → `modeBlockPlain`.
Intrinsic fast path only activates when Limit > 0.

**Setup:** Reader with intrinsic section. Intrinsic-only program. `opts.Limit=0`.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockPlain`.

---

## EX-CC-01: TestComputeColumnSets_MatchAll

**Scenario:** A match-all program (no predicates) returns nil for both wantColumns and
secondPassCols.

**Setup:** `vm.Program` with no `Predicates` field set. `opts = CollectOptions{}`.

**Assertions:** `wantColumns == nil`; `secondPassCols == nil`.

---

## EX-CC-02: TestComputeColumnSets_AllColumns

**Scenario:** `AllColumns=true` sets secondPassCols to nil even when wantColumns is
non-nil (AllColumns overrides the narrow column set).

**Setup:** Predicate program with `resource.service.name` in nodes. `opts.AllColumns=true`.

**Assertions:** `wantColumns != nil`; `secondPassCols == nil`.

---

## EX-CC-03: TestComputeColumnSets_NarrowColumns

**Scenario:** `AllColumns=false` with a non-nil wantColumns produces
`secondPassCols = searchMetaColumns ∪ wantColumns`.

**Setup:** Predicate program with `resource.service.name` in nodes. `opts.AllColumns=false`.

**Assertions:** `wantColumns != nil`; `secondPassCols != nil`;
`secondPassCols` contains all entries from `wantColumns`;
`secondPassCols` contains all entries from `searchMetaColumns()`.

---

## EX-CL-03b: TestClassifyCollect_NonIntrinsicProgram_BlockTopK

**Scenario:** Non-intrinsic program with TimestampColumn + Limit → `modeBlockTopK`
(covers the block-mode fallback even when the file has an intrinsic section, because
the program references a non-intrinsic column).

**Setup:** Reader with intrinsic section. Non-intrinsic program (`span.http.method`).
`opts.Limit=5, opts.TimestampColumn="span:start"`.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockTopK`.

---

## EX-CL-07: TestClassifyCollect_BlockTopK_NoLimit

**Scenario:** `TimestampColumn` set but `Limit == 0` → `modeBlockPlain`.
`modeBlockTopK` requires both `TimestampColumn != ""` AND `Limit > 0`.

**Setup:** Any reader. Non-intrinsic program. `opts.Limit=0, opts.TimestampColumn="span:start"`.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockPlain`.

---

## EX-CL-08: TestClassifyCollect_Direction_DoesNotAffectMode

**Scenario:** `Direction=Backward` alone (no TimestampColumn, no Limit) → `modeBlockPlain`.
Direction is applied at plan time but does not change the collect mode.

**Setup:** Any reader. Non-intrinsic program. `opts.Direction=queryplanner.Backward`.
No TimestampColumn, no Limit.

**Assertions:** `classifyCollect(r, program, opts) == modeBlockPlain`.

---

## EX-OR-01: TestIntrinsicFastPath_ORPredicateReturnsResults
*Added: NOTE-039*

**Scenario:** A query with an OR predicate on an intrinsic dict column returns correct results
via the intrinsic fast path (not zero results due to the old `collectIntrinsicLeaves` skip).

**Setup:** Reader with intrinsic section containing `resource.service.name` dict column with
at least two distinct values ("A" and "B"). Program with OR predicate:
`resource.service.name="A" || resource.service.name="B"`. `opts.Limit=100`.

**Assertions:** `Collect` returns all rows where service.name is "A" or "B"; result count
matches the union of both sets; no `errNeedBlockScan` propagated (fast path succeeds).

---

## EX-OR-02: TestIntrinsicFastPath_RegexDictColumn
*Added: NOTE-039*

**Scenario:** A regex predicate on an intrinsic dict column returns correct results via the
intrinsic fast path.

**Setup:** Reader with intrinsic section containing `resource.service.name` dict column.
Program with regex predicate `resource.service.name=~"loki-.*"`. `opts.Limit=100`.

**Assertions:** `Collect` returns rows whose service.name matches the regex; count matches
results from an equivalent equality query; no `errNeedBlockScan` propagated.

---

## EX-OR-03: TestIntrinsicFastPath_UnevaluableFallsBackToBlockScan
*Added: NOTE-039*

**Scenario:** When the intrinsic fast path cannot evaluate a predicate (returns nil from
`evalNodeBlockRefs`), `collectIntrinsicPlain` returns `errNeedBlockScan` and `Collect`
falls through to `collectBlockPlain`, producing correct results.

**Setup:** Reader with intrinsic section. Program with a predicate that is not evaluable
from intrinsic blobs (e.g., a range predicate `span:duration > 1s`). `opts.Limit=10`.

**Assertions:** `Collect` returns the same results as if the mode were `modeBlockPlain`
from the start; no empty result due to missing fallback.

---

## EX-OR-04: TestEvalNodeBlockRefs_ORUnion
*Added: NOTE-039*

**Scenario:** `evalNodeBlockRefs` with an OR node correctly unions children's refs.

**Setup:** OR node with two evaluable leaf children, each contributing disjoint BlockRefs.

**Assertions:** Returned refs = union of both children's refs. `ok == true`.

---

## EX-OR-05: TestEvalNodeBlockRefs_ORUnevaluableChild
*Added: NOTE-039*

**Scenario:** `evalNodeBlockRefs` with an OR node where one child is not evaluable returns
`(nil, false)`.

**Setup:** OR node with one evaluable leaf and one unevaluable leaf (e.g., range predicate
on dict column).

**Assertions:** Returns `(nil, false)`. Cannot union partial results.

---

## EX-OR-06: TestEvalNodeMatchKeys_ORUnionSorted
*Added: NOTE-039*

**Scenario:** `evalNodeMatchKeys` with an OR node correctly unions and sorts children's keys.

**Setup:** OR node with two evaluable leaf children, producing sorted key slices.

**Assertions:** Returned keys = sorted union (merge-dedup) of both children. `ok == true`.

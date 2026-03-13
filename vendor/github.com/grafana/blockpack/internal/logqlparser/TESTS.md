# logqlparser — Test Specifications

This document defines the required tests for the `internal/logqlparser` package.
Tests are split between `parser_test.go` (parse-level) and `compile_test.go` (compile-level
with a mock `ColumnDataProvider`).

---

## Parser Tests (parser_test.go)

### LQP-TEST-001: TestParse_EmptyQuery
**Scenario:** Empty string parses to zero matchers, zero filters.

### LQP-TEST-002: TestParse_EmptySelector
**Scenario:** `{}` parses to zero matchers, zero filters.

### LQP-TEST-003: TestParse_SingleEquals
**Scenario:** `{cluster="dev-us-central-0"}` parses to 1 MatchEqual matcher.

### LQP-TEST-004: TestParse_MultipleMatchers
**Scenario:** Two comma-separated matchers parse correctly.

### LQP-TEST-005: TestParse_ThreeLabels
**Scenario:** Three matchers including a regex (`=~`) parse correctly.

### LQP-TEST-006: TestParse_AllMatchTypes
**Scenario:** `{a="1", b!="2", c=~"3", d!~"4"}` produces all four MatchType values.

### LQP-TEST-007: TestParse_LineFilterContains
**Scenario:** `|= "error"` produces FilterContains.

### LQP-TEST-008: TestParse_LineFilterNotContains
**Scenario:** `!= "debug"` produces FilterNotContains.

### LQP-TEST-009: TestParse_LineFilterRegex
**Scenario:** `|~ "(?i)timeout|deadline|canceled"` produces FilterRegex with full pattern.

### LQP-TEST-010: TestParse_LineFilterNotRegex
**Scenario:** `!~ "(?i)debug|trace"` produces FilterNotRegex.

### LQP-TEST-011: TestParse_MultiLineFilters
**Scenario:** Three chained line filters parse in order with correct types and patterns.

### LQP-TEST-012: TestParse_BenchmarkQueries
**Scenario:** All 14 selector+filter queries from the lokibench suite parse with expected
matcher and filter counts.

### LQP-TEST-013: TestParse_StopsAtPipelineStage
**Scenario:** `{cluster="dev"} | json` stops at pipeline stage without error.

### LQP-TEST-014: TestParse_BacktickString
**Scenario:** Backtick-quoted values parse correctly.

### LQP-TEST-015: TestParse_EscapedString
**Scenario:** `\"` escape in double-quoted string produces literal `"`.

### LQP-TEST-016: TestParse_DottedLabelName
**Scenario:** `service.name` label names with dots parse correctly.

### LQP-TEST-017: TestParse_ErrorCases
**Scenario:** 8 malformed queries all return errors.

### LQP-TEST-018: TestParse_TrailingTokenError
**Scenario:** Unexpected trailing tokens after selector/filters produce errors.
**Cases:** bare text (`{a="1"} garbage`), numbers, trailing text after line filters.

---

## Compile Tests (compile_test.go)

All compile tests use a `mockProvider` that records which scan methods are called and
returns preset RowSets, allowing verification of correct method dispatch and set operations.

### LQP-TEST-100: TestCompile_MatchAll_NilSelector
**Scenario:** nil selector produces FullScan.
**Assertion:** All 4 rows returned.

### LQP-TEST-101: TestCompile_MatchAll_EmptySelector
**Scenario:** Empty LogSelector produces FullScan.

### LQP-TEST-102: TestCompile_MatcherEqual
**Scenario:** MatchEqual calls `ScanEqual("resource.cluster", "dev")`.

### LQP-TEST-103: TestCompile_MatcherNotEqual
**Scenario:** MatchNotEqual calls `ScanNotEqual("resource.env", "prod")`.

### LQP-TEST-104: TestCompile_MatcherRegex
**Scenario:** MatchRegex calls `ScanRegex("resource.ns", "mimir.*")`.

### LQP-TEST-105: TestCompile_MatcherNotRegex
**Scenario:** MatchNotRegex calls `ScanRegexNotMatch("resource.tag", "debug.*")`.

### LQP-TEST-106: TestCompile_MatcherUnsupportedType
**Scenario:** Unknown MatchType returns error.

### LQP-TEST-107: TestCompile_FilterContains
**Scenario:** FilterContains calls `ScanContains("log:body", "error")`.
Intersects with label matcher result.

### LQP-TEST-108: TestCompile_FilterNotContains
**Scenario:** FilterNotContains calls `ScanContains` then `Complement`.
**Assertion:** 3 rows (complement of 1 from 4).

### LQP-TEST-109: TestCompile_FilterRegex
**Scenario:** FilterRegex calls `ScanRegex("log:body", pattern)`.

### LQP-TEST-110: TestCompile_FilterNotRegex
**Scenario:** FilterNotRegex calls `ScanRegexNotMatch("log:body", pattern)`.

### LQP-TEST-111: TestCompile_FilterUnsupportedType
**Scenario:** Unknown FilterType returns error.

### LQP-TEST-112: TestCompile_MultiplePredicatesIntersect
**Scenario:** Two matchers + one line filter produce triple Intersect.
**Assertion:** {0,1} ∩ {0,2} ∩ {0} = {0}.

### LQP-TEST-113: TestExtractPredicates_EqualMatcher
**Scenario:** MatchEqual populates DedicatedColumns with string value.

### LQP-TEST-114: TestExtractPredicates_RegexMatcher
**Scenario:** MatchRegex populates DedicatedColumnsRegex.

### LQP-TEST-115: TestExtractPredicates_NegationMatcher
**Scenario:** MatchNotEqual does not populate AttributesAccessed or DedicatedColumns (negation-only selectors yield nil predicates).

### LQP-TEST-116: TestExtractPredicates_NoMatchers
**Scenario:** Line-filter-only query returns nil predicates.

### LQP-TEST-117: TestExtractPredicates_DedupAttributesAccessed
**Scenario:** Same column referenced twice appears once in AttributesAccessed.

### LQP-TEST-118: TestCompile_PredicatesPopulated
**Scenario:** Compile with label matcher produces non-nil Predicates.

### LQP-TEST-119: TestCompile_PredicatesNilForLineFiltersOnly
**Scenario:** Compile with only line filters produces nil Predicates.

---

## Pipeline Predicate Pushdown Tests (compile_test.go / pipeline_test.go)

### LQP-TEST-120: TestExtractPredicates_PipelineEqualPushdown
**Scenario:** Pipeline `| level="error"` pushes an equality predicate into block-level pruning.
**Setup:** LogSelector with one StageLabelFilter stage, Op=OpEqual, Name="level", Value="error".
**Assertions:**
- `preds.UnscopedColumnNames["level"]` is non-nil and contains both `"log.level"` and `"resource.level"` (bloom-OR across both scopes).
- `preds.DedicatedColumns` and `preds.AttributesAccessed` are NOT populated (pipeline equal uses UnscopedColumnNames, not DedicatedColumns).

### LQP-TEST-121: TestExtractPredicates_PipelineRegexPushdown
**Scenario:** Pipeline `| level=~"err.*"` pushes a regex predicate into block-level pruning.
**Setup:** LogSelector with one StageLabelFilter stage, Op=OpRegex, Name="level", Value=`err.*`.
**Assertions:**
- `preds.UnscopedColumnNames["level"]` is non-nil and contains both `"log.level"` and `"resource.level"` (bloom-OR across both scopes).
- `preds.DedicatedColumnsRegex` and `preds.AttributesAccessed` are NOT populated.

### LQP-TEST-122: TestExtractPredicates_PipelineNotEqualSkipped
**Scenario:** Pipeline `| level!="error"` does NOT generate block-pruning predicates (negation is unsafe).
**Setup:** LogSelector with one StageLabelFilter stage, Op=OpNotEqual, Name="level", Value="error".
**Assertions:**
- Returned predicates are nil (negation-only pipeline yields no pushdown).

### LQP-TEST-123: TestExtractPredicates_PipelineNotRegexSkipped
**Scenario:** Pipeline `| level!~"err.*"` does NOT generate block-pruning predicates.
**Setup:** LogSelector with one StageLabelFilter stage, Op=OpNotRegex.
**Assertions:**
- Returned predicates are nil.

### LQP-TEST-124: TestExtractPredicates_PipelineNumericRangePushdown
**Scenario:** Pipeline `| duration>100` pushes a range predicate into DedicatedRanges.
**Setup:** LogSelector with one StageLabelFilter stage, Op=OpGT, Name="duration", Value="100".
**Assertions:**
- `preds.DedicatedRanges["log.duration"]` is non-nil.
- `rp.MinValue.Data` == "100".
- `rp.MinInclusive` == false (exclusive GT).
- `preds.AttributesAccessed` is NOT populated (range ops write to DedicatedRanges only).

### LQP-TEST-125: TestExtractPredicates_MixedSelectorAndPipeline
**Scenario:** Stream selector matchers and pipeline label filters both contribute predicates.
**Setup:** LogSelector with one MatchEqual matcher (env=prod) and one pipeline StageLabelFilter (level=error).
**Assertions:**
- `preds.DedicatedColumns["resource.env"]` is non-nil (from selector).
- `preds.UnscopedColumnNames["level"]` is non-nil and contains both `"log.level"` and `"resource.level"` (from pipeline, bloom-OR).

### LQP-TEST-126: TestExtractPredicates_PipelineLTEPushdown
**Scenario:** Pipeline `| count<=50` pushes a range predicate into DedicatedRanges with MaxInclusive=true.
**Setup:** LogSelector with one StageLabelFilter stage, Op=OpLTE, Name="count", Value="50".
**Assertions:**
- `preds.DedicatedRanges["log.count"]` is non-nil.
- `rp.MaxValue.Data` == "50".
- `rp.MaxInclusive` == true (inclusive LTE).

### LQP-TEST-127: TestJSONStage_SkipsPrePopulatedKeys
**Scenario:** JSONStage does not overwrite labels already present in the labels map (ingest wins).
**Setup:** labels map pre-populated with `{"level": "stored-at-ingest"}`; JSON body contains `{"level":"from-pipeline","msg":"hello"}`.
**Assertions:**
- `labels["level"]` == "stored-at-ingest" (pre-populated value preserved).
- `labels["msg"]` == "hello" (new key from body is extracted normally).

### LQP-TEST-128: TestLogfmtStage_SkipsPrePopulatedKeys
**Scenario:** LogfmtStage does not overwrite labels already present in the labels map.
**Setup:** labels pre-populated with `{"level": "stored-at-ingest"}`; logfmt body is `level=from-pipeline msg=timeout`.
**Assertions:**
- `labels["level"]` == "stored-at-ingest".
- `labels["msg"]` == "timeout".

### LQP-TEST-129: TestJSONStage_EmptyLabels_ExtractsAll
**Scenario:** JSONStage with an empty labels map extracts all JSON fields normally.
**Setup:** Empty labels map; JSON body `{"level":"warn","count":99}`.
**Assertions:**
- `labels["level"]` == "warn".
- `labels["count"]` == "99".

### LQP-TEST-130: TestLogfmtStage_EmptyLabels_ExtractsAll
**Scenario:** LogfmtStage with an empty labels map extracts all logfmt fields normally.
**Setup:** Empty labels map; logfmt body `level=error service=payments`.
**Assertions:**
- `labels["level"]` == "error".
- `labels["service"]` == "payments".

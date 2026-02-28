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

- All public functions (`New`, `Execute`) must be exercised.
- Both the empty-file short-circuit and the multi-block scan path must be covered.
- The `Options.Limit` early-exit path must be exercised (EX-08).
- `SpanMatch` field population must be verified (EX-09).

---

## Integration Tests in blockio Package
*Added: 2026-02-25*

EX-01 through EX-07 are also exercised as integration tests in
`internal/modules/blockio/executor_test.go`. Those tests use the same TraceQL queries
and assertions but build trace data with `modules_blockio.Writer` (the full write path)
rather than relying solely on in-package writer helpers. This provides a full round-trip
validation: write → flush → read → query → assert.

EX-08 (Limit), EX-09 (SpanMatch fields), and EX-10 (Plan populated) are specific to
`internal/modules/executor/executor_test.go` and are not duplicated in the blockio
integration tests.

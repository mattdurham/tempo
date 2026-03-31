# queryplanner — Test Specifications

This document defines the required tests for the `queryplanner` package. Each test is
described with its scenario, preconditions, steps, and expected outcomes. All tests use a
round-trip write (via `modules_blockio.Writer`) and read (via `reader.Reader`) to produce
a real `BlockIndexer`, ensuring the planner is tested against production index structures.

---

## 1. Plan — No Predicates

### QP-T-01: TestPlanNoPredicates

**Scenario:** A non-empty file with no query predicates should return all blocks.

**Setup:**
- Write 20 spans with `MaxBlockSpans=5` (produces 4 blocks).
- Call `Plan(nil)`.

**Assertions:**
- `plan.TotalBlocks == reader.BlockCount()`.
- `len(plan.SelectedBlocks) == plan.TotalBlocks`.
- `plan.PrunedByIndex == 0`.
- `plan.SelectedBlocks` is sorted in ascending order.

---

### QP-T-02: TestPlanEmptyFile

**Scenario:** An empty file returns an empty plan regardless of predicates.

**Setup:**
- Write 0 spans. Flush.
- Call `Plan` with a non-trivial predicate.

**Assertions:**
- `plan.TotalBlocks == 0`.
- `plan.SelectedBlocks` is empty.

---

## 2. Plan — Range-Index Pruning (formerly §3)

### QP-T-06: TestPlanRangePruning

**Scenario:** Range-index lookup eliminates blocks whose range bucket does not cover the
queried value.

**Setup:**
- 3 blocks, each with 5 spans, with distinct service names: `"svc-alpha"`, `"svc-beta"`,
  `"svc-gamma"` (one service per block).
- If the reader does not have a dedicated index for `resource.service.name`, skip the test.
- Call `Plan` with `Predicate{Columns: ["resource.service.name"], Values: ["svc-alpha"],
  ColType: shared.ColumnTypeString}`.

**Assertions:**
- `plan.SelectedBlocks` is non-empty.
- `plan.PrunedByIndex > 0`.
- `len(plan.SelectedBlocks) < plan.TotalBlocks`.

---

### QP-T-07: TestPlanRangePruningNoIndex

**Scenario:** When a queried column has no dedicated index, `PrunedByIndex` remains 0.

**Setup:**
- Write 10 spans.
- Call `Plan` with `Predicate{Columns: ["span.nonexistent.col"], Values: ["anything"],
  ColType: shared.ColumnTypeString}`.

**Assertions:**
- `plan.PrunedByIndex == 0`.

---

## 4. FetchBlocks

### QP-T-04: TestFetchBlocksReturnsValidBytes

**Scenario:** `FetchBlocks` returns byte slices that parse cleanly with
`reader.ParseBlockFromBytes`.

**Setup:**
- Write 20 spans with `MaxBlockSpans=5`.
- Call `Plan(nil)` then `FetchBlocks`.

**Assertions:**
- `len(rawBlocks) == len(plan.SelectedBlocks)`.
- Each raw byte slice parses into a `BlockWithBytes` with `Block.SpanCount() > 0`.

---

## 5. Coverage Requirements

- Package statement coverage: ≥ 80%.
- All public functions (`NewPlanner`, `Plan`, `FetchBlocks`) must be exercised.
- The range pruning path must be exercised in at least one test (QP-T-06).
- The empty-file short-circuit must be exercised (QP-T-02).

---

## 6. Removed Tests (Bloom Filter Pruning)
*Updated: 2026-03-07*

The following tests were removed when the column-name bloom filter was removed
(NOTES.md §NOTE-QP-BLOOM-REMOVAL):

- **QP-T-03: TestPlanBloomPruning** — column absent from some blocks eliminated by bloom.
  No replacement needed: column presence is no longer a pruning criterion. Value-level
  pruning is covered by the CMS/Fuse8 sketch index (future Phase 5).
- **QP-T-05: TestPlanORBloomPruning** — OR bloom predicate retains blocks with any column.
  No replacement needed for the same reason.

Range tests (QP-T-06, QP-T-07) remain the primary pruning verification path.

## 7. Direction Tests

### QP-T-08: TestPlanWithOptions_ForwardOrder
5 blocks, PlanWithOptions with Forward.
**Assertions:** SelectedBlocks == [0,1,2,3,4]. Direction == Forward. Limit == 0.

### QP-T-09: TestPlanWithOptions_BackwardOrder
5 blocks, PlanWithOptions with Backward and Limit=3.
**Assertions:** SelectedBlocks == [4,3,2,1,0]. Direction == Backward. Limit == 3.

### QP-T-10: TestPlanWithOptions_BackwardSingleBlock
1 block, PlanWithOptions with Backward.
**Assertions:** SelectedBlocks == [0]. No panic.

### QP-T-11: TestPlanWithOptions_BackwardEmpty
0 blocks (empty file), PlanWithOptions with Backward.
**Assertions:** SelectedBlocks is empty. Direction == Backward.

## 8. Interval Range Pruning

### QP-T-12: TestPlanRangeIntervalPruning
3 blocks with distinct service names (svc-alpha, svc-beta, svc-gamma).
IntervalMatch predicate with Values ["svc-a", "svc-b"].
NOTE-011: interval matching for case-insensitive regex prefix lookups.

**Assertions:** At least one block selected. PrunedByIndex > 0 (svc-gamma block pruned).

---

## 9. Time-Range Pruning Tests

### QP-T-13: TestPlan_TimeRange_PrunesOutOfWindowBlocks (TP-01)

**Scenario:** Time-range pruning eliminates blocks outside the query window.

**Setup:** 3 log-file blocks with `MinStart = MaxStart` at 150, 550, 950 ns respectively.
`TimeRange{MinNano: 450, MaxNano: 650}`.

**Assertions:** `plan.SelectedBlocks == [1]` (only block 1 at ts=550 survives).
`plan.PrunedByTime == 2`.

Back-ref: `planner_log_test.go:TestPlan_TimeRange_PrunesOutOfWindowBlocks`

---

### QP-T-14: TestPlan_TimeRange_ZeroDisablesPruning (TP-02)

**Scenario:** A zero `TimeRange{}` disables time-range pruning — all blocks are selected.

**Setup:** 3 log-file blocks. `TimeRange{}` (zero value).

**Assertions:** `len(plan.SelectedBlocks) == 3`. `plan.PrunedByTime == 0`.

Back-ref: `planner_log_test.go:TestPlan_TimeRange_ZeroDisablesPruning`

---

### QP-T-15: TestPlan_TimeRange_NoOverlap_Empty (TP-03)

**Scenario:** When the time range is beyond all blocks, no blocks are selected.

**Setup:** 3 log-file blocks at timestamps 150, 550, 950. `TimeRange{MinNano: 2000, MaxNano: 3000}`.

**Assertions:** `plan.SelectedBlocks` is empty. `plan.PrunedByTime == 3`.

Back-ref: `planner_log_test.go:TestPlan_TimeRange_NoOverlap_Empty`

---

### QP-T-16: TestPlan_TimeRange_ZeroMaxNano_OpenUpperBound (TP-04)

**Scenario:** `MaxNano == 0` means no upper bound — blocks at or after `MinNano` survive.

**Setup:** 3 log-file blocks at timestamps 150, 550, 950. `TimeRange{MinNano: 450, MaxNano: 0}`.

**Assertions:** `plan.SelectedBlocks == [1, 2]`. `plan.PrunedByTime == 1`.

Back-ref: `planner_log_test.go:TestPlan_TimeRange_ZeroMaxNano_OpenUpperBound`

---

## 10. Sketch-Based Pruning Tests

### QP-T-17: TestPlanCMSPruning

**Scenario:** CMS sketch data is present; block containing only service-B has estimate==0
for service-A; combined pruning (range+fuse+CMS) selects only block 0.

**Setup:**
- Two-block file: block 0 has 5 spans with service.name="service-A", block 1 has 5 spans
  with service.name="service-B".
- Verify via `r.ColumnSketch(col).CMSEstimate("service-A")` that block 1 estimate==0.
- Call `Plan` with `Predicate{Columns:["resource.service.name"], Values:["service-A"]}`.

**Assertions:**
- `cs.CMSEstimate("service-A")[0] > 0` (block 0 has service-A).
- `cs.CMSEstimate("service-A")[1] == 0` (block 1 definitely doesn't have service-A).
- `plan.SelectedBlocks == [0]`.
- `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS > 0`.

Back-ref: `scoring_test.go:TestPlanCMSPruning`

---

### QP-T-18: TestPlanCMSNoPruneForFalsePositive

**Scenario:** A block containing the queried value is never pruned (no false negatives).

**Setup:** Same two-block file. Predicate for "service-A".

**Assertions:** Block 0 (containing service-A) is always in `plan.SelectedBlocks`.

Back-ref: `scoring_test.go:TestPlanCMSNoPruneForFalsePositive`

---

### QP-T-19: TestPlanScoring

**Scenario:** Plan succeeds and `BlockScores` is populated with sketch data.

**Setup:** Same two-block file. Predicate for "service-A".

**Assertions:**
- `plan != nil`.
- If `len(plan.SelectedBlocks) > 0` and `len(plan.BlockScores) > 0`: all surviving blocks
  have a score in `BlockScores`.

Back-ref: `scoring_test.go:TestPlanScoring`

---

### QP-T-24: TestPlanFusePruning

**Scenario:** BinaryFuse8 filter data is correct per SPEC-SK-12 (no false negatives).

**Setup:** Two-block file: block 0 = service-A, block 1 = service-B.
- Verify `cs.FuseContains(HashForFuse("service-A"))` and `FuseContains(HashForFuse("service-B"))`.

**Assertions:**
- `fuseA[0] == true` (block 0 contains service-A — no false negatives).
- `fuseB[1] == true` (block 1 contains service-B — no false negatives).
- `fuseA[1] == false` (block 1 does not contain service-A — fuse filter correct).
- `plan.SelectedBlocks == [0]` after combined pruning.

Back-ref: `scoring_test.go:TestPlanFusePruning`

---

### QP-T-25: TestPlanFuseNoPruneForMember

**Scenario:** A block containing the queried value is never pruned by fuse (no false negatives).

**Setup:** Same two-block file. Predicate for "service-A".

**Assertions:** Block 0 is always in `plan.SelectedBlocks`.

Back-ref: `scoring_test.go:TestPlanFuseNoPruneForMember`

---

### QP-T-26: TestBlockTopK

**Scenario:** `ColumnSketch.TopKMatch` returns correct per-block counts.

**Setup:** Two-block file: block 0 = 5×service-A, block 1 = 5×service-B.

**Assertions:**
- `cs.TopKMatch(HashForFuse("service-A"))[0] >= 5` (service-A in top-K for block 0).
- `cs.TopKMatch(HashForFuse("service-A"))[1] == 0` (service-A not in block 1 top-K).
- `cs.TopKMatch(HashForFuse("service-B"))[1] >= 5` (service-B in top-K for block 1).

Back-ref: `scoring_test.go:TestBlockTopK`

---

### QP-T-27: TestColumnMajorRoundTrip

**Scenario:** Column-major sketch data (HLL, CMS, Fuse, TopK) survives a full
write→flush→parse round-trip without data loss or corruption.

**Setup:** Two-block file with distinct service names. Call `r.ColumnSketch(col)` and
verify `CMSEstimate`, `FuseContains`, `TopKMatch`, and `Distinct` return non-zero/correct
values.

**Assertions:** Sketch data is non-nil. At least one column has a non-zero CMS estimate.
FuseContains returns true for a present value.

Back-ref: `scoring_test.go:TestColumnMajorRoundTrip`

---

## 11. blockSet Unit Tests

The `blockset_test.go` file tests the `blockSet` type in isolation:
- `TestBlockSet_Basic`: set/test/clear/count on small N.
- `TestBlockSet_And`: AND operation reduces candidate set.
- `TestBlockSet_Or`: OR operation expands candidate set.
- `TestBlockSet_Iter`: iter visits all set bits in order.
- `TestBlockSet_LargeBitset`: N > 64 (crosses word boundary).
- `TestBlockSet_Count`: popcount correctness.
- `TestBlockSet_NumBlocks`: capacity calculation.

Back-ref: `blockset_test.go`

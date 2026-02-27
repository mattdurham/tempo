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
- `plan.PrunedByBloom == 0`.
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

## 2. Plan — Bloom Filter Pruning

### QP-T-03: TestPlanBloomPruning

**Scenario:** A column absent from some blocks is eliminated by the bloom filter.

**Setup:**
- Block 0 (spans 0–4): spans have `"rare.attr"` attribute.
- Block 1 (spans 5–9): spans do not have `"rare.attr"`.
- Call `Plan` with predicate `Columns=["resource.rare.attr"]`.

**Assertions:**
- `plan.TotalBlocks == 2`.
- `len(plan.SelectedBlocks) < plan.TotalBlocks` (block 1 pruned).
- `plan.PrunedByBloom > 0`.

---

### QP-T-05: TestPlanORBloomPruning

**Scenario:** An OR bloom predicate (multiple columns) prunes only blocks that lack ALL
listed columns; blocks with any one column are retained.

**Setup:**
- Block 0: has `"custom.name"` only.
- Block 1: has `"custom.foo"` only.
- Block 2: has neither column.
- Call `Plan` with predicate `Columns=["resource.custom.name", "resource.custom.foo"]`.

**Assertions:**
- `plan.TotalBlocks == 3`.
- `plan.PrunedByBloom == 1` (only block 2 pruned).
- `len(plan.SelectedBlocks) == 2`.

---

## 3. Plan — Range-Index Pruning

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
- The bloom pruning path must be exercised in at least one test (QP-T-03).
- The range pruning path must be exercised in at least one test (QP-T-06).
- The empty-file short-circuit must be exercised (QP-T-02).

---

## 6. Previously Removed Tests (Now Re-added)

The following tests were removed when the dedicated column index was removed (NOTES.md §7)
and have since been re-added after the index was restored (NOTES.md §8):

- **QP-T-06: TestPlanRangePruning** — verifies dedicated index prunes irrelevant blocks
  for a service-name equality predicate. Previously QP-T-04 (old).
- **QP-T-07: TestPlanRangePruningNoIndex** — verifies that a column absent from the
  dedicated index is safely skipped with `PrunedByIndex == 0`.
  Previously QP-T-08 (old).

The following tests from the original index design were not re-added as they are covered
by the bloom OR test (QP-T-05) and range tests above:
- **QP-T-05 (old): TestPlanANDPredicates** — AND semantics via mutually exclusive predicates.
- **QP-T-06 (old): TestPlanORValues** — OR-value semantics within a single Predicate.

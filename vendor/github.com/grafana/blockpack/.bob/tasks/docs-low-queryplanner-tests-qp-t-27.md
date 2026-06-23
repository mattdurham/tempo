# Fix queryplanner/TESTS.md: Add missing QP-T-27 TestColumnMajorRoundTrip entry

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** LOW
**Source:** discover-docs (round 2)

## Problem

`TestColumnMajorRoundTrip` exists in `internal/modules/queryplanner/scoring_test.go:273`
with the comment `// QP-T-27: TestColumnMajorRoundTrip` at line 271. It verifies that
column-major sketch data survives a write+read cycle.

`internal/modules/queryplanner/TESTS.md` §10 documents QP-T-17 through QP-T-26 but
has no entry for QP-T-27.

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/queryplanner/TESTS.md` §10 (Sketch-Based Pruning Tests), add:

```
### QP-T-27: TestColumnMajorRoundTrip

**Scenario:** Column-major sketch data (HLL, CMS, Fuse, TopK) survives a full
write→flush→parse round-trip without data loss or corruption.

**Setup:** Two-block file with distinct service names. Call `r.ColumnSketch(col)` and
verify `CMSEstimate`, `FuseContains`, `TopKMatch`, and `Distinct` return non-zero/correct
values.

**Assertions:** Sketch data is non-nil. At least one column has a non-zero CMS estimate.
FuseContains returns true for a present value.

Back-ref: `scoring_test.go:TestColumnMajorRoundTrip`
```

## Acceptance criteria
- TESTS.md §10 has a QP-T-27 entry describing TestColumnMajorRoundTrip.

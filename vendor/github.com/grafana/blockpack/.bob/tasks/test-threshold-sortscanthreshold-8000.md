# Test: SortScanThreshold boundary (8000 refs)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

Code branches at `len(refs) < SortScanThreshold` (8000) between the KLL and scan paths. Tests exist for the small path (<8000) and artificially force the scan path by setting SortScanThreshold=1-2, but no test exercises the natural boundary at exactly 8000 refs or datasets that produce 8000+ matching refs without adjusting the threshold.

## Risk

Edge case where ref count hovers near the threshold could execute the wrong code path without being caught, producing incorrect results or poor performance.

## Location

File: `internal/modules/executor/stream.go:625-657`
Function: `collectIntrinsicTopK`

## Test Design

Create a blockpack file with exactly 8000+ matching refs for an intrinsic predicate (e.g., repeated spans with `resource.service.name = "target"`). Run a Collect query that naturally triggers the scan path without modifying SortScanThreshold. Assert that execution path is "intrinsic-topk-scan" and results are correct.

## Notes

Must build test data with enough spans to produce 8000+ matching refs. Can set up small threshold override to force path if needed, but should also test natural boundary. Compare results between KLL and scan paths to ensure equivalence.

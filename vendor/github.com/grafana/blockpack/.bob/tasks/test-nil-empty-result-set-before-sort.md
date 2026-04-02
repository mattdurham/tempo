# Test: Empty result set before sort

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

No test exercises the path where matching refs=0. Query result is empty (no intrinsic refs match, bloom filter filters all blocks). Sorting/deduplication logic with empty slice is untested.

## Risk

Sorting/deduplication logic might panic on empty slice.

## Location

File: `internal/modules/executor/stream.go:779-785`
Function: `collectIntrinsicTopKKLL`

## Test Design

Query with predicate that matches zero rows (e.g., `{ resource.service.name = "impossible-service" }`). Verify empty result returned, not panic.

## Notes

Test multiple zero-result scenarios: bloom filter rejects all blocks, predicate matches nothing, time range excludes all rows.

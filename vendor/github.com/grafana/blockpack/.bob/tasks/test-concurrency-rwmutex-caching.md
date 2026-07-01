# Test: RWMutex lock contention (RangeCachingProvider)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

RangeCachingProvider uses RWMutex for cache access but tests use single goroutine only. No test for multiple concurrent ReadAt calls, cache contention, or RLock downgrade edge cases.

## Risk

Race condition or deadlock under concurrent load (multiple goroutines reading same file).

## Location

File: `internal/modules/rw/caching.go:45-76`
Function: `ReadAt`

## Test Design

Use `go test -race`. Spawn 10 goroutines calling ReadAt concurrently on same file with overlapping ranges. Verify no races detected and results are correct.

## Notes

Run with race detector enabled. Verify cache is thread-safe and contention is handled correctly. Check for deadlocks or data races.

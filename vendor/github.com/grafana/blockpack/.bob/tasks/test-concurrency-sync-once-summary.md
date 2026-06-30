# Test: Lazy parsing with sync.Once (fileSummary)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

FileSketchSummary is computed once and cached via sync.Once, but no concurrent access test. No test calling GetFileSummary from multiple goroutines.

## Risk

Concurrent init could compute summary twice or panic if not properly synchronized.

## Location

File: `internal/modules/blockio/reader/reader.go:94`
Function: `GetFileSummary`, lazy initialization

## Test Design

Call GetFileSummary from 100 concurrent goroutines. Verify result is identical across all calls and computed only once (add debug counter to verify).

## Notes

Use sync.Once correctly to ensure single computation. Verify all goroutines get same result. Can add instrumentation to count actual computations.

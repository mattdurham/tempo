# Test: Nil OnStats callback

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

OnStats is checked for nil in stream.go lines 163-165, but no test calls Collect with OnStats=nil or with valid OnStats to verify callback behavior.

## Risk

Callback invoked even when nil, or not invoked when provided.

## Location

File: `internal/modules/executor/stream.go:163-165`
Function: `Collect`

## Test Design

Call Collect with OnStats=nil. Verify succeeds. Call with valid OnStats callback and verify it's invoked exactly once with expected stats.

## Notes

Use a counter or flag to verify callback is called. Test both nil and valid cases.

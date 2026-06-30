# Test: ParseBlockFromBytes first-pass decode failure

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

Parse errors are handled in stream.go lines 316-318, but no test corrupts block bytes to trigger parse failure (e.g., truncated block, bad column header). Error path is untested.

## Risk

Silent data corruption or panic if error handling is incomplete.

## Location

File: `internal/modules/executor/stream.go:316-318`
Function: `fetchBlocksByRange` or parse path

## Test Design

Write a valid block, then truncate its bytes to 10 bytes (corrupting the header and columns). Attempt ParseBlockFromBytes. Verify error is returned with clear message and no panic occurs.

## Notes

Manually corrupt blockpack bytes by truncating. Test multiple corruption points (header, column header, column data). Verify error messages are informative.

# Test: Negative or overflow shard parameters

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

Validation exists in stream.go lines 113-125 but no test verifies each boundary case actually returns the error. Tests verify the logic exists but not that it's enforced.

## Risk

Silent acceptance of invalid shard parameters leading to data loss or incorrect results.

## Location

File: `internal/modules/executor/stream.go:113-125`
Function: `Collect`

## Test Design

Call Collect with StartBlock=-1, BlockCount=-1, and overflow cases (StartBlock + BlockCount > maxint). Verify each returns expected error with clear message. Test both positive and negative bounds.

## Notes

Test error cases: negative StartBlock, negative BlockCount, overflow (start+count wraps), zero BlockCount (edge case). Verify error messages are specific to each condition.

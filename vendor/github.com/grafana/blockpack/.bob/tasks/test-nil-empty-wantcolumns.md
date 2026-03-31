# Test: Empty wantColumns (full column decode)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

wantColumns can be nil (decode all), but no test verifies performance/correctness at boundary. No test compares wantColumns=nil vs empty map to verify they're equivalent.

## Risk

Incorrect column filtering if empty map is treated differently than nil.

## Location

File: `internal/modules/executor/stream.go:128`
Function: `Collect` or stream processing

## Test Design

Call Collect with program where wantColumns=nil and again with empty map. Verify both return identical results with same performance characteristics.

## Notes

Compare results and execution statistics between the two cases. Verify no semantic difference between nil and {}.

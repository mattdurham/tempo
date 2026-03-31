# Test: Timestamp filtering boundary

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Time range filtering with minNano=0, maxNano=0 (unbounded) is tested, but no test exercises minNano=1, maxNano=0 or minNano=maxNano (point in time). Boundary cases are untested.

## Risk

Incorrect time filtering at boundaries (e.g., point-in-time query returns wrong rows, off-by-one in inclusive/exclusive bounds).

## Location

File: `internal/modules/executor/stream_log_topk.go:197-205`
Function: `filterRowsByTimeRange`

## Test Design

Query with minNano=maxNano and surrounding times. Verify exactly matching timestamps are returned, rows outside boundary are excluded.

## Notes

Test point-in-time [T, T], half-open ranges, and verify inclusive/exclusive semantics.

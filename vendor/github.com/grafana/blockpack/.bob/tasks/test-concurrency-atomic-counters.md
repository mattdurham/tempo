# Test: Atomic counter updates (TrackingProvider)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Atomics are used (ioOps, bytesRead) but tests don't verify counter values under concurrent load or off-by-one in increments.

## Risk

Metrics reported incorrectly at scale (e.g., ioOps off by 1 due to race).

## Location

File: `internal/modules/rw/tracking.go`
Function: `ReadAt`, counter updates

## Test Design

Concurrent ReadAt calls with known total bytes. Verify final ioOps and bytesRead match expected totals. Use race detector to verify no data races.

## Notes

Run with race detector. Total bytes and I/O ops should match expected values. Verify increment operations are correct.

# Test: ReadGroup I/O failure

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

ReadGroup errors are propagated in stream.go lines 295-297, but no test injects I/O failures at this point. Error handling logic is untested.

## Risk

Logic error in error handling (e.g., not cleaning up on partial reads, state leakage) could cause subsequent operations to fail silently or return corrupted data.

## Location

File: `internal/modules/executor/stream.go:295-297`
Function: `fetchBlocksByRange` or similar

## Test Design

Mock ReadGroup to return I/O error at block 2 of 3. Verify that partial results are not returned and error is properly propagated. Check no state leakage affects subsequent operations.

## Notes

Use mock/stub reader that fails on specific ReadGroup calls. Verify cleanup occurs (no orphaned block data left in fetched state). Test multiple failure points (first block, middle block, last block).

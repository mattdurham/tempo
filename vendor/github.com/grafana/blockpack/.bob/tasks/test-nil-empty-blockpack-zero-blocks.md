# Test: Empty blockpack (zero blocks)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

Code assumes at least one block in many places, but no test creates a blockpack with zero blocks. BlockCount() returns 0, but no test exercises this state.

## Risk

Panic or incorrect behavior when BlockCount() returns 0.

## Location

File: `internal/modules/blockio/reader/reader.go:203`
Function: `BlockCount`

## Test Design

Write a blockpack header/footer with blockMetadata=[] (zero blocks). Attempt to query. Verify graceful handling with no panic. Return empty result set or proper error.

## Notes

Manually construct blockpack footer with zero block metadata entries. Attempt Collect query. Verify no crash and reasonable error/empty result.

# Test: Trace index size limits (50 MiB)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** LOW
**source:** coverage-gaps

## Gap

Compact trace index is capped at 50 MiB. `MaxCompactSectionSize = 52_428_800` in constants.go, but no test creates a file with compact index near 50 MiB. No verification of behavior at overflow.

## Risk

Corrupted trace index or silent truncation if exceeding limit, making index lookup fail or return wrong results.

## Location

File: `internal/modules/blockio/shared/constants.go:94`
Function: `MaxCompactSectionSize`

## Test Design

Create a blockpack file with ~50M traces (or many blocks with large trace indices) to push compact index size near the limit. Verify it compresses under 50 MiB or errors gracefully. Check that index remains functional if near/at boundary.

## Notes

This test will be resource-intensive. May require synthetic data generation or stress testing. Focus on graceful degradation if limit is exceeded.

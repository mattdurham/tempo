# Test: Max intrinsic rows safety cap (10M)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

If total intrinsic rows exceed 10M, the section is written empty (safety cap). `MaxIntrinsicRows = 10_000_000` in constants.go, but no test reaches or exceeds this threshold. No verification that exceeding it doesn't crash or cause silent data loss.

## Risk

Silent data loss at 10M rows — intrinsic columns (trace ID, service name, etc.) vanish without error message, leading to corrupted query results.

## Location

File: `internal/modules/blockio/shared/constants.go:79`
Function: `MaxIntrinsicRows`

## Test Design

Accumulate intrinsic refs just below (9.9M) and above (10.1M) limits. Verify file is written correctly. Assert that intrinsic section is empty when limit exceeded and no panic occurs. Check file integrity post-write.

## Notes

This test will be resource-intensive. May require generating synthetic test data or mocking to reach 10M rows efficiently. Verify file integrity and error handling.

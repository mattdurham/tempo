# Test: Intrinsic page size threshold (10,000 rows)

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Columns with >10K rows use paged (v2) format. `IntrinsicPageSize = 10_000` in constants.go, but no explicit test creates a column with exactly 9,999, 10,000, or 10,001 rows to verify format version byte encoding (0x01 flat vs 0x02 paged).

## Risk

Columns at the boundary might serialize to wrong format version, breaking readers expecting flat vs paged format.

## Location

File: `internal/modules/blockio/shared/constants.go:70`
Function: `IntrinsicPageSize`

## Test Design

Write intrinsic columns with 9,999, 10,000, and 10,001 rows respectively. Inspect the serialized bytes for the format version byte (0x01 vs 0x02). Assert that flat format is used for <=10K and paged format for >10K.

## Notes

Requires building test data with exact row counts. Examine binary format to locate and verify version byte. Consider using high-volume span generation to reach row counts efficiently.

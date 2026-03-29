# Test: Slice indexing in deduplicate paths

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

Deduplication uses map/set logic but no test for first row in deduplicated set, last row, or off-by-one in row count.

## Risk

Duplicate rows leaked or rows dropped from result.

## Location

File: `internal/modules/executor/stream.go` (rowSet deduplication via rowset.go)
Function: Deduplication logic

## Test Design

Query that produces duplicate rows (e.g., same trace ID across multiple blocks). Verify each row appears exactly once. Test both first and last rows in result.

## Notes

Use queries that naturally produce duplicates across blocks. Inspect result for exact count and no repeats.

# Test: Zero-span block

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** MEDIUM
**source:** coverage-gaps

## Gap

No test creates a block with SpanCount=0. Code assumes blocks have spans in range index lookups and other places.

## Risk

Division by zero or array access panic when processing empty block.

## Location

File: `internal/modules/blockio/writer/writer.go` (implicit in block structure)
Function: Block writing/reading logic

## Test Design

Manually construct a block with SpanCount=0. Add to blockpack. Query it. Verify no crash and graceful handling.

## Notes

Requires low-level blockpack construction or mocking. Test range index lookups with zero-span block.

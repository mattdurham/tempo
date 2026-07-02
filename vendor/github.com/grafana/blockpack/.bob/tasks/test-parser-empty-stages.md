# Test: Empty pipeline stages

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** LOW
**source:** coverage-gaps

## Gap

No test for empty stage (`|` alone), multiple consecutive pipes (`||`), pipe at end of query, or stage with no arguments.

## Risk

Parser panics or silently drops stage.

## Location

File: `internal/logqlparser/ast.go`
Function: Pipeline parsing

## Test Design

Parse query strings with edge-case pipe placements. Verify error handling or correct parsing. Test all malformed cases.

## Notes

Table-driven test with various pipe placements. Verify parser either rejects gracefully or handles correctly.

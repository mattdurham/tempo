# Test: ColumnPredicate evaluation failure

**Status:** Pending
**task_type:** test-coverage
**cleanup_type:** test-coverage
**severity:** HIGH
**source:** coverage-gaps

## Gap

Predicate evaluation errors are caught in stream.go lines 322-324, but no test creates a column that causes evaluation to fail (e.g., column not found, type mismatch). Error path is untested.

## Risk

If ProgramWantColumns omits a column accessed by the predicate, evaluation fails silently or panics.

## Location

File: `internal/modules/executor/stream.go:322-324`
Function: `fetchBlocksByRange` or predicate evaluation

## Test Design

Write a query like `{ resource.missing_col = "x" }` on a blockpack file that doesn't have that column. Verify ColumnPredicate evaluation returns proper error. Also test type mismatch (query expects string, column is numeric).

## Notes

Test both missing column and type mismatch cases. Verify error message is clear and includes column name.

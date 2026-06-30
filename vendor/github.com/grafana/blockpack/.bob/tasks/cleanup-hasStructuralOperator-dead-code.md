# Cleanup: Remove dead code in hasStructuralOperator and unify with findStructuralOperator

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/traceqlparser/parser.go:70-131`

## Issue

`hasStructuralOperator` (cyclomatic complexity 40) and `findStructuralOperator` (cyclomatic complexity 31) contain near-identical scan loops over the same query string. `hasStructuralOperator` contains dead code at lines 112-118: the k-advance logic that skips the second character of multi-char operators (`>>`, `<<`, `!~`) is unreachable because those operators are already caught by the two-character check at lines 94-98 before the single-char path is ever reached.

Additionally, `hasStructuralOperator` can be simplified to a one-liner by delegating to `findStructuralOperator`, eliminating the entire 60-line duplicate scan and dropping the cyclomatic complexity of this file substantially.

## Fix

1. Remove the dead k-advance block (lines 112-118) from `hasStructuralOperator`.
2. Replace the entire body of `hasStructuralOperator` with:
   ```go
   func hasStructuralOperator(query string) bool {
       pos, _ := findStructuralOperator(query)
       return pos >= 0
   }
   ```
   This removes ~55 lines of duplicated scan logic and drops `hasStructuralOperator` from cyclo 40 to cyclo 1.

**Complexity reduction:** hasStructuralOperator: 40 → 1. Net savings: ~55 lines.

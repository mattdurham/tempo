# Cleanup: Extract inline whitespace-skip loops in traceqlparser/parser.go into a helper

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/traceqlparser/parser.go:105,119,168,174`

## Issue

The function-level parser (`hasStructuralOperator` / `findStructuralOperator`) contains four inline backward/forward whitespace-skip loops:

```go
// backward skip (appears twice)
for j >= 0 && (query[j] == ' ' || query[j] == '\t' || query[j] == '\n') {
    j--
}
// forward skip (appears twice)
for k < len(query) && (query[k] == ' ' || query[k] == '\t' || query[k] == '\n') {
    k++
}
```

These are repeated literal patterns. The struct-based parser already has a `skipWhitespace()` method (line 1294), but the function-level structural operator scanner operates on raw string indices and cannot use it.

## Fix

Add two package-level helper functions:

```go
// skipWSBack returns the index of the last non-whitespace character at or before pos.
func skipWSBack(s string, pos int) int {
    for pos >= 0 && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n') {
        pos--
    }
    return pos
}

// skipWSFwd returns the index of the first non-whitespace character at or after pos.
func skipWSFwd(s string, pos int) int {
    for pos < len(s) && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n') {
        pos++
    }
    return pos
}
```

Replace the four inline loops with calls to these helpers. This also slightly reduces the complexity of `findStructuralOperator` (cyclo 31 → ~27).

**Note:** This task is lower priority if `hasStructuralOperator` is simplified to a one-liner (cleanup-hasStructuralOperator-dead-code task), as only `findStructuralOperator` would retain these loops.

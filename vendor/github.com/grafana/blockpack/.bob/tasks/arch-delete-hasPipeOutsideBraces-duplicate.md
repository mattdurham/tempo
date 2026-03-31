# Cleanup: Delete hasPipeOutsideBraces — duplicate of findPipeOutsideBraces

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**severity:** MEDIUM
**source:** discover-arch

## Location

`internal/traceqlparser/parser.go:38-64`

## What to Delete

`hasPipeOutsideBraces` (lines 38-64, ~26 lines) is a character-for-character copy of `findPipeOutsideBraces` (lines 262-288). The only difference is the return type and final return value: `return true` vs `return i`. The function can be replaced entirely by:

```go
findPipeOutsideBraces(query) >= 0
```

**Steps:**

1. Delete the `hasPipeOutsideBraces` function (lines 38-64 in parser.go).
2. Change the call site in `ParseTraceQL` (line 23):
   ```go
   // Before:
   if hasPipeOutsideBraces(query) {
   // After:
   if findPipeOutsideBraces(query) >= 0 {
   ```

That is the entire change.

## Why

Duplicate logic with an identical algorithm. When the pipe-parsing logic needs to change (e.g. to handle backtick strings or nested braces), the change would need to be made in two places. The 2-3 Rule: one consumer for a helper that duplicates another function is unjustified. Both functions share identical comments about `||` skipping.

## Acceptance Criteria

- `hasPipeOutsideBraces` is deleted
- `ParseTraceQL` now calls `findPipeOutsideBraces(query) >= 0`
- All parser tests pass (`go test ./internal/traceqlparser/...`)
- No new functionality introduced

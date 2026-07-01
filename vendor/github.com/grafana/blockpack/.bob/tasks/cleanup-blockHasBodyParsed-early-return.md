# Cleanup: Simplify blockHasBodyParsed using slices.ContainsFunc or direct map lookup

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/modules/executor/stream_log.go:199-213`

## Issue

`blockHasBodyParsed` iterates `block.Columns()` to find the first column matching a predicate. This is a linear scan that could be expressed more clearly:

```go
func blockHasBodyParsed(block *modules_reader.Block) bool {
    for key := range block.Columns() {
        if strings.HasPrefix(key.Name, "log.") && key.Type == modules_shared.ColumnTypeRangeString {
            return true
        }
    }
    return false
}
```

While functionally correct, the `for range` with an explicit `return true` inside is idiomatic Go for "any" checks. However, this can be made more explicit using a named predicate that matches the function's documented contract, or by caching the result per block since `blockHasBodyParsed` is called once per block in both `StreamLogs` and `iterateLogRows`.

The more impactful cleanup is that `blockHasBodyParsed` is called once before the row loop and its result stored in `skipParsers` — this is already correct. No change needed for correctness.

The minor cleanup: use `maps.Keys` + `slices.ContainsFunc` is NOT idiomatic here (the for-range with early return is the correct Go pattern for "any" checks). The existing code is fine. This task is LOW priority — mark as acknowledged/no-change-needed.

## Fix

No code change needed. The existing pattern is idiomatic Go for a predicate-scan. Close this task as "already idiomatic."

**Priority:** LOW — documentation only.

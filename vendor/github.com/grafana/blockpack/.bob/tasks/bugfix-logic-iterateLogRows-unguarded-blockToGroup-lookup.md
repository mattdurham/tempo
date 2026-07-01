# Fix logic: iterateLogRows unguarded blockToGroup lookup in canSkipBlock branch

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** bug-fix
**severity:** LOW
**source:** bug-finder

## Location

`internal/modules/executor/stream_log_topk.go:267` — `iterateLogRows`

## Bug

In the `canSkipBlock` branch of `iterateLogRows`, the code reads `blockToGroup[blockIdx]`
without capturing the `ok` return value:

```go
if fetchedGroupsSeen[blockToGroup[blockIdx]] {
    delete(fetched, blockIdx)
} else {
    skippedBlocks[blockIdx] = true
}
```

If `blockIdx` is not in `blockToGroup` (missing key), Go returns `0` (the default int value),
causing the code to check whether group 0 was fetched. If group 0 happens to have been
fetched, `delete(fetched, blockIdx)` runs (a no-op for a block that was never fetched) and
`continue` executes, silently skipping the block without processing it.

The equivalent code in `topKScanBlocks` (`stream_topk.go:137-140`) correctly guards with `ok2`:

```go
gi2, ok2 := blockToGroup[blockIdx]
if ok2 && fetchedGroupsSeen[gi2] {
    delete(fetched, blockIdx)
} else {
    skippedBlocks[blockIdx] = true
}
```

## Trigger

`blockIdx` present in `plan.SelectedBlocks` but absent from `blockToGroup`. In practice both
are built from the same slice so the invariant holds, but the code is inconsistent with its
twin in `topKScanBlocks` and could silently misbehave if the invariant were ever violated.

## Impact

A block that should be processed is silently skipped, producing incomplete (wrong) query
results. No current production trigger.

## Fix

Replace the unguarded two-expression access with a two-value ok-guarded lookup, matching
the pattern in `topKScanBlocks`:

```go
gi2, ok2 := blockToGroup[blockIdx]
if ok2 && fetchedGroupsSeen[gi2] {
    delete(fetched, blockIdx)
} else {
    skippedBlocks[blockIdx] = true
}
```

## Acceptance Criteria

- The `iterateLogRows` canSkipBlock branch uses the same ok-guarded pattern as `topKScanBlocks`.
- All existing tests pass.
- No new functionality introduced.

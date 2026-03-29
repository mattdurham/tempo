# Cleanup: Unify NewColumnProvider vs newBlockColumnProvider inside executor package

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**severity:** LOW
**source:** discover-arch

## Location

`internal/modules/executor/` — multiple files

## What to Fix

Within the `executor` package, there are two names for the same constructor:

- `newBlockColumnProvider` (unexported, line 31 of `column_provider.go`) — used by `stream.go`, `stream_topk.go`, `stream_structural.go`
- `NewColumnProvider` (exported, line 782 of `column_provider.go`, one-liner wrapping `newBlockColumnProvider`) — used by `stream_log.go:137`, `stream_log_topk.go:308`, `metrics_trace.go:118`, `metrics_log.go:123`

The exported `NewColumnProvider` has no real external consumers at runtime. The only external reference is `cmd/deadcode/main.go:97` which uses it purely as a dead-code anchor (`_ = modules_executor.NewColumnProvider`).

**Steps:**

Change the 4 internal call sites that use `NewColumnProvider` to use `newBlockColumnProvider` directly:

- `stream_log.go:137`: `colProvider := NewColumnProvider(bwb.Block)` → `colProvider := newBlockColumnProvider(bwb.Block)`
- `stream_log_topk.go:308`: `rowSet, err := program.ColumnPredicate(NewColumnProvider(bwb.Block))` → use `newBlockColumnProvider`
- `metrics_trace.go:118`: same substitution
- `metrics_log.go:123`: same substitution

`NewColumnProvider` remains as the exported one-liner — no change to its definition. The dead-code anchor continues to work.

## Why

Using two names for the same operation inside one package creates confusion about whether there is a semantic difference. A reader unfamiliar with the codebase must inspect both functions to confirm they are identical. LOW severity because it is purely cosmetic, but the inconsistency is surprising.

## Acceptance Criteria

- All internal call sites use `newBlockColumnProvider`
- `NewColumnProvider` definition is unchanged (still exported, still one line)
- All tests pass (`make test`)
- No new functionality introduced

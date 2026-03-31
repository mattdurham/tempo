# Fix blockio/NOTES.md §18: Stale `modules_executor.New().Execute()` reference

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** MEDIUM
**Source:** discover-docs (round 2)

## Problem

`internal/modules/blockio/NOTES.md` §18 (line 385) says:

> It calls `modules_executor.New().Execute(r, program, modules_executor.Options{})` directly

Actual code in `internal/modules/blockio/executor_test.go:135`:
```go
rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
```

There is no `New()` constructor and no `Execute` method in the executor package.

Additionally, §18 (lines ~391-392) references `bloomPredicates` function:
> The `bloomPredicates` function in `internal/modules/executor` is exercised by every test

`bloomPredicates` does not exist. It was renamed to `BuildPredicates` and later the wrapper was removed. The actual predicate function called is `BuildPredicates` via `planBlocks`.

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/blockio/NOTES.md` §18:
1. Replace `` `modules_executor.New().Execute(r, program, modules_executor.Options{})` `` with `` `modules_executor.Collect(r, program, modules_executor.CollectOptions{})` ``.
2. Replace the `bloomPredicates` reference with `BuildPredicates` (or remove the specific function mention if it's no longer accurate).

## Acceptance criteria
- §18 says `modules_executor.Collect` not `modules_executor.New().Execute`.
- No reference to `bloomPredicates` remains.

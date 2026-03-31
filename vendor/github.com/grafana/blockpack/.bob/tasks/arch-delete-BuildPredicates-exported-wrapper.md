# Cleanup: Remove BuildPredicates exported wrapper — merge with buildPredicates

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**source:** discover-arch

## Location

`internal/modules/executor/predicates.go:185-188`

## Issue

`BuildPredicates` is an exported thin wrapper with zero production callers:

```go
// BuildPredicates is an exported thin wrapper around buildPredicates.
func BuildPredicates(r *modules_reader.Reader, program *vm.Program) []queryplanner.Predicate {
    return buildPredicates(r, program)
}
```

The only external reference is `_ = modules_executor.BuildPredicates` in `cmd/deadcode/main.go`, which is a function-value anchor (not a call). All production code calls `buildPredicates` directly within the executor package. This is a pure premature abstraction: an export created to prevent a deadcode false-positive, not because any external consumer needs it.

## What to Do

Option A (preferred — simpler):
1. Remove the `BuildPredicates` wrapper function.
2. Rename `buildPredicates` to `BuildPredicates` (export the original) at its definition (`predicates.go:36`) and at all its call sites within the executor package (`plan_blocks.go`, `stream.go`, etc.).
3. In `cmd/deadcode/main.go`, update the anchor from `_ = modules_executor.BuildPredicates` to just remove it if `BuildPredicates` is genuinely needed for the deadcode tool, or keep the anchor on the newly-renamed exported function.

Option B (if export is not wanted):
1. Remove the `BuildPredicates` wrapper.
2. Remove the deadcode anchor `_ = modules_executor.BuildPredicates` from `cmd/deadcode/main.go`.
3. `buildPredicates` stays unexported.

## Acceptance Criteria

- No two-function indirection (wrapper + real function) for predicate building.
- `make precommit` passes.
- No new functionality introduced.

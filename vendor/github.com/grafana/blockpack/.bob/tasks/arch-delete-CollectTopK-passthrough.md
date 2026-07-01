# Cleanup: Delete CollectTopK — passthrough shim with no real callers

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**source:** discover-arch

## Location

`internal/modules/executor/stream_topk.go:116-140`

## Issue

`CollectTopK` is a pure passthrough: its entire body is `return e.Collect(r, program, opts)`. The function's own comment says "CollectTopK is a compatibility shim that delegates to Collect" and "Callers should prefer Collect directly." There are zero real production callers — `api.go` already calls `Collect` directly. The only reference outside the executor package is a deadcode anchor in `cmd/deadcode/main.go` which pins `exec.Collect`, not `CollectTopK`.

SPEC-STREAM-7 documents globally-correct top-K behavior. That behavior is now implemented inside `Collect` (heap-based scan when `TimestampColumn` and `Limit` are both set). The shim adds zero value and makes the API surface confusingly large.

## What to Do

1. Delete the `CollectTopK` method from `stream_topk.go` (lines 116-140).
2. In `internal/modules/executor/SPECS.md`, update SPEC-STREAM-7 back-ref to point to `stream.go:Collect` (where top-K is actually implemented).
3. In `cmd/deadcode/main.go`, verify no anchor for `CollectTopK` remains (there is none; only `exec.Collect` is anchored).
4. Update the stale comment in `api.go` line 773 ("CollectTopK gives globally top-K results") to say "Collect gives globally top-K results".

## Acceptance Criteria

- `CollectTopK` method no longer exists in the codebase.
- `make precommit` passes.
- No new functionality introduced.
- SPEC-STREAM-7 back-ref is updated.

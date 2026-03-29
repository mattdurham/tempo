# Cleanup: Remove empty Executor struct — convert methods to package-level free functions

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**source:** discover-arch

## Location

`internal/modules/executor/executor.go:57-62`
Methods: `stream.go:101`, `stream_topk.go:134`, `stream_structural.go:37`
Call sites: `api.go:460`, `api.go:787`, `api.go:1177`

## Issue

`Executor` is an empty struct with no state:

```go
type Executor struct{}
func New() *Executor { return &Executor{} }
```

Its three methods (`Collect`, `CollectTopK`, `ExecuteStructural`) never read or write the receiver `e`. They are effectively package-level free functions masquerading as methods. Meanwhile, five other executor operations (`CollectLogs`, `StreamLogs`, `ExecuteTraceMetrics`, `ExecuteLogMetrics`, `BlockRefsFromIntrinsicTOC`) are already free functions.

This inconsistency forces every api.go call site into the pattern `modules_executor.New().Collect(...)` — allocating a useless pointer on every query — while equivalent operations are called as `executor.StreamLogs(...)`. The empty struct signals "this object has state" to every reader, which is false.

Per the 2-3 Rule: a type extracted for a single structural purpose (namespacing 3 methods) with zero state is not worth the abstraction.

## What to Do

1. Convert `(e *Executor) Collect(...)` to `func Collect(...)` (package-level free function) in `stream.go`.
2. After deleting `CollectTopK` (see arch-delete-CollectTopK task), convert `(e *Executor) ExecuteStructural(...)` to `func ExecuteStructural(...)` in `stream_structural.go`.
3. Delete `type Executor struct{}` and `func New()` from `executor.go`.
4. In `api.go`:
   - Line 460: replace `exec := modules_executor.New(); exec.ExecuteStructural(...)` with `modules_executor.ExecuteStructural(...)`.
   - Line 787: replace `modules_executor.New().Collect(...)` with `modules_executor.Collect(...)`.
   - Line 1177: replace `modules_executor.New().Collect(...)` with `modules_executor.Collect(...)`.
5. In `cmd/deadcode/main.go`: replace `exec := modules_executor.New(); exec.Collect(...)` with `modules_executor.Collect(...)`. Remove or update `exec.Collect` reference.
6. Update docstring in `executor.go` package comment (currently shows `exec := executor.New(); exec.Collect(...)`).

## Acceptance Criteria

- `type Executor struct{}` and `func New()` no longer exist.
- All three former methods are package-level free functions.
- `make precommit` passes.
- No new functionality introduced.
- Call sites in `api.go` call free functions directly.

# Fix executor/SPECS.md §11: ExecuteStructural stale `(e *Executor)` receiver

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** HIGH
**Source:** discover-docs (round 2)

## Problem

`internal/modules/executor/SPECS.md` lines 576-580 show:

```go
func (e *Executor) ExecuteStructural(
    r *reader.Reader,
    q *traceqlparser.StructuralQuery,
    opts Options,
) (*StructuralResult, error)
```

No `Executor` struct exists in the package. The actual function in
`internal/modules/executor/stream_structural.go:37` is:

```go
func ExecuteStructural(
    r *modules_reader.Reader,
    q *traceqlparser.StructuralQuery,
    opts Options,
) (*StructuralResult, error)
```

## Fix

In `internal/modules/executor/SPECS.md` §11 code block:
1. Remove `(e *Executor)` receiver.
2. Change `r *reader.Reader` to `r *modules_reader.Reader`.

Do NOT change any code. Spec text fix only.

## Acceptance criteria
- SPECS.md §11 code block matches the actual `ExecuteStructural` signature.
- No `(e *Executor)` receiver remains in the spec.

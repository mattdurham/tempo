# Fix logql/NOTES.md NOTE-011: Stale executor.New().Execute() example

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** LOW
**Source:** discover-docs (round 2)

## Problem

`internal/modules/logql/NOTES.md` NOTE-011 (line 173) uses `executor.New().Execute()` as
an example of why free functions are preferred over an Engine struct:

> This matches `executor.New().Execute()` being a thin wrapper over `ExecuteMetrics`;
> the engine struct is a vestigial convention from earlier designs.

Neither `New()` nor `Execute` exist in the executor package. The comparison still makes a
valid architectural point but references a removed API.

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/logql/NOTES.md` NOTE-011, replace:
> `executor.New().Execute()` being a thin wrapper over `ExecuteMetrics`

With:
> `executor.Collect()` being a stateless free function with no constructor ceremony

## Acceptance criteria
- NOTE-011 no longer references `executor.New().Execute()`.
- The rationale still explains why free functions are preferred.

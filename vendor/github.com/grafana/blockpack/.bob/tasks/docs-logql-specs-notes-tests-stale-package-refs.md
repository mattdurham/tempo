# Fix logql/{SPECS,NOTES,TESTS}.md: stale references to deleted package

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** spec-docs
**source:** discover-docs

## Location

- `internal/modules/logql/SPECS.md` — Responsibility Boundary table (line 3-5)
- `internal/modules/logql/NOTES.md` — NOTE-001 (line 12), NOTE-003 (line 49)
- `internal/modules/logql/TESTS.md` — preamble (line 4)

## Issue

The `internal/modules/logql/` package was deleted and all execution code merged into
`internal/modules/executor/` (documented in executor/NOTES.md NOTE-012 Addendum:
*"The internal/modules/logql package has been deleted"*). However, the three spec files
in the logql/ directory still describe the now-deleted package as if it exists:

### SPECS.md
The opening paragraph says:
```
The code lives in `internal/logqlparser/` (parse, compile, pipeline stages) and
`internal/modules/logql/` (block scanning, log streaming, metrics).
```
`internal/modules/logql/` has no Go source files.

### NOTES.md — NOTE-001 (line 12)
```
The LogQL execution engine lives in `internal/modules/logql/`, not in `internal/logqlparser/`.
```
The package no longer exists.

### NOTES.md — NOTE-003 (line 49)
```
Metric aggregation in `metrics.go` reuses `vm.QuerySpec` and `vm.AggBucket`
```
`metrics.go` was deleted with the package; the equivalent is now `executor/metrics_log.go`.

### TESTS.md — Preamble (line 4)
```
All integration tests build in-memory blockpack log files and run full queries.
```
This implies tests live in `internal/modules/logql/`, but no Go package exists. The pipeline
stage unit tests (LQL-TEST-001 through ~LQL-TEST-030) are in `internal/logqlparser/pipeline_test.go`.
The integration tests (StreamLogs, ExecuteLogMetrics) are in `internal/modules/executor/`.

## Fix

This is a CLEANUP task. Do NOT add new functionality.

**SPECS.md:** Update the Responsibility Boundary table to replace `internal/modules/logql/` with `internal/modules/executor/` for execution concerns. Example correction:
```
| Log stream query execution | `executor` (`StreamLogs`, `StreamLogsTopK`) |
| Metric aggregation execution | `executor` (`ExecuteLogMetrics`) |
```

**NOTES.md — NOTE-001:** Add an Addendum:
```
*Addendum (2026-03-17):* This module was merged into `internal/modules/executor/`.
See executor/NOTES.md NOTE-012 Addendum for the migration details.
```

**NOTES.md — NOTE-003:** Add an Addendum noting `metrics.go` was merged into `executor/metrics_log.go:ExecuteLogMetrics`.

**TESTS.md:** Update the preamble to:
- State that pipeline stage unit tests live in `internal/logqlparser/pipeline_test.go`
- State that integration tests (StreamLogs, ExecuteLogMetrics) live in `internal/modules/executor/stream_log_test.go` and `executor/metrics_log_test.go`

## Acceptance criteria

- SPECS.md no longer references `internal/modules/logql/` as a code location
- NOTES.md NOTE-001 and NOTE-003 have Addenda acknowledging the migration
- TESTS.md preamble correctly identifies where the tests live
- No code changes needed (doc fix only)

# Cleanup: Fix stale back-references in executor/NOTES.md NOTE-018

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**source:** discover-arch

## Location

`internal/modules/executor/NOTES.md:598-601`

## Issue

NOTE-018 back-refs include three non-existent functions:

```
Back-ref: `internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/modules/executor/executor.go:Execute`,        ← does not exist (fn is Collect)
`internal/modules/executor/stream.go:Stream`,           ← does not exist (fn is scanBlocks)
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/metrics.go:ExecuteMetrics`,  ← file+fn do not exist
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
```

The file `metrics.go` was split into `metrics_log.go` and `metrics_trace.go`. `ExecuteMetrics` was renamed to `ExecuteLogMetrics` and `ExecuteTraceMetrics`. The functions `Execute` and `Stream` no longer exist under those names.

## What to Do

Replace lines 597-605 with the correct back-refs:

```
Back-ref: `internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/modules/executor/stream.go:Collect`,
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`,
`internal/modules/executor/stream_topk.go:topKScanBlocks`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`
```

## Acceptance Criteria

- All back-refs in NOTE-018 point to actually-existing files and functions.
- `make precommit` passes (spec files are not linted, but verify no broken links).
- No code changes.

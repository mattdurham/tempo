# Fix executor/NOTES.md NOTE-018: three stale back-references

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** spec-docs
**source:** discover-docs

## Location

`internal/modules/executor/NOTES.md`, lines 597-605 (NOTE-018 back-ref block)

## Issue

NOTE-018 ("Two-Pass Column Decode for Non-Matching Block Elimination") has a back-ref block that contains three stale entries pointing to functions/files that no longer exist after the Execute→Collect rename (NOTE-035, 2026-03-11):

```
Back-ref: `internal/modules/executor/predicates.go:ProgramWantColumns`,
`internal/modules/executor/executor.go:Execute`,           ← STALE: no Execute in executor.go
`internal/modules/executor/stream.go:Stream`,              ← STALE: no Stream in stream.go
`internal/modules/executor/stream_log.go:StreamLogs`,
`internal/modules/executor/metrics.go:ExecuteMetrics`,     ← STALE: metrics.go was split
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics`,
`internal/modules/executor/stream_topk.go:topKScanBlocks`,
`internal/modules/executor/stream_log_topk.go:logTopKScan`,
`internal/modules/executor/stream_log_topk.go:logCollectAll`
```

Verified:
- `executor.go` has no `Execute` function (only `New()` and `SpanMatchFromRow`)
- `stream.go` has no `Stream` function (function is named `Collect`)
- `metrics.go` does not exist; the split produced `metrics_log.go:ExecuteLogMetrics` and `metrics_trace.go:ExecuteTraceMetrics`

## Fix

Remove the three stale lines and add the missing `ExecuteTraceMetrics` entry. The corrected back-ref block should be:

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

## Acceptance criteria

- No reference to `executor.go:Execute`, `stream.go:Stream`, or `metrics.go:ExecuteMetrics` remains in NOTE-018
- `metrics_trace.go:ExecuteTraceMetrics` is added to the back-ref block
- No code changes needed (doc fix only)

# Cleanup: Fix stale spec reference in executor/NOTES.md

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/modules/executor/NOTES.md:601`

## Issue

The executor NOTES.md contains a stale back-reference:
```
`internal/modules/executor/metrics.go:ExecuteMetrics`,
```

The file `metrics.go` was split into:
- `internal/modules/executor/metrics_log.go` containing `ExecuteLogMetrics`
- `internal/modules/executor/metrics_trace.go` containing `ExecuteTraceMetrics`

The stale reference points to a file and function that no longer exist, which will confuse future readers following the spec cross-reference.

## Fix

Update line 601 of `internal/modules/executor/NOTES.md` to:
```
`internal/modules/executor/metrics_log.go:ExecuteLogMetrics` and
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetrics`
```

**Effort:** 1 minute. Low risk — documentation only.

# Fix logql/SPECS.md: 12 back-refs pointing to non-existent files + wrong signatures

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** HIGH
**Source:** discover-docs (round 2)

## Problem

`internal/modules/logql/SPECS.md` has multiple cascading issues:

### 1. Back-refs point to non-existent files (12 locations)
The file references `internal/modules/logql/pipeline.go`, `engine.go`, and `metrics.go` but the
`internal/modules/logql/` package has NO Go source files — only spec files remain after the package
was merged into executor/logqlparser.

Stale back-refs (lines 51, 66, 77, 89, 102, 113, 124, 136, 149, 185, 208, 229, 254):
- `internal/modules/logql/pipeline.go:Pipeline` → `internal/logqlparser/pipeline.go:Pipeline`
- `internal/modules/logql/pipeline.go:JSONStage` → `internal/logqlparser/pipeline.go:JSONStage`
- `internal/modules/logql/pipeline.go:LogfmtStage` → `internal/logqlparser/pipeline.go:LogfmtStage`
- `internal/modules/logql/pipeline.go:LabelFormatStage` → `internal/logqlparser/pipeline.go:LabelFormatStage`
- `internal/modules/logql/pipeline.go:LineFormatStage` → `internal/logqlparser/pipeline.go:LineFormatStage`
- `internal/modules/logql/pipeline.go:DropStage` → `internal/logqlparser/pipeline.go:DropStage`
- `internal/modules/logql/pipeline.go:KeepStage` → `internal/logqlparser/pipeline.go:KeepStage`
- `internal/modules/logql/pipeline.go:LabelFilterStage` → `internal/logqlparser/pipeline.go:LabelFilterStage`
- `internal/modules/logql/pipeline.go:UnwrapStage` → `internal/logqlparser/pipeline.go:UnwrapStage`
- `internal/modules/logql/engine.go:LogEntry` → `internal/modules/executor/stream_log.go:LogEntry`
- `internal/modules/logql/engine.go:StreamLogs` → `internal/modules/executor/stream_log.go:StreamLogs`
- `internal/modules/logql/metrics.go:ExecuteLogMetrics` → `internal/modules/executor/metrics_log.go:ExecuteLogMetrics`
- `internal/modules/logql/metrics.go:LogMetricsResult` → `internal/modules/executor/metrics_log.go:LogMetricsResult`

### 2. §13 LogEntry type is wrong (line 174-179)
Documented:
```go
type LogEntry struct {
    Labels         map[string]string
    Line           string
    TimestampNanos uint64
}
```
Actual (`stream_log.go:35-46`):
```go
type LogEntry struct {
    LokiLabels     string
    Line           string
    LogAttrs       LogAttrs
    TimestampNanos uint64
}
```

### 3. §15 StreamLogs signature is wrong (line 200)
Documented: `func StreamLogs(r *reader.Reader, program *vm.Program, pipeline *Pipeline, fn LogEntryCallback) error`
Actual: `func StreamLogs(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline) ([]LogEntry, error)`

### 4. §1 Responsibility table references `StreamLogsTopK` (line 16)
`StreamLogsTopK` does not exist. The actual function is `CollectLogs`.

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/logql/SPECS.md`:
1. Update all 13 back-refs to point to correct files (see mapping above).
2. Update §13 `LogEntry` type to match actual struct.
3. Update §15 `StreamLogs` signature.
4. Update §1 responsibility table: `StreamLogsTopK` → `CollectLogs`.

## Acceptance criteria
- No back-refs point to `internal/modules/logql/*.go`.
- §13 LogEntry struct matches executor's actual struct.
- §15 StreamLogs signature matches actual code.
- §1 table says `CollectLogs` not `StreamLogsTopK`.

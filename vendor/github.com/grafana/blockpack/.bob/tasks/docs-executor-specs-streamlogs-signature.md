# Fix executor/SPECS.md §7 and §8: StreamLogs/CollectLogs wrong signatures and LogEntry type

**Type:** cleanup
**Cleanup-type:** spec-docs
**Severity:** HIGH
**Source:** discover-docs (round 2)

## Problem

`internal/modules/executor/SPECS.md` §7 and §8 have three interrelated errors:

### §7.1 (lines 330-338): Wrong StreamLogs signature
SPECS.md documents:
```go
func StreamLogs(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline, fn LogEntryCallback) error
```
Actual `stream_log.go:56`:
```go
func StreamLogs(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline) ([]LogEntry, error)
```
`fn LogEntryCallback` does not exist. Return type is wrong.

### §7.2 (lines 343-356): Wrong LogEntry struct and non-existent LogEntryCallback
SPECS.md documents:
```go
type LogEntry struct {
    Labels         logqlparser.LabelSet
    LogAttrs       map[string]string
    Line           string
    TimestampNanos uint64
}
type LogEntryCallback func(entry *LogEntry) bool
```
Actual `stream_log.go:21-46`:
```go
type LogAttrs struct {
    Names  []string
    Values []string
}
type LogEntry struct {
    LokiLabels     string
    Line           string
    LogAttrs       LogAttrs
    TimestampNanos uint64
}
```
`Labels logqlparser.LabelSet` does not exist. `LogAttrs` is a struct not a map. `LogEntryCallback` does not exist in executor.

### §8 (lines 378-424): StreamLogsTopK does not exist; actual function is CollectLogs
SPECS.md §8.1 documents:
```go
func StreamLogsTopK(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline, opts StreamOptions, fn LogEntryCallback) error
```
Actual `stream_log_topk.go:105`:
```go
func CollectLogs(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline, opts CollectOptions) ([]LogEntry, error)
```
`StreamLogsTopK`, `StreamOptions`, and `LogEntryCallback` do not exist in the executor package.

### SPEC-SL-3 (lines 363-370): Label description describes old LabelSet path
SPEC-SL-3 describes `blockLabelSet` built from `resource.*`/`log.*` columns. The actual `StreamLogs` reads `resource.__loki_labels__` directly into `LokiLabels string`.

## Fix

This is a CLEANUP task. Do NOT change any code.

In `internal/modules/executor/SPECS.md`:

1. **§7.1**: Update signature to:
   ```go
   func StreamLogs(r *modules_reader.Reader, program *vm.Program, pipeline *logqlparser.Pipeline) ([]LogEntry, error)
   ```

2. **§7.2**: Update `LogEntry` type to match actual struct (LokiLabels/LogAttrs struct/Line/TimestampNanos). Remove `LogEntryCallback` type. Update the `LogAttrs` description to describe the parallel `Names []string` / `Values []string` fields.

3. **SPEC-SL-3**: Update description to: `LokiLabels` holds the value of `resource.__loki_labels__` column for the row. Remove the LabelSet / blockLabelSet description (that applies to CollectLogs internally, not to the StreamLogs return value).

4. **§8 title and §8.1**: Rename section from "StreamLogsTopK" to "CollectLogs". Update signature to `CollectLogs(..., opts CollectOptions) ([]LogEntry, error)`. Replace `StreamOptions` with `CollectOptions` throughout §8.3 invariant descriptions. Remove `fn LogEntryCallback`.

## Acceptance criteria
- SPECS.md §7.1 signature matches `stream_log.go:StreamLogs`.
- SPECS.md §7.2 `LogEntry` struct matches actual struct.
- SPECS.md §8 is titled "CollectLogs" with correct signature.
- No references to `StreamLogsTopK`, `StreamOptions`, or `LogEntryCallback` remain in §7 or §8.

# Cleanup: Delete logTopKEntry single-field wrapper struct

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**severity:** MEDIUM
**source:** discover-arch

## Location

`internal/modules/executor/stream_log_topk.go:22-24`

## What to Delete/Simplify

`logTopKEntry` is a struct containing exactly one field: `entry LogEntry`. It exists solely to wrap a `LogEntry` for storage in the heap. Every construction is `logTopKEntry{entry: someEntry}` and every read is `.entry.TimestampNanos`, `.entry.Line`, etc.

Delete `logTopKEntry` entirely. Change `logTopKHeap.entries` from `[]logTopKEntry` to `[]LogEntry`. Update all affected code:

- `logTopKHeap.Less`: `h.entries[i].entry.TimestampNanos` → `h.entries[i].TimestampNanos`
- `logTopKHeap.Push`: type assertion `x.(logTopKEntry)` → `x.(LogEntry)`; append `x.(LogEntry)` directly
- `logTopKHeap.Pop`: return `x` is now `LogEntry` directly
- `logTopKInsert`: parameter `entry logTopKEntry` → `entry LogEntry`; all `.entry.TimestampNanos` → `.TimestampNanos`
- `logTopKCanSkipBlock`: `buf.entries[0].entry.TimestampNanos` → `buf.entries[0].TimestampNanos`
- `logCollectAll`: `all *[]logTopKEntry` → `all *[]LogEntry`; `*all = append(*all, logTopKEntry{entry: entry})` → `*all = append(*all, entry)`
- `logDeliverAll`: parameter `entries []logTopKEntry` → `entries []LogEntry`; remove copy loop (lines 477-479), directly return `entries`
- `CollectLogs` (line 185): `all := make([]logTopKEntry, ...)` → `all := make([]LogEntry, ...)`; `logDeliverAll(all, backward)` already returns `[]LogEntry`
- `logTopKScan` closure (line 410): `logTopKInsert(buf, opts.Limit, backward, logTopKEntry{entry: entry})` → `logTopKInsert(buf, opts.Limit, backward, entry)`
- The `buf.entries[0].entry.TimestampNanos` reference in `logTopKScan`'s canSkip closure (line 406) → `buf.entries[0].TimestampNanos`

## Why

The wrapper provides zero structural value. It adds a layer of `.entry.` indirection on every heap access. The heap stores `LogEntry` values by value regardless — there is no allocation difference. The NOTE-041 comment ("ts field removed — heap comparisons use entry.TimestampNanos directly") documents a prior simplification; this is the same class of cleanup.

## Acceptance Criteria

- `logTopKEntry` type is gone
- All heap operations use `LogEntry` directly
- All tests pass (`make test`)
- No new functionality introduced

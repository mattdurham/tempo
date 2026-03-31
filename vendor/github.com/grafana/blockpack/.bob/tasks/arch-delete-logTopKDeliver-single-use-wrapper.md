# Cleanup: Inline logTopKDeliver — single-line wrapper with one caller

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**source:** discover-arch

## Location

`internal/modules/executor/stream_log_topk.go:429-432` (definition)
`internal/modules/executor/stream_log_topk.go:172` (sole call site)

## Issue

`logTopKDeliver` is a one-liner that immediately delegates to `logDeliverAll`:

```go
func logTopKDeliver(buf *logTopKHeap, backward bool) []LogEntry {
    return logDeliverAll(buf.entries, backward)
}
```

It has exactly one call site (line 172). The function adds no documentation value over its body — the name `logDeliverAll` at the call site is equally descriptive. This is a 2-Rule violation: a helper extracted for a single consumer with no added semantic value.

## What to Do

1. At `stream_log_topk.go:172`, replace `logTopKDeliver(buf, backward)` with `logDeliverAll(buf.entries, backward)`.
2. Delete the `logTopKDeliver` function definition (lines 429-432).

## Acceptance Criteria

- `logTopKDeliver` no longer exists.
- `make precommit` passes.
- No new functionality introduced.

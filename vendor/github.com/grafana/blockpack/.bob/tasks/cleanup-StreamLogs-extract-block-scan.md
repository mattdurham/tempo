# Cleanup: Extract block-scan loop from StreamLogs into shared helper with iterateLogRows

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/modules/executor/stream_log.go:56-196` (cyclomatic complexity 26)

## Issue

`StreamLogs` (cyclo 26) implements its own coalesced group fetch + block parse + pipeline apply loop (lines 106-194) that is structurally similar to the scan loop inside `iterateLogRows` (which already handles group fetching). The two loops differ only in:
1. `iterateLogRows` uses the `canSkipBlock`/`canSkip`/`fn` callback protocol.
2. `StreamLogs` builds a local `results []LogEntry` slice directly.

`StreamLogs` could be refactored to use `iterateLogRows` with a `fn` callback that appends to the results slice, eliminating the duplicated coalesced I/O, group-fetch, parse, and per-row pipeline logic.

## Fix

Replace the inner loop of `StreamLogs` (lines 106-194) with a call to `iterateLogRows`:

```go
results := make([]LogEntry, 0, totalRows)
_, err := iterateLogRows(r, program, wantColumns, pipeline, CollectOptions{}, plan,
    nil, nil,
    func(ts uint64, entry LogEntry) bool {
        results = append(results, entry)
        return true
    },
)
if err != nil {
    return nil, err
}
return results, nil
```

This eliminates ~90 lines of duplicated scan machinery and drops `StreamLogs` from cyclo 26 to ~8.

**Note:** Verify that the `opts.TimeRange` zero value in the `CollectOptions` passed to `iterateLogRows` correctly means "no time filter" (it does, per the MinNano/MaxNano zero-means-open semantics). Also confirm `canSkipBlock=nil` and `canSkip=nil` are correct for the non-topK path.

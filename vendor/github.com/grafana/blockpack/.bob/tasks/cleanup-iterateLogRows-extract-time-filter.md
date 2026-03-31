# Cleanup: Extract time-range row filter from iterateLogRows into a helper

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/modules/executor/stream_log_topk.go:211-374` (cyclomatic complexity 38)

## Issue

`iterateLogRows` has cyclomatic complexity 38, driven by two sources of nesting:
1. The group-fetch + block-skip machinery (lines 235-274) — correctly complex, handles coalesced I/O.
2. The inline time-range filter loop (lines 293-318) — an 8-branch block that filters `rows` to `keptByTime`. This is self-contained logic that tests each row's timestamp against `opts.TimeRange.MinNano` / `opts.TimeRange.MaxNano`.

The time-range filter is already described in a NOTE-021 comment. Extracting it to a named helper makes the overall flow more readable and reduces the top-level cyclomatic count by approximately 6.

## Fix

Extract the time-range filter into a package-level helper:

```go
// filterRowsByTimeRange filters rowIndices to those within [minNano, maxNano].
// Zero values for minNano/maxNano mean open bounds.
// NOTE-021: called after first-pass ColumnPredicate to skip second-pass decode for out-of-range rows.
func filterRowsByTimeRange(tsCol *modules_reader.Column, rows []int, minNano, maxNano uint64) []int {
    if minNano == 0 && maxNano == 0 {
        return rows
    }
    kept := make([]int, 0, len(rows))
    for _, rowIdx := range rows {
        var ts uint64
        if tsCol != nil {
            if v, ok := tsCol.Uint64Value(rowIdx); ok {
                ts = v
            }
        }
        if minNano > 0 && ts < minNano {
            continue
        }
        if maxNano > 0 && ts > maxNano {
            continue
        }
        kept = append(kept, rowIdx)
    }
    return kept
}
```

Replace lines 293-318 in `iterateLogRows` with:
```go
keptByTime := filterRowsByTimeRange(bwb.Block.GetColumn("log:timestamp"), rows, opts.TimeRange.MinNano, opts.TimeRange.MaxNano)
if len(keptByTime) == 0 {
    continue
}
```

**Complexity reduction:** iterateLogRows: 38 → ~32. New helper: ~4.

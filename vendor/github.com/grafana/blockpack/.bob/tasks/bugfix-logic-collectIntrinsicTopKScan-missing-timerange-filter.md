# Fix logic: collectIntrinsicTopKScan omits time-range filtering

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** bug-fix
**severity:** HIGH
**source:** bug-finder

## Location

`internal/modules/executor/stream.go:764` — `collectIntrinsicTopKScan`

## Bug

`collectIntrinsicTopKScan` (the M >= SortScanThreshold path of Case B: pure intrinsic + timestamp sort) does not apply `opts.TimeRange.MinNano/MaxNano` filtering to the selected refs.

The KLL path (`collectIntrinsicTopKKLL`) explicitly applies time-range filtering at lines 729-738:

```go
if opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0 {
    filtered := pairs[:0]
    for _, p := range pairs {
        if (opts.TimeRange.MinNano == 0 || p.ts >= opts.TimeRange.MinNano) &&
            (opts.TimeRange.MaxNano == 0 || p.ts <= opts.TimeRange.MaxNano) {
            filtered = append(filtered, p)
        }
    }
    pairs = filtered
}
```

The scan path has no equivalent step. `ScanFlatColumnRefsFiltered` operates on `BlockRef` values only (no timestamps); the filter callback can check block range and set membership but cannot check timestamps. The time-range filter must be applied post-scan to the `selected` refs.

## Trigger

Any sorted query (trace or log) with all of:
- `opts.TimestampColumn != ""` (MostRecent / Oldest direction)
- `opts.Limit > 0`
- `opts.TimeRange.MinNano > 0 || opts.TimeRange.MaxNano > 0`
- A pure-intrinsic predicate (ProgramIsIntrinsicOnly returns true)
- `len(refs) >= SortScanThreshold` (8000 by default) — enough matching refs to trigger the scan path instead of the KLL path

## Impact

Rows outside the requested time window are returned to the caller. The time range specified
by the caller is silently ignored for large result sets. Wrong query results — rows that should
have been excluded by the time filter appear in the output.

## Fix

After `ScanFlatColumnRefsFiltered` returns `selected` and before returning, apply the same
time-range filter as `collectIntrinsicTopKKLL`. The selected refs do not carry timestamps, so
look up the timestamp column via `r.GetIntrinsicColumn(opts.TimestampColumn)`, build a
packed-key → timestamp map (same as `collectIntrinsicTopKKLL` does), and discard any ref
whose timestamp falls outside `[opts.TimeRange.MinNano, opts.TimeRange.MaxNano]`.

The exact filter logic already exists verbatim in `collectIntrinsicTopKKLL`; the fix is to
apply it to the scan path's `selected` slice.

No new functionality is introduced — this is restoring filtering behavior that was already
specified for Case B and already implemented in the KLL path.

## Acceptance Criteria

- A sorted query (CollectOptions with TimestampColumn, Limit, and TimeRange set) returns only
  rows within the specified time range, regardless of whether the KLL or scan path was taken.
- The scan path (len(refs) >= SortScanThreshold) applies the same time-range filter as the
  KLL path (len(refs) < SortScanThreshold).
- All existing tests pass.
- No new functionality introduced.

# Cleanup: Extract KLL and scan sub-paths from collectIntrinsicTopK into named helpers

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/modules/executor/stream.go:625-778` (cyclomatic complexity 35)

## Issue

`collectIntrinsicTopK` contains two distinct execution paths guarded by a single `if len(refs) < SortScanThreshold` branch (line 637). Each path is independently complex:

- **KLL path** (lines 638-726, ~88 lines): builds a packed-key→timestamp map, groups refs by block, sorts by MaxStart, collects (ref, ts) pairs, applies time filter, sorts by timestamp, truncates to limit.
- **Scan path** (lines 727-762, ~35 lines): packs matchKeys, calls `ScanFlatColumnRefsFiltered`, records scan stats.

Each path is self-contained with its own distinct algorithm and data structures. Splitting them into named helpers `collectIntrinsicTopKKLL` and `collectIntrinsicTopKScan` would:
- Drop `collectIntrinsicTopK` from cyclo 35 to ~4 (the dispatch logic)
- Give each path a clear name matching the NOTE-043/NOTE-044 documentation
- Make the code directly traceable to the spec entries

## Fix

Extract into two helpers:

```go
// collectIntrinsicTopKKLL handles the KLL path: M < SortScanThreshold.
// Builds a packed-key map from the timestamp column, collects (ref,ts) pairs,
// applies time filter, sorts, and truncates to limit.
// See NOTE-044.
func collectIntrinsicTopKKLL(r, refs, opts, stats) ([]BlockRef, error) { ... }

// collectIntrinsicTopKScan handles the scan path: M >= SortScanThreshold.
// Packs refs into a sorted key set, scans timestamp blob backward/forward.
// See NOTE-043.
func collectIntrinsicTopKScan(r, refs, opts, stats) ([]BlockRef, error) { ... }
```

`collectIntrinsicTopK` becomes:
```go
func collectIntrinsicTopK(...) ([]MatchedRow, error) {
    var selected []BlockRef
    var err error
    if len(refs) < SortScanThreshold {
        selected, err = collectIntrinsicTopKKLL(r, refs, opts, stats)
    } else {
        selected, err = collectIntrinsicTopKScan(r, refs, opts, stats)
    }
    if err != nil { return nil, err }
    if len(selected) == 0 { return nil, nil }
    return buildMatchedRows(r, selected, secondPassCols), nil
}
```

**Complexity reduction:** collectIntrinsicTopK: 35 → ~4. Two new helpers ~18 and ~12.

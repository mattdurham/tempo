# Cleanup: Replace sort.Slice with slices.SortFunc across the codebase

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

Multiple files — 20+ `sort.Slice` calls:
- `internal/modules/blockio/writer/intrinsic_accum.go:348,513,527,730`
- `internal/modules/blockio/writer/metadata.go:259`
- `internal/modules/blockio/writer/ts_index.go:45`
- `internal/modules/blockio/writer/writer_block.go:999`
- `internal/modules/blockio/reader/range_index.go:163,167,171`
- `internal/modules/blockio/reader/coalesce.go:41`
- `internal/modules/blockio/reader/layout.go:214,280,360`
- `internal/modules/blockio/reader/file_sketch_summary.go:127`
- `internal/modules/executor/metrics_trace.go:384,507,547`
- `internal/modules/queryplanner/explain.go:143`
- `internal/modules/sketch/topk.go:58`

## Issue

The codebase already imports `slices` in many files and uses `slices.SortFunc` in newer code (e.g. `stream.go`, `stream_log_topk.go`). However, 20+ older `sort.Slice` calls remain. The idiomatic Go 1.21+ style is `slices.SortFunc` with a comparator returning `int` (using `cmp.Compare`), which is type-safe (no index arithmetic), avoids the two-closure overhead, and makes intent clearer.

Examples:
```go
// Before
sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })

// After  
slices.SortFunc(entries, func(a, b entryType) int { return cmp.Compare(b.Count, a.Count) })
```

## Fix

Replace each `sort.Slice` call with `slices.SortFunc`. Add `cmp` import where missing. Remove `sort` import from files where it is no longer needed after the migration.

Special case at `intrinsic_accum.go:527`:
```go
// Before
sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].val, rows[j].val) < 0 })
// After
slices.SortFunc(rows, func(a, b rowType) int { return bytes.Compare(a.val, b.val) })
```

**Note:** `sort.SliceStable` (if any) should become `slices.SortStableFunc`. Verify stability is not required for the affected sorts before converting.

# Cleanup: Replace three sort.Slice calls in sortRangeEntries with slices.SortFunc

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

`internal/modules/blockio/reader/range_index.go:160-175` (sortRangeEntries function)

## Issue

`sortRangeEntries` uses three consecutive `sort.Slice` calls inside a `switch` statement, one per numeric column type. The same sort.Slice → slices.SortFunc migration applies here, but this location is notable because the three near-identical blocks can be further consolidated using `cmp.Compare` with a type-keyed decode function:

```go
// Current — three separate sort.Slice blocks
case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
    sort.Slice(entries, func(i, j int) bool {
        return decodeInt64Key(entries[i].lower) < decodeInt64Key(entries[j].lower)
    })
case shared.ColumnTypeRangeUint64:
    sort.Slice(entries, func(i, j int) bool {
        return decodeUint64Key(entries[i].lower) < decodeUint64Key(entries[j].lower)
    })
case shared.ColumnTypeRangeFloat64:
    sort.Slice(entries, func(i, j int) bool {
        return decodeFloat64Key(entries[i].lower) < decodeFloat64Key(entries[j].lower)
    })
```

## Fix

Convert each `sort.Slice` to `slices.SortFunc`:

```go
case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
    slices.SortFunc(entries, func(a, b rangeEntry) int {
        return cmp.Compare(decodeInt64Key(a.lower), decodeInt64Key(b.lower))
    })
case shared.ColumnTypeRangeUint64:
    slices.SortFunc(entries, func(a, b rangeEntry) int {
        return cmp.Compare(decodeUint64Key(a.lower), decodeUint64Key(b.lower))
    })
case shared.ColumnTypeRangeFloat64:
    slices.SortFunc(entries, func(a, b rangeEntry) int {
        return cmp.Compare(decodeFloat64Key(a.lower), decodeFloat64Key(b.lower))
    })
```

Add `cmp` and `slices` imports if not already present. Remove `sort` import if no longer needed in this file.

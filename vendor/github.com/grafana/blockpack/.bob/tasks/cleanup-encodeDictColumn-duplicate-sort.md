# Cleanup: Deduplicate sort logic in encodeDictColumn and encodePagedDictColumn

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

- `internal/modules/blockio/writer/intrinsic_accum.go:345-353` (encodeDictColumn)
- `internal/modules/blockio/writer/intrinsic_accum.go:728-735` (encodePagedDictColumn)

## Issue

Both `encodeDictColumn` and `encodePagedDictColumn` begin with an identical block to sort the dict entries:

```go
isInt64 := c.colType == shared.ColumnTypeInt64 || c.colType == shared.ColumnTypeRangeInt64
sort.Slice(c.entries, func(i, j int) bool {
    if isInt64 {
        return c.entries[i].int64Val < c.entries[j].int64Val
    }
    return c.entries[i].strVal < c.entries[j].strVal
})
```

This exact block appears twice. Any future change to the sort key (e.g. adding a new int type) must be made in both places.

## Fix

Extract a `sortDictEntries(c *dictAccum)` helper:

```go
// sortDictEntries sorts c.entries in ascending order by value.
// Int64 and RangeInt64 columns sort by int64Val; all others sort by strVal.
func sortDictEntries(c *dictAccum) {
    isInt64 := c.colType == shared.ColumnTypeInt64 || c.colType == shared.ColumnTypeRangeInt64
    slices.SortFunc(c.entries, func(a, b dictEntry) int {
        if isInt64 {
            return cmp.Compare(a.int64Val, b.int64Val)
        }
        return cmp.Compare(a.strVal, b.strVal)
    })
}
```

Replace the duplicated sort blocks in both functions with a call to `sortDictEntries(c)`. Also converts from `sort.Slice` to `slices.SortFunc` (part of the broader sort migration).

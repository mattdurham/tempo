# Fix logic: StreamFullScan returns row index instead of delivered count on early stop

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** bug-fix
**severity:** LOW
**source:** bug-finder

## Location

`internal/modules/executor/column_provider.go:601-609` — `StreamFullScan`

## Bug

`StreamFullScan` returns `i` (the row index, 0-based) when `cb(i)` returns false:

```go
func (p *blockColumnProvider) StreamFullScan(cb vm.RowCallback) (int, error) {
    n := p.block.SpanCount()
    for i := range n {
        if !cb(i) {
            return i, nil  // BUG: i is a row index, not a delivered-row count
        }
    }
    return n, nil
}
```

All other `Stream*` methods in the file use a `count` variable and return it. The interface contract `vm.ColumnDataProvider.StreamFullScan` returns `(int, error)` where the int is the number of rows delivered. When `cb` returns false on row index 5, the function should return 0 (no rows were successfully delivered before the stop, since the current row was not delivered — `cb` returned false). Returning `5` is incorrect.

No current production code calls `StreamFullScan` with a callback that returns false (all callers go through `collectStream` which always returns true). However the function is part of a shared interface and the bug exists in the implementation.

## Trigger

Any caller of `StreamFullScan` that passes a callback returning false before all rows are seen.

## Impact

Wrong count returned. No current production impact.

## Fix

Track and return the delivered count:

```go
func (p *blockColumnProvider) StreamFullScan(cb vm.RowCallback) (int, error) {
    n := p.block.SpanCount()
    count := 0
    for i := range n {
        if !cb(i) {
            return count, nil
        }
        count++
    }
    return count, nil
}
```

## Acceptance Criteria

- When the callback returns false on row index `i`, `StreamFullScan` returns the count of rows delivered before that point (0 if the first call to `cb` returns false).
- All existing tests pass.
- No new functionality introduced.

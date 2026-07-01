# Fix nil-deref: SpanMatchFromRow panics when MatchedRow.Block is nil

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** bug-fix
**severity:** HIGH
**source:** bug-finder

## Location

`internal/modules/executor/executor.go:68,74-96` — `SpanMatchFromRow` / `spanMatchFromBlock`

## Bug

`SpanMatchFromRow` passes `row.Block` directly to `spanMatchFromBlock`. Inside `spanMatchFromBlock` at line 84, `block.GetColumn(traceIDCol)` is called without a nil guard.

`MatchedRow.Block` is nil for all results produced by the intrinsic fast-path (`collectFromIntrinsicRefs`, Cases A/B/C/D): those rows have `IntrinsicFields != nil` and `Block == nil`. The `Collect()` function may return such rows for any query with an intrinsic column predicate (resource.service.name, span:duration, span:kind, etc.) and `Limit > 0`.

Internal callers in `api.go` already check `row.IntrinsicFields != nil` before accessing `row.Block`, so they are safe. However, `SpanMatchFromRow` is an exported public function — any external caller receiving a `MatchedRow` from `Collect()` and passing it to `SpanMatchFromRow` will panic if the intrinsic fast-path was active.

## Trigger

External call to `SpanMatchFromRow(row, signalType)` where `row.Block == nil` (intrinsic fast-path result).

## Impact

Nil pointer dereference panic at `block.GetColumn(...)` — crashes the calling goroutine.

## Fix

Add a nil guard at the top of `spanMatchFromBlock`:

```go
func spanMatchFromBlock(block *modules_reader.Block, blockIdx, rowIdx int, signalType uint8) SpanMatch {
    m := SpanMatch{BlockIdx: blockIdx, RowIdx: rowIdx}
    if block == nil {
        return m
    }
    // ... rest of function unchanged
```

## Acceptance Criteria

- `SpanMatchFromRow` returns a zero-value `SpanMatch` (with only `BlockIdx` and `RowIdx` set) when `row.Block == nil`, rather than panicking.
- All existing tests pass.
- No new functionality introduced.

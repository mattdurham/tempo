# Cleanup: Extract min/max update logic from addRowFromProto/addRowFromBlock into a helper

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** go-idioms
**source:** discover-quality

## Location

- `internal/modules/blockio/writer/writer_block.go:532-551` (addRowFromProto, cyclo 31)
- `internal/modules/blockio/writer/writer_block.go:806-833` (finalizeRowBookkeeping, called from addRowFromBlock)

## Issue

The block-level min/max tracking for `minStart`/`maxStart` and `minTraceID`/`maxTraceID` is duplicated with near-identical if/else patterns in both `addRowFromProto` (lines 533-551) and `finalizeRowBookkeeping` (lines 807-833). The pattern:
```go
if b.spanCount == 0 {
    b.minStart = spanStart
    b.maxStart = spanStart
} else {
    if spanStart < b.minStart { b.minStart = spanStart }
    if spanStart > b.maxStart { b.maxStart = spanStart }
}
```
can be replaced with the Go 1.21 built-in `min`/`max` functions, which eliminates the if/else branching entirely:

```go
if b.spanCount == 0 {
    b.minStart = spanStart
    b.maxStart = spanStart
    b.minTraceID = traceID
    b.maxTraceID = traceID
} else {
    b.minStart = min(b.minStart, spanStart)
    b.maxStart = max(b.maxStart, spanStart)
    // bytes.Compare for traceID stays — no built-in for [16]byte
}
```

Additionally, `bytes.Compare(ps.traceID[:], b.minTraceID[:]) < 0` at lines 545/826 could be wrapped in a named helper `traceIDBefore(a, b [16]byte) bool` to make the intent clear and reduce duplication.

## Fix

1. Replace the `spanStart` min/max if/else chains in both functions with `min()`/`max()` calls.
2. Extract a `traceIDBefore(a, b [16]byte) bool` helper for the bytes.Compare traceID comparisons used in both locations.
3. This also removes the special-casing needed for `spanCount == 0` for numeric fields (since `min(0, spanStart) == 0` correctly handles first write when zero is not a valid timestamp).

**Note:** Validate that `spanStart == 0` is not a valid production value before removing the `spanCount == 0` guard for `minStart`. If zero start times are possible, keep the guard for `minStart`/`maxStart` but use min/max for the update branch.

**Complexity reduction:** Removes ~12 branch conditions across the two functions.

# Cleanup: Inline spanMatchFromBlock — single consumer, adds no value

**Status:** Pending
**task_type:** cleanup
**cleanup_type:** architecture
**severity:** LOW
**source:** discover-arch

## Location

`internal/modules/executor/executor.go:66-91`

## What to Inline

`spanMatchFromBlock` (lines 63-91) is called by exactly one function: `SpanMatchFromRow` (lines 59-61). `SpanMatchFromRow` does nothing except call `spanMatchFromBlock` with its arguments unpacked from a `MatchedRow`. The function body is ~22 lines.

**Steps:**

1. In `SpanMatchFromRow`, replace the single-line delegation with the body of `spanMatchFromBlock` directly:

```go
// Before:
func SpanMatchFromRow(row MatchedRow, signalType uint8) SpanMatch {
    return spanMatchFromBlock(row.Block, row.BlockIdx, row.RowIdx, signalType)
}

// After:
func SpanMatchFromRow(row MatchedRow, signalType uint8) SpanMatch {
    m := SpanMatch{BlockIdx: row.BlockIdx, RowIdx: row.RowIdx}
    if row.Block == nil {
        return m
    }
    traceIDCol := "trace:id"
    spanIDCol := "span:id"
    if signalType == modules_shared.SignalTypeLog {
        traceIDCol = "log:trace_id"
        spanIDCol = "log:span_id"
    }
    if col := row.Block.GetColumn(traceIDCol); col != nil {
        if v, ok := col.BytesValue(row.RowIdx); ok && len(v) == 16 {
            copy(m.TraceID[:], v)
        }
    }
    if col := row.Block.GetColumn(spanIDCol); col != nil {
        if v, ok := col.BytesValue(row.RowIdx); ok {
            m.SpanID = make([]byte, len(v))
            copy(m.SpanID, v)
        }
    }
    return m
}
```

2. Delete `spanMatchFromBlock`.

## Why

The exported/unexported split is not justified here — there is no other caller that needs the unexported version. The single-hop delegation adds a call frame with no clarification of intent. The 2-3 Rule: a helper with one consumer and no added logic should be inlined.

Note: the existing bug-fix task (`bugfix-nil-deref-SpanMatchFromRow-block-nil.md`) already handles the nil-block guard. This task is orthogonal but can be done after or together with that fix.

## Acceptance Criteria

- `spanMatchFromBlock` is deleted
- `SpanMatchFromRow` contains the inlined body
- All tests pass (`make test`)
- No new functionality introduced

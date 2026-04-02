# Task: Unify Trace and Log Execution Path Through Executor Module

## Goal

Both trace queries (`streamFilterProgram`) and log queries (`streamLogProgram`) currently
duplicate the block-scan loop inline in `api.go`. This task moves that loop into
`internal/modules/executor` as a new `Stream` method, so both signals flow through the same
package path. Signal-specific branching (timestamp column name, direction) lives inside
the module rather than in duplicated top-level functions.

## Background Research

See `.bob/state/brainstorm.md` for full analysis. Summary:

There are currently **three** places doing the same block-scan loop:

1. `executor.Execute()` — `internal/modules/executor/executor.go` — the proper module version,
   eager `FetchBlocks`, batch result `[]SpanMatch`, already has signal-type branching via
   `r.SignalType()`.
2. `streamFilterProgram` — `api.go:308` — duplicates executor logic inline, also eager.
3. `streamLogProgram` — `api.go:616` — lazy `CoalescedGroups`/`ReadGroup`, direction,
   per-row `log:timestamp` filter, stats. The good modern version, but only for logs.

## Target Package Flow (Post-Task)

```
TraceQL → traceqlparser → vm.CompileTraceQLFilter → program
  → executor.Stream(StreamOptions{TimestampColumn: ""})
      → queryplanner.PlanWithOptions
      → CoalescedGroups + ReadGroup (lazy ~8MB)
      → ColumnPredicate → callback(block, blockIdx, rowIdx)
  → api.go callback: NewSpanFieldsAdapter + extractIDs → SpanMatch → fn

LogQL → logqlparser → logqlparser.Compile → program
  → executor.Stream(StreamOptions{TimestampColumn: "log:timestamp"})
      → same executor path, per-row time filter applied
  → api.go callback: NewSpanFieldsAdapter + extractIDs → SpanMatch → fn
```

## Implementation Plan

### 1. New file: `internal/modules/executor/stream.go`

Add these types:

```go
// StreamOptions configures streaming execution for both trace and log signals.
type StreamOptions struct {
    Limit           int
    TimeRange       queryplanner.TimeRange
    Direction       queryplanner.Direction  // Forward or Backward (default Backward)
    // TimestampColumn is the column for per-row time filtering.
    // "" disables per-row filtering (trace mode).
    // "log:timestamp" for log files.
    TimestampColumn string
    OnStats         func(StreamStats)
}

// StreamStats reports block I/O after execution.
type StreamStats struct {
    TotalBlocks    int
    PrunedByTime   int
    PrunedByBloom  int
    PrunedByIndex  int
    SelectedBlocks int
    FetchedBlocks  int  // ≤ SelectedBlocks when limit causes early stop
}

// StreamCallback receives each matched row. Return false to stop iteration.
type StreamCallback func(block *modules_reader.Block, blockIdx, rowIdx int) bool
```

Add method on `*Executor`:

```go
func (e *Executor) Stream(
    r *modules_reader.Reader,
    program *vm.Program,
    opts StreamOptions,
    fn StreamCallback,
) error
```

Implementation:
1. `planner.PlanWithOptions(predicates, opts.TimeRange, PlanOptions{Direction, Limit})`
2. `defer opts.OnStats(...)` if set
3. `r.CoalescedGroups(plan.SelectedBlocks)` → groups
4. Lazy loop over `plan.SelectedBlocks`:
   - fetch group on first access (one ~8MB coalesced read)
   - `r.ParseBlockFromBytes`
   - `program.ColumnPredicate`
   - if `!opts.Forward`: reverse row slice
   - per-row: if `TimestampColumn != ""`, check start/end bounds
   - call `fn(block, blockIdx, rowIdx)` — stop if false or limit hit
5. Return nil

### 2. Update `api.go`

**`streamFilterProgram`** becomes:
```go
func streamFilterProgram(r *Reader, program *vm.Program, opts QueryOptions, fn SpanMatchCallback) error {
    return modules_executor.New().Stream(r, program, modules_executor.StreamOptions{
        Limit: opts.Limit,
    }, func(block *modules_reader.Block, blockIdx, rowIdx int) bool {
        fields := modules_blockio.NewSpanFieldsAdapter(block, rowIdx)
        traceIDHex, spanIDHex := extractIDs(block, rowIdx)
        match := &SpanMatch{Fields: fields, TraceID: traceIDHex, SpanID: spanIDHex}
        if !fn(match, true) {
            fn(nil, false)
            return false
        }
        return true
    })
}
```

**`streamLogProgram`** becomes similar wrapper passing:
- `TimestampColumn: "log:timestamp"`
- `Direction`, `TimeRange`, `Limit`, `OnStats` from `opts`

### 3. Spec files — `internal/modules/executor/`

- **`SPECS.md`**: Add SPEC entry for `Stream` (lazy fetch invariant, direction, per-row filter)
- **`NOTES.md`**: Dated entry explaining Stream vs Execute (streaming callback vs batch result;
  lazy coalesced vs eager fetch)
- **`TESTS.md`**: Test cases for trace path, log path, direction, early-stop with
  `FetchedBlocks < SelectedBlocks`

### 4. Out of scope for this task

- `logql/engine.go` `StreamLogs` — also has an inline lazy loop (pipeline path only);
  can be unified in a follow-on task
- `streamStructuralQuery` — separate structural query path, not in scope
- `executor.Execute()` — keep as-is for batch callers (tempoapi, tests)

## Files to Touch

| File | Change |
|---|---|
| `internal/modules/executor/stream.go` | **New** — Stream, StreamOptions, StreamStats, StreamCallback |
| `internal/modules/executor/executor.go` | Package doc update only |
| `internal/modules/executor/SPECS.md` | Add Stream SPEC entry |
| `internal/modules/executor/NOTES.md` | Add dated design note |
| `internal/modules/executor/TESTS.md` | Add test scenarios |
| `api.go` | Replace `streamFilterProgram` and `streamLogProgram` inline loops |

## Acceptance Criteria

- [ ] `go build ./...` passes
- [ ] `make precommit` passes (format, lint, complexity, coverage)
- [ ] Trace queries produce identical results before and after
- [ ] Log queries produce identical results (direction, time range, limit, stats all preserved)
- [ ] `streamFilterProgram` and `streamLogProgram` contain no inline fetch loops
- [ ] `executor.Stream` tested for both signal paths, direction, and early-stop stats
- [ ] Spec files updated

## Key Invariants to Preserve

- Lazy fetch: `FetchedBlocks` ≤ `SelectedBlocks` when limit causes early stop
- Direction: trace path gets Forward/Backward for free (even if not used today)
- Per-row time filter: only applied when `TimestampColumn != ""`
- `fn(nil, false)` sentinel still fired on normal completion (api.go callback handles this)
- Single I/O per coalesced group — never per-column or per-block reads

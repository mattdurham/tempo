# valuecounts — Design Notes

Non-obvious design decisions, rationale, and invariants for the `internal/modules/valuecounts`
package (VCNT value-counts section, issue #400).

---

## NOTE-VC-001 — Signed counts express retention/compaction as delta accounting

Date: 2026-06-25

A VCNT `Record.Count` is a signed `int64`. Positive counts are introductions (a value appears
in N spans within a time window in this file); negative counts are accounting deltas — a prior
file's contribution being subtracted because the source block was deleted by retention or
superseded by a compacted output.

`Compact` groups by `(ColumnName, TimeStart, TimeEnd, Value)`, sums `Count`, and **drops any
group whose sum is <= 0**. This makes the count files self-healing under retention: the value
disappears from query results exactly when its net live count reaches zero, with no separate
tombstone state to track. The compactor emits `Negate(r)` for each consumed input block's
record so that values present in both input and output net to zero churn.

Back-ref: `Compact`, `Negate` in compaction.go / record.go.

---

## NOTE-VC-002 — TimeEnd is part of the merge key

Date: 2026-06-25

`mergeKey` includes both `TimeStart` and `TimeEnd`. Two records for the same value that share a
`TimeStart` but cover different windows are intentionally NOT merged — they describe different
time intervals and their counts are independent. Excluding `TimeEnd` from the key would conflate
a `[100,200]` window with a `[100,300]` window and corrupt time-bounded query results.

---

## NOTE-VC-003 — Snappy-chunked section with a (column, time_start) directory

Date: 2026-06-25

The VCNT section is split into independently-snappy-compressed chunks of up to
`shared.ValueCountsRecordsPerChunk` records. The chunk directory stores each chunk's first
`(MinColumn, MinTimeStart)` and byte extent, mirroring the value index VINX layout. A
time-bounded lookup (`DecodeTimeRange` / `ValuesInRange`) skips chunks whose `MinTimeStart`
exceeds the query's `maxTS`. Because records within a chunk can carry differing windows, a
per-record overlap test (`TimeStart <= maxTS && TimeEnd >= minTS`) is still applied after decode.

The directory is sorted by `(MinColumn, MinTimeStart)`, not `MinTimeStart` alone, so a
later-listed chunk for a *different* column may reset `MinTimeStart` to a small value. The skip
is therefore a `continue`, never a `break`.

Back-ref: `DecodeTimeRange` in section.go.

---

## NOTE-VC-004 — Top-N and cardinality share one summation pass over live values

Date: 2026-06-30

Issue #400 was reopened to add the tag-value autocomplete / dropdown primary use case
("top 10 values for column between T1 and T2") plus the cardinality gate for cube creation
(#445). `ValuesInRange`, `TopNInRange`, and `CardinalityInRange` all answer questions about the
same underlying set: the distinct values of a column whose net live count over the query window
is `> 0`. They are therefore implemented on a single unexported helper, `sumLiveValues`, which
decodes the overlapping chunks once, sums `Count` per value for the requested column, and drops
any value whose net count is `<= 0` (the same liveness rule as `Compact`, NOTE-VC-001).

The three public entry points differ only in how they shape that set:

  - `ValuesInRange` — sorts by `Value` ascending (stable listing / merge-friendly order).
  - `TopNInRange` — sorts by `Count` descending, **tie-broken by `Value` ascending** so the
    ranked result is deterministic for a given input, then truncates to `n` (n <= 0 => all).
  - `CardinalityInRange` — returns the count of live distinct values.

None of these open a blockpack data file — they read only the consolidated `.vcnt` section, so a
dropdown lookahead or a cube cardinality gate never touches a span. The deterministic tie-break
matters because two equally-frequent values must rank in a stable order across repeated queries
and across compaction (which can reorder records but not change per-value sums).

Back-ref: `sumLiveValues`, `TopNInRange`, `CardinalityInRange` in query.go.

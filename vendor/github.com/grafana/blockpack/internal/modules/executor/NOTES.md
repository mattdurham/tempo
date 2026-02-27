# executor — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/modules/executor` package.

---

## 1. Responsibility Boundary
*Added: 2026-02-10*

The executor owns span-level evaluation. Block selection (which blocks to read) is
delegated to `queryplanner`; raw I/O (coalescing, wire parsing) is delegated to
`blockio/reader`. The executor only decides which spans within a selected block match the
query.

---

## 2. Execute Takes *reader.Reader, Not BlockIndexer
*Added: 2026-02-10*

**Decision:** `Execute` takes `*reader.Reader` directly rather than `queryplanner.BlockIndexer`.

**Rationale:** After block selection, the executor needs `ParseBlockFromBytes` and
`BlockMeta` — methods that are on `*reader.Reader` but not part of `BlockIndexer`.
Extending `BlockIndexer` with parsing methods would conflate two distinct concerns:
index lookup (what the planner needs) and block decoding (what the executor needs).
Using `*reader.Reader` directly is explicit and avoids unnecessary interface widening.

`*reader.Reader` still satisfies `BlockIndexer` via Go structural typing, so the planner
works without a type assertion.

---

## 3. ColumnDataProvider for Modules Block
*Added: 2026-02-10*

**Decision:** The executor implements `vm.ColumnDataProvider` directly against
`*reader.Block` (modules format) in `column_provider.go`, rather than converting to the
old `blockpackio.Block` type via the adapter in `internal/modules/blockio/adapter.go`.

**Rationale:** The adapter conversion copies every column dictionary, which allocates
significantly more memory than necessary. The modules `Block` type exposes its columns
directly via `GetColumn`, making a direct implementation straightforward and allocation-free.

---

## 4. RowSet Is Sorted by Construction
*Added: 2026-02-10*

**Decision:** `rowSet` maintains rows in ascending order. Stream scans iterate rows
0..n-1 sequentially, so `Add` is called in ascending order. `ToSlice` returns the
backing slice directly without re-sorting.

**Rationale:** Sorted row indices are required for:
- `Contains` (binary search, O(log n))
- `Union/Intersect/Complement` (merge-style algorithms, O(n+m))
- Deterministic output ordering

---

## 5. SpanMatch Contains Block and Row Indices
*Added: 2026-02-10*

**Decision:** `SpanMatch` includes `BlockIdx` and `RowIdx` in addition to `TraceID` and
`SpanID`.

**Rationale:** The caller (e.g. a Tempo query handler) may want to fetch additional column
values for matched spans without re-scanning the block. Having `BlockIdx` and `RowIdx`
allows targeted column access via `ParseBlockFromBytes` + `GetColumn(name).StringValue(rowIdx)`.

---

## 6. Removed: Predicate Extraction and Dedicated Index Pruning
*Added: 2026-02-25*

**Decision:** The `predicates.go` file and the `extractPredicates` function were removed.
The executor now passes `nil` predicates to `queryplanner.Planner.Plan`.

**What was removed:**
- `predicates.go` — `extractPredicates`, `encodeValue`: converted `vm.Program.Predicates.RangeColumns`
  to `[]queryplanner.Predicate` by encoding `vm.Value` instances to the range-index
  wire format (8-byte LE for numerics, raw string for strings).
- The call `extractPredicates(program, r)` in `Execute`.

**Rationale:** The range index was removed from `queryplanner.BlockIndexer`
(see `queryplanner/NOTES.md §7`). Without the `RangeColumnType` and `BlocksForRange`
methods on the interface, predicate extraction is a no-op. Block selection now relies solely
on bloom-filter pruning; all further filtering happens at span-evaluation time via
`program.ColumnPredicate`.

**Addendum (2026-02-25):** §6 was superseded. The executor no longer passes `nil` to
`planner.Plan`. It calls `buildPredicates(r, program)` (in `predicates.go`), which
extracts column names and encoded values from `program.Predicates` and builds
`[]queryplanner.Predicate` for bloom-filter and range-index block pruning. For AND queries
(`HasOROperations = false`), one `Predicate` per column is created — range columns also
carry `Values` (wire-encoded) and `ColType` for range-index pruning; non-range columns
get bloom-only `Predicate` values. For OR queries (`HasOROperations = true`), one
`Predicate` with all columns is created (no `Values`) — bloom pruning only. This gives
two-stage block-level pruning. See queryplanner/NOTES.md §8 for range-pruning design.

---

## 7. Integration Coverage via blockio/executor_test.go
*Added: 2026-02-25*

**As of the modules executor migration:** the primary integration tests for this
executor live in `internal/modules/blockio/executor_test.go` (EX-01 through EX-07),
not only in `internal/modules/executor/executor_test.go`.

`internal/modules/blockio/executor_test.go` exercises the full round-trip:
writer (`modules_blockio.NewWriterWithConfig`) → reader
(`modules_reader.NewReaderFromProvider`) → this executor (`New().Execute`).

**Coverage provided by the blockio integration tests:**
- `bloomPredicates` with real data (AND and OR query paths, EX-01, EX-03, EX-04, EX-06)
- `Execute` empty-file short-circuit (EX-05)
- Multi-block scanning with `BlocksScanned >= 2` assertion (EX-04)
- Zero-match result (EX-02)

`internal/modules/executor/executor_test.go` additionally covers:
- `Options.Limit` early-exit (EX-08)
- `SpanMatch.TraceID` / `SpanMatch.SpanID` field population (EX-09)
- `result.Plan.TotalBlocks` populated (EX-10)

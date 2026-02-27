# queryplanner — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`queryplanner` package. These notes complement SPECS.md and are intended to prevent
re-introducing decisions that were deliberately reversed.

---

## 1. Why a Separate Package?
*Added: 2026-02-10*

**Decision:** Block selection logic lives in `queryplanner`, not in `blockio/reader` or the
executor.

**Rationale:** The `blockio/reader` package is responsible for *how* to read blocks
(coalescing, wire parsing, caching). Deciding *which* blocks to read is a distinct concern.
Mixing them would couple the storage layer to query semantics and prevent alternative
executors from reusing the same pruning logic.

---

## 2. BlockIndexer Interface
*Added: 2026-02-10*

**Decision:** `Planner` depends on the `BlockIndexer` interface, not on `*reader.Reader`
directly.

**Rationale:** Go structural typing means `*reader.Reader` satisfies `BlockIndexer` without
any changes to the reader package. The interface:
- Allows the planner to be tested with a lightweight in-memory stub.
- Allows alternative storage backends to be plugged in.
- Makes the dependency explicit: the planner only needs `BlockCount`, `BlockMeta`, and
  `ReadBlocks`.

**Consequence:** `selection.go` must not import `blockio/reader` — it must operate on
`BlockIndexer` only.

---

## 3. Bloom Filter Is the Only Pruning Stage
*Added: 2026-02-10*

**Decision:** The planner performs exactly one pruning stage: column-name bloom filter.

**Rationale:** A bloom filter miss is a cheap, reliable "definitely absent" signal — no I/O,
O(1) per block. Any block whose bloom filter reports a queried column as absent cannot
contain matching spans and is safely dropped. This is the highest-leverage pruning available
without touching index structures.

---

## 4. FetchBlocks Is a Thin Delegation
*Added: 2026-02-10*

**Decision:** `FetchBlocks` does not implement coalescing or I/O itself. It delegates
directly to `BlockIndexer.ReadBlocks`.

**Rationale:** Coalescing strategy (gap size, waste ratio) belongs to the storage layer, not
the planner. The `reader.Reader` implementation uses `AggressiveCoalesceConfig` (4 MB gap,
100% waste ratio), which is appropriate for object storage. If a different backend requires
different coalescing, it can implement `ReadBlocks` accordingly.

**Addendum (2026-02-25):** `FetchBlocks` → `BlockIndexer.ReadBlocks` is now the canonical
call site for coalesced block I/O in the modules execution path. Previously, the old
`ModulesBlockReader` adapter's `CoalesceBlocks` returned one read per block (no actual
merging). The new path through `reader.ReadBlocks` uses `AggressiveCoalesceConfig`,
merging adjacent blocks into bulk reads. See also blockio/NOTES.md §13 addendum.

---

## 5. Sorted SelectedBlocks
*Added: 2026-02-10*

**Decision:** `Plan.SelectedBlocks` is always sorted in ascending order.

**Rationale:**
- The `ReadBlocks` coalescing algorithm requires sorted input to identify adjacent ranges.
- Sorted output is predictable and easier to verify in tests.
- `setToSortedSlice` converts the `map[int]struct{}` candidate set to a sorted slice in
  O(n log n), which is negligible compared to the subsequent I/O.

---

## 6. No Caching in the Planner
*Added: 2026-02-10*

**Decision:** The planner holds no mutable state beyond a reference to `BlockIndexer`. It
performs no caching of plan results.

**Rationale:** The `BlockIndexer` implementation (`reader.Reader`) handles caching at the
storage layer. Adding a second cache layer in the planner would complicate invalidation
without meaningful benefit. Each `Plan` call is stateless and safe for concurrent use if
the underlying `BlockIndexer` is.

---

## 7. Removed: Range Column Index
*Added: 2026-02-25*

**Decision:** The cross-block range index was removed from the queryplanner.

**What was removed:**
- `RangeColumnType(col string) (shared.ColumnType, bool)` from `BlockIndexer`
- `BlocksForRange(col string, key RangeValueKey) ([]int, error)` from `BlockIndexer`
- `Values []string` and `ColType shared.ColumnType` from `Predicate`
- `PrunedByIndex int` from `Plan`
- The two-stage pruning loop in `Plan()` (bloom → range index)
- The entire `applyPredicate` function and `pruneResult` struct from `selection.go`
- The `predicates.go` file in the executor package (which encoded vm.Values to wire format)

**Rationale:** The range index added significant interface surface, correctness
constraints (wire encoding, OR-query safety, bucket boundary invariants), and tight coupling
between the planner and the blockio format's internal range-bucket representation. The
bloom filter alone provides meaningful pruning for the common case (column absent from a
block). Span-level evaluation in the executor handles all remaining filtering correctly.
Dedicated index pruning can be reintroduced as a separate, optional layer without
modifying the planner's core interface.

**Addendum (2026-02-25):** Dedicated index pruning was re-added as described in §8.
`RangeColumnType` and `BlocksForRange` are back in `BlockIndexer`.
`Predicate.Values`, `Predicate.ColType`, and `Plan.PrunedByIndex` are back.
`predicates.go` in the executor encodes `vm.Values` to wire format again.
The rationale for removal stands: the range index is now an optional second stage
that is only used when `Predicate.Values` is non-empty — the bloom-only path
remains the default when no equality predicates are present.

---

## 8. Range-Index Pruning Design
*Added: 2026-02-25*

**Decision:** Range-index pruning is a second stage after bloom-filter pruning, not a
replacement. Both stages operate on the same candidate set in sequence.

**Design:**
1. `Predicate` gains `Values []string` (wire-encoded query values) and
   `ColType shared.ColumnType` (needed to call `BlocksForRange`).
2. `pruneByIndex` in `selection.go` is called after `pruneByBloom` for each predicate.
   It calls `r.BlocksForRange(col, val)` for each value in `pred.Values`, unions
   the resulting block sets, then intersects with the current candidates. Blocks not in
   the union are removed (pruned by index).
3. `Plan.PrunedByIndex` tracks the additional blocks eliminated by index lookup.
4. Range pruning is skipped when `len(pred.Values) == 0` or `len(pred.Columns) != 1`
   (OR bloom predicates use multiple columns; range lookup requires exactly one column).

**Concrete impact:** `{ resource.service.name = "auth" }` on 100 blocks each with 2000
distinct service names:
- Bloom only: all 100 blocks pass (service.name is present in every block)
- Range pruning: only the ~1-2 blocks whose range bucket includes "auth" are selected

**OR query safety:** For OR queries (`HasOROperations = true`), `buildPredicates` in
the executor produces a single combined bloom predicate with no `Values`. This means
OR queries get bloom pruning only. Range pruning for OR queries would require unioning
block sets across all OR branches, which risks false negatives when one branch is
unindexed. The conservative choice is correct: bloom-only for OR queries.

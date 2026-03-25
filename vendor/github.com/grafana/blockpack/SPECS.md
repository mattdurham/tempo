# Blockpack — Top-Level Architectural Specifications

*Added: 2026-03-24*

These invariants apply across the entire codebase. Module-level specs
(`internal/modules/*/SPECS.md`) inherit these and may add more specific constraints.

---

## ARCH-001: Single Read Path

All block data fetches from storage MUST flow through a single code path:
`ReadCoalescedBlocks` (via `Reader.ReadGroup` or `Reader.ReadGroupColumnar`).

No executor, planner, or API code may call the storage provider directly to
fetch block bytes. This ensures that pooling, coalescing, caching, and
observability are applied uniformly.

**Rationale:** Multiple read paths create maintenance burden and make it
impossible to enforce cross-cutting concerns (buffer pooling, metrics,
cache integration) consistently. Discovered through pprof analysis that showed
`ReadCoalescedBlocks` as the #1 allocation hot spot — fixing one path is only
effective if it is the only path.

**Back-ref:** `internal/modules/blockio/reader/coalesce.go:ReadCoalescedBlocks`,
`internal/modules/blockio/reader/reader.go:ReadGroup`

---

## ARCH-002: Single Decode Path

All block bytes-to-column decoding MUST flow through `parseBlockColumnsReuse`
with an explicit `wantColumns` set.

No code may decode block bytes directly (e.g., by manually parsing column
metadata and calling individual decode functions outside of `parseBlockColumnsReuse`).

**Rationale:** Column decode is the dominant CPU cost after I/O. A single
decode path ensures that `wantColumns` projection (skip non-needed columns),
intern-string reuse, and lazy-column registration are applied consistently.
Multiple decode paths would require duplicating these optimizations and would
make future improvements (e.g., sub-block column I/O per SPEC-005) harder to
land.

**Back-ref:** `internal/modules/blockio/reader/block_parser.go:parseBlockColumnsReuse`,
`internal/modules/blockio/reader/reader.go:ParseBlockFromBytes`

---

## ARCH-003: Separation of I/O and Decode

The I/O phase (fetching raw bytes from storage) and the decode phase
(parsing bytes into typed column data) MUST be kept separate:

- **I/O phase**: `ReadGroup` / `ReadCoalescedBlocks` — produces `map[blockIdx][]byte`.
  Must not perform any column parsing.
- **Decode phase**: `ParseBlockFromBytes` / `parseBlockColumnsReuse` — consumes
  `[]byte`, produces a `*Block` with typed columns. Must not issue any I/O.

**Rationale:** Separation enables the parallel I/O pattern in `forEachBlockInGroups`
(all groups fetched concurrently, then parsed sequentially). Mixing I/O and decode
would prevent parallelising the latency-bound fetch phase while keeping the
CPU-bound parse phase on a single goroutine.

**Back-ref:** `internal/modules/executor/stream.go:forEachBlockInGroups`

---

## ARCH-004: Buffer Ownership — Pool and Release

Raw block byte buffers follow strict ownership rules:

1. **Allocation**: `ReadCoalescedBlocks` allocates one pooled read buffer per
   coalesced group. Per-block copies are made immediately; the pooled buffer is
   returned before the function returns.
2. **Lifetime**: Each per-block `[]byte` is owned by the `fetched` map entry.
   It MUST be released (via `delete(fetched, blockIdx)`) immediately after
   `ParseBlockFromBytes` returns — not held until the enclosing function exits.
3. **No aliasing**: After `ParseBlockFromBytes` completes, the `[]byte` MUST NOT
   be retained by any live `*Column` value. All column data is decoded into
   independent allocations during the decode phase.

**Rationale:** Holding block bytes past their decode point inflates the GC live
set and delays collection. Measured impact: releasing bytes immediately in
`forEachBlockInGroups` reduced post-stress memory from 11 GiB to 4 GiB.

**Back-ref:** `internal/modules/blockio/reader/coalesce.go:coalescedReadPool`,
`internal/modules/executor/stream.go:forEachBlockInGroups`

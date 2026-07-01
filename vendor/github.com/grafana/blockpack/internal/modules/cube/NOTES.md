# Cube Module Design Notes

## NOTE-CUBE-004: Snappy Chunking at 2048 Cells

**Date:** 2026-06-29  
**Decision:** Chunk size set to 2048 cells nominal (24 KB raw → ~8-12 KB compressed).

**Rationale:**

- Matches value index precedent (`internal/modules/valueindex/entries.go` uses 2048 entries/chunk)
- VCNT uses 4096 records/chunk (precedent for snappy-chunked sections)
- 2048 cells × 12 bytes = 24 KB raw fits comfortably in L2 cache (~256 KB)
- Snappy decompression of 8-12 KB chunks is fast (~50-100 μs, proven in benchmarks)
- Balances decompression overhead (too small → many decompress ops) vs memory footprint (too large → high RSS)

**Alternative considered:** 512, 1024, 4096 cells. 512 too small (excessive chunk directory overhead), 4096 too large (memory pressure for sparse queries).

**Measurement plan:** BENCH-CUBE-002 will benchmark varying chunk sizes (512, 1024, 2048, 4096) to validate choice.

---

## NOTE-CUBE-005: Binary Search (O(log n)) vs Arithmetic (O(1))

**Date:** 2026-06-29  
**Decision:** Use binary search on sorted cells (O(log n)) for random access, not arithmetic on dense grid (O(1)).

**Rationale:**

- **Sparse storage dominates:** At realistic 1-10% fill, sparse 12B cells save 85-98% storage vs dense 8B cells (see brainstorm storage calculations).
- **O(log n) acceptable latency:** Binary search over 10k cells = ~14 comparisons = 10-50 μs. Query latency dominated by S3 GET (50-100ms), so binary search is <0.1% overhead.
- **Chunk directory amortizes:** Range queries (`GetCellsInRange`) scan multiple adjacent cells after one binary search on the directory.
- **Proven pattern:** SpanTree (`internal/modules/blockio/shared/constants.go:140-155`) uses fixed-stride records + binary search successfully at large scale.

**Alternative considered:** Dense 8-byte position-derived cells (ticket #442 original proposal). Rejected because:

1. Contradicts lth prior decision (memory `6cff0daf`: "sparse data with gaps break position-based indexing")
2. Wastes 85-98% storage at realistic sparse fill
3. Ticket's 6.9 GB/30d storage number implies high cardinality (not 100×20 dense), reinforcing sparse is correct

**Measurement plan:** BENCH-CUBE-003 will measure GetCell latency (target: <50 μs for 10k cells, cold cache).

---

## NOTE-CUBE-006: Sort Order (Minute First, Then Dimensions)

**Date:** 2026-06-29  
**Decision:** Sort cells by `(Minute ASC, Dim1ID ASC, Dim2ID ASC)`.

**Rationale:**

- **Time-first for chunk pruning:** Chunk directory stores `min_minute` per chunk. Sorting by minute first enables range queries to skip entire chunks where `min_minute > max_query_minute` (O(log chunks) scan, not O(chunks) linear scan).
- **Secondary sort on dimensions for merge:** During compaction (L0 → L1 → L2), merging multiple files with the same sort order is a standard multi-way merge-sort (proven in VCNT `internal/modules/valuecounts/compaction.go:15-85`).
- **Matches VCNT precedent:** VCNT sorts by `(ColumnName, TimeStart, Value, Count)` — time is the primary key for range-based pruning.

**Alternative considered:** `(Dim1ID, Dim2ID, Minute)` (dimension-first). Rejected because chunk pruning by minute is the dominant query pattern (time-bounded metrics queries like `rate_over_time[5m]`).

**Query implications:** Queries like "all cells for service=auth, status=200, time=[T1..T2]" benefit from sorted-by-minute order:

1. Binary search chunk directory for chunks in [T1, T2] range
2. Decompress only those chunks (not whole file)
3. Within-chunk binary search for (minute, dim1_id, dim2_id) tuples

---

## NOTE-CUBE-007: Accumulator is single-minute; the caller rotates buckets

_Added: 2026-06-29 (issue #443)_

The accumulator is deliberately scoped to **one** minute bucket, fixed at construction. The
ingest loop (out of scope for #443) owns one accumulator per active cube and, on each
wall-clock minute boundary (and on shutdown), calls `FlushTo` — which writes the file and
`Reset`s the accumulator to the next minute. Keeping the minute out of the per-span hot path
means `Add` does zero time math; the bucket is implicit.

`Add` interns dimension strings into the accumulator's own `Dictionary` and counts into a
`map[cellKey]uint32` keyed by the interned `(dim1_id, dim2_id)`. At `Encode`, IDs are resolved
back to strings and fed to a fresh `cube.Writer`, which re-interns them into the per-file
dictionary. The double-intern is intentional: in-memory IDs only need to be internally
consistent for counting, and re-interning once per minute (not per span) is negligible while
keeping the file's dictionary self-contained (NOTE-CUBE-002, per-file scope).

`cube.Writer` gained `Encode() ([]byte, error)` so the accumulator can produce bytes for the
object store without a temp file; `Flush(path)` now calls `Encode` then writes atomically.

**Back-ref:** `internal/modules/cube/accumulator.go:Add,Encode,FlushTo,Reset`

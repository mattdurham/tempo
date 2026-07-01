# Cube Module Specifications

## Module Responsibility

The cube module implements the binary on-disk format for pre-aggregated metrics cubes. A cube file (`.cube`) stores sparse 12-byte cells representing pre-computed span counts per (minute, dimension1, dimension2) tuple. The format enables random access via binary search on sorted cells and supports snappy-chunked storage for efficient compression and range queries.

---

## SPEC-CUBE-007: Chunk Format (Snappy-Compressed)

**Invariant:** Cells are stored in independently snappy-compressed chunks, with a nominal chunk size of 2048 cells (24 KB raw → ~8-12 KB compressed).

**Wire format:**

```
[Chunk Payload] snappy-compressed(cell_count[2] + cells:(minute[4]+dim1_id[2]+dim2_id[2]+count[4])×count)
```

**Rationale:** Snappy-chunked storage enables:

- O(1) decompression per chunk (not whole-file decompress)
- Bounded memory footprint (decompress only needed chunks)
- Efficient range queries (skip chunks via directory)

**Bounds check:** `cell_count` MUST be validated before allocating the slice (`if cell_count > 65536 { return error }`).

**Back-ref:** `internal/modules/cube/chunk.go:EncodeChunk`, `internal/modules/cube/chunk.go:DecodeChunk`

---

## SPEC-CUBE-008: Chunk Directory Format

**Invariant:** A chunk directory records one entry per chunk: (min_minute, comp_off, comp_len), enabling O(log chunks) binary search to find target chunks.

**Wire format:**

```
[Chunk Directory]
  dir_count[4]
  entries: dir_count × ChunkDirEntry {
    min_minute[4]  // first cell's minute in this chunk
    comp_off[4]    // byte offset from chunks_section_start
    comp_len[4]    // snappy-compressed length
  }
```

**Sort order:** Entries MUST be sorted by `min_minute` ASC to enable binary search.

**Back-ref:** `internal/modules/cube/chunk.go:ChunkDirEntry`, `internal/modules/cube/chunk.go:EncodeChunkDirectory`, `internal/modules/cube/chunk.go:DecodeChunkDirectory`

---

## SPEC-CUBE-009: Random Access via Binary Search

**Invariant:** Cell lookup is O(log chunks) + O(log cells_per_chunk) via binary search on:

1. Chunk directory (find chunk where `min_minute >= target_minute`)
2. Decompressed chunk cells (find cell where `(minute, dim1_id, dim2_id) == target`)

**Algorithm:**

```
GetCell(minute, dim1, dim2):
  1. Lookup dim1, dim2 → dim1_id, dim2_id (O(1) via Dictionary reverse maps)
  2. Binary search chunk directory for first entry where min_minute >= minute
  3. Decompress target chunk (snappy.Decode)
  4. Binary search within chunk for (minute, dim1_id, dim2_id) tuple
  5. Return count or (0, false) if not found
```

**Pruning:** Range queries (`GetCellsInRange`) skip chunks where `min_minute > max_query_minute`.

**Back-ref:** `internal/modules/cube/reader.go:GetCell`, `internal/modules/cube/reader.go:GetCellsInRange`

---

## SPEC-CUBE-010: Round-Trip Invariant

**Invariant:** All encode/decode operations MUST satisfy round-trip correctness:

- `DecodeChunk(EncodeChunk(cells)) == cells`
- `DecodeChunkDirectory(EncodeChunkDirectory(dir)) == dir`
- Writer → Reader end-to-end: `Reader.GetCell(minute, dim1, dim2)` returns the exact count written by `Writer.AddCell(minute, dim1, dim2, count)`

**Test strategy:** Every encode/decode function has a corresponding round-trip test (TEST-CUBE-007, TEST-CUBE-008, TEST-CUBE-012).

**Back-ref:** `internal/modules/cube/chunk_test.go:TestChunkEncodeDecodeRoundTrip`, `internal/modules/cube/reader_test.go:TestReaderEndToEndRoundTrip`

## SPEC-CUBE-011: Ingest accumulation and per-minute flush (issue #443)

`Accumulator` counts spans into per-`(dim1_id, dim2_id)` cells for a single minute
bucket, entirely in memory (no per-span I/O), then encodes one #442 cube file at flush.

**Contract:**

- `NewAccumulator(def, minute)` binds the accumulator to one minute bucket.
- `Add(span)` returns `(counted, err)`: a span is **skipped** (`counted=false`, `err=nil`)
  when it lacks either dimension column or fails any of `def.Filters`; it is **counted**
  (the matching cell's `uint32` count increments) otherwise. An error is returned only when
  a dimension dictionary is exhausted (>65535 distinct values).
- `NumericFilter(col, op, threshold)` rejects a span whose `col` is absent (the filter
  cannot be satisfied) — e.g. a `duration < threshold` cube never counts a span without a
  duration, and never counts one whose duration violates the bound.
- `Encode()` stamps **every** cell with the accumulator's minute, so the resulting file has
  `MinMinute == MaxMinute == minute` (a partial-minute / shutdown flush carries the correct
  single-minute range).
- `FlushTo(store, tenant)` encodes and `Put`s the file at `Filename(tenant, id)` then `Reset`s;
  an idle minute (no cells) is a no-op returning `("", nil)`.
- `Reset(minute)` clears cells + dictionary and rebinds the minute — flushed spans are never
  recounted (no double counting).
- `Filename(tenant, id)` = `<tenant>/cubes/<hex id>/L0-<xid>.cube`.

**Back-ref:** `internal/modules/cube/accumulator.go`, `internal/modules/cube/writer.go:Encode`

## SPEC-CUBE-012: CubeDefinition / RegistryEntry (issue #444)

`RegistryEntry` is the full, stable description of one active cube stored in `<tenant>/cubes/index.json`.
`CubeID = hex(SHA256(tenant+sorted(dims)+sorted(filters))[:8])` — deterministic so concurrent creators converge.
`DefFilterOp` (GT/GTE/LT/LTE/EQ) is the wire/JSON form; separate from the runtime `FilterOp` used by the accumulator.

**Back-ref:** `internal/modules/cube/definition.go`

## SPEC-CUBE-013: CardinalityGate (issue #445)

`CheckCardinality` guards cube creation by checking per-dimension distinct-value counts and
estimated combined (dim1×dim2) cells against configurable limits using value count index data.
UUID/high-entropy columns are always rejected. Returns `*CardinalityError` with an actionable
reason string and suggestion. The caller supplies pre-fetched VCNT data+dir — zero S3 I/O inside
the gate.

Default limits: MaxDistinctPerDim=1000, MaxCombinedCells=50000.

**Back-ref:** `internal/modules/cube/cardinality.go`

## SPEC-CUBE-014: Registry (issue #444)

`Registry` persists the per-tenant cube index in object storage with S3 conditional-PUT concurrency
control. Concurrent writers compute the same deterministic CubeID; only one conditional PUT succeeds —
the rest see a 412/ErrConflict and re-read. Up to 5 exponential-backoff retries (50ms base, doubling).
`Add` is idempotent. `Remove` is idempotent. Per-tenant limit (default 1000) enforced before Add.

**Back-ref:** `internal/modules/cube/registry.go:Registry`

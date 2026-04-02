# Perf: Parallelize buildBlock across blocks

**Status:** TODO
**Created:** 2026-04-02
**Priority:** HIGH
**Effort:** 2-3 days

## Motivation

Profiling of the blockpack shadow pod in tempo-dev-02 (2026-04-02) shows blockpack
consuming only ~25% more total CPU than the parquet shadow pod, yet achieving 2.7x
worse catch-up throughput (2.8s/s vs 7.6s/s lag reduction). The gap is not raw compute
but concurrency: parquet uses `ConcurrentRowGroupWriter` to parallelize column writes
across goroutines; blockpack's `buildBlock` is entirely single-threaded.

`buildBlock` accounts for ~30% of total CPU. Each block's content is independent —
column encoding, sketch computation, min/max tracking, and slab allocation touch no
shared state between blocks.

## Approach

Fan-out / fan-in pattern:

1. **Fan-out**: `flushBlocks` already splits pending spans into per-block groups.
   Launch one goroutine per group calling `buildBlock(...)` → `builtBlock{}`.
   Each goroutine gets its own `blockBuilder` (from a pool) and `zstdEncoder`.

2. **Fan-in**: collect `builtBlock` results in original order, then sequentially:
   - Assign block IDs (0, 1, 2, ...)
   - Write serialized block bytes to output
   - Update `traceIndex`, `rangeIdx`, `sketchIdx`

## Constraints

- **Trace index** (`traceIndex map[[16]byte][]uint16`): block IDs must be assigned
  in order, but the index update is O(1) per trace and happens at fan-in time — not
  a blocker.
- **`intrinsicAccum`**: currently fed row-by-row during `buildBlock`. Must be split
  into per-block accumulators and merged at fan-in, or fed at fan-in from the
  `builtBlock` result. This is the most complex part of the refactor.
- **`blockBuilder` reuse**: `w.bb` is a single reused instance. Replace with a
  `sync.Pool` of `*blockBuilder` objects.
- **`zstdEncoder`**: one per Writer, stateful. Pool alongside `blockBuilder`.
- **Output ordering**: fan-in must write blocks in the original group order to keep
  the file-level byte stream deterministic.

## Files

- `internal/modules/blockio/writer/writer.go` — `flushBlocks`, `buildAndWriteBlock`
- `internal/modules/blockio/writer/writer_block.go` — `buildBlock`, `blockBuilder`
- `internal/modules/blockio/writer/intrinsic_accum.go` — per-block accumulator split

## Expected Impact

`buildBlock` is ~30% of CPU and fully parallelizable across the available cores
(8 in the shadow pod). With N=4 parallel blocks, throughput should approach 2x;
with N=8, potentially 3-4x. Combined with the bloom (BinaryFuse8 removal) and
slab fixes already landed, this should close most of the remaining gap with parquet.

## Notes

- Observed in tempo-dev-02 shadow pod profiling session 2026-04-02
- Blockpack total CPU: 2,963s vs parquet 2,373s (+25%) for same workload window
- Parquet catch-up: 7.6s/s; blockpack catch-up: 2.8s/s (2.7x gap)
- Smaller wins available (intrinsicFieldFor map → direct slice index, ~10%) but
  parallelism is the right lever to close the throughput gap

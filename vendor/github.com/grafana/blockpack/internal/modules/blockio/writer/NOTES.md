# Writer Module — Design Notes

## NOTE-001: Intrinsic Columns — Stored Exclusively in Intrinsic TOC Section
*Added: 2026-03-25*

*Addendum (2026-03-25): Original entry claimed dual-storage (block columns AND intrinsic
section). That was incorrect. Intrinsic columns are written ONLY to the intrinsic TOC
section; `addPresent` calls for these columns were removed. This addendum corrects the record.*

*Addendum (2026-03-26): Rolled back. See NOTE-002. Dual storage is restored — intrinsic
columns are written to BOTH block column payloads (via `addPresent`) AND the intrinsic TOC
section. The exclusive-intrinsic model introduced by PR #172 caused O(8.6B) reverse-lookup
operations per query and has been reverted.*

~~**Decision:** `blockBuilder` writes intrinsic columns (trace:id, span:id, span:parent_id,
span:name, span:kind, span:start, span:duration, span:status, span:status_message,
resource.service.name) ONLY to the intrinsic accumulator (via `feedIntrinsic*` calls).
They are NOT written to block column payloads via `addPresent`.~~

~~**Rationale:**~~
~~- The intrinsic section enables fast pre-filtering (bloom, min/max range index) and O(1)~~
~~  identity lookup via `lookupIntrinsicFields` without full block decodes.~~
~~- Removing dual-storage eliminates redundant data in block payloads and simplifies the~~
~~  write path.~~
~~- The executor's `nilIntrinsicScan` mechanism handles nil block columns for intrinsic~~
~~  fields, returning FullScan results for AND intersection safety.~~

~~**Compaction path:** `addRowFromBlock` feeds intrinsic data via `feedIntrinsicsFromReader`,~~
~~which reads from the source reader's intrinsic section. No `addPresent` calls are issued~~
~~for intrinsic columns.~~

*This entry is superseded by NOTE-002. Dual storage is now in effect for all write paths.*

**Back-ref:** `internal/modules/blockio/writer/writer_block.go:newBlockBuilder`,
`internal/modules/blockio/writer/writer_block.go:addRowFromProto`,
`internal/modules/blockio/writer/writer_block.go:addRowFromTempoProto`,
`internal/modules/blockio/writer/writer_block.go:feedIntrinsicsFromReader`

---

## NOTE-002: Rollback to Dual Storage — Intrinsic Columns in Both Block Payloads and Intrinsic Section
*Added: 2026-03-26*

**Decision:** Restore dual storage for intrinsic columns (trace:id, span:id, span:parent_id,
span:name, span:kind, span:start, span:duration, span:status, span:status_message,
resource.service.name). `blockBuilder` writes these columns to BOTH the block column payload
(via `addPresent`) AND the intrinsic TOC accumulator (via `feedIntrinsic*` calls).

This reverts the exclusive-intrinsic model introduced by PR #172.

**Root cause of the rollback:** PR #172's exclusive-intrinsic model created a severe reverse
lookup performance regression. The intrinsic section is sorted by VALUE (for range scans and
bloom pruning). There is no secondary index from BlockRef → value. Reverse lookups —
materializing field values for a known (blockIdx, rowIdx) during result collection — must scan
all N intrinsic entries because pages partition by value, not by BlockRef. Page-level min/max
cannot help: they bound the value range within the page, not the BlockRef range.

With 2.8M spans × 11 intrinsic columns × 14 files this caused O(8.6B) operations per query
in the worst case. A `refIndex` (map[uint32]uint32) was considered as an alternative but adds
28–60 MB memory per open file, which is unacceptable for deployments with many concurrent
readers.

**Why dual storage:** Block column payloads provide O(1) row access by (blockIdx, rowIdx) for
result materialization — the executor reads the block once and addresses rows directly.
The intrinsic TOC section provides O(1) value-range scans and bloom pruning for zero-block-read
fast paths. Both access patterns are required; dual storage is the simplest way to serve both.

**Size trade-off:** Dual storage increases file size by approximately 20% compared to the
exclusive-intrinsic model. This is acceptable because request latency on object storage
(50–100 ms per API call) dominates query cost — the extra bytes transferred are negligible
compared to the query time saved by avoiding O(8.6B) per-query scan operations.

**Executor impact:** The `nilIntrinsicScan`, `userAttrProgram`, and
`filterRowSetByIntrinsicNodes` workarounds introduced for the exclusive-intrinsic model
(SPEC-STREAM-10, NOTE-050, NOTE-051) remain in place. They are now conservative no-ops for
the block-scan path (block columns are populated, so `nilIntrinsicScan` is never triggered),
but the intrinsic fast paths (Cases A–D, zero-block-read) continue to rely on the intrinsic
section as before.

**Back-ref:** `internal/modules/blockio/writer/writer_block.go:addRowFromProto`,
`internal/modules/blockio/writer/writer_block.go:addRowFromTempoProto`,
`internal/modules/blockio/writer/writer_block.go:addRowFromBlock`,
`internal/modules/blockio/writer/writer_block.go:feedIntrinsicsFromIndex`

---

## NOTE-003: CMS Removal — SKTE Writer Format, No CMS Data Written
*Added: 2026-04-02*

**Decision:** Remove CMS accumulation and marshalling from the sketch writer. New files use
SKTE format (magic `0x534B5445`). The `colSketch` struct holds HLL, TopK, and SketchBloom;
`blockSketchSet.add()` feeds all three incrementally — no key accumulation needed.

**Rationale:**
- CMS added ~70% to per-file sketch section size. At production scale (multi-GB blockpack
  files) the sketch index alone exceeded heap limits during compaction, causing OOM.
- TopK provides approximate (Space-Saving upper-bound) frequency counts for hot values.
  For values outside the top-K, the planner makes a conservative pass — no pruning — rather
  than relying on CMS frequency estimates.
- SketchBloom (fixed 2 KiB, k=7, incremental Add()) replaces BinaryFuse8, eliminating the
  large keys []uint64 accumulation buffer that required all keys before construction.

**Wire format:** SKTE per-column layout:
1. Presence bitset: `ceil(num_blocks/8)` bytes
2. Distinct counts: `num_blocks × 4 LE uint32` (HLL cardinality per block)
3. TopK section: `topk_k[1]` + per present block: `entry_count[1]` + `(fp[8 LE], count[2 LE])` pairs
4. Bloom section: `bloom_size[2 LE]` + per present block: `bloom_data[bloom_size]` (fixed 2048 bytes)

No CMS bytes are written. Legacy SKTC/SKTD readers skip CMS bytes zero-alloc via
`skipColumnCMS` in the reader package (see reader/NOTES.md NOTE-010).

**Back-ref:**
- `internal/modules/blockio/writer/sketch_index.go:writeSketchIndexSection`
- `internal/modules/blockio/writer/sketch_index.go:sketchSectionMagic` (0x534B5445 = SKTE)
- `internal/modules/blockio/writer/writer_log.go:blockSketchSet`

---

## NOTE-004: Parallel Block Building — Inter-Block Concurrency via errgroup + sync.Pool
*Added: 2026-04-02*

**Decision:** `flushBlocks()` and `flushLogBlocks()` build blocks concurrently using
`errgroup.Group` limited to `runtime.NumCPU()` goroutines. Each goroutine draws a
`*blockBuilder` from a `sync.Pool` (`bbPool`) and a `*zstdEncoder` from `encPool`, builds
one block, and returns both to their respective pools. After all goroutines complete, a serial
pass writes payloads and updates `blockMetas`, `rangeIdx`, `sketchIdx`, and `traceIndex` in
block-ID order.

For trace blocks, each goroutine receives its own `*intrinsicAccumulator` (`localAccum`).
After the parallel phase, `localAccum` values are merged into `w.intrinsicAccum` via the
new `merge()` method in block-ID order. No sorting is performed at merge time; sorting
happens later in `encodeColumn`. Log blocks have no `intrinsicAccum`, so the merge step
is absent from `flushLogBlocks`.

**Rationale:** Block building (OTLP→column decode, dict/delta/XOR encoding, zstd compress)
is CPU-bound and has no shared mutable state within a block. Blockpack block writing was
2.7× slower than parquet in benchmarks; parquet achieves speed by building multiple
row-groups concurrently. This change applies the same approach.

**Consequence:**
- The Writer remains NOT thread-safe from the caller's perspective (the `inUse` guard is
  unchanged). Concurrency is internal to `flushBlocks`.
- The `bb *blockBuilder` field and `enc *zstdEncoder` field are replaced by `bbPool` and
  `encPool` (`sync.Pool`). The builder-cache optimization (reusing column builders across
  blocks) is preserved: builders are returned to the pool with their `builderCache` intact,
  so the next goroutine that checks out the builder reuses cached column builders from prior
  blocks. `zstdEncoder` is NOT goroutine-safe (its `buf []byte` is mutated by `compress()`),
  so each goroutine must use its own encoder from the pool.
- Block IDs are pre-assigned before the parallel phase using the formula
  `baseID + i` where `baseID = len(w.blockMetas)`. This ensures deterministic block
  ordering even when goroutines finish out-of-order.
- `buildAndWriteBlock` and `buildAndWriteLogBlock` are removed; their logic is inlined
  into `flushBlocks` and `flushLogBlocks` respectively.
- `blockBuilder.reset()` allocates fresh maps for `traceRows` and `colMinMax` rather than
  clearing in-place. This breaks the aliasing between `builtBlock` map fields and the
  pooled builder, preventing pool reuse from corrupting prior results.

**Back-ref:** `internal/modules/blockio/writer/writer.go:flushBlocks`,
`internal/modules/blockio/writer/writer.go:flushLogBlocks`,
`internal/modules/blockio/writer/intrinsic_accum.go:merge`

---

## NOTE-005: vectorAccumulator Pattern — Accumulate-at-Build, Serialize-at-Flush (2026-04-02)
*Added: 2026-04-02*

**Decision:** The `vectorAccumulator` field in the `Writer` follows the same accumulate-at-block,
serialize-at-flush pattern as `sketchIdx` and `fileBloomSvcNames`. `accumulateBlock` is called
in the `flushBlocks` serial merge pass (after the parallel build phase) using vectors extracted
from `builtBlock.blockVectors`; `build()` is called once in `Flush()` to train the PQ codebook
and produce the serialized section bytes. The vector section is written AFTER the intrinsic
section and BEFORE the footer.

**Rationale:** PQ training requires all vectors to be present (reservoir sampling over the full
file). Training per-block would produce per-block codebooks, making cross-block ADC comparison
impossible. Accumulating all vectors and training once at flush time is the only option that
supports asymmetric distance computation (ADC) across blocks.

**Consequence:** Writers with `VectorDimension > 0` emit a V5 footer instead of V4.
V4 readers encountering a V5 footer: the V5 footer is 46 bytes and V4 is 34 bytes; the V5
detection code in the reader tries V5 first, so this is handled gracefully.
Writers with `VectorDimension == 0` (or no vectors added) continue to emit V4 footers —
no behavioral change for non-vector workflows.

**Back-ref:** `internal/modules/blockio/writer/vector_index.go:vectorAccumulator`,
`internal/modules/blockio/writer/vector_index.go:serializeVectorIndexSection`,
`internal/modules/blockio/writer/writer.go:Flush`,
`internal/modules/blockio/writer/writer_block.go:builtBlock`

---

## NOTE-006: Per-Block Vector Storage and Dimension Validation (2026-04-02)
*Added: 2026-04-02*

**Decision:** `vectorBlockEntry` now carries `vectors [][]float32` for per-block raw vector
storage. `build()` encodes each block's vectors immediately after training, then clears
`entry.vectors` to free memory. The `allVectors` field (formerly holding all file vectors
in one slice) has been removed. Additionally, `accumulateBlock` validates incoming vector
dimension against `a.dim` and skips mismatched blocks.

**Rationale:**
1. **Memory safety:** The old `allVectors` field accumulated all file vectors simultaneously.
   For a 1M-span file at dim=768, this required ~3 GiB peak RSS at flush time, making the
   feature unusable in production. Per-block storage allows each block's vectors to be freed
   immediately after encoding, capping peak RSS to `max_block_vectors × dim × 4` bytes.
2. **Panic prevention:** Mixed-dimension vectors in `allVectors` caused a SIMD panic in
   `vectormath.Mean` and `extractSubvecs` when subsequent blocks had different dimensions.
   Dimension validation at `accumulateBlock` entry prevents this.

**Consequence:** Per-block `vectors` fields are `nil` after `build()` returns. Code that
inspects `vectorBlockEntry.vectors` after flush will see nil slices — this is intentional.
Skipping dimension-mismatched blocks produces a codebook trained only on the valid vectors;
no error is returned (consistent with the writer's non-panicking contract).

**Back-ref:** `internal/modules/blockio/writer/vector_index.go:accumulateBlock`,
`internal/modules/blockio/writer/vector_index.go:build`

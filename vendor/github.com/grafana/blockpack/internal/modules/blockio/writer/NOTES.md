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

For trace blocks, each goroutine receives its own in-memory `*intrinsicAccumulator`
(`localAccum`). After the parallel phase, `localAccum` rows are spilled into the file-level
on-disk accumulator `w.intrinsicAccum` (a `*tempFileAccum`, NOTE-461) via `spillMerge` in
block-ID order. No sorting is performed at spill time; sorting happens later in `encodeColumn`
when each column is rebuilt from its spill file. (The original in-memory `merge()` method was
removed when the file-level accumulator moved on-disk — see NOTE-461.)

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
`internal/modules/blockio/writer/intrinsic_tempfile.go:spillMerge`

---

## NOTE-465: Remove the ~700-block intrinsic fast-path cliff — two causes (issue #384) (2026-06-22)
*Added: 2026-06-22*

**Two independent ~700-block cliffs, same issue.** Issue #384 reported the intrinsic fast
path disabling itself above ~700–800 assigned blocks. There were two distinct causes, fixed
together under this note:

1. **Metrics path — `MaxIntrinsicRows` section drop (this section, writer side).** A query
   fully covered by intrinsic columns silently lost its intrinsic section at write time.
2. **Search/Collect mixed path — Case C/D 50% selectivity guard (executor side, see
   `internal/modules/executor/stream.go:collectFromIntrinsicRefs`).** The partial-AND
   pre-filter for mixed queries (`{ span:kind = server && span.http.method = "GET" }`) fell
   back to the full block scan whenever its candidate set covered more than HALF the file's
   internal blocks (`uniqueBlocks*2 > BlockCount`). A broad-but-not-universal intrinsic
   predicate like `kind = server` covers most blocks of a large file, so above ~50% coverage
   (~700 of ~1400 blocks) the guard abandoned the fast path even though pruning the remaining
   blocks still skips real block I/O + second-pass decode + VM eval. Replaced the 50% cliff
   with a coverage heuristic (`minPruneShiftCDsel = 4`): keep the fast path as long as the
   pre-filter prunes at least 1/16 of the blocks; fall back only when candidate coverage is
   near-total. General heuristic, no benchmark-specific value. Covered by
   `TestExecutionPath_MixedSelectivityGuard` (40/52/90/100% coverage, asserting identical
   result counts on both execution paths).

**Problem (metrics / writer):** The intrinsic/dedicated-column metrics fast path (zero block
I/O) silently stopped triggering for queriers assigned more than ~700–800 blocks. A query fully covered by
intrinsic columns (e.g. `{kind = server} | histogram_over_time(duration) by
(resource.service.name)`) reported `full_fetch_skipped=false` and `pruned_by_intrinsic_toc=0`
on 93/96 querier calls, each then doing an unnecessary full block fetch. Root cause was NOT in
the executor dispatch — it was at write time: `MaxIntrinsicRows = 10_000_000` plus
`overCap()`. When any single intrinsic column exceeds the cap, `writeV8IntrinsicBlobs` writes
**no** intrinsic columns at all (empty TOC). The reader then reports `HasIntrinsicSection() ==
false` / `HasIntrinsicColumn() == false`, so `metricsColumnsAreIntrinsic` returns false and the
metrics path falls through to a full block scan. Files assigned ~814–3,598 blocks hold roughly
11–50M spans, so `span:start` (one row per span) blew past 10M and the whole section was
dropped — a hard performance cliff, not a gradual degradation.

**Why 10M was obsolete:** the cap dates to when the file-level intrinsic section was built
entirely in memory — `O(all columns × all spans)`, tens of GiB, the OOM source. NOTE-461
replaced that with per-column on-disk spill: at `Flush()` exactly ONE column is rebuilt in
memory at a time, so peak write RSS is `O(largest single column's rows)`. The on-disk column
blobs are also page-encoded (`deltaPageSize = 1024` for delta-uint64; paged dict), so the
reader streams pages and never materializes a whole column. With both write- and read-time
memory bounded independent of total file rows, the 10M section-drop bought no memory safety —
it only disabled the fast path.

**Decision (initial):** Raise `MaxIntrinsicRows` to `100_000_000` as a sanity guard. 100M is
principled, not benchmark-tuned: a single-column rebuild at 100M rows is ≈1–2 GiB transient,
within `MaxMetadataSize` (2 GiB), and comfortably above realistic large files (≈50M rows).

**Decision (final, commit dafcf3e8):** Remove `MaxIntrinsicRows` entirely. After observing that
even the raised 100M value left a visible cliff (blocks with >100M spans still emitted an empty
intrinsic section), the cap was removed completely. With NOTE-461 in place both write-time
memory (O(largest single column rebuild)) and read-time memory (paged streaming) are bounded
independent of total row count — the cap provided no remaining safety. `overCap()` on both
accumulators is retained as an always-false stub for interface compatibility; `TestMaxIntrinsicRows_OverCap`
and related tests were deleted as they tested behaviour that no longer exists.

**Back-ref:** `internal/modules/blockio/shared/constants.go` (MaxIntrinsicRows removed),
`internal/modules/blockio/writer/intrinsic_accum.go:overCap` (always false),
`internal/modules/blockio/writer/intrinsic_tempfile.go:overCap` (always false),
`internal/modules/blockio/writer/v8_sections.go:writeV8IntrinsicBlobs`,
`internal/modules/executor/metrics_trace_intrinsic.go:metricsColumnsAreIntrinsic`

---

## NOTE-461: File-level intrinsic accumulator spills to per-column temp files (issue #380) (2026-06-21)
*Added: 2026-06-21*

**Problem:** The file-level intrinsic section (trace:id, span:id, span:kind, span:name,
resource.service.name, span:start/duration, …) was built entirely in memory by the
`intrinsicAccumulator` on the `Writer`. It accumulated `map[column]→[]entry` for EVERY span
in the output file and was only serialised at `Flush()`. A pyroscope profile during compaction
showed `(*intrinsicAccumulator).merge` as the dominant live allocation (tens of GB across
workers): for 8 L1 blocks × ~2M spans ≈ 16M spans across ~15 columns this is ~10–30 GiB. This
caused OOMKills and forced the `max_input_blocks: 2` workaround even though output block bytes
already stream to disk (NOTE on streaming compaction output).

**Decision:** Replace the file-level accumulator with `tempFileAccum`
(`intrinsic_tempfile.go`). Per-block accumulation still uses the in-memory
`intrinsicAccumulator` (`localAccum`) — bounded by one block and built in the parallel phase.
The serial merge pass now calls `spillMerge`, which appends each block's rows to a **per-column
on-disk spill file** instead of growing in-memory maps. At `Flush()`, `encodeColumn` rebuilds
ONE column's in-memory `flatAccum`/`dictAccum` from its spill file, encodes it via the existing
wire-format path (`encodeXORBytesIntrinsic` / `encodeDeltaUint64Intrinsic` /
`encodePagedDictColumn`, which sort internally), then releases it before the next column. Peak
RSS for the intrinsic section becomes O(largest single column's rows) instead of O(all columns ×
all spans) — e.g. ~one trace:id column (~320 MiB at 16M spans) vs ~30 GiB.

**Wire format unchanged:** spill files are an internal scratch encoding; the on-disk
intrinsic section bytes are byte-for-byte identical to the old in-memory path (verified by
equivalence tests comparing `tempFileAccum.encodeColumn` to `intrinsicAccumulator.encodeColumn`).

**Spill ordering:** rows are appended in write order — block-by-block in ascending blockID,
row-by-row within each block — so each column file is naturally ordered by `(blockIdx ASC,
rowIdx ASC)`. Correctness does not depend on this (the encode path re-sorts per wire-format
requirement: value order for flat columns, value-grouped for dict columns); the spill order
just documents the sequential-block-access property from issue #380. Dict entries are expanded
back to one `(value, ref)` record per ref on spill and re-deduplicated at encode time, keeping
the spill append-only and streamable.

**Lifetime:** `tempFileAccum` is created lazily on the first `flushBlocks()` that has a
`localAccum` (`ensureIntrinsicAccum`), so a Writer that produces an empty file never touches
the filesystem. It is closed (spill files removed; owned temp dir removed) by a `defer` at the
top of `Flush()` covering all exit paths including errors, so a failed compaction does not leak
scratch files. When `Config.ScratchDir` is set (compaction passes its `StagingDir`), spill
files land on the same volume as the staged output block; otherwise a unique `os.MkdirTemp`
dir under `os.TempDir()` is created and owned by the accumulator.

**Trade-off:** intrinsic data is now written to disk twice (spill, then read back to encode)
and re-snappy-compressed once, adding sequential disk I/O during compaction. This is accepted:
the streaming spill is sequential and on the local scratch volume, and OOMKills aborting the
whole job are far costlier than the extra I/O. Allows `max_input_blocks` 8+ without OOMKills.

**Consequence:** the in-memory `intrinsicAccumulator.merge` method was removed (fully
superseded by `spillMerge`); `merge`'s unit test was deleted and replaced by `tempFileAccum`
tests including per-type equivalence and spill-merge equivalence. The unused
`intrinsicAccumulator.columnNames` (the file-level encode path no longer calls it) was also
removed.

**Back-ref:** `internal/modules/blockio/writer/intrinsic_tempfile.go`,
`internal/modules/blockio/writer/writer.go:ensureIntrinsicAccum`,
`internal/modules/blockio/writer/writer.go:flushBlocks`,
`internal/modules/blockio/writer/config.go:ScratchDir`,
`internal/modules/blockio/compaction/compaction.go:ensureWriter`

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

---

## NOTE-007: V14 Writer Redesign — zstd Removal, Per-Column Snappy, Sectioned Metadata (2026-04-10)
*Added: 2026-04-10*

**Decision:** Redesign the write path for V14 format:
1. Remove all internal zstd compression from encoding types (dict, delta, XOR, prefix, delta-dict).
2. Apply one outer `snappy.Encode` per column blob at the block level, immediately after `buildData()`.
3. Replace the single snappy-compressed metadata blob with 6 type-keyed independently-compressed
   sections (block index, range index, trace index, TS index, sketch, file bloom) plus one
   name-keyed section directory entry per file-level intrinsic column blob.
4. Write a new V7 footer (18 bytes: magic+version[=7]+dir_offset+dir_len). Version 7 avoids
   collision with agentic's V5 (46-byte vector footer) and V6 (58-byte compact-traces footer).
5. Remove `encPool sync.Pool` and the `zstdEncoder` type entirely.

**Why remove zstd from encoding internals:**
The original design applied zstd inside each encoding type (e.g. `dict_zstd[4+N]` for the
dictionary payload). This created two levels of compression: zstd inside the raw blob, then
the entire column payload was conceptually uncompressed at the block level. The inner zstd
provided good compression ratios but:
- Required pooled `*zstd.Encoder` instances (concurrency complexity).
- Made it impossible to snappy-wrap the whole column as a single unit, since zstd's framing
  is already embedded inside.
- Prevented the reader from knowing `uncompressed_len` without decompressing.

In V14, each encoding type writes raw bytes (no zstd). A single `snappy.Encode(rawBlob)` at
the block level wraps the entire column in one compressed unit. Snappy is faster than zstd
and sufficient for the column-level wrapper since the encoding itself (delta, dict, XOR, prefix)
already reduces redundancy before snappy sees it.

**Why sectioned metadata:**
The V12/V13 metadata blob bundled all file-level indexes into one snappy-compressed region.
To check a file-level bloom filter, the reader had to decompress and parse the entire blob
(hundreds of MB for large files with many distinct values). SPEC-ROOT-013 and SPEC-ROOT-014
require each independently-used data structure to be independently readable. With 6 type-keyed
sections plus name-keyed intrinsic column entries, a bloom check costs one `ReadAt` +
one `snappy.Decode` of only the bloom section bytes, and any intrinsic column is directly
addressable from the section directory without a separate TOC read.

**encPool removal:**
`encPool` was a `sync.Pool` of `*zstdEncoder` objects — one per goroutine in the parallel
block build phase. Removing zstd from encoding types eliminates the need for per-goroutine
encoders entirely. Block builders in V14 call `snappy.Encode(nil, rawBlob)` inline, which
is stateless and goroutine-safe. No pool is needed.

Back-ref: `internal/modules/blockio/writer/column_builder.go` (zstdEncoder removed),
`internal/modules/blockio/writer/writer_block.go` (outer snappy per column),
`internal/modules/blockio/writer/metadata.go:writeFooterV7`,
`internal/modules/blockio/writer/writer.go:flushBlocks` (encPool removed)

---

## NOTE-36: Bloom-Enabled Compact Index (Version 2)
*Added: 2026-04-14*

The compact trace index version 2 embeds a per-file trace ID bloom filter in the
section header, enabling O(1) negative lookups before loading the full hash map.
Readers call `BlocksForTraceIDCompact` which checks the bloom first and only proceeds
to the hash-map lookup on a bloom hit.

Wire format:
```
magic[4] + version[1]=2 + block_count[4] + bloom_bytes[4] + bloom_data[bloom_bytes]
+ block_table[block_count×12] + trace_index_bytes[...]
```

Back-ref: `internal/modules/blockio/writer/metadata.go:writeCompactTraceIndex`
Back-ref: `internal/modules/blockio/reader/trace_index.go:BlocksForTraceIDCompact`

---

## NOTE-37: Trace Index v2 — Block IDs Only, Reader-Side Span Scan
*Added: 2026-04-14*

The v2 trace block index stores only block IDs per trace (not per-span row indices).
The reader scans the `trace:id` column within the identified blocks to locate matching
spans. This reduces index size significantly versus a span-index approach.

Note: `compaction/NOTES.md § NOTE-37` is a separate entry covering log file re-sort
during compaction. This entry (in writer/NOTES.md) covers the trace-index wire format.

Back-ref: `internal/modules/blockio/writer/metadata.go:writeTraceBlockIndexSection`

---

## NOTE-38: Exact-Value Range Index for Low-Cardinality Columns
*Added: 2026-04-14*

For columns where every block has `min == max` (single distinct value per block),
the range index uses an exact-value map instead of KLL quantile buckets. Each distinct
value maps directly to its block IDs, giving zero false positives for equality predicates.
The wire format is identical to the KLL path — readers use the same binary-search lookup.

Back-ref: `internal/modules/blockio/writer/range_index.go:applyRangeBucketsForColumn`
Back-ref: `internal/modules/blockio/writer/range_index.go:tryApplyExactValues`

---

## NOTE-38b: Exact-Value Path Safety Invariant — All Blocks Must Be Single-Value
*Added: 2026-04-14*

The exact-value path (NOTE-38) is only safe when ALL blocks have `min == max`. If any
block spans multiple values (`minKey != maxKey`), storing it under only minKey and maxKey
causes false-negative pruning: intermediate values are missed by point lookup. When any
block has `min != max`, `tryApplyExactValues` returns false and falls through to the KLL
overlap path. This applies to all range column types: RangeInt64, RangeUint64,
RangeFloat64, RangeString.

Back-ref: `internal/modules/blockio/writer/range_index.go:tryApplyExactValues`

---

## NOTE-39: encodeXORBytesIntrinsic — XOR Encoding for Large Bytes Intrinsic Columns
*Added: 2026-04-22*

**Decision:** `encodeColumn` now dispatches to `encodeXORBytesIntrinsic` when
`len(c.bytesValues) > 0` (i.e., the column is a bytes type with > IntrinsicPageSize rows).

**Rationale:** See shared/NOTES.md NOTE-013. The paged flat path produced 2× inflation for
random-byte IDs. `encodeXORBytesIntrinsic` collapses all 226 snappy calls into one.

**`xorBytesLen` reuse:** The `xorBytesLen` helper (encoding_xor.go:73) is called directly.
It is already in the `writer` package. No duplication. `xorBytesLen` (not `xorBytes`) is
required because it always produces exactly `len(a)` bytes, making `xor_data_len`
self-describing at decode time. Using `xorBytes` instead would produce `max(len(a),len(b))`
bytes, breaking the decoder's assumption that `xor_data_len == original value length`.

**Non-paged bytes path unchanged:** Small bytes columns (≤ IntrinsicPageSize rows) continue
to use `encodeFlatColumn`. The compression ratio problem only manifests at scale (> 1 page).

Back-ref: `internal/modules/blockio/writer/intrinsic_accum.go:encodeXORBytesIntrinsic`,
          `internal/modules/blockio/writer/intrinsic_accum.go:encodeColumn`

---

## NOTE-V8-002: writeV8Sections — per-column blobs and unified ToC

**Context:** `Flush()` unconditionally calls `writeV8Sections()`. V8 replaces the monolithic
`SectionRangeIndex` and `SectionSketchIndex` blobs with per-column blobs — one ToCEntry per
column per section type.

**Per-column range blobs:** `writeOneColumnRangeBlob(cd)` serializes one column's range data
without the `col_name_len[2]+col_name` prefix present in the V14 monolithic range section.
The blob starts at `col_type[1]` followed by bucket metadata and value entries.
Returns nil if `cd.values` is empty (column has no bucket entries — not indexed).

**Per-column sketch blobs:** `writeOneColumnSketchBlob(colName, sketchIdx)` writes a
self-contained blob: `num_blocks[4]+presence[ceil(n/8)]+distinct[n*4]+topk+bloom`.
The `num_blocks[4]` prefix makes the blob self-describing — the reader does not need
global state to decode presence bits.

**Intrinsic blobs (already compressed):** Intrinsic blobs from `intrinsicAccum.encodeColumn()`
are already snappy-compressed. They are written directly to disk (NOT passed through
`writeToCEntry` which would double-compress). The corresponding ToCEntries record their
offsets directly. `fetchToCSection` calls `decodeBoundedSnappy` on fetch, so on-disk
blobs must be snappy-compressed exactly once.

**V8 is unconditional:** All files are written with V8 footer and per-column ToCEntries.
There is no fallback to V7 footer or monolithic section layout.

Back-ref: `internal/modules/blockio/writer/writer.go:writeV8Sections`,
          `internal/modules/blockio/writer/metadata.go:writeFooterV8`,
          `internal/modules/blockio/writer/metadata.go:writeOneColumnRangeBlob`,
          `internal/modules/blockio/writer/sketch_index.go:writeOneColumnSketchBlob`

## NOTE-AP-001: AllPresent encoding kinds — skip presence-RLE for fully-present columns

For a fully-present dense column (every row has a value), the per-row presence bitset is
all-ones and its RLE encoding is pure overhead: ~`nRows/8` bytes plus the per-column RLE
encode cost. Intrinsic columns such as `span:id`, `span:start`, and `trace:id` are always
100% present, so this overhead is paid on every block for every such column.

**Change:** seven new "AllPresent" encoding kinds (15–21, defined in `shared/constants.go`),
one per dense encoding family:

- `KindDictionaryAllPresent` (15)
- `KindInlineBytesAllPresent` (16) — reader-only; the writer never emits InlineBytes
- `KindDeltaUint64AllPresent` (17)
- `KindRLEIndexesAllPresent` (18)
- `KindXORBytesAllPresent` (19)
- `KindPrefixBytesAllPresent` (20)
- `KindDeltaDictionaryAllPresent` (21)

Each AllPresent kind is **wire-identical to its base dense kind except the
`presence_rle_len[4] + presence_rle_data` segment is omitted entirely**. The kind byte itself
signals "every row is present." There are no sparse AllPresent variants — sparse-with-all-present
is a contradiction.

**Selection** (`encoding_presence.go:selectAllPresent`): each encoder counts `presentCount`
during the existing presence-bitset build. When `presentCount == nRows` (and `nRows > 0`), it
emits the AllPresent variant via `shared.AllPresentKindFor` and skips both the
`EncodePresenceRLE` call and the presence segment (`appendPresenceSegment` becomes a no-op).
Low-cardinality dictionary columns that auto-upgrade to RLE select `KindRLEIndexesAllPresent`.

**Rollout flag:** `Config.DisableAllPresentEncoding` (default false → AllPresent on) forces the
legacy presence-RLE form for every column, so a writer can be deployed ahead of readers that
understand the new kinds (NOTE-007 additive-evolution precedent). The flag is applied to a
process-level `atomic.Bool` (`constants.go:allPresentEncodingEnabled`) because the encoders run
on per-block goroutines; in practice the value is a deploy-level constant.

**Backwards compatibility:** new kind IDs only; no `enc_version` bump. Old readers reject unknown
kinds at `reader/column.go:readColumnEncoding`. New readers map AllPresent kinds back to their
base kind via `shared.BaseKindFor` and synthesize a fully-present presence vector with
`shared.AllPresentBitset` (no byte reads). Reading is unaffected by the writer flag — readers
always accept both forms.

Back-ref: `shared/constants.go` (kinds + `AllPresentKindFor`/`BaseKindFor`/`IsAllPresentKind`),
          `shared/presence_rle.go:AllPresentBitset`,
          `writer/encoding_presence.go`, `writer/encoding_{delta,xor,prefix,dict}.go`,
          `reader/column.go:readColumnEncoding`/`decodePresenceMaybe`, NOTE-007.

## NOTE-215: bit-packed DeltaUint64 (kinds 22/23)

`encodeDeltaUint64` (kind 5) snaps every offset to a byte width (1/2/4/8 bytes). Real ingest
commonly produces offset ranges that fall just above a byte boundary — most notably `span:start`,
whose ~60 s window in ns needs ~36 bits but rounds up to a full 8-byte width, wasting 28 bits per
offset. NOTE-215 adds `encodeDeltaUint64BitPacked` (`encoding_delta_bitpacked.go`): one
`bit_width` (0–64) chosen as `bits.Len64(maxOffset)`, then every offset packed into a contiguous
LSB-first bit stream (`writeBitsLE`). Wire format and selection rule: SPECS §9.4.1, SPEC-006.

**Why DeltaUint64 only?** Bit-width selection only helps encodings whose payload density depends
on the *local* value range. Dictionary's win is the global dict (indexes are already tiny); RLE
already encodes locality; XOR/Prefix are local by construction. Delta is the single density-
sensitive integer encoding, so it is the only kind that gains from packing at the bit level.

**Selection** (`shouldUseBitPackedDelta`, called from `uint64ColumnBuilder.buildData` after
`shouldUseDeltaEncoding`): emit kind 22/23 only when bit packing saves ≥ `bitPackedDeltaMinSavedBits`
(4) bits per offset versus the kind-5 byte width AND there are ≥ `bitPackedDeltaMinPresent` (64)
present rows to amortize the fixed header. All-zero offsets (`bit_width == 0`) keep kind 5 (no
payload either way). `base` and `maxOffset` are computed once via the shared
`deltaBaseAndMaxOffset` helper so both delta encoders agree on the same base/range.

**AllPresent composition:** a fully-present bit-packed column selects kind 23 via
`selectAllPresent` + `shared.AllPresentKindFor` (NOTE-AP-001), omitting the presence-RLE segment.

**Rollout flag:** `Config.DisableBitPackedDelta` (default false → bit-packed on) forces the
legacy kind-5 form, mirroring the NOTE-AP-001 atomic-bool pattern
(`constants.go:bitPackedDeltaEncodingEnabled`).

**Backwards compatibility:** new kind IDs only; no `enc_version` bump. Old readers reject unknown
kinds at `reader/column.go:readColumnEncoding`. Compaction is transparent — it reads decoded
values via the reader API and re-encodes via the writer, so the new kind needs no compaction code.

Back-ref: `shared/constants.go` (kinds 22/23 + AllPresent maps),
          `writer/encoding_delta_bitpacked.go`, `writer/encoding_delta.go:deltaBaseAndMaxOffset`,
          `writer/column_types.go:buildData`, `reader/column.go:decodeDeltaUint64BitPacked`,
          SPECS §9.4.1, SPEC-006, NOTE-AP-001, NOTE-007.

## NOTE-217: uniform-length XORBytes (kinds 24/25/28)

`encodeXORBytes` (kinds 8/9) writes `val_len[4] + xor_bytes` per present row. For ID columns
where every present value shares one length (span:id=8B, trace:id=16B, UUIDs=16B), that per-row
length prefix is pure overhead — 50% of the wire bytes for an 8-byte ID. NOTE-217 adds a
uniform-length variant: `uniformValueLen` (`encoding_xor.go`) checks, in a single pass over the
present rows, whether `presentCount > 1` and every present value has the same non-zero length;
if so `encodeXORBytesUniform` writes one `uniform_len[4]` after the presence segment, then packs
the XOR payload as a fixed-width array with no per-row length prefix. Wire format and selection
rule: SPECS §9.5.1, SPEC-006.

**Why XORBytes (and not the variable case generically)?** The win is the dropped per-row length
prefix plus the removed per-row `appendUint32LE` in the encode loop and per-row length read in the
decode loop. It only applies when lengths are actually equal — exactly the ID-column case XOR
already targets. Mismatched lengths, single-value, or zero-length columns fall through to kinds
8/9/19 unchanged.

**AllPresent composition:** a fully-present uniform column selects kind 28 via `selectAllPresent`
+ `shared.AllPresentKindFor` (NOTE-AP-001), omitting the presence-RLE segment. There is no sparse
AllPresent (a contradiction) and no `present_count` field for kind 25 — like kinds 8/9 the decoder
walks the presence bitset directly.

**InlineBytes uniform (kinds 26/27) is reader-only.** The current writer never selects the
InlineBytes family (all bytes columns go to XORBytes/PrefixBytes/Dictionary), so there is no
`encodeInlineBytesUniform` — adding one would be dead code. The reader still decodes kinds 26/27
(`reader/column.go:decodeInlineBytesUniform`) for forward compatibility and any external producer.

**Rollout flag:** `Config.DisableUniformBytes` (default false → uniform on) forces the legacy
variable-length form, mirroring the NOTE-AP-001 atomic-bool pattern
(`constants.go:uniformBytesEncodingEnabled`).

**Backwards compatibility:** new kind IDs only; no `enc_version` bump. Old readers reject unknown
kinds at `reader/column.go:readColumnEncoding`. Compaction is transparent — it reads decoded
values via the reader API and re-encodes via the writer, so the new kinds need no compaction code.

Back-ref: `shared/constants.go` (kinds 24/25/26/27/28 + AllPresent maps),
          `writer/encoding_xor.go:uniformValueLen,encodeXORBytesUniform`,
          `writer/constants.go:uniformBytesEncodingEnabled`, `writer/config.go:DisableUniformBytes`,
          `reader/column.go:decodeXORBytesUniform,decodeInlineBytesUniform`,
          SPECS §9.3.1, §9.5.1, SPEC-006, NOTE-AP-001, NOTE-007.

## NOTE-218: per-page DeltaUint64 (kind 39)

`encodeDeltaUint64BitPacked` (NOTE-215, kind 22) picks one column-wide `bit_width` from the
global max offset. Real OTLP ingest is often bursty-then-trickle: a tight burst of spans (small
offsets) followed by a sparse trickle (large offsets). One column-wide width is inflated by the
trickle's large offsets, wasting bits on every burst offset. NOTE-218 adds
`encodeDeltaUint64Paged` (`encoding_delta_paged.go`, kind 39): the present rows are split into
fixed-size pages (`deltaPageSize` = 1024 present rows) and each page picks its own `page_base` +
`page_bit_width`, so the burst pages pack narrow and only the trickle page pays the wide width.
Wire format and selection rule: SPECS §9.4.2, SPEC-006.

**Selection (`shouldUsePagedDelta`):** chosen over kind 22/5 only when the column spans at least
`pagedDeltaMinPages` (2) pages AND the simulated per-page packed-bit sum is at least 12.5%
(`pagedDeltaMinSavedBitFraction`) smaller than the column-wide bit-packed payload. The check runs
**before** the NOTE-215/kind-5 decisions in `uint64ColumnBuilder.buildData`. At the default
`defaultMaxBlockSpans = 2000` most blocks span <2 pages and stay on kind 22/5 — the win triggers
intentionally only for larger blocks with bimodal timestamp distributions.

**Why DeltaUint64 only?** Per-page width selection helps only encodings whose payload density is
locality-sensitive. Dictionary/PrefixBytes win from a *global* dict (per-page metadata on dict
indexes adds no skip — dict-index order ≠ value order, and the index array is already ~1 B/row).
RLEIndexes already encodes locality; paging fragments runs. XORBytes is local by construction.
Delta is the only kind whose `base + max_offset` is region-sensitive. This is one new kind only;
Dictionary stays untouched.

**No sparse/AllPresent variant.** The gain is the per-page width adaptation, not the presence
layout, so kind 39 always emits the presence-RLE segment.

**Page size is duplicated in the reader.** The wire format does not store per-page row counts —
the reader (`deltaPageSizeReader`) derives page boundaries from the same fixed `deltaPageSize`.
The two constants MUST stay in sync; changing one side corrupts decode.

**Rollout flag:** `Config.DisablePagedDelta` (default false → per-page on) forces the single-page
forms, mirroring the NOTE-215 atomic-bool pattern (`constants.go:pagedDeltaEncodingEnabled`).

**Backwards compatibility:** new kind ID only; no `enc_version` bump. Old readers reject the
unknown kind at `reader/column.go:readColumnEncoding`. Compaction is transparent — it reads
decoded values via the reader API and re-encodes via the writer, so the new kind needs no
compaction code.

Back-ref: `shared/constants.go` (kind 39 KindDeltaUint64Paged),
          `writer/encoding_delta_paged.go:encodeDeltaUint64Paged,shouldUsePagedDelta`,
          `writer/column_types.go:uint64ColumnBuilder.buildData`,
          `writer/constants.go:pagedDeltaEncodingEnabled`, `writer/config.go:DisablePagedDelta`,
          `reader/column.go:decodeDeltaUint64Paged,deltaPageSizeReader`,
          SPECS §9.4.2, SPEC-006, NOTE-215, NOTE-007.

## NOTE-219: Gorilla-XOR Float64 (kinds 40/41)

`float64ColumnBuilder.buildData` previously routed every float column to the Dictionary path
(kinds 1/2, RLE-upgraded for tiny dicts). That is correct for **low-cardinality** floats but
wasteful for **high-cardinality, value-correlated** floats — the `ColumnTypeRangeFloat64` columns
created by numeric-string promotion (NOTE-40), e.g. body-parsed `latency_ms="423.7"`. For those
the dictionary stores ~one entry per row (no real dedup) and pays ~13 B/val after snappy, while
the values are quasi-monotonic and would compress to ~2–3 B/val under Gorilla-XOR (Pelkonen et
al., VLDB 2015). NOTE-219 adds `encodeGorillaFloat64` (`encoding_gorilla.go`, kind 40, AllPresent
kind 41): the first present value is stored verbatim, each subsequent value is XORed against its
predecessor and the meaningful bits packed LSB-first (reusing `writeBitsLE` from NOTE-215). Wire
format and selection rule: SPECS §9.8, SPEC-006.

**Two-population analysis — DO NOT widen the guard naively.** The float column population splits:

- **Low-cardinality** (HTTP sampling ratios `0.0/0.1/0.5/1.0`, rounded utilization gauges, TLS
  versions as floats, health-check percentages): the dict has ≤16 entries; Dictionary+RLE gives
  ~1–2 B/val (sometimes 50× compression). **Gorilla would REGRESS these to ~2–3 B/val.** They MUST
  stay on Dictionary.
- **High-cardinality correlated** (NOTE-40 numeric-string-promoted latencies/durations,
  high-precision OTLP attrs, metric observation values): mostly distinct, adjacency-correlated.
  Dictionary cannot help; Gorilla wins 4–6×.

`shouldUseGorillaFloat64` separates them with a purely data-driven rule (never name/type-based, so
it generalizes): `presentCount ≥ 64` AND `distinctPresentValues > max(64, presentCount/4)`. The
cardinality is counted on the raw IEEE-754 bit pattern (`float64PresenceAndCardinality`), so
-0.0/+0.0 and distinct NaN payloads count separately — matching the encoder's exact-roundtrip
contract. **Widening this guard (e.g. to `cardinality > 16`) without re-running the threshold
sweep across traces/logs/metrics would capture the low-cardinality population and regress it.**

**Exactness.** The encoder operates on raw 64-bit words, never float arithmetic, so every IEEE-754
value round-trips exactly: NaN (quiet/signaling, any payload, any sign), ±Inf, ±0.0, and denormals.
`leading` is clamped to 31 (5-bit field, standard Gorilla); `meaningful_len` is stored as `len-1`
in 6 bits; the decoder reconstructs `trailing = 64 - leading - meaningful_len`. `stream_bit_len`
bounds the readable bits so trailing zero padding in the last byte is never read as a control bit.

**No sparse variant.** High-cardinality float columns are overwhelmingly fully present after
numeric-string promotion; the dense kind already carries interleaved nulls via presence-RLE, and
the AllPresent layering (NOTE-AP-001) emits kind 41 for the common fully-present case. Sparse
(>50% nulls) high-cardinality floats are rare enough not to justify a fourth kind.

**Rollout flag:** `Config.DisableGorillaFloat64` (default false → Gorilla on) forces the Dictionary
form for every float column, mirroring the NOTE-215/217/218 atomic-bool pattern
(`constants.go:gorillaFloat64EncodingEnabled`).

**Backwards compatibility:** new kind IDs (40/41) only; no `enc_version` bump. Old readers reject
the unknown kinds at `reader/column.go:readColumnEncoding`. Compaction is transparent — it reads
decoded values via the reader API and re-encodes via the writer, re-evaluating selection on the
re-encoded column population, so the new kinds need no compaction code.

Back-ref: `shared/constants.go` (kinds 40/41 KindGorillaFloat64[AllPresent]),
          `writer/encoding_gorilla.go:encodeGorillaFloat64,shouldUseGorillaFloat64,float64PresenceAndCardinality`,
          `writer/column_types.go:float64ColumnBuilder.buildData`,
          `writer/constants.go:gorillaFloat64EncodingEnabled`, `writer/config.go:DisableGorillaFloat64`,
          `reader/column.go:decodeGorillaFloat64,decodeGorillaStream`,
          SPECS §9.8, SPEC-006, NOTE-40, NOTE-215, NOTE-AP-001, NOTE-007.

---

## NOTE-220 — V15 inline tiny columns in the block TOC

For tiny columns (low-cardinality intrinsics after RLE/dict, short-tail attributes) the per-column
framing dominates the payload: a V14 TOC entry's offset tail is `data_offset[8] + compressed_len[4]
+ uncompressed_len[4]` = 16 bytes, plus the per-column outer snappy framing (~5 B), plus the data
itself (often < 16 B). For such columns the framing is larger than the payload and the snappy
round-trip is pure overhead.

**Format (SPEC §12.2.1).** V15 (`VersionBlockV15` = 15) keeps the V14 24-byte block header
unchanged and adds a per-column `flags[1]` byte after `col_type`. When `ColFlagInline` (0x01) is
set, the column's raw blob follows as `inline_len[1] (≤ ColInlineMaxLen=255) + inline_data` — no
`data_offset`, no `compressed_len`, no data-section blob, and no outer snappy.

**Writer selection (`writer_block.go:finalize`).** After `buildData()` + outer-snappy, the writer
compares the inline tail cost `1 + len(raw)` against the non-inline cost `16 + len(compressed)`
(the shared `flags[1]` cancels). It marks the column inline when `1 + len(raw) < 16 +
len(compressed)` AND `len(raw) ≤ ColInlineMaxLen`. So inline is chosen only when it is strictly
smaller on the wire. Inline columns contribute nothing to the data section and do not advance the
running data offset; the TOC-size and write loops branch on `bl.inline`.

**Reader (`block_parser.go:parseColumnMetadataArray`).** Branches on `hdr.version`. V15 reads the
`flags` byte; on inline it slices `inline_data` straight out of the TOC bytes into
`colMetaEntry.inlineData` and uses it as the already-decompressed raw blob (no offset chase, no
snappy). The eager-decode and `AddColumnsToBlock` paths decode directly from `inlineData`. The
lazy-registration loop sets `Column.rawEncoding` directly so `ensureDecompressed` is a no-op and
`decodeNow` decodes straight from the inline bytes — inline columns skip the defer-decompression
machinery entirely (the "column exists?" fast path of NOTE-39 becomes a direct slice).

**`readSufficientToC`** grows the cold ToC read until `parseColumnMetadataArray` succeeds, which
now requires the inline bytes to be present, so the cached ToC always covers inline data. The
columnar assembled-buffer paths copy the full ToC prefix (`raw[:tocEnd]`, which includes inline
data) and skip `compressedLen == 0` columns from the per-column blob fetch — wanted inline columns
are served from the copied TOC prefix with zero extra fetch.

**Rollout flag:** `Config.EnableInlineColumns` (default **false** → V14). Unlike the NOTE-215/217/
218/219 Disable* flags this defaults OFF because V15 is a block-format version bump: a V14-only
reader cannot read a V15 block, so the writer must be deployed AFTER readers understand V15.
Process-level atomic `constants.go:inlineColumnsEnabled`; `emittedBlockVersion()` returns V15 when
set. Readers accept both V14 and V15. Compaction reads V14/V15 transparently and emits the
configured output version.

Back-ref: `shared/constants.go` (VersionBlockV15, ColFlagInline, ColInlineMaxLen),
          `writer/writer_block.go:finalize`, `writer/writer_log.go:buildLogBlock`,
          `writer/constants.go:inlineColumnsEnabled,emittedBlockVersion`,
          `writer/config.go:EnableInlineColumns`, `writer/writer.go:NewWriterWithConfig`,
          `reader/colmetaentry.go`, `reader/block_parser.go:parseColumnMetadataArray,parseBlockColumnsReuse`,
          `reader/reader.go:AddColumnsToBlock`, SPECS §12.2.1, NOTE-39, NOTE-V14-001, NOTE-007.

## NOTE-221 — data-driven encoding selector for the bytes column path (issue #333)

The legacy bytes selector (`encoding_select.go:isIDColumn/isURLColumn`, dispatched in
`bytesColumnBuilder.buildData`) chose the encoding purely from the column NAME suffix with **zero
inspection of the actual values**. Two concrete failure modes:

1. `isIDColumn("customer.duration_id")` → forced XOR onto a duration value because the name ends
   in `_id`. The suffix lied about the value shape.
2. `isURLColumn("config.file.path")` → forced Prefix onto paths with no shared prefix across rows,
   so the prefix dictionary stored a single empty prefix and every row paid the indirection.

NOTE-221 replaces the name-suffix dispatch with a two-tier system (`bytes_cost_select.go`):

**Tier 1 — semantic overrides.** A small, deliberate allow-list (`shared.semanticBytesOverrides`,
queried via `shared.SemanticBytesOverride`) of **intrinsic** columns whose best encoding is known
a-priori: `trace:id`/`log:trace_id` → DeltaDictionary (sorted 16-byte IDs), `span:id`/
`span:parent_id`/`log:span_id` → XOR (fixed-width IDs with shared high-order bits). Only intrinsic
names are eligible — user attribute names never match (`SemanticBytesOverride("attr.trace:id")`
returns `SemanticBytesNone`). Each entry carries a `reason` justification; the issue requires a
>10% win over the cost path before an entry is added.

**Tier 2 — cost-based selection.** `gatherBytesStats` does a single streaming pass over the present
values (alloc-free: cap-N FNV fingerprint distinct estimator, common-prefix fold, uniform-length
detector, total bytes). Pure estimators (`estimateDictBytesCost`/`estimateXORBytesCost`/
`estimatePrefixBytesCost`) rank candidate families by estimated wire bytes; the cheapest wins.

**Demoted name heuristics.** `isIDColumn`/`isURLColumn` survive only as a near-tie tiebreak
(`nameSuffixHint`): they break the decision toward XOR/Prefix only when the runner-up's estimate is
within `bytesCostTiebreakFraction` (5%) of the winner AND the hint targets one of the two
contenders. They can never override a clear cost winner — fixing both failure modes above.

**DeltaDictionary is NOT a cost candidate.** Its only edge over plain Dictionary is a delta-coded
index stream, which shrinks bytes only when the dictionary is sorted AND the per-row indexes are
clustered — a property the streaming stats can't cheaply establish. Crediting it unconditionally
made it spuriously beat Dictionary on unsorted low-cardinality columns. It is reachable only via
the semantic-override table, where sortedness is known a-priori.

The sparse/dense and AllPresent/uniform variants are still derived downstream by the encoders
(NOTE-AP-001, NOTE-217); the selector only picks the encoding *family*. No wire-format change, no
`enc_version`/block-version bump — this is purely a writer-side selection change, transparent to
readers and compaction.

Back-ref: `writer/bytes_cost_select.go`, `writer/column_types.go:bytesColumnBuilder.buildData`,
          `writer/encoding_select.go:isIDColumn,isURLColumn`,
          `shared/column_classify.go:SemanticBytesOverride,semanticBytesOverrides`,
          SPEC-006, NOTE-AP-001, NOTE-217, NOTE-V14-002.

## NOTE-222 — Data-driven Delta encoding for int64 columns (2026-06-12)

*Added: 2026-06-12*

**Decision:** `int64ColumnBuilder.buildData()` now runs a cheap cardinality estimate
(`cheapCardinalityInt64`, capped at 4 — same pattern as `cheapCardinalityUint64`) and
calls `shouldUseDeltaInt64(minVal, maxVal, cardinality)` before falling through to
Dictionary. When the column is Delta-eligible it follows the same path as `uint64`:
`encodeDeltaUint64` → `encodeDeltaUint64BitPacked` → `encodeDeltaUint64Paged`.

**Sign-extension invariant:** `shouldUseDeltaInt64` computes the range as
`uint64(maxVal - minVal)` — unsigned subtraction on the signed bit pattern, always
correct when `maxVal >= minVal` (guaranteed by the builder's min/max tracking).
When encoding, values are cast to `[]uint64` (bit-pattern-preserving); offsets are
`uint64(v) - uint64(base)`, always non-negative since `base = min(present values)`.
The reader reconstructs int64 values via `int64(base + offset)` using `colType`
(see `isDeltaInt64ColType` + `promoteToInt64Dict` in `reader/column.go`).

**Why this matters:** `ColumnTypeRangeDuration` (span duration, DB query time,
HTTP response time from numeric-string promotion via NOTE-40) and `ColumnTypeRangeInt64`
have the same quasi-monotonic, narrow-range-within-block profile as `span:start`
(uint64). Delta encoding wins significantly over Dictionary for these columns.
No new kinds are needed — kinds 5/22/23 already exist and the reader already decodes
them; only colType routing was missing.

Back-ref: `internal/modules/blockio/writer/column_types.go:int64ColumnBuilder.buildData`,
          `internal/modules/blockio/writer/encoding_select.go:shouldUseDeltaInt64`,
          `internal/modules/blockio/reader/column.go:isDeltaInt64ColType`,
          `internal/modules/blockio/reader/column.go:promoteToInt64Dict`

## NOTE-223: Range-readable chunked trace index (issue #340)
*Added: 2026-06-13*

**Problem:** the trace index was written as one snappy-compressed section
(`ToCSubTypeTrace`). A trace-by-id lookup had to fetch and decompress the *entire* section
to locate the blocks for a single trace. Profiling showed `FindTraceByID` was the largest
allocator on the read path, ~97% of it the whole-section fetch+decompress — a cost
proportional to the trace count of the file, not to the one trace requested.

**Solution:** write the trace index as `ToCSubTypeTraceChunked` — a section written *raw*
(via `writeRawToCEntry`, not snappy-compressed as a whole) so it is byte-addressable:
a fixed header, a chunk directory `(first_trace_id, comp_off, comp_len)`, then
`ChunkedTraceEntriesPerChunk`-sized independently snappy-compressed chunks, then the trace
ID bloom. Each chunk decompresses to a v2 mini-body (`fmt_version[1] + entry_count[4] +
sorted entries`) so the reader reuses the existing entry encode/scan. New files write only
this section (leaner — no duplication); the reader prefers it and falls back to the legacy
`ToCSubTypeTrace` section when absent.

Back-ref: `internal/modules/blockio/writer/chunked_trace_index.go:buildChunkedTraceIndex`,
          `internal/modules/blockio/writer/v8_sections.go:writeRawToCEntry`

---

## NOTE-399: Stop storing the span:end per-row block column (synthesize on read)
*Added: 2026-06-15*

**Problem:** `span:end` was written as a per-row block column on every write path
(`feedSpanTiming` for ingest, `applySpanEnd` for compaction) even though it is **fully
derivable** from `span:start + span:duration`, both of which are always stored. On a
representative block the per-row `span:end` payload accounted for ~8% of file size — pure
redundancy. It was already absent from the intrinsic TOC section and synthesized on read
there (`reader.synthesizeSpanEnd`); only the block-column copy remained.

**Decision:** Stop emitting the `span:end` per-row block column entirely. Keep only the
`span:end` range-index min/max so block pruning of `span:end` predicates stays correct
(16 bytes per block, negligible). All read paths already resolve `span:end` via
`GetIntrinsicColumn("span:end")` → `synthesizeSpanEnd`, so query results are byte-identical.

**Write paths changed:**
- `feedSpanTiming` (proto ingest): dropped the `addPresent(span:end, ...)` call; kept
  `updateMinMaxNum` for the range index.
- `applySpanEnd` (compaction, legacy source blocks that still carry span:end): no longer
  writes a block column — only updates the range-index min/max from the source value.
- `applySpanDuration` now returns the duration value so `finalizeRowBookkeeping` can
  synthesize the `span:end` range-index entry (`start + duration`) when the source block
  lacks `span:end` (i.e. it was itself written under NOTE-399). Per-row payload is never
  re-emitted.
- `Writer.AddRow` validation relaxed: a source block is accepted when it carries *either*
  `span:end` (legacy) *or* `span:duration` (from which `span:end` is derivable).

**Back-compat:** readers still read `span:end` when a legacy file contains it (the
intrinsic synthesis path is preferred; the block column is simply ignored on the read
side since the executor always goes through `GetIntrinsicColumn`).

Back-ref: `internal/modules/blockio/writer/writer_block.go:feedSpanTiming`,
          `internal/modules/blockio/writer/writer_block.go:applySpanEnd`,
          `internal/modules/blockio/writer/writer_block.go:applySpanDuration`,
          `internal/modules/blockio/writer/writer_block.go:finalizeRowBookkeeping`,
          `internal/modules/blockio/writer/writer.go:AddRow`,
          `internal/modules/blockio/reader/intrinsic_reader.go:synthesizeSpanEnd`

## NOTE-402 — Skip per-column sketch on identity / high-entropy ID columns (issue #354)

**Decision:** Do not build the per-column sketch (HLL distinct-count + TopK + 2 KiB SketchBloom)
for identity / high-entropy ID columns: `span:id`, `span:parent_id`, `log:span_id`.

**Why:** The per-column SketchBloom powers predicate block-pruning (`queryplanner/scoring.go`,
`FuseContains`). On high-cardinality identity columns every block holds thousands of distinct
random IDs, so the bloom is saturated (~5% FPR at 5 000 values) and prunes nothing — yet each
such column emits a *maximal* 2 KiB bloom per block, making these the single most expensive
sketches on disk (~3–4% of file size on a representative block). No TraceQL query computes
quantiles/histograms or block-prunes on span/parent IDs (ID lookups use the trace-ID bloom and
equality scans, independent of the per-column sketch), so dropping these sketches loses nothing.

**Mechanism:** A `shared.ShouldSketchColumn(name)` predicate (allow-list in
`internal/modules/blockio/shared/column_classify.go`) gates the single write-side chokepoint,
`blockSketchSet.add` (`sketch_index.go`). Skipped columns are never inserted into the
`blockSketchSet`, so `writeOneColumnSketchBlob` returns `nil` for them and they never appear in
the sketch TOC.

**Back-compat / no format change:** The reader already tolerates an absent per-column sketch —
`Reader.ColumnSketch` returns `nil` and the scoring/pruning path takes its conservative
"pass all candidates" branch. Existing files are unaffected; new files simply emit fewer sketch
blobs. Timestamps (`span:start`/`span:end`/`__timestamp__`) and durations (`span:duration`) are
NOT skipped — they back range-boundary pruning and quantile/histogram estimation. `trace:id`
is intentionally NOT skipped here (treated separately per the issue).

Back-ref: `internal/modules/blockio/shared/column_classify.go:ShouldSketchColumn`,
          `internal/modules/blockio/writer/sketch_index.go:add`

## NOTE-405 — Per-column zstd codec, benefit-gated, for V15 column blobs (issue #355)

*Added: 2026-06-16*

**Problem:** the per-column section/page compressor is snappy. Measured on real encoded
payloads, zstd (`SpeedDefault`) beats snappy substantially on the dict/ID-encoded columns
(trace:id ~30% of snappy size, span:parent_id ~61%, span:id ~63%) while showing ~0% headroom on
already-bit-packed columns (span:start/span:end). The read path is I/O/alloc-bound, so zstd's
2–3× slower decode is single-digit ms, immaterial against object-storage round-trip latency.

**Change:** a purely additive per-column codec choice on the V15 column TOC entry. Added
`shared.ColFlagZstd` (0x02) to the V15 per-column flags byte (NOTE-220 added the byte; only bit 0
ColFlagInline was used). When the per-column zstd rollout flag is active, `blockBuilder.finalize`
compresses each non-inline V15 blob with BOTH snappy and zstd and keeps zstd only when
`len(zstd)·zstdBenefitDen < len(snappy)·zstdBenefitNum` (currently 97/100 → zstd ≥3% smaller),
flagging those blobs with ColFlagZstd. Incompressible/bit-packed blobs (no headroom) and inline
columns (stored raw) stay on snappy. The benefit comparison runs BEFORE the inline decision so
inline weighs against the smaller codec.

**Codec selection is keyed off metadata, not the blob source.** The reader's
`colMetaEntry.zstd` (parsed from the flags byte) drives `decompressV14ColumnData[Into]`'s codec
in every decode path — eager full-block, lazy WantOnly defer-decompress, and the compaction
pass-through `resolveColumnData` (which returns the same on-disk bytes). No file size grows: a
blob lacking the bit decodes as snappy byte-for-byte as before.

**Rollout:** `Config.EnableZstdColumns` (default OFF, requires `EnableInlineColumns`/V15). The
reader is always codec-aware so no reader version bump is needed; emission is toggle-gated so it
can be enabled and reverted without a format change. Encoder is a package-level
`zstd.SpeedDefault`/`EncoderConcurrency(1)` writer constructed lazily (the OFF default never
builds it), mirroring the existing vectorF32 encoder.

Back-ref: `shared/constants.go:ColFlagZstd`, `writer/constants.go:zstdColumnsEnabled` +
          `zstdBenefit{Num,Den}`, `writer/config.go:EnableZstdColumns`,
          `writer/writer_block.go:finalize` (codec choice + flag write) +
          `getColumnZstdEncoder`; reader side: `reader/block_parser.go:parseColumnMetadataArray`
          (flag parse) + `decompressV14ColumnData[Into]` (codec select),
          `reader/colmetaentry.go:zstd`, `reader/column.go:compressedZstd`.

## NOTE-411 — Writer always emits v2 paged intrinsic columns (issue #357)

*Added: 2026-06-16*

**Problem:** the writer split intrinsic columns by row count: ≤`IntrinsicPageSize` (10k) rows
went to the v1 monolithic format (`encodeFlatColumn`/`encodeDictColumn`), above went to the
non-legacy formats (XORBytes/DeltaUint64 single-page for flat, paged for dict). That split kept
two reader decode paths alive (`decodeLegacyFlatBlob`/`decodeLegacyDictBlob`) and forced the
NOTE-406/407 streaming group-by paths to retain a non-streamable v1 fallback (paged dict IS
page-streamable; v1 dict is not).

**Change (write path only):** `intrinsicAccumulator.encodeColumn` no longer consults
`IntrinsicPageSize` as a format-selection threshold. It always emits the v2 paged format for
non-empty columns — XORBytes for flat bytes, DeltaUint64 for flat uint64 (both single-page paged
blobs), paged dict for dict columns. `IntrinsicPageSize` is retained as the per-page CHUNK size
for multi-page columns (`encodePagedDictColumn`); it is NOT removed (setting it to 0 would divide
by zero in the page-count math and break `parallelPageDecodeMinRows`). Empty columns still fall
through to `encodeFlatColumn` (a degenerate v1 blob with nothing to stream).

**Reader cleanup is deliberately NOT done here.** Existing on-disk blocks still contain v1 blobs;
`decodeLegacyFlatBlob`/`decodeLegacyDictBlob` and the streaming v1 fallback MUST stay until those
blocks are recompacted out (per issue #357 trade-offs). This is the safe half of the issue; the
reader-side deletion is a follow-up gated on block migration.

**Latent format-dispatch bugs surfaced.** Because small columns were always v1 Flat/Dict before,
several executor format switches handled only `IntrinsicFormatFlat` (reading `col.Uint64Values`)
and silently routed everything else to an absent/empty path. With small span:duration now always
`IntrinsicFormatDeltaUint64`, those switches collapsed histograms to boundary-0 and zeroed SUM/AVG.
Folded `IntrinsicFormatDeltaUint64` (and `IntrinsicFormatXORBytes` where bytes group-by applies)
into the Flat branch of: `streamHistogramGroupBy`, `streamHistogramGroupByID`,
`streamHistogramGroupByIDSingle`, `accumulateAggDirectScanCol`, `buildAggValsForRef`,
`buildAggValsMap`, and `buildDictIdxForRefs`. Same class as the NOTE-410 fix, re-surfaced for ALL
small duration columns by always-paged. `countIntrinsicHistogramBoundaries` already handled Delta
via its sorted-gallop fast path (NOTE-379) and needed no change.

Back-ref: `writer/intrinsic_accum.go:encodeColumn` + `decodeIntrinsicColumnBlob` (test helper now
uses eager-refs decode since flat columns are paged-lazy-ref); executor folds in
`metrics_trace_intrinsic.go`; guard tests `executor/intrinsic_alwayspaged_test.go`.

---

## NOTE-446: Per-block per-column statistics section (ToCSubTypeColStats, issue #364)
*Added: 2026-06-18*

**Problem:** Blockpack could not skip column fetches for a column wholly absent from a block.
Q9 `{kind=server} >> {kind=client && span.rpc.method != ""}` fetched `span.rpc.method` for every
block even when entirely null (the common case). `!=` predicates produced NO pruning node
(`extractTraceQLNodes`: negations cannot prune), so the planner kept every block.

**Solution:** a new file-level ToC section `ToCSubTypeColStats = 9` with one entry per block
holding packed per-column statistics. Wire format (`shared/colstats.go`):
`entry_count[4]` then per block `block_idx[2] + col_count[2]` then per column
`name_len[2]+name + stats_flags[1] + present_count[4]` and, when `stats_flags & 0x01`,
`min_uint64[8] + max_uint64[8]`. Snappy-compressed whole section, fetched lazily once per file
and cached per-Reader (mirrors the TS/bloom section path).

**Writer:** stats are captured inside `blockBuilder.finalize` while the column builders are still
live — `present_count = rowCount() - nullCount()` per column, plus a numeric `[min,max]` range
pulled from the per-block `colMinMax` bookkeeping. Numeric range is emitted ONLY for unsigned
families (uint64 / duration): the LE bytes are compared as uint64, which is wrong for signed
int64 (negatives sort wrong under unsigned compare), so int64 columns carry presence only. Both
the trace and log flush passes feed `w.colStatsByBlock`; the section is sorted by block index and
written in `writeV8FileSections`. Computing during `finalize` avoids a second walk over the columns
(they are cleared right after).

**Executor:** `pruneByColStats` (in `plan_blocks.go`) runs last in `planBlocks`, after intrinsic
TOC intersection, refining the selected set with no extra I/O beyond the lazy ColStats fetch. A
leaf that requires presence (RequirePresent / equality / range / pattern) prunes a block where the
column has `present_count == 0`; a numeric leaf bound that cannot intersect the block `[min,max]`
prunes the block. Conservative: AND rejects if any child rejects, OR rejects only if all children
reject, and an unevaluable predicate keeps the block.

**The `!= ""` fix:** `attr != ""` requires the attribute to be present (an absent row never
matches), so OpNeq against an empty string literal compiles to a `RequirePresent` RangeNode
(`extractNeqNode`, renamed from `extractNeqPresenceNode`) — scoped to one leaf or, for unscoped
attrs, an OR over resource/span/log presence. NOTE-453 (issue #369) later generalized this: ALL
scoped `!= V` (any V) now emit presence + an `OR(> V, < V)` range rewrite for KLL bounds pruning;
see vm/executor NOTE-453.

**Backward compatibility:** new optional section; old readers skip unknown ToC types. Files without
the section get no ColStats pruning (`HasColStats()` returns false). No format-version bump.

Back-ref: `shared/colstats.go` (codec), `writer/writer_block.go:finalize` (capture),
`writer/v8_sections.go:writeV8FileSections` (write), `reader/parser.go:ColStats/HasColStats`
(read), `executor/plan_blocks.go:pruneByColStats`, `vm/traceql_compiler.go:extractNeqNode`.

**Extension (NOTE-448, issue #367):** `HasNumRange` was subsequently extended to `ColumnTypeFloat64`,
`ColumnTypeRangeFloat64`, `ColumnTypeInt64`, and `ColumnTypeRangeInt64`. Float64 uses the
existing `math.Float64bits` LE encoding (already populated in `numMinKey`/`numMaxKey`). Int64
uses the raw int64 bit pattern as uint64 LE — correct for round-trip via `int64(stat.MinNum)`
on the executor side. The executor's `colStatsRejects` was updated to dispatch on value type
before choosing the comparison path; old files (HasNumRange=false for these types) are unaffected.

**Extension (NOTE-452, issue #373):** `HasNumRange` extended to `ColumnTypeBool`. Bool min/max
is tracked as uint64 `0`/`1` (true=1, false=0) in `updateMinMaxFromAttr` /
`updateLogMinMaxFromAttr`, giving each block a `[min,max]` range over `{0,1}`. Bool remains
EXCLUDED from the on-disk range index (no `RangeBool` type): the exclusion was moved out of
`updateMinMaxFromAttr` into the range-index build loops in `writer.go`
(`if mm.colType == ColumnTypeBool { continue }`). This lets the executor prune blocks for
`attr = true` (`blockMax == 0` → all false) and `attr = false` (`blockMin == 1` → all true)
while keeping `BlocksForRange` bool-free. See executor NOTE-452.

## NOTE-458 — Writer.FlushedBytes(): real on-disk size for block-cutting (issue #377)

`CurrentSize()` only reflects the spans still pending in the in-memory buffer; after each
internal `flushBlocks()` auto-flush (at `MaxBufferedSpans`) the pending buffer is cleared,
so `CurrentSize()` drops back toward 0. A caller polling it during ingestion to decide when
to cut a block sees a sawtooth that never reaches the configured byte limit.

`FlushedBytes()` returns `w.out.total` — the monotonically increasing count of bytes the
`countingWriter` has actually written to the `OutputStream` (block payloads are written in
the serial merge pass of `flushBlocks`, and the V8 sections at final `Flush`). Combined with
`CurrentSize()` (an estimate for the not-yet-encoded pending tail), a caller gets an accurate
running on-disk size throughout ingestion.

Motivation: Tempo's `vblockpack` WAL block previously estimated `DataLength()` as
`meta.TotalObjects × bytesPerSpan`. But `TotalObjects` is now a **trace** count (one
`ObjectAdded` per `AppendTrace`), aligning blockpack with parquet's trace-based
`max_compaction_objects`. The old per-span estimate therefore under-counted by the
spans-per-trace factor (~8×), letting WAL blocks grow far past `max_block_bytes`.
`DataLength()` now uses `FlushedBytes() + CurrentSize()` instead. Anchored in
`cmd/deadcode/main.go` (public API consumed only by Tempo).

## NOTE-462 — SpanTree structural index (issue #381)

A new ToC entry (`ToCSubTypeSpanTree`) stores, per span, `(traceID, spanID, parentID, dfsIn,
dfsOut, blockIdx, rowIdx)`. The DFS in/out counters — assigned per trace by a depth-first walk
— collapse ancestor/descendant checks to two integer comparisons (`shared.IsDescendant`):
`J descends from I  iff  I.dfsIn < J.dfsIn AND J.dfsOut < I.dfsOut`. This enables block-level
structural pruning for TraceQL `>>`/`<<`/`~` without loading spans and reconstructing the tree,
and subsumes the chunked trace index for trace-by-ID (every `(blockIdx,rowIdx)` per trace is
present).

**Streaming requirement (mirrors NOTE-461):** one record per span would be an O(spans)
in-memory accumulator. `spanTreeAccum` (writer/spantree_tempfile.go) instead runs a bounded
EXTERNAL SORT: during block build it appends fixed-stride spill records to runs of at most
`spanTreeRunRecords` (sorted in memory, then flushed to a run file, buffer released — peak
RAM = one run buffer, ~9 MiB). At `Flush()` a k-way `container/heap` merge over the sorted run
files yields global `(traceID, spanID)` order; consecutive same-traceID records form one trace
that is held in memory (bounded by the largest single trace, NOT the file), DFS-numbered
(`dfsNumber`, iterative DFS — no recursion depth risk; dangling/cyclic parents degrade to roots
so every span still gets a valid interval), and emitted into independently-snappy-compressed
chunks. A trace's records never split across a chunk boundary, so one chunk read resolves any
single trace.

**Section framing** mirrors the chunked trace index (issue #340): written RAW via
`writeRawToCEntry` = header[36] + chunk directory[28/entry] + concatenated snappy chunks +
trace-ID bloom. Within a chunk the decoded records are fixed stride
(`shared.SpanTreeRecordSize`=44) so the reader binary-searches on traceID without a full decode.

**Reader** (reader/spantree.go): `ensureSpanTreeSection` parses header+directory in one range
read; `SpanTreeForTrace(traceID)` bloom-rejects, binary-searches the directory for the one
chunk, range-reads + snappy-decodes it, then binary-searches the fixed-stride records.

**Migration:** new blocks write SpanTree; the old compact trace index *decoder* is retained for
reading pre-migration blocks. Compaction rebuilds SpanTree from output spans automatically
(compaction feeds the Writer via AddRow → buildBlock → localAccum → the same serial merge pass
that feeds `feedSpanTreeFromAccum`); old ToC entries are never copied forward.

**Codec & predicates** live in `shared/spantree.go` (EncodeSpanTreeRecord/DecodeSpanTreeRecord,
IsDescendant/IsAncestor/IsRoot/AreSiblings) so writer and reader share one wire format.

---

## NOTE-468 — SpanTree per-chunk span-ID blooms + GetTraceByID fast path (issue #388)

SpanTree v2 (`SpanTreeVersion = 0x02`) adds a **per-chunk span-ID bloom region** and wires
`GetTraceByID` through the SpanTree, which is the prerequisite for dropping the per-block
`trace:id`/`span:id` columns (issue #389 / NOTE-050) without re-introducing the whole-file
intrinsic-scan regression NOTE-293 fixed.

**Wire format (v2):** the section grows from a 36-byte header (v1) to a 44-byte header by
appending `span_bloom_off[4]` + `span_bloom_stride[4]`. The body layout becomes
`header[44] + chunkDir[N×28] + spanBloomRegion[N×SpanTreeChunkBloomSize] + compressedChunks +
traceIDBloom` — the span-bloom region sits between the directory and the chunk bodies, and each
directory entry's `compOff` points past the region into the chunk bodies.

**Per-chunk bloom:** `spanTreeEncoder` accumulates an `SpanTreeChunkBloomSize` (8 KiB) span-ID
bloom for the open chunk (`shared.AddSpanIDToBloom` on each emitted record), then moves it into
the region on `sealChunk`. At ~SpanTreeRecordsPerChunk span IDs per chunk with `SpanIDBloomK=6`
hashes this is ≈2% FPR, so a span lookup admits ≈1 real chunk + ≈0 false positives.

**Backwards compatibility:** the reader accepts both v1 and v2 (`SpanTreeVersionV1`/
`SpanTreeVersion`). A v1 file has `spanBloomStride == 0`; `SpanTreeChunksForSpan` then reports
*all* chunks as candidates (vacuously correct, no false negatives) and `TestSpanIDBloom`
returns true for an empty bloom. v2 files remain readable by the v1 trace-by-ID path because
the appended header fields and the new region are simply not referenced by it.

**Reader lookups** (reader/spantree.go): `ensureSpanBloomRegion` range-reads the whole region
in one I/O (cached per-Reader); `SpanTreeChunksForSpan(spanID)` probes every chunk bloom in
memory; `SpanTreeRecordForSpan(spanID)` resolves a span ID to its `(traceID, blockIdx, rowIdx)`
record by decoding only the candidate chunk(s) — a span→trace capability that did not exist
before.

**Executor wiring** (root `reader.go` `GetTraceByID`): when `HasSpanTree()`, `SpanTreeForTrace`
returns the exact `(BlockIdx, RowIdx)` and `SpanID` for every span in the trace, so the
per-block `trace:id` column scan (`MatchingBytesRows`) and per-block `span:id` column reads are
skipped entirely; span IDs are sourced authoritatively from the SpanTree records. The legacy
per-block-column scan and the whole-file intrinsic fallback are retained only for pre-SpanTree
files. This does not yet drop the columns from block payloads — that is the follow-up (#389)
now unblocked.

---

## NOTE-466: MinHash-primary sort order rejected (issue #385)
*Added: 2026-06-23*

**Decision:** Keep the production span order `(service.name, span.name, MinHash, TraceID)`
(NOTE-457). Issue #385 proposed inverting to MinHash-primary `(MinHash, TraceID)` or a coarse
8-bit bucket hybrid `(MinHashBucket8, TraceID)` on the hypothesis that attribute-set similarity
clustering — independent of service/span name — would improve column compression. **It does
not.** Both candidates were rejected.

**Method:** `compareSpanSortKey` was extracted into a swappable package var `spanSortKeyCmp`
(default = the production comparator). A reproducible white-box benchmark
(`sort_order_investigation_test.go`, `TestSortOrderInvestigation_MinHashPrimary`) writes one
representative multi-service corpus — where attribute "shapes" deliberately cross-cut services,
the best case for the proposal — end-to-end through the real writer under each order, then
measures total compressed file size and per-block distinct service.name / span.name counts.

**Findings (representative corpus, 22 blocks):**
- File size: MinHash-primary was *larger* than prod; the bucket hybrid was larger still. The
  proposed compression win does not materialize — it regresses.
- Range-index homogeneity: prod keeps blocks near single-service (because service.name is the
  primary key) — ~2 distinct services/block — while both candidates scatter every service into
  every block (= service count/block). Dropping service.name as the primary key destroys the
  NOTE-457 exact-value range-index fast path (min==max ⇒ zero-false-positive equality pruning)
  for `{resource.service.name = X}`, the single most common metrics filter.

**Why the hypothesis failed:** the ID columns (span:id, trace:id, span:parent_id) dominate
compressed bytes at ~1.0x regardless of order, so the addressable string-column fraction is
small; and `span:name` already compresses extremely well under the current order because
span.name is a primary key. MinHash-primary trades away guaranteed service/name homogeneity
(real query-pruning value) for marginal, here-negative, string-column gains.

**Reproduce:** `go test ./internal/modules/blockio/writer/ -run TestSortOrderInvestigation -v`
The candidate comparators live test-only in `export_test.go`
(`CompareSpanSortKeyMinHashPrimaryForTest`, `CompareSpanSortKeyMinHashBucketForTest`) so they
are not dead code in production.

**Back-ref:** `internal/modules/blockio/writer/writer_sort.go:compareSpanSortKey`,
`internal/modules/blockio/writer/sort_order_investigation_test.go`.

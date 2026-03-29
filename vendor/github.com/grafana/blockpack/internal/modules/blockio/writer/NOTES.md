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

## NOTE-003: V1 Monolithic Format Removed (2026-03-28)
*Added: 2026-03-28*

**Decision:** Removed `encodeFlatColumn` and `encodeDictColumn` (v1 monolithic encoders).
`encodeColumn` now always calls `encodePagedFlatColumn` or `encodePagedDictColumn`.

**Rationale:** V1 blobs cannot carry per-page ref index (MinRef/MaxRef/RefBloom) since
they have no page structure. Removing v1 simplifies the codec by eliminating the dual
encode/decode path. Single-entry columns now produce a 1-page v2 blob (~100 bytes of TOC
overhead), which is acceptable.

**Impact:** Old v1 blobs return a decode error from `DecodeIntrinsicColumnBlob` (message:
"unsupported intrinsic column format"). All in-memory test blobs are rebuilt using the
writer's new v2 path, so test regressions are eliminated. On-disk files from before this
change would need to be rewritten; the parity test confirms the end-to-end path works.

Back-ref: `internal/modules/blockio/writer/intrinsic_accum.go:encodeColumn`,
`internal/modules/blockio/shared/intrinsic_codec.go:DecodeIntrinsicColumnBlob`

---

## NOTE-004: Ref-Bloom Added to Paged Columns (2026-03-28)
*Added: 2026-03-28*

**Decision:** `encodePagedFlatColumn` and `encodePagedDictColumn` now compute per-page
`MinRef`/`MaxRef` and `RefBloom` alongside the existing value bloom and value min/max.

**Rationale:** Allows `GetIntrinsicColumnForRefs` to skip pages during reverse lookup.
Previously, `lookupIntrinsicFields` decoded all N rows in all pages; now it decodes only
pages whose ref-range covers the M target refs.

**RefBloom key:** 4-byte LE encoding of `uint32(blockIdx)<<16|uint32(rowIdx)`.

**RefBloom parameters:** Same as value bloom — `IntrinsicPageBloomK=7`,
`IntrinsicPageBloomBitsPerItem=10`, clamped to `[IntrinsicPageBloomMinBytes=16, 4096]`.

**Helper functions added:**
- `computePageRefRange(refs []BlockRef) (minRef, maxRef uint32, refBloom []byte)` — flat columns
- `collectDictPageRefs(entries, entryRanges) []BlockRef` — dict columns (gathers refs for one page)

Back-ref: `internal/modules/blockio/writer/intrinsic_accum.go:encodePagedFlatColumn`,
`internal/modules/blockio/writer/intrinsic_accum.go:encodePagedDictColumn`,
`internal/modules/blockio/writer/intrinsic_accum.go:computePageRefRange`

## NOTE-005: Identity Columns Removed from Intrinsic Accumulator (2026-03-29)
*Added: 2026-03-29*

**Decision:** `feedIntrinsicBytes` calls for `trace:id`, `span:id`, `span:parent_id` and
`feedIntrinsicString` for `span:status_message` are removed from `addRowFromProto`,
`addRowFromTempoProto`, `applyTraceID`, `applySpanID`, `applySpanParentID`, and the
`spanStatusMsgColumnName` case in `addRowFromBlock`. The corresponding `addPresent` calls
are retained — block column payloads still store these values. `feedIntrinsicsFromIndex`
now skips feeding these four columns to avoid carrying them forward in new intrinsic
accumulators. `computePageRefRange` and `collectDictPageRefs` removed (no longer needed).

**Rationale:** These are identity/display-only columns — they are never used as predicate
targets in the intrinsic predicate-evaluation path (`scanIntrinsicLeafRefs`). They only
appeared in the intrinsic section to support `lookupIntrinsicFields` reverse lookups during
field population. Since field population for Case A (plain) now uses `forEachBlockInGroups`
(block reads), the intrinsic section entries are redundant.

**Storage savings:** Removing these 4 columns shrinks the intrinsic section from 11 columns
to 7, approximately 48% reduction in intrinsic section size (~125 MB per large file). The
block column payloads retain these values unchanged.

**Back-ref:** `writer/writer_block.go:addRowFromProto`, `writer/writer_block.go:addRowFromTempoProto`,
`writer/writer_block.go:applyTraceID`, `writer/writer_block.go:feedIntrinsicsFromIndex`

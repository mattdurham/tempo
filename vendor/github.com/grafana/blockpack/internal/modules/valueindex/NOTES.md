
---

## NOTE-VI-011 — Type-aware canonical comparison for numeric columns

Date: 2026-06-19

Range/between predicate matching and sort ordering for numeric column types (uint64, int64,
float64) must decode canonical LE bytes back to native types before comparison. LE byte
ordering does not preserve numeric ordering across byte boundaries (e.g. uint64(255) encodes
as [0xFF,0x00,...] and uint64(256) encodes as [0x00,0x01,...], so bytes.Compare would
incorrectly report 255 > 256).

`compareCanonical()` centralises this logic and is used by:

- `rangePredicate.Match` and `betweenPredicate.Match` in predicate.go
- `sortRawSlice` in writer.go (called by both writer and compaction)

String, bytes, bool, and UUID columns retain `bytes.Compare` which is correct for their
encodings (UTF-8 lexicographic order is byte-order-safe).

Back-ref: `internal/modules/valueindex/predicate.go:compareCanonical`

---

## NOTE-VI-012 — Range* column types are indexed as their scalar equivalents

Date: 2026-06-25

`ColumnTypeRange*` columns are NOT [start, end] pairs. They are scalar columns whose values
participate in the block-level range index (hence the name). Each Range* type stores a single
value of the same bit width as its non-Range counterpart:

| ColumnType | Go type passed to AddEntry | Canonical encoding |
|---|---|---|
| `ColumnTypeRangeInt64` | `int64` | 8-byte LE (same as ColumnTypeInt64) |
| `ColumnTypeRangeDuration` | `int64` (nanoseconds) | 8-byte LE (same as ColumnTypeInt64) |
| `ColumnTypeRangeUint64` | `uint64` | 8-byte LE (same as ColumnTypeUint64) |
| `ColumnTypeRangeFloat64` | `float64` | 8-byte LE IEEE 754 (same as ColumnTypeFloat64) |
| `ColumnTypeRangeString` | `string` | UTF-8 bytes (same as ColumnTypeString) |
| `ColumnTypeRangeBytes` | `[]byte` | raw bytes (same as ColumnTypeBytes) |

This means `duration > 50ms` (where `duration` is a `ColumnTypeRangeDuration` column storing
nanosecond int64 values) is answered by the value index directly — the predicate is a normal
`rangePredicate` with `OpGT` and an int64 threshold, identical to querying any int64 column.

`compareCanonical()` was extended with the same groupings so sort order is correct for
numeric Range*types. The KLL sketch in `buildKLL()` treats numeric Range* types as uint64
bit-patterns, which is correct since the KLL is only used for bucket boundaries.

Only `ColumnTypeVectorF32` remains excluded — it is a float32 array with no natural scalar.

Back-ref: `internal/modules/valueindex/hash.go:CanonicalValue`,
`internal/modules/valueindex/predicate.go:compareCanonical`,
`internal/modules/valueindex/writer.go:buildKLL`

---

## NOTE-VI-014 — BlockID field added to Entry (VINX v2)

Date: 2026-06-25

Each posting list entry now carries a `BlockID uint32` — the zero-based index of the block
within `SourceRef` that contains the indexed span. This eliminates a block-index lookup
when resolving a `QueryResult` back to span data:

**Before (v1):** open `SourceRef` → load block index (`ToCSubTypeBlockIndex`) → search for
`TraceID` → seek to block → decode.

**After (v2):** open `SourceRef` → seek directly to block `BlockID` → decode. One index
lookup eliminated per result.

The cost is 4 bytes per posting list entry. In practice the overhead is near-zero after
snappy compression because spans from the same trace cluster into the same block, so
`BlockID` values repeat heavily across entries.

**Wire format change:** `ValueIndexEntriesVersion` bumped from `0x01` to `0x02`.
The old version constant is retained as `ValueIndexEntriesVersionV1 = 0x01`.
`DecodeChunkRange` and `decodeVINXSection` accept both versions; v1 files decode with
`decodeChunkPayloadV1` which sets `BlockID = 0` for all entries (callers must fall back
to block-index lookup for v1 results).

Back-ref: `internal/modules/valueindex/entries.go:Entry`,
`internal/modules/valueindex/entries.go:encodeChunkPayload`,
`internal/modules/valueindex/entries.go:decodeChunkPayload`,
`internal/modules/valueindex/entries.go:decodeChunkPayloadV1`,
`internal/modules/valueindex/reader.go:QueryResult`,
`internal/modules/blockio/shared/constants.go:ValueIndexEntriesVersion`
`internal/modules/valueindex/predicate.go:compareCanonical`,
`internal/modules/valueindex/writer.go:buildKLL`

## NOTE-VI-024 — ColTypeName bucket helper (issue #409)

Date: 2026-06-28

`ColTypeName(colType) string` (in `hash.go`, alongside `ColHash`) returns the
short, human-readable S3 path segment for a column type, used by the consumer to
keep same-name-different-type index files under distinct prefixes (full rationale
in `valueindexconsumer/NOTES.md` NOTE-VI-024). The mapping mirrors `CanonicalValue`
exactly: Range* types collapse to their scalar bucket (they are indexed as their
scalar equivalent, NOTE-VI-012). Unindexable types (VectorF32) return `""` so
callers can reject rather than silently bucket under an empty segment.

## NOTE-VI-026 — Writer external sort-merge bounds peak memory (issue #413)

Date: 2026-06-28

`writerImpl` previously held every posting-list entry in memory from `AddEntry`
until `Flush`, then sorted in place, copied the whole slice to a parallel `[]Entry`,
and encoded all chunks at once. For high-volume low-cardinality columns
(`resource.k8s.namespace.name` ~7M refs, `resource.k8s.cluster.name` ~6.9M refs) this
peaked the consumer at ~2 GB RSS and triggered kubelet OOM-kills. pprof attributed
~64 GB allocated to `AddEntry`, ~54 GB cumulative to the flush sort/encode, ~11 GB to
the dedup pass, ~10 GB to snappy.

Fix (issue Option A — streaming external sort-merge):

- `AddEntry` buffers entries until `shared.ValueIndexWriterSpillEntries` (500k, ~36 MB
  at ~72 B/entry), then sorts that run and spills it to a temp file (`runspill.go`:
  `writeRun`), resetting the buffer. Peak in-memory buffer is one run regardless of
  total cardinality.
- `Flush` k-way merges all spilled runs plus the sorted in-memory tail
  (`runspill.go`: `mergeRuns`), deduplicating on the fly with the *same* key as
  `deduplicateEntries`, streaming the result through the shared serialization core.
- The serialization core was extracted into `writerImpl.assemble` (writer.go), which
  drives a streaming `chunkEncoder` (entries.go), an incremental `kllBuilder`
  (writer.go), and incremental VHIX hash-index construction from a single sorted pass.
  Peak memory there is one chunk plus the directory/hash-index, which scale with
  *distinct values* (tiny for low-cardinality columns), never the full posting list.
- The in-memory fast path (`flushSorted`) is unchanged for columns that never spill:
  it sorts, dedups, and calls the same `assemble`. `EncodeEntries` now delegates to
  `chunkEncoder` so chunk encoding has a single implementation.

Correctness guarantee: `TestSpillPathEquivalence` asserts the spilled output is
**byte-identical** to the in-memory output for the same logical input, so the spill is
a pure memory optimization with no wire-format change. `mergeRuns` uses
`compareRawEntry` (matching `sortRawSlice`) and `sameEntry` (matching
`deduplicateEntries`) so order and dedup are identical to the fast path.

The spill codec recomputes `valueHash` from `canonicalValue` on read rather than
persisting it (saves 16 B/entry on disk). Run temp files are always cleaned up via
`discardRuns` (called from `Close` and deferred in the merge `Flush`).

Affects both the standalone consumer binary and the in-process Tempo module, since
both go through `valueindex.Writer`. The compaction path (`flushBatch`) is unchanged —
it still receives an already-sorted slice and calls `flushSorted`.

Back-ref: `internal/modules/valueindex/runspill.go`,
`internal/modules/valueindex/writer.go:AddEntry`,
`internal/modules/valueindex/writer.go:Flush`,
`internal/modules/valueindex/writer.go:assemble`,
`internal/modules/valueindex/writer.go:newKLLBuilder`,
`internal/modules/valueindex/entries.go:chunkEncoder`,
`internal/modules/blockio/shared/constants.go:ValueIndexWriterSpillEntries`


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

> **SUPERSEDED by NOTE-V2-002 (issue #423):** the per-entry `BlockID uint32` described
> below is replaced by a 5-byte `BlockFileRef` (page+length) for direct ranged GETs. The
> v1/v2 dual-decode design described here (`decodeChunkPayloadV1`, dual version accept)
> was never present in the current single-version reader. Retained for history.

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

## NOTE-VI-027 — BlockRef: page-addressed block reference for v2 files (issue #417)

Date: 2026-06-28

### Why

v2 blockpack files align every inner block to a 4 096-byte page boundary (PR 2 of
issue #417). This allows value-index entries to carry a **direct page reference**
instead of a zero-based block index (`BlockID uint32`). A querier that holds a v2
value-index entry can compute the exact S3 byte range immediately:

    Range: bytes = PageNum * 4096, length = LenPages * 4096

No TOC fetch, no block-index lookup — one round trip from index hit to block bytes.

### Wire encoding

`BlockRef` encodes as 5 bytes on the wire:

| Field | Wire size | Type in memory | Max value |
|---|---|---|---|
| `PageNum` | 3 bytes LE | uint32 | 16,777,215 pages × 4096 = 64 GiB |
| `LenPages` | 2 bytes LE | uint16 | 65,535 pages × 4096 = 256 MiB |

This is one byte more than the legacy `BlockID uint32` (4 bytes). The increase is
acceptable: for a typical index file with millions of entries the overhead is ~2-3%
of index size, far less than the ~50% file-size reduction from removing IntrinsicTOC.

### Design decisions

- **PageNum is uint24 on wire, uint32 in memory** — avoids a custom uint24 Go type;
  `AppendBlockRef` / `DecodeBlockRef` handle the 3-byte encoding explicitly.
- **LenPages is the padded length** — the block byte length rounded up to the next
  4096-byte multiple before dividing. Readers ignore trailing padding.
- **`BlockRefFromByteRange` is the canonical constructor** — validates alignment,
  rounds up length, checks overflow bounds. Direct construction of `BlockRef{}` is
  for tests and decoding only.
- **The `PageSize` constant (4096) is defined here** — it is the common coupling
  point between the writer alignment (PR 2) and the value-index encoding (PR 5).
  Both must agree on this value.

### Backward compatibility

`BlockRef` is used only in v2 value-index entries (a new `ValueIndexEntriesVersion`
will gate its use in PR 5). Existing v1 value-index files continue to use `BlockID
uint32` unchanged.

Back-ref: `internal/modules/valueindex/blockref.go`,
`internal/modules/valueindex/blockref_test.go`

## NOTE-VI-027b — BlockRef wire encoding in VINX entries (issue #417 PR5)

Date: 2026-06-28

### VINX entry version

`ValueIndexEntriesVersion` bumped from `0x01` to `0x02` (in `shared/constants.go`).
The old constant is retained as `ValueIndexEntriesVersionV1 = 0x01` for backward compat.

The VINX header version byte is now written **dynamically** based on whether any entry
in the flush batch carries a non-zero `BlockRef`. This means:
- v1-only batches (zero BlockRef on all entries) write version `0x01` — full backward compat.
- v2 batches (any entry with non-zero BlockRef) write version `0x02`.

Readers accept both versions: `decodeVINXSection` validates against either constant and
passes the parsed version to `DecodeChunkRange`/`DecodeAllChunks` via the `ver uint8` param.
`DecodeChunkRange` then routes to `decodeChunkPayload` (v2, 5-byte BlockRef) or
`decodeChunkPayloadV1` (v1, 4-byte BlockID) accordingly.

### Spill file format

`writeRawEntry`/`readRawEntry` prefix each record with a 1-byte version flag
(`spillV2Flag = 1` for v2, 0 for v1), eliminating ambiguous heuristic detection.

### Writer.AddEntryV2

New method on the `Writer` interface for callers that source entries from v2 blockpack files.
Takes a `BlockRef` directly instead of a zero-based `blockID`. Internal `rawEntry` carries
both `blockID uint32` and `blockRef BlockRef`; only one is non-zero per entry.

### ValueIndexEntry.BlockRef (public API)

`blockpack.ValueIndexEntry` now carries `BlockRef valueindex.BlockRef` populated by
`extractBlockColumns` from `meta.PageNum` when non-zero (v2 files).

### ColumnEntry.BlockRef (consumer spill)

`valueindexconsumer.ColumnEntry` now carries `BlockRef valueindex.BlockRef`. The consumer
spill codec encodes it as `block_page[3]+block_len_pages[2]` (5 bytes) after the existing
`block_id[4]` field. Fixed header size updated from 37 to 42 bytes.

Back-refs:
- `internal/modules/valueindex/entries.go:decodeChunkPayload`, `decodeChunkPayloadV1`
- `internal/modules/valueindex/entries.go:encodeChunkPayload`
- `internal/modules/valueindex/entries.go:DecodeChunkRange`, `DecodeAllChunks`
- `internal/modules/valueindex/writer.go:AddEntryV2`, `encodeVINXSectionVer`
- `internal/modules/valueindex/runspill.go:writeRawEntry`, `readRawEntry`
- `internal/modules/blockio/shared/constants.go:ValueIndexEntriesVersion`, `ValueIndexEntriesVersionV1`
- `valueindex_extract.go:ValueIndexEntry.BlockRef`
- `internal/modules/valueindexconsumer/consumer.go:ColumnEntry.BlockRef`
- `internal/modules/valueindexconsumer/service.go:writeEntry`, `readEntry`

## NOTE-VI-027c — v2 BlockRef round-trip test (issue #417 PR5)

The existing `TestBlockRef_*` tests in `blockref_test.go` cover encoding/decoding.
A v2 round-trip integration test should be added (TODO PR6) that:
1. Writes a v2 blockpack (EnableV2Format=true)
2. Calls ExtractValueIndexEntries → yields entries with non-zero BlockRef
3. Calls AddEntryV2 on a valueindex.Writer
4. Flushes and opens reader
5. Asserts QueryResult.BlockRef.ByteOffset() matches the original block's Offset

## NOTE-VI-028 — SourceRef string table for v3/v4 files (issue #432)

Each `Entry` carries a `SourceRef` (the S3 key of the source data blockpack,
~60 bytes). Inlining it per entry (v1/v2 layout) repeats the same path thousands
of times in a compacted file. v3/v4 files instead write a file-level string table
(`stringtable.go`) once, immediately after the VINX header (`str_table_len` at
header[18:22]), and each entry stores a `uint16` index into it. Readers load the
table on open and resolve indexes back to full paths during decode.

Design decisions:
- **uint16 index → at most `MaxStringTableEntries` (65535) distinct SourceRefs
  per file.** `StringTable.Intern` returns `(_, false)` once full; `encodeEntriesV4`
  surfaces this as `ErrStringTableOverflow` instead of silently writing index 0
  (which would collapse all overflow entries onto one wrong SourceRef — silent
  corruption). A plain `Writer.Flush` propagates the error to its caller.
- **Compaction merges string tables implicitly, not via index remapping.** Readers
  decode entries to fully-resolved SourceRef *strings*; compaction re-sorts/dedups
  those and the writer rebuilds a fresh table on output (re-interning). This is
  simpler and bug-resistant compared to remapping old→new indexes per input file —
  there is no separate `MergeStringTables` function (an earlier stub of that idea
  was removed as dead code).
- **Overflow split.** `writeCompacted` enforces the uint16 cap *in addition to* the
  byte-size cap: it walks the (value-sorted) entries accumulating distinct
  SourceRefs and closes the current output file before a new distinct SourceRef
  would push the count past `MaxStringTableEntries`. So a compaction that would
  reference > 65535 source files emits multiple output files, each self-contained
  and within its own uint16 index space. No entry is dropped or corrupted.

Back-refs:
- `internal/modules/valueindex/stringtable.go:StringTable`, `MaxStringTableEntries`, `ErrStringTableOverflow`
- `internal/modules/valueindex/writer.go:encodeEntriesV4`, `assemble` (overflow propagation)
- `internal/modules/valueindex/compaction.go:writeCompacted` (overflow split)

## NOTE-VI-032 — DiscoverIndexFiles: S3 lister → time-filtered, sorted file keys (issue #458)

Date: 2026-06-30

`DiscoverIndexFiles` (discovery.go) is the bridge between the on-disk value-index
layout written by the consumer and the pure in-memory `QueryFiles` path. Given a
`Lister` (satisfied by `blockpack.WritableStorage` and
`valueindexcompactor.IndexStore`), it lists
`<tenant>/<indexPrefix>/<colHash>/<colTypeName>/` once, filters by
`ParseFilenameV2`+`IsInTimeRange`, sorts by `SortFileMetas`
(Level ASC, WallMinSec ASC), and returns the FULL keys (not leaf names) so the
caller can Get/Download directly.

Key decisions:
- Full key preservation: `ParseFilenameV2` only sets the leaf name in
  `FileMeta.Filename`; we overwrite it with the full S3 key before sorting so the
  returned slice is directly fetchable.
- Malformed keys are skipped (not errored): an object sharing the prefix that is
  not a value-index file cannot be queried anyway; failing the whole discovery on
  one stray key would be brittle.
- v1 filenames (no embedded time range) always match — they fall back through
  `ParseFilenameV2`→`ParseFilename` and their zero WallMin/MaxSec is treated by
  `IsInTimeRange` as "matches all".
- No overlap returns (nil, nil) — callers distinguish "no files" from "[]".

This performs a live LIST per call; the in-process listing cache (issue #462)
wraps it as the production optimisation.

Back-refs:
- `internal/modules/valueindex/discovery.go:DiscoverIndexFiles`, `Lister`
- `internal/modules/valueindex/filename.go:ParseFilenameV2`, `IsInTimeRange`, `SortFileMetas`
- `internal/modules/valueindexconsumer/service.go:indexKeyV2` (the layout this mirrors)

## NOTE-VI-034 — IndexFileCache: in-process value-index file-discovery cache (issue #462)

Date: 2026-06-30

`IndexFileCache` (filecache.go) wraps the `Lister` used by `DiscoverIndexFiles`
(NOTE-VI-032) and replaces the live-per-query S3 LIST with a cached, periodically
refreshed listing. At 100+ qps over ~700 column directories a LIST per query is too
expensive; since filenames embed the wall-clock time range, the per-query time
filter is O(1) per file once the directory listing is cached.

Design:
- Caches the *parsed* listing (`[]FileMeta` with full keys, pre-sorted by
  `SortFileMetas`) per `(colHash, colType)`. `FilesForTimeRange` then runs
  `IsInTimeRange` over the cached metas in memory — no S3 round-trip on warm hits.
  It returns byte-identical keys/order to `DiscoverIndexFiles` (covered by a test
  that asserts equality against `DiscoverIndexFiles`).
- Cold miss: synchronous LIST+parse, cache, then filter. A concurrent cold miss for
  the same column prefers the already-populated entry (single source of truth) but
  still marks it accessed.
- Background refresh: `Background(ctx)` ticks every `ttl` (default 30s) and re-lists
  only columns *accessed since the previous sweep*, clearing the `accessed` flag.
  Cold columns nobody queries are skipped, so the sweep cost scales with the active
  working set, not the ~700-directory total. A LIST error during refresh KEEPS the
  stale listing rather than evicting (a transient failure must not blind queries).
- `Invalidate(colHash, colType)` evicts one column; the compactor calls it after
  merging so the next query re-lists and sees the merged layout.
- TTL tolerance: a new L0 file may be invisible for up to one TTL. Acceptable —
  blocks take ~90s to flush anyway.

Integration (the `FilesForTimeRange`-replaces-`DiscoverIndexFiles` wiring in the
index query path) is deferred to issue #461, which introduces the index-driven
query functions and the process-level singleton (`ConfigureValueIndex`) that owns
the cache. The cache itself is self-contained and fully unit-tested here.

Back-refs:
- `internal/modules/valueindex/filecache.go:IndexFileCache`
- `internal/modules/valueindex/discovery.go:DiscoverIndexFiles`, `Lister`
- `internal/modules/valueindex/filename.go:ParseFilenameV2`, `IsInTimeRange`, `SortFileMetas`


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

**Addendum (2026-07-07, issue #490, tasks A-8/#102 + #116):** WRITE support for V1
(pre-BlockRef) entries is retired. `writer.go`'s `assemble()` function now returns
`fmt.Errorf("valueindex: legacy pre-BlockRef entries unsupported — data was not fully migrated
as assumed (see NOTES.md NOTE-VI-014)")` when a `Flush()` batch has at least one entry but none
carries a v2+ `BlockRef`/`SpanID` (`seen && !anyBlockRef`), instead of silently encoding a
V1-shaped VINX section. A batch with ZERO entries still succeeds via the pre-existing
empty-file encoding, now tagged `shared.ValueIndexEntriesVersion` (V2) rather than V1 (task
#116's fix — the V1 tag on new empty files was functionally inert but left the one-format
write directive incompletely closed) — this preserves `compaction.go`'s `writeCompacted()`
"always produce at least one output file, even when there are no entries" contract, unrelated
to legacy V1 data, while ensuring literally no new file of any shape carries a V1 tag.

READ support (decode dispatch) for V1 is intentionally UNCHANGED — `entries.go:220`'s `case
shared.ValueIndexEntriesVersionV1` and `reader.go`'s dispatch still decode V1-shaped files
during the transition period. `shared.ValueIndexEntriesVersionV1` therefore remains defined
and in active use for decode — it is NOT deleted by this change. `ValueIndexEntriesVersionV3`
confirmed to have zero production write call sites (decode dispatch and tests only) — no
action needed on that constant.

Back-refs: `internal/modules/valueindex/writer.go:assemble` (the `else` branch of `if
anyBlockRef`, and the `!seen` empty-batch branch, now a `switch`), `internal/modules/valueindex/
writer_test.go:TestWriter_ReturnsErrorOnLegacyV1EncodeAttempt`,
`internal/modules/valueindex/entries.go:220` (read dispatch, unchanged),
`internal/modules/blockio/compaction/compaction.go:writeCompacted` (the empty-batch contract
this change must not break). Test: `TESTS.md` TEST-VI-13.

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

**Addendum (2026-07-07, issue #490, task A-5/#99):** the "v1 filenames... always match" behavior
described above (item 3 of "Key decisions") is REMOVED — see NOTE-VI-037's addendum for the full
detail. Every filename discovered going forward is v2-formatted; `ParseFilenameV2` now hard-errors
on a v1-shaped filename instead of falling back to `ParseFilename`.

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

---

## NOTE-VI-037 — File discovery cache coherence: write-through + targeted eviction + V2-named compaction output (issue #431)

Date: 2026-06-30

Issue #431's acceptance criteria for value-index file discovery were partially met by
NOTE-VI-032 (`DiscoverIndexFiles`) and NOTE-VI-034 (`IndexFileCache` periodic refresh).
Two population paths from the issue's proposed model were missing, plus a latent pruning
bug in the compactor's output naming:

1. **Compactor wrote v1 filenames (latent pruning bug).** `valueindexcompactor.Service`
   emitted merged output with `FormatFilename` (no embedded time range), while the consumer
   emits L0 with `FormatFilenameV2`. `IsInTimeRange` treats a zero `WallMin/MaxSec` (v1
   names) as *match-all*, so every compacted file (L1+) was returned by discovery for
   *every* query window — silently defeating time-range pruning for all data above level 0.
   Fix: the compaction output callback now `OpenReader`s the merged bytes, reads the footer
   `WallMinTS/WallMaxTS`, and writes `FormatFilenameV2(outputLevel, min, max, id)`. The
   embedded range is asserted to equal the footer range in
   `TestRunOnce_OutputFilenamesEmbedTimeRange`.

2. **Write-through on flush (`IndexFileCache.AddFile`).** Inserts a single freshly written
   file's `FileMeta` into the cached listing for its column so it becomes queryable
   *immediately* rather than after up to one refresh TTL. Replaces an existing key in place
   (no duplicates) and re-`SortFileMetas` so order matches `DiscoverIndexFiles`. On an
   *uncached* column it is a deliberate no-op: seeding a single-file entry would hide every
   other existing file until the next refresh, whereas a cold miss lists the whole directory
   and discovers the file anyway.

3. **Targeted eviction (`IndexFileCache.RemoveFiles`).** Surgically drops the exact set of
   compactor-deleted keys from a cached column without dropping the whole listing. Preferred
   over `Invalidate` for compactor deletes: `Invalidate` forces the next query to re-LIST a
   hot directory (a thundering re-list), while `RemoveFiles` is an in-memory filter that
   keeps the rest of the listing warm. `Invalidate` is retained for the unknown-key-set case.

Cross-process note: the consumer (writer) and querier (reader) are separate processes, so
`AddFile`/`RemoveFiles` are in-process cache primitives the querier-side integration drives
from flush/compaction *events* — they are not called by the consumer or compactor processes
directly. Anchored in cmd/deadcode/main.go as querier-facing public API.

Back-refs:
- `internal/modules/valueindex/filecache.go:IndexFileCache.AddFile`, `RemoveFiles`
- `internal/modules/valueindexcompactor/service.go` (V2-named output)
- `internal/modules/valueindex/filename.go:FormatFilenameV2`, `IsInTimeRange`

**Addendum (2026-07-07, issue #490, task A-5/#99):** `ParseFilenameV2`'s v1-fallback call site
and `FileMeta.IsInTimeRange`'s corresponding "v1 filename → match all" zero-time special case
(both referenced above and in NOTE-VI-032) are removed — `ParseFilenameV2` now hard-errors on a
non-4-part (v1-shaped) filename instead of falling back to `ParseFilename`. Every filename
produced going forward is v2-formatted (`FormatFilenameV2`, embedding an explicit wall-clock
time range — this note's own item 1 already made the compactor's output V2-named), so there is
no longer a legitimate v1-shaped VI name to special-case. The deprecated
`ValueIndexFilenamePattern` constant in `shared/constants.go` is deleted alongside it.
**`ParseFilename` itself (the generic `L<level>-<id>` parser) is NOT deleted** — it remains live
for the unrelated BucketGroup/VCNT filename family (`valueindexcompactor/service.go:337`,
`valuecountscompactor/service.go:220`).

Back-refs (addendum): `internal/modules/valueindex/filename.go:ParseFilenameV2`/
`IsInTimeRange`, `internal/modules/blockio/shared/constants.go` (deleted
`ValueIndexFilenamePattern`), `internal/modules/valueindex/discovery.go` (stale comment
corrected), `internal/modules/valueindexcompactor/service.go` (stale comment corrected). Tests:
`filename_v2_test.go:TestParseFilenameV2_RejectsV1Filename` (replaces
`TestFilenameV1Compatibility`), `discovery_test.go:TestDiscoverIndexFiles_V1FilenameSkipped`
(replaces `TestDiscoverIndexFiles_V1FilenameAlwaysIncluded`).

---

## NOTE-VI-038 — TraceID index: parent/child span structure (issue #428)

Date: 2026-06-30

The TraceID index (stored under the `hash("trace:id")` column directory, `uuid` type
segment) is a specialised value-index variant that encodes the parent/child relationship
between spans so a complete trace can be reconstructed from the index alone — no data-block
scan needed for identity, only for materializing fields.

### Why a separate payload (TraceGroup) instead of the standard Entry posting list

A standard `Entry` answers "which spans have column C = value V at time T" — it is keyed by
*value*. The TraceID index is keyed by *traceID* and must additionally carry `ParentSpanID`
for tree assembly. Rather than overload `Entry` (which would carry a dead ParentSpanID field
in every standard value-index file), the TraceID index uses its own `TraceGroup` /
`SpanEntry` payload. The *outer* file framing (header/blocks/TOC/footer with min/max time) is
shared with the standard value index; only the inner chunk payload differs.

### SpanEntry direct addressing

Each `SpanEntry` carries `SourceRef` (string-table interned, like NOTE-VI-028), `BlockRef`
(page-addressed, like NOTE-VI-027), and `RowIdx` — enough to fetch the exact span row without
scanning. `ParentSpanID` is zero for the root span.

### Compaction (MergeTraceGroups)

- Groups for the same TraceID across input files are merged into one TraceGroup.
- `(TraceID, SpanID)` pairs are deduplicated — first occurrence wins.
- The merged `TimeSec` is the **minimum** across inputs (earliest bucket the trace was seen).
- Retention drop: a `SourceExists` callback (one S3 HEAD per unique SourceRef, cached by the
  compactor) lets MergeTraceGroups skip spans whose data block was deleted by retention. A
  group whose every span is dropped is removed entirely. This mirrors the standard
  value-index stale-ref handling (NOTE-VI-037 / #399) — no explicit delete messages needed.

**Addendum (2026-07-04):** `MergeTraceGroups`'s retention-check parameter was widened from the
bespoke `SourceExists func(sourceRef string) bool` described above to `RefChecker`
(`IsLive(ctx, sourceRef) (bool, error)`) plus a threaded `context.Context`, matching every
other compaction code path in this package, as part of wiring this codec into
`valueindexcompactor` (its first real caller — see NOTE-VI-064). See `SPECS.md` SPEC-VI-6 for
the full contract.

### Querier (AssembleTrace) — partial-trace handling

`AssembleTrace` builds the span tree. Spans with a present parent attach as children;
**orphans** (a non-zero ParentSpanID with no matching SpanEntry in the group — the parent
arrived in a different not-yet-compacted L0 file or was retention-dropped) attach at the root
level and the result is flagged `Partial`. Genuine root spans (zero ParentSpanID) are always
roots and never set Partial. Trees are deterministic (children and roots sorted by SpanID).

Back-refs:
- `internal/modules/valueindex/traceindex.go` (TraceGroup, SpanEntry, EncodeTraceGroups,
  DecodeTraceGroups, MergeTraceGroups, AssembleTrace)
- `internal/modules/blockio/shared/constants.go:ValueIndexTraceVersion`

**Addendum (2026-07-04):** This codec sat unwired (no producer, no consumer, no query-path
caller) from its 2026-06-30 introduction until 2026-07-04. A since-superseded investigation
into `GetTraceByID`'s unconditional full-block-scan OOM incident briefly inferred this file was
"dead code... superseded by the later v2 BucketGroup format (NOTE-VI-043)" based only on a
repo-wide grep for callers. That inference does not hold up under a full read of this file and
its git history and is retracted: no NOTES.md entry anywhere claims or implies supersession,
`traceindex.go`'s `TraceGroup`/`SpanEntry` payload and NOTE-VI-043's `BucketGroup` payload serve
genuinely different query shapes (exact trace-ID tree reconstruction vs. generic attribute
value lookup) and share the same outer file framing / `BlockRef` addressing primitive by
design, and `traceindex.go` was added the day *after* the old in-file DFS trace index was
removed (#438) — internally consistent with being the planned replacement, not an abandoned
experiment. See NOTE-VI-063 for the wiring effort that gives this codec its first real callers.

---

## NOTE-VI-043 — v2 BucketGroup internal file format (issue #427)

Date: 2026-06-30

Defines the v2 on-disk/S3 value-index file format: time-bucketed, value-grouped posting
lists. It replaces the flat per-span `Entry` row (entries.go, one row per span) with a
structure that groups every span sharing a `(time_sec, canonical_value)` key under one
`BucketGroup`, nesting spans beneath the data blocks that contain them. This deduplicates
the `(value, time_sec)` key and the per-block `SourceRef`/`BlockRef` across the many spans
that share them.

This NOTE is the format definition only — the consumer write path (#429), the querier read
path (#424), and migration (#425) are separate issues. The format is self-contained and
exercised end-to-end by `bucketfile_test.go` / `bucketmerge_test.go`.

### File layout (bucketfile.go)

```
[ File Header  ]   magic "VBG2"[4] + version[1]
[ Block 0 ]        one block per (time/value) bucket-group set
...
[ Block N ]
[ String Table ]   SourceRef dedup (reused from NOTE-VI-028 stringtable.go)
[ Block Index  ]   one BlockDirEntry per block: comp_off/len + min/max time + min/max value
[ Footer       ]   fixed 53 bytes: magic + block-index off/len + str-table off/len +
                   file min/max time_sec + version
```

Min/max time live in the **footer** so the querier prunes whole files by time range with a
ranged tail GET of only `BucketFooterSize` bytes — no body fetch (`DecodeBucketFooter` +
`BucketFooter.OverlapsTimeRange`). Block directory offsets are file-absolute (shifted by the
header length at encode time).

### Block layout

```
MinTimeSec[8] MaxTimeSec[8]
min_value_len[2] min_value[N]  max_value_len[2] max_value[N]
bloom_len[4] bloom[N]            -- value bloom (bucketbloom.go)
group_count[4]
  BucketGroup (sorted time_sec ASC, value ASC):
    time_sec[8] value_len[2] value[N] ref_count[2]
      BucketBlockRef:
        source_id[2] page_num[3] len_pages[2] span_count[2]
          SpanRef: trace_id[16] idx_count[2] idx[2]*idx_count
```

Block payloads are snappy-compressed; the directory stores the compressed offset/len.

### Value bloom (bucketbloom.go)

Per-block bloom over canonical values (NOT trace IDs — that filter assumes 16-byte uniform
input). Variable-length values are hashed with two independent FNV-1a passes (one salted +
forced odd) and Kirsch-Mitzenmacher double hashing, k=7, ~10 bits/value, clamped
[16 B, 1 MiB]. A negative `TestValueBloom` definitively skips the block; false positives only
cost an unnecessary block read, never a wrong answer.

### Compaction merge (bucketmerge.go)

`MergeBucketFiles` merges any number of files into one single-block file: groups keyed by
`(time_sec, value)`, refs keyed by `(SourceRef path, page)`, spans keyed by TraceID with
span-index sets unioned/deduplicated. Each input file has its own string table, so SourceIDs
are meaningful only relative to their file — the merge re-interns every SourceRef path into a
fresh dense output table and rewrites the ids; `ErrStringTableOverflow` if > 65535 distinct
sources (compaction must split). `SplitIntoBlocks` repartitions the merged single block into
N blocks of `groupsPerBlock` each, recomputing per-block metadata + bloom. Metadata
(min/max time/value, bloom) is always rebuilt via `BucketBlock.ComputeBlockMeta`.

Back-refs:
- `internal/modules/valueindex/bucketfile.go` (BucketFile/BucketBlock/BucketGroup/
  BucketBlockRef/SpanRef, EncodeBucketFile, DecodeBucketFile)
- `internal/modules/valueindex/bucketbloom.go` (ValueBloomSize, AddValueToBloom, TestValueBloom)
- `internal/modules/valueindex/bucketmerge.go` (MergeBucketFiles, SplitIntoBlocks)
- `internal/modules/valueindex/bucketquery.go` (DecodeBucketFooter, LookupValue, MayContainValue)

## NOTE-VI-044 — v2 querier direct block fetch (issue #424)

A value-index hit (`LookupResult`) carries `(SourceRef, BlockRef, TraceID)`. In a v2
blockpack file every inner block is self-contained (all columns, identity included) and
page-aligned, so `BlockRef` (NOTE-VI-027) names the exact byte range of one block. That
means a hit resolves with a single ranged GET — **no TOC fetch** is required, unlike the v1
path (`search_trace_vi.go`) which resolves `BlockID → offset` through the reader's TOC.

`blockfetch.go` implements the I/O-planning half of that lookup path:

- `GroupHitsBySource(results)` — partitions `[]LookupResult` by `SourceRef` (one S3 object
  per file). Sorted by SourceRef; input order preserved within a group; empty-SourceRef
  hits dropped (not fetchable).
- `CoalesceBlockRefs(refs, cfg)` — merges adjacent/overlapping `BlockRef` byte ranges within
  one file into as few `BlockRefRead`s (ranged GETs) as possible. Same gap/waste/max-read
  policy as `reader.CoalesceBlocks` (uses `shared.CoalesceConfig`, e.g.
  `shared.AggressiveCoalesceConfig`), but operates on page-addressed refs rather than
  TOC-resolved offsets. Input need not be sorted; exact-duplicate refs collapse to one.
- `FetchBlocks(fetcher, sourceRef, hits, cfg)` — drives a caller-supplied `BlockFetcher`
  (ranged-GET half of the storage backend) over the coalesced reads and slices each response
  back into per-block byte slices (`FetchedBlock{Ref, Data}`), deduplicated by BlockRef and
  copied into independent allocations so the merged read buffer can be released. A short read
  is a hard error (truncated file / out-of-range ref would silently drop spans).

Block decode + in-block traceID lookup stays with the caller (executor/reader): a v2 block
decodes standalone from its raw bytes (`Reader.ParseBlockFromBytes` uses only `meta.Offset`
as a cache key and the block's own span count), then the caller binary-searches the sorted
`trace:id` column to assemble matching spans.

Back-refs:
- `internal/modules/valueindex/blockfetch.go` (GroupHitsBySource, CoalesceBlockRefs,
  BlockFetcher, FetchBlocks, BlockRefRead, SourceHits, FetchedBlock)

---

## NOTE-VI-045 — v2 BucketGroup consumer write path + querier read path (issue #429)

Date: 2026-06-30

Wires the v2 BucketGroup file format (NOTE-VI-043) end to end: the value-index write path now
emits BucketGroup files, and the querier reads them.

### Writer (`Writer.FlushBucket`)

`FlushBucket(ctx, groupsPerBlock)` reuses the existing sort/dedup/spill-merge machinery
(`sortRawSlice`, `deduplicateEntries`, `mergeRuns`) but feeds the sorted `rawEntry` stream into
`assembleBucket` instead of the flat VINX encoder. `assembleBucket` groups entries by
`(TimeSec, CanonicalValue)`, nests one `BucketBlockRef` per `(SourceID, BlockRef)` and one
`SpanRef` per `TraceID` (unioning span row indexes), then splits the single logical block into
blocks of at most `groupsPerBlock` (default `shared.ValueIndexBucketGroupsPerBlock` = 4096) via
`SplitIntoBlocks` and serializes with `EncodeBucketFile`. An empty writer returns `nil` (not an
empty file) so callers skip the S3 PUT.

**Dedup key now includes `rowIdx`+`spanID`** (`deduplicateEntries`, `runspill.sameEntry`) and
`sortRawSlice`/`compareRawEntry` tiebreak on `rowIdx`. Before this, two distinct spans of the
same trace in the same block at the same time collapsed to one entry — fine for the flat format
(one row per span-value) but wrong for BucketGroups, which must preserve every span row index.

### Consumer / in-process L0 writer

`valueindexconsumer.Service.flushColumn` and `blockpack.WriteValueIndexL0` (`valueindex_l0write.go`)
call `FlushBucket` and read the file-level wall-time range from `DecodeBucketFooter` (the footer
carries min/max `time_sec` directly — no `OpenReader` needed).

### Compactor (`CompactBucketFiles`)

`valueindex.CompactBucketFiles(ctx, files, cfg, groupsPerBlock, output)` decodes each input file,
drops dead-source `BucketBlockRef`s (per `cfg.Checker`, `filterDeadRefs`), merges via
`MergeBucketFiles`, re-splits, and encodes. `CompactStats` counts retained vs dropped
BucketBlockRefs. `valueindexcompactor.Service.mergeLevel` uses it in place of the flat
`OpenReader`/`CompactFiles` path.

### Querier read path

`valueindex.QueryBucketFiles(pred, timeRange, files...)` is the BucketGroup analog of `QueryFiles`:
it decodes each file, evaluates the predicate against each group's canonical value, and flattens
every matching `SpanRef` span index into one `LookupResult{SourceRef, BlockRef, TraceID, RowIdx}`.
`vibuilder` now calls it. Because BucketGroups store span **row indexes** (not the 8-byte SpanID),
span identity in the executor changed from `(TraceID, SpanID)` to `(SourceRef, BlockPage, RowIdx)`:
`VILookupResult` gained `BlockPage`/`BlockLen`; `viSpanKey` packs `TraceID[16]+BlockPage[4]+RowIdx[2]`
(22 bytes) and `viSpanCmp` compares `SourceRef` first so two files reusing a page number cannot
collide. The trace-search path (`search_trace_vi.go`) resolves each `BlockPage` back to a block
index via the new `reader.Reader.BlockIndexForPage`; a page naming no block ⇒ index/data skew ⇒
fall back to a scan (replaces the old out-of-range `BlockID` check).

The flat VINX writer (`Writer.Flush`/`assemble`), reader (`OpenReader`/`QueryFiles`), and
compaction (`CompactFiles`) remain in the tree (still unit-tested) but have no production callers;
their removal is left to a follow-up cleanup.

Back-refs:
- `internal/modules/valueindex/writer.go` (FlushBucket, assembleBucket)
- `internal/modules/valueindex/bucketmerge.go` (CompactBucketFiles, filterDeadRefs)
- `internal/modules/valueindex/bucketquery.go` (QueryBucketFiles)
- `internal/modules/blockio/reader/reader.go` (BlockIndexForPage)
- `internal/modules/executor/metrics_trace.go` (VILookupResult.BlockPage, viSpanKey/viSpanCmp)
- `internal/modules/executor/search_trace_vi.go` (BlockPage → block-index resolution)

## NOTE-VI-046 — Streaming compaction for the v2 BucketGroup merge path (issue: OOM fix)

Date: 2026-07-02

The v2 BucketGroup merge path's OOM risk was `MergeBucketFiles`'s map-of-maps
(`groups map[string]*groupAcc` with nested `refAcc`/`spanAcc` accumulators), which
materializes the *entire* merged output in memory before `SplitIntoBlocks` even starts
cutting output blocks — **not** "K input files open at once" (K is already small/bounded per
compaction pass, and a k-way merge inherently needs all K cursors live regardless of
granularity; K-many decoded files was never the actual cost driver).

### Fix: heap-based k-way merge at file granularity

`StreamCompactBucketFiles` + `BucketFileIterator` (`stream_compaction.go`) perform a
heap-based k-way merge over per-file iterators, replacing the map-of-maps with: (a) K decoded
input files (already resident regardless of merge strategy), (b) a tiny transient per-key
merge buffer (sized to however many of the K files currently share the smallest key, not the
whole key space), (c) one in-progress output block (at most `groupsPerBlock` groups). See
SPEC-VI-1 (cross-block ordering, the property that lets `BucketFileIterator` trust a plain
sequential walk) and SPEC-VI-2 (`StreamCompactBucketFiles`'s merge-semantics-equivalence and
peak-memory-bound contract) in `SPECS.md`.

`valueindexcompactor.Service.mergeLevel` now decodes/filters one input file at a time
immediately after each `store.Get`, instead of downloading all input bytes upfront into a
`fileBytes [][]byte` slice — see `valueindexcompactor/NOTES.md` NOTE-VI-046 for the
`mergeLevel`-side half of this change.

### Granularity decision: file, not block

File-granularity (decode a whole input file, iterate its blocks/groups in memory) was chosen
over block-granularity (ranged per-block fetch) because `valueindexcompactor.IndexStore` has
no ranged-read method today — it is an exported public interface already hand-patched into
tempo-mrd's local tree pending a real vendor bump, so adding a ranged-read method now would be
a breaking cross-repo interface change not justified by this fix. It also would not remove
much: `DecodeBucketFile` still needs the file's tail (footer + block index + string table) via
at least one more ranged read per file regardless of block-level fetching.

**Follow-up (not built):** block-granularity (ranged per-block fetch via `DecodeBucketFooter`'s
tail-read plus the block index) would further bound peak memory to `max(single block)` instead
of `max(single file)`. Worth pursuing if a single compacted file ever grows large enough that
one fully-decoded file becomes the bottleneck. Blocked on an `IndexStore` ranged-read API
addition.

Back-refs: `internal/modules/valueindex/stream_compaction.go`,
`internal/modules/valueindexcompactor/service.go:mergeLevel`.

## NOTE-VI-047 — Cross-block global ordering is an emergent guarantee, not a designed one

Date: 2026-07-02

Blocks within a single `BucketFile` are globally ordered by `(TimeSec ASC, CanonicalValue
ASC)` end-to-end — not just internally sorted within each block. `SplitIntoBlocks`
(`bucketmerge.go:145-176`) is the only function that ever partitions groups into multiple
blocks: it flattens every existing block's `Groups` into one slice, sorts that slice
**globally** by `(TimeSec, CanonicalValue)`, then cuts it into fixed-size blocks sequentially
(`all[start:end]`, no reshuffling after the cut). Because the cut points walk monotonically
through one already-sorted slice, block N's last group is always `<=` block N+1's first group
by construction. Every multi-block `BucketFile` in this codebase goes through
`SplitIntoBlocks` — both the write path (`writerImpl.assembleBucket`) and the compaction path
(`CompactBucketFiles` → `MergeBucketFiles` → `SplitIntoBlocks`) — so the guarantee holds
universally for self-produced files, not just for compactor output.

This property was discovered by reading `SplitIntoBlocks`'s implementation while
investigating whether `BucketFileIterator` (the streaming compaction fix, NOTE-VI-046) could
trust a plain sequential block-by-block walk within one file, or would need to open all of a
file's blocks up front to find the true next-smallest group across blocks. It was not an
explicitly documented invariant before this investigation — it is a side effect of how
`SplitIntoBlocks` happens to be implemented, not a property anyone deliberately designed the
format around. The formal statement of the invariant lives in `SPECS.md` SPEC-VI-1; this note
records why it was worth looking for and what would have broken had it not held (a per-file
iterator would have needed to buffer or re-sort across blocks, defeating the point of
streaming at file granularity).

**Do not confuse this with the *trace* blockpack format's block ordering, which has no such
guarantee.** `blockio/writer.sortPending` sorts spans by `(service.name, MinHashSig, TraceID)`
before cutting trace blocks — that key has **no timestamp component at all** (see
`blockio/NOTES.md` §2/§32), so trace blocks carry no inherent time or value ordering relative
to each other; `BlockMeta.MinStart/MaxStart` ranges across trace blocks can and do overlap
arbitrarily, which is exactly why the trace format needs a separate TS index
(`blockio/writer/ts_index.go`) to support time-range pruning at all. The BucketGroup VI
format's `SplitIntoBlocks` sort key directly includes both dimensions the format is queried
by (`time_sec`, `value`), so no separate cross-block index is needed for a streaming iterator
to trust block order — this is a structural difference between the two formats, not a
coincidence, and a future change to either format's block-cutting sort key must not assume
the other format's ordering behavior.

Back-refs: `internal/modules/valueindex/bucketmerge.go:SplitIntoBlocks`,
`internal/modules/valueindex/stream_compaction.go:BucketFileIterator`.

---

## NOTE-VI-051 — TimeSec minute-aligned truncation (cardinality reduction)

Date: 2026-07-02

### What changed

`buildSpanStartSecByRef` (`valueindex_extract.go:159-186`) previously floored `span:start`
(nanoseconds) to the whole second (`v / 1_000_000_000`). It now floors to the whole minute:

```go
const secondsPerMinute = 60
m[key] = (v / 1_000_000_000) / secondsPerMinute * secondsPerMinute
```

This is the value that becomes `ValueIndexEntry.TimeSec` and, from there, the `TimeSec` half
of the `(TimeSec, CanonicalValue)` merge/sort key that `stream_compaction.go`,
`bucketfile.go`/`bucketmerge.go`, and `writer.go` all key on (SPEC-VI-1/SPEC-VI-3). See
SPEC-VI-4 for the formal invariant statement.

### Cardinality rationale

Per-second `TimeSec` produces up to 60x more distinct `(TimeSec, CanonicalValue)` groups per
minute than necessary for a busy column, driving up postings/group count and the CPU/memory
cost of decoding and scanning them at query time. Minute-alignment collapses all spans sharing
a value within the same wall-clock minute into one `BucketGroup`, directly reducing group
density for high-cardinality-by-time columns without changing the wire format (TimeSec remains
an opaque uint64 in both the flat VINX and v2 BucketGroup formats — see NOTE-VI-043/NOTE-VI-045
— so old second-granularity data and new minute-granularity data coexist without a migration;
they simply won't retroactively coalesce with each other across the transition, which ages out
naturally via retention).

### Mandatory paired query-side widening requirement (cross-repo)

This truncation is **not safe to ship alone**. tempo-mrd's index-driven TraceQL search path
(`tempodb/encoding/vblockpack/value_index_query.go:nanoWindowToSec`) converts a query's
`[startNano, endNano]` window into `[minSec, maxSec]`, and `minSec` is compared against
`TimeSec` by `bucketquery.go:LookupValue`'s exact-inclusion filter
(`TimeSec < minTS || TimeSec > maxTS`) with no downstream fallback for a wrongly-excluded
entry. Before this change, both sides floored to the same (second) granularity, so flooring's
monotonicity made the comparison always safe (over-inclusion only, never under-inclusion).
After this change, `nanoWindowToSec`'s `minSec` MUST also floor to the same 60-second alignment
(`minSec = (startNano/1_000_000_000) / 60 * 60`) or a genuinely in-range span whose
minute-floored `TimeSec` falls before a non-aligned `minSec` is silently dropped from search
results — a real false-negative TraceQL search bug with no error surfaced. `maxSec` needs no
corresponding change (see SPEC-VI-4's algebraic justification). This is a hard, coordinated,
cross-repo correctness dependency: the blockpack truncation and the tempo-mrd widening must
ship in the same rollout, or with the tempo-mrd widening deployed first (a wider query bound
against old, non-minute-floored `TimeSec` data is always safe; the unsafe direction is
deploying the blockpack truncation ahead of the tempo-mrd widening).

### Permanent 1-minute resolution cap on the dormant CountOverTime/RateOverTime fast path

`internal/modules/valueindex/metrics.go`'s `CountOverTime`/`RateOverTime`/`TimeBuckets`
compute TraceQL metrics directly from VI `TimeSec` values against an arbitrary `StepNano`
(exercised at 5-second buckets by `valueindex_e2e_test.go`'s
`TestValueIndexE2E_CountOverTime`, which constructs `TimeSec` values directly via
`AddEntryV4` and does not go through `buildSpanStartSecByRef` — so that test is unaffected by
this change; see `TESTS.md`). This fast path has no live production caller today (not
re-exported by the root `api.go`, and no caller found in tempo-mrd) — a dormant, tested, but
unwired feature. Minute-quantizing `TimeSec` permanently caps this feature's usable resolution
at 1 minute, whatever step size a future caller might request, once it is ever wired up. Not a
regression today (nothing currently depends on finer resolution), but a real design constraint
worth knowing before someone wires this up expecting sub-minute granularity.

Back-refs: `valueindex_extract.go:buildSpanStartSecByRef`, `internal/modules/valueindex/metrics.go`
(`CountOverTime`, `RateOverTime`, `TimeBuckets`), and (external, cross-repo) tempo-mrd's
`tempodb/encoding/vblockpack/value_index_query.go:nanoWindowToSec`. See `SPECS.md` SPEC-VI-4
and `TESTS.md` TEST-VI-7.

## NOTE-VI-053 — Disk-backed streaming merge: completing NOTE-VI-046's deferred block-granularity follow-up

Date: 2026-07-03

### Framing: completing NOTE-VI-046, not contradicting it

NOTE-VI-046 (2026-07-02) correctly deferred block-granularity compaction because it required a
**remote** `IndexStore` ranged-read API addition — an exported, cross-repo interface change
(`IndexStore` is hand-patched into tempo-mrd's local tree pending a real vendor bump) not
justified at the time. This entry does **not** reopen or reverse that decision. It achieves
the same block-granularity goal — bounding peak decoded memory per input file to
`max(single block)` instead of `max(single file)` — via a fundamentally different mechanism
NOTE-VI-046's author did not consider: ranging against a **local** temp file, populated by one
ordinary whole-object `s.store.Get` (the exact same single call `IndexStore` already makes
today), rather than against the remote object store directly. Local files support trivial
random-access reads (`os.File.ReadAt`) that a remote object store's `Get`/`Peek`-only
`IndexStore` interface does not — that is precisely why NOTE-VI-046 rejected *remote* ranged
reads and precisely why this entry's *local* ranging sidesteps the exact obstacle NOTE-VI-046
identified. **Zero `IndexStore` interface changes** are made by this redesign.

### The new GroupIterator interface

`StreamCompactBucketFiles`' k-way merge (`bucketIteratorHeap`) was confirmed (by reading its
`Less`/`Push`/`Pop` implementation directly) to call only `Peek()`/`Advance()` on an iterator
— nothing about a whole-file in-memory representation is structurally required. `GroupIterator`
makes this explicit as an exported interface:

```go
type GroupIterator interface {
    Peek() (*BucketGroup, bool)
    Advance(ctx context.Context)
    StringTable() *StringTable
    Err() error
    Close() error
}
```

Both `BucketFileIterator` (existing, whole-file in-memory decode — compile-time-asserted via
`var _ GroupIterator = (*BucketFileIterator)(nil)`) and the new `diskBucketFileIterator`
(lazy, block-at-a-time decode from a local temp file) satisfy it. See `SPECS.md` SPEC-VI-5 for
the formal Peek/Advance/Err contract this interface establishes.

**Why `Advance` gained a `ctx context.Context` parameter** (a fallibility gap the original
Peek/Advance shape didn't need to address, since `BucketFileIterator.Advance` never fails): a
disk-backed `Advance()` can perform local disk I/O (decoding the next block) and may invoke a
retention `RefChecker.IsLive` call that itself performs network I/O (e.g.
`cachingRefChecker`/`SourceExister`, an S3 HEAD). Two options were weighed: storing `ctx` on
the iterator struct at construction time (rejected — a stored `ctx` on a long-lived object is
the exact anti-pattern the "don't store contexts in structs" guideline exists to prevent) vs.
threading `ctx` through `Advance` itself (chosen — mechanical, and every call site inside
`StreamCompactBucketFiles`'s merge loop already has `ctx` in scope). `Peek()` deliberately
stays ctx-less and error-free (SPEC-VI-5): the "current" block's groups are always already
resident by the time `Peek()` is called, since block 0 is decoded synchronously inside the
constructor and block N+1 is decoded synchronously inside the `Advance()` call that crosses
into it — never inside `Peek()` itself.

### diskBucketFileIterator: eager metadata, lazy blocks

`NewDiskBucketFileIterator(ctx, path, checker) (GroupIterator, error)` opens `path` and
eagerly decodes header magic, footer, string table, and block directory — all bounded/cheap
regardless of file size (footer is fixed-size; the string table scales with distinct interned
source paths; the block directory scales with block count, never group/span count) — then
eagerly decodes forward from block 0 until it finds a block with at least one
(optionally retention-filtered) live group, so `Peek()` never needs to perform I/O (per
SPEC-VI-5). Each subsequent `Advance(ctx)` that crosses a block boundary decodes exactly the
next block (`ReadAt` the compressed bytes, `snappy.Decode`, `decodeBucketBlock`,
`filterDeadRefsBlock`) and discards the previous block's groups (ordinary Go GC — no explicit
nil-out needed). Peak decoded memory per open disk-backed iterator is therefore one block,
regardless of the file's total block count or size.

**Per-block retention filtering:** `filterDeadRefsBlock` is a new function
(`disk_iterator.go`) factored out of `bucketmerge.go`'s existing whole-file `filterDeadRefs`,
which is now a thin loop calling it once per block with one `live map[uint16]bool` cache
shared across the whole file — a behavior-preserving refactor (not a behavior change):
`filterDeadRefs`'s existing tests pass unmodified, and
`TestFilterDeadRefsBlock_MatchesWholeFileFiltering` proves per-block evaluation with a
persistent cache produces byte-identical retain/drop decisions and identical
`checker.IsLive` call counts (at most once per distinct `SourceID` per file, matching the
prior whole-file cache's behavior) as the original whole-file evaluation, including when a
`SourceID` repeats across multiple blocks.

**Corruption-handling split (a deliberate behavior change from today, documented prominently
here since it diverges from the prior silent-skip-on-any-decode-failure behavior):**
- **Header magic mismatch only** → treated as "legacy pre-v2 file, skip this file, do not
  abort the merge" — `NewDiskBucketFileIterator` returns `(nil, nil)`, an explicit untyped nil
  assigned directly to the `GroupIterator` return slot (never a typed-nil `*diskBucketFileIterator`
  wrapped in the interface — the nil-interface-trap this repo's own past-feedback flags as a
  recurring bug class). `TestNewDiskBucketFileIterator_LegacyFileReturnsTrueNilInterface`
  asserts the returned `GroupIterator == nil` compares true, which only holds for a genuine
  nil interface.
- **Any other decode failure** (footer/string-table/block-index corruption, discovered eagerly
  at construction; or an individual block's snappy/decode corruption, discovered lazily at
  that block's `Advance`) is a real error — returned by the constructor or surfaced via
  `Err()` — that aborts the whole merge, rather than being silently skipped as a legacy file
  would be. Before this change, `DecodeFilteredBucketFile`'s eager whole-file decode could not
  distinguish "which specific check failed," so every non-legacy decode failure was treated
  identically to a header-magic mismatch (silent skip). See
  `valueindexcompactor/NOTES.md` NOTE-VI-052 for the caller-side (`mergeLevel`) framing of why
  this is an intentional, safer change, not a regression.

### Ownership/cleanup contract

Mirrors `runspill.go`'s `runFile.remove()` pattern (single owner, close+remove together):

| Outcome | Who closes the fd | Who removes the local temp file (`path`) |
|---|---|---|
| Header magic mismatch (legacy skip) | `NewDiskBucketFileIterator` | caller — ownership never transferred |
| Any other constructor error | `NewDiskBucketFileIterator` | caller — ownership never transferred |
| Success (live iterator returned) | the iterator's own `Close()` | the iterator's own `Close()` (idempotent) |

The caller (`valueindexcompactor.mergeLevel`) retains `path` ownership in the first two rows
because no iterator was ever successfully constructed to hand it to; on success, ownership of
both the fd and `path` fully transfers to the returned iterator, and the caller only needs
`defer it.Close()`.

### StatsProvider: an optional, separate interface for retention-filter bookkeeping

`diskBucketFileIterator` additionally implements `StatsProvider` (`Stats() CompactStats`) —
deliberately **not** part of `GroupIterator` itself, since `Stats()` is never called by the
merge algorithm (`StreamCompactBucketFiles`/`mergeGroupsAtKey`/`bucketIteratorHeap`), only by
a caller's own metrics bookkeeping. Before this redesign, `valueindexcompactor.mergeLevel`
read `CompactStats` directly from `DecodeFilteredBucketFile`'s return value (computed
up front, whole-file); after this redesign, retention-filter stats accumulate incrementally
as `Advance` decodes each block, so `mergeLevel` type-asserts a fully-drained iterator to
`StatsProvider` to read the final per-file total once merging completes. Keeping this a
separate, optional interface avoids widening `GroupIterator`'s contract (and therefore every
implementation's required method set, including any future `GroupIterator` that has no
retention-filter stats to report) for a concern the merge algorithm has no stake in.

### The vi-merge-*.tmp naming convention: deliberately distinct from runspill.go

Both the input side (`valueindexcompactor`, `vi-merge-in-*.tmp`, staged before constructing a
`diskBucketFileIterator`) and the output side (this package, `stream_compaction.go`'s
`StreamCompactBucketFiles`, `vi-merge-out-*.tmp`) share the common `vi-merge-` prefix so one
glob (`SweepOrphanedMergeTempFiles`, below) catches both. This prefix is deliberately distinct
from `runspill.go`'s existing `vi-run-*.tmp` spill-file convention (NOTE-VI-026), so the new
startup sweep never touches `runspill.go`'s own in-flight spill files, and vice versa.
`runspill.go` itself is not modified by this redesign; its own lack of an equivalent startup
sweep is a latent, low-probability, out-of-scope gap (unchanged by this work).

`SweepOrphanedMergeTempFiles() (removed int, err error)` globs
`filepath.Join(os.TempDir(), "vi-merge-*.tmp")` and removes every match, best-effort (a
missing/unreadable directory yields `(0, nil)`, not an error; individual removal failures are
collected — the first is returned — but every match is still attempted). Intended to be
called once, at `valueindexcompactor.NewService` construction, to clean up files left behind
by a prior process that crashed mid-merge (local disk, once a k8s `emptyDir` is added per
NOTE-VI-052's flagged deployment prerequisite, persists across container restarts of the same
pod but not across pod reschedules). A sweep failure is logged/metriced but never fails
service construction.

### filterDeadRefs refactor: behavior-preserving, no invariant change

`bucketmerge.go`'s `filterDeadRefs` (whole-file) is now implemented as a thin loop calling the
new `filterDeadRefsBlock` once per block, aggregating `CompactStats` — this is a **pure
composition refactor**, not a behavior change: `filterDeadRefs`'s existing tests pass
unmodified, and no existing SPECS.md/TESTS.md wording describing whole-file retention
filtering needed to change as a result (the observable behavior, including the per-file
`live map[uint16]bool` cache-reuse semantics, is identical before and after).

Back-refs: `internal/modules/valueindex/disk_iterator.go` (`diskBucketFileIterator`,
`NewDiskBucketFileIterator`, `filterDeadRefsBlock`, `StatsProvider`),
`internal/modules/valueindex/stream_compaction.go` (`GroupIterator`),
`internal/modules/valueindex/temp_cleanup.go` (`SweepOrphanedMergeTempFiles`),
`internal/modules/valueindex/bucketmerge.go` (`filterDeadRefs` refactor), and (cross-package)
`internal/modules/valueindexcompactor/NOTES.md` NOTE-VI-052 (the `mergeLevel`/input-staging
half of this same redesign).

**Addendum (2026-07-07, issue #490, task A-4/#98):** the non-streaming `CompactBucketFiles`
function this note contrasts `StreamCompactBucketFiles` against (see NOTE-VI-046's original
framing) is now deleted outright — zero remaining production callers confirmed
(`valueindexcompactor/service.go` only ever called `StreamCompactBucketFiles`). The streaming
path documented in this note is the sole compaction implementation for BucketGroup files.
Dedicated tests (`bucketcompact_test.go`, `TestCompactBucketFilesErrorsOnCorruptV2`,
`TestStreamCompactBucketFiles_MatchesNonOverlapping` and its `groupTuple`/`groupTuples`
comparison helpers) removed alongside it; `BenchmarkCompactBucketFiles_OldVsStreaming` is now
`BenchmarkStreamCompactBucketFiles`, a standalone baseline rather than an old-vs-new comparison
(see `BENCHMARKS.md` BENCH-VI-1).

## NOTE-VI-063 — `traceindex.go` gets its first real callers: extraction → consumer flush wiring (issue #428 wiring, Stage 1-2)

Date: 2026-07-04

The `TraceGroup`/`SpanEntry` codec documented in NOTE-VI-038 (added 2026-06-30, previously
unwired — see that entry's 2026-07-04 Addendum) now has real producers: root-package
`extractBlockColumns` surfaces `ParentSpanID` per row (additive field, see
`valueindexconsumer/NOTES.md` NOTE-VI-060), and `valueindexconsumer` buffers every span row
into a per-tenant `TraceGroup` and flushes it via `EncodeTraceGroups` under this package's
existing, unmodified `hash("trace:id")` colDir/discovery layout (`valueindexconsumer/NOTES.md`
NOTE-VI-061/062). No change to `traceindex.go`'s own encode/decode/merge/assemble functions was
needed for this stage — the wiring is entirely upstream (extraction) and downstream
(consumer), consuming the existing codec as-is.

**Caveat — REAL and LIVE, corrected 2026-07-05 (originally, incorrectly, assessed as
informational/not-reachable):** the root-package `WriteValueIndexL0` (`valueindex_l0write.go`,
NOTE-VI-042) is a separate synchronous write path that indexes every column — including
`trace:id` — via the *generic* `BucketGroup` format (NOTE-VI-043) into the same
`hash("trace:id")` colDir this wiring now uses for `TraceGroup`-format files. **This was
originally reported here as "not currently reachable... zero real callers," based on a grep
that only checked `/home/mdurham/source/tempo-mrd`.** `WriteValueIndexL0` in fact has real,
live production callers in `/home/mdurham/source/blockpack_collection/tempo`'s
`tempodb/encoding/vblockpack/compactor.go` and `create.go`, gated behind
`value_index_enabled`. This is a real, currently-unfixed gap, not a future risk — it must
exclude `trace:id` (mirroring `valueindexconsumer`'s own sentinel-column design,
`valueindexconsumer/SPECS.md` SPEC-VI-4) or it writes an incompatible file format into this
colDir for any tenant with that flag on, today. Full detail in `valueindexconsumer/SPECS.md`
SPEC-VI-2's corrected "Caveat" paragraph and `valueindexconsumer/NOTES.md` NOTE-VI-042's third
addendum. Tracked as task #93 (code fix, outside this spec-oracle's own remit).

**Still to come (separate tasks):** the compactor's format-dispatch branch and
`MergeTraceGroups`'s `RefChecker` signature widening (task #86), and `GetTraceByID`'s read-side
wiring (task #87) — this entry covers only the write-side (extraction + consumer flush) half
of the effort.

Back-refs: root `valueindex_extract.go:extractBlockColumns`,
`internal/modules/valueindexconsumer/traceflush.go`,
`internal/modules/valueindexconsumer/service.go:ingest`.

## NOTE-VI-064 — `MergeTraceGroups` gets its first real caller: compactor format-dispatch (issue #428 wiring, Stage 3)

Date: 2026-07-04

`valueindexcompactor`'s `mergeTraceLevel` (`internal/modules/valueindexcompactor/
traceindex_dispatch.go`) is `MergeTraceGroups`'s first production caller since it was added by
NOTE-VI-038. Because it had zero callers, widening its `SourceExists func(string) bool`
parameter to the standard `RefChecker` interface (`SPECS.md` SPEC-VI-6) was free — no existing
call sites needed migration, only the function's own 4 existing tests
(`TestMergeTraceGroups_MergeSameTrace/DedupSpan/StaleSourceDropped/AllStaleDropsGroup`, updated
mechanically to pass a `context.Background()` and a `RefChecker`-implementing test fake in
place of the prior `func(string) bool` literals — no change to the tests' own assertions).

The dispatch decision (which colDir routes to `mergeTraceLevel` vs. the standard `mergeLevel`)
lives in `valueindexcompactor`, not here — see `valueindexcompactor/NOTES.md` for the
Finding-2 rationale (why the dispatch must happen before the `vbg2Magic` purge loop, not only
inside the merge function itself).

Back-refs: `internal/modules/valueindex/traceindex.go:MergeTraceGroups`,
`internal/modules/valueindexcompactor/traceindex_dispatch.go:mergeTraceLevel`,
`:isTraceIndexColDir`. See `SPECS.md` SPEC-VI-6.

## NOTE-VI-046 — corrupt v2 file must ERROR, not silently skip (issue #469 audit, step 1)

Date: 2026-07-05

The trace-by-id review (2026-07-05, `reader.go:findTraceGroupInCandidates`) identified a
general silent-partial-result bug class: a value-index consumer that treats its result as
authoritative coverage but silently skips a file it fails to decode will under-count without
error. Issue #469 step 1 audited the search/metrics path for the same gap.

**Finding.** `DecodeBucketFile` returned an undifferentiated error for two very different cases:
(a) bad header/footer magic — the input is not a v2 BucketGroup file at all (a legacy pre-v2 or
stray object sharing the prefix; holds no v2 postings, so skipping it cannot drop any live
posting), and (b) a decode failure *past* the magic — a genuinely corrupt v2 file (bad footer
offsets, truncated block index, snappy failure, block-body overrun; skipping it silently drops
real postings). Three consumers conflated the two by skipping on *any* decode error:
`QueryBucketFiles` (the live search/metrics query path — a covered-but-empty column is treated
as authoritative coverage per NOTE-VI-033, so a silent skip produced an authoritative
under-count with no fallback), `CompactBucketFiles`, and `DecodeFilteredBucketFile`. Only the
disk-backed `NewDiskBucketFileIterator` (the live compactor path via `StreamCompactBucketFiles`)
already had the correct discipline: skip only on the header-magic check, error on everything
else.

**Fix.** `DecodeBucketFile` now wraps `ErrNotBucketFile` on bad-magic *and* too-short inputs
(neither can be a v2 file), and returns every other failure as a bare decode error. Callers
iterating a discovered file set skip only `errors.Is(err, ErrNotBucketFile)` and surface any
other decode error so the caller falls back to a full scan (query) or aborts the merge
(compaction) rather than silently under-counting — matching `NewDiskBucketFileIterator` and
the trace-by-id "any doubt ⇒ full scan" contract.

**Secondary fix (found by the corruption test).** `DecodeBucketFile`'s footer bounds check
`off+len > len(data)` was uint64-overflow-unsafe: a corrupt offset of ~2^64 wrapped past the
guard and *panicked* the decode goroutine on the slice. Now each offset/length is checked
against `len(data)` individually before the (now overflow-free) sum, so a corrupt file yields
a clean error instead of a panic in the querier.

This audit did NOT remove the search/metrics full-scan fallback (issue #469 step 3) — that
remains gated on completing the correctness confirmation (step 2). It closes the analogous
silent-partial-result gap the fallback would otherwise mask.

Back-refs: `internal/modules/valueindex/bucketfile.go:DecodeBucketFile` (ErrNotBucketFile,
overflow-safe bounds), `:bucketquery.go:QueryBucketFiles`, `:bucketmerge.go:CompactBucketFiles`,
`:stream_compaction.go:DecodeFilteredBucketFile`. Regression tests:
`bucketquery_corruption_test.go`, `bucketfile_test.go:TestDecodeBucketFileBadMagic/CorruptionNotBadMagic`.

## NOTE-VI-070 — `WriteValueIndexL0` (root package) builds the TraceGroup index too (issue #468 data-production gap)

Date: 2026-07-06

Issue #468 wired `GetTraceByID` to consult a real `LookupStore` when `value_index_query.enabled`
(tempo `backend_block.go`), but that wiring was inert: the only code that ever *built* a
`TraceGroup` entry was `valueindexconsumer`'s `bufferTraceRow`/`flushTraceGroups`
(NOTE-VI-063/traceflush.go) — an async, Redis-Streams-consumer path. `value-index-consumer`
(the Kubernetes deployment running that consumer) was confirmed at 0 replicas on
the dev test cluster, "replaced by inline `ValueIndexSink`" per `setup.sh`'s own comment. The
inline sink is `WriteValueIndexL0` (root `valueindex_l0write.go`, NOTE-VI-042) — the only
value-index write path that has actually run against live production data. Before this note,
it only knew enough about the TraceGroup format to *exclude* `trace:id` from the standard
per-column path (NOTE-VI-068); it never built a TraceGroup itself. Net effect: #468's lookup
had no data to ever find — every trace-by-id query would silently fall through to the full-scan
fallback forever, correctly (per SPEC-ROOT-018's "index is a hint" contract) but uselessly.

**Fix.** `WriteValueIndexL0` now accumulates `valueindex.SpanEntry` rows keyed by `TraceID`
during its single `ExtractValueIndexEntries` pass, triggered on the `span:id` sentinel column
exactly like `valueindexconsumer.ingest()` does (accumulate for the trace index AND continue to
standard per-column indexing — `span:id` is not excluded like `trace:id` is). Unlike the
consumer's `traceGroupBuffer` (which spills to disk across many async messages within a flush
window), this is a single synchronous pass over one reader, so an in-memory map
(`l0TraceAccum`) is sufficient — no cross-call buffering needed. The accumulated groups are
encoded via `valueindex.EncodeTraceGroups` and PUT under the same
`colHash("trace:id")/uuid/L0-...` key layout `flushTraceGroups` uses, so the querier's
`DiscoverIndexFiles`/compactor's format-dispatch need no changes to find them.

Back-refs: `valueindex_l0write.go:WriteValueIndexL0`, `:flushAndPutTraceGroups`,
`:l0TraceAccum`. Regression test: `valueindex_l0write_test.go:TestWriteValueIndexL0_BuildsTraceGroupIndex`
(confirmed red before the fix — 0 trace-group files produced — green after).

## NOTE-VI-071 — trace-by-ID index is authoritative; `getTraceByIDFullScan` removed (issue #473)

Date: 2026-07-06

Issue #473 (closing direction, maintainer comment): make the trace-by-ID `TraceGroup` index
authoritative and remove `getTraceByIDFullScan` (root `reader.go`) — the pre-existing
unconditional index fallback — gated on the standard build/test/race/precommit discipline, not
on observed live coverage. This mirrors SPEC-ROOT-019 (issue #474), which had just done the same
for the search/metrics index.

**The fallbacks split into three categories** (the reusable classification from NOTE-VI-047),
and the fix treats each differently:

1. **No index provided (`lister == nil || tenant == ""`)** — genuine "there is no index to
   consult," not indeterminacy. This is the WAL block (tempo `wal_block.go` permanently passes
   `nil` — freshly-ingested data is never indexed) and a backend block when the value-index
   query feature is disabled. The scan is the *only* correct path here, so it is KEPT — but
   renamed `getTraceByIDFullScan` → `scanTraceByID` and re-documented as the no-index path, NOT
   "the index fallback." The issue's "remove `getTraceByIDFullScan`" is satisfied: the function
   with that fallback contract is gone; a scan for the no-index case remains because a WAL block
   would otherwise return nothing.
2. **Authoritative miss** — the index was consulted and holds no covering entry: either
   `DiscoverIndexFiles` found zero candidate files for the window, or readable candidates simply
   lack the trace. Now returns `(nil, nil)` — an empty result, NO scan. The accepted, known
   consequence (issue #473, and the sibling NOTE-VI-070 coverage gap): any trace written before
   `fd2726f0` has no entry and reads as "not found." No backfill; retention ages it out.
3. **Index/data inconsistency** — a discovery-time `List` error, a fetch/decode failure on a
   candidate (corrupt index), an index-named page that does not resolve in `r`
   (`BlockIndexForPage !ok`; also how a cross-file span manifests), a block with no bytes / a
   parse failure, or a defensive re-verify mismatch (`rowMatchesTraceID`). All previously silent
   fallbacks; now `(nil, err)` — the index and the data file are out of sync and the caller
   observes it, exactly as SPEC-ROOT-019 does for the search/metrics path.

**Why `findTraceGroupInCandidates`/`materializeTraceGroup` changed their return shape.** Both
returned a bare `ok bool` (any doubt ⇒ fall back). They now return an explicit `error` for
category (3) and reserve `false`/empty for category (2), so the caller can distinguish "not
found" from "corrupt/skewed." A decode failure on one L0 candidate is no longer skipped ("try
the next") — under the authoritative contract a silent skip would let the index under-report a
trace's spans with no observable signal.

**A latent test bug surfaced.** `TestGetTraceByID_MultiBlockTraceViaIndex` hardcoded "target
span i lives at physical block 2*i," an unsound guess (the writer does not guarantee block order
matches submission order). Under the old hint contract the wrong index entries silently fell back
to a full scan, so the test passed for the wrong reason. Under the authoritative contract the
mismatch now (correctly) errors, exposing the guess. Fixed by building the fixture's TraceGroup
from real `ExtractValueIndexEntries` output (`realTraceGroupFor`), addressing each span by its
ACTUAL `BlockRef`+`RowIdx` — the same discipline `traceindex_pipeline_test.go` already used.

Back-refs: root `reader.go:GetTraceByID`, `:getTraceByIDViaIndex`, `:findTraceGroupInCandidates`,
`:materializeTraceGroup`, `:scanTraceByID`; root `SPEC.md` SPEC-ROOT-018 (revised) and
SPEC-ROOT-019 (the sibling authoritative contract). Tests: `gettracebyid_index_test.go`
(authoritative outcomes), `traceindex_pipeline_test.go` (end-to-end multi-L0 merge on the
authoritative path).

## NOTE-VI-072 — zero index files for a known-non-empty window is a coverage gap, not "not found"

Date: 2026-07-06

NOTE-VI-071 made `getTraceByIDViaIndex` authoritative, but `DiscoverIndexFiles` returning zero
candidate files was still treated the same as "a covering file exists but lacks this trace":
both returned `(nil, nil)`. That conflates two different situations. `queryMinSec`/`queryMaxSec`
come from the caller's own block metadata — a block `GetTraceByID` is being asked about
specifically because it holds real data — so zero index files covering that exact window means
the trace-by-ID index never ran against a block known to be non-empty. That is an indexing
coverage gap (a pipeline problem worth surfacing), not "the index consulted its records and
found nothing" (a legitimate per-trace miss).

**Fix.** `getTraceByIDViaIndex` now returns an error, not `(nil, nil)`, when `len(keys) == 0`.
The other miss case is unchanged: at least one index file covers the window but holds no entry
for this specific trace (e.g. async pipeline lag between block flush and index build for that
one trace, per `TestGetTraceByID_FreshTraceNotYetIndexedIsNotFound`) is still an authoritative,
error-free "not found" — only the total-absence-of-coverage case changed.

Back-refs: root `reader.go:getTraceByIDViaIndex`. Test:
`gettracebyid_index_test.go:TestGetTraceByID_NoIndexFileForKnownNonEmptyBlockErrors` (renamed
from `TestGetTraceByID_NoIndexFileIsAuthoritativeNotFound`, which locked in the now-incorrect
behavior).

## NOTE-VI-073 — `scanTraceByID` (the no-index path) removed entirely; `GetTraceByID` requires a lister

Date: 2026-07-06

NOTE-VI-071 kept `scanTraceByID` (renamed from `getTraceByIDFullScan`) for category 1 — the
"no index to consult" case (`lister == nil || tenant == ""`) — because tempo's WAL block
(`wal_block.go`) permanently called `GetTraceByID` with a nil lister, and a WAL block can never
have trace-index coverage. That justification no longer holds: tempo's live-store switched its
`storage.trace.block.version` from `vblockpack` to `vParquet4` (this also governs
`LiveStore.WAL.Version`, per `cmd/tempo/app/modules.go`), so live-store no longer creates
vblockpack WAL blocks at all. Block-builder still writes vblockpack WAL blocks (its own
`block.version` is unchanged), but nothing calls `FindTraceByID` against a block-builder WAL
block — block-builder only builds/completes blocks, and `tempo-cli` block-query tools read
backend blocks, not local WAL directories. With no live caller left for the no-index case,
`scanTraceByID` became genuine dead code.

**Fix.** `GetTraceByID` now requires `lister != nil && tenant != ""` unconditionally, returning
an error immediately otherwise (`"lister and tenant are required (NOTE-VI-073) -- there is no
scan fallback"`). `scanTraceByID` and its exclusive dependencies (`fetchAllBlockBytes`,
`scopeMatchingBlocks`, `parseBlocksWithWant`, `traceByIDParseConcurrency`) were deleted from
root `reader.go`. Shared helpers used by both the old scan path and the index path
(`rowMatchesTraceID`, `buildSpanMatch`) were kept — the index path still needs them.

**Tempo-side companion change.** `wal_block.go`'s `FindTraceByID` no longer calls into
`blockpack.GetTraceByID` at all (it would now hard-error on every call, breaking the interface
contract). It returns `(nil, nil)` immediately — an unreachable-but-still-correct "not found" —
without reading or decoding any WAL data.

Back-refs: root `reader.go:GetTraceByID`. Tests: every test file that previously called
`GetTraceByID` with a nil lister to get "scan ground truth" was rewritten to assert against the
fixture's own known values instead (`gettracebyid_test.go`, `gettracebyid_index_test.go`,
`gettracebyid_lookupstore_alias_test.go`, `api_test.go`, `traceindex_pipeline_test.go`); the
scan-specific regression tests and benchmarks in `gettracebyid_test.go` were deleted outright
since they had no path left to test. Tempo: `tempodb/encoding/vblockpack/roundtrip_test.go`
(`TestWalBlock_FindTraceByID_NeverFinds`, renamed from `TestWalBlock_FindTraceByID_PermanentlySkipsIndex`).

## NOTE-VI-074 — a genuinely empty file short-circuits `GetTraceByID`, ahead of the lister requirement

Date: 2026-07-06

NOTE-VI-073 removing `scanTraceByID` exposed a real gap in NOTE-VI-072's reasoning: "zero
index files for the caller's window is a coverage gap, not a legitimate miss" explicitly relies
on the caller only ever asking about a block it knows holds data. Tempo's `backend_block.go`
also falls back to a nil lister when `value_index_query` is disabled (mirroring the WAL case
NOTE-VI-073 addressed) — and for a genuinely empty block (zero traces, `r.BlockCount() == 0`,
e.g. tempo's `TestEmptyBlock`), that premise is false: there is trivially nothing to index, so
zero covering index files is expected, not a gap.

**Fix.** `GetTraceByID` checks `r.BlockCount() == 0` right after basic trace-ID format
validation and returns `(nil, nil)` immediately — before the lister/tenant requirement
(NOTE-VI-073) is even checked. An empty file has nothing to look up regardless of whether the
index is configured, so it must not demand one.

Back-refs: root `reader.go:GetTraceByID`. Test: `gettracebyid_index_test.go:TestGetTraceByID_EmptyFileIsNotFoundNotError`.

## NOTE-VI-075 — v2 batched TraceGroup index format: partial-read trace-by-id (issue #476)

Date: 2026-07-06

Production incident 2026-07-06 (issue #475 handled the immediate cache/dedup mitigation)
traced back to the trace-by-id **TraceGroup** index format being structurally unable to
support partial reads. The old flat-blob format (`ValueIndexTraceVersion = 0x01`) wrote
`version + string_table + group_count + [groups...]` as a single snappy stream — no offset
table, no per-block metadata, no bloom — so every trace-by-id lookup required a whole-object
download + full decode just to check for one trace ID. Well-compacted files ran 19MB-205MB in
production; `findTraceGroupInCandidates` fully decoded every candidate with no early exit.

### The redesign

Rebuild TraceGroup to match blockpack's own working design for exactly this problem — the v2
**BucketGroup** search/metrics format (NOTE-VI-043/045). New format, magic `"VTG2"`,
`TraceFileVersion = 0x02`:

```
magic[4] version[1]                          -- header
[ block 0 ] ... [ block N ]                  -- snappy-compressed block bodies
string_table
block_index (TOC: one traceBlockDirEntry per block)
footer[traceFooterSize]                      -- fixed 53 bytes, tail-addressable
```

Block body: `min_trace_id[16] max_trace_id[16] min_time[8] max_time[8] bloom_len[4] bloom[N]
group_count[4] [groups...]`.

**Key ordering decision.** Groups sort by `(TraceID ASC, TimeSec ASC)` — TraceID FIRST,
unlike BucketGroup's `(TimeSec, value)` and unlike the old flat format's `(TimeSec, TraceID)`.
TraceID leads because it is the trace-by-id **point-lookup key**: ordering by TraceID makes
each block's `[minTraceID, maxTraceID]` a tight, seekable bound, so the block directory prunes
whole blocks by TraceID range with no body fetch. The per-block bloom is over the block's
16-byte trace IDs (reusing `bucketbloom.go`'s value bloom, which is defined over arbitrary
bytes — trace IDs are just fixed-length values). Blocks are count-triggered at
`shared.ValueIndexTraceGroupsPerBlock` (default 4096), mirroring
`ValueIndexBucketGroupsPerBlock`. This also subsumes the compactor's unbounded
`mergeTraceLevel` output concern (issue #476 point 4): the read path never loads the whole
file, only surviving blocks, so a large single output file no longer costs a large read.

### Read path (`traceindexquery.go`)

`LookupTraceGroupPartial(store, key, traceID, minSec, maxSec)`:
1. Ranged tail read of `TraceFooterSize` bytes → file min/max time (prune whole file by time).
2. One ranged read of the contiguous `string table + block directory` tail region.
3. Prune blocks by `[minTraceID,maxTraceID]` range then time overlap.
4. For each surviving block, ranged-read + snappy-decode ONLY that block, test the trace-ID
   bloom (definite-no skip), then scan for the target TraceID.

A miss anywhere resolves with zero body fetches; a hit fetches only the covering block(s).
Groups for the same trace can straddle adjacent blocks (disjoint L0 spans), so matches are
merged with the same live-merge semantics as `MergeTraceGroups` (dedup by SpanID, min TimeSec).

### Interface change (breaking)

`LookupStore` now embeds `TraceRandomReader` (Size + ReadAt) in addition to Lister +
TraceIndexGetter. Get is retained only for the legacy flat-blob format (no footer/TOC to seek
within). **Non-breaking for tempo in practice:** its `minioVIStore` and `cachingStore` already
implement Size + ReadAt for the search/metrics path, so they satisfy the widened interface
with no new methods.

### Migration

`DecodeTraceGroups` dispatches on format: `isTraceV2` (both header + footer magic == `"VTG2"`)
→ block-by-block decode; otherwise → `decodeLegacyTraceGroups` (the old 0x01 flat blob). Old
files written before rollover still read; new writes use v2. `findTraceGroupInCandidates`
probes each candidate's format with a single ranged footer read (`probeTraceV2`) and uses the
partial path for v2, whole-file Get + decode for legacy. No backfill — retention ages out the
mixed window.

Back-refs: `internal/modules/valueindex/traceindex.go` (EncodeTraceGroups/DecodeTraceGroups,
encodeTraceBlock, traceBlockDirEntry, decodeLegacyTraceGroups),
`internal/modules/valueindex/traceindexquery.go` (DecodeTraceFooter, LookupTraceGroupPartial,
TraceRandomReader), `internal/modules/valueindex/lookupstore.go` (widened LookupStore),
`reader.go` (findTraceGroupInCandidates + probeTraceV2),
`internal/modules/blockio/shared/constants.go` (ValueIndexTraceGroupsPerBlock, TraceFileVersion
context). Tests: `traceindexquery_test.go`, `traceindex_test.go`.

## NOTE-VI-076 — `GetTraceByID` sourceRef filter: sibling-block index entries are not skew (issue #479)

Date: 2026-07-06

Discovered immediately after deploying #475/#476/#477/#478: trace-by-id queries against
already-flushed backend blocks returned HTTP 500 with `index/data skew: index-named page N
does not resolve in file`, and the *same page number recurred across unrelated block IDs* —
systematic, not corruption.

**Root cause.** `GetTraceByID` runs once per candidate block (tempo's `tempodb.Find` fans out
over every block whose window overlaps the query). `DiscoverIndexFiles` selection is
window-based, not block-specific, so when `valueindexcompactor` merges many source blocks'
trace-by-id entries into one wide compacted index file (the normal case: files span dozens of
blocks' data), *every* block whose window overlaps that file is a discovery candidate, resolves
the target `TraceGroup`, and tries to materialize it. `materializeTraceGroup` never checked
whether a matched `SpanEntry.SourceRef` actually corresponded to the reader `r` it was given —
it called `r.BlockIndexForPage(span.BlockRef.PageNum)` on every span. For every block except
the one that owns that page, this fails: the page is real and valid, just not in *this* block's
file — surfacing as the spurious skew error. And because tempo's querier
(`modules/querier/querier.go`) fails the ENTIRE trace-by-id query on ANY single block error
(`multierr.Combine(blockErrs...)`), one spurious sibling error killed the whole response even
when the correct block resolved it. #476 made this newly load-bearing: correct cross-block
discovery (fixing the minute-floor bug that had masked most sharing via false "zero candidates"
misses) meant the wide compacted files were now actually consulted.

**Fix.** Widen `GetTraceByID` (and `getTraceByIDViaIndex`, `materializeTraceGroup`) with a
`sourceRef string` param — the exact object key of the block being queried, matching what
`WriteValueIndexL0`/compaction stamp on each `SpanEntry.SourceRef` (tempo's
`blockObjectKey(tenant, blockID)` = `<tenant>/<block-id>/data.blockpack`). When `sourceRef` is
non-empty, `materializeTraceGroup` drops any `SpanEntry` whose `SourceRef != sourceRef` BEFORE
attempting `BlockIndexForPage` — sibling entries never touch this reader. Three outcomes:

  - Some entries match this sourceRef and resolve ⇒ materialize them (correct spans for THIS
    block); sibling entries silently ignored.
  - After filtering, zero spans remain ⇒ authoritative "not found in THIS block" `(nil, nil)`,
    NOT skew. The sibling block whose sourceRef matches resolves the trace in its own parallel
    `GetTraceByID` call.
  - An entry that DOES match this sourceRef but still doesn't resolve ⇒ genuine index/data skew,
    still an error (unchanged).

An empty `sourceRef` disables the filter (v1 back-compat: callers/tests with no per-block key
keep the original "every entry must resolve or it's skew" behavior).

Mirrors the search/metrics path (`QueryTraceQLFromIndex`, `search_trace_vi.go`), which already
takes a `sourceRef` and skips `m.SourceRef != sourceRef` — this closes the same gap on the
trace-by-id path.

Back-refs: root `reader.go` (`GetTraceByID`, `getTraceByIDViaIndex`, `materializeTraceGroup`);
tempo `tempodb/encoding/vblockpack/backend_block.go` (`FindTraceByID` passes
`blockObjectKey(...)`). Tests: `gettracebyid_index_test.go`
(`TestGetTraceByID_SiblingSourceRefFilteredNotSkew`,
`TestGetTraceByID_AllSiblingSourceRefsIsNotFound`,
`TestGetTraceByID_MatchingSourceRefButUnresolvableIsStillSkew`). Follow-up worth considering
(not in scope): should tempo's querier tolerate per-block errors when at least one block
succeeded, so a genuinely corrupt block cannot poison an otherwise-successful query?

**Addendum (2026-07-07, issue #489 D3B checkpoint ruling):** the resolve/skew-detection algorithm
this note originally described inline in `materializeTraceGroup` (root `reader.go`) has been
extracted verbatim into `executor.ResolveTraceGroupSourceRef`
(`internal/modules/executor/structural_traceresolve.go`, NOTE-VI-089) so it can be shared with
issue #489's multi-file structural path. `materializeTraceGroup` is now a thin delegate over that
function plus its own `SpanFieldsProvider` conversion step; this note's `sourceRef`-filter
contract (including the "zero spans survive filter is not-found, not skew" rule) is unchanged and
still fully describes the behavior — only the implementation's file location moved. See
NOTE-VI-089 for the extraction's own rationale.

## NOTE-VI-077 — `StreamCompactBucketFiles` honors `MaxOutputBytes` by splitting at block boundaries (issue #482)

Date: 2026-07-06

Before this change the v2 BucketGroup compaction path (`StreamCompactBucketFiles`) always
emitted exactly one output file per merged input set, regardless of size — the config's
`MaxOutputBytes` was threaded only through the legacy flat-VINX `CompactFiles` path. In
production this let common indexed columns (e.g. `span:kind`) grow to 75–223 MB per file
across L0→L1→L3 compaction levels, so every query touching such a file paid a larger minimum
I/O and decode cost than necessary. This is the BucketGroup sibling of the size-bounding gap
the trace-by-id TraceGroup work addressed; it was never ported to the search/metrics path.

`StreamCompactBucketFiles` now takes a `maxOutputBytes int64` argument. When `> 0`, after each
output block is cut it evaluates a **projected finalized file size** (`projectedFileSize`) and,
if that meets or exceeds the cap, finalizes the current output file (writes its string table +
block index + footer, hands its path to the `output` callback) and starts a fresh one. Key
decisions:

- **Rotation only at a block boundary, never mid-block.** A block's groups reference the
  current file's string table by interned index; splitting mid-block would strand references
  to SourceRefs the new file's fresh table does not carry. The size check therefore runs only
  right after `streamOutputWriter.add` reports it cut a block (`blockCut`). Consequence: the
  effective rotation granularity is one block (`ValueIndexBucketGroupsPerBlock` = 4096 groups),
  so a file can overshoot the cap by at most one block's serialized size. This matches the cap
  being "approximate" (same posture as the flat-VINX path's byte heuristic).

- **Project the *finalized* size, not just the body.** For a many-block file the tail (string
  table + block index + fixed footer) dominates the compressed body — a `bodyEnd`-only check
  would essentially never fire. `projectedFileSize` adds the exact string-table encoded size
  (`StringTable.EncodedSize`, no allocation) plus the exact block-index size derived from the
  flushed `dir` entries plus the fixed footer.

- **Advance contributors before the split check.** `collectContributionsAtKey` pops every
  contributing iterator off the merge heap, so `h.Len()` is 0 mid-merge for a single-input
  compaction even when that input still has groups. The "more input remaining" guard
  (`h.Len() > 0`, which prevents ever finalizing a rotation that would leave a trailing empty
  file) is therefore evaluated only *after* `advanceContributors` restores the heap.

- **Per-file state is bundled.** Each output file gets its own `bucketOutputFile` (temp file +
  buffered writer + fresh `StringTable` + `streamOutputWriter`); rotation replaces the live
  one with a new instance. `finalizeBucketOutputFile` clears the handed-off file handle so the
  function's single deferred `cleanup` only ever removes the in-progress (un-finalized) file,
  not one the `output` callback now owns.

- **Caller wiring.** `valueindexcompactor`'s `mergeLevel` passes `s.cfg.MaxOutputBytes`; its
  existing `output` callback (which reads the temp file, embeds the file's min/max time in a
  fresh V2 filename, and Puts it) already runs once per emitted file, so multi-file output
  drops straight in — each split file gets its own `NewID()` filename and independent
  time-range for `DiscoverIndexFiles` pruning. `MaxOutputBytes <= 0` disables splitting (a
  single output file, as before).

Back-refs: `internal/modules/valueindex/stream_compaction.go`
(`StreamCompactBucketFiles`, `projectedFileSize`, `bucketOutputFile`,
`finalizeBucketOutputFile`), `internal/modules/valueindex/stringtable.go`
(`StringTable.EncodedSize`), `internal/modules/valueindexcompactor/service.go` (`mergeLevel`).
Tests: `stream_compaction_test.go`
(`TestStreamCompactBucketFiles_MaxOutputBytesSplits`,
`TestStreamCompactBucketFiles_MaxOutputBytesNoSplitWhenUnderCap`);
`service_test.go` (`TestMergeLevel_MaxOutputBytesSplitsIntoMultipleFiles`). See `SPECS.md`
SPEC-VI-2 (Addendum 2026-07-06).

---

## NOTE-VI-079 — Legacy v1 flat-blob TraceGroup decode removed; trace-by-id is v2-only (issue #490, task A-2/#97)

Date: 2026-07-07

Issue #490's project-wide stored-data wipe makes the v1 flat-blob TraceGroup format
(superseded by the v2 batched format, NOTE-VI-075) unreachable — no stored file can still be in
that shape. Trace-by-id lookups and `DecodeTraceGroups` are now v2-only: a candidate/file that
isn't the v2 batched TraceGroup format (magic `"VTG2"`) is a hard decode error instead of
falling back to a whole-object `Get`+legacy-decode.

**Fix.** Root `reader.go`'s `findTraceGroupInCandidates` now calls
`valueindex.LookupTraceGroupPartial` unconditionally (deleted the `probeTraceV2` dispatch +
whole-object fallback branch; `probeTraceV2` itself deleted). `traceindex.go`'s
`DecodeTraceGroups` now errors immediately if `!isTraceV2(data)` (deleted
`decodeLegacyTraceGroups`). `shared.ValueIndexTraceVersion` deleted (confirmed zero remaining
production references before deletion).

**Not touched:** `valueindexcompactor/traceindex_dispatch.go:82` is a plain call into the
now-v2-only `DecodeTraceGroups`, not a distinct dispatch branch — nothing to delete there.

Back-refs: root `reader.go:findTraceGroupInCandidates` (deleted `probeTraceV2`),
`internal/modules/valueindex/traceindex.go:DecodeTraceGroups` (deleted
`decodeLegacyTraceGroups`), `internal/modules/blockio/shared/constants.go` (deleted
`ValueIndexTraceVersion`). See NOTE-VI-075 (the v2 format this completes the cutover to).
Tests: `traceindexquery_test.go` (`TestTraceV2_FormatDetection`, legacy fixture deleted),
`traceindex_test.go` (`TestDecodeTraceGroups_ImplausibleGroupCountRejected`, rewritten against
the v2 per-block decoder). See `TESTS.md` TEST-VI-14.

---

## NOTE-VI-081 — Ranged-read path for v2 BucketGroup files: shared metadata decode, extracted matching (issue #488, B-2/B-4)

Date: 2026-07-07

Issue #488's read-path modernization replaces vibuilder's whole-file download + in-memory
`QueryBucketFiles` scan with a partial-read path that fetches only the footer, block directory,
and surviving block bodies. Two design choices make this safe rather than a second,
independently-maintained implementation of the v2 `BucketGroup` binary format:

**Shared metadata decode (`RangedSource`/`ReadBucketFileMetadata`, SPEC-VI-7/8).**
`disk_iterator.go`'s pre-existing `readBucketFileMetadata` (compaction's disk-streaming path,
SPEC-VI-5's `GroupIterator` lineage) already did footer→string-table→block-directory decoding
against a local `*os.File`. Rather than write a second version of this offset arithmetic against
vibuilder's `FileStore`, `bucketfile_metadata.go` extracts a store-agnostic
`ReadBucketFileMetadata(src RangedSource)` and rewrites `disk_iterator.go`'s version as a thin
`*os.File`-adapter wrapper over it, unchanged in signature/behavior. `RangedSource` is
deliberately minimal (`Size`/`ReadAt` only) so both a local file and an object-storage adapter
(`vibuilder.storeRangedSource`, NOTE-VI-084) satisfy it without either depending on the other's
concrete type.

**Extracted, shared block matching (`matchGroupsInBlock`).** `QueryBucketFiles`' inner
per-block loop (time-range check, then predicate/group matching) is extracted into
`matchGroupsInBlock` and called identically by both `QueryBucketFiles` (whole-file) and the new
`QueryBucketFileRanged` (SPEC-VI-10). This is a correctness-by-construction choice, not just
code reuse: a hand-duplicated second copy of the matching logic in the ranged path would be one
more place a future predicate-type addition or edge-case fix could be applied to only one of the
two paths, silently reintroducing exactly the kind of read-path divergence issue #476/NOTE-VI-046
warned about for trace-by-id lookups. With the shared helper, the two paths are structurally
incapable of disagreeing on which groups a given block yields — the only remaining difference is
which blocks' bodies get read at all (SPEC-VI-9's directory-level value prune, new in the ranged
path only, since `QueryBucketFiles` already has the whole block decoded by the time matching
happens and has nothing left to prune before reading).

Back-refs: `internal/modules/valueindex/bucketfile_metadata.go`,
`internal/modules/valueindex/bucketquery.go:QueryBucketFiles,matchGroupsInBlock`,
`internal/modules/valueindex/bucketquery_ranged.go:QueryBucketFileRanged`,
`internal/modules/valueindex/disk_iterator.go:readBucketFileMetadata` (the adapter). See
`SPECS.md` SPEC-VI-7/8/9/10.

## NOTE-VI-082 — Corrected stale premise: no post-read bloom check exists in `QueryBucketFiles`; `QueryBucketFileRanged` correctly adds none (issue #488, B-4)

Date: 2026-07-07

`plan.md` and `task-breakdown.md` §B-4 described the ranged-read design as including a "bloom
check (post-read, unchanged)" step, implying `QueryBucketFiles` already performed one after
decoding a block and that the new ranged path should replicate it. **Verified by direct read,
oracle-checked (spec-oracle-b, 2026-07-07): this step never existed.** `QueryBucketFiles`'
per-block loop (`bucketquery.go:QueryBucketFiles`) does exactly two things per block —
`OverlapsTimeRange` (time prune), then `matchGroupsInBlock` (group/predicate matching) — with no
call to `BucketBlock.MayContainValue` (the bloom test) anywhere in between or afterward.
`matchGroupsInBlock` itself (`bucketquery.go:matchGroupsInBlock`, the function `QueryBucketFileRanged`
shares per NOTE-VI-081) checks only `g.TimeSec` bounds and `pred.Match(g.CanonicalValue)` per
group — again, no bloom involvement.

`BucketBlock.MayContainValue`/`TestValueBloom` (`bucketquery.go:MayContainValue`) exist in this package and
remain available, but are unused by the `QueryBucketFiles`/`matchGroupsInBlock`/
`QueryBucketFileRanged` call chain as of this writing (whether they are called from elsewhere in
the package — e.g. a different, non-`QueryBucketFiles` caller — was not checked as part of this
finding and is out of scope for it).

**No code change follows from this correction.** `QueryBucketFileRanged` deliberately does not
add a bloom check that `QueryBucketFiles` never had — doing so would be new, unrequested
behavior beyond parity, not a gap-fill. Cross-path parity between the two read functions is
guaranteed structurally by both calling the literally-shared `matchGroupsInBlock` (NOTE-VI-081),
not by independently replicating a step that turned out not to be real. This is recorded here,
following the same pattern established in this project's Phase A work (a plan/breakdown
description verified absent from actual code rather than implemented as described), and is
called out in the issue #488 closing-comment draft (task B-8) as a second documented scope
clarification alongside the ruling-14 numeric-range-pruning boundary (SPEC-VI-9).

Back-refs: `internal/modules/valueindex/bucketquery.go:QueryBucketFiles,matchGroupsInBlock,BucketBlock.MayContainValue`,
`internal/modules/valueindex/bucketquery_ranged.go:QueryBucketFileRanged`. See `SPECS.md`
SPEC-VI-10 (states this explicitly as part of `QueryBucketFileRanged`'s contract).

## NOTE-VI-083 — `DecodeBucketFooter` wraps `ErrNotBucketFile` on both branches, enabling footer-only skip classification (issue #488, B-4)

Date: 2026-07-07

`DecodeBucketFooter` (`bucketquery.go:DecodeBucketFooter`) now wraps `ErrNotBucketFile` on
**both** of its error branches — data too short to hold a footer and bad footer magic — rather
than returning a plain, unwrapped error on either. This lets a **footer-only** reader (code that
has read only the fixed-size footer bytes, not a whole decoded `BucketFile`) use a single
`errors.Is(err, ErrNotBucketFile)` check as its complete skip-vs-abort signal, exactly mirroring
how `QueryBucketFiles`' whole-file path already classifies `DecodeBucketFile`'s magic-mismatch
case (`bucketquery.go:QueryBucketFiles`'s `ErrNotBucketFile`-skip branch). `QueryBucketFileRanged`'s
`readBucketFileFooter` helper (`bucketfile_metadata.go`) is the first, and as of this writing
only, consumer of this footer-only classification.

**Behavior classification: additive, not breaking.** Confirmed by grep across the repository
that no existing caller of `DecodeBucketFooter` checked its returned error by string text or by
any means other than a plain non-nil check; widening the returned error to additionally satisfy
`errors.Is(_, ErrNotBucketFile)` cannot break an existing non-nil-only check, since every such
check still observes a non-nil error exactly when it did before.

Back-refs: `internal/modules/valueindex/bucketquery.go:DecodeBucketFooter`,
`internal/modules/valueindex/bucketfile_metadata.go:readBucketFileFooter` (the consumer). See
`SPECS.md` SPEC-VI-8.

---

## NOTE-VI-088 — MaterializeTraceGroupMultiFile: Option A multi-file trace materialization for structural queries (issue #489, plan-d.md §D3, team-lead ruling 4; relocated root→executor per D3B checkpoint ruling)

**What this is.** `executor.MaterializeTraceGroupMultiFile(ctx, readerFor StructuralReaderProvider, group valueindex.TraceGroup, traceID [16]byte, maxConcurrentReaderOpens int) ([]ResolvedSpan, error)` (`internal/modules/executor/structural_multifile.go`) resolves EVERY distinct `SourceRef` present in a `TraceGroup` against its OWN reader, with bounded concurrency (`golang.org/x/sync/errgroup` + `SetLimit`, defaulting to `defaultMaxConcurrentReaderOpens = 8` when the caller passes `<= 0`).

**Location note (superseding an earlier draft of this same design):** this function, its `StructuralReaderProvider` func type, and its `ResolvedSpan` result type originally landed (uncommitted) in root `structural_multifile.go` as unexported symbols. Team-lead's D3B checkpoint ruling relocated all three into `internal/modules/executor` (same filename, now exported `MaterializeTraceGroupMultiFile`/`StructuralReaderProvider`/`ResolvedSpan`) because D3B/D4/D6 all live in this package and need these types directly — an unexported root type is unreachable from `executor`, and `executor` importing root would be a cycle (root already imports `executor`). No design/contract change accompanied the move, only location + exported-name capitalization.

**Why this is additive, not a change to `GetTraceByID`'s existing single-file behavior.** `GetTraceByID` (root `reader.go`) resolves a `TraceGroup`'s spans against ONE caller-owned reader, FILTERING OUT (not erroring on) any span whose `SourceRef` doesn't match that one reader's file — correct for, and unchanged for, its own per-block parallel-call pattern (each sibling block resolves its own spans independently, NOTE-VI-076). A structural query's trace, however, commonly spans multiple compaction-boundary files (team-lead ruling 4, 2026-07-07: "this is the COMMON case, not an edge case" — structural evaluation needs the trace's FULL span set assembled in one place), so silently dropping sibling-file spans here would produce a materially WRONG structural-query answer (a missing ancestor/descendant), not merely a partial view another parallel call fills in.

**Each file's actual resolve/skew-detection is delegated to `ResolveTraceGroupSourceRef` (NOTE-VI-089) — now the literal single shared implementation for both entry points**, a STRONGER guarantee than the original design (which had structural's multi-file path calling root's `materializeTraceGroup` once per file — a caller/delegate relationship, not a shared primitive). See NOTE-VI-089 for the algorithm itself.

**Two distinct, both call-failing error classes (never a silent drop, never a partial/degraded result):**
- `ErrStructuralMultiFileCoverageGap` (`errors.Is`-comparable): the caller-supplied `StructuralReaderProvider` could not open a `SourceRef` at all — a genuine multi-file coverage gap (team-lead ruling 4: "typed error ONLY for genuinely missing/unreadable SourceRefs").
- `ResolveTraceGroupSourceRef`'s own index/data skew error (reused as-is, wrapped with the sourceRef context but not re-typed): a `SourceRef` that DOES open but whose claimed block/row doesn't check out.
Either failure fails the WHOLE `MaterializeTraceGroupMultiFile` call via `errgroup.Wait()` — no partial per-file success is ever surfaced to the caller as a usable result.

**`StructuralReaderProvider` (exported func type) — Option A, caller owns reader lifecycle.** `func(ctx context.Context, sourceRef string) (*modules_reader.Reader, error)` resolves a `SourceRef` (the S3 object key already stamped on every `SpanEntry`, NOTE-VI-076) to an already-open reader. Option A (team-lead ruling, 2026-07-07) deliberately reuses whatever reader cache/pool the caller (tempo's querier, via root D5's `QueryStructuralFromIndex`) already maintains rather than building a second cache inside blockpack. Root's `Reader` type is an alias for `modules_reader.Reader`, so root callers pass their own `*blockpack.Reader` values here unchanged — no adapter needed.

**`ResolvedSpan` — single-source-of-truth shape for D3B/D4/D6.** `ResolvedSpan{Reader, SourceRef, Span valueindex.SpanEntry, BlockIdx int, RowIdx uint16}` is returned specifically so D3B's candidate-verification (targeted row reads), D4, and D6 can all consume the exact per-span "which reader resolved this, at which block/row" record without re-deriving it — defined once here per plan-d.md's single-source-of-truth process learning.

**Determinism, not I/O-completion-order.** Results are grouped by `SourceRef` in **first-seen order** within `group.Spans` (`distinctSourceRefsInOrder`), not goroutine-completion order — output is stable across runs regardless of which concurrent `readerFor` call finishes first.

Back-refs: `internal/modules/executor/structural_multifile.go:MaterializeTraceGroupMultiFile, StructuralReaderProvider, ResolvedSpan, ErrStructuralMultiFileCoverageGap, distinctSourceRefsInOrder`, `internal/modules/executor/structural_traceresolve.go:ResolveTraceGroupSourceRef` (NOTE-VI-089, the shared resolve primitive this delegates to per SourceRef). Spec invariants referenced (not modified): NOTE-VI-076 (the single-file sourceRef-filter behavior this is additive to, never changes), SPEC-ROOT-001 (bounded concurrency + typed errors, no panic on a coverage gap or skew), NOTE-VI-072 (the coverage-gap-vs-legitimate-miss shape this new error class deliberately mirrors). Tests: `internal/modules/executor/structural_multifile_test.go` (`TestMaterializeTraceGroupMultiFile_SingleSourceRef_MatchesExistingBehavior`, `_TwoSourceRefs_ResolvesBoth`, `_UnreadableSourceRef_ReturnsTypedCoverageGapError`, `_SkewWithinOneFile_StillErrors`, `_BoundedConcurrency_NeverExceedsLimit`, `_ManySourceRefs_AllResolveNoDeadlock`). Issue #489.

---

## NOTE-VI-089 — ResolveTraceGroupSourceRef: the resolve/skew-detection algorithm extracted from root's materializeTraceGroup, now the single shared primitive for both single-file and multi-file trace-by-id resolution (issue #489, plan-d.md D3B checkpoint ruling)

**What this is.** `executor.ResolveTraceGroupSourceRef(reader *modules_reader.Reader, group valueindex.TraceGroup, traceID [16]byte, sourceRef string) ([]ResolvedTraceRow, error)` (`internal/modules/executor/structural_traceresolve.go`) is the canonical trace-group resolve/skew-detection algorithm: filter `group.Spans` to `sourceRef` (empty disables the filter — v1 back-compat, NOTE-VI-076 unchanged), resolve each surviving span's `BlockRef` to a block index, read + parse the needed blocks, and defensively re-verify each resolved row's own `trace:id` column value against the caller's `traceID`. Every failure mode — an index-named page that doesn't resolve, a `ReadBlocks`/parse failure, or a resolved row whose `trace:id` column doesn't match — is index/data skew and returns an error, never a silent drop. When `sourceRef` is non-empty and zero spans survive the filter, that is an authoritative "not found in THIS file" (`(nil, nil)`), not skew — a sibling file's own resolve call handles those spans (this is `materializeTraceGroup`'s ORIGINAL NOTE-VI-076 contract, preserved exactly).

**Moved verbatim from root's `materializeTraceGroup` (`reader.go`) — algorithm unchanged, only relocated.** Root's `materializeTraceGroup` is now a THIN DELEGATE: it calls `ResolveTraceGroupSourceRef`, then performs only the genuinely root-only remaining step — building its own public `[]SpanMatch` via `SpanFieldsProvider`/`Clone`/pooling, which is a public-API-facing concern that deliberately did NOT move (it has no reason to live in `executor`, and root already owns that machinery for every other query path). `MaterializeTraceGroupMultiFile` (`structural_multifile.go`, NOTE-VI-088) instead converts each `ResolvedTraceRow` to the lighter `ResolvedSpan` shape D3B/D4/D6 need. Error messages inside `ResolveTraceGroupSourceRef` omit the `"GetTraceByID: "` prefix (it is no longer a `GetTraceByID`-specific primitive); root's thin delegate re-adds that prefix when wrapping, so `GetTraceByID`'s existing external error text is unchanged for every caller depending on it today.

**Why this move, and not the reverse.** D3B/D4/D6 all live in `internal/modules/executor` and need the resolved-row shape directly (single-source-of-truth requirement, plan-d.md) — the original unexported root type was unreachable from `executor`, and `executor` importing root back would be an import cycle (root already imports `executor` for `api.go`/`query_traceql.go`). `ResolveTraceGroupSourceRef` itself has zero root-package dependency (`modules_reader`/`modules_shared`/`valueindex` only), making it the one piece that could move without loss — the genuinely root-only remainder (`SpanFieldsProvider` conversion) stayed exactly where it belongs.

**Stronger single-source-of-truth guarantee than the original D3 design achieved.** Before this extraction, structural's multi-file path (`MaterializeTraceGroupMultiFile`) CALLED root's `materializeTraceGroup` once per file — a caller/delegate relationship between two independently maintained implementations that COULD have drifted. After this extraction, `GetTraceByID`'s single-file path and structural's multi-file path both call down into this ONE function — there is no longer a second implementation to keep in sync, only one algorithm with two different result-shape converters layered on top (`SpanMatch` for root, `ResolvedSpan`/`ResolvedTraceRow` for executor).

**Verification: no test regression.** All 19 pre-existing `GetTraceByID` tests (`gettracebyid_index_test.go` et al.) pass unchanged post-extraction — confirming the algorithm is byte-for-byte behaviorally identical to its pre-move form, just relocated. No new dedicated test file exists for `ResolveTraceGroupSourceRef` itself; it is exercised both by those 19 existing `GetTraceByID` tests (via root's thin delegate) and by `structural_multifile_test.go`'s 6 tests (via `MaterializeTraceGroupMultiFile`) — this dual coverage is itself part of the single-source-of-truth proof.

Back-refs: `internal/modules/executor/structural_traceresolve.go:ResolveTraceGroupSourceRef, ResolvedTraceRow, groupTraceGroupSpansByBlock, rowMatchesTraceIDColumn`, root `reader.go:materializeTraceGroup` (now a thin delegate — see this note's own back-ref, NOT a separate implementation), `structural_multifile.go:MaterializeTraceGroupMultiFile` (NOTE-VI-088, the sibling consumer). Tests: `gettracebyid_index_test.go` (all 19, unchanged, exercising this via root's delegate), `internal/modules/executor/structural_multifile_test.go` (all 6, exercising this via `MaterializeTraceGroupMultiFile`). Issue #489.

---

## NOTE-VI-094 — LookupResult.SpanID / VILookupResult.SpanID is unconditionally zero for every entry the BucketGroup write path produces — a wire-format property, not a column-identity conditional (issue #489, task #12 post-mortem)

**The empirical fact.** `valueindex.LookupResult.SpanID` / `VILookupResult.SpanID` is zero for every entry `WriteValueIndexL0` writes, for every column, always. Any code that groups or joins `LookupResult`/`VILookupResult` values keyed by `SpanID` against a second data source's own SpanID will match nothing, always — not a partial-match degradation, a total one, that still returns `ok=true` (authoritative-looking success) with zero results.

**The verified mechanism.** Extraction (`valueindex_extract.go:extractBlockColumns`, `~L248-277`) correctly reads a real, non-zero SpanID from the block's own `span:id` column at each row and attaches it to EVERY column's `ValueIndexEntry` — no exceptions, no per-column split. Write-time routing (`valueindex_l0write.go:171-185`) correctly keys `AddEntryV4`-vs-`AddEntryV2` on `e.SpanID != zero`, not on column identity, so `AddEntryV4` genuinely fires and carries a real SpanID into `rawEntry.spanID` for ordinary attribute columns too (proven by live instrumentation, not merely inferred from reading the code). **The information is discarded one step later, in `assembleBucket`** (`internal/modules/valueindex/writer.go:264-325`, specifically line 313): `SpanRef{TraceID: re.traceID}` never reads `re.spanID`, because `SpanRef` (`internal/modules/valueindex/bucketfile.go:71-74`) has no SpanID field to receive it — only `SpanIndexes []uint16` and `TraceID [16]byte`. **The read side confirms the same absence symmetrically:** `matchGroupsInBlock` (`internal/modules/valueindex/bucketquery.go:154-182`, the shared group-matching primitive for both `QueryBucketFiles` and the ranged read path) builds every `LookupResult` it emits with no SpanID field ever set, for every group/span it walks — confirming the zeroing covers EVERYTHING on the BucketGroup query path, including the `span:id` sentinel column's own entries, with no per-column split anywhere in the pipeline, read or write side.

**This is a deliberate design choice (NOTE-VI-045/#429), not an oversight in the format itself — what went undocumented until this incident was the choice's downstream IMPLICATION.** `SpanRef`'s field set (`SpanIndexes`+`TraceID`, no SpanID) is exactly NOTE-VI-045/#429's own addressing decision: a span is identified by `(SourceID/page, RowIdx)` plus `TraceID`, not by SpanID — a legitimate, intentional format choice, not a bug. The bug was that this choice's necessary consequence for every FUTURE consumer — "never join a `LookupResult`/`VILookupResult` against another data source by `SpanID`, because the format cannot carry one" — was never written down anywhere until task #12 forced the issue. This note exists specifically to close that documentation gap, not to relitigate NOTE-VI-045's own addressing choice, which remains correct and unchanged.

**Correct key instead: `(SourceRef, BlockRef/BlockPage, RowIdx)`** — the address the write path actually and unconditionally guarantees for every entry (NOTE-VI-045). D3's `ResolvedSpan`/`ResolvedTraceRow` (SPEC-VIS-3/4) and D4/D6's `structuralSpanAddr` (NOTE-VI-092/093) already key on exactly this address.

**Provenance.** Found by coder-d3's first genuine end-to-end test exercising the REAL write path (`WriteValueIndexL0` → search-VI → `QueryStructuralFromIndex`) — every prior hand-constructed `LookupResult`/`VILookupResult` fixture in this phase's test suite set SpanID to an artificially-correct value, masking the defect for the suite's entire duration. See NOTE-VI-092/093 for the fix as applied to D4/D6, and TEST-VI-22/EX-36 for the resulting mandatory test-class policy.

**A second, independent consumer of this same invariant — now RESOLVED (task #13).** `QueryTraceQLFromIndex` (`internal/modules/executor/search_trace_vi.go`, issue #459 lineage, pre-existing and production-live, unrelated to #489's original scope but folded into this phase since it shared the identical root cause) previously built its public `SpanMatch.SpanID` directly from the always-zero `VILookupResult.SpanID` field. **Fixed (task #13):** the function now requests the `span:id` column via `withSpanIDColumn` and resolves each matched row's real SpanID from the already-decoded block via `resolveRowSpanID`, never trusting `VILookupResult.SpanID` — mirroring how the structural path already obtains real span identity from block rows rather than from VI lookup results. The function's own doc comment now states this invariant directly, matching this note.

**Test coverage (task #13).** `search_trace_vi_realvi_test.go`'s `TestQueryTraceQLFromIndex_RealWriteValueIndexL0_SpanIDIsCorrect` is the TEST-VI-22/EX-36-class real-write-path test for this fix — mutation-verified (reverting the fix reproduces the exact `"0000000000000000"` production symptom, confirming the test is not vacuous).

**Other `SpanMatch.SpanID` consumers audited, confirmed unaffected (task #13's mandatory item 3).** `spanmatch.go`'s `Clone()` is a pure pass-through with no independent trust in the value; root `api.go`'s two `SpanMatch` construction sites are the now-fixed search-index path (task #13) and `ExecuteStructural`'s SCAN-based structural engine, which has always read SpanID directly from decoded block columns rather than from any `VILookupResult` and was never exposed to this defect class; `metrics_trace.go` never reads `.SpanID` from its own `VILookupResult` mirror type anywhere in its output construction. No further consumers of this invariant remain to audit.

**Existing code comment corrected alongside this finding.** `valueindex/query.go`'s `LookupResult.SpanID` doc comment ("zero for v1-v3 files") undersold the real scope — it is zero for every version, unconditionally, for the reason above; recommend correcting that comment alongside whichever fix lands next in this area.

Back-refs: `internal/modules/valueindex/bucketfile.go:SpanRef`, `internal/modules/valueindex/writer.go:assembleBucket`, `internal/modules/valueindex/bucketquery.go:matchGroupsInBlock`, `valueindex_extract.go:extractBlockColumns`, `valueindex_l0write.go` (write-time routing), `internal/modules/valueindex/query.go:LookupResult` (comment needs correction), `internal/modules/executor/metrics_trace.go:VILookupResult` (NOTE-VI-035), `internal/modules/executor/search_trace_vi.go:QueryTraceQLFromIndex, withSpanIDColumn, resolveRowSpanID` (task #13). See TEST-VI-22 (`TESTS.md`) and EX-36 (`internal/modules/executor/TESTS.md`) for the mandatory test-class response, NOTE-VI-092/093 (`internal/modules/executor/NOTES.md`) for D4/D6's fix. Issue #489, tasks #12/#13.

**2026-07-08 amendment (Phase D holistic review fix pass, LOW finding).** The comment correction this note itself recommended above has now landed: `internal/modules/valueindex/query.go`'s `LookupResult` doc comment and its `SpanID` field comment were updated to state the field is unconditionally zero for EVERY version on the BucketGroup write/query path (not merely "v1-v3 files"), citing this note by ID. No behavior change — `deduplicateLookupResults`' own dedup key was deliberately left untouched (still keys on `(TraceID, SpanID, BlockRef.PageNum, BlockRef.LenPages)`, i.e. effectively `(TraceID, BlockRef)` in production since SpanID is always zero there) — that remains a follow-up ticket per this note's own out-of-scope framing, not fixed in this pass.

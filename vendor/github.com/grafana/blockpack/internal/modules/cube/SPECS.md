# Cube Module Specifications

## Module Responsibility

The cube module implements the binary on-disk format for pre-aggregated metrics cubes. A cube file (`.cube`) stores sparse cells representing pre-computed span aggregates per (minute, dimension1, dimension2) tuple. The format enables random access via binary search on sorted cells and supports snappy-chunked storage for efficient compression and range queries.

**As of issue #491:** a cell's base fields (`Minute`/`Dim1ID`/`Dim2ID`/`Count`) still support the original pure-count use case (`count_over_time`/`rate`) byte-identically, but a cube may also materialize zero or more per-attribute records (`Sum`/`Min`/`Max`/`SampleCount`/`Buckets`) — see `SPEC-CUBE-001` for the `AggCell` type and `SPEC-CUBE-021` for the wire-format-v2 header/record layout. The module supports every TraceQL metrics aggregation (`count_over_time`, `rate`, `sum_over_time`, `min_over_time`, `max_over_time`, `avg_over_time`, `histogram_over_time`, `quantile_over_time`), not just count.

---

## SPEC-CUBE-001: AggCell — the sole cell/record type (issue #491, E-3 APPENDIX 2)

**Invariant:** `AggCell` is the SOLE cell type in this package — there is no separate, narrower
"Cell" type. Base fields (`Minute uint32`, `Dim1ID uint16`, `Dim2ID uint16`, `Count uint32`) are
FLATTENED directly onto the struct, exactly matching the pre-#491 12-byte wire layout when
`Aggs` is empty. `Aggs []AggAttrValues` carries zero or more per-aggAttr records; `len(Aggs)` must
equal the owning file's `Header.NumAggAttrs` (`SPEC-CUBE-021`) — empty/nil is wire-tolerated (and
byte-identical to the pre-#491 format) but is never actually produced by any real
`Definition`/`CreationTrigger.TryCreate` path, since `duration` is mandatory in every v2 cube's
`AggAttrs` (E-4/E-6). Sort order: `(Minute ASC, Dim1ID ASC, Dim2ID ASC)` — `Aggs` never
participates in ordering (`CompareAggCell`). All fields little-endian.

**Binding ruling (do not reintroduce a separate Cell type):** a prior revision of this file had a
narrower `Cell` type (`EncodeCell`/`DecodeCell`/`CompareCell`) with `AggCell` nesting it as a
`.Cell` field. This was DELETED, not deprecated-and-kept, per team-lead's binding "no coexistence,
one-current-version" ruling (task #40, "APPENDIX 2 compliance") — matching this project's standing
no-backward-compat/one-format-version policy (`SPEC-ROOT-013` precedent) applied at the Go-type
level, not just the wire-format level. Any future task that might be tempted to reintroduce a
narrower type for convenience must not — `AggCell` with `Aggs` empty already serves that purpose
with zero duplication.

**Back-ref:** `internal/modules/cube/cell.go:AggCell,EncodeAggCell,DecodeAggCell,CompareAggCell`

---

## SPEC-CUBE-002: Dictionary — per-file string-to-uint16 mapping

**Invariant:** A `Dictionary` maps each dimension's distinct string values to a `uint16` ID
(0-65535), independently per dimension (`dim1Map`/`dim2Map`) and per file — dictionaries are
NOT shared or global across cube files. Wire format: `dim1_count[2] + (len[2]+bytes)*` followed
by the identical layout for dim2. Interning (`InternDim1`/`InternDim2`) is append-only and
returns an error once a dimension exceeds 65535 distinct values (matches
`CardinalityGate`'s `MaxDistinctPerDim` default of 1000, which keeps real dictionaries far
below this hard ceiling). Decode rebuilds both reverse maps from the decoded value slices.

**Back-ref:** `internal/modules/cube/dict.go:Dictionary,InternDim1,InternDim2,EncodeDictionary,DecodeDictionary`

---

## SPEC-CUBE-003: Header format (36 bytes)

**Invariant:** Every cube file begins with a fixed 36-byte header: `magic[4] version[1]
num_agg_attrs[1] reserved[2] cube_id[16] min_minute[4] max_minute[4] resolution[4]`, all
little-endian. `resolution` is `1` (L0), `60` (L1), or `1440` (L2) — see `SPEC-CUBE-016`.
`min_minute`/`max_minute` bound the file's cell range and are set from the first/last cell after
sorting (see `SPEC-CUBE-006`'s section-ordering contract for how the header relates to the rest
of the file). `num_agg_attrs` is described fully in `SPEC-CUBE-021` (wire format v2).

**Back-ref:** `internal/modules/cube/file.go:Header,EncodeHeader,DecodeHeader`

---

## SPEC-CUBE-004: Footer format (40 bytes)

**Invariant:** Every cube file ends with a fixed 40-byte footer: `magic[4] version[1]
reserved[3] cell_count[8] dict_offset[8] chunks_offset[8] dir_offset[8]`, all little-endian.
The three offset fields are absolute byte offsets from the start of the file (not relative to
the footer) and let a reader seek directly to the dictionary, the chunk data, or the chunk
directory without a linear scan — see `SPEC-CUBE-006`.

**Back-ref:** `internal/modules/cube/file.go:Footer,EncodeFooter,DecodeFooter`

---

## SPEC-CUBE-005: Magic/version validation — shared corruption-detection contract

**Invariant:** Both `DecodeHeader` and `DecodeFooter` independently validate the leading
4-byte magic number (`0x43554245`, ASCII `"CUBE"`) and reject any mismatch with a typed error
before decoding any other field. This gives corruption detection at BOTH ends of the file
independently — a header-only or footer-only corruption is each caught at its own end, rather
than relying on a single check at one end of the file to vouch for the whole file's integrity.
`DecodeHeader` additionally rejects any file whose `Version` does not equal `VersionCube` — see
`NOTE-CUBE-013`.

**Back-ref:** `internal/modules/cube/file.go:MagicCube,DecodeHeader,DecodeFooter`

---

## SPEC-CUBE-006: Overall file layout — section ordering and offset chain

**Invariant:** A cube file's on-disk layout, in order, is: `Header(36B)` → `Dictionary` →
snappy-compressed cell chunks (`SPEC-CUBE-007`) → `ChunkDirectory` (`SPEC-CUBE-008`) →
`Footer(40B)`. The footer's `dict_offset`/`chunks_offset`/`dir_offset` fields (`SPEC-CUBE-004`)
record the absolute byte offset of each section's start, computed by the writer as a running
sum of the preceding sections' encoded lengths (`dictOffset = len(header)`, `chunksOffset =
dictOffset + len(dict)`, `dirOffset = chunksOffset + len(chunks)`). This offset chain is what
lets `Reader` open a file and jump straight to any section without decoding the ones before it.

**Back-ref:** `internal/modules/cube/writer.go:Writer.Encode`

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

**Full-fidelity counterpart:** `GetAggCell`/`GetAggCellsInRange` mirror this exact contract but
return the complete `AggCell` (base fields + every per-aggAttr record) rather than just `Count` —
see `SPEC-CUBE-022`.

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

**Addendum (2026-07-08, issue #491, task E-4):** `NewAccumulator`'s signature changed to
`(def Definition, minute uint32) (*Accumulator, error)` — it now validates `def` (see
`SPEC-CUBE-025`) and can fail. `Add` also updates per-`Definition.AggAttrs` state
(`Sum`/`Min`/`Max`/`SampleCount`/`Buckets`) alongside the base count increment described above —
see `SPEC-CUBE-025` for the current, complete contract.

**Back-ref:** `internal/modules/cube/accumulator.go`, `internal/modules/cube/writer.go:Encode`

## SPEC-CUBE-012: CubeDefinition / RegistryEntry (issue #444)

`RegistryEntry` is the full, stable description of one active cube stored in `<tenant>/cubes/index.json`.
`CubeID = hex(SHA256(tenant+sorted(dims)+sorted(filters))[:8])` — deterministic so concurrent creators converge.
`DefFilterOp` (GT/GTE/LT/LTE/EQ) is the wire/JSON form; separate from the runtime `FilterOp` used by the accumulator.

**Addendum (2026-07-08, issue #491, task E-6):** `CubeID`'s hash formula gained a fourth,
`aggAttrs` segment — the 3-segment `hex(SHA256(tenant+dims+filters)[:8]))` formula described above
is the pre-#491 formula and is no longer what the code computes. See **SPEC-CUBE-020** for the
current, complete identity contract. `RegistryEntry` also gained an `AggAttrs []string` field not
described above — see SPEC-CUBE-020.

**Addendum (2026-07-08, issue #491 Phase E fix pass, review.md Issue 1):** the
`entry.AggAttrs -> Definition.AggAttrs` conversion (`AggAttrDefsFor`, formerly the unexported
`aggAttrDefsFor` in backfill.go, exported for reuse) is now the single source of truth for BOTH
`CubeRegistryEntryToDefinition` (cube_ingest.go, the real forward-ingest path) and
`Backfiller.processMinute` (backfill.go) — see SPEC-CUBE-027 for the full fix rationale.

**Back-ref:** `internal/modules/cube/definition.go`

## SPEC-CUBE-013: CardinalityGate (issue #445)

`CheckCardinality` guards cube creation by checking per-dimension distinct-value counts and
estimated combined (dim1×dim2) cells against configurable limits using value count index data.
UUID/high-entropy columns are always rejected. Returns `*CardinalityError` with an actionable
reason string and suggestion. The caller supplies pre-fetched VCNT data+dir — zero S3 I/O inside
the gate.

Default limits: MaxDistinctPerDim=1000, MaxCombinedCells=50000.

**Addendum (2026-07-08, issue #491, task E-7):** `CardinalityLimits` gains
`MaxCombinedCellBytes` (default `50_000*12=600,000` — a sizing heuristic derived from the
pre-#491 `MaxCombinedCells*12-byte` figure, not a compatibility mechanism; no wire byte from any
prior format is preserved by this calculation). `CheckCardinality` gains an `aggAttrs
[]AggAttrDef` parameter; after the existing per-dimension and combined-cell-count checks, a new
combined-byte-cost check computes `combinedCells * recordWidthFor(len(aggAttrs))` (`chunk.go`'s
single source of truth for the record-width formula, `SPEC-CUBE-021`) and rejects via a
`*CardinalityError` when it exceeds `MaxCombinedCellBytes` — a cube with few cells but many/wide
aggAttr records can be rejected even when the plain cell-count check would pass.
`combinedCells` is now computed for BOTH the 1-dimension (`len(dimValues[0])`) and 2-dimension
(`dim1×dim2`) cases (previously only computed for 2 dimensions) so the byte-cost check applies to
single-dimension cubes too — the plain `MaxCombinedCells` count-check itself is unchanged and
still applies only at 2 dimensions.

**Back-ref:** `internal/modules/cube/cardinality.go`

## SPEC-CUBE-014: Registry (issue #444)

**[UPDATED, 2026-07-11 — entryStore refactor, non-behavioral, task #158/#160]** `Registry`
no longer talks to `ObjectStore` directly: it holds a package-private `entryStore` interface
(`load`/`addEntry`/`removeEntry`/`updateWatermarksEntry`), and `blobEntryStore` (`entry_store.go`)
is the ONLY current implementation, wrapping `ObjectStore` exactly as described below — this
entry's contract is unchanged from the caller's perspective, only the internal structure moved
(the conditional-PUT retry loops NOTE-CUBE-009 describes now live in `blobEntryStore.addEntry`/
`removeEntry`/`updateWatermarksEntry`, moved verbatim from `Registry.Add`/`Remove`/
`UpdateWatermarks`). This refactor exists so `Registry` can ALSO sit on top of a Postgres-backed
`EntryStore` (exported, `entry_store.go`, task #160) via `NewRegistryFromEntryStore` without any
change to `Registry`'s own public methods — mirrors `internal/modules/viusage`'s identical
refactor (`viusage/SPECS.md` SPEC-VIUSAGE-4's own `[UPDATED]` annotation) exactly, except cube's
`entryStore` has 4 narrow methods (one per Add/Remove/UpdateWatermarks operation) rather than
viusage's single generic `upsertEntry(createIfMissing, mutate)` — see `entry_store.go`'s own doc
comment for why cube's existing write operations don't share viusage's create-or-mutate-one-entry
shape.

`Registry` persists the per-tenant cube index in object storage with S3 conditional-PUT concurrency
control. Concurrent writers compute the same deterministic CubeID; only one conditional PUT succeeds —
the rest see a 412/ErrConflict and re-read. Up to 5 exponential-backoff retries (50ms base, doubling).
`Add` is idempotent. `Remove` is idempotent.

**[UPDATED, 2026-07-14 — issue #497]** There is no per-tenant active-cube limit. The
`MaxCubesPerTenant` constant, `Registry.maxCubes` field, and `ErrLimitReached` error type were
REMOVED outright (this project's no-backward-compat convention — no deprecated-but-kept field, no
compat shim); `addEntry`/`AddEntry` no longer take a `maxCubes` parameter. See `NOTE-CUBE-029`.

**Back-ref:** `internal/modules/cube/registry.go:Registry`; `internal/modules/cube/entry_store.go:entryStore,blobEntryStore,EntryStore,externalEntryStoreAdapter,NewRegistryFromEntryStore`.

---

## SPEC-CUBE-015: CreationTrigger — first-query cube-creation path (issue #446)

**Invariant:** `CreationTrigger.TryCreate` registers a new cube for `(tenant, dims, filters)`
on the first query that pattern receives, gated by the cardinality gate (`SPEC-CUBE-013`)
against caller-supplied VCNT data. On success the cube is added to the `Registry` via
idempotent conditional-PUT (`SPEC-CUBE-014`); a `TriggerResult.Created` flag distinguishes
"this call registered it" from "another concurrent caller already had." `TryCreate` never
blocks the query response — the caller is responsible for any async forward-ingest/backfill
after a successful create. Returns `*CardinalityError` on gate rejection, or a wrapped storage
error otherwise.

**[UPDATED, 2026-07-14 — issue #497]** There is no per-tenant active-cube limit gate. Active
cube count per tenant is unbounded — `TriggerConfig.MaxCubesPerTenant` and the
`*ErrLimitReached` return were REMOVED outright, not kept as a dead/unused field. See
`NOTE-CUBE-029`.

**Addendum (2026-07-08, issue #491, task E-4/E-7):** `TryCreate` gains an `aggAttrs
[]AggAttrDef` parameter and calls `validateDefinition(Definition{AggAttrs: aggAttrs})` as its
literal first step — before any registry I/O or `ComputeCubeID` call (a pure structural
precondition, independent of registry state; see `NOTE-CUBE-018`). Returns `*DefinitionError`
when `aggAttrs` omits `duration`. `RegistryEntry.AggAttrs` is now populated from the validated
`aggAttrs` at creation time (previously always empty from this path). The cardinality gate call
now also passes `aggAttrs` through for the byte-cost check (`SPEC-CUBE-013`'s addendum).

**Housekeeping note:** `backfill.go`'s own code comment currently also cites `SPEC-CUBE-015` — a
pre-existing collision with this entry, both dating to the original 2026-06-29 commit. This entry
(`CreationTrigger`) is the correct, authoritative one for "015"; `Backfiller`'s own contract is
documented separately at `SPEC-CUBE-026`. The code comment should be updated to cite
`SPEC-CUBE-026` when convenient, not treated as a landing blocker.

**Back-ref:** `internal/modules/cube/trigger.go:CreationTrigger,TryCreate`

---

## SPEC-CUBE-016: Rollup — multi-file merge to a coarser resolution (issue #448)

**Invariant:** `Rollup` merges cells from any number of already-open `RollupInput` readers into
a single sorted `[]MergedCell` at `targetLevel` resolution (`1`, `60`, or `1440` minutes per
bucket, `SPEC-CUBE-003`). Merge is an exact sum: cells from different inputs sharing the same
`(bucketMinute, dim1Value, dim2Value)` triple after minute-bucketing (`minute/targetLevel*
targetLevel`) have their counts added — no approximation. Dimension IDs are resolved to their
string values via each input's own dictionary (`SPEC-CUBE-002`) before merging, since different
input files have independently-numbered dictionaries. `RollupToWriter` is a convenience that
feeds the merged result into a fresh `Writer` (`SPEC-CUBE-010`) ready to flush. Empty `inputs`
or an input with `Reader == nil` is skipped, not an error; zero total cells returns `(nil, nil)`.

**Addendum (2026-07-08, issue #491, task E-5):** `MergedCell` gains `AggAttrs []AggAttrValues` —
reuses `AggAttrValues` (`cell.go`) directly rather than a separate merged-value type (see
`NOTE-CUBE-016`). Per-aggAttr merge rule: `SampleCount`/`Sum` are additive; `Min`/`Max` are
pairwise; `Buckets` are element-wise additive across the fixed 64-slot axis — the bucket axis is
level-independent, so no rebucketing is ever needed regardless of `targetLevel`. An input's
aggAttr with `SampleCount==0` contributes NOTHING to the running merge (guards against leaking the
`Min`/`Max` sentinel-init values, `SPEC-CUBE-021`); after merging all inputs, any aggAttr whose
`SampleCount` is still 0 has its `Min`/`Max` explicitly reset to 0 in the output — the sentinel
values never leak into a `MergedCell`. `Count`/`Sum`/`Min`/`Max`/`Buckets` are all associative and
order-independent across rollup levels: `L0→L1→L2` produces the same result as `L0→L2` directly
(verified by `TestRollup_L0ToL1ToL2_MatchesL0ToL2Direct`, a genuine 3-input grouping test).
`Reader.GetCellsRange` (the rollup-internal convenience wrapper) now returns `[]AggCell` (was
`[]Cell` pre-task-#40) — same function name and call sites, just carrying the type-unified shape.

**Addendum (2026-07-08, issue #491 Phase E fix pass, go-presubmit.md #3):** `RollupL0`/`RollupL1`/
`RollupL2` are now re-exported from `cube_ingest.go` as `CubeRollupL0`/`CubeRollupL1`/
`CubeRollupL2` (plain `uint32` constants, not the internal `RollupLevel` type, so tempo's existing
uint32-typed local constants can reference them without a type-conversion ripple at every use
site) — tempo's `cube_scheduler.go`/`cube_compactor.go` previously duplicated these three literal
values as independently-maintained unexported constants with no compile-time link back to this
package's canonical definition.

**Back-ref:** `internal/modules/cube/rollup.go:Rollup,RollupToWriter,NewRollupInput,Reader.GetCellsRange`

---

## SPEC-CUBE-017: QueryRouter — route a query pattern to the best matching cube (issue #449)

**Invariant:** `QueryRouter.Route(tenant, dims, filters, requestedResolution, watermarkMinute,
queryMaxMinute)` performs zero I/O — it matches against a pre-loaded `[]RegistryEntry`
snapshot the caller supplies. A cube matches when its deterministic `CubeID`
(`ComputeCubeID(tenant, dims, filters)`, `SPEC-CUBE-012`) equals the query's own computed ID —
i.e. exact tenant+dims+filters identity, never a superset/subset or best-effort match.
`ResolutionLevel` snaps a requested minutes-per-bucket value DOWN to the coarsest available
rollup level not exceeding it (`≥1440→L2, ≥60→L1, else L0`). On a match, `RoutingResult`
reports `CubeMinMinute`/`CubeMaxMinute` (the watermark-bounded coverage window) so the caller
knows to use the value-index fallback for minutes below `CubeMinMinute`. `Found=false` signals
no matching cube — the caller both falls back to a full scan for the current query AND may use
the miss to trigger cube creation (`SPEC-CUBE-015`).

**Addendum (2026-07-08, issue #491, task E-6b):** `Route`'s signature gained a `neededAttr
string` parameter and its matching/decision logic gained two new invariants — see
`SPEC-CUBE-023` for the full contract.

**Back-ref:** `internal/modules/cube/router.go:QueryRouter,Route,ResolutionLevel`

---

## SPEC-CUBE-018: Compactor — L0 merge, L1/L2 rollup planning, execution, and eviction (issues #450/#453)

**Invariant:** `Compactor` performs three independent, file-level-only operations (no per-span
S3 reads — only reads/writes whole cube files via the caller-supplied `FileStore`):
1. `PlanL0Merge` groups small L0 files by their `MinMinute`'s hour bucket and emits a
   `CompactionPlan` for any hour with `≥ L0MergeThreshold` (default 60) files. **Amended
   2026-07-08 (issue #491 Phase E fix pass, review.md Issue 6):** a file whose OWN span already
   straddles an hour boundary (`MinMinute` and `MaxMinute` fall in different hour buckets — only
   possible for a previously-merged L0 output, never a fresh per-minute flush) is now EXCLUDED
   from grouping entirely, never placed by its start hour as the original design here described.
   Including such a file would produce (or perpetuate) a merged output that itself straddles the
   boundary, which `PlanL1Rollup`'s strict per-hour containment check (below) can then never
   select for either hour it touches — stranding that hour at L0 forever with no operator-visible
   signal. A straddling file is left un-merged (still individually queryable at minute
   resolution — no data loss). Because every file placed into a group is now fully contained
   within one hour, every future merge output is too — this fix is self-correcting.
2. `PlanL1Rollup` selects all L0 files whose full range falls within one hour and returns them
   as inputs for an L1 rollup, or `(nil, false)` if none exist.
3. `Execute` reads a plan's input files, merges via `RollupToWriter` (`SPEC-CUBE-016`), writes
   the single output file, then deletes all inputs — an empty post-merge result (all inputs
   canceled out or contained no overlapping cells) deletes inputs without writing an output.
4. `Evict`/`ShouldEvict` remove a cube's files and registry entry once its last-queried
   timestamp exceeds `EvictionWindowDays` (default 30) — full teardown, not a soft-delete.

**Addendum (2026-07-08, issue #491, task E-12a):** `CompactorConfig.L0RetentionMinutes` (default
180) bounds how long an L0 file survives after being rolled into L1 before `EvictAgedL0` may
delete it. `RegistryEntry.Watermarks map[uint32]ResolutionWatermark` (keyed by `RollupLevel` —
1/60/1440) tracks, per resolution, the `[MinMinute, MaxMinute]` window that level is COMPLETELY
covered — populated by `Compactor.Execute` on every successful rollup write via
`Registry.UpdateWatermarks` (same conditional-PUT retry discipline as `Add`/`Remove`; EXPANDS the
existing range rather than replacing it — callers supply monotonically-extending, adjacent
windows; gap detection is the router's query-time concern, not this write-time bookkeeping).

`Execute`'s deletion rule: an L0-to-L0 merge (`plan.Level == RollupL0`, `PlanL0Merge`'s output)
deletes its inputs immediately (no resolution change). An L0→L1 rollup (`plan.Level == RollupL1`)
is the ONLY retention-decoupled case — its L0 inputs survive until `EvictAgedL0` confirms they are
both past `L0RetentionMinutes` and covered by the just-written `Watermarks[RollupL1]`. An L1→L2
rollup (`plan.Level == RollupL2`) deletes its L1 inputs immediately — retention is L0-specific and
never extends to L1. `EvictAgedL0(ctx, cubeID, file, nowMinute)` enforces both conditions
(`nowMinute - file.MaxMinute >= L0RetentionMinutes` AND `Watermarks[RollupL1]` fully covers the
file's range) before deleting; a non-L0 file passed to it is a caller error (returns an error, not
a silent no-op).

**Back-ref:** `internal/modules/cube/compactor.go:Compactor,PlanL0Merge,PlanL1Rollup,Execute,EvictAgedL0,Evict,ShouldEvict`

---

## SPEC-CUBE-019: Log2 histogram bucketing — dependency-free port of tempo's Log2Bucketize/Log2QuantileWithBucket (issue #491)

**Invariant:** `Log2Bucketize`, `BucketIndex`, `BucketMax`, and `Log2QuantileFromBuckets` are a
byte-for-byte port of tempo's `pkg/traceql.Log2Bucketize`/`Log2QuantileWithBucket`
(`tempo/pkg/traceql/engine_metrics.go:2135-2145,2154-2219`), kept dependency-free by design — the
`cube` package must never import `tempo`. `BucketCount=64`: slot 0 is always zero (the smallest
non-excluded boundary is `2^1=2`, matching `Log2Bucketize`'s own `v<2` exclusion, which returns
the `-1` sentinel); slots 1-63 hold counts for boundary `2^k`. `Log2Bucketize(v)` returns the
ceiling power-of-two boundary for `v`, or `-1` when `v<2` (excluded from any histogram entirely)
**or when `v>=2^63+1`** (the true ceiling boundary would be `2^64`, which overflows `uint64`'s
`1<<64` to `0` per Go's defined shift-count-`>=`-width semantics rather than any in-range value;
this is a deliberate, intentional divergence from tempo's own `Log2Bucketize`, which does not
guard this case — see NOTE-CUBE-028). `BucketIndex`/`BucketMax` are exact inverses of each other
over the valid `[1,63]` slot range.
`Log2QuantileFromBuckets(p, buckets)` walks buckets in ascending index order accumulating counts
until `ceil(p*total)` samples (minimum 1) are consumed, returning `(0,-1)` for an invalid `p`
(NaN/<0/>1) or all-empty buckets, an exact `BucketMax(bucket)` when the accumulated count lands
exactly on `maxSamples`, and an exponentially-interpolated value between the containing bucket's
max and the prior bucket's boundary otherwise.

**Back-ref:** `internal/modules/cube/bucket.go:Log2Bucketize,BucketIndex,BucketMax,Log2QuantileFromBuckets,BucketCount`

---

## SPEC-CUBE-020: Cube identity — aggAttrs joins CubeID unconditionally (issue #491, ruling 3)

**Invariant:** `ComputeCubeID(tenant, dimensions, filters, aggAttrs) string` computes an 8-byte hex
CubeID over FOUR segments: `hex(SHA256(computeDimsFiltersKey(tenant,dims,filters) + "\x00" +
sorted(aggAttrs))[:8])`. The `aggAttrs` segment joins identity UNCONDITIONALLY — there is no
empty-aggAttrs special case; `aggAttrs` can never legitimately be empty (`duration` is always
present in every cube's attribute set, ruling 3), so no code path produces the old pre-#491
3-segment-only hash anymore (see `SPEC-CUBE-012`'s addendum). `computeDimsFiltersKey(tenant, dims,
filters) string` is the extracted, exact 3-segment hash that was `ComputeCubeID`'s entire body
before this change — it is the single source of truth both `ComputeCubeID` and the router's
superset-matching grouping key (task E-6b, `SPEC-CUBE-023`) build on, so the two can never
independently drift apart. `RegistryEntry` gains `AggAttrs []string` (`json:"agg_attrs,omitempty"`)
— the SET of materialized aggregate-attribute columns; the aggregation FUNCTION set
(count/sum/min/max/buckets, computed uniformly for every attribute in the set) does NOT join
identity, only the attribute-name set does. A cube's `AggAttrs` is fixed for its entire life
(never grows after creation).

**Vetoed design, recorded for future readers (team-lead ruling, second-round correction):** an
earlier plan draft special-cased `len(aggAttrs)==0` to reproduce the exact pre-#491 3-segment hash
byte-for-byte, framed as preserving existing pure-count cubes' `CubeID` identity across the
upgrade. **Rejected** for two independent reasons: (1) this project's no-backward-compat rule
(reaffirmed by #490's flat, no-fallback removal of the old trace-by-id format) forbids designing
compat shims for old data at all — there is no live legacy cube data whose identity needs
preserving; (2) even independent of that rule, the input the shim was meant to trigger on
(`aggAttrs` empty) can never legitimately occur in the finished system, since `duration` is
mandatory in every cube's attribute set — a special case guarding an input that cannot occur is a
compat shim with nothing left to be compatible with. `ComputeCubeID`'s hash is simply the
4-segment computation, always, with no degenerate case.

**Back-ref:** `internal/modules/cube/definition.go:ComputeCubeID,computeDimsFiltersKey,RegistryEntry.AggAttrs`

---

## SPEC-CUBE-021: Wire format v2 — Header.NumAggAttrs and per-aggAttr records (issue #491, E-3)

**Invariant:** `VersionCube` is 2 (bumped from 1). The 36-byte `Header` (`SPEC-CUBE-003`)
repurposes the previously-reserved `buf[5]` byte as `NumAggAttrs uint8` — the count of per-aggAttr
540-byte records appended to every `AggCell` (`SPEC-CUBE-001`) in the file's chunks, in
`RegistryEntry.AggAttrs` order (attribute identity/order comes from the registry, never
self-declared per-file — `SPEC-CUBE-020`). `DecodeHeader` rejects any file whose `Version !=
VersionCube` with a typed error (`NOTE-CUBE-013`) — there is no v1 read path.

`AggAttrValues` (`AggAttrRecordWidth = 4+8+8+8+512 = 540` bytes: `SampleCount(4) Sum(8,float64)
Min(8,float64) Max(8,float64) Buckets(512 = 64×uint64)`) is the fixed-width per-aggAttr record.
The full per-cell record width is `recordWidthFor(numAggAttrs) = 12 + numAggAttrs*540` (`chunk.go`)
— the single place this arithmetic is computed; any future consumer (e.g. the cardinality gate's
byte-cost accounting, task E-7) must call this function rather than re-deriving the formula.

`SampleCount == 0` in an `AggAttrValues` means no valid sample was ever observed for that
attribute at that cell — the signal downstream merge/response code must check before trusting
`Min`/`Max` (sentinel-initialized to `±math.MaxFloat64`, must not leak into output when
`SampleCount == 0`; `SPEC-CUBE-016`'s addendum documents how the rollup merge handles this).

`NumAggAttrs == 0` (the pure-count case, `AggCell.Aggs` empty) is BYTE-IDENTICAL to the pre-#491
fixed-12-byte format.

**Back-ref:** `internal/modules/cube/file.go:Header,EncodeHeader,DecodeHeader,VersionCube`,
`internal/modules/cube/cell.go:AggAttrValues,AggAttrRecordWidth`,
`internal/modules/cube/chunk.go:recordWidthFor`

---

## SPEC-CUBE-022: Unified chunk codec and Reader accessor naming (issue #491, E-3 APPENDIX 2)

**Invariant:** `EncodeChunk`/`DecodeChunk`/`SplitCellsIntoChunks` (`chunk.go`) operate on
`[]AggCell` — there is exactly ONE chunk codec path, not two. (An earlier design, since revised,
had a separate plain-`[]Cell` pair alongside an `EncodeAggChunk`/`DecodeAggChunk` full-fidelity
pair; both were unified into one path once `AggCell` became the sole cell type — `SPEC-CUBE-001`.)
`DecodeChunk` uses `recordWidthFor(numAggAttrs)` to compute the correct byte stride; `EncodeChunk`
validates every cell's `len(Aggs) == numAggAttrs` before encoding.

Reader-side, `GetCell`/`GetCellsInRange` and `GetAggCell`/`GetAggCellsInRange` are two NAME PAIRS
over the SAME underlying data — `GetCell` extracts just `.Count` from `GetAggCell`'s full
`AggCell` result; `GetCellsInRange` is now a direct one-line delegation to `GetAggCellsInRange`
(identical `[]AggCell` return type). The narrower-sounding names are kept alive as a courtesy to
existing call sites (avoiding a purely mechanical rename ripple into unrelated in-flight tasks),
not because they return a different or narrower type anymore — callers should not assume
`GetCellsInRange`'s result lacks aggAttr data.

**Back-ref:** `internal/modules/cube/chunk.go:EncodeChunk,DecodeChunk,SplitCellsIntoChunks`,
`internal/modules/cube/reader.go:GetCell,GetAggCell,GetCellsInRange,GetAggCellsInRange`

---

## SPEC-CUBE-023: Route — superset tie-break and resolution-completeness-or-decline (issue #491, E-6b, rulings 3/4(b))

**Superset tie-break invariant:** `Route(tenant, dims, filters, neededAttr, ...)` matches
candidate cubes via `computeDimsFiltersKey` (the same 3-segment hash `ComputeCubeID` uses,
`SPEC-CUBE-020`), NOT exact `CubeID` equality — because `AggAttrs` now varies independently
within one `(dims, filters)` grouping. `neededAttr == ""` matches any candidate regardless of its
attribute set (`count_over_time`/`rate` need no specific attribute); a non-empty `neededAttr`
filters to candidates whose `AggAttrs` contains it. When multiple candidates match, `Route`
deterministically prefers the SMALLEST `AggAttrs` set that still covers `neededAttr`
(`smallestSupersetNewest`), breaking ties by newest `CreatedAt`.

**Resolution-completeness-or-decline invariant:** once `ResolutionLevel` picks a target
resolution, `Route` verifies the chosen cube's `Watermarks[level]` (`SPEC-CUBE-018`'s addendum)
COMPLETELY covers `[watermarkMinute, queryMaxMinute]` before returning `Found=true`. A missing
watermark for that level, or one that only PARTIALLY covers the requested window, declines the
WHOLE query (`Found=false`) — never a partial/mixed-resolution answer. This is a genuine behavior
change from the pre-E-6b `Route`, which returned `Found=true` on any `CubeID` match regardless of
actual data coverage.

**Addendum (2026-07-08, issue #491 Phase E fix pass, review.md Issue 5):** `RoutingResult` no
longer carries `CubeMinMinute`/`CubeMaxMinute` fields. They were added when `Route` still modeled
partial coverage (serve the cube for its own covered window, fall back to the value index for
earlier minutes) — a model this same spec entry's resolution-completeness-or-decline invariant
above already replaced. Confirmed via grep that the only real caller of `Route`
(`tempo/tempodb/encoding/vblockpack/cubequerypath.go`) never read either field, so they were
removed outright rather than re-documented.

**Back-ref:** `internal/modules/cube/router.go:Route,smallestSupersetNewest,containsString,RoutingResult`

---

## SPEC-CUBE-024: ValidateFileMatchesRegistry / AggAttrsMismatchError — registry-vs-file consistency check (issue #491, E-6b, APPENDIX 3)

**Invariant:** `ValidateFileMatchesRegistry(fileNumAggAttrs uint8, entry RegistryEntry) error` is
a pure, I/O-free comparison between an already-OPENED cube file's decoded
`(*Reader).NumAggAttrs()` and its own `RegistryEntry.AggAttrs` count — answering "does this
EXISTING file match its EXISTING registry entry" at READ time. This is a DIFFERENT failure class
from `SPEC-CUBE-023`'s validation and from E-4's `validateDefinition`/`NOTE-CUBE-015`:
`validateDefinition` gates NEW cube creation before any file exists and cannot answer a
registry-vs-file drift question even in principle (it has no access to an already-written file's
bytes). Returns `*AggAttrsMismatchError` (`CubeID`, `FileNumAggAttrs`, `RegistryAggAttrsCount`) on
a count mismatch — modeled on this codebase's existing `T1b`/`NOTE-VI-078` index-vs-data-
inconsistency error family (a real mismatch fails loudly, never silently parsed around).

**Explicit scope limit:** this phase implements the COUNT check only
(`fileNumAggAttrs != len(entry.AggAttrs)`), not attribute IDENTITY. The wire format does not carry
per-file attribute names — `SPEC-CUBE-020`'s ruling-3 fixed-for-life design keeps attribute names
in the registry only, never self-declared per-file. A future phase adding per-file attribute-name
self-description could naturally extend this check to compare identities, not just counts.

**Back-ref:** `internal/modules/cube/definition.go:ValidateFileMatchesRegistry,AggAttrsMismatchError`,
`internal/modules/cube/reader.go:NumAggAttrs`

---

## SPEC-CUBE-025: Accumulator per-aggAttr computation — SpanValues.Float64, sentinel init, mandatory duration (issue #491, E-4)

**Invariant:** `SpanValues` gains `Float64(column string) (float64, bool)` — the single numeric-
extraction method every aggAttr computation uses (`Sum`/`Min`/`Max`/`Avg` via the raw float64;
`Buckets` via `uint64(v)` for `AggAttrTypeInt64`-typed attrs only, per ruling 1's type-gated
scope). `NewAccumulator(def, minute) (*Accumulator, error)` validates `def` via
`validateDefinition` before construction — returns `*DefinitionError` when `def.AggAttrs` omits
`DurationColumn`. `Add` extracts each `Definition.AggAttrs` entry via `Float64`; a span missing a
given attribute simply does not update that attribute's state for this call (the base `Count`
still increments as long as both dimensions are present and all filters pass) — there is no
partial-attribute error. For a present value: `SampleCount` increments, `Sum` accumulates, `Min`/
`Max` update via direct comparison, and for `AggAttrTypeInt64` attrs with `v >= 0`, the matching
`Log2Bucketize` bucket increments (skipped when the value is `<2` after the `uint64` cast — the
`-1` sentinel — or negative, since a negative value never validly casts to `uint64`).

`AggAttrValues.Min`/`Max` are sentinel-initialized to `math.MaxFloat64`/`-math.MaxFloat64`
(`newCellAggState`) so the first real sample always wins the initial comparison. `SampleCount==0`
is the signal that no valid sample was ever observed for that attribute at that cell — downstream
code (rollup merge, response mapping) MUST check this before trusting `Min`/`Max`, to avoid
leaking the sentinel values into output.

**Back-ref:** `internal/modules/cube/accumulator.go:SpanValues,NewAccumulator,Add,addAggAttrs,newCellAggState,validateDefinition,DefinitionError`

---

## SPEC-CUBE-026: Backfiller — historical population from the value index (issue #491, E-8)

**Invariant:** `Backfiller` processes one cube's historical backfill newest→oldest from the value
index (no data-block reads), writing one cube file per minute via the existing `Accumulator`
(`SPEC-CUBE-011`). `Definition.AggAttrs` is built from the registry entry's real `AggAttrs`
(`[]string` of column names, `SPEC-CUBE-020`) via `aggAttrDefsFor`, instead of a hardcoded
duration-only placeholder — `aggAttrDefsFor` types `DurationColumn` as `AggAttrTypeInt64` and
every other column as `AggAttrTypeFloat64` (a bare column name carries no type information, see
`NOTE-CUBE-017`). `lookupAggAttrValues` issues one additional `ValueIndexSource.LookupColumn`
call per aggAttr column not already covered by a dimension, joining results back to each span by
`(TraceID, SpanID)` — the same join-key pattern the pre-existing dim1/dim2 code uses.
`BackfillWatermark` tracks the oldest fully-backfilled minute; the querier uses cube files for
minutes ≥ watermark, the value index for earlier minutes. An idle minute (no data) writes nothing
(sparse cube).

**Housekeeping note:** `backfill.go`'s own code comment currently cites `SPEC-CUBE-015` (a
pre-existing collision with `trigger.go`'s own, correct `SPEC-CUBE-015` citation, both dating to
the same original commit) — this is the file's real, authoritative entry; the code comment should
be updated to cite `SPEC-CUBE-026` when convenient, not treated as a landing blocker.

**Addendum (2026-07-08, issue #491 Phase E fix pass, review.md Issue 3):** `aggAttrDefsFor` was
renamed/exported as `AggAttrDefsFor` (see SPEC-CUBE-012's addendum below) so
`CubeRegistryEntryToDefinition` (cube_ingest.go) can reuse it. `processMinute`'s `Definition` now
also sets `Filters` from `b.entry.Filters`, converted via the new `ColumnFilterToFilter`
(SPEC-CUBE-027) — previously `Filters` was never set, so historical backfill of a filtered cube
(#480) silently counted every span matching the cube's dimensions, ignoring the filter entirely.
`valueIndexSpanValues.Int64` — previously an unconditional `(0, false)` stub — now parses
`SourceRef` as an integer the same way `Float64` already parses it as a float; this was a
necessary companion fix, since `NumericFilter`-based predicates read exclusively via `Int64`, and
without it every numeric filter would have rejected every span once `Filters` started being
populated (an empty-but-not-obviously-wrong backfill instead of a correct one).

**Back-ref:** `internal/modules/cube/backfill.go:Backfiller,BackfillWatermark,AggAttrDefsFor,lookupAggAttrValues,viEntryKey,valueIndexSpanValues.Int64`

---

## SPEC-CUBE-027: ColumnFilterToFilter — single source of truth for applying a cube's baked-in filters to real span data (issue #491 Phase E fix pass, review.md Issues 1/2/3, 2026-07-08)

**Invariant:** `CubeRegistryEntryToDefinition` (cube_ingest.go) now copies `entry.AggAttrs` into
the returned `Definition.AggAttrs` via `AggAttrDefsFor` (previously omitted entirely — since
`tempo/tempodb/encoding/vblockpack/cubemanager.go`'s `loadDefs` is the only real production caller
of `LoadCubeDefinitions`/`CubeRegistryEntryToDefinition`, and `NewCubeAccumulator`'s
`validateDefinition` requires `DurationColumn` present in `AggAttrs` (SPEC-CUBE-025), this omission
meant EVERY real cube failed validation and was silently dropped by `filterValidCubeDefs` — forward
ingest never activated any cube in production).

`ColumnFilterToFilter(cf ColumnFilter) Filter` is the single source of truth for converting a
`RegistryEntry`-persisted `ColumnFilter` into a runtime `Filter` predicate, shared by BOTH real code
paths that apply a cube's baked-in filter to real span data: forward ingest (tempo's
`cubemanager.go` passes it directly as `LoadCubeDefinitions`' `filterFn` parameter — previously
always `nil`, so `def.Filters` was always empty in production) and backfill
(`Backfiller.processMinute`, SPEC-CUBE-026's addendum — previously never read `entry.Filters` at
all). A numeric threshold (via `NumericFilter`) is preferred when `cf.Value` parses as one — as an
`int64`/`int`/`float64` directly, or (the real production shape, since
`cubequerypath.go`'s `extractFilters` always encodes the operand as a string via
`traceql.Static.EncodeToString(false)`) as a string parseable via `strconv.ParseInt`, then
`time.ParseDuration(...).Nanoseconds()`, then `strconv.ParseFloat`. When no numeric parse succeeds
and `cf.Op == DefFilterOpEQ`, this falls back to a new `StringFilter(column, value string) Filter`
(accumulator.go, sibling to `NumericFilter`) — string equality via `SpanValues.String`. Any other
combination (a GT/GTE/LT/LTE op paired with a non-numeric-parseable value) is unrepresentable and
returns `nil`; callers skip a `nil` filter rather than treating it as fatal, matching this
package's existing "skip malformed individual entries" tolerance.

**Why fixing AggAttrs alone would have been WORSE, not better:** if `CubeRegistryEntryToDefinition`
had only gained the `AggAttrs` copy without `ColumnFilterToFilter` also being wired in at the same
time, every filtered cube (#480) would have started silently counting UNFILTERED spans the moment
forward ingest began activating cubes — a silently-wrong-answer regression of exactly the class
#480 was designed to prevent, just via ingest instead of query routing. Both fixes shipped together
for this reason.

**Back-ref:** `internal/modules/cube/definition.go:ColumnFilterToFilter,defOpToFilterOp,numericFilterValue`, `internal/modules/cube/accumulator.go:StringFilter`, `cube_ingest.go:CubeRegistryEntryToDefinition,CubeColumnFilterToFilter,NewCubeStringFilter`

## SPEC-CUBE-028: `Route` serves the covered sub-range instead of declining on partial coverage (issue #217, ruling 4(b) revisit)

*Added: 2026-07-13*

Supersedes ruling 4(b)'s original all-or-nothing contract (`SPEC-CUBE-023`/E-6b). `RoutingResult`
gains `CoveredMinMinute`/`CoveredMaxMinute uint32`, meaningful only when `Found=true`. `Route`
now returns `Found=false` ONLY when the chosen resolution level has no watermark entry at all, or
its watermark's covered range has NO overlap with `[watermarkMinute, queryMaxMinute]`. Any
non-empty overlap returns `Found=true` with `CoveredMinMinute`/`CoveredMaxMinute` set to that
overlap — equal to the full requested window when coverage is complete, so callers have one code
path regardless of whether coverage is partial or complete.

**Scope boundary (unchanged from the original ruling 4(b)):** a genuine INTERIOR gap in a single
resolution level's own coverage (covered, then a hole, then covered again) is NOT representable
by the single `[MinMinute,MaxMinute]` `ResolutionWatermark` pair and remains out of scope — see
`NOTE-CUBE-026`. Mixed-resolution stitching (Decision 1, tie-break across levels) is untouched.

Back-ref: `internal/modules/cube/router.go:RoutingResult,Route`. See `NOTE-CUBE-026`. Issue #217.

## SPEC-CUBE-029: consumer's PARTIAL answer is a fallback-of-last-resort, not an automatic final answer (2026-07-13 follow-up, does not amend SPEC-CUBE-028)

*Added: 2026-07-13.* This entry constrains a CONSUMER of `Route`'s result (`Route` itself,
`SPEC-CUBE-028`, is unchanged), recorded here since the consumer lives in tempo (out of this
repo's own build) but the invariant belongs with the spec it depends on.

When `Route` returns `Found=true` with `CoveredMinMinute`/`CoveredMaxMinute` narrower than the
requested window (edge-truncated partial coverage), the caller MUST NOT treat that partial answer
as automatically final. The caller must first attempt any other always-available, zero-cube-
dependency answering path for the full requested window (tempo: the VI/scan metrics path,
`blockpack.ExecuteMetricsTraceQL`) and use it if it succeeds; the cube's own partial answer is
used only once that other path also declines or errors for the window — mirroring the existing
`Found=false`/`ErrCubeWarming` fallback precedence exactly, so both "no coverage at all" and
"partial coverage" degrade through the SAME fallback-of-last-resort discipline rather than the
former falling through and the latter short-circuiting.

**Known limitation carried over from `SPEC-CUBE-028`, not fixed by this entry:** the covered/
uncovered boundary this spec narrows to is watermark-derived (whole minutes), with no relationship
to the query's own `Step` alignment. A step wider than one minute whose window straddles that
boundary can be computed from fewer minutes than its full width with no per-step signal
distinguishing it from a fully-covered step. See `NOTE-CUBE-027` for the full rationale for
documenting rather than fixing this in the same pass.

Back-ref: tempo-side `tempodb/encoding/vblockpack/backend_block.go:QueryRange`,
`tempodb/encoding/vblockpack/cubequerypath.go:tryQueryFromCube`. See `NOTE-CUBE-027`. Issue #217,
review `tempo/.bob/state/217-review.md`.

## SPEC-CUBE-030: `Reader.BytesRead` — exact encoded byte count, captured at open time (issue #218, Phase 5)

*Added: 2026-07-13.*

**Invariant:** `Reader.BytesRead() int64` returns exactly `len(data)` of the byte slice the
`Reader` was opened from — set once, at construction, by both `OpenReader` (the file's on-disk
size, via `os.ReadFile`) and `OpenReaderFromBytes` (the caller-supplied in-memory slice's length,
e.g. an S3 GET response body). This is a plain stored field (`bytesRead int64`), not a
recomputation — there is exactly one place per constructor that sets it, matching this package's
existing single-source-of-truth convention for constructor-time invariants.

**Why this exists:** issue #218 attributes a query's total bytes-read cost across its per-source
categories (VI/VCNT/cube/data-file). A cube-answered query previously had no way to report how many
bytes its own cube file cost to read — `len(data)` was discarded immediately after decoding in both
`OpenReader`/`OpenReaderFromBytes`. `BytesRead()` is net-new capture, not a threading of an
already-computed value (unlike the other three #218 categories, which already existed as tempo-side
locals — see the #218 plan's §A3).

**Back-ref:** `internal/modules/cube/reader.go:Reader.BytesRead,OpenReader,OpenReaderFromBytes`.
Tempo-side consumer: `tempodb/encoding/vblockpack/cubequerypath.go` (captures the value at the
point the cube reader is opened, sets `SearchMetrics.CubeBytesRead`). Issue #218.

## SPEC-CUBE-031: Native Postgres EntryStore must be behaviorally identical to the blob-backed implementation (issue #506)

*Added: 2026-07-15*

**Invariant:** A native Postgres `EntryStore` implementation (`cube.PgEntryStore`) MUST be
behaviorally identical to the blob-backed implementation (`blobEntryStore`) for every `Registry`
public method (`Load`/`Add`/`Remove`/`UpdateWatermarks`) — backend choice never changes
`Registry`'s observable contract, per SPEC-CUBE-014's own `[UPDATED]` design intent.
`PgEntryStore.AddEntry` is idempotent on an already-present `CubeID` (mirrors
`blobEntryStore.addEntry`'s contract, SPEC-CUBE-014); no per-tenant cube-count limit is enforced
(issue #497, NOTE-CUBE-029 — unbounded on both backends). `PgEntryStore.UpdateWatermarksEntry`
expands an existing watermark range (min-of-mins, max-of-maxes) exactly as
`blobEntryStore.updateWatermarksEntry` does, and errors if the CubeID is not found.

Verified end-to-end by `TestCubeRegistry_BlobAndPgBackends_IdenticalBehavior`
(`pg_blob_differential_test.go`) — runs the IDENTICAL operation sequence (3 Adds, 1 Remove, 2
UpdateWatermarks calls narrowing-then-widening, 1 duplicate Add) against both a blob-backed and a
Postgres-backed `Registry`, then asserts their resulting `Load()` sets are field-equal (CubeID,
Tenant, Dimensions, AggAttrs, Resolution, Watermarks) after sorting both by CubeID.

**Back-ref:** `internal/modules/cube/pg_entry_store.go`,
`internal/modules/cube/pg_blob_differential_test.go:TestCubeRegistry_BlobAndPgBackends_
IdenticalBehavior`. See NOTE-CUBE-030. Issue #506.

## SPEC-CUBE-032: `CubeQueryPath`/`RunCubeBackfill` orchestration contract, at blockpack root (issue #508)

*Added: 2026-07-16. ID-numbering caveat: confirmed via direct inspection of this file's own
highest existing ID (`SPEC-CUBE-031`) rather than the `blockpack_search_modules` MCP tool or a
spec-oracle agent scoped to this worktree — neither was reachable in this session.*

**Invariant:** The registry-cache/route/fan-out/rollup/creation-trigger orchestration that used to
live entirely in tempo (`tempodb/encoding/vblockpack/cubequerypath.go` + `cube_backfill.go`) now
lives at blockpack root as `CubeQueryPath` (constructor `NewCubeQueryPath` + method `QueryRange`)
and a standalone `RunCubeBackfill` function. This is a relocation of orchestration, not a new
architectural layer — the underlying `internal/modules/cube` package (`Registry`, `QueryRouter`,
`CreationTrigger`, `Backfiller`, `Rollup`) is unchanged by this move.

- **No blockpack-side singleton.** `NewCubeQueryPath` returns a plain `*CubeQueryPath` value.
  Tempo's existing `sync.Once`-guarded `ConfigureCubeQueryPath`/`getCubeQueryPath` wrapper is
  unchanged and now simply constructs blockpack's type inside it.
- **Creation-cooldown semantics:** `CubeQueryPathConfig.CreateCooldown` (default 1 minute) rate-
  limits `TryCreate` re-attempts to at most once per `(tenant, dims, filters)` key, exactly
  mirroring tempo's pre-move `createSeen` map — prevents a wide-fan-out query from firing a storm
  of concurrent creation attempts for the identical shape.
- **Backfill is caller-launched, not auto-spawned.** `QueryRange` never calls `RunCubeBackfill`
  itself. On every trigger attempt (`maybeCreateCube`), `CubeQueryPathConfig.OnCreateAttempt` (if
  non-nil) is invoked synchronously with `(entry, created, hasL0Watermark, triggerErr)` — the
  caller (tempo) decides whether/how to launch `RunCubeBackfill` (typically in its own goroutine,
  wrapped in its own metrics/logging). blockpack has zero logging/metrics dependencies by design
  and must never import tempo's `jobstore` package directly (mirrors `SPEC-CUBE-019`'s existing
  backend-agnostic-worker-coordination boundary).
- **Partial-coverage pass-through is verbatim.** `CubeQueryPathResult.PartialCoverage`/
  `CoveredMinMinute`/`CoveredMaxMinute` mirror `RoutingResult`'s own fields exactly (see
  `SPEC-CUBE-028`/`SPEC-CUBE-029`) — `QueryRange` narrows the actual cell read to the covered
  sub-range via `cubeCoveredWindow`, never reading cells outside a resolution level's confirmed-
  covered window, same as the pre-move tempo code.
- **File-discovery uses a narrow `Lister`, never `CubeFileStore.List`.** `NewCubeQueryPath` takes
  a separate `Lister` parameter (`List(ctx, prefix) ([]string, error)`) for its own raw
  prefix-listing fan-out; `CubeFileStore` is used only for `.Get`/`.Put`. Reusing
  `CubeFileStore.List` here would issue two GETs per unmerged L0 file (one inside `List`'s
  `readFileInfo` ranged-header-read fallback, one full GET moments later in the fetch loop) — a
  real I/O-cost regression against this repo's root `SPEC.md` "single I/O per object where
  possible" invariant, for exactly the common, high-file-count case a wide-time-range metrics
  query fans out over.
- **Cube-file S3 directory names are zero-padded to 16 bytes (32 hex chars), never the raw
  8-byte/16-hex-char registry `CubeID`.** `Accumulator.FlushTo`/`Filename` always encode the FULL
  `[16]byte` `Definition.ID` (see `cube_compactor.go`'s `cubeFileStore.List` comment: "Registry
  stores 16-hex-char IDs (8 bytes); S3 dirs use 32-hex-char (16 bytes, zero-padded)").
  `QueryRange` MUST zero-pad `RegistryEntry.CubeID` the same way before building its `Lister`
  listing prefix — using the unpadded registry `CubeID` directly (as the pre-move tempo code did)
  never matches any real file's actual directory and silently declines every cube query into
  "not found" forever. See `NOTE-CUBE-034` for the incident this was caught by.

**Back-ref:** `cube_query_path.go:CubeQueryPath,NewCubeQueryPath,QueryRange,loadEntries,
invalidateCache,maybeCreateCube,buildVCNTSection`, `cube_backfill_runner.go:RunCubeBackfill,
LoadCubeEntry,NewCubeVIBackfillSource`. See `NOTE-CUBE-031`, `NOTE-CUBE-032`, `NOTE-CUBE-034`.
Issue #508.

## SPEC-CUBE-033: `AllDimSentinel` — shared dimension placeholder for single- AND zero-dimension cubes (issue #508)

*Added: 2026-07-16. Same ID-numbering caveat as SPEC-CUBE-032.*

**Invariant:** `AllDimSentinel = "__all__"` (`definition.go`, root-aliased as
`blockpack.CubeAllDimSentinel`) is the ONE fixed placeholder value used whenever a cube
`Definition` has no real column for a dimension slot:

- A **single-dimension** cube's `Dim2Column` (no second dimension to key on).
- A **zero-dimension** (ungrouped) cube's `Dim1Column` AND `Dim2Column` (no dimension at all —
  every span accumulates into one shared cell).

Forward-ingest (`cube_ingest.go:CubeRegistryEntryToDefinition`) and backfill
(`backfill.go:Backfiller.processMinute`) MUST both use this exact constant for these slots, never
an independently-hardcoded literal — `CubeRollup` merges series by the literal dictionary-encoded
dim1/dim2 STRING value, so two different "no dimension" placeholder strings for the same logical
cube produce two distinct, un-merged series instead of one. See `NOTE-CUBE-033` for the real,
latent bug this closes (a `"_"` vs `"__all__"` mismatch between backfill- and forward-ingest-
written files for every single-dimension cube).

**Zero-dimension backfill is a documented, PERMANENT limitation, not a bug.**
`Backfiller.processMinute`'s `len(entry.Dimensions) == 0` guard (and `RunCubeBackfill`'s own
mirrored early-exit guard, `cube_backfill_runner.go`) both return a `*DefinitionError`
immediately, without retrying — never even attempting a single VI lookup. This is intentional:
`ValueIndexSource.LookupColumn` is fundamentally a per-attribute-value inverted-index lookup
("which spans have column X = value Y"); there is no "enumerate every span" VI query. A
zero-dimension cube has no dimension column to anchor such a lookup on, so there is no way to
reconstruct its historical per-minute counts from the value index at all — a structural
limitation of the mechanism, not something a different sentinel or extra plumbing would fix. A
zero-dimension cube therefore only ever accumulates data going FORWARD from its creation moment.

**Addendum (2026-07-16, issue #511): the "PERMANENT limitation" claim above is superseded — zero-dimension backfill now works.** The paragraph above is retained for history (per this file's own "never delete, mark superseded" convention) but its central claim is no longer true: `Backfiller.processMinute`'s `len(entry.Dimensions) == 0` guard, and `RunCubeBackfill`'s mirrored guard, were both REMOVED. A zero-dimension entry now backfills successfully via the new `processMinuteZeroDim` method. The blocking premise above — "there is no `enumerate every span` VI query" — was correct as stated but turned out not to be the dead end it looked like: every v2 cube's `AggAttrs` mandatorily includes `DurationColumn` (`SPEC-CUBE-025`/E-4), and `LookupColumn(DurationColumn, minSec, maxSec)` already returns one VI entry per span carrying a duration value in that window — which is, in practice, every span. `processMinuteZeroDim` anchors on that lookup as its span-enumeration source instead of a dimension column, and reuses the anchor entry's own `SourceRef` directly as the duration value (no separate aggAttr join needed for `DurationColumn` itself) — `lookupAggAttrValues` gained an `extraExcluded map[string]bool` parameter specifically so this anchor value is not redundantly re-fetched by a second lookup. This is a genuinely different mechanism from "a different sentinel or extra plumbing," which the superseded paragraph correctly ruled out — it is a new use of an EXISTING mandatory-attribute lookup as an implicit span enumerator, not a new VI query capability. The `AllDimSentinel` dual-use documented above (single-dimension `Dim2Column`; zero-dimension `Dim1Column`+`Dim2Column`) is unaffected and remains fully accurate. See `NOTE-CUBE-035` for the full design history (including two superseded design revisions) and the accepted, pre-existing colType-defaulting risk this fix inherits unchanged.

**Back-ref:** `definition.go:AllDimSentinel`, `cube_ingest.go:CubeAllDimSentinel,
CubeRegistryEntryToDefinition`, `backfill.go:Backfiller.processMinute`,
`cube_backfill_runner.go:RunCubeBackfill`. See `NOTE-CUBE-033`. Issue #508. Addendum back-ref (issue #511):
See `NOTE-CUBE-035`.

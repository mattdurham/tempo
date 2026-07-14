# valuecounts — Design Notes

Non-obvious design decisions, rationale, and invariants for the `internal/modules/valuecounts`
package (VCNT value-counts section, issue #400).

---

## NOTE-VC-001 — Signed counts express retention/compaction as delta accounting

Date: 2026-06-25

A VCNT `Record.Count` is a signed `int64`. Positive counts are introductions (a value appears
in N spans within a time window in this file); negative counts are accounting deltas — a prior
file's contribution being subtracted because the source block was deleted by retention or
superseded by a compacted output.

`Compact` groups by `(ColumnName, TimeStart, TimeEnd, Value)`, sums `Count`, and **drops any
group whose sum is <= 0**. This makes the count files self-healing under retention: the value
disappears from query results exactly when its net live count reaches zero, with no separate
tombstone state to track. The compactor emits `Negate(r)` for each consumed input block's
record so that values present in both input and output net to zero churn.

Back-ref: `Compact`, `Negate` in compaction.go / record.go.

---

## NOTE-VC-002 — TimeEnd is part of the merge key

Date: 2026-06-25

`mergeKey` includes both `TimeStart` and `TimeEnd`. Two records for the same value that share a
`TimeStart` but cover different windows are intentionally NOT merged — they describe different
time intervals and their counts are independent. Excluding `TimeEnd` from the key would conflate
a `[100,200]` window with a `[100,300]` window and corrupt time-bounded query results.

---

## NOTE-VC-003 — Snappy-chunked section with a (column, time_start) directory

Date: 2026-06-25

The VCNT section is split into independently-snappy-compressed chunks of up to
`shared.ValueCountsRecordsPerChunk` records. The chunk directory stores each chunk's first
`(MinColumn, MinTimeStart)` and byte extent, mirroring the value index VINX layout. A
time-bounded lookup (`DecodeTimeRange` / `ValuesInRange`) skips chunks whose `MinTimeStart`
exceeds the query's `maxTS`. Because records within a chunk can carry differing windows, a
per-record overlap test (`TimeStart <= maxTS && TimeEnd >= minTS`) is still applied after decode.

The directory is sorted by `(MinColumn, MinTimeStart)`, not `MinTimeStart` alone, so a
later-listed chunk for a *different* column may reset `MinTimeStart` to a small value. The skip
is therefore a `continue`, never a `break`.

Back-ref: `DecodeTimeRange` in section.go.

---

## NOTE-VC-004 — Top-N and cardinality share one summation pass over live values

Date: 2026-06-30

Issue #400 was reopened to add the tag-value autocomplete / dropdown primary use case
("top 10 values for column between T1 and T2") plus the cardinality gate for cube creation
(#445). `ValuesInRange`, `TopNInRange`, and `CardinalityInRange` all answer questions about the
same underlying set: the distinct values of a column whose net live count over the query window
is `> 0`. They are therefore implemented on a single unexported helper, `sumLiveValues`, which
decodes the overlapping chunks once, sums `Count` per value for the requested column, and drops
any value whose net count is `<= 0` (the same liveness rule as `Compact`, NOTE-VC-001).

The three public entry points differ only in how they shape that set:

  - `ValuesInRange` — sorts by `Value` ascending (stable listing / merge-friendly order).
  - `TopNInRange` — sorts by `Count` descending, **tie-broken by `Value` ascending** so the
    ranked result is deterministic for a given input, then truncates to `n` (n <= 0 => all).
  - `CardinalityInRange` — returns the count of live distinct values.

None of these open a blockpack data file — they read only the consolidated `.vcnt` section, so a
dropdown lookahead or a cube cardinality gate never touches a span. The deterministic tie-break
matters because two equally-frequent values must rank in a stable order across repeated queries
and across compaction (which can reorder records but not change per-value sums).

Back-ref: `sumLiveValues`, `TopNInRange`, `CardinalityInRange` in query.go.

---

## NOTE-VC-005 — VCNT compactor service overview (issue #400)

Date: 2026-07-02

`internal/modules/valuecountscompactor` is the periodic compaction stage for VCNT
(unique-value-count) files, mirroring the value-index pipeline's compactor
(`valueindexcompactor`, NOTE-VI-017) at the level of overall shape, with several
deliberate simplifications specific to VCNT's much simpler retention model:

- **Single-pass-per-level discipline** — one `compactColumn` call merges only the lowest level
  meeting its file-count threshold, same as NOTE-VI-017's rule for the value-index compactor.
  This keeps each pass same-level and avoids cascading L0→L1→L2 merges in one run.
- **Retention has no external existence-probe step** — unlike `valueindexcompactor`, which
  probes `SourceExister.Exists` per unique `SourceRef` to drop entries from deleted sources,
  VCNT has no per-record source pointer to probe. Retention/liveness is entirely expressed by
  `Compact`'s own net-sum-`<=`-0 rule (NOTE-VC-001) — a value's count simply nets to zero once
  every file that ever introduced it has been superseded or retention-deleted upstream. There is
  no VCNT equivalent of `SourceExister`/`cachingRefChecker`.
- **Write-then-delete crash safety, with a caveat unlike VI** — the merged output's `Put` must
  succeed before any input key is deleted. A crash *before* any Delete, or a fully successful
  Delete batch, degrades gracefully: every input ends up either untouched (recompacted next pass)
  or fully consumed. This is **not** "identical in spirit" to NOTE-VI-017's guarantee for the
  *partial*-failure case, though (a 2026-07-02 go-presubmit review, CRITICAL, corrected this
  overclaim): `valueindexcompactor`'s equivalent is safe there because `valueindex.CompactFiles`
  dedups by identity — duplicate entries are removed by the merge, so reprocessing a surviving
  un-deleted input is a no-op. `valuecounts.Compact` has no identity field on `Record` at all and
  instead SUMS `Count` per merge key, so if `store.Delete` fails for even one input in a batch
  while the rest succeed (and the merged output's `Put` already succeeded), that surviving input
  can be summed a *second* time by a later merge, permanently double-counting its value.
  `valuecountscompactor` mitigates (does not eliminate) this by retrying each failed `Delete` a
  few times before giving up, and surfacing an exhausted-retry failure via both the returned
  error and a dedicated Prometheus counter operators must alert on — see `valuecountscompactor`
  NOTE-VC-009.
- **One-level directory walk, not three** — VCNT's key layout has no `<type>` path segment
  (unlike valueindex's `<tenant>/<hash>/<type>/` layout, NOTE-VI-017-b), so there is no
  equivalent need for a three-step `ListDirs` walk. A single-level walk over the column
  directory is sufficient.
- **`Store` is a narrower interface** than `valueindexcompactor.IndexStore` — it omits `Peek`
  (VCNT files carry no magic header to sniff, unlike VI's bucket-file format) and has no
  `SourceExister` equivalent (retention is `Compact`'s own net-sum-`<=`-0 rule, not an external
  existence probe). It DOES include `ListDirs` — required by the one-level directory walk
  described in the next bullet — `valueindexcompactor.IndexStore` also has `ListDirs`, so this
  is not a point of difference between the two interfaces.
- **`MaxRecordsPerMerge` admission gate** — `CompactBatchBytes` (the byte-budget config knob
  bounding how many input files a single `mergeLevel` call reads) only caps *compressed* input
  bytes, not decoded record count. VCNT files can compress unusually well at small per-file
  sizes (low-cardinality columns, short values), so a byte-capped batch can admit far more
  decoded records than the byte number alone suggests — a real risk of decode-time memory
  blowup that a compressed-byte budget alone does not catch. `mergeLevel` therefore also
  enforces a decoded-record-count ceiling (`MaxRecordsPerMerge`, default 3,000,000, sized from a
  rough ~300B RSS/record estimate) incrementally during its per-file decode loop — once the
  running decoded-record count would exceed the ceiling, remaining files in the level are
  deferred to the next pass rather than decoded now.

Full design lives in `.bob/state/plan.md` and `.bob/state/brainstorm.md` (Addendum 2) for the
in-progress implementation (Phases 8–12).

Back-ref: `internal/modules/valuecountscompactor/` (service.go, store.go, config.go — under
active implementation).

**Addendum (2026-07-07, issue #490, task A-3/#110):** `DecodeLegacyVCNTFile` (the single-chunk
reconstruction fallback for pre-self-describing objects) is deleted — tempo's `vcntwriter.go`
now writes self-describing `EncodeVCNTFile` output unconditionally (task A-Tempo-1/#111), so no
live write path can produce the legacy shape anymore, and the project-wide stored-data wipe
retires any that already exist. `DecodeVCNTObject` is simplified to call `DecodeVCNTFile`
directly (no more `errors.Is(err, ErrNotSelfDescribing)` dispatch) but is KEPT as its own named
function (real production callers: `valuecountscompactor/service.go`,
`vcnt.go:VCNTBuildSectionFromObjects`). `ErrNotSelfDescribing` is KEPT — it still distinguishes
"not a self-describing file" from a genuine malformed-trailer decode error on data that IS
self-describing, independent of the now-removed fallback consumer.

Back-refs: `internal/modules/valuecounts/selfdescribing.go` (deleted `DecodeLegacyVCNTFile`,
simplified `DecodeVCNTObject`). Tests: removed `TestDecodeLegacyVCNTFile_SingleChunkReconstruction`
(TEST-VC-3), `TestDecodeLegacyVCNTFile_MultiChunkDataErrors` (TEST-VC-4),
`TestDecodeVCNTObject_TriesSelfDescribingThenLegacy` (TEST-VC-5, superseded); added
`TestDecodeVCNTObject_DecodesSelfDescribing` (TEST-VC-7). Cross-repo fallout:
`valuecountscompactor`'s `putL0`/`putL0Multi`/`putVCNT` test-fixture helpers were writing stale
legacy-format fixtures (accurate before `A-Tempo-1`/#111, wrong after) — fixed to use
`EncodeVCNTFile`, resolving two resulting test failures (one of which had silently broadened a
quarantine test's blast radius). See `valuecountscompactor/NOTES.md` for that module's own
addendum.

**Addendum, part 2 (2026-07-07, issue #490, task A-7/#101 — found as fallout during an
unrelated task):** root package `vcnt_test.go`'s
`TestVCNTBuildSectionFromObjects_LegacyAndSelfDescribing` (written during task A-1/#96, testing
legacy+self-describing VCNT object merging) became dead functionality once A-3/#110 removed
`DecodeVCNTObject`'s legacy fallback — deleted, along with its now-unused `legacyVCNTObjectFor`
helper. `vcnt.go`'s `VCNTBuildSectionFromObjects` doc comment ("decode each object via
`DecodeVCNTObject` (handles both the self-describing and legacy single-chunk formats
transparently)") is corrected to drop the "legacy" claim. This root-package test lived outside
`internal/modules/valuecounts/`'s own directory, so it was not caught by A-3's own
grep/verification scope — the general lesson: a legacy-format removal's blast radius can extend
to root-package tests exercising the internal package's public re-exports, not just the internal
package's own test suite.

---

## NOTE-VC-006 — Self-describing file format decision, and the vcntwriter.go bug it works around

Date: 2026-07-02

`vcntwriter.go` (in tempo-mrd, out of this repo's scope to edit) discards the `[]ChunkDirEntry`
that `EncodeRecords` returns — nothing persists it alongside the written `.vcnt` object today.
Every file written so far is very likely exactly one chunk (`vcntwriter.go`'s write pattern is
one-column-one-flush, which never accumulates enough records in one call to cross
`shared.ValueCountsRecordsPerChunk`), and is therefore reconstructible as a single
`ChunkDirEntry{CompOff: 0, CompLen: len(data)}` — but this was an unverified, fragile assumption
baked into every reader before this change, not an enforced or documented contract.

`selfdescribing.go` adds two things:

1. A genuinely self-describing file format (`EncodeVCNTFile`/`DecodeVCNTFile`, SPEC-VC-2) for
   this package's own future output — the embedded directory means any consumer can decode a
   VCNT object directly from object storage without a side channel, closing the gap
   `vcntwriter.go` currently leaves open. `EncodeVCNTFile` appends the chunk directory and a
   fixed 12-byte trailer (`dirCount[4] + bodyLen[4] + magic[4]`, magic `0x56434E31` "VCN1")
   directly after the body, mirroring `section.go`'s existing length-prefixed, little-endian
   encoding conventions.
2. A defensive, explicitly-scoped single-chunk-reconstruction fallback
   (`DecodeLegacyVCNTFile`) for reading files written by today's real `vcntwriter.go`, with an
   explicit decode-error path — per SPEC-ROOT-010, a payload that actually spans more than one
   chunk must error, never silently decode as partial or wrong data (confirmed empirically in
   `TestDecodeLegacyVCNTFile_MultiChunkDataErrors`: `snappy.Decode` fails outright on a
   concatenated multi-chunk payload, no extra sanity check was needed to guarantee this).
   `DecodeVCNTObject` composes both paths (self-describing first, legacy fallback **only** when
   the error is `ErrNotSelfDescribing`, `errors.Is`-comparable — any other decode error is a
   genuine failure and must not be silently reinterpreted as "must be legacy") as the single
   entry point a compactor should call to decode an arbitrary VCNT object of unknown origin.

This makes the compactor correct today regardless of whether any file in the wild has ever
actually exceeded one chunk, and gives tempo-mrd's eventual follow-up work a concrete target
format to switch `vcntwriter.go`'s write path to.

**Explicitly out of scope for this change:** extending the root `blockpack` package's `vcnt.go`
re-exports with `EncodeVCNTFile`/`DecodeVCNTFile` aliases — root `CLAUDE.md` requires explicit
user permission before adding new public API surface. Flag this as a follow-up decision if/when
`tempo-mrd` wiring to the new format starts.

**Historical note (2026-07-07, issue #490):** the "out of scope" caveat above was resolved —
`blockpack.EncodeVCNTFile` was added under task A-1/#96, and the `DecodeLegacyVCNTFile` fallback
this entry describes was removed under task A-3/#110. See NOTE-VC-005's addendum and NOTE-VC-015
for the full disposition; `SPECS.md` SPEC-VC-4/SPEC-VC-5 for the current contracts.

Back-refs: `internal/modules/valuecounts/selfdescribing.go` (`EncodeVCNTFile`,
`DecodeVCNTFile`, `DecodeLegacyVCNTFile` [removed, see NOTE-VC-005 addendum], `DecodeVCNTObject`),
`SPECS.md` SPEC-VC-2.

---

*(NOTE-VC-007 is reserved for `internal/modules/valuecountscompactor/NOTES.md`'s compactor-specific design note, per the shared NOTE-VC-N counter across both modules — see spec-oracle ruling. Not a gap/typo.)*

## NOTE-VC-008 — decodeDirEntries dirCount OOM guard (SPEC-ROOT-001/SPEC-ROOT-012 analog)

Date: 2026-07-02

`decodeDirEntries`'s `count` parameter comes directly from `DecodeVCNTFile`'s trailer-read
`dirCount` field — an untrusted, file-supplied `uint32` read straight off the wire before any
validation. The original implementation passed it straight into
`make([]ChunkDirEntry, 0, count)` as an allocation-capacity hint. A corrupted or malicious
trailer claiming e.g. `dirCount = 0xFFFFFFFF` against a near-empty (or entirely absent) actual
directory triggered an unrecoverable `runtime: out of memory` throw — not a catchable Go
`panic`, so no caller-side `recover()` could contain it, directly violating SPEC-ROOT-001's "no
panics on any runtime-reachable path" invariant (the throw is worse than a panic: it kills the
process outright) and the same class of risk SPEC-ROOT-012 already guards against for snappy
decompression sizes.

**Fix:** `decodeDirEntries` now rejects `count > len(data)/minDirEntrySize` (where
`minDirEntrySize = 2+8+4+4 = 18` bytes, the smallest possible encoded entry — empty
`MinColumn`) with a clean decode error, before the `make()` call. This bounds the allocation
hint to what the actual supplied directory bytes could possibly hold, regardless of what the
trailer claims.

Regression-tested by `TestDecodeVCNTFile_RejectsImpossibleDirCount` — see TEST-VC-6.

Back-ref: `internal/modules/valuecounts/selfdescribing.go:decodeDirEntries`,
`minDirEntrySize` constant. `SPECS.md` SPEC-VC-2 (updated with this bound as part of
`DecodeVCNTFile`'s contract).

---

## NOTE-VC-011 — CompactVCNTRecords: exposing Compact publicly for cross-repo test reuse

Date: 2026-07-02

### Why

tempo-mrd's VCNT per-span minute-bucketing work needed a regression test proving that records
from two independent block-builder flushes landing in the same wall-clock minute actually
compact into one merged record (`TestVCNTFlush_CrossBlockMinuteCoalescing`,
`tempodb/encoding/vblockpack/vcntwriter_test.go`). Without a public entry point to this
package's `Compact` (SPEC-VC-1), that test would have needed to reimplement the
group-by-`(ColumnName, TimeStart, TimeEnd, Value)`/sum/drop-nonpositive merge logic locally —
a duplicate of real production logic that could silently drift out of sync with the actual
`Compact` implementation over time, defeating the point of the regression test (it would only
prove the test's own reimplementation is internally consistent, not that the real compaction
path merges correctly).

### What was added

`blockpack.CompactVCNTRecords(records []VCNTRecord) []VCNTRecord` (`vcnt.go:46`) — a
one-line, behavior-preserving wrapper: `return valuecounts.Compact(records)`. No new logic,
no new semantics; see `SPECS.md` SPEC-VC-3 for the formal contract (identical to SPEC-VC-1,
re-exported).

### Why this is safe / minimal

- `VCNTRecord` is already a type alias for `valuecounts.Record` (`vcnt.go:21`, predates this
  change), so the wrapper needs no conversion logic — a pure pass-through.
- This follows the same re-export pattern already established for `SortVCNTRecords` and
  `EncodeVCNTRecords` (`vcnt.go:30`, `vcnt.go:36`) — `CompactVCNTRecords` completes the trio of
  publicly-reachable VCNT record operations (sort, encode, compact) rather than introducing a
  new kind of public surface. **Historical note (2026-07-07, issue #490, task A-17/#121):**
  `EncodeVCNTRecords` itself is since removed — see NOTE-VC-015. This sentence is left as
  written for historical accuracy about why `CompactVCNTRecords` was added at the time.
- Per root `CLAUDE.md`, new public API surface on the root `blockpack` package requires
  explicit user permission — obtained for this specific addition. **Superseded by the blanket
  public-API change permission granted 2026-07-07 for the read-path modernization project — see
  NOTE-VC-015 — which now covers further `valuecounts` re-export changes without per-addition
  approval.**
- A deadcode-anchor call (`_ = blockpack.CompactVCNTRecords(nil)`, `cmd/deadcode/main.go:395`)
  was added alongside the other VCNT* wrapper anchors there, required for `make precommit`'s
  deadcode check — mechanical, not a design decision.

### Cross-repo consumer

tempo-mrd's `vcntwriter_test.go:TestVCNTFlush_CrossBlockMinuteCoalescing` calls
`CompactVCNTRecords` directly on the decoded output of two independent `vcntAccumulator.flush`
calls simulating separate block-builder passes, asserting exactly one merged record survives
with the shared minute-aligned `(TimeStart, TimeEnd)` key and summed `Count` — this is the
real end-to-end proof that VCNT's per-span minute bucketing (tempo-mrd, out of this repo's
scope) actually enables cross-block compaction, not just a same-file coalescing test. Because
tempo-mrd builds in vendor mode, this function was also synced into
`vendor/github.com/grafana/blockpack/vcnt.go` in that repo — no other vendored file was
touched.

Back-refs: `vcnt.go:CompactVCNTRecords`, `internal/modules/valuecounts/compaction.go:Compact`.
`SPECS.md` SPEC-VC-3.

---

## NOTE-VC-012 — VCNTBuildSectionFromObjects: consolidate fetched .vcnt objects into one gate-ready section

Date: 2026-07-06

### Why

The cube cardinality gate (`cube.CheckCardinality`, driven by `CubeCreationTrigger.TryCreate`)
consumes a *single* VCNT section `(data []byte, dir []ChunkDirEntry)` and answers per-dimension
distinct-value queries against it via `ValuesInRange`. But real VCNT data for a proposed cube's
dimensions is spread across *many* `.vcnt` objects in S3 — one per column-hash directory, one per
block-builder flush / compaction pass. tempo's `maybeCreateCube` had no way to turn "N fetched
`.vcnt` objects across M dimensions" into the one section the gate wants, so it passed `nil` and the
gate was a permanent no-op in production (issue #483).

### What was added

`blockpack.VCNTBuildSectionFromObjects(objects [][]byte) ([]byte, []VCNTChunkDirEntry)` (`vcnt.go`):
decode each object via `DecodeVCNTObject`, concatenate all records, run `Compact` (delta
accounting — so a later retention/compaction delta correctly nets a value out of the merged live
set), and re-encode via `EncodeRecords`. The output is byte-for-byte the same section shape the
gate already consumes. **Historical note (2026-07-07, issue #490, task A-3/#110):**
`DecodeVCNTObject` no longer "handles both the self-describing and legacy single-chunk formats
transparently" — its legacy fallback was removed; it now decodes only the self-describing
format. This function's own doc comment was corrected accordingly (see NOTE-VC-005 addendum,
part 2).

### Design decisions

- **Skip-on-error, never fail the build.** A single corrupt/truncated `.vcnt` object is skipped, not
  propagated — a bad file for one dimension must not defeat the gate for every other dimension. An
  all-empty/all-corrupt input yields an empty-but-valid section, which the gate reads as "no
  coverage" and passes by default. This preserves the prior `nil`-data best-effort contract exactly:
  wiring real data can only *tighten* the gate where coverage exists, never newly *block* creation
  where coverage is missing.
- **Compact, not raw concat.** Feeding un-compacted records to `ValuesInRange` would still net
  correctly (it sums per value and drops `<= 0`), but compacting once here keeps the section small and
  makes the delta-accounting semantics explicit at build time rather than deferring them to every
  per-dimension read.

### Consumer

tempo `cube_backfill.go:buildVCNTSection` lists+downloads the `.vcnt` files under each dim's
`unique_values/<colHash>/` prefix (through the shared `cachingStore`-wrapped `minioVIStore`, issue
#478) and calls this to build the section `maybeCreateCube` hands to `TryCreate`. VCNT filenames carry
no embedded time range (unlike VI files), so all of a column's files are fetched and the query window
is applied at the record level by the gate's `ValuesInRange` decode. **[Superseded by NOTE-VC-017,
issue #494, 2026-07-10]** — VCNT filenames now DO carry a genuine embedded time range
(`FormatFilenameV2`/`ParseFilenameV2`, once the three-part deployment described in NOTE-VC-017
lands together: this blockpack change, tempo's L0-writer switch, and the full VCNT data wipe).
This sentence is left as written for historical accuracy about `VCNTBuildSectionFromObjects`'s
original design constraint (2026-07-06); that function itself is unaffected by #494's filename
change — it still fetches and decodes whatever files exist under the prefix and does not filter
by the filename-embedded range, so its own "fetch everything, filter at record level" behavior is
unchanged even though the underlying files it fetches now happen to carry more metadata in their
names.

**Addendum (2026-07-07, go-presubmit Fix 4):** the skip-on-error design above is unchanged, but it
previously gave zero operator visibility into how many objects were skipped, unlike the sibling
`valuecountscompactor/service.go:mergeLevel` consumer of the identical `DecodeVCNTObject` failure
(which increments a `filesQuarantined` Prometheus counter documented as "a deliberate, logged
data-loss event -- alert if non-zero"). `VCNTBuildSectionFromObjects` now returns a third value,
`skipped int`, counting both empty (`len(obj)==0`) and undecodable objects:

```go
func VCNTBuildSectionFromObjects(objects [][]byte) (data []byte, dir []VCNTChunkDirEntry, skipped int)
```

tempo's `buildVCNTSection` call site now logs a warning when `skipped > 0`; no metrics/alerting
plumbing was added (deliberately out of scope — a debug-level operational signal is sufficient here,
unlike `mergeLevel`'s counter, since a bad `.vcnt` object for one dimension is expected to
self-correct on the next compaction/flush cycle rather than representing a permanent loss).

Back-refs: `vcnt.go:VCNTBuildSectionFromObjects`, `internal/modules/cube/cardinality.go:CheckCardinality`,
`internal/modules/valuecounts/selfdescribing.go:DecodeVCNTObject`. Issue #483. Test:
`vcnt_test.go:TestVCNTBuildSectionFromObjects_SkipsCorruptAndEmpty` (asserts `skipped == 3`).

---

## NOTE-VC-013 — SelectivityInRange: a value-scoped selectivity oracle for AND-leaf ordering

Date: 2026-07-06

`SelectivityInRange(data, dir, column, value, minTS, maxTS)` is the Phase 1 primitive of the
cost-based leaf-resolution ordering work (issue #484). Given a single canonical-encoded value,
it sums the net live `Count` for that one value over the window and returns a
`SelectivityEstimate{Count, Covered}` — an approximate span count for the leaf predicate
`column = value`, cheap enough to consult before deciding which columns' value-index files to
fetch from object storage.

### Why a separate primitive, not `ValuesInRange` + a lookup

`ValuesInRange` enumerates *every* distinct value of the column and their counts, then a caller
would scan for the one it wants. For a selectivity oracle over a specific value that is wasted
work (build the full per-value map, sort, allocate a slice, discard all but one entry).
`SelectivityInRange` sums only records matching the requested value in a single pass, no
per-value map, no sort, no `[]ValueCount` allocation. Both share `DecodeTimeRange` and the same
signed delta-accounting liveness rule (NOTE-VC-001), so results are consistent between them.

### `Covered` is load-bearing — do not collapse it into `Count == 0`

The estimate distinguishes two cases the caller must treat oppositely for ordering:

  - **Covered, Count == 0** — the value index affirmatively knows this value matches zero live
    spans in the window. This leaf is *maximally selective*: resolve it first, an empty result
    short-circuits the whole AND group (mirrors NOTE-019's `len(result)==0` early exit, applied
    one level up at index-fetch-decision time — Phase 2).
  - **Not Covered** — no VCNT record exists for the (column, value, window). This carries *no*
    selectivity signal; the caller must fall back to its no-coverage policy (resolve last /
    unknown), never treat the leaf as if it matched zero spans.

Collapsing these into a single "zero" would make an uncovered leaf masquerade as maximally
selective and wrongly short-circuit an AND that actually has matches — a correctness bug, not
just a bad estimate. Hence the explicit `Covered` flag.

### Clamp to >= 0

Net-negative sums are possible transiently (a retention/compaction delta subtracted before its
matching introduction is seen, given VCNT's async batch writes). A negative live count is
meaningless as a span estimate, so `Count` is clamped to zero — reading as "zero live spans"
while `Covered` stays true. Accuracy only needs to be directionally correct for ordering (issue
#484), so this clamp is safe.

Back-refs: `SelectivityInRange`, `SelectivityEstimate` in query.go. Issue #484.

---

## NOTE-VC-014 — ColumnTotalInRange: the selectivity DENOMINATOR

Date: 2026-07-06

`ColumnTotalInRange(data, dir, column, minTS, maxTS)` is the population-side counterpart to
NOTE-VC-013's `SelectivityInRange` and the primitive #486 (selectivity-aware execution) needs
to recognize a low-selectivity predicate. `SelectivityInRange` gives the NUMERATOR — the net
live span count for one `column = value` pair; `ColumnTotalInRange` gives the DENOMINATOR — the
total live span count across ALL values of the column over the window. Their ratio is a leaf's
selectivity fraction: a `column = value` whose value accounts for most of the column's spans is
low-selectivity, so value-index pruning on it would skip almost nothing (the `kind=server`
case in #481/#486).

### Sum of LIVE per-value counts, not a raw record sum

It reuses `sumLiveValues` (the same primitive behind `ValuesInRange`/`CardinalityInRange`) and
sums the returned per-value counts. This is deliberately the sum of the LIVE values (each net
count > 0, NOTE-VC-001) rather than a raw sum over `DecodeTimeRange` records: a raw sum would
fold in a value's negative retention/compaction delta before its matching introduction is seen
and could read a genuinely-populated column as a smaller (or transiently negative) total. Because
`ColumnTotalInRange` is a denominator, an under-count there would INFLATE a leaf's computed
selectivity fraction and wrongly flag a selective predicate as low-selectivity. Summing live
per-value counts keeps `ColumnTotal >= sum of the same column's SelectivityInRange numerators`
by construction, so a leaf's fraction is always in `[0, 1]`.

### `Covered` mirrors SelectivityEstimate.Covered

Like the numerator, the result carries a `Covered` flag distinct from a zero `Total`: no VCNT
record for the column at all is "no signal" (caller applies its no-coverage policy), NOT a
genuine zero population. The queryplan classifier reads `!Covered` (or a non-positive total) as
`UnknownSelectivity` and leaves the strategy choice to the caller's default.

Back-refs: `ColumnTotalInRange`, `ColumnTotal` in query.go. Issue #486
(`queryplan.Classify`, NOTE-QP-002, is the consumer).

---

## NOTE-VC-015 — VCNT write/decode path consolidated to self-describing EncodeVCNTFile; EncodeVCNTRecords removed (issue #490, tasks A-1/#96, A-3/#110, A-17/#121)

Date: 2026-07-07

This note records the umbrella sequencing across three tasks that together retire VCNT's
pre-self-describing write/decode path in favor of a single self-describing format everywhere:

1. **Prerequisite (task A-Tempo-1/#111, tempo-mrd, out of this repo's scope):** tempo's
   `vcntwriter.go` switched to persisting the `[]ChunkDirEntry` it previously discarded, writing
   self-describing `EncodeVCNTFile` output unconditionally. This had to land FIRST — removing the
   reader-side legacy fallback (step 3 below) before this shipped would have made a live,
   currently-written object unreadable.
2. **`blockpack.EncodeVCNTFile(records []VCNTRecord, perChunk int) []byte`** (`vcnt.go`, task
   A-1/#96, coder-2-3) — a new public root-package re-export, added so tempo-mrd's
   `vcntwriter.go` could call the self-describing encoder without importing the internal
   `valuecounts` package. `VCNTRecord` is already a type alias for `valuecounts.Record` (predates
   this change), so this is a pure pass-through: `func EncodeVCNTFile(records []VCNTRecord,
   perChunk int) []byte { return valuecounts.EncodeVCNTFile(records, perChunk) }`. Round-trip
   tested by `vcnt_test.go:TestEncodeVCNTFile_RoundTrip`. Its `cmd/deadcode/main.go` anchor
   (`_ = blockpack.EncodeVCNTFile(nil, 0)`) was caught missing from the original A-1 landing and
   added as a follow-up fix (2026-07-07, coder-2-3).
3. **`DecodeLegacyVCNTFile`/`DecodeVCNTObject`'s fallback-dispatch role removed** (task A-3/#110,
   coder-1-3) — see NOTE-VC-005's addendum above for the full detail (deleted symbols, kept
   symbols, test changes, and the cross-repo `valuecountscompactor` fixture fallout this surfaced).
4. **`blockpack.EncodeVCNTRecords` removed** (`vcnt.go`, task A-17/#121, coder-2-3) — it had zero
   remaining consumers once tempo's `vcntwriter.go` switched to `EncodeVCNTFile` (step 1) and
   tempo's `cube_vcnt_fetch_test.go` test helper (`vcntObj()`) was updated to match (that helper
   was still hand-constructing legacy-format fixtures — a latent, not-yet-manifesting break since
   tempo's *vendored* blockpack copy had not yet received step 3's decode-side removal; fixed
   forward-compatibly ahead of the eventual `A-Tempo-3` revendor). Its `cmd/deadcode/main.go`
   anchor and root-package `vcnt_test.go` test were removed alongside it. One encoder, one format.

> Blanket public-API change permission granted by maintainer for the read-path modernization
> project (2026-07-07), superseding per-addition approval; API-level backward compatibility
> explicitly out of scope — public shims/aliases may be removed and signatures changed freely,
> with the sole consumer (tempo) updated in lockstep.

**Cross-cutting pattern, worth flagging beyond this specific removal:** the `cube_vcnt_fetch_test.go`
`vcntObj()` fixture bug (step 4) is the SECOND instance in this project of the same bug class —
the first was `valuecountscompactor`'s `putL0`/`putL0Multi`/`putVCNT` fixtures (step 3's
addendum, NOTE-VC-005 above). Both were test helpers that silently kept hand-constructing a
legacy wire format after the corresponding decoder's legacy-fallback path was removed or
scheduled for removal, masked because the consuming code (`VCNTBuildSectionFromObjects`,
`DecodeVCNTObject`'s old fallback) treats decode failure as skip-not-error. Neither was caught by
`deadcode`/`go vet`/compilation — only by an explicit consumer/fixture audit. Fixture-generation
helpers that hand-construct a wire format should be treated as a first-class audit target
whenever a legacy decode path is removed.

Back-refs: `vcnt.go:EncodeVCNTFile` (added, task A-1), `vcnt.go:EncodeVCNTRecords` (deleted, task
A-17), `internal/modules/valuecounts/selfdescribing.go` (`DecodeLegacyVCNTFile` deleted,
`DecodeVCNTObject` simplified — task A-3; see NOTE-VC-005 addendum). Test:
`vcnt_test.go:TestEncodeVCNTFile_RoundTrip`. Cross-repo: tempo-mrd's `vcntwriter.go` (task
A-Tempo-1/#111), `cube_vcnt_fetch_test.go:vcntObj()` (task A-17/#121, forward-compatible fix
ahead of `A-Tempo-3`'s revendor). See `SPECS.md` SPEC-VC-4 (decode contract) and SPEC-VC-5
(EncodeVCNTFile contract).

---

## NOTE-VC-016 — SelectivityPerMinute: the per-minute sibling of SelectivityInRange (issue #487)

Date: 2026-07-07

`SelectivityPerMinute(data, dir, column, value, minTS, maxTS)` is the C1 blockpack-side primitive
for #487's time-slice job sharding: instead of collapsing `[minTS, maxTS]` into one scalar
(`SelectivityInRange`, NOTE-VC-013), it buckets the same value-scoped sum by `Record.TimeStart`
and returns `[]MinuteCount`, ascending by `Minute`. Every VCNT record already carries a
single-minute `TimeStart == TimeEnd` (tempo `vcntwriter.go`'s `minuteBucket` floor), so bucketing
by `TimeStart` is exactly "one bucket per live minute," with no independent floor/alignment logic
of its own — this function trusts the write path's existing minute alignment rather than
re-deriving it.

### Why the caller needs per-minute signal, not the scalar

Time-slice construction (C3, `queryplan.BuildTimeSlices`) needs to see WHICH minutes in the
window carry live matches for the lead leaf so it can size slices adaptively — dense minutes get
narrower slices, sparse/empty minutes get wider ones. `SelectivityInRange`'s single scalar throws
away exactly the signal this needs; `SelectivityPerMinute` is a parallel, not a replacement,
primitive for that reason.

### Same liveness rule as the rest of the package

Minutes whose net summed `Count` is `<= 0` are dropped entirely (NOTE-VC-001) — never returned as
`Count: 0`. This mirrors `sumLiveValues`' behavior but is intentionally NOT built on top of it
(that function is value-agnostic; this one filters to a single `(column, value)` pair in the same
single pass as the minute-bucket sum, avoiding a full per-value map for a query that only wants
one value).

Back-refs: `SelectivityPerMinute`, `MinuteCount` in perminute.go. Issue #487, task C1.

---

## NOTE-VC-017 — VCNT filenames now embed a genuine wall-clock time range (issue #494)

Date: 2026-07-10

### Why

`valuecountscompactor`'s prior file-selection strategy only understood compaction level
(`L<level>-<id>.vcnt`, v1) — it could group same-level files for merging but had no way to
prefer merging files whose time ranges actually overlap or sit close together, since the
filename carried no time information and decoding every candidate file just to inspect its
range would defeat the point of a cheap, listing-time selection strategy. Issue #494 asked for
compaction to actually merge files with overlapping/similar time ranges, which requires that
range to be visible without a decode.

### What was added

- **`FormatFilenameV2`/`ParseFilenameV2`** (`filename.go`): a strict, 4-dash-part filename shape
  `L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt` that embeds the file's `[min TimeStart, max
  TimeEnd]` range directly in the object key. See `SPECS.md` SPEC-VC-7 for the full contract,
  including the "no v1 fallback" parsing rule and the `wallMinSec > wallMaxSec` rejection.
- **`TimeRange(records []Record) (minSec, maxSec uint64)`** (`timerange.go`, new file): computes
  the genuine range via a full `O(n)` scan, deliberately never assuming `TimeEnd` is
  sort-order-monotonic (NOTE-VC-002) — factored into this shared package rather than duplicated
  in `valuecountscompactor` and tempo's `vcntwriter.go` so the non-monotonicity correctness
  property is proven once (`TestTimeRange_MaxTimeEndNotLastSortedRecord`) and reused by both real
  call sites.
- **Root-package re-exports** (`vcnt.go`, task A-2/#92): `VCNTFormatFilenameV2`,
  `VCNTFileMeta` (alias for `FileMeta`), `VCNTParseFilenameV2`, `VCNTRecordTimeRange`, and
  `VCNTObjectKeyV2` (the v2 analog of the existing `VCNTObjectKey`, building the full S3 key from
  tenant/indexPrefix/column/id plus the new range parameters) — added under the blanket
  public-API change permission already in effect for this pipeline (NOTE-VC-015).
- **`valuecountscompactor`'s `compactColumn`/`mergeLevel` switched to v2 exclusively** — see that
  module's own new dated NOTE for the clustering design built on top of this filename change
  (`valuecountscompactor/NOTES.md`, new entry) and `SPECS.md` SPEC-VC-3.

### The three-things-must-land-together constraint — no gradual/mixed-format transition

This filename change has **zero backward-compatibility code by design** (this project's standing
no-backward-compat directive): `ParseFilenameV2` treats a v1-shaped filename as a plain parse
error, with no fallback path to the old 2-part shape. This means three separate changes must land
together as a single all-or-nothing deployment, not a staged rollout:

1. **This blockpack change** — `valuecountscompactor` requires v2 filenames to select anything.
2. **tempo's `vcntwriter.go` L0 writer switch** (Part B of #494, out of this repo's scope) — must
   start producing v2 filenames, or every newly-written L0 file becomes invisible to the
   compactor from the moment Part A deploys.
3. **A full VCNT data wipe** (every existing `.vcnt` file, every tenant, every environment) — any
   pre-existing v1-format file that survives the wipe does not get force-migrated or corrupted;
   it simply sits permanently unparseable, and therefore permanently unmerged, by
   `compactColumn`'s `ParseFilenameV2` parse loop (see `valuecountscompactor` NOTES.md's own new
   entry for the full straggler-file safety argument).

There is no intermediate state where some files are v1 and some v2 within the same deployment —
by design, per this project's no-backward-compat directive, not an oversight.

Back-refs: `internal/modules/valuecounts/filename.go` (`FormatFilenameV2`, `ParseFilenameV2`,
`FileMeta`), `internal/modules/valuecounts/timerange.go` (`TimeRange`), `vcnt.go`
(`VCNTFormatFilenameV2`, `VCNTParseFilenameV2`, `VCNTFileMeta`, `VCNTRecordTimeRange`,
`VCNTObjectKeyV2`). `SPECS.md` SPEC-VC-7. Tests: `filename_v2_test.go`, `timerange_test.go` — see
`TESTS.md` TEST-VC-9/TEST-VC-10. Supersedes the "VCNT filenames carry no embedded time range"
statement in NOTE-VC-012 (which remains, marked superseded, for historical accuracy about
`VCNTBuildSectionFromObjects`'s original 2026-07-06 design constraint). See
`valuecountscompactor/NOTES.md`'s own new dated entry for the clustering algorithm built on top
of this filename change, and its `SPECS.md` SPEC-VC-3.

## NOTE-VC-020 — colHash -> column-name audit manifest lives in `internal/modules/colhashmanifest`, not here (task #216, cross-reference entry)

Date: 2026-07-13

`ColHash` (`filename.go`) is a genuine one-way hash — `hex(SHA256(colName)[:16])`, identical to
`valueindex.ColHash`, with no other mechanism anywhere to recover the source column name from a
hash. Browsing VCNT's on-disk layout (`<tenant>/indexes/unique_values/<colHash>/...`) directly
shows only opaque hash-named directories. Task #216 closes this with a best-effort, per-tenant,
JSON-serialized manifest (`internal/modules/colhashmanifest`, mirroring `internal/modules/cube`'s
own `Registry` pattern) mapping `colHash -> colName`, shared between VI and VCNT since both
compute the IDENTICAL hash function independently and could genuinely collide on the same
column.

**Why the new code does not live in this package.** `valuecounts` itself never touches object
storage; the module that actually performs a real object-storage write using
`valuecounts.ColHash`-keyed paths is `internal/modules/valuecountscompactor` (for L1+ merges —
see that package's NOTES.md NOTE-VC-019 for why compaction, not an L0 write blockpack does not
own, is VCNT's earliest available hook). The manifest's own registry logic
(`internal/modules/colhashmanifest`) intentionally does NOT import `valuecounts` (or
`valueindex`) — it only ever handles `(tenant, colHash, colName)` as plain strings its callers
already resolved, preserving the existing deliberate decoupling between the VI and VCNT
`ColHash` implementations (see `valuecountscompactor/service.go`'s `ownsShard` doc comment, and
`colhashmanifest/NOTES.md` NOTE-COLMANIFEST-1 for the full argument).

This manifest is purely additive, best-effort observability — it is NEVER consulted by any of
this package's own read/write/query logic (including `Compact`, `SelectivityInRange`, or any
decode path), and nothing in `valuecounts` changed to support it.

Back-refs: `internal/modules/colhashmanifest` (the shared registry — see its own SPECS.md/
NOTES.md for the full contract), `internal/modules/valuecountscompactor/NOTES.md` NOTE-VC-019
(the actual VCNT-side call site and hook rationale), `internal/modules/valueindex/NOTES.md`
NOTE-VI-107 (VI's symmetric cross-reference entry), `filename.go:ColHash`.

---

## NOTE-VC-022 — Duration histogram: 16-bucket hardcoded array chosen over cube's log2 scheme; over-estimate direction locked in (issue #205, Phase A)

Date: 2026-07-13

**Why a fresh, hardcoded 16-value millisecond array instead of reusing
`internal/modules/cube/bucket.go`'s `Log2Bucketize`.** Two designs were explored and rejected
before this one: (1) an 8-bucket hand-picked SLO-aligned scheme (the original brainstorm), and
(2) reusing cube's own 64-slot power-of-two-in-nanoseconds `Log2Bucketize` boundaries directly, or
a "custom 0 bucket + 31 `Log2Bucketize` buckets" variant. Both alternates to the final design were
rejected because cube's own boundaries serve a different consumer (real
`histogram_over_time()`/quantile ANSWERS, which must stay byte-identical to tempo's
`pkg/traceql.Log2Bucketize` — SPEC-CUBE-019) and waste almost the entire bucket budget crossing
from nanoseconds into low seconds, leaving no resolution for common query thresholds like
`duration > 10s`. The final 16-entry array is structurally the original 8-bucket brainstorm idea,
doubled for finer resolution, with zero dependency on `cube`'s code — `internal/modules/cube` is
explicitly out of scope for this feature and is not modified by it.

**Discrete, not cumulative — why this needed its own explicit statement.** "Floor semantics"
alone is ambiguous between a discrete/density histogram (one sample increments exactly one
bucket) and a cumulative/CDF histogram (one sample increments every bucket on one side of it).
This design is discrete: a write-side sample increments exactly `Counts[BucketIndex(v)]`, never a
range. `EstimateThreshold`/`EstimateBetween` (SPEC-VC-8) separately SUM a contiguous range of
these already-discrete buckets at READ time as an estimation technique over an approximate
predicate — this is a distinct layer from write-time storage and must not be conflated with it.

**Over-estimate direction, locked in.** For `>`/`>=`/`<`/`<=`/`between`, the bucket straddling a
non-boundary threshold is counted as fully matching (over-estimate), never excluded
(under-estimate). An over-estimate only ever costs a missed I/O-reduction opportunity at
plan-time selectivity classification (the planner keeps the safer, more-expensive default); it
never causes a wrong answer, since the downstream block-scan/value-index path always re-verifies
the real data regardless of dispatch strategy. Equality is always unestimable
(`TimeCompareOp.OpEQ` -> `known=false`) — no resolution finer than a bucket width exists.

**Never-drop-records audit.** `DurationBucketBoundsMillis[0] == 0` means `BucketIndex` always
finds at least bucket 0 for any non-negative input — the never-drop-by-construction guarantee
holds by the array's own shape, with no extra runtime guard code. Separately,
`DurationHistogramInRange`'s per-bucket "drop net `<= 0` to zero" rule (mirroring
`sumLiveValues`/NOTE-VC-001) is retention/compaction accounting, not sample classification: a
bucket's live count reaching zero because every write netted against a matching deletion delta
means the bucket genuinely has zero live spans right now — reporting it as absent is correct, not
a dropped record. The never-drop principle governs write-time sample classification (Phase B,
out of this repo's scope — tempo's `vcntwriter.go`), not this orthogonal retention model.

**Why `TimeCompareOp` is exported from this package** rather than reusing
`internal/modules/vibuilder`'s own unrelated, unexported `timeCompareOp`: that type solves a
different, unrelated decidability problem (VI's own exact-lookup gate) and is not visible outside
its package; this package's read primitive needs its own operator type so a future caller (the
Phase C `queryplan` cost-function adapter) can construct one without introducing a dependency on
`vibuilder`.

**Separately flagged, out-of-scope, pre-existing finding in `internal/modules/cube`'s own shipped
code** (unrelated to this feature, found incidentally while evaluating and rejecting reuse of its
`Log2Bucketize`, kept here per this project's "note it, tell the user, don't file a ticket
unprompted" convention for out-of-scope gaps): for a pathologically large Int64/Duration aggAttr
value (`v >= 2^63+1` — unreachable with any real span duration, since `2^63`ns is roughly 292
years, but not provably unreachable for corrupted/adversarial input), `Log2Bucketize`'s internal
`1 << (64 - bits.LeadingZeros64(v-1))` shift wraps to `0` under Go's defined shift semantics for a
count `>= 64` on a `uint64`, so `cube/accumulator.go`'s own `if boundary != -1` guard does not
catch it and `BucketIndex(0) == bits.TrailingZeros64(0) == 64` — one past cube's own valid
`[1,63]` slot range, an out-of-bounds index into `agg.Buckets[64]`. Not fixed here; `cube` is
out of scope for this task (see this package's own histogram.go and SPEC-VC-8 — no file under
`internal/modules/cube/` is touched by #205's Phase A).

Back-refs: `internal/modules/valuecounts/histogram.go`. `SPECS.md` SPEC-VC-8. Tests:
`histogram_test.go` — see `TESTS.md` TEST-VC-11. Issue #205, Phase A.

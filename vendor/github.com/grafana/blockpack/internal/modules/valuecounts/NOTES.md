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

Back-refs: `internal/modules/valuecounts/selfdescribing.go` (`EncodeVCNTFile`,
`DecodeVCNTFile`, `DecodeLegacyVCNTFile`, `DecodeVCNTObject`), `SPECS.md` SPEC-VC-2.

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
  new kind of public surface.
- Per root `CLAUDE.md`, new public API surface on the root `blockpack` package requires
  explicit user permission — obtained for this specific addition. This is not a blanket
  license for further ad hoc `valuecounts` re-exports.
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
decode each object via `DecodeVCNTObject` (handles both the self-describing and legacy single-chunk
formats transparently), concatenate all records, run `Compact` (delta accounting — so a later
retention/compaction delta correctly nets a value out of the merged live set), and re-encode via
`EncodeRecords`. The output is byte-for-byte the same section shape the gate already consumes.

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
is applied at the record level by the gate's `ValuesInRange` decode.

Back-refs: `vcnt.go:VCNTBuildSectionFromObjects`, `internal/modules/cube/cardinality.go:CheckCardinality`,
`internal/modules/valuecounts/selfdescribing.go:DecodeVCNTObject`. Issue #483.

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

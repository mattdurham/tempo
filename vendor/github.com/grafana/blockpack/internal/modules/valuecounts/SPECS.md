# valuecounts — Interface and Behaviour Specification

This document defines the public contracts, input/output semantics, and invariants for the
`internal/modules/valuecounts` package. It complements `NOTES.md` (design rationale) and
`TESTS.md` (test plan), per root `SPEC.md` SPEC-ROOT-009.

When code conflicts with this file, this file wins.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecountscompactor/SPECS.md`'s own separate `SPEC-VC-N` sequence — each
module's SPECS.md numbers from 1, per the established convention in
`valueindex/SPECS.md`/`valueindexcompactor/SPECS.md` sharing the `SPEC-VI-N` prefix with
independent per-file counters). IDs are assigned in ascending order and never reused or
renumbered; superseded entries are marked `[SUPERSEDED by SPEC-VC-N]` rather than deleted.

Next free ID: **SPEC-VC-10**.

---

## SPEC-VC-1: Compact retention semantics
*Added: 2026-07-02*

**Contract:** `Compact(records []Record) []Record` groups its input by the merge key
`(ColumnName, TimeStart, TimeEnd, Value)`, sums `Count` within each group, and drops any group
whose summed `Count` is `<= 0` from the output. A dropped group means the value's net live
count has reached zero across all inputs — it no longer exists in any live block and must not
appear in query results.

**Rules:**

- Grouping is exact on all four key fields; `TimeEnd` is part of the key (see NOTE-VC-002) —
  two records sharing `ColumnName`/`TimeStart`/`Value` but differing `TimeEnd` are never merged.
- `Count` is a signed `int64`; summation, not counting, is the merge operation.
- Groups summing to exactly `0` are dropped, not retained with `Count: 0` — VCNT has no
  zero-count tombstone representation.
- `Compact` sorts its input in place (via `Sort`) as part of computing group runs; callers
  needing the original order preserved must copy the slice first.
- Empty input (`len(records) == 0`) returns `nil`, not an empty non-nil slice. An input that
  compacts to zero live groups also returns `nil`.
- The returned slice is in canonical VCNT order (`ColumnName, TimeStart, Value, Count` per
  `Sort`).

**Rationale:** This is the self-healing delta-accounting mechanism described in NOTE-VC-001 —
retention and compaction are both expressed as negative-count records, with no separate
tombstone state to track or garbage-collect.

Back-ref: `internal/modules/valuecounts/compaction.go:25` (func `Compact`).

---

## SPEC-VC-2: EncodeVCNTFile / DecodeVCNTFile self-describing format contract
*Added: 2026-07-02*

**Contract:** `EncodeVCNTFile(records []Record, perChunk int) []byte` produces a VCNT object
that carries its own chunk directory, so any consumer can decode it directly from object
storage without a side-channel directory.

**Wire format:**

```
[snappy-chunked body, identical to EncodeRecords's body output]
[ChunkDirEntry × dirCount, each: min_column_len[2 LE] + min_column[N] + min_time_start[8 LE]
                                  + comp_off[4 LE] + comp_len[4 LE]]
[trailer: dir_count[4 LE] + body_len[4 LE] + magic[4 LE] = 0x56434E31 ("VCN1")]
```

Trailer size is fixed at 12 bytes. Directory entry encoding mirrors `section.go`'s existing
length-prefixed, little-endian conventions.

**`DecodeVCNTFile(data []byte) ([]Record, error)` rules:**

- Reads the trailing 12 bytes; if `len(data) < 12` or the magic field doesn't match
  `0x56434E31`, returns `ErrNotSelfDescribing` (wrapped via `%w`, `errors.Is`-comparable).
  **[Updated by SPEC-VC-4, issue #490, task A-3/#110]** As of the legacy-decoder removal, this
  is no longer a signal to retry via a different decoder — `DecodeVCNTFile` is now the sole
  decode path (see SPEC-VC-4), so `ErrNotSelfDescribing` on real data represents a genuine
  format error. It remains a distinct sentinel (not folded into a generic decode error) because
  it still usefully distinguishes "not shaped like a self-describing file at all" from "shaped
  like one but has a malformed trailer/directory."
- Validates `bodyLen` is within `[0, len(data)-trailerSize]` before slicing; out-of-bounds
  `bodyLen` is a genuine decode error (not `ErrNotSelfDescribing`).
- The trailer-supplied `dirCount` is validated against `len(dirBytes)/minDirEntrySize` before
  being used as a `make([]ChunkDirEntry, 0, dirCount)` capacity hint — an unvalidated,
  corrupted or malicious `dirCount` (e.g. `0xFFFFFFFF`) would otherwise trigger an
  unrecoverable OOM `runtime.throw` rather than a clean decode error (SPEC-ROOT-001, mirroring
  SPEC-ROOT-012's decompression-bomb-guard principle). A `dirCount` exceeding what the
  remaining bytes could possibly hold is a decode error, not a panic.
- On success, delegates to `DecodeAll(body, dir)` using the embedded directory.

**`DecodeLegacyVCNTFile(data []byte) ([]Record, error)` rules: [SUPERSEDED by SPEC-VC-4 —
function removed outright, issue #490, task A-3/#110, 2026-07-07]**

- ~~Reconstructs a single-entry directory (`ChunkDirEntry{CompOff: 0, CompLen: len(data)}`)
  covering the entire payload and decodes via `DecodeAll`.~~
- ~~Valid only for objects that never exceeded one snappy chunk. Multi-chunk data fed through
  this path returns an error.~~
- Kept below for history only; see SPEC-VC-4 for the current (single-decode-path) contract.

**`DecodeVCNTObject(data []byte) ([]Record, error)` rules: [REVISED by SPEC-VC-4 — issue #490,
task A-3/#110, 2026-07-07; the function itself is NOT removed]**

- ~~Tries `DecodeVCNTFile` first. On success, returns its result. If `DecodeVCNTFile` returns
  `ErrNotSelfDescribing`, falls back to `DecodeLegacyVCNTFile`.~~ Now calls `DecodeVCNTFile`
  directly and propagates any error unchanged — there is no fallback decoder left to dispatch to.
- Retained as its own named function (not inlined into callers) because it has real production
  callers (`valuecountscompactor/service.go`, `vcnt.go:VCNTBuildSectionFromObjects`) that
  shouldn't need churn if a future VCNT format variant is ever added.
- See SPEC-VC-4 for the current authoritative contract.

**Rationale (historical):** `vcntwriter.go` (tempo-mrd, out of this repo's scope) used to
discard the `[]ChunkDirEntry` that `EncodeRecords` returns, so no persisted VCNT object could
carry its own directory. `EncodeVCNTFile`/`DecodeVCNTFile` closed that gap for this package's own
output; `DecodeLegacyVCNTFile` and `DecodeVCNTObject` let a compactor built on this package
correctly read files written by that un-migrated write path in the interim. **As of task
A-Tempo-1/#111 (2026-07-07), `vcntwriter.go` itself writes self-describing `EncodeVCNTFile`
output unconditionally, closing the gap at the source — see SPEC-VC-4.**

Back-ref: `internal/modules/valuecounts/selfdescribing.go` (`EncodeVCNTFile`,
`DecodeVCNTFile`, `decodeDirEntries`, `DecodeVCNTObject` — `DecodeLegacyVCNTFile` deleted, see
SPEC-VC-4).

---

## SPEC-VC-3: CompactVCNTRecords — public re-export of Compact for external callers
*Added: 2026-07-02*

**Contract:** `blockpack.CompactVCNTRecords(records []VCNTRecord) []VCNTRecord` (root package,
`vcnt.go:46`) is a thin, behavior-preserving public wrapper around this package's
`Compact` (SPEC-VC-1): `func CompactVCNTRecords(records []VCNTRecord) []VCNTRecord { return
valuecounts.Compact(records) }`. It introduces no new semantics — every rule in SPEC-VC-1
(grouping by `(ColumnName, TimeStart, TimeEnd, Value)`, signed-count summation, drop-if-`<=0`,
sorts input in place, `nil` on empty/fully-dropped input, canonical output order) applies
identically through this entry point.

**Rationale for existing as a public wrapper:** exposes the real merge/compaction logic to
callers outside this module (specifically tempo-mrd's block-builder test suite) so they can
exercise genuine cross-block coalescing behavior end-to-end instead of maintaining a
test-local reimplementation of `Compact`'s grouping/summing/drop rules that could silently
drift from the real logic. `VCNTRecord` (`vcnt.go:21`) is already a type alias for
`valuecounts.Record`, so no data conversion occurs at the wrapper boundary.

**Scope note:** this was originally the only new root-package (`blockpack`) public API surface
added for this purpose under root `CLAUDE.md`'s per-addition-approval rule. **Superseded
2026-07-07:** the maintainer granted blanket public-API change permission for the read-path
modernization project (see `NOTES.md` NOTE-VC-015), which now covers further `valuecounts`
root-package re-export additions/removals without per-addition approval — see SPEC-VC-4/SPEC-VC-5
for the changes made under that grant.

Back-refs: `vcnt.go:46` (`CompactVCNTRecords`), `internal/modules/valuecounts/compaction.go:25`
(`Compact`, SPEC-VC-1). Consumer: tempo-mrd's `tempodb/encoding/vblockpack/vcntwriter_test.go`
(`TestVCNTFlush_CrossBlockMinuteCoalescing`, out of this repo's scope — tempo-mrd has no
SPECS.md/NOTES.md convention for this code). See `NOTES.md` NOTE-VC-011.

---

## SPEC-VC-4: DecodeVCNTFile is the sole supported decode path (legacy single-chunk reconstruction removed)
*Added: 2026-07-07*

**Contract:** `DecodeVCNTFile` is the only supported decode path for VCNT objects in this
package. The legacy single-chunk reconstruction fallback (`DecodeLegacyVCNTFile`, formerly
described under SPEC-VC-2) is removed — issue #490, task A-3/#110 — now that the write path
(tempo-mrd's `vcntwriter.go`, task A-Tempo-1/#111) always emits self-describing output
unconditionally, and the project-wide stored-data wipe retires any pre-existing legacy-shaped
objects.

**Rules:**

- `DecodeVCNTFile(data []byte) ([]Record, error)` behavior is UNCHANGED from SPEC-VC-2's
  original description (trailer/magic validation, `bodyLen` bounds check, `dirCount` OOM guard)
  — only its role changes, from "one of two dispatch targets" to "the only decode path."
- `ErrNotSelfDescribing` is KEPT as a distinct sentinel (still `errors.Is`-comparable) — it
  still usefully distinguishes "not shaped like a self-describing file" from a genuine
  malformed-trailer/directory decode error on data that IS self-describing. It no longer
  signals "retry via a different decoder" (there is none); on real input it represents a
  genuine format error.
- `DecodeVCNTObject(data []byte) ([]Record, error)` is KEPT as a named function — it now calls
  `DecodeVCNTFile` directly and propagates any error unchanged (no fallback dispatch). It
  remains the compactor's per-input-file decode entry point (`valuecountscompactor/service.go`,
  `vcnt.go:VCNTBuildSectionFromObjects`).
- No migration/dual-read window is provided or needed: the project's stored-data wipe (issue
  #490) removes any object that could still be in the legacy shape before this ships.

**Rationale:** One encoder, one decoder, per this project's no-backward-compatibility directive
— see `NOTES.md` NOTE-VC-015 for the full three-task sequencing (A-1/A-3/A-17) this contract is
the end-state of.

Back-refs: `internal/modules/valuecounts/selfdescribing.go` (`DecodeVCNTFile`, `DecodeVCNTObject`
— `DecodeLegacyVCNTFile` deleted). `NOTES.md` NOTE-VC-005 (addendum), NOTE-VC-015.

---

## SPEC-VC-5: EncodeVCNTFile — public re-export of EncodeVCNTFile for external callers
*Added: 2026-07-07*

**Contract:** `blockpack.EncodeVCNTFile(records []VCNTRecord, perChunk int) []byte` (root
package, `vcnt.go`) is a thin, behavior-preserving public wrapper around this package's
`EncodeVCNTFile` (SPEC-VC-2): `return valuecounts.EncodeVCNTFile(records, perChunk)`. It
introduces no new semantics — every rule in SPEC-VC-2's `EncodeVCNTFile` contract (wire
format, trailer layout) applies identically through this entry point.

**Note:** the root package's older `EncodeVCNTRecords` re-export (a non-self-describing,
single-chunk-only encoder) is REMOVED (issue #490, task A-17/#121, 2026-07-07) — it had zero
remaining consumers once tempo-mrd's `vcntwriter.go` switched to this function. `EncodeVCNTFile`
is now the sole root-package VCNT encoder. See `NOTES.md` NOTE-VC-015.

Back-refs: `vcnt.go:EncodeVCNTFile`, `internal/modules/valuecounts/selfdescribing.go:EncodeVCNTFile`
(SPEC-VC-2). Test: `vcnt_test.go:TestEncodeVCNTFile_RoundTrip`. Consumer: tempo-mrd's
`vcntwriter.go` (out of this repo's scope).

## SPEC-VC-6: SelectivityPerMinute — per-minute selectivity contract
*Added: 2026-07-07*

**Contract:** `SelectivityPerMinute(data []byte, dir []ChunkDirEntry, column string, value
[]byte, minTS, maxTS uint64) ([]MinuteCount, error)` decodes `[minTS, maxTS]` via
`DecodeTimeRange`, sums `Record.Count` for the exact `(column, value)` pair grouped by
`Record.TimeStart`, and returns one `MinuteCount{Minute, Count}` per distinct live minute,
sorted ascending by `Minute`. `Minute` is a unix-second, 60-aligned bucket boundary inherited
from the write path (tempo `vcntwriter.go`'s `minuteBucket`) — this function does not re-derive
or re-validate minute alignment, it trusts `Record.TimeStart` is already floored.

**Liveness rule:** a minute whose net summed `Count` is `<= 0` is dropped from the result, never
returned as `Count: 0` (same rule as `SelectivityInRange`/`ValuesInRange`, NOTE-VC-001). Records
for a different column or a different value in the same window contribute nothing.

**No files opened:** like every other function in this file, it operates only on the
already-decoded VCNT section (`data`/`dir`) and opens no blockpack data files.

Back-refs: `perminute.go:SelectivityPerMinute`, `MinuteCount`. Test: `perminute_test.go`. Issue
#487, task C1. See `NOTES.md` NOTE-VC-016.

---

## SPEC-VC-7: FormatFilenameV2 / ParseFilenameV2 v2 filename contract; TimeRange full-scan guarantee
*Added: 2026-07-10*

**Contract:** `FormatFilenameV2(level int, wallMinSec, wallMaxSec uint64, id string) string`
produces a `.vcnt` filename of the form `L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt` — a
strict superset of the v1 shape (`L<level>-<id>.vcnt`, `FormatFilename`/`ParseFilename`, unchanged)
that additionally embeds the file's genuine wall-clock time range, so a compactor can select
merge candidates by time proximity in O(1) per file (no decode required) rather than only by
compaction level (issue #494).

**`ParseFilenameV2(name string) (FileMeta, error)` rules:**

- Requires exactly 4 dash-separated parts after the mandatory `L` prefix and `.vcnt` suffix:
  `<level>-<wallMinSec>-<wallMaxSec>-<id>`. **There is no v1 fallback** — a syntactically valid
  v1 filename (2 parts: `<level>-<id>`) is a defined parse error here, not a silently-accepted
  degenerate case, per this project's no-backward-compatibility directive (issue #494, R4/R1).
  This is the exact, sole mechanism by which a straggler v1-format file is safely and
  permanently skipped (never merged, never corrupted) by any caller that switches to
  `ParseFilenameV2` — see `valuecountscompactor` SPEC-VC-3 and NOTES.md's new dated entry.
- `level` and both `wallMinSec`/`wallMaxSec` segments must parse as their respective integer
  types (`strconv.Atoi`, `strconv.ParseUint(..., 64)`); the `id` segment must be non-empty.
  Any failure returns a zero `FileMeta` and a wrapped error — never a partially-populated
  `FileMeta`.
- **`wallMinSec > wallMaxSec` is rejected as a malformed filename** — a reversed range can never
  be produced by a well-formed writer (the range is the true `[min TimeStart, max TimeEnd]`
  scan of the records the file contains, so `min <= max` always), so a filename claiming
  otherwise is corrupt or adversarial input. This validation is the root-cause fix for a
  finding (task #100, 2026-07-10) that `valuecountscompactor`'s pure-function
  `clusterByTimeRange` was the only place defending against a reversed range, via an explicit
  (not merely commented-away) underflow guard on its own `uint64` subtraction — `ParseFilenameV2`
  rejecting the malformed input at its origin protects every consumer of `FileMeta`, not just
  that one call site; `clusterByTimeRange`'s own guard remains as defense-in-depth against a
  `levelFile` constructed by a future caller that bypasses `ParseFilenameV2`.
- `FileMeta{Filename, ID, Level, WallMinSec, WallMaxSec}` is the parsed result; there is no
  `WallMinSec == WallMaxSec` restriction — a file covering exactly one instant is valid.

**`(*FileMeta) IsInTimeRange(queryMinSec, queryMaxSec uint64) bool` contract:** returns
`m.WallMaxSec >= queryMinSec && m.WallMinSec <= queryMaxSec` — standard half-open-interval
overlap test against the file's own `[WallMinSec, WallMaxSec]`. An exact-boundary touch (query
range's edge equals the file range's edge) counts as overlapping.

**`SortFileMetas(metas []FileMeta)` contract:** sorts ascending by `(Level, WallMinSec,
WallMaxSec)` in that priority order — mirrors `lessFileMeta`'s exact comparator. Not currently
consumed by production code in this package (`valuecountscompactor` sorts its own `levelFile`
slice independently inside `clusterByTimeRange`/`capBatchBytes`); provided as a public
convenience for any future caller that needs a deterministic ordering directly over parsed
`FileMeta` values.

**`TimeRange(records []Record) (minSec, maxSec uint64)` contract:** returns the true
`(min(TimeStart), max(TimeEnd))` across every record via a full, unconditional `O(n)` scan.
**Callers must never assume sort-order monotonicity on `TimeEnd`** — `Compact`'s own sort order
is `(ColumnName, TimeStart, Value, Count)` (SPEC-VC-1); `TimeEnd` is explicitly not a sort key
(NOTE-VC-002), so the record with the largest `TimeStart` (sorted last) does not necessarily
carry the largest `TimeEnd`. `TimeRange` does not special-case a pre-sorted or single-chunk
input to avoid a full scan — the scan is the entire guarantee. Returns `(0, 0)` for an empty
slice (not an error — there is no valid non-degenerate range to report for zero records).

**Rationale:** Both contracts exist to support issue #494's compaction-time clustering: a
compactor needs (a) a way to know a candidate file's time range without decoding it
(`FormatFilenameV2`/`ParseFilenameV2`, read from the object key alone), and (b) a way to compute
the genuine range of a just-merged output before writing its v2 filename
(`TimeRange`, over the already-decoded, already-`Compact`-ed record slice `mergeLevel` holds in
memory). `TimeRange` is factored into this shared package (rather than duplicated in
`valuecountscompactor` and tempo's `vcntwriter.go`) specifically so this non-monotonicity
correctness property is proven once via `TestTimeRange_MaxTimeEndNotLastSortedRecord` and reused
by both real call sites through the `VCNTRecordTimeRange` re-export (`vcnt.go`), rather than
each maintaining its own copy that could silently regress to the "last-sorted record's TimeEnd"
bug this test exists to catch.

Back-refs: `internal/modules/valuecounts/filename.go` (`FormatFilenameV2`, `ParseFilenameV2`,
`FileMeta`, `IsInTimeRange`, `SortFileMetas`, `lessFileMeta`), `internal/modules/valuecounts/timerange.go`
(`TimeRange`). Root-package re-exports: `vcnt.go` (`VCNTFormatFilenameV2`, `VCNTParseFilenameV2`,
`VCNTFileMeta`, `VCNTRecordTimeRange`, `VCNTObjectKeyV2`). Consumer:
`internal/modules/valuecountscompactor/service.go` (`compactColumn`'s `ParseFilenameV2` parse
loop, `mergeLevel`'s `TimeRange` call before writing the merged output's v2 filename — see
`valuecountscompactor` SPEC-VC-3). Tests: `filename_v2_test.go`, `timerange_test.go` — see
`TESTS.md` TEST-VC-9/TEST-VC-10.

---

## SPEC-VC-8: Duration histogram — 16-bucket array, discrete assignment, never-drop guarantee, value encoding
*Added: 2026-07-13*

**Contract (issue #205, Phase A):** `DurationBucketBoundsMillis [16]uint64` is a fixed,
hardcoded array of bucket lower-boundaries in milliseconds — `{0, 1, 5, 10, 50, 100, 500, 1_000,
5_000, 10_000, 30_000, 60_000, 300_000, 600_000, 1_800_000, 3_600_000}` — never derived from a
formula and never shared with `internal/modules/cube`'s own `Log2Bucketize` scheme (a different,
incompatible boundary set for a different consumer; see NOTE-VC-022).

`BucketIndex(valueMillis uint64) int` returns the largest index `i` such that
`DurationBucketBoundsMillis[i] <= valueMillis` (floor semantics). Because
`DurationBucketBoundsMillis[0] == 0`, every non-negative `valueMillis` maps to a valid index —
this is the never-drop-by-construction guarantee: no input can fail to place into some bucket, and
no separate clamp/guard code is needed. Index 15 (the 1hr boundary) is the sole open-ended
catch-all for every value `>= 1hr` — there is no separate tail bucket.

**Discrete, not cumulative:** `BucketIndex` identifies exactly ONE bucket per value. A caller
that increments a histogram from a sample increments exactly `Counts[BucketIndex(v)]` and no
other bucket's counter — this is a discrete/density histogram, not a cumulative/CDF one. The
read-side `EstimateThreshold`/`EstimateBetween` (below) separately SUM a contiguous range of
these already-discrete buckets as an estimation technique — that summing never changes how
`Counts` was populated and must not be conflated with the storage layer being cumulative.

**Value encoding:** `EncodeHistogramValue`/`decodeHistogramValue` are the canonical
`Record.Value` encoding for a bucket boundary — a fixed 8-byte little-endian `uint64`, deliberately
not `valueindex.CanonicalValue` (these 16 synthetic values are never compared against or sorted
alongside any other column's values; `ColumnName` alone already scopes every VCNT read).
`decodeHistogramValue` returns `ok=false` (never panics) for any value that isn't exactly 8 bytes,
per SPEC-ROOT-001's no-panic rule. `EncodeHistogramValue` is exported (Phase B0, #205) so the root
package (`vcnt.go`) can re-export it as `VCNTDurationHistogramValue` for tempo's writer; the decode
side stays unexported since only this package's own `DurationHistogramInRange` needs it.

`HistogramColumnName(column string) string` returns `column + "#hist"` — the synthetic VCNT
column name a duration histogram's records are stored under. Collision-free: no existing VCNT
column name contains `#`, and OTLP attribute keys structurally cannot either.

`DurationHistogramInRange(data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64)
(DurationHistogram, error)` decodes `[minTS, maxTS]` via the existing `DecodeTimeRange`, filters
to `ColumnName == HistogramColumnName(column)`, decodes each record's `Value` into a boundary and
maps it to its array index via an exact-match lookup (a decoded boundary matching none of the 16
known values is a corrupt/unrecognized record — skipped, never causing an error or a panic), sums
`Count` per bucket, and drops a bucket to `0` (never negative) when its net sum is `<= 0` —
mirroring `sumLiveValues`'s liveness rule (NOTE-VC-001/004). This per-bucket zero-floor is
retention/compaction accounting, not a violation of the never-drop principle above (that
principle governs write-time sample classification, not read-time live-count accounting — see
NOTE-VC-022 for the full audit). `Covered` is `true` iff at least one histogram record for
`column` existed in the decoded window, regardless of whether every bucket ended up net-zero
(mirrors `ColumnTotalInRange`'s own `Covered` semantics exactly).

`EstimateThreshold(op TimeCompareOp, thresholdMillis uint64) (count int64, known bool)` and
`EstimateBetween(loMillis, hiMillis uint64) (count int64, known bool)` approximate a
`>`/`>=`/`<`/`<=`/`between` predicate by summing a contiguous range of buckets. Per the
over-estimate rule (#205 §0.1): the bucket straddling a threshold is always counted IN FULL
(never excluded) — an over-estimate only ever costs a missed I/O-reduction opportunity at plan
time, never a wrong answer, since the downstream block-scan/value-index path always re-verifies
the real data regardless of dispatch strategy. `EstimateBetween` caps the degenerate case (both
bounds landing in the same bucket) at that single bucket's own count, never double-counting it.
`TimeCompareOp.OpEQ` (equality) is always `known=false` — genuinely unestimable at any resolution
finer than a bucket width.

Back-refs: `internal/modules/valuecounts/histogram.go` (`DurationBucketBoundsMillis`,
`BucketIndex`, `HistogramColumnName`, `DurationHistogram`, `DurationHistogramInRange`,
`TimeCompareOp`, `EstimateThreshold`, `EstimateBetween`, `sumRange`). Tests: `histogram_test.go`
— see `TESTS.md` TEST-VC-11. `NOTES.md` NOTE-VC-022.

---

## SPEC-VC-9: DurationHistogramPerMinuteInRange / MinuteDurationHistogram — per-minute duration histogram contract
*Added: 2026-07-14*

**Contract (issue #499, Phase 1):** `DurationHistogramPerMinuteInRange(data []byte, dir
[]ChunkDirEntry, column string, minTS, maxTS uint64) ([]MinuteDurationHistogram, error)` is
`DurationHistogramInRange`'s (SPEC-VC-8) per-minute sibling, mirroring `SelectivityPerMinute`'s
(SPEC-VC-6) exact relationship to `SelectivityInRange`: instead of collapsing `[minTS, maxTS]`
into one `DurationHistogram`, it buckets the SAME histogram records (`ColumnName ==
HistogramColumnName(column)`) by `Record.TimeStart` and returns one `MinuteDurationHistogram{
Minute, Histogram}` per distinct LIVE minute, sorted ascending by `Minute`. Two records sharing
the same `TimeStart` but different boundary values both contribute to that ONE minute's
`Histogram.Counts` at their respective bucket indices — the grouping key is `TimeStart` alone, not
`(TimeStart, boundary)`.

**Liveness rule (binding, same as SPEC-VC-6):** a minute whose EVERY bucket nets to `<= 0` is
dropped from the result entirely — never retained as an all-zero, `Covered: true` entry. This
mirrors `SelectivityPerMinute`'s per-minute liveness rule exactly (NOTE-VC-001/016), applied here
per-minute across all 16 buckets rather than to a single scalar sum. A minute with at least one
net-positive bucket is retained, with every other (net-`<=`-0) bucket in that minute's own
`Histogram.Counts` floored to `0` (never negative) — mirroring `DurationHistogramInRange`'s
per-bucket floor rule (SPEC-VC-8).

**No coverage returns empty, never an error:** a column with no histogram records at all in the
window returns `(nil or empty slice, nil error)` — mirrors `SelectivityPerMinute`'s own
uncovered-column contract exactly, never a defined-error case.

**Same 16 fixed boundaries:** reuses `DurationBucketBoundsMillis`/`boundaryToIndex` unchanged — no
new boundary scheme is introduced for the per-minute variant.

**No files opened:** like every other function in this file, it operates only on the
already-decoded VCNT section (`data`/`dir`) and opens no blockpack data files.

`MinuteDurationHistogram{Minute uint64; Histogram DurationHistogram}` is the per-minute-histogram
pair type — `DurationHistogram`'s own contract (SPEC-VC-8) is unchanged and reused as-is.

Back-refs: `internal/modules/valuecounts/histogram_perminute.go` (`DurationHistogramPerMinuteInRange`,
`MinuteDurationHistogram`). Tests: `histogram_perminute_test.go` — see `TESTS.md` TEST-VC-12. Issue
#499, Phase 1. See `NOTES.md` NOTE-VC-023.

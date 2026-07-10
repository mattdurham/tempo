# valuecounts — Test Specifications

This document defines the required tests for the `internal/modules/valuecounts` package. Each
test is described with its scenario, setup, and expected assertions, per root `SPEC.md`
SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecountscompactor/TESTS.md`'s own separate `TEST-VC-N` sequence). IDs are
assigned in ascending order and never reused or renumbered.

Next free ID: **TEST-VC-11**.

---

## TEST-VC-1: TestEncodeDecodeVCNTFile_RoundTrip
*Added: 2026-07-02*

**Scenario:** `EncodeVCNTFile`/`DecodeVCNTFile` round-trip records spanning multiple chunks.

**Setup:** 10 records sorted into canonical order, encoded with `perChunk=3` (spans 4 chunks).

**Assertions:** `DecodeVCNTFile` returns no error; the decoded records equal the input set
under an order-independent multiset comparison (`requireRecordsEqual`/`recordKeys`).

**Spec invariants tested:** SPEC-VC-2.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestEncodeDecodeVCNTFile_RoundTrip`.

---

## TEST-VC-2: TestDecodeVCNTFile_RejectsNonSelfDescribing
*Added: 2026-07-02*

**Scenario:** `DecodeVCNTFile` must distinguish "not self-describing" from generic corruption.

**Setup:** Non-self-describing bytes (no embedded directory/trailer) fed directly to
`DecodeVCNTFile`.

**Assertions:** Returns a non-nil error, and `errors.Is(err, ErrNotSelfDescribing)` is true.

**Spec invariants tested:** SPEC-VC-2, SPEC-VC-4.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeVCNTFile_RejectsNonSelfDescribing`.

---

## TEST-VC-3: TestDecodeLegacyVCNTFile_SingleChunkReconstruction — REMOVED (issue #490, task A-3/#110, 2026-07-07)
*Added: 2026-07-02*

**Removed:** `DecodeLegacyVCNTFile` (the function this test exercised) was deleted once
tempo-mrd's `vcntwriter.go` write path switched to self-describing `EncodeVCNTFile` output
(task A-Tempo-1/#111) and the project's stored-data wipe retired any legacy-shaped objects. ID
retained (not reused) per the file's numbering convention. See `NOTES.md` NOTE-VC-005 addendum,
NOTE-VC-015; `SPECS.md` SPEC-VC-4.

---

## TEST-VC-4: TestDecodeLegacyVCNTFile_MultiChunkDataErrors — REMOVED (issue #490, task A-3/#110, 2026-07-07)
*Added: 2026-07-02*

**Removed:** same removal as TEST-VC-3 — `DecodeLegacyVCNTFile` deleted in full. ID retained
(not reused) per the file's numbering convention. See `NOTES.md` NOTE-VC-005 addendum,
NOTE-VC-015; `SPECS.md` SPEC-VC-4.

---

## TEST-VC-5: TestDecodeVCNTObject_TriesSelfDescribingThenLegacy — SUPERSEDED by TEST-VC-7 (issue #490, task A-3/#110, 2026-07-07)
*Added: 2026-07-02*

**Superseded:** this table-driven test's legacy-decode case (case 2) and corrupt-data case
(case 3) no longer apply to a single function once `DecodeVCNTObject`'s fallback dispatch was
removed. Replaced by TEST-VC-7 (`TestDecodeVCNTObject_DecodesSelfDescribing`), which covers
`DecodeVCNTObject`'s remaining (self-describing-only) contract. See `NOTES.md` NOTE-VC-005
addendum, NOTE-VC-015; `SPECS.md` SPEC-VC-4.

---

## TEST-VC-6: TestDecodeVCNTFile_RejectsImpossibleDirCount
*Added: 2026-07-02*

**Scenario:** SPEC-ROOT-001/SPEC-ROOT-012-analog regression guard — `decodeDirEntries` must
reject a trailer-supplied `dirCount` that exceeds what the actual directory bytes could
possibly hold, rather than passing it straight into `make([]ChunkDirEntry, 0, dirCount)` as an
allocation hint (a `dirCount = 0xFFFFFFFF` against near-empty directory bytes previously
crashed the process with an unrecoverable `runtime: out of memory` throw, not a catchable
panic).

**Setup:** Hand-built 12-byte trailer only (`dirCount=0xFFFFFFFF`, `bodyLen=0`,
`magic=vcntFileMagic`) with zero actual directory bytes present, passed to `DecodeVCNTFile`.

**Assertions:** Returns a non-nil error; does not attempt the oversized allocation.

**Spec invariants tested:** SPEC-VC-2, SPEC-ROOT-001.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeVCNTFile_RejectsImpossibleDirCount`.

---

## TEST-VC-7: TestDecodeVCNTObject_DecodesSelfDescribing
*Added: 2026-07-07*

**Scenario:** `DecodeVCNTObject` (now a direct, no-fallback call to `DecodeVCNTFile`) still
decodes self-describing VCNT objects correctly after the legacy-fallback dispatch was removed
(issue #490, task A-3/#110).

**Setup:** `EncodeVCNTFile` output fed to `DecodeVCNTObject`.

**Assertions:** Returns no error; decoded records match the input set (order-independent
comparison).

**Spec invariants tested:** SPEC-VC-4.

Back-ref: `internal/modules/valuecounts/selfdescribing_test.go:TestDecodeVCNTObject_DecodesSelfDescribing`.

## TEST-VC-8: SelectivityPerMinute — bucketing, liveness, isolation, and time-bound coverage
*Added: 2026-07-07*

**Scenario:** `SelectivityPerMinute` (SPEC-VC-6) must bucket a value's summed count by
`Record.TimeStart`, apply the same net-`<=`-0 liveness drop as the rest of the package, and
isolate its target `(column, value)` from unrelated columns/values sharing the same minute.

**Setup/Assertions (`perminute_test.go`):**

- `TestSelectivityPerMinute_BucketsByTimeStart` — four records across three minutes (60, 120,
  300), two of which share minute 60. Asserts three ascending-`Minute` `MinuteCount` results,
  with the shared-minute pair summed (`{60,7}`, `{120,3}`, `{300,9}`).
- `TestSelectivityPerMinute_DropsNetNonPositiveMinutes` — a net-zero minute (5 + -5) and a
  net-negative minute (a single -1 record, minute 180) are both dropped; only the one net-positive minute
  (`{120,3}`) survives.
- `TestSelectivityPerMinute_IgnoresOtherColumnsAndValues` — records for a different value
  ("POST") and a different column ("resource.service.name") sharing the same minute must not
  contribute to the target `(column="span:name", value="GET")`'s sums.
- `TestSelectivityPerMinute_TimeBounded` — a `[minTS, maxTS]` window excludes a record outside
  it; only the in-window minute's bucket is returned.

**Assertions (common):** no error; returned slice is exactly the expected `[]MinuteCount`
(order-sensitive, ascending by `Minute`).

**Spec invariants tested:** SPEC-VC-6.

Back-ref: `internal/modules/valuecounts/perminute_test.go`. Issue #487, task C1.

---

## TEST-VC-9: filename_v2_test.go — FormatFilenameV2/ParseFilenameV2/IsInTimeRange/SortFileMetas
*Added: 2026-07-10*

**Scenario:** Locks in SPEC-VC-7's v2 filename contract: round-trip encoding, the strict
4-dash-part shape with no v1 fallback, the `wallMinSec > wallMaxSec` rejection, and the
`IsInTimeRange`/`SortFileMetas` helpers built on `FileMeta` (issue #494).

**Setup/Assertions (`filename_v2_test.go`):**

- `TestFormatFilenameV2_RoundTrip` — `FormatFilenameV2(1, 100, 200, "abc123")` produces
  `"L1-100-200-abc123.vcnt"`; `ParseFilenameV2` on that string returns the exact original
  `FileMeta`.
- `TestFormatFilenameV2_MinEqualsMax` — `WallMinSec == WallMaxSec` (a file covering exactly one
  instant) round-trips without error.
- `TestParseFilenameV2_MalformedInputs` — table of 8 malformed shapes (wrong suffix, missing `L`
  prefix, v1 2-part shape, non-integer level/minSec/maxSec, empty id segment, and the reversed
  range `"L0-500-100-abc.vcnt"`) each return a non-nil error and a zero `FileMeta`.
- `TestParseFilenameV2_V1ShapeReturnsError` — a genuine v1 filename produced by
  `FormatFilename` is fed to `ParseFilenameV2`; asserts a non-nil error. This is the mandatory
  R4 regression guard: the exact mechanism by which a straggler v1-format file is safely skipped
  by `compactColumn` with no special-case code.
- `TestIsInTimeRange` — table of 7 cases (overlap at start/end, exact match, query-inside-file,
  file-inside-query, disjoint before, disjoint after) against a fixed `FileMeta{WallMinSec:
  1000, WallMaxSec: 2000}`.
- `TestSortFileMetas` — three `FileMeta` values sort ascending by `(Level, WallMinSec,
  WallMaxSec)` in that priority order.

**Spec invariants tested:** SPEC-VC-7.

Back-ref: `internal/modules/valuecounts/filename_v2_test.go`. Issue #494, task A1/#91.

---

## TEST-VC-10: timerange_test.go — TimeRange full-scan and non-monotonic-TimeEnd correctness
*Added: 2026-07-10*

**Scenario:** Locks in SPEC-VC-7's `TimeRange` contract: a full `O(n)` scan for both bounds,
with the mandatory R3 adversarial guard against assuming `TimeEnd` is sort-order-monotonic
(NOTE-VC-002) — the exact bug class this function exists to prevent (issue #494).

**Setup/Assertions (`timerange_test.go`):**

- `TestTimeRange_Empty` — `TimeRange(nil)` returns `(0, 0)`.
- `TestTimeRange_SingleRecord` — a single `{TimeStart: 42, TimeEnd: 99}` record returns
  `(42, 99)`.
- `TestTimeRange_MaxTimeEndNotLastSortedRecord` — three records sorted by `TimeStart` ascending
  where the FIRST record has the far-largest `TimeEnd` (9999) and the LAST has a small one (600).
  Asserts `maxSec == 9999`, explicitly not `600` (the last-sorted record's `TimeEnd` — the bug a
  naive "return the last element" implementation would produce).
- `TestTimeRange_MinAlsoComputedByScan` — the smallest `TimeStart` is not the first record in the
  slice; asserts the true minimum (100) is still found, proving `TimeRange` does not assume any
  pre-sort by `TimeStart` either.

**Spec invariants tested:** SPEC-VC-7.

Back-ref: `internal/modules/valuecounts/timerange_test.go`. Issue #494, task A1/#91.

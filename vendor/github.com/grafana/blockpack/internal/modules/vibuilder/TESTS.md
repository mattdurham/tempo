# vibuilder — Test Specifications

This document defines the required tests for the `internal/modules/vibuilder` package. Each
test is described with its scenario, setup, and expected assertions, per root `SPEC.md`
SPEC-ROOT-009.

This file is created for the first time as part of issue #488 (read-path modernization, task
B-7) — prior to this, `vibuilder` had only a `NOTES.md`.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VB-N` (file-scoped per
SPEC-ROOT-009 — distinct from `valueindex/TESTS.md`'s own `TEST-VI-N` counter, matching the
`SPEC-VB-N`/`SPEC-VI-N` separation established in this package's `SPECS.md`). IDs are assigned
in ascending order and never reused or renumbered.

Next free ID: **TEST-VB-4**.

---

## TEST-VB-1: `lookupColumn` parity — ranged path matches the pre-refactor whole-file path exactly
*Added: 2026-07-07*

**Scenario:** `lookupColumn`'s observable output (matched `[]VILookupResult`, `filesRead`,
`bytesRead`) must be byte-identical before and after the B-5 rewire from
`downloadAll`+`valueindex.QueryBucketFiles` onto `queryKeysRanged`+
`valueindex.QueryBucketFileRanged` — the ranged-read path is a pure I/O optimization, not a
behavior change.

**Setup:** `TestLookupColumn_ParityWithRangedPath` (`builder_ranged_test.go`) builds a
3-block v2 `BucketGroup` fixture via `buildRangedParityFile` (six distinct `(time, value)`
groups split at `groupsPerBlock=2`, engineered so one block's `[MinValue, MaxValue]` excludes
the test predicate's target value entirely — satisfying the B-1 fixture requirement of ≥2 blocks
with ≥1 block excluding the predicate). Records `lookupColumn`'s exact sorted result (via
`sortVILookupResultsForGolden`, which imposes an order independent of either read path's
internal block/group traversal order) as a literal golden value written directly into the test,
pinned first against the OLD path (task B-1, before any ranged-read code existed) and re-run
unmodified against the NEW path (task B-6, after B-5's rewire landed).

**Assertions:** matched results equal the golden `[]VILookupResult` slice exactly (both entries
for the "charlie" equality predicate, correct `SourceRef`/`TimeSec`/`BlockPage`/`BlockLen`/
`TraceID` per entry); `filesRead == 1`; `bytesRead == len(data)` (the full object size, per
SPEC-VB-2's accounting contract — even though the ranged path itself transfers fewer bytes
underneath).

## TEST-VB-2: `lookupColumn` I/O-counting — a fully time-excluded file costs exactly one `ReadAt` (the footer)
*Added: 2026-07-07*

**Scenario:** end-to-end (`vibuilder`/`FileStore` level) analog of
`valueindex.TestQueryBucketFileRanged_TimeExcludedFile_FooterOnly` — a query window that does
not overlap a value-index file's own time range must cost `lookupColumn` exactly one `ReadAt`
against that file (the fixed-size footer at its known trailing offset), with zero matches and no
error.

**Setup:** `TestLookupColumn_TimePrunedReadsFooterOnly` (`builder_ranged_test.go`) builds a
single-block fixture with `TimeSec=1000`, queries with `timeRange = [5000, 6000]` (fully
outside), against a `countingFileStore` that records every `ReadAt`'s exact `(offset, length)`
per key — not just a call count.

**Assertions:** `results` empty, no error; `filesRead == 1` (the file still counts toward
`FilesRead` even though its body was never fetched — an accounting choice shared with
SPEC-VB-2's `bytesRead` contract, both reflecting "this file was consulted," not "this file's
body was read"); `bytesRead == len(data)`; exactly one recorded `ReadAt` for the file, at offset
`len(data) - valueindex.BucketFooterSize` with length `valueindex.BucketFooterSize` — proving
the file-level time prune (`valueindex/SPECS.md` SPEC-VI-10, step 2) fires before any other read.

## TEST-VB-3: `lookupColumn` I/O-counting — a value excluded by every block costs footer + string-table + block-directory reads, zero block bodies
*Added: 2026-07-07*

**Scenario:** end-to-end analog of
`valueindex.TestQueryBucketFileRanged_ValueExcludedByAllBlocks_FooterAndDirOnly` — a predicate
value excluded by every block's `[MinValue, MaxValue]` (`blockExcludedByValue`, `valueindex/SPECS.md`
SPEC-VI-9) must cost `lookupColumn` exactly the footer, string table, and block directory reads
— zero block-body reads — with zero matches and no error.

**Setup:** `TestLookupColumn_ValuePrunedReadsFooterAndDirOnly` (`builder_ranged_test.go`)
reuses the same 3-block fixture as TEST-VB-1 (block value ranges `[alpha,bravo]`,
`[charlie,delta]`, `[echo,foxtrot]`), queries for `"zzz"` (sorts lexicographically after every
block's `MaxValue`) with a `timeRange` that overlaps the file (so only value-range pruning, not
time pruning, can be responsible for excluding every block), against a `countingFileStore`.

**Assertions:** `results` empty, no error; `filesRead == 1`; `bytesRead == len(data)`; the
recorded `ReadAt` offsets for the file are exactly `{footer offset, footer.StringTableOff,
footer.BlockIndexOff}` (order-independent set comparison) — no offset corresponding to any
block's `CompOff` appears, proving `blockExcludedByValue` correctly excluded every block before
any block-body `ReadAt` was issued.

---

## Coverage note

`lookupColumnAll`'s ranged-path behavior is exercised indirectly by the pre-existing
`TestLookupColumnAll_PreservesFirstTypeOrdering` (`builder_test.go`, `vibuilder/NOTES.md`
NOTE-VI-049) and the `TestBuildSource_*` suite, both of which route through the same
`queryKeysRanged`/`storeRangedSource` machinery TEST-VB-1/2/3 exercise directly via
`lookupColumn` — no separate ranged-specific fixture is currently maintained for
`lookupColumnAll` beyond these, since `queryKeysRanged` is the single shared implementation both
callers use (SPEC-VB-2).

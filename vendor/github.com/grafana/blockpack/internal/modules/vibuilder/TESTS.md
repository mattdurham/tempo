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

Next free ID: **TEST-VB-7**.

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

## TEST-VB-4: `LeafIndexable` — per-leaf shape resolvability, all rejected/accepted shapes
*Added: 2026-07-07 (issue #487, task T5b)*

**Scenario:** `LeafIndexable` (SPEC-VB-3) must accept exactly the leaf shapes `buildPredicate`
can represent and reject everything else, with no I/O.

**Setup/Assertions (`leaf_indexable_test.go`):**

- `TestLeafIndexable_Nil` — `nil` returns `false`.
- `TestLeafIndexable_SingleEqualityValue` — a single-value equality leaf returns `true`.
- `TestLeafIndexable_MultiValueIsNotIndexable` — an equality leaf with more than one value
  returns `false`.
- `TestLeafIndexable_Range` — a range/between leaf returns `true`.
- `TestLeafIndexable_Regex` — a regex leaf with a non-empty `Pattern` returns `true`.
- `TestLeafIndexable_RequirePresentOnlyIsNotIndexable` — a `RequirePresent`-only leaf returns
  `false`.
- `TestLeafIndexable_EmptyLeafIsNotIndexable` — a leaf with no `Values`/`Min`/`Max`/`Pattern`
  returns `false`.

**Spec invariants tested:** SPEC-VB-3.

Back-ref: `internal/modules/vibuilder/leaf_indexable_test.go`. Issue #487.

## TEST-VB-5: `ColumnWatermark`/`BuildSource`'s R7 coverage gate — boundary conditions and both `src.Add` sites (#496)
*Added: 2026-07-10*

**Scenario:** `ColumnWatermark.CoversRange`'s 4 branches (SPEC-VB-4) and `BuildSource`'s
gated `src.AddLeaf` call site (leaf predicates) — an absent watermark entry must behave
exactly as pre-#496.

**Setup/Assertions:**
- `TestColumnWatermark_CoversRange_DoneAlwaysTrue`, `_NeverTriggeredAlwaysFalse`,
  `_InProgressCoversOnlyFromWatermarkForward`, `_InProgressExactlyAtWatermarkBoundaryCovers`
  (`watermark_test.go`) — direct-call tests covering all 4 branches, including the explicit
  `minSec == WatermarkSec` boundary-inclusive case.
- `TestBuildSource_WatermarkGateSkipsUncoveredColumn` / `_WatermarkCoveredColumnResolves`
  (`builder_watermark_test.go`) — a leaf-predicate column with an uncovered/covered watermark
  is skipped/resolved accordingly.
- `TestBuildSource_WatermarkAbsentColumnUnaffected` — no watermark entry for a column (the
  common case, dedicated columns) behaves exactly as pre-#496 — `ok=false` in the map lookup
  degrades the gate to a no-op.
- `TestBuildSource_MixedLeaves_OneGatedOneNot_EdgeCase1` — plan.md's own "Edge Case 1: a query
  references BOTH a dedicated and a non-dedicated column in one predicate tree" — one leaf
  gated, one not, in the SAME query.
- **Superseded (task #211, 2026-07-12):** `TestBuildSource_MatchAll_WatermarkGateSkipsUncoveredColumn`
  / `_MatchAll_WatermarkCoveredColumnResolves` originally covered a SECOND `src.Add` call site
  (match-all/`Columns`-list queries) that no longer exists — that branch was removed entirely
  because it was always wasted I/O for a shape `executor.viMatchSpans` declines unconditionally
  anyway (see SPEC-VB-4's own correction). Renamed
  `TestBuildSource_NodesEmptyColumnsPopulated_DeclinesRegardlessOfWatermark` /
  `_DeclinesEvenWithWatermarkCoverage` (same file) — now pin that this shape declines
  immediately with ZERO I/O regardless of watermark state, not that the watermark gate itself
  resolves it either way.

**Spec invariants tested:** SPEC-VB-4.

Back-refs: `internal/modules/vibuilder/watermark_test.go` (4 tests),
`internal/modules/vibuilder/builder_watermark_test.go` (7 tests). Cross-referenced (not
duplicated) from `internal/modules/viusage/TESTS.md`'s own #496 test-plan record.

---

## TEST-VB-6: `LeafColumns` — per-leaf enumeration, no dedup, match-all handling, ColType resolution, root re-export parity (#496)
*Added: 2026-07-10*

**Scenario:** SPEC-VB-5's full contract: nil/empty handling, single-leaf indexable/
unindexable shapes, mixed indexable+unindexable leaves each reported, the load-bearing
no-dedup guarantee, OR-composite descent, the match-all case, `ColType` resolution, and root
re-export parity.

**Setup/Assertions (`leaf_columns_test.go`):**
- `TestLeafColumns_NilOrEmptyReturnsNil` — a nil program or a program with no `Nodes`/
  `Columns` returns `nil`.
- `TestLeafColumns_SingleEqualityLeafIsIndexable` — a plain equality leaf reports
  `Indexable: true` with its resolved `ColType`.
- `TestLeafColumns_RequirePresentLeafIsNotIndexable` / `_MultiValueLeafIsNotIndexable` — shapes
  `LeafIndexable`/`buildPredicate` already reject report `Indexable: false`.
- `TestLeafColumns_MixedIndexableAndUnindexableLeavesReportsEach` — a program with BOTH shapes
  reports one entry per leaf with the correct verdict per leaf, not a single aggregate.
- `TestLeafColumns_DuplicateColumnAcrossTwoLeavesProducesTwoEntries` — **load-bearing, the
  binding no-dedup guarantee**: the same column named by two leaves produces TWO entries, not
  one — proving `LeafColumns` does not silently collapse repeats, leaving de-duplication (if a
  caller needs it) entirely to the caller.
- `TestLeafColumns_ORCompositeDescendsIntoChildren` — an OR-composite node's children are
  individually enumerated (via `collectLeaves`'s own tree walk), not skipped or flattened
  incorrectly.
- `TestLeafColumns_MatchAllColumnListReportsEachAsIndexable` — a match-all query
  (`{} | rate()`-shaped, `Nodes` empty, `Columns` populated) reports one entry per listed
  column, `Indexable: true`, zero `ColType` (no predicate to resolve a type from).
- `TestLeafColumns_ColTypeResolvedForEachIndexableShape` — `ColType` matches
  `buildPredicate`'s own resolved type for each indexable leaf, not merely a nonzero
  placeholder.

**Root re-export parity (`valueindex_leafcolumns_test.go`, root package):**
- `TestLeafColumns_RootReexportMatchesVibuilderPackage` — `blockpack.LeafColumns` produces the
  identical result as `vibuilder.LeafColumns` for the same program (mirrors the existing
  `TestAllLeavesIndexable_RootReexportMatchesQueryplanPackage` pattern for the analogous
  aggregate function).
- `TestLeafColumns_RootReexportNilProgram` — the root re-export handles a nil program
  identically to the vibuilder function (`nil`, no panic).

**Spec invariants tested:** SPEC-VB-5.

Back-refs: `internal/modules/vibuilder/leaf_columns_test.go` (9 tests),
`valueindex_leafcolumns_test.go` (2 tests, root package).

---

## TEST-VB-7: `decidableTimeBucketThreshold` — millisecond-bucket comparison decidability, per operator, brute-force-verified (task #204, CRITICAL)
*Added: 2026-07-12*

**Scenario:** SPEC-VB-7's full contract — for every comparison operator (`>`, `>=`, `<`, `<=`,
`==`) against a millisecond-truncated dedicated time column (span:start/span:duration), the
decidability gate must agree exactly with the mathematical ground truth (whether every real
nanosecond value a stored bucket could represent agrees on the comparison's outcome), not merely
match a set of recorded expected outputs.

**Setup/Assertions (`decidability_test.go`, white-box `vibuilder` package):**
- `TestDecidableTimeBucketThreshold_MatchesBruteForceGroundTruth` — for a sweep of buckets and
  remainders across every operator, brute-forces all 1,000,000 real nanosecond values the
  ambiguous bucket could represent and asserts `decidableTimeBucketThreshold`'s `ok` verdict
  matches `bucketIsGapFree`'s ground truth exactly, and that a decidable bucket threshold equals
  `Tq` (nanos div 1e6) unchanged.
- `TestDecidableTimeBucketThreshold_TruthTable` — pins the concrete named cases from
  SPEC-VB-7's truth table, including both original bug reports (`>1ms` false-negative shape,
  `>=1.6ms`/`<=1.6ms` false-positive shape) and equality's unconditional undecidability.

**Setup/Assertions (`valueindex_boundary_decidability_test.go`, root `blockpack_test` package —
real write-path end-to-end):**
- Five real spans with exact nanosecond durations (1.000/1.400/1.500/1.999/2.000ms) written via
  `blockpack.NewWriter`, extracted through the REAL `blockpack.ExtractValueIndexEntries`
  (applies the real millisecond truncation), fed into a real `valueindex.Writer.FlushBucket`, and
  queried via `vibuilder.BuildSource` with a compiled TraceQL program — not a hand-built,
  pre-truncated fixture.
- `TestBoundaryDecidability_GTE_WholeMillisecondIsDecidable` / `_LT_WholeMillisecondIsDecidable`
  — a millisecond-aligned threshold returns the exact real match set for `>=`/`<`.
- `TestBoundaryDecidability_GTE_SubMillisecondDeclines` / `_LT_SubMillisecondDeclines` — a
  non-aligned threshold declines (`ok=false`) for `>=`/`<`, including the exact false-positive
  bug report (`>=1.6ms`).
- `TestBoundaryDecidability_GT_WholeMillisecondDeclines` — the exact false-negative bug report
  (`>1ms`) now declines instead of silently dropping real matches.
- `TestBoundaryDecidability_GT_R999999IsDecidable` / `_LTE_R999999IsDecidable` — the rare but
  real decidable case for `>`/`<=` (`Tr == 999_999`).
- `TestBoundaryDecidability_LTE_SubMillisecondDeclines` — `<=`'s non-decidable case declines.
- `TestBoundaryDecidability_EQ_AlwaysDeclines` — equality declines at BOTH a millisecond-aligned
  and a non-aligned threshold, proving there is no decidable case for `==` at all.

**Mutation-verified:** temporarily reverting `decidableTimeBucketThreshold` to task #203's
original "floor both sides, keep the operator" behavior reproduces both documented wrong-answer
shapes exactly (verified via a scratch reproduction, not left in the tree) — the `>1ms` case
drops the real 1.4/1.5/1.999ms spans (false negative) and the `>=1.6ms` case incorrectly includes
the real 1.0/1.4/1.5ms spans (false positive). Restoring the fix passes all cases again.

**Spec invariants tested:** SPEC-VB-7.

Back-refs: `internal/modules/vibuilder/decidability_test.go` (2 tests, ~70 brute-force subtests),
`valueindex_boundary_decidability_test.go` (9 tests, root package).

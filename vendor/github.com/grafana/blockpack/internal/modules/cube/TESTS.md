# Cube Module Test Plan

## TEST-CUBE-007: Chunk Encode/Decode Round-Trip

**Goal:** Verify `DecodeChunk(EncodeChunk(cells)) == cells` for all cell counts (0, 1, 2048).

**Test cases:**

- Empty chunk: `[]Cell{}` → encoded → decoded → verify empty slice
- Single cell: `[]Cell{{Minute: 100, Dim1ID: 1, Dim2ID: 2, Count: 10}}` → verify exact match
- Full chunk: 2048 cells (generate programmatically with sequential minutes) → verify all cells match
- Verify compressed size < raw size for 2048 cells (snappy effectiveness)

**Acceptance:** All test cases pass, no panics on decode.

**File:** `internal/modules/cube/chunk_test.go:TestChunkEncodeDecodeRoundTrip`

---

## TEST-CUBE-008: Chunk Snappy Compression

**Goal:** Verify snappy-compressed chunks produce smaller output than raw input for realistic cell counts.

**Test cases:**

- 2048 cells with varied dimensions (100 distinct dim1, 20 distinct dim2) → verify `len(compressed) < len(raw)`
- Measure compression ratio: expect ~60-70% compression (24 KB → 8-12 KB)

**Acceptance:** Compression ratio within expected range, no decompression errors.

**File:** `internal/modules/cube/chunk_test.go:TestChunkCompressionRatio`

---

## TEST-CUBE-009: Reader GetCell (Hit and Miss Cases)

**Goal:** Verify `Reader.GetCell(minute, dim1, dim2)` returns correct count for present cells and `(0, false)` for absent cells.

**Test setup:** Build a test file with 300 cells in 3 chunks:

- Chunk 0: minutes 100-199, dim1="auth", dim2="200", counts [1..100]
- Chunk 1: minutes 200-299, dim1="auth", dim2="200", counts [101..200]
- Chunk 2: minutes 300-399, dim1="billing", dim2="404", counts [201..300]

**Test cases:**

- **Hit in chunk 0:** `GetCell(150, "auth", "200")` → `(51, true)`
- **Hit in chunk 2:** `GetCell(350, "billing", "404")` → `(251, true)`
- **Miss (minute out of range):** `GetCell(9999, "auth", "200")` → `(0, false)`
- **Miss (dim1 not in dict):** `GetCell(150, "nonexistent", "200")` → `(0, false)`
- **Miss (dim2 not in dict):** `GetCell(150, "auth", "999")` → `(0, false)`

**Acceptance:** All hit cases return correct count, all miss cases return `(0, false)`, no panics.

**File:** `internal/modules/cube/reader_test.go:TestReaderGetCell`

---

## TEST-CUBE-010: Reader GetCellsInRange (Chunk Pruning)

**Goal:** Verify `Reader.GetCellsInRange(minMinute, maxMinute, dim1, dim2)` returns only cells in the time range and prunes chunks outside the range.

**Test setup:** Build a test file with 5 chunks spanning minutes 0-499 (100 cells/chunk).

**Test cases:**

- **Full range:** `GetCellsInRange(0, 499, dim1, dim2)` → all 500 cells
- **Partial range:** `GetCellsInRange(120, 230, dim1, dim2)` → cells in minutes 120-230 only
- **Empty range:** `GetCellsInRange(600, 700, dim1, dim2)` → no cells
- **Cross-chunk range:** `GetCellsInRange(90, 310, dim1, dim2)` → cells from chunks 0,1,2,3
- **Single-minute range:** `GetCellsInRange(150, 150, dim1, dim2)` → 1 cell (minute 150)

**Acceptance:** All ranges return correct cell subsets, chunk pruning verified via instrumentation (log or count decompressed chunks).

**File:** `internal/modules/cube/reader_test.go:TestReaderGetCellsInRange`

---

## TEST-CUBE-011: Reader Binary Search Correctness

**Goal:** Verify binary search on chunk directory and within-chunk cells is correct under edge cases.

**Test cases:**

- **Exact boundary:** Cell at `minute = chunk[i].MinMinute` (first cell in chunk) → found in chunk i, not chunk i-1
- **Last cell in chunk:** Cell at last minute in chunk 0 → found in chunk 0, not chunk 1
- **Between chunks:** Query minute falls between chunk 0 max and chunk 1 min → return `(0, false)` (no such cell)
- **Single-chunk file:** Binary search with only 1 chunk (edge case: no directory scan)

**Acceptance:** All edge cases return correct results, no off-by-one errors.

**File:** `internal/modules/cube/reader_test.go:TestReaderBinarySearchEdgeCases`

---

## TEST-CUBE-012: End-to-End Round-Trip (Writer → Reader)

**Goal:** Verify `Writer.Flush()` → `Reader.OpenReader()` → `Reader.GetCell()` preserves all cell counts exactly.

**Test setup:**

1. Generate 5000 random cells: random minute in [0, 10000], 100 distinct dim1 values, 20 distinct dim2 values, random count in [1, 1000]
2. Write all cells via `Writer.AddCell()`, then `Writer.Flush()` to temp file
3. Open temp file via `Reader.OpenReader()`
4. For each original cell: verify `Reader.GetCell(minute, dim1, dim2)` returns exact count

**Test cases:**

- All 5000 cells found with correct counts
- Query non-existent cell → `(0, false)`
- Verify file size is reasonable (sparse storage, not bloated)

**Acceptance:** 100% of cells found, no false positives, file size < 100 KB (5000 cells × 12 bytes = 60 KB raw + overhead).

**File:** `internal/modules/cube/reader_test.go:TestReaderEndToEndRoundTrip` (integration test)

---

## Coverage Goals

- **Target coverage:** >80% across all cube module files
- **Priority:**
  - 100% coverage on encode/decode functions (critical correctness)
  - 100% coverage on binary search logic (edge cases)
  - 80%+ on Reader.GetCell/GetCellsInRange (query path)
  - 70%+ on Writer (optional, can defer to #443)

## Ingest accumulation (issue #443)

- **TEST-CUBE-013** `TestAccumulatorAdd` — spans increment matching cells; distinct
  `(dim1,dim2)` pairs produce distinct cells.
- **TEST-CUBE-013b** `TestCubeFilename` — `<tenant>/cubes/<hex id>/L0-<xid>.cube` layout.
- **TEST-CUBE-014** `TestAccumulatorFilter` — `duration < threshold` (and missing column) not counted.
- **TEST-CUBE-015** `TestAccumulatorSkipsMissingDims` — span missing either dimension is skipped.
- **TEST-CUBE-016** `TestAccumulatorFlushRoundTrip` — `FlushTo` → store → `cube.Reader` returns
  exact counts; every cell carries the accumulator's minute.
- **TEST-CUBE-017** `TestAccumulatorResetNoDoubleCount` — flush resets; next minute does not carry over.
- **TEST-CUBE-018** `TestAccumulatorFlushEmptyNoOp` — idle minute writes nothing.
- **TEST-CUBE-019** `TestWriterEncodeMatchesFlush` — `Encode()` bytes equal the `Flush()` file.

---

## TEST-CUBE-020..060 — Registry/CardinalityGate/CreationTrigger/Backfill/Rollup/Router/Compactor suite (issues #444-#453, #480)

Landed across the `f9c39e74` epic commit (2026-06-29) and the #480 filter-identity fix
(2026-07-06); each ID below is a real, implemented, correctly-numbered test.

| ID | Test | File | What it pins |
|---|---|---|---|
| TEST-CUBE-020 | `TestComputeCubeID_Stable` | definition_test.go | Same inputs always produce the same CubeID |
| TEST-CUBE-021 | `TestComputeCubeID_DimOrderNormalized` | definition_test.go | Reversed dimension order gives the same ID |
| TEST-CUBE-022 | `TestComputeCubeID_DifferentTenants` | definition_test.go | Different tenants produce different IDs |
| TEST-CUBE-023 | `TestComputeCubeID_FilterOrderNormalized` | definition_test.go | Filter order doesn't affect the ID |
| TEST-CUBE-024 | `TestRegistryEntry_JSONRoundTrip` | definition_test.go | RegistryEntry survives a JSON round-trip |
| TEST-CUBE-025 | `TestIDFromBytes_RoundTrip` | definition_test.go | IDFromBytes round-trips a hex CubeID |
| TEST-CUBE-026 | `TestCheckCardinality_EmptyDataPasses` | cardinality_test.go | nil/empty VCNT data → gate passes (no data = no restriction) |
| TEST-CUBE-027 | `TestCheckCardinality_TooManyDistinctRejects` | cardinality_test.go | >1000 distinct values → rejected |
| TEST-CUBE-028 | `TestCheckCardinality_UUIDRejects` | cardinality_test.go | UUID/high-entropy columns always rejected |
| TEST-CUBE-029 | `TestCheckCardinality_CombinedLimitEnforced` | cardinality_test.go | Combined dim1×dim2 cell-count limit enforced |
| TEST-CUBE-030 | `TestCheckCardinality_LowCardinalityPasses` | cardinality_test.go | Low-cardinality dimensions pass |
| TEST-CUBE-031 | `TestRegistry_EmptyStoreReturnsEmptyIndex` | registry_test.go | Empty store → empty index, not an error |
| TEST-CUBE-032 | `TestRegistry_AddLoadRoundTrip` | registry_test.go | Add then Load returns what was added |
| TEST-CUBE-033 | `TestRegistry_AddIdempotent` | registry_test.go | Adding the same CubeID twice yields one entry |
| TEST-CUBE-034 | `TestRegistry_ConcurrentAddSameID` | registry_test.go | Concurrent Add with the same CubeID produces exactly one entry |
| TEST-CUBE-035 | `TestRegistry_RemoveIdempotent` | registry_test.go | Remove deletes the cube; removing an absent cube is a no-op |
| TEST-CUBE-036 | `TestRegistry_NoLimitEnforced_UnboundedCubeCount` | registry_test.go | No per-tenant cube-count limit (removed, issue #497): adding 1500 distinct cubes for one tenant via `Registry.Add` succeeds with no error, and `Load` returns all 1500 |
| TEST-CUBE-037 | `TestRegistry_IsActive` | registry_test.go | IsActive returns correct results |
| TEST-CUBE-038 | `TestRegistry_IndexJSONValid` | registry_test.go | index.json is valid JSON with a version field |
| TEST-CUBE-039 | `TestCreationTrigger_FirstQueryCreates` | trigger_test.go | First query for a pattern triggers cube creation |
| TEST-CUBE-040 | `TestCreationTrigger_SecondCallNoDuplicate` | trigger_test.go | Second call for the same pattern returns the existing entry, not a duplicate |
| TEST-CUBE-041 | `TestCreationTrigger_CardinalityRejectionPropagates` | trigger_test.go | Cardinality-gate rejection propagates as an error |
| TEST-CUBE-042 | `TestTrigger_NoLimitEnforced_UnboundedCubeCount` | trigger_test.go | No per-tenant cube-count limit (removed, issue #497): `CreationTrigger.TryCreate` registers 1100 distinct patterns for one tenant with no error, and the registry ends up holding all 1100 |
| TEST-CUBE-043 | `TestCreationTrigger_ConcurrentFirstQueries` | trigger_test.go | Concurrent first queries for the same pattern produce exactly one entry |
| TEST-CUBE-044 | `TestBackfiller_SingleDimWritesFile` | backfill_test.go | Single-dimension backfill writes a cube file for a minute with data |
| TEST-CUBE-045 | `TestBackfiller_EmptyMinuteWritesNothing` | backfill_test.go | Empty minute writes nothing (sparse cube) |
| TEST-CUBE-046 | `TestBackfiller_WatermarkAdvances` | backfill_test.go | Watermark advances correctly; progress called for each minute |
| TEST-CUBE-047 | `TestBackfiller_CtxCancellationStops` | backfill_test.go | Context cancellation stops the backfill loop |
| TEST-CUBE-048 | `TestRollup_L0ToL1SumsHour` | rollup_test.go | L0→L1 rollup sums 60 minutes into 1 hourly bucket |
| TEST-CUBE-049 | `TestRollup_DisjointDictMerge` | rollup_test.go | Dictionary merge handles disjoint value sets correctly |
| TEST-CUBE-050 | `TestRollupToWriter_ProducesReadableFile` | rollup_test.go | RollupToWriter produces a readable cube file with correct counts |
| TEST-CUBE-051 | `TestRollup_EmptyInputsReturnNil` | rollup_test.go | Empty inputs return nil, not an error |
| TEST-CUBE-052 | `TestQueryRouter_ExactMatch` | router_test.go | Exact match routes to the cube |
| TEST-CUBE-053 | `TestQueryRouter_NoMatch` | router_test.go | No cube match returns Found=false |
| TEST-CUBE-054 | `TestQueryRouter_ResolutionSnapping` | router_test.go | Resolution snapping selects the correct rollup level |
| TEST-CUBE-055 | `TestResolutionLevel` | router_test.go | ResolutionLevel standalone function, all three levels |
| TEST-CUBE-056 | `TestPlanL0Merge_MeetsThreshold` | compactor_test.go | PlanL0Merge returns a plan when ≥ threshold files cover the same hour |
| TEST-CUBE-057 | `TestPlanL0Merge_BelowThreshold` | compactor_test.go | PlanL0Merge returns nothing when below threshold |
| TEST-CUBE-058 | `TestCompactor_ExecuteMergesAndDeletes` | compactor_test.go | Execute merges cells and deletes input files (pure L0 merge case) |
| TEST-CUBE-059 | `TestCompactor_ShouldEvictAccuracy` | compactor_test.go | ShouldEvict returns true for old last-queried timestamps |
| TEST-CUBE-060 | `TestRouter_FilterDifferentiatesCubes` | router_test.go | Two cubes with identical dims but different filters must not share a cube; filtered/unfiltered queries must not cross-route (issue #480; renumbered from an original 056 collision with TestPlanL0Merge_MeetsThreshold via task E-15) |

---

## TEST-CUBE-061..097 — aggAttrs / wire-format-v2 / router-watermark suite (issue #491)

Every ID below was assigned by spec-oracle-e before or immediately after landing, per this
phase's ID-routing convention (FOOTGUN 5, `.bob/state/spec-knowledge-phase-e.md`).

| ID | Test | File | What it pins |
|---|---|---|---|
| TEST-CUBE-061 | `TestComputeCubeID_DifferentAggAttrs_ProducesDifferentIDs` | definition_test.go | Differing only in aggAttrs content produces different CubeIDs — the attribute SET joins identity (ruling 3) |
| TEST-CUBE-062 | `TestComputeCubeID_AggAttrsOrderIndependent` | definition_test.go | aggAttrs order does not affect the CubeID |
| TEST-CUBE-063 | `TestComputeCubeID_AlwaysIncludesFourthSegment` | definition_test.go | No code path produces the old 3-segment-only hash anymore (replaces the vetoed byte-identity test) |
| TEST-CUBE-064 | `TestComputeDimsFiltersKey_IsThe3SegmentPrefixOfComputeCubeID` | definition_test.go | computeDimsFiltersKey is a stable, independently-testable value ComputeCubeID builds on |
| TEST-CUBE-065 | `TestRegistryEntry_AggAttrs_RoundTripsThroughJSON` | definition_test.go | RegistryEntry.AggAttrs round-trips through JSON |
| TEST-CUBE-066 | `TestComputeCubeID_EmptyAggAttrs_StillHashesFourthSegment` | definition_test.go | nil/empty aggAttrs is a LIVE production path (router.go/trigger.go pass nil pending E-6b/E-10) yet the vetoed len==0 shortcut was never silently reintroduced (fix per reviewer-e2 mutation finding #38) |
| TEST-CUBE-067 | `TestValidateDefinition_RejectsMissingDuration` | accumulator_test.go | THE test pinning the third-round clamp: a Definition whose AggAttrs omits DurationColumn is rejected by NewAccumulator |
| TEST-CUBE-068 | `TestValidateDefinition_AcceptsDurationPlusOtherAttrs` | accumulator_test.go | Positive case — AggAttrs containing duration plus a second attr is accepted |
| TEST-CUBE-069 | `TestAccumulator_Add_SumMinMaxSampleCount_SingleAggAttr` | accumulator_test.go | Known non-power-of-two duration values accumulate exact Sum/Min/Max/SampleCount |
| TEST-CUBE-070 | `TestAccumulator_Add_Buckets_MatchesLog2Bucketize` | accumulator_test.go | Buckets[] exactly matches Log2Bucketize's boundaries across several power-of-two buckets |
| TEST-CUBE-071 | `TestAccumulator_Add_DurationBelow2ns_ExcludedFromBucketsNotFromCount` | accumulator_test.go | duration=1 (<2ns) still increments base Count/SampleCount/Sum but contributes nothing to Buckets[] (ruling 1) |
| TEST-CUBE-072 | `TestAccumulator_Add_FloatTypedAttr_NeverPopulatesBuckets` | accumulator_test.go | A second, Float64-typed attribute accumulates Sum/Min/Max but its Buckets stays all-zero (ruling 1 scope boundary) |
| TEST-CUBE-073 | `TestAccumulator_Encode_RoundTripsThroughRealWriter` | accumulator_test.go | Accumulator.Encode() → OpenReaderFromBytes → GetAggCellsInRange round-trips every AggAttrValues field exactly (real write-path proof) |
| TEST-CUBE-074 | `TestCreationTrigger_TryCreate_RejectsDefinitionMissingDuration` | trigger_test.go | validateDefinition's second call site (TryCreate) rejects a Definition missing duration |
| TEST-CUBE-075 | `TestCheckCardinality_RejectsOnByteCost_EvenWhenCellCountUnderLimit` | cardinality_test.go | Byte-cost check rejects a cube even when the plain cell-count check would pass |
| TEST-CUBE-076 | `TestCheckCardinality_ErrorIsCardinalityError` | cardinality_test.go | Byte-cost rejection returns a *CardinalityError (errors.As-comparable) |
| TEST-CUBE-077 | `TestCompactor_Execute_DeletesL0MergeInputsImmediately` | compactor_test.go | A pure L0-to-L0 merge (plan.Level==RollupL0) deletes its inputs immediately |
| TEST-CUBE-078 | `TestCompactor_Execute_NoLongerDeletesL0InputsImmediately` | compactor_test.go | An L0→L1 rollup (plan.Level==RollupL1) does NOT delete its L0 inputs immediately |
| TEST-CUBE-079 | `TestCompactor_Execute_StillDeletesL1InputsAfterL2Rollup` | compactor_test.go | An L1→L2 rollup (plan.Level==RollupL2) deletes its L1 inputs immediately — retention is L0-specific |
| TEST-CUBE-080 | `TestCompactor_Execute_UpdatesWatermarkOnSuccess` | compactor_test.go | Execute updates RegistryEntry.Watermarks[plan.Level] on every successful rollup write |
| TEST-CUBE-081 | `TestCompactor_EvictAgedL0_DeletesOnlyPastRetentionAndAlreadyRolledUp` | compactor_test.go | EvictAgedL0 deletes only when BOTH past L0RetentionMinutes AND covered by Watermarks[RollupL1] |
| TEST-CUBE-082 | `TestCompactor_EvictAgedL0_NeverDeletesFileWithinRetentionWindow` | compactor_test.go | A file within the retention window is never evicted, regardless of watermark coverage |
| TEST-CUBE-083 | `TestCompactor_EvictAgedL0_NeverDeletesFileNotYetRolledUp` | compactor_test.go | A file past retention but not yet covered by the L1 watermark is never evicted |
| TEST-CUBE-084 | `TestRegistry_UpdateWatermarks_MergesExistingRange` | registry_test.go | UpdateWatermarks expands (min-of-mins, max-of-maxes) an existing range rather than replacing it |
| TEST-CUBE-085 | `TestRegistry_UpdateWatermarks_CubeNotFoundReturnsError` | registry_test.go | UpdateWatermarks against an unregistered CubeID returns an error |
| TEST-CUBE-086 | `TestRegistry_UpdateWatermarks_ConditionalPutRetryDiscipline` | registry_test.go | UpdateWatermarks uses the same 5-retry/50ms-doubling conditional-PUT discipline as Add/Remove |
| TEST-CUBE-087 | `TestBackfill_RealWriteReadPath_JoinsByTraceAndSpanID_NotJustTrace` | backfill_test.go | Real-write-path proof (Writer.AddEntryV4→Flush→OpenReader→Reader.Lookup) that the (TraceID,SpanID) join correctly attributes per-span aggAttr values without cross-contaminating trace siblings (fix #46) |
| TEST-CUBE-088 | `TestRoute_SupersetTieBreak_PrefersSmallestCoveringSet` | router_test.go | Route prefers the smallest AggAttrs superset that still covers neededAttr |
| TEST-CUBE-089 | `TestRoute_SupersetTieBreak_OnlySupersetCoversNeededAttr` | router_test.go | Only a genuine superset (containing neededAttr) is eligible, not any dims+filters match |
| TEST-CUBE-090 | `TestRoute_SupersetTieBreak_EqualSetsBreakByNewestCreatedAt` | router_test.go | Equal-size AggAttrs sets break ties by newest CreatedAt |
| TEST-CUBE-091 | `TestRoute_NeededAttrEmpty_MatchesAnyCandidate` | router_test.go | neededAttr=="" matches any candidate regardless of its attribute set |
| TEST-CUBE-092 | `TestRoute_ResolutionCompleteness_DeclinesWhenWatermarkDoesNotCoverFullWindow` | router_test.go | A partial watermark at the chosen resolution declines the whole query |
| TEST-CUBE-093 | `TestRoute_ResolutionCompleteness_FullCoverageSucceeds` | router_test.go | Full watermark coverage at the chosen resolution succeeds |
| TEST-CUBE-094 | `TestRoute_ResolutionCompleteness_NoWatermarkAtAllDeclines` | router_test.go | A missing watermark for the chosen resolution declines the whole query |
| TEST-CUBE-095 | `TestValidateFileMatchesRegistry_MatchSucceeds` | router_test.go | ValidateFileMatchesRegistry succeeds when file NumAggAttrs matches the registry entry's AggAttrs count |
| TEST-CUBE-096 | `TestValidateFileMatchesRegistry_MismatchReturnsTypedError` | router_test.go | A count mismatch returns a typed error, not a silent pass |
| TEST-CUBE-097 | `TestValidateFileMatchesRegistry_ErrorIsAggAttrsMismatchError` | router_test.go | The returned error is an *AggAttrsMismatchError (errors.As-comparable) |

## TEST-CUBE-098..102 — Phase E fix pass (issue #491, review.md/go-presubmit.md, 2026-07-08)

| ID | Test | File | Scenario/Setup/Assertions |
|----|------|------|---------------------------|
| TEST-CUBE-098 | `TestCubeIngest_PublicAPI_LoadCubeDefinitions_CopiesAggAttrs` | cube_ingest_publicapi_test.go (root package) | Setup: real Registry (Add/Load) backed by an in-memory ObjectStore fake, a realistic RegistryEntry with AggAttrs populated. Assertion: LoadCubeDefinitions' resulting Definition.AggAttrs is copied from entry.AggAttrs with correct type-defaulting (DurationColumn→Int64, other→Float64), and NewCubeAccumulator succeeds on it. Mutation-verified: reverting the AggAttrs copy in CubeRegistryEntryToDefinition fails this test. |
| TEST-CUBE-099 | `TestBackfill_ProcessMinute_AppliesRegisteredFilter` | backfill_test.go | Setup: a filtered RegistryEntry (duration > threshold), two VI-backed spans (one above, one below threshold). Assertion: only the above-threshold span is counted in the resulting cube file (Count, SampleCount, Sum). Mutation-verified: reverting the Filters wiring in processMinute's Definition literal, OR reverting the Int64 landmine fix, both independently fail this test. |
| TEST-CUBE-100 | `TestPlanL0Merge_ExcludesHourStraddlingFile` | compactor_test.go | Setup: one previously-merged L0 file whose own span straddles an hour boundary, plus enough well-contained hour-0 files to hit the merge threshold. Assertion: the straddling file is never included in any plan; the well-contained hour still merges normally, with output fully contained in that hour. Mutation-verified: reverting to grouping purely by MinMinute/60 (no hourMin!=hourMax guard) fails this test. |
| TEST-CUBE-101 | `TestColumnFilterToFilter` | definition_test.go | Table test covering every representable ColumnFilter.Value shape (numeric string, Go-duration string, direct float64/int, non-numeric string via EQ fallback to StringFilter) plus the unrepresentable case (GT on a non-numeric string, which must return nil). |
| TEST-CUBE-102 | `TestColumnFilterToFilter_MissingColumnRejected` | definition_test.go | A span missing the filtered column is rejected by both the NumericFilter-backed and StringFilter-backed conversion paths — a missing column can never satisfy a filter. |

## TEST-CUBE-103 — Cube-bytes capture for byte-breakdown metrics (issue #218, Phase 5)

| ID | Test | File | Scenario/Setup/Assertions |
|----|------|------|---------------------------|
| TEST-CUBE-103 | `TestReader_BytesRead_ReportsExactInputLength` | reader_test.go | Setup: a real cube file built via Writer.AddAggCell→Encode, opened both via OpenReaderFromBytes(data) and via OpenReader(path) (data written to a temp file first). Assertion: BytesRead() equals exactly len(data) for both open paths — mutation-verified (an off-by-one mutation on BytesRead()'s return was confirmed to fail this test, then reverted). |

## TEST-CUBE-104/105 — Log2Bucketize 1<<64 overflow bounds-safety fix (NOTE-CUBE-028)

| ID | Test | File | Scenario/Setup/Assertions |
|----|------|------|---------------------------|
| TEST-CUBE-104 | `TestLog2Bucketize_PathologicallyLargeValueExcludedNotOOB` | bucket_test.go | v=2^63 still ceilings correctly to bucket 63 (the largest valid boundary); v=2^63+1 and v=MaxUint64 both return the -1 sentinel instead of the pre-fix 1<<64-wraps-to-0 result. |
| TEST-CUBE-105 | `TestAccumulator_Add_PathologicallyLargeDuration_ExcludedNotOOB` | accumulator_test.go | A span whose duration is the smallest float64 strictly greater than 2^63 flows through the real Add→addAggAttrs path without panicking; SampleCount/Sum still reflect the sample but every Buckets[] slot stays zero. Mutation-verified: reverting the bucket.go fix reproduces the exact pre-fix `index out of range [64] with length 64` panic at accumulator.go's Buckets write. |


## TEST-CUBE-106..112 — Native Postgres EntryStore (issue #506)

| ID | Test | File | What it pins |
|---|---|---|---|
| TEST-CUBE-106 | `TestPgEntryStore_AddEntry_IdempotentAndUnboundedCubeCount` | pg_entry_store_test.go | AddEntry is idempotent on a repeated CubeID; a second, distinct CubeID for the same tenant succeeds unconditionally (no cardinality gate, issue #497) |
| TEST-CUBE-107 | `TestPgEntryStore_RemoveEntry_IdempotentOnAbsent` | pg_entry_store_test.go | RemoveEntry on an absent cube is a no-op; removing a present cube deletes it |
| TEST-CUBE-108 | `TestPgEntryStore_UpdateWatermarksEntry_MinOfMinsMaxOfMaxes` | pg_entry_store_test.go | UpdateWatermarksEntry expands (min-of-mins, max-of-maxes) an existing range rather than replacing it, across a narrowing then a widening call |
| TEST-CUBE-109 | `TestPgEntryStore_UpdateWatermarksEntry_NotFoundErrors` | pg_entry_store_test.go | UpdateWatermarksEntry against an unregistered CubeID returns an error |
| TEST-CUBE-110 | `TestPgEntryStore_Load_ReturnsAllRowsForTenant_NotOtherTenants` | pg_entry_store_test.go | Load returns exactly one tenant's rows, never leaking another tenant's entries |
| TEST-CUBE-111 | `TestPgEntryStore_AddEntry_ConcurrentFirstCubeRegistration_NoDuplicateOrRace` | pg_entry_store_test.go | 20 goroutines racing AddEntry for the SAME brand-new CubeID on a zero-cube tenant produce exactly one persisted row — the specific race `pg_advisory_xact_lock` exists to prevent, since the first cube for a tenant has no row to `SELECT ... FOR UPDATE`. Mutation-verified (see NOTE-CUBE-030): lock removal alone did not reliably fail; a temporary artificial delay was added to make the race deterministic, confirmed to fail with a real duplicate-key violation, then reverted. |
| TEST-CUBE-112 | `TestCubeRegistry_BlobAndPgBackends_IdenticalBehavior` | pg_blob_differential_test.go | Runs the identical Add/Remove/UpdateWatermarks/duplicate-Add operation sequence against both a blob-backed and a Postgres-backed Registry; asserts the resulting Load() sets are field-equal — proves SPEC-CUBE-031's behavioral-identity invariant end-to-end, not just structurally |

**Next free ID: TEST-CUBE-113.**

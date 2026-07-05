# valuecountscompactor — Test Specifications

This document defines the required tests for the `internal/modules/valuecountscompactor`
package (plus its public re-export package, `valuecountscompactor/`), per root `SPEC.md`
SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-VC-N` (file-scoped per
SPEC-ROOT-009 — this file's own sequence, numbering from 1, independent of
`internal/modules/valuecounts/TESTS.md`'s own separate `TEST-VC-N` sequence). IDs are assigned
in ascending order and never reused or renumbered.

Next free ID: **TEST-VC-25**.

---

## TEST-VC-1: TestConfig_WithDefaults
*Added: 2026-07-02*

**Scenario:** A zero-value `Config`'s `withDefaults()` fills every defaultable field.

**Assertions:** `IndexPrefix`, `CompactInterval`, `CompactThresholdFiles`, `CompactBatchBytes`,
and `MaxRecordsPerMerge` all equal their `Default*` constants.

Back-ref: `internal/modules/valuecountscompactor/config_test.go:TestConfig_WithDefaults`.

---

## TEST-VC-2: TestConfig_WithDefaults_PreservesSet
*Added: 2026-07-02*

**Scenario:** `withDefaults()` must not overwrite explicitly-set fields.

**Assertions:** A `Config` with every defaultable field set to a non-zero custom value survives
`withDefaults()` unchanged.

Back-ref: `internal/modules/valuecountscompactor/config_test.go:TestConfig_WithDefaults_PreservesSet`.

---

## TEST-VC-3: TestConfig_AllTenants
*Added: 2026-07-02*

**Scenario:** `Config.allTenants()` is true iff `Tenants == ["*"]` exactly.

**Assertions:** `["*"]` → true; `["t1"]` → false; `["*", "t1"]` → false; `nil` → false.

Back-ref: `internal/modules/valuecountscompactor/config_test.go:TestConfig_AllTenants`.

---

## TEST-VC-4: TestFakeStore_ListDirsOneLevel
*Added: 2026-07-02*

**Scenario:** The `fakeStore` test harness's `ListDirs` proves the one-level directory-walk
shape VCNT's compactor relies on (no `<type>` segment, unlike VI's three-level walk).

**Assertions:** Given two objects under distinct column-hash subdirectories,
`ListDirs("t1/indexes/unique_values/")` returns exactly those two immediate child directory
prefixes.

Back-ref: `internal/modules/valuecountscompactor/store_test.go:TestFakeStore_ListDirsOneLevel`.

---

## TEST-VC-5: TestNewCompactorMetrics_NilRegisterer
*Added: 2026-07-02*

**Scenario:** A nil `Registerer` must yield a nil `*compactorMetrics`, and every method on a nil
receiver must be a safe no-op.

**Assertions:** `newCompactorMetrics(nil)` is nil; calling every metrics method on the nil
receiver does not panic.

Back-ref: `internal/modules/valuecountscompactor/metrics_test.go:TestNewCompactorMetrics_NilRegisterer`.

---

## TEST-VC-6: TestNewCompactorMetrics_DoubleRegister
*Added: 2026-07-02*

**Scenario:** Registering metrics twice against the same `*prometheus.Registry` (e.g. two
`Service` instances sharing a registerer) must reuse existing collectors, not panic or create
duplicates.

**Assertions:** Two `newCompactorMetrics(reg)` calls against the same registry return
`*compactorMetrics` whose `runs`/`errors` collectors are the same pointer (`assert.Same`);
`reg.Gather()` succeeds.

Back-ref: `internal/modules/valuecountscompactor/metrics_test.go:TestNewCompactorMetrics_DoubleRegister`.

---

## TEST-VC-7: TestBuildWorkList_OneLevelWalk
*Added: 2026-07-02*

**Scenario:** `buildWorkList` discovers all `(tenant, colDir)` pairs via the one-level
`unique_values/<colHash>/` walk.

**Setup:** Two columns' worth of L0 files under one tenant.

**Assertions:** `buildWorkList` returns exactly 2 work items, one per column hash, both tagged
with the correct tenant.

Back-ref: `internal/modules/valuecountscompactor/service_internal_test.go:TestBuildWorkList_OneLevelWalk`.

---

## TEST-VC-8: TestOwnsShard_MatchesValueIndexConvention
*Added: 2026-07-02*

**Scenario:** Guards the implicit cross-module hash-construction coupling between
`valuecounts.ColHash` and `valueindex.ColHash` (`valuecountscompactor` NOTE-VC-007) — they are
not compile-time linked, but `ownsShard`'s sharding decision depends on both packages producing
byte-identical hashes for the same column name.

**Assertions:** A `ShardCount=1` service owns every column (fail-open no-op case). For a
`ShardCount=4` service, `valuecounts.ColHash("span:name")` and `valueindex.ColHash("span:name")`
are asserted byte-identical, and `ownsShard` returns the same result for both.

**Spec invariants tested:** documents the coupling risk described in NOTE-VC-007; not itself a
SPEC-VC entry (no independent invariant of this package's own contract is being verified beyond
"ownsShard is deterministic given a hash string").

Back-ref: `internal/modules/valuecountscompactor/service_internal_test.go:TestOwnsShard_MatchesValueIndexConvention`.

---

## TEST-VC-9: TestCompactColumn_LowestLevelMeetingThreshold
*Added: 2026-07-02*

**Scenario:** `compactColumn` must compact only the lowest compaction level meeting
`CompactThresholdFiles`, leaving higher levels untouched in the same pass (single-pass-per-level
discipline).

**Setup:** 8 L0 files + 8 L1 files in one column directory, `CompactThresholdFiles=8`.

**Assertions:** All 8 L0 files are processed (removed); all 8 L1 files remain untouched; the
`columnsCompacted{level="0"}` counter is 1 and `{level="1"}` is 0.

**Spec invariants tested:** SPEC-VC-1.

Back-ref: `internal/modules/valuecountscompactor/service_internal_test.go:TestCompactColumn_LowestLevelMeetingThreshold`.

---

## TEST-VC-10: TestCompactColumn_MalformedFilenamesSkipped
*Added: 2026-07-02*

**Scenario:** An unparseable filename must survive untouched (not deleted) — unlike
`valueindexcompactor`'s magic-byte-purge branch, VCNT files carry no magic header, so a
name-parse failure alone does not prove the object is garbage.

**Setup:** One non-`.vcnt`-suffixed garbage key plus 8 valid L0 files, `CompactThresholdFiles=8`.

**Assertions:** The garbage key still exists after `compactColumn` runs; all 8 valid L0 files
are processed; `files_skipped_total` is 1.

Back-ref: `internal/modules/valuecountscompactor/service_internal_test.go:TestCompactColumn_MalformedFilenamesSkipped`.

---

## TEST-VC-11: TestCompactColumn_BatchBytesCap
*Added: 2026-07-02*

**Scenario:** `CompactBatchBytes` caps the oldest-sorted subset of files admitted into one
merge; the remainder is deferred to a later pass while still meeting the
`CompactThresholdFiles` progress floor.

**Setup:** 4 files of 100 bytes each, `CompactThresholdFiles=2`, `CompactBatchBytes=250`.

**Assertions:** At least 2 files are processed (the threshold floor); fewer than all 4 fit
under the byte cap; at least one file is deferred (remains in place).

**Spec invariants tested:** SPEC-VC-2 (the complementary byte-based gate in `compactColumn`,
distinct from `mergeLevel`'s decoded-record-count gate).

Back-ref: `internal/modules/valuecountscompactor/service_internal_test.go:TestCompactColumn_BatchBytesCap`.

---

## TEST-VC-12: TestResolveTenants_AllTenantsDiscovery
*Added: 2026-07-02*

**Scenario:** `Tenants: ["*"]` discovers all tenants by listing, and each compacts
independently via `RunOnce`.

**Setup:** Two tenants ("alpha", "beta"), each with 8 L0 files for the same column.

**Assertions:** Both tenants end up with exactly one L1 file each after `RunOnce`.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestResolveTenants_AllTenantsDiscovery`.

---

## TEST-VC-13: TestMergeLevel_MergesAndDeletesInputs
*Added: 2026-07-02*

**Scenario:** `mergeLevel` (driven via `RunOnce`/`compactColumn`, since `mergeLevel` is
unexported) correctly merges overlapping keys, drops a net-dead group, writes exactly one
output file at `level+1`, and deletes inputs only after the output `Put` succeeds.

**Setup:** 3 L0 inputs: two share a merge key (`Count` 5 and 3, summing to 8), one is a
distinct key with `Count=-2` (nets to `<=0`, must be dropped).

**Assertions:** All 3 inputs are deleted; exactly one output file exists at level 1; its
decoded content is one record (`span:name`, value `"v"`, `Count=8"`); the dead-key group does
not appear; `records_read_total`=3, `records_written_total`=1.

**Spec invariants tested:** SPEC-VC-1.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_MergesAndDeletesInputs`.

---

## TEST-VC-14: TestMergeLevel_PutFailureKeepsInputs
*Added: 2026-07-02*

**Scenario:** Write-then-delete crash safety — a failing `Put` to the L1 output key must leave
every input in place.

**Setup:** 3 L0 inputs; a `putFailStore` wrapper fails every `Put` to a key containing `"L1-"`.

**Assertions:** `RunOnce` returns an error; all 3 inputs still exist afterward.

**Spec invariants tested:** SPEC-VC-1 (write-then-delete crash safety rule).

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_PutFailureKeepsInputs`.

---

## TEST-VC-15: TestMergeLevel_AllRecordsNetZero_NoOutputWritten
*Added: 2026-07-02*

**Scenario:** When every group in a merge nets to `<= 0`, `valuecounts.Compact` returns nil, so
`mergeLevel` must write zero output files while still deleting the (fully-dropped) inputs.

**Setup:** 2 L0 inputs sharing a key with `Count` 5 and -5 (nets to exactly 0).

**Assertions:** After `RunOnce`, the store is empty — no output file was written, and both
(fully-dropped) inputs were deleted.

**Spec invariants tested:** SPEC-VC-1.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_AllRecordsNetZero_NoOutputWritten`.

---

## TEST-VC-16: TestMergeLevel_MaxRecordsPerMerge_DefersRemainderToNextPass
*Added: 2026-07-02*

**Scenario:** A small `MaxRecordsPerMerge` stops admission mid-batch once the decoded-record
ceiling is reached (after the `CompactThresholdFiles` progress floor); processed files' output
is written and only those inputs deleted; deferred files remain untouched.

**Setup:** 5 L0 files (1 record each via `putL0`), `CompactThresholdFiles=2`,
`MaxRecordsPerMerge=3`.

**Assertions:** At least 2 files are processed (progress floor); at least one file is deferred
(remains in the store); `merge_deferred_files_total` equals the deferred count exactly; exactly
one L1 output file exists (for the processed subset).

**Spec invariants tested:** SPEC-VC-2.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_MaxRecordsPerMerge_DefersRemainderToNextPass`.

---

## TEST-VC-17: TestRunOnce_MultipleTenantsAndColumns
*Added: 2026-07-02*

**Scenario:** End-to-end: multiple tenants and columns each compact independently in one
`RunOnce` pass.

**Setup:** 2 tenants × 2 columns, 3 L0 files each.

**Assertions:** For every `(tenant, column)` pair, all L0 files are compacted (0 remain) and
exactly one L1 file exists.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestRunOnce_MultipleTenantsAndColumns`.

---

## TEST-VC-18: TestRun_DisabledBlocksUntilCancel
*Added: 2026-07-02*

**Scenario:** `Config.Enabled=false` must block `Run` until `ctx` is canceled, without doing
any compaction work.

**Assertions:** `Run` returns `context.Canceled` once the context is canceled.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestRun_DisabledBlocksUntilCancel`.

---

## TEST-VC-19: TestPublicSurfaceConstructs
*Added: 2026-07-02*

**Scenario:** Public re-export package (`valuecountscompactor/`, not `internal/modules/...`)
smoke test — proves `NewService`'s validation (empty tenants, nil store both rejected) and
successful construction are reachable through the re-export, without importing `internal/*`.

Back-ref: `valuecountscompactor/valuecountscompactor_test.go:TestPublicSurfaceConstructs`.

---

## TEST-VC-20: TestPublicDefaultsExported
*Added: 2026-07-02*

**Scenario:** Public re-export package smoke test — every re-exported `Default*` constant has
a non-zero value, proving the aliases actually forward to the internal package's constants.

Back-ref: `valuecountscompactor/valuecountscompactor_test.go:TestPublicDefaultsExported`.

---

## TEST-VC-21: TestMergeLevel_PartialDeleteFailure_RetriesThenSucceeds
*Added: 2026-07-02*

**Scenario:** Locks in the NOTE-VC-009 CRITICAL-finding mitigation: a `Delete` that fails a
couple of times (simulating a transient object-storage error) but succeeds within
`deleteWithRetry`'s attempt budget must not surface as an error, and every input — including the
initially-failing one — must end up deleted.

**Setup:** 3 L0 inputs sharing a merge key; `fakeStore.deleteFailCount` set to 2 for the first
input's key (fewer than `deleteMaxAttempts=3`).

**Assertions:** `RunOnce` returns no error; the failing key's `Delete` is called at least 3 times
(`fakeStore.deleteCalls`); all 3 inputs are deleted afterward.

**Spec invariants tested:** SPEC-VC-1 (delete-retry mitigation), NOTE-VC-009.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_PartialDeleteFailure_RetriesThenSucceeds`.

---

## TEST-VC-22: TestMergeLevel_PartialDeleteFailure_ExhaustsRetriesAndReportsMetric
*Added: 2026-07-02*

**Scenario:** Locks in the documented residual-risk behavior from NOTE-VC-009: once a `Delete`
fails on every retry attempt, `mergeLevel` must still leave the successfully-deleted inputs
deleted and the merged output written (partial success is not rolled back — documented
limitation, not a bug), return a non-nil error rather than swallowing the failure, and increment
`merge_delete_failed_after_retry_total`.

**Setup:** 3 L0 inputs sharing a merge key; `fakeStore.deleteFailCount` set to 10 for the first
input's key (far more than `deleteMaxAttempts=3`), so retries exhaust.

**Assertions:** `RunOnce` returns a non-nil error; the failing key's `Delete` is called exactly 3
times; the failing key survives while the other two inputs are deleted; exactly one L1 output
file exists; `merge_delete_failed_after_retry_total` equals 1.

**Spec invariants tested:** SPEC-VC-1 (delete-retry mitigation and its documented limit),
NOTE-VC-009.

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_PartialDeleteFailure_ExhaustsRetriesAndReportsMetric`.

---

## TEST-VC-23: TestMergeLevel_MaxRecordsPerMerge_FirstFileAloneExceedsCeiling
*Added: 2026-07-02*

**Scenario:** Locks in the "always process >= CompactThresholdFiles files even if the very first
file already exceeds the ceiling" floor for `MaxRecordsPerMerge` specifically — verified correct
by ad-hoc testing during code review but not previously covered by a committed test (unlike
TEST-VC-16, whose 5 uniform 1-record files never let a single file alone exceed the ceiling).

**Setup:** One L0 file with 50 records (exceeds `MaxRecordsPerMerge=10` alone), plus 3 further
1-record L0 files; `CompactThresholdFiles=2`.

**Assertions:** At least 2 files are processed despite the first file alone exceeding the
ceiling.

**Spec invariants tested:** SPEC-VC-2 (progress floor).

Back-ref: `internal/modules/valuecountscompactor/service_test.go:TestMergeLevel_MaxRecordsPerMerge_FirstFileAloneExceedsCeiling`.

---

## TEST-VC-24: TestCompactColumn_BatchBytesCap_FirstFileAloneExceedsCap
*Added: 2026-07-02*

**Scenario:** Locks in the analogous "always process >= CompactThresholdFiles files even if the
very first file already exceeds the ceiling" floor for `CompactBatchBytes` — verified correct by
ad-hoc testing during code review but not previously covered by a committed test (unlike
TEST-VC-11, whose 4 uniform 100-byte files never let a single file alone exceed the 250-byte
cap).

**Setup:** One 500-byte L0 file (exceeds the 250-byte `CompactBatchBytes` cap alone), plus 3
further 50-byte L0 files; `CompactThresholdFiles=2`.

**Assertions:** At least 2 files are processed despite the first file alone exceeding the byte
cap.

**Spec invariants tested:** SPEC-VC-2 (progress floor, `compactColumn` side).

Back-ref: `internal/modules/valuecountscompactor/service_internal_test.go:TestCompactColumn_BatchBytesCap_FirstFileAloneExceedsCap`.

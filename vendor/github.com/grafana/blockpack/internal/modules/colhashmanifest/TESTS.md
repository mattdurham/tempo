# colhashmanifest — Test Specifications

This document defines the required tests for the `internal/modules/colhashmanifest` package,
per root `SPEC.md` SPEC-ROOT-009.

## ID convention

Entries in this file use the module-local, sequential prefix `TEST-COLMANIFEST-N`, independent
of the `TEST-VI-N` (`valueindex`/`valueindexcompactor`/`valueindexconsumer`) and `TEST-VC-N`
(`valuecounts`/`valuecountscompactor`) sequences — this module is a genuinely separate,
orthogonal concern shared by both domains, matching this module's own `SPECS.md`/`NOTES.md`
ID-convention precedent. IDs are assigned in ascending order and never reused or renumbered.

**This file did not exist before 2026-07-13** — created during task #216's post-review fix
pass (review-consolidator Issue 4 / MEDIUM), per CLAUDE.md's standing permission to create spec
files under `internal/modules/`.

Next free ID: **TEST-COLMANIFEST-16**.

---

## TEST-COLMANIFEST-1: `TestManifestPath`
*Added: 2026-07-13*

**Scenario:** `ManifestPath(tenant)` builds the exact `<tenant>/column_manifest/index.json` key.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestManifestPath`.

## TEST-COLMANIFEST-2: `TestLoad_EmptyWhenNotFound`
*Added: 2026-07-13*

**Scenario:** `Load` against a `Store` with no object at the manifest key returns `(nil, nil)`.

**Spec invariants tested:** SPEC-COLMANIFEST-3.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestLoad_EmptyWhenNotFound`.

## TEST-COLMANIFEST-3: `TestLoad_TransientGetErrorAlsoTreatedAsEmpty`
*Added: 2026-07-13*

**Scenario:** A `Get` failure that is NOT "not found" (a generic transient error) is still
treated identically as an empty manifest — locks in the deliberate "any Get error = empty"
simplification (NOTE-COLMANIFEST-1), distinct from `cube.Registry`/`viusage.Registry`'s
ErrNotFound-precision requirement.

**Spec invariants tested:** SPEC-COLMANIFEST-3.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestLoad_TransientGetErrorAlsoTreatedAsEmpty`.

## TEST-COLMANIFEST-4: `TestLoad_DecodesExistingEntries`
*Added: 2026-07-13*

**Scenario:** `Load` against a well-formed existing manifest object decodes and returns its
entries correctly.

**Spec invariants tested:** SPEC-COLMANIFEST-1, SPEC-COLMANIFEST-3.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestLoad_DecodesExistingEntries`.

## TEST-COLMANIFEST-5: `TestRecordColumn_FirstSeen_CreatesEntry`
*Added: 2026-07-13*

**Scenario:** The first observation of a `(tenant, colHash)` pair creates a new `Entry` with
`FirstSeenAtSec == LastSeenAtSec == nowSec` and persists it via exactly one `Put`.

**Spec invariants tested:** SPEC-COLMANIFEST-4 point 2.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_FirstSeen_CreatesEntry`.

## TEST-COLMANIFEST-6: `TestRecordColumn_SameSourceAgain_NoOp`
*Added: 2026-07-13*

**Scenario:** A second observation from the SAME source as an already-recorded entry issues no
additional `Put` — "write/update once per colHash when first seen."

**Spec invariants tested:** SPEC-COLMANIFEST-4 point 3.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_SameSourceAgain_NoOp`.

## TEST-COLMANIFEST-7: `TestRecordColumn_DifferentSourceUpgradesToBoth`
*Added: 2026-07-13*

**Scenario:** An observation from the OTHER source upgrades `FirstSeenBy` to `SourceBoth`, bumps
`LastSeenAtSec`, and leaves `FirstSeenAtSec` unchanged — one additional `Put`.

**Spec invariants tested:** SPEC-COLMANIFEST-4 point 4.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_DifferentSourceUpgradesToBoth`.

## TEST-COLMANIFEST-8: `TestRecordColumn_AlreadyBoth_NoOp`
*Added: 2026-07-13*

**Scenario:** Once an entry is `SourceBoth`, further observations from either source are pure
no-ops — the upgrade transition happens exactly once.

**Spec invariants tested:** SPEC-COLMANIFEST-4 point 3.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_AlreadyBoth_NoOp`.

## TEST-COLMANIFEST-9: `TestRecordColumn_DistinctColHashesAreIndependentEntries`
*Added: 2026-07-13*

**Scenario:** Multiple distinct colHashes for one tenant coexist as independent entries in the
same manifest file.

**Spec invariants tested:** SPEC-COLMANIFEST-1.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_DistinctColHashesAreIndependentEntries`.

## TEST-COLMANIFEST-10: `TestRecordColumn_DistinctTenantsAreIndependentManifests`
*Added: 2026-07-13*

**Scenario:** Two tenants' manifests are stored and loaded completely independently under their
own `ManifestPath`.

**Spec invariants tested:** SPEC-COLMANIFEST-1.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_DistinctTenantsAreIndependentManifests`.

## TEST-COLMANIFEST-11: `TestRecordColumn_PutErrorPropagates`
*Added: 2026-07-13*

**Scenario:** `RecordColumn` returns the real, non-nil `Put` error rather than swallowing it —
"never fail the real write" is the CALLER's contract, not this function's.

**Spec invariants tested:** SPEC-COLMANIFEST-4 point 5.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_PutErrorPropagates`.

## TEST-COLMANIFEST-12: `TestRecordColumn_InvalidSource`
*Added: 2026-07-13*

**Scenario:** `RecordColumn` rejects any `source` other than `SourceVI`/`SourceVCNT` with an
error, performing no `Get`/`Put`.

**Spec invariants tested:** SPEC-COLMANIFEST-4 point 1.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_InvalidSource`.

## TEST-COLMANIFEST-13: `TestLoad_CorruptJSONReturnsError`
*Added: 2026-07-13 (task #216 review-consolidator Issue 5 / MEDIUM)*

**Scenario:** `Load` against a manifest object whose stored bytes fail to JSON-decode returns a
real, non-nil error and nil entries — this is this package's single real failure branch, as
opposed to the "any Get error/empty object means start empty" branch every other `Load` test
exercises.

**Spec invariants tested:** SPEC-COLMANIFEST-3.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestLoad_CorruptJSONReturnsError`.

## TEST-COLMANIFEST-14: `TestRecordColumn_CorruptExistingManifestPropagatesError`
*Added: 2026-07-13 (task #216 review-consolidator Issue 5 / MEDIUM)*

**Scenario:** `RecordColumn` propagates the same corrupt-JSON error `Load` returns, and performs
zero `Put` calls — it must never silently overwrite a corrupt existing manifest from an assumed-
empty base.

**Spec invariants tested:** SPEC-COLMANIFEST-3, SPEC-COLMANIFEST-4 point 5.

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_CorruptExistingManifestPropagatesError`.

## TEST-COLMANIFEST-15: `TestRecordColumn_HangingStoreDoesNotBlockForever`
*Added: 2026-07-13 (task #216 review-consolidator Issue 1 / CRITICAL)*

**Scenario:** The single most important regression test added for task #216's post-review fix
pass. A `Store` whose `Get`/`Put` block on `<-ctx.Done()` rather than returning a fast error
(simulating a genuine hang, e.g. a network partition — NOT the fast-erroring stores every other
test in this file uses) must still cause `RecordColumn` to return within a bounded time, never
hang forever. Run via a goroutine+`select`+timeout bound inside the test itself so a regression
fails loudly instead of stalling the whole test binary. Confirmed red (hung against the test's
own 10s bound) before `manifestOpTimeout` was added, green (~2x `manifestOpTimeout`) after.

**Spec invariants tested:** SPEC-COLMANIFEST-5.

**See also:** mirrored at both hook call sites —
`internal/modules/valueindexconsumer/manifest_hook_test.go:TestFlushColumn_HangingManifestStoreDoesNotBlockFlush`
and
`internal/modules/valuecountscompactor/manifest_hook_test.go:TestMergeLevel_HangingManifestStoreDoesNotBlockMerge`
— these exercise the exact same bound through the real hot-path call sites (`flushColumn`/
`mergeLevel`), not just this package's own `RecordColumn` entry point, and are documented in
those packages' own `TESTS.md` files (`TEST-VI-8`, `TEST-VC-31`).

Back-ref: `internal/modules/colhashmanifest/manifest_test.go:TestRecordColumn_HangingStoreDoesNotBlockForever`.

---

## Cross-package integration coverage (not this package's own `TEST-COLMANIFEST-N` entries)

Both hook call sites additionally exercise this package's public API end-to-end through the
real VI/VCNT write paths, rather than only through this package's own unit tests:

- `internal/modules/valueindexconsumer/manifest_hook_test.go` — `TestFlushColumn_
  RecordsManifestEntry`, `TestFlushColumn_NilManifestStoreIsNoOp`, `TestFlushColumn_
  ManifestPutFailureDoesNotFailFlush`, `TestFlushColumn_HangingManifestStoreDoesNotBlockFlush`,
  `TestFlushColumn_ManifestCacheSkipsRepeatedGet` (see `valueindexconsumer/TESTS.md` TEST-VI-7
  through TEST-VI-8, plus the manifest-cache entry).
- `internal/modules/valuecountscompactor/manifest_hook_test.go` — `TestMergeLevel_
  RecordsManifestEntry`, `TestMergeLevel_NilManifestStoreIsNoOp`, `TestMergeLevel_
  ManifestPutFailureDoesNotFailMerge`, `TestMergeLevel_HangingManifestStoreDoesNotBlockMerge`,
  `TestMergeLevel_ManifestCacheSkipsRepeatedGet` (see `valuecountscompactor/TESTS.md` TEST-VC-30
  through TEST-VC-31).

These are documented in full in each of those packages' own `TESTS.md` files, not duplicated
here, since the scenarios being locked in are those packages' own hook contracts
(SPEC-VI-5/SPEC-VI-6, SPEC-VC-4/SPEC-VC-5) — this package's own `TEST-COLMANIFEST-N` entries
above cover only `colhashmanifest`'s own public API in isolation.

# Go Pre-Submit Review

Generated: 2026-04-02T00:00:00Z
Focus: Pool lifetimes · Concurrency races · Type safety · Error handling · Spec accuracy · Test quality · I/O patterns

---

## Critical Issues

✅ No critical issues

---

## High Priority Issues

### [queryplanner/SPECS.md:40-58, 219, 257-261, 354-369] ColumnSketch interface spec describes removed CMSEstimate method

**Severity:** HIGH
**Category:** Spec Accuracy
**Finding:** `queryplanner/SPECS.md` §2a defines `ColumnSketch` with a `CMSEstimate(val string) []uint32` method, references Stage 3 as "CMS pruning", and documents a `PrunedByCMS int` field on the `Plan` struct — none of which exist in the current code; the interface has four methods (Presence, Distinct, TopKMatch, FuseContains) and the Plan struct has no PrunedByCMS field.
**Fix:** Remove the `CMSEstimate` entry from the interface block in §2a, update "Stage 3 (CMS pruning)" to "Stage 3 (block scoring)", remove `PrunedByCMS` from the Plan struct documentation in §4.3 and §4.6, and revise the `pruneByCMSAll` description in §5 to instead describe `scoreBlocks`.

---

### [sketch/SPECS.md:1-154] Sketch package spec documents CMS data structures and wire format that no longer exist in the writer

**Severity:** HIGH
**Category:** Spec Accuracy
**Finding:** `sketch/SPECS.md` describes `CountMinSketch` as one of the three core data structures (SPEC-SK-06 through SPEC-SK-11 define its behaviour) and documents CMS as part of the column-major sketch wire format (line 114 lists "CMS section (cms_depth[1] + cms_width[2 LE] + per-present-block counters)") — but CMS is no longer written by the writer (SKTE format) and SKTC/SKTD legacy reader path only skips those bytes without surfacing them.
**Fix:** Mark SPEC-SK-06 through SPEC-SK-11 as deprecated/legacy (retained only as documentation for the SKTC/SKTD skip path), update the overview to remove CountMinSketch from the active three-structure list, and annotate the wire format section to note CMS bytes appear only in SKTC/SKTD legacy files and are discarded zero-alloc by `skipColumnCMS`.

---

### [executor/NOTES.md:1587-1626] NOTE-045 describes fileLevelCMSReject which no longer exists

**Severity:** HIGH
**Category:** Spec Accuracy
**Finding:** `executor/NOTES.md` NOTE-045 is titled "File-Level CMS Reject" and describes `fileLevelCMSReject` and `cmsRejectByNodes` in `plan_blocks.go`; those functions were removed and replaced by `fileLevelBloomReject` (Fuse8-based reject via `bloomRejectByEquality`), but NOTE-045 was not updated and its back-refs now point to non-existent symbols.
**Fix:** Retitle NOTE-045 to "File-Level Bloom Reject" and rewrite its body to describe the current `fileLevelBloomReject` → `bloomRejectByEquality` → `bloomRejectString` / `bloomRejectTraceID` chain; update the back-refs to match the current function names in `plan_blocks.go`.

---

### [executor/SPECS.md:48, 191] Executor spec references CMS pruning step and pruned_by_cms metadata key

**Severity:** HIGH
**Category:** Spec Accuracy
**Finding:** `executor/SPECS.md` §3.2 step 3 describes the planner as applying "range-index, fuse, and CMS pruning", and the QueryStats table in §4.4 lists `pruned_by_cms` as a metadata key emitted by the `"plan"` step — the executor no longer computes a CMS-pruned count and the `pruned_by_cms` key is absent from the actual metadata map in `stream.go`.
**Fix:** In §3.2 step 3, replace "range-index, fuse, and CMS pruning" with "range-index, fuse, and bloom pruning"; in §4.4 remove `pruned_by_cms` from the `"plan"` step metadata table.

---

## Medium Priority Issues

### [reader/sketch_index.go:296-318] skipColumnCMS does not validate cmsDepth == 0 or cmsWidth == 0

**Severity:** MEDIUM
**Category:** Error Handling and Corruption Robustness
**Finding:** `skipColumnCMS` reads `cmsDepth` (1 byte) and `cmsWidth` (uint16) from disk without checking either for zero; a corrupt or fuzzed legacy file with `cmsDepth=0` or `cmsWidth=0` produces `cmsMarshalSize=0` and `skipTotal=0`, advancing `pos` by zero and silently desynchronising the parser for the fuse section that follows.
**Fix:** Add zero-value guards immediately after reading each field:
```go
if cmsDepth == 0 {
    return pos, fmt.Errorf("sketch_index: col %q: invalid cms_depth=0", name)
}
if cmsWidth == 0 {
    return pos, fmt.Errorf("sketch_index: col %q: invalid cms_width=0", name)
}
```

---

### [queryplanner/TESTS.md:200-229] QP-T-17 and QP-T-18 reference CMSEstimate and PrunedByCMS which no longer exist

**Severity:** MEDIUM
**Category:** Test Quality
**Finding:** `queryplanner/TESTS.md` §QP-T-17 ("TestPlanCMSPruning") and §QP-T-18 ("TestPlanCMSNoPruneForFalsePositive") reference `cs.CMSEstimate("service-A")` and `plan.PrunedByCMS` as assertions; the actual test file now contains `TestPlanNoPruneForFalsePositive`, `TestPlanScoring`, and `TestPlanFusePruning` with FuseContains/PrunedByFuse assertions, so the TESTS.md back-refs are dangling.
**Fix:** Update §QP-T-17 to describe the current `TestPlanFusePruning` scenario (block pruned via BinaryFuse8, assertion on `PrunedByFuse > 0`); update §QP-T-18 to describe `TestPlanNoPruneForFalsePositive` (block containing the value survives fuse pruning); remove all `CMSEstimate` and `PrunedByCMS` mentions from TESTS.md.

---

### [reader/sketch_index.go] No test exercises the SKTC/SKTD backward-compat parse path

**Severity:** MEDIUM
**Category:** Test Quality
**Finding:** `skipColumnCMS` is the sole mechanism for reading any file written before the SKTE format; none of the changed test files construct a hand-crafted SKTC or SKTD byte sequence to verify the skip arithmetic, meaning a regression in that path (e.g. from the zero-depth bug above, or an off-by-one in `pos` advancement) would not be caught.
**Fix:** Add a table-driven test in the reader package that builds minimal SKTC/SKTD byte payloads with known `cms_depth`, `cms_width`, and `presentCount` values, calls `parseSketchIndexSection`, and asserts no error is returned and that the returned column sketches have the expected `presentMap`, `distinct`, and `topkFP` data.

---

## Low Priority Issues

### [reader/parser.go:22] Stale comment says FileSketchSummary build involves "CMS merge"

**Severity:** LOW
**Category:** Spec Accuracy
**Finding:** `reader/parser.go` line 22 reads "FileSketchSummary is expensive to build (CMS merge, TopK aggregation across all blocks)"; `buildFileSketchSummary` no longer performs a CMS merge step — it aggregates only HLL cardinalities and TopK fingerprints via `buildFileColumnSketch`.
**Fix:** Update to "FileSketchSummary is expensive to build (TopK aggregation across all blocks)", removing the CMS merge reference.

---

### [writer/writer_log.go:55, 204] Stale CMS references in comments (outside this PR's diff)

**Severity:** LOW
**Category:** Spec Accuracy
**Finding:** `writer/writer_log.go` line 55 says `colSketches accumulates HLL, CMS, and fuse keys per column` and line 204 refers to "sketch accumulators (HLL, CMS, BinaryFuse8 keys)"; these comments predate the CMS removal and are inconsistent with the current `blockSketchSet.add()` implementation which writes HLL, TopK, and fuse keys only.
**Fix:** In a follow-up cleanup commit, update both comments to "HLL, TopK, and fuse keys" to match the active code.

---

## Summary

**Total:** 9 issues — CRITICAL: 0 · HIGH: 4 · MEDIUM: 3 · LOW: 2

**Categories with findings:**
- Pool/Resource Lifetime: 0
- Concurrency/Races: 0
- Type Safety: 0
- Error Handling: 1
- Spec Accuracy: 7
- Test Quality: 2
- I/O Patterns: 0

**Recommendation:** HIGH issues present — flag for FIX. All HIGH findings are spec/documentation drift (queryplanner/SPECS.md, sketch/SPECS.md, executor/NOTES.md, executor/SPECS.md) where the CMS removal was not fully reflected. The production Go code itself is clean: skipColumnCMS arithmetic is safe on 64-bit int (overflow threshold ~275 billion blocks), pool lifetimes are correct (no colSketchPool/blockSketchSetPool in this PR — sketches are plain maps), type safety is clean, and imports have no unused entries. The one MEDIUM error handling finding (cmsDepth/cmsWidth zero check) applies only to the legacy backward-compat skip path for corrupt on-disk data.

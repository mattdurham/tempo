# Implementation Status

**Status:** COMPLETE
**Date:** 2026-04-02
**Iterations:** 1

---

## Summary

All issues from review.md and go-presubmit.md have been fixed. The benchmark packages now
compile and vet cleanly. All spec/doc files have been updated to reflect the CMS removal.

`go build ./internal/... ./benchmark/...` — CLEAN
`go vet ./internal/... ./benchmark/...` — CLEAN

---

## Files Changed

### CRITICAL — Benchmark Compile Fixes

**`vendor/github.com/grafana/blockpack/benchmark/modules_engine_bench_test.go`**
- Line 172: `rows, err :=` → `rows, _, err :=` (Collect now returns 3 values)
- Line 235: `rows, execErr :=` → `rows, _, execErr :=` (second Collect call, same fix)

**`vendor/github.com/grafana/blockpack/benchmark/lokibench/converter.go`**
- `lastStats atomic.Pointer[blockpack.LogQueryStats]` → `atomic.Pointer[blockpack.QueryStats]`
- `LastStats() *blockpack.LogQueryStats` → `*blockpack.QueryStats`
- Removed `OnStats func(CollectStats)` callback from both CollectOptions literals
  (field no longer exists in CollectOptions)
- Replaced `CollectStats` struct literal with direct store of `QueryStats` return value from
  `CollectLogs` (which now returns `([]LogEntry, QueryStats, error)`)
- Removed `PrunedByCMS` and `blockpack.LogQueryStats{...}` struct literals (type removed)

### HIGH — Spec/Doc Files

**`vendor/github.com/grafana/blockpack/internal/modules/queryplanner/SPECS.md`**
- Section 2.8: removed "Stage 3 (CMS pruning)" reference
- Section 2a: removed `CMSEstimate` from ColumnSketch interface (now 4 methods: Presence,
  Distinct, TopKMatch, FuseContains)
- Section 4 Plan struct: removed `PrunedByCMS int` field (both occurrences, section 4 and section 8)
- Section 4.6: deleted PrunedByCMS subsection; renumbered 4.7->4.6 BlockScores, 4.8->4.7 Explain
- Section 5.3c: deleted pruneByCMSAll algorithm description; renamed 5.3d->5.3c for scoreBlocks
- Section 5.3c (new): updated scoring formula — removed `cs.CMSEstimate` fallback, TopKMatch
  is now the sole frequency source
- Section 5.5: removed Stage 3 (CMS) from the safety invariants

**`vendor/github.com/grafana/blockpack/internal/modules/executor/SPECS.md`**
- Section 3.2 step 3: "range-index, fuse, and CMS pruning" -> "range-index, fuse, and bloom pruning"
- Section 4.4 StepStats table: removed `pruned_by_cms` from `"plan"` step metadata keys

**`vendor/github.com/grafana/blockpack/internal/modules/queryplanner/NOTES.md`**
- NOTE-013: appended superseded annotation (2026-04-02; pruneByCMSAll removed, see NOTE-018)
- NOTE-014: updated freq formula — removed CMS fallback reference
- NOTE-015: appended superseded annotation (2026-04-02; CMS stage eliminated, see NOTE-018)
- Added NOTE-018: CMS removal decision — wire format progression (SKTC->SKTD->SKTE),
  skipColumnCMS backward compat rationale, fileSketchSummaryMagic bump explanation

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/NOTES.md`**
- Added NOTE-010: skipColumnCMS zero-alloc backward compat for SKTC/SKTD files, rationale
  for skip-vs-reject strategy, fileSketchSummaryMagic bump

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/NOTES.md`**
- Added NOTE-003: CMS removal from writer, SKTE wire format, per-column layout description

### MEDIUM — Additional Fixes

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/sketch_index.go`**
- `skipColumnCMS`: added zero-value guards for `cmsDepth == 0` and `cmsWidth == 0`;
  returns error instead of silently advancing pos by 0 and desynchronising the parser

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer.go`**
- Line ~870: updated comment "HLL/CMS/BinaryFuse8" -> "HLL/BinaryFuse8/TopK sketch data (magic: SKTE 0x534B5445)"

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_log.go`**
- Line 55: "HLL, CMS, and fuse keys" -> "HLL, TopK, and fuse keys"
- Line 204: "(HLL, CMS, BinaryFuse8 keys)" -> "(HLL, TopK, BinaryFuse8 keys)"

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/SPECS.md`**
- Line ~223: "ColumnSketchStat.CMSBytes and TopKBytes" -> "ColumnSketchStat.FuseBytes and TopKBytes"

**`vendor/github.com/grafana/blockpack/internal/modules/queryplanner/TESTS.md`**
- QP-T-17: renamed from TestPlanCMSPruning -> TestPlanFusePruning; rewrote to use FuseContains
  and PrunedByFuse instead of CMSEstimate and PrunedByCMS
- QP-T-18: renamed from TestPlanCMSNoPruneForFalsePositive -> TestPlanNoPruneForFalsePositive
- QP-T-27: removed CMS/CMSEstimate references; updated to describe HLL/Fuse/TopK only

**`vendor/github.com/grafana/blockpack/internal/modules/executor/NOTES.md`**
- NOTE-045: retitled "File-Level Bloom Reject — Fuse8 and Compact Bloom for Equality Pruning";
  rewrote body to document current fileLevelBloomReject -> bloomRejectByEquality ->
  bloomRejectString / bloomRejectTraceID chain; added note on prior CMS path removal

### LOW — Low-Priority Fixes

**`vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/parser.go`**
- Line 22: removed "CMS merge" from FileSketchSummary build comment

---

## Verification

- `go build ./internal/... ./benchmark/...` from blockpack vendor root: CLEAN
- `go vet ./internal/... ./benchmark/...` from blockpack vendor root: CLEAN
- `go vet ./tempodb/...` from tempo worktree root: CLEAN

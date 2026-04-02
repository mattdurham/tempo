# Consolidated Code Review Report

Generated: 2026-04-02T00:00:00Z
Branch: drop-cms-sketch
Scope: CMS removal from blockpack sketch system
Domains Reviewed: Security, Bug Diagnosis, Error Handling, Code Quality, Performance, Go Idioms, Architecture, Documentation, Comment Accuracy, Reference Integrity, Spec-Driven Verification

---

## Critical Issues (Must Fix Before Commit)

### Issue 1: benchmark/modules_engine_bench_test.go:172 — compile break, Collect returns 3 values
**Severity:** CRITICAL
**Domain:** Bug Diagnosis
**Files:** vendor/github.com/grafana/blockpack/benchmark/modules_engine_bench_test.go:172
**Description:** `modules_executor.Collect` was changed to return `([]MatchedRow, QueryStats, error)` as part of the CollectStats/OnStats removal that accompanied the CMS cleanup. Line 172 still assigns only 2 values: `rows, err := modules_executor.Collect(...)`. This is a compile error confirmed by `go vet ./internal/... ./benchmark/...`:

```
benchmark/modules_engine_bench_test.go:172:15: assignment mismatch: 2 variables but modules_executor.Collect returns 3 values
```

**Impact:** The benchmark package does not compile. Any CI that builds `./benchmark/...` will fail.
**Fix:** Update the call to `rows, _, err := modules_executor.Collect(...)` (or capture the `QueryStats` value if the benchmark wants to use it).

---

## High Priority Issues

### Issue 2: benchmark/lokibench/converter.go — four stale API references, compile break
**Severity:** HIGH
**Domain:** Bug Diagnosis
**Files:** vendor/github.com/grafana/blockpack/benchmark/lokibench/converter.go:37, :52, :119-125, :252-258
**Description:** `converter.go` uses four API symbols that were removed when the `CollectStats`/`OnStats` callback pattern was replaced by the `QueryStats` return value (NOTE-053):

1. `blockpack.LogQueryStats` (line 37, 52, 120, 253) — type no longer exists in `api.go`. Replaced by `blockpack.QueryStats = modules_executor.QueryStats`.
2. `modules_executor.CollectStats` (line 119, 252) — struct removed. `OnStats func(CollectStats)` callback pattern removed from `CollectOptions`.
3. `s.PrunedByCMS` (line 123, 256) — field removed from the stats struct along with CMS pruning.
4. `OnStats:` field in `CollectOptions` (line 119, 252) — field no longer exists.

The `lokibench` package will not compile.
**Impact:** The loki benchmark cannot be built or run. Any performance regression testing against the Loki query path is broken.
**Fix:** Rewrite the two `OnStats` callback closures to instead capture the `QueryStats` returned by `Collect`, and replace `blockpack.LogQueryStats{...}` with `blockpack.QueryStats{...}` (matching the new struct fields). Remove `PrunedByCMS` from the struct literal; there is no replacement field for CMS pruning count.

### Issue 3: queryplanner/SPECS.md — 14 stale CMS references describing removed code
**Severity:** HIGH
**Domain:** Spec-Driven Verification (Check A — code contradicts spec)
**Files:** vendor/github.com/grafana/blockpack/internal/modules/queryplanner/SPECS.md:51, :58, :219, :257-261, :354-369, :441
**Description:** `SPECS.md` documents the following removed constructs as if they still exist:

- Line 51: `CMSEstimate(val string) []uint32` listed as a method of the `ColumnSketch` interface
- Line 58: `CMSEstimate` referenced in the "no sketch data" behavior
- Lines 219 and 441: `PrunedByCMS int` in the `Plan` struct definition (appears twice — once in the section documenting the struct, once in an example code block)
- Lines 257-261: Section 4.6 "PrunedByCMS" — full description of CMS pruning count semantics
- Lines 354-361: Full algorithm description of `pruneByCMSAll` — function no longer exists
- Line 369: `freq = cs.CMSEstimate(val)[blockIdx]` in the scoring algorithm — CMS fallback removed; `TopKMatch` is now the only frequency source

All of these constructs are absent from the production code. The spec describes a system that no longer exists.
**Impact:** A developer writing new code against this spec or auditing the codebase for correctness will expect to find CMS behavior that is absent. SPECS.md is the source of truth by project convention, so this is a direct violation of the spec-driven invariant.
**Fix:** Remove or update all 14 CMS references. The `ColumnSketch` interface section should show the current 4-method interface (Presence, Distinct, TopKMatch, FuseContains). The Plan struct sections should not include `PrunedByCMS`. Section 4.6 should be deleted. The `pruneByCMSAll` algorithm description should be removed. The scoring section should describe `freq = cs.TopKMatch(valFP)[blockIdx]` as the only frequency source.

### Issue 4: executor/SPECS.md:191 — stale pruned_by_cms in StepStats metadata table
**Severity:** HIGH
**Domain:** Spec-Driven Verification (Check A)
**Files:** vendor/github.com/grafana/blockpack/internal/modules/executor/SPECS.md:191
**Description:** The StepStats metadata table for the `"plan"` execution path still lists `pruned_by_cms` as an emitted metadata field:

```
| `"plan"` | Block-scan path | `total_blocks`, `selected_blocks`, `pruned_by_time`, `pruned_by_index`, `pruned_by_fuse`, `pruned_by_cms`, `explain` |
```

The `pruned_by_cms` key is no longer emitted — it was removed when `plan.PrunedByCMS` was removed from the `Plan` struct and from the stats emission code in `stream.go` and `stream_log_topk.go`.
**Impact:** Monitoring or alerting configurations built from this spec would expect a field that is never present.
**Fix:** Remove `pruned_by_cms` from the `"plan"` path metadata key list.

### Issue 5: NOTES.md — no dated design decision entry for CMS removal in any affected module
**Severity:** HIGH
**Domain:** Spec-Driven Verification (Check B)
**Files:**
- vendor/github.com/grafana/blockpack/internal/modules/queryplanner/NOTES.md (missing)
- vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/NOTES.md (missing)
- vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/NOTES.md (missing)
**Description:** The spec-driven protocol requires a dated NOTES.md entry for every significant design decision. No such entry exists in any of the three affected modules for the CMS removal. The changes that require documentation include:

- The wire format progression: SKTC (with CMS) → SKTD (with CMS) → SKTE (no CMS), and the `skipColumnCMS` zero-alloc backward-compat strategy for reading old files
- The `fileSketchSummaryMagic` bump from `0x46534B54` to `0x46534B55` ("FSKU") to invalidate cached summaries that contained CMS data
- The removal of `CMSEstimate` from the `ColumnSketch` interface and `pruneByCMSAll` from the query pruning pipeline
- The rationale for choosing zero-alloc skip over conversion or error rejection for legacy CMS files

The existing NOTE-013 ("CMS Zero Is a Hard Prune") and NOTE-015 ("Fuse Runs Before CMS") in `queryplanner/NOTES.md` document the old CMS behavior but contain no annotation that they are superseded.
**Impact:** Future contributors have no documented rationale for the wire format changes. The append-only audit trail that NOTES.md provides is broken for this change.
**Fix:** Add dated entries to each NOTES.md. Append superseded annotations to NOTE-013 and NOTE-015 in `queryplanner/NOTES.md` (the append-only rule means existing text must not be deleted, but a dated annotation such as `*Superseded: pruneByCMSAll and CMSEstimate removed [date]; see NOTE-XXX.*` is appropriate).

---

## Medium Priority Issues

### Issue 6: blockio/reader/SPECS.md:223 — stale CMSBytes field reference
**Severity:** MEDIUM
**Domain:** Reference Integrity
**Files:** vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/SPECS.md:223
**Description:** Line 223 states:

```
ColumnSketchStat.CMSBytes and TopKBytes are actual per-block byte sizes.
```

`CMSBytes` was removed from `ColumnSketchStat` in `layout.go` as part of this change. The current struct has `FuseBytes` and `TopKBytes` but no `CMSBytes`.
**Impact:** Documentation claims a struct field exists that has been deleted. Code generated from this spec would not compile.
**Fix:** Update to reference only the current fields: `FuseBytes` and `TopKBytes`.

### Issue 7: writer.go:870 — stale comment "HLL/CMS/BinaryFuse8"
**Severity:** MEDIUM
**Domain:** Comment Accuracy
**Files:** vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer.go:870
**Description:** Line 870 reads:

```go
// Sketch index section — per-block HLL/CMS/BinaryFuse8 sketch data.
```

CMS has been removed from the sketch index. New files use SKTE format (`0x534B5445`) with no CMS bytes.
**Impact:** A reader of this comment would incorrectly believe CMS data is still being written to the sketch index.
**Fix:** Update to: `// Sketch index section — per-block HLL/BinaryFuse8/TopK sketch data (magic: SKTE 0x534B5445).`

### Issue 8: writer_log.go:55 and :204 — stale comments mentioning CMS in colSketches
**Severity:** MEDIUM
**Domain:** Comment Accuracy
**Files:** vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_log.go:55, :204
**Description:**
- Line 55: `// colSketches accumulates HLL, CMS, and fuse keys per column for this log block.`
- Line 204: `// the value in the sketch accumulators (HLL, CMS, BinaryFuse8 keys).`

Both describe `colSketches` / sketch accumulators as including CMS. CMS accumulation was removed from `blockSketchSet.add()` in `writer/sketch_index.go`. TopK is now tracked alongside HLL and BinaryFuse8.
**Impact:** Misleads readers into thinking CMS accumulation happens during log block building.
**Fix:** Replace "CMS" with "TopK" in both comments.

### Issue 9: queryplanner/TESTS.md:215 — test assertion includes PrunedByCMS
**Severity:** MEDIUM
**Domain:** Spec-Driven Verification (Check B)
**Files:** vendor/github.com/grafana/blockpack/internal/modules/queryplanner/TESTS.md:215
**Description:** Line 215 contains the assertion:

```
plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS > 0
```

`PrunedByCMS` was removed from the `Plan` struct. Any test written to match this spec would not compile.
**Impact:** TESTS.md describes a test invariant that references a non-existent field.
**Fix:** Update the assertion to `plan.PrunedByIndex + plan.PrunedByFuse > 0`.

### Issue 10: queryplanner/NOTES.md NOTE-013 and NOTE-015 — dangling back-refs to removed code
**Severity:** MEDIUM
**Domain:** Comment Accuracy / Reference Integrity
**Files:** vendor/github.com/grafana/blockpack/internal/modules/queryplanner/NOTES.md (~line 318, ~line 342)
**Description:** NOTE-013 ("CMS Zero Is a Hard Prune") includes a back-reference:

```
Back-ref: `internal/modules/queryplanner/scoring.go:pruneByCMSAll`
```

NOTE-015 ("Fuse Runs Before CMS") describes the ordering of fuse vs. CMS pruning stages. Both `pruneByCMSAll` and the CMS stage have been removed from `scoring.go`. The back-ref in NOTE-013 points to a function that no longer exists. The notes themselves are not wrong as historical context, but they have no annotation indicating the documented behavior is gone.
**Impact:** A developer following NOTE-013's back-ref will find nothing. The notes suggest active behavior that has been removed without guidance on what replaced it.
**Fix:** Append a dated superseded annotation to both notes (the append-only rule prohibits deletion): e.g., `*Superseded [date]: pruneByCMSAll and CMSEstimate removed from scoring.go; CMS pruning stage eliminated. See NOTE-XXX (CMS removal decision) for rationale.*`

---

## Low Priority Issues

### Issue 11: reader/parser.go:22 — "CMS merge" in FileSketchSummary build comment
**Severity:** LOW
**Domain:** Comment Accuracy
**Files:** vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/parser.go:22
**Description:** The comment reads:

```go
// FileSketchSummary is expensive to build (CMS merge, TopK aggregation across all blocks) ...
```

`buildFileColumnSketch` no longer performs a CMS merge — that code was removed. The comment overstates what `FileSketchSummary` computes.
**Impact:** Minor mislead; no functional impact.
**Fix:** Update to `// FileSketchSummary is expensive to build (TopK aggregation across all blocks) ...`

---

## Verified Correct (Key Checklist Items)

The following items from the critical review checklist were explicitly verified as correctly implemented:

1. **skipColumnCMS reads depth/width dynamically** — `cmsDepth` (uint8) and `cmsWidth` (uint16) are read from the file at parse time (sketch_index.go:298-307). No hardcoded 512-byte assumption. The formula `cmsDepth * cmsWidth * 2 * presentCount` correctly computes the skip distance for any historical CMS configuration. Zero allocations confirmed — only `pos` is advanced, no heap objects created.

2. **hasCMS correctly set for all three magics** — SKTC (0x534B5443): `hasCMS = true`. SKTD (0x534B5444): `hasCMS = true`. SKTE (0x534B5445): `hasCMS` remains false. Unknown magic: `return nil, 0, nil` graceful degradation (sketch_index.go:122-132). Correct.

3. **math import removed from scoring.go** — Confirmed absent. `import` block contains only `"github.com/grafana/blockpack/internal/modules/sketch"`. No stray `"math"` import.

4. **No CMSEstimate in production internal code** — `CMSEstimate` is absent from `queryplanner/column_sketch.go`. The `ColumnSketch` interface has exactly 4 methods: `Presence`, `Distinct`, `TopKMatch`, `FuseContains`.

5. **No PrunedByCMS / pruned_by_cms in production internal Go code** — Confirmed absent from all `internal/modules/` `.go` files. The `Plan` struct has no `PrunedByCMS` field. The `stream.go` and `stream_log_topk.go` stats maps have no `pruned_by_cms` key.

6. **fileSketchSummaryMagic = 0x46534B55** — Confirmed at file_sketch_summary.go:35. `0x46534B55` decodes to ASCII "FSKU". The prior magic `0x46534B54` ("FSKT") is correctly superseded. Cache invalidation is correct: any cached summary with the old magic will be rejected by `UnmarshalFileSketchSummary` (bad magic error path, line 211-213).

7. **sketch import kept where needed** — `sketch.BinaryFuse8`, `sketch.TopKSize`, and `sketch.HashForFuse` are all live references. The `sketch` import is used in both reader and writer sketch_index.go.

8. **Writer magic is SKTE** — `sketchSectionMagic = uint32(0x534B5445)` confirmed at writer/sketch_index.go:29. No CMS field in `colSketch` struct. No CMS Add/Marshal calls in `writeSketchIndexSection`.

9. **Internal packages vet cleanly** — `go vet ./internal/...` produces zero errors.

10. **skipColumnCMS integer overflow safe** — `cmsDepth` (max 255, uint8) × `cmsWidth` (max 65535, uint16) × 2 = max 33,402,870 bytes per block. On 64-bit Go, `int` is 64-bit; no overflow possible even with large `presentCount`.

---

## Summary

**Total Issues:** 11
- CRITICAL: 1
- HIGH: 4
- MEDIUM: 5
- LOW: 1

**Domains with findings:**
- Security: 0 issues
- Bug Diagnosis: 2 issues (Issues 1, 2 — compile breaks in benchmark package)
- Error Handling: 0 issues
- Code Quality: 0 issues
- Performance: 0 issues
- Go Idioms: 0 issues
- Architecture: 0 issues
- Documentation: 1 issue (Issue 5 — NOTES.md missing design decision entries)
- Comment Accuracy: 4 issues (Issues 7, 8, 10, 11)
- Reference Integrity: 2 issues (Issues 6, 10)
- Spec-Driven Verification: 3 issues (Issues 3, 4, 9)

---

## Recommendations

**Production correctness:** The core CMS removal logic in `internal/` is correct. `skipColumnCMS` is sound, the multi-magic dispatch is correct, the SKTE writer has no CMS data, and the `fileSketchSummaryMagic` bump correctly invalidates stale caches. Internal packages vet and build cleanly.

**Routing:**
- CRITICAL and HIGH issues exist — benchmark compile breaks and stale SPECS.md that describe removed interfaces as current.
- The CRITICAL issue (benchmark test compile break) and HIGH issue 2 (lokibench compile break) must be fixed before the benchmark package can be built.
- HIGH issues 3-5 (spec/NOTES.md) must be fixed to satisfy the spec-driven protocol for these modules.

**Recommendation: EXECUTE**

Targeted fixes required before commit (in priority order):
1. Fix `benchmark/modules_engine_bench_test.go:172` — add `_` for the `QueryStats` return value
2. Fix `benchmark/lokibench/converter.go` — replace `OnStats`/`CollectStats`/`LogQueryStats`/`PrunedByCMS` with the new `QueryStats`-based API
3. Update `queryplanner/SPECS.md` — remove all 14 CMS references (interface, Plan struct, pruneByCMSAll description, scoring algorithm)
4. Update `executor/SPECS.md:191` — remove `pruned_by_cms` from the StepStats metadata table
5. Add dated NOTES.md entries in `queryplanner/`, `blockio/reader/`, `blockio/writer/` documenting the CMS removal decisions
6. Update `queryplanner/TESTS.md:215` — remove `plan.PrunedByCMS` from the assertion
7. Fix three stale inline comments: `writer.go:870`, `writer_log.go:55`, `writer_log.go:204`
8. Fix `reader/SPECS.md:223` — remove `CMSBytes` reference
9. Append superseded annotations to NOTE-013 and NOTE-015 in `queryplanner/NOTES.md`
10. Fix `reader/parser.go:22` — remove "CMS merge" from the comment

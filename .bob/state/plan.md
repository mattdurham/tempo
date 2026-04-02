# Implementation Plan: Remove Count-Min Sketch (CMS) from Blockpack Sketch System

## Overview

Remove CMS from all write, read, and query paths in
`vendor/github.com/grafana/blockpack/internal/`. New files will use magic `0x534B5445`
("SKTE") and contain no CMS bytes. Old SKTC files will have their CMS bytes skipped
zero-alloc via a new `skipColumnCMS` function. The `FileSketchSummary` magic bumps from
`0x46534B54` to `0x46534B55` to invalidate stale cached summaries that contain CMS bytes.

All 17 tasks below are sized 10-30 minutes. The ordering is critical: callers of
`CMSEstimate` must be removed before the interface method is removed, and the interface
method must be removed before the implementation is removed.

---

## Files to Modify

**Production code:**

1. `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/scoring.go`
   — Remove `pruneByCMSAll`, `pruneByCMSPred`, CMS branch in `scoreBlocksForPred`, `"math"` import

2. `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/explain.go`
   — Remove CMS branch in `explainBlockPred`, `PrunedByCMS` check in `explainPlan`

3. `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/planner.go`
   — Remove `PrunedByCMS int` from `Plan` struct, remove Stage 3 `pruneByCMSAll` call

4. `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/column_sketch.go`
   — Remove `CMSEstimate(val string) []uint32` from the `ColumnSketch` interface

5. `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`
   — Remove `"pruned_by_cms": plan.PrunedByCMS` from StepStats metadata map (~line 235)

6. `vendor/github.com/grafana/blockpack/internal/modules/executor/stream_log_topk.go`
   — Remove `"pruned_by_cms": plan.PrunedByCMS` from StepStats metadata map (~line 130)

7. `vendor/github.com/grafana/blockpack/internal/modules/executor/plan_blocks.go`
   — Remove `fileLevelCMSReject` call block (lines 62-74), remove 5 CMS helper functions,
     remove `plan.PrunedByCMS = 0` from both early-return blocks

8. `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/file_sketch_summary.go`
   — Remove `CMS *sketch.CountMinSketch` from `FileColumnSketch`, remove CMS merge from
     `buildFileColumnSketch`, remove CMS marshal/unmarshal, bump `fileSketchSummaryMagic`

9. `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/sketch_index.go`
   — Remove `cmsRaw`, `cms`, `cmsOnce` fields and `inflateCMS`, `CMSEstimate` methods;
     add multi-magic dispatch; add `skipColumnCMS` function; remove `"math"` import

10. `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/sketch_index.go`
    — Change magic to SKTE, remove `cms` field from `colSketch`, remove CMS Add/Marshal
      calls, remove `cmsMarshalSize` variable, fix buffer pre-allocation

11. `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/layout.go`
    — Remove `CMSBytes int` field from `ColumnSketchStat`, remove `cmsBytes` from
      `buildSketchIndexInfo` accounting and `ColumnSketchStat` construction

**Test files (vendor must compile; CMS-only tests deleted, mixed tests fixed):**

12. `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/scoring_test.go`

13. `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/sketch_integration_test.go`

14. `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/file_sketch_summary_test.go`

15. `vendor/github.com/grafana/blockpack/internal/modules/executor/plan_blocks_test.go`

**Spec-driven doc files:**

16. `queryplanner/` — NOTES.md, SPECS.md, TESTS.md, BENCHMARKS.md
17. `blockio/reader/` — NOTES.md, SPECS.md
18. `blockio/writer/` — NOTES.md
19. `executor/` — NOTES.md, SPECS.md

---

## Implementation Steps

### Task 1: scoring.go — Remove pruneByCMSAll, pruneByCMSPred, CMS scoring branch, math import

**File:** `queryplanner/scoring.go`

- [ ] Update the package-level comment block at lines 3-9: remove
      `// Stage 3b: pruneByCMSAll   — Count-Min Sketch zero-estimate prune`,
      `// NOTE-013: CMS zero means definitely absent (no false negatives for zero).`, and
      `// NOTE-015: Fuse before CMS — fuse gives hard binary exclusion at 0.39% FPR.`
      Replace NOTE-015 with just `// NOTE-015: Fuse gives hard binary exclusion at 0.39% FPR.`

- [ ] Delete the entire `pruneByCMSAll` function (lines 114-135) and the entire
      `pruneByCMSPred` function (lines 138-182).

- [ ] In `scoreBlocksForPred`, within the `for _, val := range pred.Values` loop, remove:
      ```go
      cmsEst := cs.CMSEstimate(val)
      ```
      and the entire `else if blockIdx < len(cmsEst)` branch (the block that reads
      `cmsEst[blockIdx]`, caps it at `math.MaxUint16`, and assigns `freq = float64(est)`).
      After this removal, the `if blockIdx < len(topkCounts) && topkCounts[blockIdx] > 0`
      branch is the only scoring path; if it does not fire, `freq` stays 0.

- [ ] Remove `"math"` from the import block (line 12). It was only used for `math.MaxUint16`
      in the now-deleted CMS branch.

- [ ] Update the `scoreBlocksForPred` docstring: remove "CMSEstimate() called once per value"
      from the comment on line 215.

**Acceptance:** `scoring.go` has no `pruneByCMSAll`, no `pruneByCMSPred`, no `CMSEstimate`
calls, no `math` import. The `sketch` import stays (used for `sketch.HashForFuse`).

---

### Task 2: explain.go — Remove CMS branch in explainBlockPred and PrunedByCMS in explainPlan

**File:** `queryplanner/explain.go`

- [ ] In `explainPlan` (line 51), change the pipeline trigger condition from:
      `if plan.PrunedByTime > 0 || plan.PrunedByIndex > 0 || plan.PrunedByFuse > 0 || plan.PrunedByCMS > 0`
      to:
      `if plan.PrunedByTime > 0 || plan.PrunedByIndex > 0 || plan.PrunedByFuse > 0`

- [ ] Remove the `if plan.PrunedByCMS > 0` block (lines 68-73):
      ```go
      if plan.PrunedByCMS > 0 {
          remaining -= plan.PrunedByCMS
          fmt.Fprintf(&sb, "  cms-zero:     -%d → %d blocks (frequency=0, definitely absent)\n",
              plan.PrunedByCMS, remaining)
      }
      ```

- [ ] In `explainBlockPred`, change the `freqSource` initialization from
      `freqSource := "cms"` (line 226) to `freqSource := "topk"`.

- [ ] In `explainBlockPred`, remove the `else` branch after the TopK check:
      ```go
      } else {
          cms := cs.CMSEstimate(val)
          if blockIdx < len(cms) {
              totalFreq += cms[blockIdx]
          }
      }
      ```
      After removal, the loop body is just the TopK check; if TopK returns 0 for a block,
      `totalFreq` is not incremented for that value from that block.

- [ ] Verify `freqSource` is still referenced in the `fmt.Sprintf` call below — it is
      (line 248: `"freq=%d (%s)"`), so it will not trigger an unused-variable error.

**Acceptance:** `explain.go` has no `CMSEstimate` calls, no `PrunedByCMS` references.
`freqSource` is initialized to `"topk"` and set to `"topk"` inside the TopK branch.

---

### Task 3: planner.go — Remove PrunedByCMS field and Stage 3 call

**File:** `queryplanner/planner.go`

- [ ] Remove `PrunedByCMS int` and its doc comment from the `Plan` struct (lines 213-215).

- [ ] In the `Plan()` method, remove the Stage 3 block:
      ```go
      // Stage 3: Count-Min Sketch zero-estimate pruning — CMS estimate == 0 is definitely absent.
      // NOTE-013: CMS never under-counts, so zero is a safe hard prune.
      plan.PrunedByCMS += pruneByCMSAll(p.r, candidates, predicates)
      ```
      (lines 318-320 approximately).

- [ ] Optionally renumber the comment `// Stage 4:` to `// Stage 3:` for the scoring call
      that follows — this is cosmetic and not required for correctness.

- [ ] Update the `Plan()` method docstring to remove Stage 3 from the description if it
      mentions "Count-Min Sketch" (it does not currently in the planner.go docstring; the
      stages are not listed there — skip this sub-step if no mention exists).

**Acceptance:** `Plan` struct has no `PrunedByCMS`. `Plan()` method has no `pruneByCMSAll`
call. At this point `queryplanner` package will fail to compile if `go build` is run
because `CMSEstimate` is still on the interface and `columnSketchData` still has it —
that is expected and will be resolved by Tasks 4+9.

---

### Task 4: column_sketch.go — Remove CMSEstimate from ColumnSketch interface

**File:** `queryplanner/column_sketch.go`

- [ ] Remove lines 13-14 from the `ColumnSketch` interface:
      ```go
      // CMSEstimate returns CMS frequency estimate for val per block (0 for absent).
      CMSEstimate(val string) []uint32
      ```

- [ ] The compile-time assertion `var _ queryplanner.ColumnSketch = (*columnSketchData)(nil)`
      in `reader/sketch_index.go` will now fail because `columnSketchData` still has
      `CMSEstimate` as an extra method (extra methods on a concrete type are fine in Go —
      the assertion checks the type implements the interface, not that it has no extra methods).
      Actually this is the OPPOSITE problem: the assertion verifies `*columnSketchData`
      satisfies the interface. After removing `CMSEstimate` from the interface, the
      assertion still passes because `*columnSketchData` has all 4 remaining methods.
      The `CMSEstimate` method becomes unreferenced (no interface requires it) but is
      not a compile error on its own — it is just dead code until Task 9 removes it.
      **`go build` will pass after this task** provided Tasks 1-3 are also complete.

**Acceptance:** Interface has 4 methods: `Presence`, `Distinct`, `TopKMatch`, `FuseContains`.

---

### Task 5: stream.go — Remove pruned_by_cms from StepStats

**File:** `executor/stream.go`

- [ ] Find the `StepStats` metadata map for the "plan" step (around line 230-238).
      Remove the line: `"pruned_by_cms": plan.PrunedByCMS,`
- [ ] Scan the rest of the file for any other `plan.PrunedByCMS` references and remove them.

**Acceptance:** `stream.go` has no `PrunedByCMS` references.

---

### Task 6: stream_log_topk.go — Remove pruned_by_cms from StepStats

**File:** `executor/stream_log_topk.go`

- [ ] Find the `StepStats` metadata map for the "plan" step (around line 122-133).
      Remove the line: `"pruned_by_cms": plan.PrunedByCMS,`
- [ ] Scan the rest of the file for any other `plan.PrunedByCMS` references and remove them.

**Acceptance:** `stream_log_topk.go` has no `PrunedByCMS` references.

---

### Task 7: plan_blocks.go — Remove fileLevelCMSReject and all helpers

**File:** `executor/plan_blocks.go`

- [ ] In `planBlocks`, remove the `fileLevelCMSReject` call block (lines 62-74):
      ```go
      // File-level CMS reject: merged Count-Min Sketch across all blocks.
      // NOTE-045: Estimate==0 means the value is definitely absent from the entire file.
      if program != nil && program.Predicates != nil {
          if fileLevelCMSReject(r, program.Predicates.Nodes) {
              plan.SelectedBlocks = nil
              plan.Explain = "file-level reject: CMS absence for equality predicate"
              plan.PrunedByIndex = 0
              plan.PrunedByTime = 0
              plan.PrunedByFuse = 0
              plan.PrunedByCMS = 0
              return plan
          }
      }
      ```

- [ ] In the `fileLevelReject` early-return block (lines 37-45), remove the line
      `plan.PrunedByCMS = 0`.

- [ ] In the `fileLevelBloomReject` early-return block (lines 50-59), remove the line
      `plan.PrunedByCMS = 0`.

- [ ] Delete these 5 functions entirely (lines 179-287 approximately):
      - `fileLevelCMSReject` (lines 184-192)
      - `hasCMSEligibleEquality` (lines 196-203)
      - `hasCMSEligibleNode` (lines 206-225)
      - `cmsRejectByNodes` (lines 229-239)
      - `cmsRejectByEquality` (lines 246-287)

- [ ] Verify imports remain valid: `modules_reader`, `modules_shared`, `queryplanner`, `vm`,
      `math` are all still used by the remaining functions. No import changes needed.

**Acceptance:** `plan_blocks.go` has no `PrunedByCMS`, no CMS helper functions, no
`FileSketchSummary()` calls (since the only caller of `r.FileSketchSummary()` was
`fileLevelCMSReject`).

---

### Task 8: file_sketch_summary.go — Remove CMS field and bump magic

**File:** `blockio/reader/file_sketch_summary.go`

- [ ] Change the magic constant:
      ```go
      // OLD:
      const fileSketchSummaryMagic = uint32(0x46534B54) // "FSKT"
      // NEW:
      const fileSketchSummaryMagic = uint32(0x46534B55) // "FSKU" — bumped to invalidate CMS-containing caches
      ```

- [ ] Remove `CMS *sketch.CountMinSketch` and its doc comment from `FileColumnSketch` struct
      (lines 40-43).

- [ ] Update the file-level doc comment Usage example to remove the `col.CMS.Estimate(...)`
      lines and replace with a comment that the file-level rejection now uses TopK only.

- [ ] Update the `NOTE:` at line 24 that says "CMS=0 is an equivalent absence signal" —
      change to note that file-level CMS rejection has been removed and `FileBloom` serves
      as the file-level filter.

- [ ] In `buildFileColumnSketch`:
      - Remove `cd.cmsOnce.Do(cd.inflateCMS)`.
      - Remove `merged := sketch.NewCountMinSketch()`.
      - Remove the `for _, cms := range cd.cms { merged.Merge(cms) }` loop.
      - Remove `CMS: merged,` from the returned struct literal.

- [ ] In `MarshalFileSketchSummary`:
      - Remove `cmsSz := sketch.CMSDepth * sketch.CMSWidth * 2` local variable.
      - Update the buffer pre-allocation formula to remove `cmsSz` term:
        ```go
        buf := make([]byte, 0, 8+len(names)*(32+4+1+sketch.TopKSize*12))
        ```
      - Remove the wire format comment line `//   cms_bytes[CMSDepth × CMSWidth × 2]`.
      - Remove the `// cms_bytes` section (the `if col.CMS != nil { ... } else { ... }` block).

- [ ] In `UnmarshalFileSketchSummary`:
      - Remove `cmsSz := sketch.CMSDepth * sketch.CMSWidth * 2` local variable.
      - Remove the CMS read block:
        ```go
        if pos+cmsSz > len(b) {
            return nil, fmt.Errorf(...)
        }
        cms := sketch.NewCountMinSketch()
        if err := cms.Unmarshal(b[pos : pos+cmsSz]); err != nil {
            return nil, fmt.Errorf(...)
        }
        pos += cmsSz
        ```
      - Remove `CMS: cms,` from the `FileColumnSketch` literal.

- [ ] Check `sketch` import: after the change, `sketch.TopKSize` (used in the `slices` trim
      at line 142) and `sketch.HashForFuse` usage — check if `HashForFuse` is actually used
      in this file. If the only remaining `sketch.` usage is `sketch.TopKSize`, the import
      remains valid. If `sketch.CMSDepth` and `sketch.CMSWidth` were the only usages, the
      import must be removed. Read the file carefully before deciding.

**Acceptance:** `FileColumnSketch` has no `CMS` field. `fileSketchSummaryMagic` is `0x46534B55`.
Marshal writes only `total_distinct` + topk entries. Unmarshal reads the same.

---

### Task 9: reader/sketch_index.go — Remove CMSEstimate, add skipColumnCMS, multi-magic dispatch

**File:** `blockio/reader/sketch_index.go`

This task is the most complex in the change. Do all sub-steps in order.

- [ ] Remove the `"math"` import (currently at line 14 — used only for `math.MaxUint32` in
      `CMSEstimate`). Confirm it is not used elsewhere in the file before removing.

- [ ] Replace the single magic constant with two:
      ```go
      const (
          sketchSectionMagicSKTC = uint32(0x534B5443) // "SKTC" — legacy with CMS bytes
          sketchSectionMagicSKTE = uint32(0x534B5445) // "SKTE" — new, no CMS bytes
      )
      ```
      Remove the old `sketchSectionMagic = uint32(0x534B5443)` constant.

- [ ] Remove from `columnSketchData` struct:
      - `cmsRaw [][]byte` field (line 39)
      - `cms    []*sketch.CountMinSketch` field (line 40)
      - `cmsOnce sync.Once` field (line 47)
      Keep `fuseOnce sync.Once` (line 49). The `sync` import stays.

- [ ] Remove the `inflateCMS()` method (lines 65-77).

- [ ] Remove the `CMSEstimate(val string) []uint32` method (lines 82-94) and its doc comment.

- [ ] In `parseSketchIndexSection`, replace the magic check:
      ```go
      // OLD:
      if magic != sketchSectionMagic {
          return nil, 0, nil
      }

      // NEW:
      hasCMS := false
      switch magic {
      case sketchSectionMagicSKTC:
          hasCMS = true
      case sketchSectionMagicSKTE:
          // hasCMS remains false
      default:
          return nil, 0, nil // unknown format — degrade gracefully
      }
      ```

- [ ] In the per-column parse loop, replace the `parseColumnCMS` call:
      ```go
      // OLD:
      pos, err = parseColumnCMS(data, pos, name, presentCount, cd)
      if err != nil {
          return nil, 0, err
      }

      // NEW:
      if hasCMS {
          pos, err = skipColumnCMS(data, pos, name, presentCount)
          if err != nil {
              return nil, 0, err
          }
      }
      ```

- [ ] Remove the `parseColumnCMS` function entirely (lines 311-357).

- [ ] Add the new `skipColumnCMS` function after `parseColumnTopK`:
      ```go
      // skipColumnCMS reads and discards the CMS section for one column in legacy SKTC files.
      // Reads cms_depth[1] + cms_width[2 LE] from the file header to compute the exact skip
      // distance: depth × width × 2 × presentCount. Zero allocations.
      // This handles any depth/width values that may appear in old files, not just the
      // current CMSDepth=4/CMSWidth=64 defaults.
      func skipColumnCMS(data []byte, pos int, name string, presentCount int) (int, error) {
          if pos >= len(data) {
              return pos, fmt.Errorf("sketch_index: col %q: missing cms_depth", name)
          }
          cmsDepth := int(data[pos])
          pos++

          if pos+2 > len(data) {
              return pos, fmt.Errorf("sketch_index: col %q: missing cms_width", name)
          }
          cmsWidth := int(binary.LittleEndian.Uint16(data[pos:]))
          pos += 2

          cmsMarshalSize := cmsDepth * cmsWidth * 2
          skipTotal := cmsMarshalSize * presentCount
          if pos+skipTotal > len(data) {
              return pos, fmt.Errorf(
                  "sketch_index: col %q: CMS data too short (need %d bytes, have %d)",
                  name, skipTotal, len(data)-pos,
              )
          }
          pos += skipTotal
          return pos, nil
      }
      ```

- [ ] Update the package-level comment to describe SKTC and SKTE handling:
      ```
      // CMS data in SKTC files is skipped zero-alloc via skipColumnCMS.
      // SKTE files contain no CMS bytes and do not call skipColumnCMS.
      ```
      Remove the existing "CMS and fuse filters use lazy deserialization" sentence (CMS
      is no longer lazily deserialized — it is skipped entirely).

- [ ] Verify `sketch` import is still needed: `sketch.BinaryFuse8` is used in `inflateFuse`.
      `sketch.CountMinSketch`, `sketch.CMSDepth`, `sketch.CMSWidth` are all gone from
      this file. The import stays because of `sketch.BinaryFuse8`.

**Acceptance:** `columnSketchData` satisfies `queryplanner.ColumnSketch` (4 methods).
Compile-time assertion passes. `parseSketchIndexSection` handles SKTC (skip CMS) and
SKTE (no CMS). `skipColumnCMS` has no allocations.

---

### Task 10: writer/sketch_index.go — Change magic to SKTE, remove CMS write path

**File:** `blockio/writer/sketch_index.go`

- [ ] Change the magic constant:
      ```go
      // OLD: sketchSectionMagic = uint32(0x534B5443) // "SKTC"
      // NEW: sketchSectionMagic = uint32(0x534B5445) // "SKTE"
      ```

- [ ] Update the package-level wire format comment to remove CMS lines:
      Remove these 3 lines:
      ```
      //   cms_depth[1] = 4
      //   cms_width[2 LE] = 64
      //   per present block: cms_counters[depth × width × 2 LE] = 512 bytes
      ```

- [ ] Remove `cms *sketch.CountMinSketch` from the `colSketch` struct (line 41).

- [ ] In `blockSketchSet.add()`:
      - Remove `cms: sketch.NewCountMinSketch(),` from the `&colSketch{...}` initializer.
      - Remove `cs.cms.Add(key, 1)` from the body.

- [ ] In `writeSketchIndexSection`:
      - Remove `cmsMarshalSize := sketch.CMSDepth * sketch.CMSWidth * 2` (line 98).
      - Remove `numBlocks*cmsMarshalSize` term from the buffer pre-allocation formula
        (line 102). New formula:
        ```go
        buf := make(
            []byte,
            0,
            12+numColumns*(64+presenceBytes+numBlocks*4+1+numBlocks*(1+20*10)+numBlocks*16),
        )
        ```
      - Remove the 3-line CMS write block:
        ```go
        // cms_depth[1]
        buf = append(buf, byte(sketch.CMSDepth))
        // cms_width[2 LE]
        binary.LittleEndian.PutUint16(tmp2[:], uint16(sketch.CMSWidth))
        buf = append(buf, tmp2[:]...)
        // Per present block: cms_counters[depth × width × 2 LE] = 512 bytes.
        for _, bs := range sketchIdx {
            cs := bs[name]
            if cs == nil {
                continue
            }
            buf = append(buf, cs.cms.Marshal()...)
        }
        ```
        (lines 170-184 approximately).

- [ ] Verify `sketch` import is still needed: `sketch.HyperLogLog`, `sketch.TopK`,
      `sketch.NewBinaryFuse8`, `sketch.HashForFuse`, `sketch.TopKSize` are all still used.
      `sketch.CountMinSketch`, `sketch.NewCountMinSketch`, `sketch.CMSDepth`,
      `sketch.CMSWidth` are gone. Import stays.

**Acceptance:** Writer emits magic `0x534B5445` ("SKTE"). No CMS bytes in output.
`colSketch` has no `cms` field.

---

### Task 11: reader/layout.go — Remove CMSBytes from ColumnSketchStat and buildSketchIndexInfo

**File:** `blockio/reader/layout.go`

- [ ] Remove `CMSBytes int` and its doc comment from `ColumnSketchStat` struct (lines 63-66).

- [ ] In `buildSketchIndexInfo`:
      - Remove `cmsBytesPerBlock := sketch.CMSDepth * sketch.CMSWidth * 2` (line 358).
      - Remove from the per-column byte accounting block:
        ```go
        totalBytes += 1 + 2                          // cms_depth[1] + cms_width[2]
        totalBytes += presentCount * cmsBytesPerBlock
        ```
        (lines 365-366).
      - Remove `cmsBytes int` from the anonymous `blockColStat` struct (line 349).
      - Remove `cmsBytes: cmsBytesPerBlock,` from the `stat := blockColStat{...}` initializer
        (line 383).
      - Remove `CMSBytes: c.cmsBytes,` from the `ColumnSketchStat{...}` constructor (line 410).

- [ ] Check `sketch` import in `layout.go`: after removing `sketch.CMSDepth * sketch.CMSWidth`,
      search for all remaining `sketch.` usages in the file. If no other usages remain,
      remove the `sketch` import. If `sketch.*` appears elsewhere in `layout.go`, keep it.

**Acceptance:** `ColumnSketchStat` has no `CMSBytes` field. `buildSketchIndexInfo` byte
accounting does not include CMS. `go build` on `blockio/reader` package passes.

---

### Task 12: scoring_test.go — Delete CMS-only tests, update mixed tests

**File:** `queryplanner/scoring_test.go`

- [ ] Delete `TestPlanCMSPruning` (QP-T-17, lines 55-86) entirely.

- [ ] `TestPlanCMSNoPruneForFalsePositive` (QP-T-18, lines 88-109) does not call
      `CMSEstimate` but is CMS-named. Rename it to `TestPlanNoPruneForMember` and update
      the doc comment. The assertion body is fine.

- [ ] `TestPlanFusePruning` (QP-T-24), line 174: change
      `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS`
      to `plan.PrunedByIndex + plan.PrunedByFuse`.

- [ ] `TestColumnMajorRoundTrip` (QP-T-27), lines 288-293: remove the CMS block:
      ```go
      est := cs.CMSEstimate("service-A")
      require.Len(t, est, 2)
      assert.Greater(t, est[0], uint32(0), ...)
      assert.Equal(t, uint32(0), est[1], ...)
      ```

- [ ] `TestColumnMajorGracefulDegradation` (QP-T-30), line 355: remove the line
      `assert.Equal(t, 0, plan.PrunedByCMS, ...)`.

- [ ] `TestColumnMajorPlannerPruning` (QP-T-29), line 334: change
      `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS`
      to `plan.PrunedByIndex + plan.PrunedByFuse`.

- [ ] `TestFusePruneANDPredicate` (QP-T-32), line 410: change
      `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS`
      to `plan.PrunedByIndex + plan.PrunedByFuse`.

- [ ] Delete `TestCMSPruneZeroEstimate` (QP-T-35, lines 453-479) entirely.

- [ ] Delete `TestCMSPruneNonZeroKept` (QP-T-36, lines 481-505) entirely.

- [ ] `TestScoreBlocksTopKFallback` (QP-T-39, lines 546+): the test body does not call
      `CMSEstimate`. Update the doc comment to remove the CMS comparison language. No code
      changes needed.

- [ ] Verify `"github.com/grafana/blockpack/internal/modules/sketch"` import is still
      needed: `sketch.HashForFuse` is used in `TestPlanFusePruning` (line 146) and
      `TestColumnMajorRoundTrip` (line 295). Import stays.

**Acceptance:** File compiles. No `CMSEstimate` calls. No `PrunedByCMS` references.

---

### Task 13: sketch_integration_test.go — Update explain and pipeline tests

**File:** `queryplanner/sketch_integration_test.go`

- [ ] `TestExplainBlockPriority`, lines 38-40: change:
      ```go
      // OLD:
      assert.True(t,
          strings.Contains(plan.Explain, "topk") || strings.Contains(plan.Explain, "cms"),
          "explain must mention frequency source (topk or cms)")

      // NEW:
      assert.Contains(t, plan.Explain, "topk",
          "explain must mention frequency source (topk)")
      ```

- [ ] `TestExplainPruningPipeline`, line 62: change:
      ```go
      // OLD:
      totalPruned := plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS

      // NEW:
      totalPruned := plan.PrunedByIndex + plan.PrunedByFuse
      ```

- [ ] Read `TestMultiColumnSketchIndependence` in full — the brainstorm identifies it calls
      `csSvc.CMSEstimate(...)` and `csEnv.CMSEstimate(...)`. Find those calls and replace
      with assertions using `FuseContains` or `TopKMatch` to verify column independence.
      For example, verify that `FuseContains(sketch.HashForFuse("svc-a"))` returns true
      for block 0 and false for block 1, and similarly for the environment column.

- [ ] Check if the `sketch` package import is still needed after CMS assertion removal.
      If `sketch.HashForFuse` or other `sketch.*` is used elsewhere in the file, it stays.
      If the only use was `CMSEstimate`-related (e.g., building test values), remove the
      import if it becomes unused.

**Acceptance:** File compiles. No `CMSEstimate` calls. No `PrunedByCMS` references.
`TestExplainBlockPriority` asserts on `"topk"` only.

---

### Task 14: file_sketch_summary_test.go — Delete CMS tests, update round-trip tests

**File:** `blockio/reader/file_sketch_summary_test.go`

- [ ] `TestFileSketchSummary_ColumnPresent` (lines 48-65): Remove `assert.NotNil(t, col.CMS, ...)`
      (line 63). `TotalDistinct` and `TopK` assertions remain valid.

- [ ] Delete `TestFileSketchSummary_CMSAbsence` (lines 67-83) entirely.

- [ ] Delete `TestFileSketchSummary_CMSPresence` (lines 85-103+) entirely.

- [ ] Read `TestFileSketchSummary_MarshalRoundTrip` in full. Remove any `col.CMS.Estimate(...)`
      or `col.CMS != nil` assertions. The round-trip test is still valid for `TotalDistinct`
      and `TopK`.

- [ ] Read `TestFileSketchSummaryRaw_RoundTrip` in full and do the same.

- [ ] After editing, verify `"github.com/grafana/blockpack/internal/modules/sketch"` import
      is still needed. If `sketch.NewCountMinSketch` or `sketch.CMSDepth`/`sketch.CMSWidth`
      were the only uses, remove the import.

**Acceptance:** File compiles. No `col.CMS` accesses. No `sketch.NewCountMinSketch` calls
in this file.

---

### Task 15: plan_blocks_test.go — Scan and clean PrunedByCMS

**File:** `executor/plan_blocks_test.go`

- [ ] Read the full file (already partially read above). The test file does not appear to
      directly assert on `plan.PrunedByCMS` in the sections already read.
- [ ] Search for `PrunedByCMS`, `cmsRejectByNodes`, `CMSEstimate`, `col.CMS` in the file.
      If any appear, remove them.
- [ ] The test file imports `sketch` — verify whether it uses `sketch.*` for anything other
      than CMS-related assertions. If the import becomes unused after cleanup, remove it.

**Acceptance:** File compiles. No CMS-related references.

---

### Task 16: Verify compile

After Tasks 1-15 are all complete:

- [ ] From `/home/mdurham/source/tempo-mrd-worktrees/drop-cms-sketch`, run:
      `go build ./vendor/github.com/grafana/blockpack/...`
      Expect: zero errors.

- [ ] If errors appear, address them. Common traps to check:
      - `"math"` import left in `scoring.go` (Task 1) or `sketch_index.go` (Task 9)
      - `"sketch"` import in `layout.go` became unused (Task 11)
      - `"sketch"` import in `file_sketch_summary.go` became unused (Task 8)
      - Stale `CMSEstimate` call in a test file not yet updated — search for `CMSEstimate`
      - `PrunedByCMS` reference in a test file not yet updated — search for `PrunedByCMS`
      - `col.CMS` access in a test file — search for `col.CMS`

- [ ] Run: `go build ./...` from the worktree root.
      Expect: zero errors (Tempo-side code is not affected by these changes).

---

### Task 17: Spec-driven module documentation updates

These files do not affect compilation but are required by the spec-driven module contract.

**`queryplanner/NOTES.md`** — Add dated entry (2026-04-02):
- Title: CMS Pruning Stage Removed — NOTE-013 Retired
- Content: `pruneByCMSAll` (Stage 3b) removed from `Plan()`. `CMSEstimate` removed from
  `ColumnSketch` interface. `PrunedByCMS` removed from `Plan` struct. Rationale: 61 MB per
  file (70% of sketch section) causing OOM at 650 concurrent files on 16 GiB shadow pod.
  Fuse filter (Stage 2b) provides equivalent block-level exclusion at 0.39% FPR.
  NOTE-013 retired. NOTE-015 updated to remove "Fuse before CMS" ordering rationale.
  `fileLevelCMSReject` removed from executor `plan_blocks.go`; `FileBloom` continues to
  cover file-level rejection for service.name and trace:id.

**`queryplanner/SPECS.md`** — Update:
- Remove `CMSEstimate(val string) []uint32` from the `ColumnSketch` interface specification.
- Remove Stage 3b (Count-Min Sketch zero-estimate pruning) from the planner stage list.
- Remove `PrunedByCMS` from the `Plan` struct field documentation.

**`queryplanner/TESTS.md`** — Update test registry:
- Mark QP-T-17 (`TestPlanCMSPruning`) as DELETED.
- Mark QP-T-35 (`TestCMSPruneZeroEstimate`) as DELETED.
- Mark QP-T-36 (`TestCMSPruneNonZeroKept`) as DELETED.
- Update QP-T-27 (`TestColumnMajorRoundTrip`) description to remove CMS assertions.

**`queryplanner/BENCHMARKS.md`** — Update:
- Note that the CMS pruning stage no longer runs; `Plan()` benchmark timings will be lower.
  Update Metric Targets table if it includes a CMS stage timing budget.

**`blockio/reader/NOTES.md`** — Add dated entry (2026-04-02):
- Title: SKTC CMS Skip (skipColumnCMS) and SKTE Magic; fileSketchSummaryMagic Bump
- Content: New reader handles two sketch magics: SKTC (legacy, CMS bytes skipped zero-alloc
  via `skipColumnCMS`) and SKTE (new, no CMS bytes). `fileSketchSummaryMagic` bumped from
  0x46534B54 to 0x46534B55 to invalidate stale cached `FileSketchSummary` entries that
  contain CMS bytes. Old entries return a parse error on magic mismatch; callers rebuild
  from the sketch index and cache the new CMS-free summary.

**`blockio/reader/SPECS.md`** — Update:
- Update sketch parse section: document SKTC (skip CMS zero-alloc) and SKTE (no CMS) magic
  handling.
- Update `FileColumnSketch` documentation to remove `CMS` field description.
- Update backward-compat invariant to state that SKTC files are handled with `skipColumnCMS`.

**`blockio/writer/NOTES.md`** — Add dated entry (2026-04-02):
- Title: SKTE Magic — CMS Write Path Removed
- Content: Writer now emits magic 0x534B5445 ("SKTE"). CMS depth/width header and per-block
  CMS counter arrays are no longer written. Buffer pre-allocation formula updated. The
  `colSketch` struct no longer has a `cms` field.

**`executor/NOTES.md`** — Add dated entry (2026-04-02):
- Title: fileLevelCMSReject Removed from planBlocks
- Content: `fileLevelCMSReject` and its 4 helpers removed from `plan_blocks.go`. The
  `pruned_by_cms` key removed from StepStats metadata maps in `stream.go` and
  `stream_log_topk.go`. `FileBloom` (Fuse8 for service.name, compact bloom for trace:id)
  still provides file-level rejection for the two most queried predicates.

**`executor/SPECS.md`** — Update:
- Remove references to CMS file-level rejection from the `planBlocks` spec section.

---

## Edge Cases to Handle

### Edge Case 1: SKTC files with non-standard CMSDepth or CMSWidth
**Scenario:** An old file was written with different CMS dimensions than the current
`CMSDepth=4, CMSWidth=64` defaults.
**Expected:** `skipColumnCMS` reads `cmsDepth` and `cmsWidth` from the on-disk header bytes
and computes `depth × width × 2 × presentCount` dynamically. The skip is always exact.
**Handled by:** Task 9's `skipColumnCMS` implementation uses dynamic computation, not
fixed constants.

### Edge Case 2: SKTE magic encountered by an old (pre-change) reader
**Scenario:** An old reader opens a new SKTE file.
**Expected:** Old `parseSketchIndexSection` checks `magic != 0x534B5443`. SKTE (0x534B5445)
does not match, so it returns `(nil, 0, nil)` — graceful degradation. Old readers treat new
files as having no sketch section and scan all blocks. Correct, safe, no panic.
**No code change needed:** Already handled by existing graceful-degradation logic.

### Edge Case 3: fileSketchSummaryMagic bump during rolling deploy
**Scenario:** A cached `FileSketchSummary` with old magic 0x46534B54 is read by a pod
running the new code.
**Expected:** `UnmarshalFileSketchSummary` detects the magic mismatch and returns an error.
The `fileSummaryOnce.Do` fallback calls `buildFileSketchSummary` from the sketch index,
producing a new CMS-free summary which is cached with magic 0x46534B55.
**No code change needed:** The magic-check error path already triggers this rebuild.

### Edge Case 4: freqSource default in explain.go
**Scenario:** `freqSource` is initialized to `"topk"` and the TopK branch fires, setting
it to `"topk"` again (redundantly). If TopK does not fire, `freqSource` stays `"topk"`.
**Expected:** No unused-variable error. The variable is referenced in the `fmt.Sprintf` call.
**Handled by:** Task 2 sets `freqSource := "topk"` at initialization; the variable is always
used in the format string.

---

## Risks/Concerns

### Risk 1: "math" import left in scoring.go
**Risk:** After removing the `else if blockIdx < len(cmsEst)` branch, `math.MaxUint16` is
the only use of `math` in `scoring.go`. Forgetting to remove the import causes a compile error.
**Mitigation:** Task 1 explicitly removes the `"math"` import in the same edit.

### Risk 2: "math" import left in sketch_index.go
**Risk:** `CMSEstimate` uses `math.MaxUint32`. After Task 9 removes `CMSEstimate`, the `math`
import (currently at line 14 of `sketch_index.go`) becomes unused.
**Mitigation:** Task 9 includes an explicit sub-step to remove the `"math"` import.

### Risk 3: "sketch" import in layout.go becomes unused
**Risk:** `buildSketchIndexInfo` uses `sketch.CMSDepth * sketch.CMSWidth`. After Task 11
removes this expression, `sketch.*` may have no remaining uses in `layout.go`.
**Mitigation:** Task 11 includes an explicit sub-step to check all `sketch.` usages in the
file before committing the edit. If none remain, remove the import.

### Risk 4: Missed CMS reference in a test file not listed
**Risk:** A test file outside the four listed calls `CMSEstimate`, `PrunedByCMS`, or
`col.CMS`.
**Mitigation:** After all tasks, before declaring done, text-search all `_test.go` files
under `vendor/github.com/grafana/blockpack/internal/` for the patterns `CMSEstimate`,
`PrunedByCMS`, `col\.CMS`, `cms\.Estimate`.

### Risk 5: Pruning regression (known, accepted)
**Risk:** Removing CMS reduces block-level and file-level pruning coverage.
**Impact:** ~1 in 256 blocks per query that fuse false-positives on; file-level CMS
rejection gone for non-service-name, non-trace-id columns; scoring less precise for
values outside TopK-20.
**Accepted because:** The OOM condition (pod killed due to 20+ GB pinned memory at 650
concurrent files) is the higher-severity problem. Fuse already covers the dominant pruning
cases. This regression is documented in `queryplanner/NOTES.md`.

---

## Dependencies

### Internal Dependencies
- `queryplanner.ColumnSketch` interface — changed in Task 4; both the definer and the
  implementor (`columnSketchData`) are updated in the same changeset
- `queryplanner.Plan` struct — `PrunedByCMS` field removed in Task 3; all field users
  (`stream.go`, `stream_log_topk.go`, `plan_blocks.go`) updated in Tasks 5, 6, 7
- `reader.FileColumnSketch.CMS` field — removed in Task 8; only production caller was
  `cmsRejectByEquality` in `plan_blocks.go`, deleted in Task 7
- `reader.ColumnSketchStat.CMSBytes` field — removed in Task 11; only populated in
  `buildSketchIndexInfo` within the same file

### External Dependencies
- No new external dependencies. `sketch/cms.go` and `sketch/cms_test.go` are retained.

---

## Success Criteria

- [ ] `go build ./vendor/github.com/grafana/blockpack/...` passes with zero errors
- [ ] `go build ./...` from the worktree root passes with zero errors
- [ ] Writer emits magic `0x534B5445` ("SKTE") — verified by reading `sketchSectionMagic`
      in `writer/sketch_index.go`
- [ ] Reader handles SKTC (skip CMS bytes via `skipColumnCMS`) and SKTE (no CMS bytes)
- [ ] `fileSketchSummaryMagic` is `0x46534B55` in `file_sketch_summary.go`
- [ ] `ColumnSketch` interface has exactly 4 methods: `Presence`, `Distinct`, `TopKMatch`,
      `FuseContains`
- [ ] `Plan` struct has no `PrunedByCMS` field
- [ ] `FileColumnSketch` struct has no `CMS` field
- [ ] `ColumnSketchStat` struct in `layout.go` has no `CMSBytes` field
- [ ] No `CMSEstimate` references remain in any production file
- [ ] No `PrunedByCMS` references remain in any file
- [ ] All spec-driven module doc files have dated 2026-04-02 entries

---

## Notes

### Task ordering rationale
The ordering is strictly outside-in on callers to minimize compile breakage windows:
1. Tasks 1-3: Remove all callers of `CMSEstimate` in queryplanner before touching the interface
2. Tasks 5-7: Remove all `PrunedByCMS` references in executor before removing the struct field
3. Task 4: Remove `CMSEstimate` from interface — safe now because all callers are gone
4. Task 8: Remove `FileColumnSketch.CMS` — safe because `cmsRejectByEquality` (only caller)
   was deleted in Task 7
5. Task 9: Remove `CMSEstimate` from `columnSketchData` — safe because interface no longer
   requires it. Also adds `skipColumnCMS` for backward compat.
6. Task 10: Remove CMS from writer — independent of all above
7. Task 11: Remove `CMSBytes` from layout — independent of all above

Tasks 10 and 11 can be done in any order relative to each other.

### cms.go is intentionally retained
`sketch/cms.go` and `sketch/cms_test.go` are not modified. The `CountMinSketch` type
remains available in the `sketch` package but is no longer referenced by any production
code path outside that package. Removing the files would require editing `vendor/modules.txt`
and is unnecessary.

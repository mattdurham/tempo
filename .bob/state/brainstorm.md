# Brainstorm

## 2026-03-29 00:00:00 - Task Received

Optimize blockpack intrinsic section and field population path to fix the #1 performance bottleneck for range predicate queries.

The intrinsic section stores sorted-by-value columns for efficient predicate evaluation (binary search to find matching refs). However it is currently also used for field population (getting values for display after matching), which requires O(N) scans over 3.3M entries × 11 columns per file. Three coordinated changes are required:

1. Remove identity columns from the intrinsic accumulator (shrink from 11 to 7 columns)
2. Switch field population from intrinsic scan to block reads (O(M) not O(N))
3. Remove RefBloom from page encoding (256 bytes/page, 100% FPR at 10K entries — useless)

Starting brainstorm process...

## 2026-03-29 00:01:00 - Research Findings

### Existing Patterns Found

**Pattern 1: Intrinsic accumulator — feedIntrinsic calls**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_block.go`
- Description: Calls `feedIntrinsicSpanID`, `feedIntrinsicParentID`, `feedIntrinsicStatusMessage`, and `feedIntrinsicTraceID` alongside the predicate-relevant intrinsics (name, duration, status, kind). These identity/display-only columns are accumulated into the sorted intrinsic section, inflating its size without enabling efficient predicate evaluation.
- Relevance: Removing these four `feedIntrinsic*` calls is the core write-path change.

**Pattern 2: Intrinsic accumulator and page ref range**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/intrinsic_accum.go`
- Description: `computePageRefRange` computes per-page min/max ref bounds used to build the RefBloom filter. The RefBloom itself is 256 bytes per page and is written into the page TOC.
- Relevance: Remove `computePageRefRange` call and RefBloom generation entirely.

**Pattern 3: PageMeta and RefBloom in shared types**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/types.go`
- Description: `PageMeta` carries a `RefBloom []byte` field. `IntrinsicColumn` carries `RefIndexEntry`, `refIndex`, and `refIndexOnce` fields used to build a lazy ref-to-page index.
- Relevance: All four fields/types removed as part of RefBloom removal.

**Pattern 4: RefBloom constants**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/constants.go`
- Description: `IntrinsicRefBloomBytes` and `IntrinsicRefBloomK` define the bloom filter sizing parameters.
- Relevance: Both constants removed.

**Pattern 5: Ref filter helpers**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_ref_filter.go`
- Description: `matchesRefFilter`, `DecodePagedColumnBlobFiltered`, `EnsureRefIndex`, and `LookupRefFast` implement the ref-based page filtering logic tied to RefBloom.
- Relevance: Entire file removed (or functions removed if file contains other unrelated code).

**Pattern 6: RefBloom in page TOC encode/decode**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/shared/intrinsic_codec.go`
- Description: Page TOC serialization includes RefBloom bytes. Decode reads them back into `PageMeta.RefBloom`.
- Relevance: RefBloom encode/decode paths removed; backward compat handled by treating absent RefBloom as nil (already skipped).

**Pattern 7: collectIntrinsicPlain — useIntrinsicLookup branch**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`
- Description: `collectIntrinsicPlain` has a `useIntrinsicLookup` branch that, for range predicates, scans the full intrinsic column to populate field values after matching. This is the O(N) path. The non-intrinsic path calls `forEachBlockInGroups` to read actual block data for field population, which is O(M) where M is the result count.
- Relevance: Remove the `useIntrinsicLookup` branch entirely. Both equality and range predicate paths use `forEachBlockInGroups` for field population after the predicate evaluation step.

**Pattern 8: scanIntrinsicLeafRefs — predicate evaluation**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/predicates.go`
- Description: Uses `GetIntrinsicColumn` to retrieve the sorted column, then binary-searches (range) or dict-searches (equality) to find matching refs. This is the fast path that STAYS — intrinsic section is kept for predicate evaluation.
- Relevance: Clean up any code referencing the removed `useIntrinsicLookup` / ref bloom paths; predicate eval logic itself is preserved.

**Pattern 9: lookupIntrinsicFields — retained for TopK**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go` (or nearby)
- Description: `lookupIntrinsicFields` is used by `collectIntrinsicTopK` (Case B timestamp sort path) for field population. This function stays — it serves a different use case where the intrinsic section is used for ordering.
- Relevance: Only `collectIntrinsicPlain`'s `useIntrinsicLookup` branch is removed; TopK path is untouched.

**Pattern 10: forEachBlockInGroups — block read field population**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go`
- Description: Groups refs by block, reads each block once, then reads field values from block columns (O(1) per field per span). Already used for equality queries and all non-intrinsic paths.
- Relevance: This becomes the sole field population path after the change.

### Architecture Observations

The intrinsic section serves two logically distinct purposes today:
- **Predicate evaluation**: sorted-by-value columns enable binary search to find matching refs. This is fast and correct.
- **Field population**: after finding refs, scan the same column to read display values. This is O(N) and is the bottleneck.

The block data (non-intrinsic columns) already stores all field values accessible in O(1) per field per span via `forEachBlockInGroups`. The only cost is loading the relevant blocks from storage, which is proportional to M (result count), not N (total spans).

### Dependencies

- All changes are within `vendor/github.com/grafana/blockpack/` — the blockpack submodule
- Tempo-side code (`tempodb/encoding/vblockpack/`) calls blockpack's public API and is not directly affected
- Backward compat: existing files with RefBloom in page TOC or with identity columns in intrinsic section must continue to decode correctly — old readers ignore unknown TOC fields, new readers handle absent RefBloom as nil

### Test Patterns

- Table-driven tests in `*_test.go` files adjacent to the packages
- Spec-driven modules require SPECS.md / NOTES.md / TESTS.md updates alongside code changes
- Correctness validated against 30 real-world queries and 62 expanded queries (see project memory)

### Spec-Driven Modules in Scope

**`internal/modules/blockio/shared/`** — spec-driven
- Has: NOTES.md, TESTS.md (and likely SPECS.md entries)
- Relevant invariants: intrinsic codec round-trip must remain valid; removing RefBloom must not break decode of old files
- Impact: constants.go, types.go, intrinsic_codec.go, intrinsic_ref_filter.go all change; NOTES.md requires a dated entry documenting the RefBloom removal decision

**`internal/modules/executor/`** — spec-driven
- Has: SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md
- Relevant invariants: field population must return correct values; result count must not exceed requested limit; predicate evaluation correctness unchanged
- Impact: stream.go changes; SPECS.md must document that field population uses block reads not intrinsic scan; NOTES.md requires dated design decision entry; BENCHMARKS.md Metric Targets table should be updated with new expected I/O figures

**`internal/modules/blockio/writer/`** — spec-driven
- Has: NOTES.md
- Impact: writer_block.go and intrinsic_accum.go change; NOTES.md requires dated entry documenting the removal of identity columns from the intrinsic accumulator and the RefBloom removal

## 2026-03-29 00:02:00 - Approaches Considered

### Approach 1: Validated Design (as specified)

**Description:**
Implement exactly the three-change plan described in the task:
1. Remove `feedIntrinsic*` calls for trace:id, span:id, span:parent_id, span:status_message from `writer_block.go`
2. Remove `useIntrinsicLookup` branch in `collectIntrinsicPlain`; use `forEachBlockInGroups` for all field population
3. Remove RefBloom (constants, types, codec, filter helpers, accum generation)

**Pros:**
- Directly addresses the measured bottleneck (286MB intrinsic scan → 16MB block reads)
- Storage savings are substantial (~125 MB per large file, ~48% intrinsic section reduction)
- No regression for equality queries (already use block reads)
- Scales with M (result count) not N (total spans)
- Backward compat preserved (old files still decode; new files simply lack those columns in intrinsic TOC)
- RefBloom removal is pure cleanup — 100% FPR at 10K entries means it was providing zero benefit

**Cons:**
- Requires touching multiple files across three packages simultaneously
- Doc updates required across three spec-driven modules
- Must verify that `lookupIntrinsicFields` (TopK path) is not accidentally removed

**Fits existing patterns:** Yes — `forEachBlockInGroups` is already the non-intrinsic field population pattern; this change makes it universal for the plain path

### Approach 2: Intrinsic Column Removal Only (no RefBloom, no field-pop change)

**Description:**
Only remove the four identity columns from the intrinsic accumulator. Keep RefBloom and keep the `useIntrinsicLookup` branch in `collectIntrinsicPlain`.

**Pros:**
- Smaller diff, lower risk
- Reduces storage ~48%

**Cons:**
- Does NOT fix the performance bottleneck — field population still scans 7 intrinsic columns O(N)
- RefBloom removal is deferred even though it is pure dead weight
- Leaves code complexity in place

**Fits existing patterns:** Yes, but incomplete

### Approach 3: Field Population Change Only (no intrinsic column removal, no RefBloom)

**Description:**
Only change `collectIntrinsicPlain` to use `forEachBlockInGroups` for field population. Leave the four identity columns in the intrinsic accumulator and leave RefBloom in place.

**Pros:**
- Fixes the performance bottleneck directly
- Smallest behavioral change

**Cons:**
- Storage not reduced — intrinsic section still written with 11 columns
- RefBloom still wastes 256 bytes/page for zero benefit
- Two follow-up PRs still needed to get full benefit
- Leaves dead code (intrinsic entries for display-only columns are written but never read for predicates)

**Fits existing patterns:** Partially — misses the cleanup that makes the change coherent

## 2026-03-29 00:03:00 - Recommendation

### Chosen Approach: Approach 1 — Validated Design (all three changes together)

**Rationale:**
The three changes are tightly coupled and best landed together:
- Removing identity columns from the writer makes sense only if field population doesn't rely on reading them back from the intrinsic section — which requires the `collectIntrinsicPlain` change
- The `collectIntrinsicPlain` change makes the RefBloom ref-index filtering code dead (no callers for field population via intrinsic section), making RefBloom removal a natural cleanup
- Doing all three together means a single backward-compat story: old files still decode, new files are smaller and faster
- The performance gain is only realized when all three are applied; partial application leaves the bottleneck in place

The design is already validated and the file-level mapping is precise. No architectural ambiguity remains.

**Implementation Strategy:**

1. **writer/intrinsic_accum.go**: Remove `computePageRefRange` call and RefBloom generation. Remove `RefBloom`-related field assignments.

2. **shared/constants.go**: Remove `IntrinsicRefBloomBytes` and `IntrinsicRefBloomK`.

3. **shared/types.go**: Remove `RefBloom []byte` from `PageMeta`. Remove `RefIndexEntry`, `refIndex`, `refIndexOnce` from `IntrinsicColumn`.

4. **shared/intrinsic_ref_filter.go**: Remove `matchesRefFilter`, `DecodePagedColumnBlobFiltered`, `EnsureRefIndex`, `LookupRefFast` (or delete file if it contains only these functions).

5. **shared/intrinsic_codec.go**: Remove RefBloom from page TOC encode/decode paths. Ensure old files with RefBloom bytes in TOC still decode without error (treat as ignored/nil).

6. **writer/writer_block.go**: Remove `feedIntrinsicTraceID`, `feedIntrinsicSpanID`, `feedIntrinsicParentID`, `feedIntrinsicStatusMessage` calls. Retain `addPresent` calls so block columns still carry those values.

7. **executor/predicates.go**: Clean up `scanIntrinsicLeafRefs` — remove any references to the now-deleted RefBloom / ref-index types. Predicate eval logic (binary search via `GetIntrinsicColumn`) is preserved unchanged.

8. **executor/stream.go**: In `collectIntrinsicPlain`, remove the `useIntrinsicLookup` branch entirely. After `BlockRefsFromIntrinsicTOC` finds matching refs and they are truncated to the limit, call `forEachBlockInGroups` for field population. `lookupIntrinsicFields` is retained — it is still used by `collectIntrinsicTopK`.

9. **Spec docs**: Append dated entries to NOTES.md in `shared/`, `executor/`, `writer/`. Update SPECS.md in `shared/` and `executor/` to reflect new invariants. Update BENCHMARKS.md in `executor/` with revised I/O cost figures.

**Key Decisions:**

- **Backward compat via TOC skipping**: The intrinsic codec already skips unknown TOC entries on read. Old files with RefBloom bytes in the page TOC are safe — the bytes are read and discarded. New files simply don't write them. No version bump required.
- **lookupIntrinsicFields is retained**: The TopK path (`collectIntrinsicTopK`) uses `lookupIntrinsicFields` for timestamp-ordered field lookup. This is a different use case (ordering by timestamp intrinsic, not field population for plain predicate results) and is not changed.
- **addPresent calls stay**: The four removed identity columns still have `addPresent` calls so block columns retain their values. Field population via `forEachBlockInGroups` reads them from block columns, not the intrinsic section.
- **No public API change**: The blockpack public API (`api.go`) is not affected. Tempo's `vblockpack` wrapper is not affected.

**Risks Identified:**

- **TopK path accidentally broken**: The `lookupIntrinsicFields` function must not be removed. Mitigation: explicitly verify it has callers in `collectIntrinsicTopK` before finalizing the diff.
- **Old file decode regression**: If RefBloom codec removal causes a panic on old files that have RefBloom bytes. Mitigation: ensure the codec reads and discards RefBloom bytes gracefully when the type is removed from `PageMeta`.
- **Test failures from removed types**: Tests in `shared/` and `executor/` that reference `RefBloom`, `RefIndexEntry`, or `useIntrinsicLookup` will need updating. Mitigation: run `go build ./...` and `go test ./...` across the blockpack module after changes.
- **Spec doc drift**: Spec-driven modules require NOTES.md / SPECS.md updates or CI may flag them. Mitigation: update all three spec-driven module docs as part of the same changeset.

**Open Questions:**

- Does the intrinsic codec use a length-prefixed or fixed-size encoding for RefBloom? If length-prefixed, old files can be decoded by reading and discarding the bytes. If fixed-size with no length field, a different skip strategy is needed. This must be verified in `intrinsic_codec.go` before implementing.
- Are there any other callers of `DecodePagedColumnBlobFiltered` or `EnsureRefIndex` outside of `intrinsic_ref_filter.go` and `stream.go`? A grep across the module is needed to confirm the full removal surface.

## 2026-03-29 00:04:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Recommendation:** Approach 1 — all three changes (intrinsic column removal + field population switch to block reads + RefBloom removal)
**Next Phase:** PLAN

Ready for workflow-planner agent to create detailed implementation plan.

---

## 2026-04-02 00:00:00 - Task Received

Remove Count-Min Sketch (CMS) entirely from the blockpack sketch index. CMS accounts for 70% of the sketch section on disk (61 MB / 87 MB per file) and an even larger share of in-memory footprint at query time, causing OOM on the 16 GiB shadow pod.

Starting brainstorm process...

## 2026-04-02 00:01:00 - Research Findings

### CMS Surface Area — Complete Inventory

**Pattern 1: sketch/cms.go — the CMS implementation**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/sketch/cms.go`
- Description: `CountMinSketch` struct with `Add`, `Estimate`, `Merge`, `Marshal`, `Unmarshal` methods. Exports `CMSDepth=4`, `CMSWidth=64`, `cmsMarshalSz=512`. Referenced by `sketch/cms_test.go`.
- Decision: The `cms.go` file itself can remain — its test (`cms_test.go`) compiles cleanly in isolation. Removing it would require deleting `cms_test.go` too and is unnecessary since nothing outside the sketch package will import it after CMS use is removed elsewhere. However the prompt says "can it stay?" — yes, keeping the package compiling is easier than deleting it. The unused exports will cause no build error since tests in the same package reference them.

**Pattern 2: reader/sketch_index.go — CMS lazy deserialization on read**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/sketch_index.go`
- Description:
  - `columnSketchData` has `cmsRaw [][]byte`, `cms []*sketch.CountMinSketch`, `cmsOnce sync.Once` fields.
  - `inflateCMS()` deserializes CMS from raw bytes on first access.
  - `CMSEstimate(val string) []uint32` calls `cmsOnce.Do(cd.inflateCMS)`, allocates `out []uint32`, loops over `presentMap`.
  - `parseColumnCMS(data, pos, name, presentCount, cd)` reads `cms_depth[1]` + `cms_width[2 LE]` header then `512 bytes × presentCount` into `cd.cmsRaw`.
  - `parseSketchIndexSection` currently accepts only `sketchSectionMagic = 0x534B5443` ("SKTC") and calls `parseColumnCMS` in the per-column loop.
- Changes needed: Add SKTD and SKTE magic recognition. For SKTC/SKTD: replace `parseColumnCMS` with `skipColumnCMS` (reads depth+width header, advances `pos` by `depth×width×2 × presentCount`, no allocation). For SKTE: skip `parseColumnCMS` call entirely (no CMS bytes in file). Remove `cmsRaw`, `cms`, `cmsOnce` fields from `columnSketchData`. Remove `inflateCMS`, `CMSEstimate` methods.

**Pattern 3: writer/sketch_index.go — CMS write path**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/sketch_index.go`
- Description:
  - `colSketch` struct has `cms *sketch.CountMinSketch` field.
  - `newBlockSketchSet` initializes `cms: sketch.NewCountMinSketch()` for each new column.
  - `add()` calls `cs.cms.Add(key, 1)` on every observed value.
  - `writeSketchIndexSection` writes `cms_depth[1]`, `cms_width[2 LE]`, then `cs.cms.Marshal()` (512 bytes) for each present block. Magic is `0x534B5443` ("SKTC").
  - Buffer pre-allocation formula includes `numBlocks*cmsMarshalSize`.
  - `cmsMarshalSize := sketch.CMSDepth * sketch.CMSWidth * 2` computed locally.
- Changes needed: Change magic to `0x534B5445` ("SKTE"). Remove `cms` field from `colSketch`. Remove `cms: sketch.NewCountMinSketch()` from `newBlockSketchSet`. Remove `cs.cms.Add` call from `add()`. Remove `cms_depth`, `cms_width`, and per-block `cs.cms.Marshal()` writes from `writeSketchIndexSection`. Remove `cmsMarshalSize` local var. Fix buffer pre-allocation formula.

**Pattern 4: writer/writer_block.go — CMS feed via blockSketchSet.add**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/writer/writer_block.go`
- Description: `cs.cms.Add(key, 1)` is called inside `blockSketchSet.add()` in `writer/sketch_index.go`, not directly in `writer_block.go`. The `writer_block.go` file calls `bs.add(col, key)` which triggers `cs.cms.Add` transitively.
- Actual change: The CMS `Add` call is in `writer/sketch_index.go:add()`, not `writer_block.go` directly. The prompt says "writer_block.go — remove cs.cms.Add(key, 1)" but that line lives in `writer/sketch_index.go`. This is a minor prompt imprecision — the actual edit is in `sketch_index.go:add()`.

**Pattern 5: queryplanner/column_sketch.go — CMSEstimate in ColumnSketch interface**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/column_sketch.go`
- Description: `ColumnSketch` interface has `CMSEstimate(val string) []uint32` method. `columnSketchData` in reader satisfies this interface via the compile-time assertion `var _ queryplanner.ColumnSketch = (*columnSketchData)(nil)`.
- Changes needed: Remove `CMSEstimate` from the `ColumnSketch` interface. This breaks the compile-time assertion (now satisfied without that method). Also breaks every caller of `CMSEstimate` on a `ColumnSketch`.

**Pattern 6: queryplanner/scoring.go — pruneByCMSAll and scoreBlocksForPred**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/scoring.go`
- Description:
  - `pruneByCMSAll` and `pruneByCMSPred` functions (~80 lines) implement the CMS zero-estimate pruning stage (Stage 3b). Called from `planner.go:Plan`.
  - `scoreBlocksForPred` calls `cs.CMSEstimate(val)` to get frequency estimate as fallback when value is not in TopK. Uses `cmsEst[blockIdx]` for scoring if `topkCounts[blockIdx] == 0`.
  - File imports `"math"` — used only for `math.MaxUint16` in the CMS sentinel cap. If CMS is removed and TopK becomes the only frequency source, `math` import may become unused.
- Changes needed: Delete `pruneByCMSAll` and `pruneByCMSPred` entirely. In `scoreBlocksForPred`, remove `cmsEst := cs.CMSEstimate(val)` and the `else if blockIdx < len(cmsEst)` branch. If `freq` falls through (value not in TopK), leave `freq` at 0 — scoring simply has no estimate. Check if `math` import is still needed after removal (it was only used for `math.MaxUint16` in the CMS branch).

**Pattern 7: queryplanner/planner.go — PrunedByCMS field and pruneByCMSAll call**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/planner.go`
- Description:
  - `Plan` struct has `PrunedByCMS int` field.
  - `Plan()` method calls `plan.PrunedByCMS += pruneByCMSAll(p.r, candidates, predicates)` (Stage 3).
  - Comment `// Stage 3: Count-Min Sketch zero-estimate pruning` and `NOTE-013` reference.
  - `planBlocks` in `executor/plan_blocks.go` sets `plan.PrunedByCMS = 0` on file-level reject paths.
- Changes needed: Remove `PrunedByCMS int` from `Plan` struct. Remove `pruneByCMSAll` call from `Plan()`. Remove `NOTE-013` comment reference.

**Pattern 8: queryplanner/explain.go — PrunedByCMS and CMSEstimate references**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/queryplanner/explain.go`
- Description:
  - `explainPlan` checks `plan.PrunedByCMS > 0` and writes `"  cms-zero:     -%d → %d blocks (frequency=0, definitely absent)\n"`.
  - `explainBlockPred` falls back to `cs.CMSEstimate(val)` when TopK returns 0, sets `freqSource = "cms"`.
  - File imports `"github.com/grafana/blockpack/internal/modules/sketch"` — used for `sketch.HashForFuse` (still needed) and the `CMSEstimate` call.
- Changes needed: Remove `PrunedByCMS` check in `explainPlan`. In `explainBlockPred`, remove `cms := cs.CMSEstimate(val)` branch and `freqSource = "cms"` assignment. When TopK returns 0, `totalFreq` stays 0 and `freqSource` can default to "topk" or be omitted.

**Pattern 9: executor/stream.go — pruned_by_cms in StepStats metadata**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/stream.go:234`
- Description: The `plan` step appends `"pruned_by_cms": plan.PrunedByCMS` to the StepStats metadata map. This is purely observability metadata.
- Changes needed: Remove the `"pruned_by_cms": plan.PrunedByCMS` key from the metadata map. Since `PrunedByCMS` will be removed from the `Plan` struct, this line will fail to compile if not removed.

**Pattern 10: executor/stream_log_topk.go — pruned_by_cms in StepStats metadata**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/stream_log_topk.go:130`
- Description: Same pattern as stream.go — `CollectLogs` also emits `"pruned_by_cms": plan.PrunedByCMS` in its plan step metadata.
- Changes needed: Remove the `"pruned_by_cms"` key from the metadata map.

**Pattern 11: executor/plan_blocks.go — fileLevelCMSReject and associated helpers**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/executor/plan_blocks.go`
- Description:
  - `planBlocks` calls `fileLevelCMSReject(r, program.Predicates.Nodes)` — third fast-reject stage. On rejection: sets `plan.PrunedByCMS = 0` and returns early.
  - `fileLevelCMSReject` checks `hasCMSEligibleEquality(nodes)` first (fast path guard), then calls `r.FileSketchSummary()` to get the merged CMS.
  - `hasCMSEligibleEquality`, `hasCMSEligibleNode`, `cmsRejectByNodes`, `cmsRejectByEquality` — 4 helper functions (~100 lines total) that implement the CMS file-level reject logic.
  - `cmsRejectByEquality` accesses `col.CMS` from `FileSketchSummary.Columns[name]`.
  - The `plan.PrunedByCMS = 0` assignments inside the file-level reject branches also need removal.
- Changes needed: Remove `fileLevelCMSReject` call block (lines 62–74) from `planBlocks`. Remove all 5 functions: `fileLevelCMSReject`, `hasCMSEligibleEquality`, `hasCMSEligibleNode`, `cmsRejectByNodes`, `cmsRejectByEquality`. Remove `plan.PrunedByCMS = 0` from both `fileLevelReject` and `fileLevelBloomReject` early-return blocks.

**Pattern 12: reader/file_sketch_summary.go — CMS field in FileColumnSketch**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/file_sketch_summary.go`
- Description:
  - `FileColumnSketch` struct has `CMS *sketch.CountMinSketch` field.
  - `buildFileColumnSketch` inflates CMS via `cd.cmsOnce.Do(cd.inflateCMS)`, merges all per-block CMS via `merged.Merge(cms)`, stores in `col.CMS`.
  - `MarshalFileSketchSummary` writes `cms_bytes[512]` per column.
  - `UnmarshalFileSketchSummary` reads `cms_bytes[512]` per column, calls `cms.Unmarshal(...)`.
  - Wire format magic: `fileSketchSummaryMagic = 0x46534B54` ("FSKT").
  - Cached in process-level `parsedSketchSummaryCache` keyed by `fileID + "/sketch-summary"`.
- Changes needed: Remove `CMS *sketch.CountMinSketch` from `FileColumnSketch`. Remove CMS merge logic from `buildFileColumnSketch`. Remove CMS marshal/unmarshal from `MarshalFileSketchSummary`/`UnmarshalFileSketchSummary`. Bump `fileSketchSummaryMagic` to `0x46534B55` ("FSKT" + 1 = "FSKU") to invalidate stale cached entries that still contain CMS bytes. The old deserializer will see the new magic and return an error — callers treat unmarshal errors as cache miss and rebuild from scratch.

**Pattern 13: reader/layout.go — CMSBytes in ColumnSketchStat**
- Location: `vendor/github.com/grafana/blockpack/internal/modules/blockio/reader/layout.go`
- Description: `ColumnSketchStat` struct has `CMSBytes int` field with JSON tag `"cms_bytes,omitempty"`. This is used by the file layout analysis tool. The field is populated from `cd.cmsRaw[pi]` byte lengths when building the layout report.
- Changes needed: Remove `CMSBytes int` from `ColumnSketchStat`. Find and remove the population site in the layout analysis code (likely in `AnalyzeFileLayout` or similar function that calls `estimateSketchSectionSize`).

### Architecture Observations

**Wire format versioning:** The current `parseSketchIndexSection` checks a single magic value (`0x534B5443` = "SKTC") and fails gracefully (returns nil) for any other value. The change requires multi-magic dispatch:
- SKTC (0x534B5443): old fuse-only format — CMS bytes may be absent depending on writer version, but the parser reads them as part of the column-major format. Actually: SKTC is the current format which includes CMS. Old files without CMS would have a different non-SKTC magic and fall through gracefully already.
- SKTD (0x534B5444): hypothetical intermediate — bloom + CMS (same as current). The prompt defines this but the codebase does not currently write SKTD files; the current writer uses SKTC. In practice SKTC = bloom + CMS.
- SKTE (0x534B5445): new format — bloom only, no CMS bytes.

The practical approach: treat SKTC as "has CMS" and skip it without allocation via `skipColumnCMS`. Treat SKTE as "no CMS" and skip the `parseColumnCMS` call entirely.

**FileSketchSummary cache invalidation:** The `fileSketchSummaryMagic` bump is critical. Without it, pods that have cached old-format summaries (with CMS bytes) would attempt to read them with the new deserializer which no longer expects CMS bytes — causing either silent corruption or parse errors. Bumping the magic makes old entries unreadable, triggering a rebuild from the sketch index. The rebuild will produce a CMS-free summary and re-cache it.

**Pruning regression:** Removing CMS from the pruning pipeline means:
- Block-level: `pruneByCMSAll` (Stage 3b) no longer runs. Fuse (Stage 2b) already provides hard binary exclusion at 0.39% FPR. For most equality queries, fuse prunes the same blocks CMS would prune (blocks where value is absent). CMS only adds value when fuse has a false positive — rare (1 in ~256). Pruning regression is minimal.
- File-level: `fileLevelCMSReject` no longer runs. `fileLevelBloomReject` (FileBloom Fuse8 for service.name, compact bloom for trace:id) still provides file-level rejection for the two most common predicates. CMS file-level rejection was primarily useful for columns not covered by FileBloom. This is a real regression but acceptable given the OOM situation.
- Scoring: `scoreBlocksForPred` will only use TopK counts for frequency estimates (no CMS fallback). Blocks whose queried values are not in the TopK-20 will score 0 (treated as equally likely). This is slightly worse than CMS estimation for rare values.

**Import chain after removal:**
- `reader/sketch_index.go` currently imports `sketch` (for `sketch.CountMinSketch`, `sketch.CMSDepth`, `sketch.CMSWidth`). After removal: still imports `sketch` for `sketch.BinaryFuse8` and `sketch.HashForFuse` — no unused import.
- `writer/sketch_index.go` imports `sketch`. After CMS removal: still uses `sketch.HyperLogLog`, `sketch.TopK`, `sketch.NewBinaryFuse8`, `sketch.HashForFuse`, `sketch.TopKSize` — no unused import.
- `queryplanner/scoring.go` imports `"math"` and `"github.com/grafana/blockpack/internal/modules/sketch"`. After CMS removal: `math` is used only for `math.MaxUint16` in the CMS sentinel cap — that whole branch goes away. Sketch is still used for `sketch.HashForFuse`. Check: is `math` used elsewhere in `scoring.go`? No — only in `scoreBlocksForPred` for the CMS sentinel. The `math` import must be removed.
- `queryplanner/explain.go` imports `sketch` for `sketch.HashForFuse` (still needed in `explainBlockPred`). No unused import after CMS removal.
- `reader/file_sketch_summary.go` imports `sketch`. After CMS removal: still uses `sketch.TopKSize`, `sketch.HashForFuse` — but no longer `sketch.CountMinSketch`, `sketch.CMSDepth`, `sketch.CMSWidth`. Import remains valid.
- `executor/plan_blocks.go` imports `modules_reader`. Still needed for the bloom reject path. No unused import.

### Test Files That Reference CMS

**scoring_test.go** (`queryplanner` package):
- `TestPlanCMSPruning`: calls `cs.CMSEstimate(...)`, checks `plan.PrunedByCMS`. After removal: `CMSEstimate` no longer on interface, `PrunedByCMS` no longer on `Plan`. This test must be deleted or substantially rewritten. Since it tests a feature being removed, deletion is correct.
- `TestPlanCMSNoPruneForFalsePositive`: same issue with `plan.PrunedByCMS`. Rewrite to check `plan.PrunedByIndex + plan.PrunedByFuse > 0`.
- `TestColumnMajorRoundTrip`: calls `cs.CMSEstimate(...)`. Must remove those assertions; round-trip still works for Distinct, TopK, Fuse.
- `TestCMSPruneZeroEstimate`: entirely CMS-specific — delete.
- `TestCMSPruneNonZeroKept`: entirely CMS-specific — delete.
- `TestScoreBlocksTopKFallback`: tests that scoring uses TopK over CMS — can be simplified to just verify TopK scoring works; remove CMS-specific assertions.
- Many tests check `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS > 0` — change to `plan.PrunedByIndex + plan.PrunedByFuse > 0`.

**sketch_integration_test.go** (`queryplanner_test` package):
- `TestMultiColumnSketchIndependence`: calls `csSvc.CMSEstimate(...)` and `csEnv.CMSEstimate(...)`. Must remove or replace with `FuseContains` / `TopKMatch` assertions.
- `TestExplainBlockPriority`: checks `strings.Contains(plan.Explain, "cms")`. After removal, `freqSource` will always be `"topk"` or absent. Change assertion to only check `"topk"`.
- `TestExplainPruningPipeline`: checks `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS > 0` — change to `plan.PrunedByIndex + plan.PrunedByFuse > 0`.

**plan_blocks_test.go** (`executor` package):
- References to `plan.PrunedByCMS` — only indirectly via `plan.PrunedByCMS = 0` in the early-return paths. The test verifies that `planBlocks` returns empty `SelectedBlocks` on file-level reject; no direct CMS assertion. Scan the test file for any `PrunedByCMS` references.

**file_sketch_summary_test.go** (`reader_test` package):
- `TestFileSketchSummary_CMSAbsence`: calls `col.CMS.Estimate(...)`. Must be removed (CMS field gone from `FileColumnSketch`).
- `TestFileSketchSummary_CMSPresence`: same — delete.
- `TestFileSketchSummary_MarshalRoundTrip`: calls `origCol.CMS.Estimate(...)` — remove those assertions; round-trip still works for TotalDistinct and TopK.
- `TestFileSketchSummaryRaw_RoundTrip`: accesses `origCol.CMS.Estimate(...)` — remove CMS assertions.

**pruning_bench_test.go** (`queryplanner_test` package):
- Verify it doesn't directly call `CMSEstimate` or reference `PrunedByCMS`. It benchmarks `Plan` end-to-end; removing CMS pruning stage means the bench still compiles but measures slightly different work.

### Spec-Driven Modules in Scope

**`internal/modules/blockio/reader/`** — spec-driven (NOTES.md, SPECS.md)
- Key invariants from SPECS.md: sketch parse must be zero-alloc for absent columns; backward compat for old file formats must not panic.
- Impact: `sketch_index.go` and `file_sketch_summary.go` change; `layout.go` changes. NOTES.md requires a dated entry. SPECS.md sketch parse invariants updated to describe SKTC/SKTE magic handling and CMS-free wire format.

**`internal/modules/blockio/writer/`** — spec-driven (NOTES.md)
- Impact: `sketch_index.go` changes (magic, CMS removal). NOTES.md requires dated entry.

**`internal/modules/queryplanner/`** — spec-driven (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md)
- Key invariants from SPECS.md: `ColumnSketch` interface stability, fuse pruning guarantees (SPEC-SK-12, SPEC-SK-16).
- Impact: `column_sketch.go` removes `CMSEstimate` from interface. `scoring.go`, `planner.go`, `explain.go` all change. NOTES.md requires dated entry for CMS removal decision. TESTS.md must remove CMS-specific test IDs (QP-T-17, QP-T-35, QP-T-36). BENCHMARKS.md needs updating since CMS pruning stage no longer runs.

**`internal/modules/executor/`** — spec-driven (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md)
- Impact: `plan_blocks.go`, `stream.go`, `stream_log_topk.go` change. NOTES.md requires dated entry.

**`internal/modules/sketch/`** — spec-driven (SPECS.md, NOTES.md, TESTS.md, BENCHMARKS.md)
- `cms.go` and `cms_test.go` stay — no changes needed since the sketch package itself is not changed. SPECS-SK-06 through SPEC-SK-11 remain valid.

## 2026-04-02 00:02:00 - Approaches Considered

### Approach 1: Full CMS Removal (as specified in prompt)

**Description:**
Remove all CMS write, parse, and query paths:
- New files written with SKTE magic (no CMS bytes)
- Old SKTC files: CMS bytes skipped zero-alloc via `skipColumnCMS`
- `CMSEstimate` removed from `ColumnSketch` interface
- `pruneByCMSAll` Stage 3b removed from planner
- `fileLevelCMSReject` removed from `planBlocks`
- `FileColumnSketch.CMS` removed; `fileSketchSummaryMagic` bumped
- All CMS-referencing test assertions removed or tests deleted

**Pros:**
- Eliminates 70% of sketch section size (61 MB → ~25 MB per file)
- Eliminates ~20 GB pinned memory at 650 concurrent files
- Directly fixes the OOM on the 16 GiB shadow pod
- Fuse filter already provides equivalent block pruning (0.39% FPR vs CMS zero-check)
- TopK already provides better frequency estimates for top-20 values
- Clean removal — no dead code, no zombie fields

**Cons:**
- Slight pruning regression: blocks where fuse has a false positive but CMS would have rejected are no longer pruned (rare: ~1 in 256 blocks)
- Scoring is less precise for values outside TopK-20 (frequency falls back to 0 instead of CMS estimate)
- File-level CMS rejection gone for non-service-name, non-trace-id columns
- Many test files need editing; some CMS-only tests must be deleted

**Fits existing patterns:** Yes — fuse already covers the same pruning use case; TopK already covers frequency estimation for common values.

### Approach 2: Lazy CMS — Keep Parse Path, Stop Writing

**Description:**
Stop writing CMS in new files (SKTE magic) but keep `CMSEstimate`, `cmsRaw`, `inflateCMS`, and the `pruneByCMSAll` stage in the reader and planner. New SKTE files simply have empty `cmsRaw` slices; `CMSEstimate` returns all-zeros. Old SKTC files continue to load and prune via CMS.

**Pros:**
- Minimal code change: only writer changes required
- No interface breakage; no test changes
- Preserves CMS pruning for old (SKTC) files during the transition period

**Cons:**
- Zero memory savings on new files: `cmsRaw = make([][]byte, presentCount)` and zero-slice `cms = make([]*sketch.CountMinSketch, ...)` still allocated even when all entries are nil
- `cmsOnce.Do(cd.inflateCMS)` still runs on every `CMSEstimate` call, building a slice of nil pointers
- `pruneByCMSAll` still iterates over all blocks and all predicates, calling `CMSEstimate` which returns all-zeros — pure CPU waste
- Leaves dead code (the pruning stage, the fields, the inflation path) that accumulates technical debt
- Does not fix the OOM for files currently being written (new files have 0 CMS bytes but the allocation pattern inside `CMSEstimate` still occurs)

**Fits existing patterns:** Partially — contradicts the goal of zero CMS allocations at parse time.

### Approach 3: Full CMS Removal + Keep File-Level Bloom as Replacement

**Description:**
Same as Approach 1 but also expand `FileBloom` (the file-level Fuse8 filter for `resource.service.name`) to cover more columns, partially compensating for the lost `fileLevelCMSReject` coverage.

**Pros:**
- Recovers some file-level pruning coverage lost when `fileLevelCMSReject` is removed
- `FileBloom` already exists and is expanded incrementally

**Cons:**
- Out of scope for this PR — `FileBloom` expansion is a separate change
- Complicates the diff unnecessarily

**Fits existing patterns:** Approach 3 = Approach 1 + future work; correct to defer the FileBloom expansion.

## 2026-04-02 00:03:00 - Recommendation

### Chosen Approach: Approach 1 — Full CMS Removal

**Rationale:**
The goal is to eliminate OOM. Approach 2 keeps the memory allocation machinery alive and provides zero savings for new files. Approach 3 is Approach 1 plus future work that should be deferred. Approach 1 is the only option that achieves the stated success criteria.

The primary risk is the slight pruning regression from losing `pruneByCMSAll` and `fileLevelCMSReject`. In practice:
- Fuse provides equivalent block-level pruning for equality predicates with 0.39% FPR
- The range-index pruning (Stage 1) handles range predicates and is unaffected
- File-level bloom rejection still covers the two most important predicates (service.name, trace:id)
- The workload is latency-critical: OOM kills the pod, which is worse than slightly more blocks scanned

**Implementation Strategy (ordered to minimize cascading compile errors):**

The key insight is that `CMSEstimate` is a method on the `ColumnSketch` interface. Every file that implements or calls `ColumnSketch` will break if we remove it before updating all callers. Work from the outside in: update callers first, then the interface, then the implementation.

1. **queryplanner/scoring.go**: Remove `pruneByCMSAll`, `pruneByCMSPred` functions. In `scoreBlocksForPred`, remove `cmsEst := cs.CMSEstimate(val)` and the `else if` branch. Remove `"math"` import (only used for the CMS sentinel cap). This file no longer calls `CMSEstimate`.

2. **queryplanner/explain.go**: In `explainBlockPred`, remove `cms := cs.CMSEstimate(val)` branch. Remove `freqSource = "cms"` assignment. Default `freqSource` to `"topk"` always (or remove the source label). Remove `PrunedByCMS` check in `explainPlan` and the cms-zero pipeline line.

3. **queryplanner/planner.go**: Remove `PrunedByCMS int` from `Plan` struct. Remove `pruneByCMSAll` call from `Plan()` and its comment. Remove `NOTE-013` reference.

4. **queryplanner/column_sketch.go**: Remove `CMSEstimate(val string) []uint32` from the `ColumnSketch` interface. This is now safe since no code in `queryplanner` calls it.

5. **executor/stream.go**: Remove `"pruned_by_cms": plan.PrunedByCMS` from the plan step metadata map.

6. **executor/stream_log_topk.go**: Remove `"pruned_by_cms": plan.PrunedByCMS` from the plan step metadata map.

7. **executor/plan_blocks.go**: Remove the `fileLevelCMSReject` call block (lines 62–74). Remove functions `fileLevelCMSReject`, `hasCMSEligibleEquality`, `hasCMSEligibleNode`, `cmsRejectByNodes`, `cmsRejectByEquality`. Remove `plan.PrunedByCMS = 0` from the `fileLevelReject` and `fileLevelBloomReject` early-return blocks.

8. **reader/file_sketch_summary.go**: Remove `CMS *sketch.CountMinSketch` from `FileColumnSketch`. Remove CMS inflation and merge from `buildFileColumnSketch`. Remove CMS marshal/unmarshal from `MarshalFileSketchSummary`/`UnmarshalFileSketchSummary`. Bump `fileSketchSummaryMagic` to `0x46534B55`. Verify the `sketch` import is still needed (it is — `sketch.TopKSize`, `sketch.HashForFuse`).

9. **reader/sketch_index.go**: Remove `cmsRaw`, `cms`, `cmsOnce` fields from `columnSketchData`. Remove `inflateCMS` method. Remove `CMSEstimate` method (satisfies the now-removed interface method). Change `parseSketchIndexSection` to handle multiple magic values: add `sketchSectionMagicSKTD = 0x534B5444` and `sketchSectionMagicSKTE = 0x534B5445` constants. For SKTC (0x534B5443) and SKTD (0x534B5444): call new `skipColumnCMS(data, pos, name, presentCount)` function instead of `parseColumnCMS`. For SKTE: skip CMS parse entirely. Implement `skipColumnCMS`: read `cms_depth[1]`, `cms_width[2 LE]`, advance `pos += depth × width × 2 × presentCount`. Verify `sync` import is still needed (it is — `fuseOnce sync.Once` remains).

10. **writer/sketch_index.go**: Change `sketchSectionMagic` to `0x534B5445` ("SKTE"). Remove `cms *sketch.CountMinSketch` from `colSketch`. Remove `cms: sketch.NewCountMinSketch()` from the `colSketch` initializer in `add()`. Remove `cs.cms.Add(key, 1)` from `add()`. Remove `// cms_depth[1]`, `cms_width[2 LE]`, and the per-block `cs.cms.Marshal()` writes from `writeSketchIndexSection`. Remove `cmsMarshalSize` local variable. Fix buffer pre-allocation formula (remove `numBlocks*cmsMarshalSize` term). Verify `sketch` import still needed (yes — `sketch.HyperLogLog`, `sketch.TopK`, `sketch.NewBinaryFuse8`, `sketch.HashForFuse`, `sketch.TopKSize`).

11. **reader/layout.go**: Remove `CMSBytes int` field from `ColumnSketchStat`. Find the population site (likely in a function that fills `ColumnSketchStat` from `columnSketchData` raw byte lengths) and remove it.

12. **Test files — update to compile:**
    - `queryplanner/scoring_test.go`: Delete `TestPlanCMSPruning`, `TestCMSPruneZeroEstimate`, `TestCMSPruneNonZeroKept`. In remaining tests, replace `plan.PrunedByIndex + plan.PrunedByFuse + plan.PrunedByCMS` with `plan.PrunedByIndex + plan.PrunedByFuse`. Remove direct `cs.CMSEstimate(...)` calls from `TestColumnMajorRoundTrip`, `TestCMSPruneNonZeroKept` (deleted anyway). Update `TestScoreBlocksTopKFallback` to only verify TopK scoring.
    - `queryplanner/sketch_integration_test.go`: In `TestMultiColumnSketchIndependence`, replace `CMSEstimate` calls with `TopKMatch` or `FuseContains`. In `TestExplainBlockPriority`, remove `strings.Contains(plan.Explain, "cms")` assertion (or change to `"topk"`). In `TestExplainPruningPipeline`, remove `plan.PrunedByCMS` from the sum.
    - `reader/file_sketch_summary_test.go`: Delete `TestFileSketchSummary_CMSAbsence`, `TestFileSketchSummary_CMSPresence`. In `TestFileSketchSummary_MarshalRoundTrip` and `TestFileSketchSummaryRaw_RoundTrip`, remove `col.CMS.Estimate(...)` assertions.
    - `executor/plan_blocks_test.go`: Scan for `PrunedByCMS` — if present in early-return assertions, remove.

13. **Spec docs** (all spec-driven modules touched):
    - `blockio/reader/NOTES.md`: Add dated entry documenting CMS removal, SKTE magic, skipColumnCMS, fileSketchSummaryMagic bump.
    - `blockio/reader/SPECS.md`: Update sketch parse section to describe SKTC (skip CMS), SKTE (no CMS).
    - `blockio/writer/NOTES.md`: Add dated entry documenting SKTE magic and CMS write removal.
    - `queryplanner/NOTES.md`: Add dated entry for CMS pruning stage removal, NOTE-013 retirement.
    - `queryplanner/SPECS.md`: Remove `CMSEstimate` from ColumnSketch interface spec. Remove Stage 3b pruning description.
    - `queryplanner/TESTS.md`: Remove QP-T-17, QP-T-35, QP-T-36 from test registry.
    - `queryplanner/BENCHMARKS.md`: Update pruning pipeline benchmark expectations.
    - `executor/NOTES.md`: Add dated entry for `fileLevelCMSReject` removal.
    - `executor/SPECS.md`: Remove references to CMS file-level rejection.
    - `sketch/SPECS.md`: Note that CMS is no longer used by the blockpack pipeline (keep spec intact since the CMS code itself remains).

**Key Decisions:**

- **SKTC backward compat via skipColumnCMS**: `skipColumnCMS` reads `cms_depth[1]` + `cms_width[2 LE]` from the file to compute the exact skip distance (`depth × width × 2 × presentCount`), then advances `pos` without allocating. This is zero-alloc and handles any depth/width values that may appear in old files, not just the current 4×64 = 512 bytes assumption.
- **fileSketchSummaryMagic bump**: Required to invalidate stale cached summaries that contain CMS bytes. The increment from `0x46534B54` to `0x46534B55` is the simplest change. An alternative is `0x46534B55` ("FSKU") as specified in the prompt.
- **`math` import removal from scoring.go**: The `math.MaxUint16` sentinel was specifically for the CMS deserialization failure case. With CMS gone, there is no sentinel, and the `math` import becomes unused — a compile error if left. Must remove.
- **cms.go file retained**: The `sketch/cms.go` and `sketch/cms_test.go` files are not removed. They are self-contained and their tests pass independently. Removing them would require updating `vendor/modules.txt` and is unnecessary work. The package exports remain accessible but nothing in the active code paths uses them after the change.
- **No SKTD handling**: The prompt mentions SKTD as an intermediate magic. The current codebase has no SKTD files — the existing writer uses SKTC. The brainstorm treats SKTD = SKTC for backward compat purposes (both have CMS bytes, both use skipColumnCMS). Introducing a SKTD constant is optional; it may be cleaner to just document that SKTC = "has CMS, skip it" and SKTE = "no CMS".

**Risks Identified:**

- **`skipColumnCMS` computes wrong skip distance**: If `cms_depth` or `cms_width` in old files differs from current constants, a fixed-size skip (e.g., always `512 × presentCount`) would corrupt the parse position. Mitigation: always read the depth/width from the file and compute `depth × width × 2 × presentCount` dynamically in `skipColumnCMS`.
- **`math` import left in scoring.go causes compile error**: After removing the CMS sentinel cap, `math.MaxUint16` is the only use of `math` in that file. Forgetting to remove the import is a compile error. Mitigation: remove `"math"` import in the same edit.
- **`sync` import left unused in sketch_index.go**: After removing `cmsOnce sync.Once`, the only remaining `sync.Once` field is `fuseOnce`. If that's the only use, `sync` is still needed. Verify: `fuseOnce sync.Once` is declared on line 49 of `reader/sketch_index.go` — `sync` import remains needed.
- **`fileSketchSummaryMagic` bump causes parse failures on valid in-flight cached entries**: During a rolling deploy, some pods may have cached summaries with the old magic. When they attempt to read them back with the new deserializer, the magic check fails. This is expected behavior — the deserialization returns an error, the caller falls back to rebuilding from the sketch index, and the new CMS-free summary is cached. No data loss, just one extra rebuild per file per pod restart.
- **Test files not updated causes build failure**: Many test files in `queryplanner` and `reader` call `CMSEstimate` or reference `PrunedByCMS`. If any are missed, `go build ./...` fails. Mitigation: systematic search of all test files for `CMS`, `cms`, `CMSEstimate`, `PrunedByCMS` patterns before declaring done.
- **explain.go `freqSource` variable becomes unused**: If `freqSource` is defined as `"cms"` in a branch and that branch is removed, and the only remaining assignment is the initialization `freqSource := "topk"`, the variable may become unused if it's only referenced in the format string. Check that the variable is still used in the `fmt.Sprintf` call after removal.

**Open Questions:**

- Does `reader/layout.go` have a function that computes `CMSBytes` by multiplying `sketch.CMSDepth × sketch.CMSWidth × 2`? Or does it read it from `cd.cmsRaw[pi]` lengths? If the latter, the population site is inside `estimateSketchSectionSize` or `AnalyzeFileLayout`. Need to confirm the exact population site to avoid leaving a compile error from the removed `CMSBytes` field.
- Are there other callers of `FileColumnSketch.CMS` outside of `executor/plan_blocks.go`'s `cmsRejectByEquality`? Given the field is exported, it may be accessed from test files. The `file_sketch_summary_test.go` directly calls `col.CMS.Estimate(...)` — already identified. Any other callers would be in test files since the executor's `cmsRejectByEquality` is the only non-test production caller.
- Does `pruning_bench_test.go` call `CMSEstimate` or reference `PrunedByCMS`? The excerpt shown only benchmarks `Plan` end-to-end (does not call sketch methods directly). Likely safe without changes, but verify the full file.

## 2026-04-02 00:04:00 - BRAINSTORM COMPLETE

**Status:** Complete
**Recommendation:** Approach 1 — Full CMS Removal following the 13-step ordered implementation strategy
**Next Phase:** PLAN

Ready for workflow-planner agent to create detailed implementation plan.

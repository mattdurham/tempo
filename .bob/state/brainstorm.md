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

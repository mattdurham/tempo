# Implementation Plan: Remove scan-fallback + retire DispatchBoundedRecentFirst via real early-stopping index resolution

## Overview

Two deliverables, sequenced so the first ships independently of the second:

1. **Phase 0** — remove the two remaining unconditional full-block-scan sites (case 1,
   `vr==nil`) in `Fetch`'s filter and structural branches, add two new blockpack
   trace-by-id sentinels, and do a wording/doc-comment cleanup pass. Small, safe,
   ~1 day, zero dependency on anything below.
2. **Phases 1-8** — retire `DispatchBoundedRecentFirst`/`RecentFirstBudget` entirely by
   building real newest-first early-stopping index resolution (single-leaf, multi-leaf OR,
   multi-leaf AND via a new anchor+confirm design, and structural's candidate ordering),
   so no case is ever "too expensive to serve" via the index. ~7-11 days.

This plan does not re-litigate scope — both pieces are user-confirmed, final. All file
paths/line numbers below were verified against current source during planning (not
grepped-and-assumed); line numbers will drift slightly as earlier phases land, but the
functions/signatures named are stable anchors.

**Correction log:**
- Phase 1.1 originally proposed minting a new "SPEC-VI-6" ID for the decode-time ordering
  assertion. team-spec-oracle confirmed SPEC-VI-6 is already taken (MergeTraceGroups'
  RefChecker contract, unrelated) — a real collision. Corrected: Phase 1.1 now AMENDS
  SPEC-VI-1 in place (same invariant claim, only its enforcement level changes from
  emergent-property to decode-time-asserted — this file's own "Addendum/Update" convention
  on SPEC-VI-1's existing Caveat paragraph is the right mechanism, not a new ID). Phase 8's
  genuinely NEW correctness claim (early-stopping top-N-by-recency parity with full
  resolution) is not an amendment to SPEC-VI-1's block-ordering claim — it gets its own
  new entry, SPEC-VI-12 (confirmed next-free ID), added to Phase 8's scope below.
- Phase 6's structural-dispatch-guard question (originally left as an open question for
  the team lead) is now RESOLVED per team-brainstormer's investigation: the guard STAYS,
  for a real, non-obvious reason unrelated to DT1. See Phase 6 below — this is now a hard
  constraint, not an open question.
- A real gap surfaced during #189/Phase 6 execution — match-all/intrinsic-only queries
  (zero leaf columns, nothing for the value index to evaluate) were unconditionally
  hard-erroring under Phase 0+6's new decline contract, breaking any real search request
  lacking an explicit filter leaf. Resolved via a new, narrowly-scoped Phase 6b (bounded
  newest-first match-all materializer) — see below. This is explicitly NOT a revival of
  `RecentFirstBudget`/`DispatchBoundedRecentFirst` (which stays retired for its own,
  different, expensive-filter use case).
- **Phase 6b landed (#192, implemented by coder-2).** One design claim in the original
  Phase 6b write-up was corrected post-implementation: the slice-job window-scoping safety
  property (`TestFetch_MatchAll_SliceJob_WindowScopingIsSafe`) does NOT hold via any
  window-partitioning inside `QueryNewestFirstMatchAll`/`BlockIndicesNewestFirst`
  themselves (the original text incorrectly analogized this to the filter path's
  `sourceRef` scoping) — it holds via `SPEC-STREAM-4`'s existing per-ROW time filtering in
  blockpack's `Collect` path (`stream.go`), the same mechanism every other query already
  relies on. Confirmed empirically and mutation-verified by coder-2. Corrected in Phase 6b's
  own section below — cite the test and SPEC-STREAM-4, not the sourceRef analogy, in any
  future reference to this property.

---

## Phase 0: Remove the unconditional vr==nil scan (Approach A) — INDEPENDENT, ships first

### Files to modify

**`tempodb/encoding/vblockpack/backend_block.go`**

1. Filter branch, inside `Fetch`, replace the `else` fallthrough at lines 957-975:

   ```go
   } else if getValueIndexQueryReader() != nil {
       needsBoundedRead = true
   }
   // else: vr == nil — falls through to unconditional scan (TO BE REMOVED)
   ```

   New behavior: when `idxErr == nil && !ok && getValueIndexQueryReader() == nil`,
   return immediately:

   ```go
   } else if getValueIndexQueryReader() != nil {
       needsBoundedRead = true
   } else {
       slog.Error("vblockpack Fetch: value index not configured on this querier", "query", query)
       return traceql.FetchSpansResponse{}, fmt.Errorf("vblockpack Fetch: %w", ErrMaterializedIndexBuilding)
   }
   ```

2. Structural branch, `case !opts.IndexOnly && getValueIndexQueryReader() == nil:` at
   line 1025 — currently a deliberate no-op (falls through to the unconditional scan
   switch at line 1084-1099). Change to return the same hard error:

   ```go
   case !opts.IndexOnly && getValueIndexQueryReader() == nil:
       slog.Error("vblockpack Fetch: structural query, value index not configured on this querier", "query", query)
       return traceql.FetchSpansResponse{}, fmt.Errorf("vblockpack Fetch: %w", ErrMaterializedIndexBuilding)
   ```

3. Verify (grep, do not assume) whether any other caller of `Fetch` depends on the
   vr==nil-unconditional-scan behavior via `wal_block.go` — confirmed in brainstorm this
   file has its own separate, untouched `Fetch` that calls `blockpack.QueryTraceQL`
   directly and never routes through `backend_block.go`'s `Fetch` at all. No change needed
   there; state this explicitly in the PR description so a reviewer doesn't go looking.

**`tempodb/encoding/vblockpack/slice_errors.go`**

Add a third sentinel alongside `ErrSliceIndexCoverageGap` and `ErrSearchNoCoverage`:

```go
// ErrMaterializedIndexBuilding is returned when the value-index query path is not
// configured on this querier at all (vr == nil) — a deployment-level absence, distinct
// from ErrSearchNoCoverage (configured, but this specific column/window lacks coverage).
// No supported deployment scans as a fallback for either case (issue #<task>).
var ErrMaterializedIndexBuilding = errors.New("vblockpack: materialized index is not configured on this querier")
```

Keep `ErrSearchNoCoverage` as a distinct, coexisting sentinel (case 2 — configured but
this specific column/window lacks coverage) rather than replacing it — recommended in the
brainstorm's Key Decisions, because operators want to distinguish "deployment never
configured the index" (actionable: configure it) from "not backfilled yet for this
window" (actionable: wait/retry) in logs/metrics even though the HTTP-facing text is
similar.

**`tempodb/encoding/vblockpack/decline_response.go`**

Add a case for `ErrMaterializedIndexBuilding` in `DeclineErrorToHTTPResponse`, and update
existing message text to use consistent "materialized index" phrasing:

```go
case errors.Is(err, ErrMaterializedIndexBuilding):
    return http.StatusUnprocessableEntity,
        "search requires the materialized value index, which is not configured on this querier",
        true
```

Update `ErrSearchNoCoverage`'s existing message (line 85-88) to read:
`"search index has no coverage yet (materialized index still building for this window) and no bounded-recent-first path was authorized"`
— NOTE: drop the "no bounded-recent-first path was authorized" clause once Phase 7
removes bounded-recent-first entirely; track as a follow-up edit in Phase 7, not now.

Leave `ErrCubeWarming`'s message (line 78-83) as its distinct "retry shortly" wording —
deliberate exception, self-healing case, not a permanent decline. Do NOT change it to
"materialized index building" phrasing (this would misdescribe a case where a retry is
guaranteed to eventually succeed with no operator action, vs. case 1 which needs operator
action).

Fix the stale comment at line 72: `"There is no longer an explicit opt-out setting
(2026-07-11...)"` — reword to scope the claim correctly (an explicit operator off-switch
was removed when a backend IS configured; `vr==nil` still occurs whenever no backend at
all is configured for this querier — that's exactly case 1 above).

**blockpack: `reader.go`** (public API addition — needs explicit sign-off, see Notes for
coder phase)

Two new exported sentinels, replacing the bare `fmt.Errorf` at the two sites:

```go
// ErrTraceByIDIndexNotConfigured is returned by GetTraceByID when no lister/tenant was
// supplied — a caller error, not a coverage gap (NOTE-VI-073: there is no scan fallback).
var ErrTraceByIDIndexNotConfigured = errors.New("blockpack: GetTraceByID: lister and tenant are required, there is no scan fallback")

// ErrTraceByIDCoverageGap is returned when zero trace-by-ID index files cover a window
// GetTraceByID's caller knows is non-empty — an indexing coverage gap, not an
// authoritative not-found (NOTE-VI-072).
var ErrTraceByIDCoverageGap = errors.New("blockpack: GetTraceByID: no trace-by-ID index files cover the query window, index coverage gap")
```

- `reader.go:431-435` (`lister == nil || tenant == ""` guard) — wrap:
  `return nil, fmt.Errorf("GetTraceByID: %w", ErrTraceByIDIndexNotConfigured)`
- `reader.go:487-492` (`len(keys) == 0` guard inside `getTraceByIDViaIndex`) — wrap:
  `return nil, fmt.Errorf("GetTraceByID: no trace-by-ID index files cover window [%d,%d] for tenant %q: %w", queryMinSec, queryMaxSec, tenant, ErrTraceByIDCoverageGap)`

**tempo: wire the two new blockpack sentinels through `decline_response.go`** with cases
mapping both to the same "materialized index building" 422 message family (the
`ErrTraceByIDIndexNotConfigured` case mirrors `ErrMaterializedIndexBuilding`'s text; the
`ErrTraceByIDCoverageGap` case mirrors `ErrSearchNoCoverage`'s text). Confirm
`modules/querier/http.go`'s `handleError` actually calls `DeclineErrorToHTTPResponse` for
`FindTraceByID`'s error path too (brainstorm noted it currently reaches the default 500
branch with no case at all — verify this call site exists and is wired the same way
Fetch's errors are before assuming the new cases take effect).

**Doc-comment cleanup (same diff, per brainstorm's "fix while touching this file"
finding):**
- `cubequerypath.go:11` — replace "Falls back to the full block scan on any error or cache
  miss." with a sentence describing the actual current behavior (falls through to the
  VI/metrics decline path, which itself hard-errors, never scans).
- `value_index_query.go` doc-comment references at lines 6, 80, 214, 227, 390, 516 (verify
  exact current line numbers before editing — file has grown since the brainstorm's read)
  — replace "falls back to a full block scan" phrasing with the accurate post-#481/post-
  this-task description.
- `rawfilestore.go:113` — same "falls back to a correct full scan" phrasing; this one
  describes `mapRawNotFound`'s behavior for a 404 File (retention race), which is
  UNCHANGED and still correct as "skip the file, the caller's build continues" — reword
  to avoid the word "scan" (the caller's build continuing is not a scan fallback, it is
  the existing 404-skip contract) rather than deleting the point being made.
- `value_index_structural_query.go` package doc comment (near the `tryStructuralIndexFetch`
  section, "nothing in the frontend builds a DispatchTimeSliced plan for a structural
  query yet") — **confirmed STALE by team-brainstormer's second pass: `structural_sharder.go`
  already builds `DispatchTimeSliced` plans for structural queries today** (DT1's Option B
  shipped). Reword to describe current reality: `DispatchTimeSliced` is the live,
  already-shipped dispatch mode for coverage-eligible structural queries; the interim-gate
  framing this comment originally described predates that shipment. See Phase 6 below for
  the full, now-resolved design context this comment needs to reflect.

### Phase 0 Test Plan (TDD)

**Rewrite, do not delete, these existing tests (they assert the OPPOSITE of new
behavior by name — a mutation-tested revert-and-confirm-red cycle is mandatory, not a
green diff):**

1. `fetch_bounded_dispatch_test.go:194` `TestFetch_VIDisabled_StillFullScans_Unchanged` →
   rename to `TestFetch_VIDisabled_HardErrors` (or similar), assert
   `errors.Is(err, ErrMaterializedIndexBuilding)` instead of asserting a full scan
   happened.
2. `fetch_bounded_dispatch_test.go:225` `TestFetch_VIDisabled_StillFullScans_Unchanged_WithLimit`
   → same rename/rewrite; a limit present must NOT change the outcome (still hard-errors,
   never routes to bounded — this case predates Phase 1-8 entirely, since vr==nil means
   there is no index to have declined against).
3. `structural_dispatch_test.go:335` `TestFetch_StructuralIndexOnlyFalse_VIDisabled_StillFullScans`
   → same rename/rewrite for the structural twin.

**Mutation-verify each: revert the `backend_block.go` fix locally, confirm the rewritten
test goes red (still expects the hard error, code still scans) — NOT the reverse (a test
that would pass either way is not a real regression guard). Restore the fix, confirm
green.**

**New test:** confirm the two blockpack sentinels propagate correctly through
`decline_response.go` end to end (a real `GetTraceByID` call with `lister == nil` against
a real reader built via the actual write path — per this session's standing "no
hand-built fixtures" convention — asserting the final HTTP status/message, not just the
Go error).

### Notes

- Phase 0 has **zero dependency** on Phases 1-8 and can ship, be reviewed, and be merged
  entirely independently — if the team lead wants an early checkpoint mid-cycle, this is
  it.
- Public API changes in this phase (blockpack): 2 new exported `var Err... = errors.New(...)`
  in `reader.go`. Small, additive, no removal. Needs explicit sign-off per blockpack's
  CLAUDE.md ("Do not add new public API surface without explicit user permission") before
  implementation — flag in the PR description, don't just add and hope.

---

## Phase 1: Spec/ordering foundation

### 1.1 — Amend SPEC-VI-1 in place (promote its enforcement level; do NOT mint a new ID)

The chronological-by-construction guarantee this whole plan depends on is currently
documented under SPEC-VI-1 as an emergent property, not asserted at decode time:

- File level: `SortFileMetas` (`internal/modules/valueindex/filename.go:120-126`) sorts
  `(Level ASC, WallMinSec ASC, WallMaxSec ASC)` — filename-derived, zero I/O.
- Block level: `SplitIntoBlocks` (`internal/modules/valueindex/bucketmerge.go:147-174`)
  `sort.Slice`s the full merged group list by `(TimeSec ASC, CanonicalValue ASC)` before
  chunking into fixed-size blocks by contiguous slice range — so block N+1 in the on-disk
  directory always covers strictly later time than block N, by construction, not merely
  filterably. L0 files are single-block (`writer.go:338-347`) — no block-ordering
  question for the common freshest case.
- Group level: `sortBucketBlock` (`internal/modules/valueindex/bucketfile.go:196-202`)
  sorts `Groups` by `(TimeSec ASC, CanonicalValue ASC)`.

**Correction (per team-spec-oracle): SPEC-VI-6 is already taken (MergeTraceGroups'
RefChecker contract, unrelated to block ordering) — minting a new ID for this assertion
would be a real collision. The correct mechanism is to AMEND SPEC-VI-1 in place**, using
this file's own established "Addendum/Update" convention on SPEC-VI-1's existing Caveat
paragraph: the underlying invariant CLAIM is unchanged (block directories are, and always
have been, chronologically ordered by construction for the value-index `BucketGroup`
format), only its ENFORCEMENT LEVEL is being promoted — from "emergent property, never
asserted on read" to "decode-time-asserted." This is exactly the kind of update SPEC-VI-1's
Caveat paragraph exists to record, not a new claim requiring a new ID.

**Recommendation (stated, not hedged): add the decode-time assertion now, do not leave
this purely emergent.** Early-stopping is about to hard-depend on this ordering for
correctness (a violated invariant would silently return the wrong newest-N, not crash) —
exactly the class of bug an exact-set-comparison test (Phase 8) can catch in a test
fixture but NOT in production if a future compaction-path change (e.g. a new merge
strategy) violates the invariant without touching `SplitIntoBlocks` itself. Add:

```go
// SPEC-VI-1 (amended, this task): promoted from an emergent property to a decode-time-
// asserted invariant. DecodeBucketFile asserts block directory entries are non-decreasing
// in MinTimeSec, since SplitIntoBlocks/sortBucketBlock guarantee this at write time and
// every early-stopping consumer now depends on it.
```

in `internal/modules/valueindex/bucketfile.go`'s `DecodeBucketFile` (or its metadata-decode
sibling `ReadBucketFileMetadata` in `bucketfile_metadata.go` — confirm exact function name
during implementation), checking `dir[i].MinTimeSec <= dir[i+1].MinTimeSec` for all `i` and
returning a typed decode error (not a panic) on violation. This is a NEW invariant check,
cheap (O(n) over an already-in-memory small directory), and turns a silent-wrong-answer
failure mode into a loud, typed error — consistent with this session's "authoritative
index must fail loudly, never mask" convention already established for NOTE-VI-071/078.

**Spec-work scope for this sub-phase:** update SPEC-VI-1's own entry (via the
team-spec-oracle workflow — route this through the spec-oracle agent, do not edit
SPECS.md/NOTES.md directly per this repo's standing convention) to add the
Addendum/Update noting the decode-time assertion, the new typed error it returns on
violation, and a back-ref to `DecodeBucketFile`/`ReadBucketFileMetadata`. Confirm with
team-spec-oracle that SPEC-VI-1's existing scope (value-index `BucketGroup` format) is
correctly distinct from the separate TraceID index (SPEC-VI-6, MergeTraceGroups'
RefChecker contract, deliberately ordered differently) so the amendment doesn't
accidentally widen SPEC-VI-1's claim to cover TraceID-index ordering it was never meant
to describe.

### 1.2 — Reverse block-directory iteration primitive

**File:** `internal/modules/valueindex/bucketquery_ranged.go`

Add a limit-aware, newest-first sibling of `QueryBucketFileRanged` (lines 36-109):

```go
// QueryBucketFileRangedNewestFirst mirrors QueryBucketFileRanged but iterates dir in
// REVERSE (newest block first — SplitIntoBlocks/sortBucketBlock guarantee ascending
// on-disk order, SPEC-VI-1 amended) and stops once len(out) >= limit (limit <= 0 means
// unbounded, identical to the non-limited sibling). matchGroupsInBlock's own group-level
// order within a decoded block must also be consumed in reverse (see 1.3) for a
// correct global newest-first order, not just block-level.
func QueryBucketFileRangedNewestFirst(
    ctx context.Context, src RangedSource, pred Predicate, timeRange *[2]uint64, limit int,
) ([]LookupResult, error) {
```

Implementation: same footer/tail read and file-level/block-level prune logic as
`QueryBucketFileRanged`, but the `for i := range dir` loop (line 88) becomes
`for i := len(dir) - 1; i >= 0; i--`, and after each block's `matchGroupsInBlock` call,
check `if limit > 0 && len(out) >= limit { return out, nil }`. Added as a SIBLING function
(matching this package's own established pattern for `lookupColumn`/`lookupColumnAll`
having match-all/predicate variants) — never modify `QueryBucketFileRanged` itself, since
every existing non-early-stopping caller must remain byte-identical.

### 1.3 — Reverse in-block group consumption

**File:** `internal/modules/valueindex/bucketquery.go` (where `matchGroupsInBlock` lives —
confirm exact file during implementation, brainstorm cited both bucketquery.go and
bucketquery_ranged.go as candidate homes)

Add a `matchGroupsInBlockReverse` (or a `reverse bool` parameter, whichever keeps
`matchGroupsInBlock`'s existing signature stable for its many other callers) that walks
`blk.Groups` from `len(Groups)-1` down to `0` instead of ascending — the data is already
sorted `(TimeSec ASC, CanonicalValue ASC)` at write time (1.1), so this is a pure
in-memory reversal, zero re-encode, zero extra I/O.

### 1.4 — Reverse multi-file ordering

**File:** `internal/modules/valueindex/filename.go`

Add a newest-first sibling sort, not a parameter on `SortFileMetas` (existing callers —
`DiscoverIndexFiles`, `filecache.go`'s `listColumn`/cache-insert paths — must keep today's
`(Level ASC, WallMinSec ASC, WallMaxSec ASC)` order unchanged):

```go
// SortFileMetasNewestFirst sorts by (Level ASC, WallMaxSec DESC, WallMinSec DESC) — lowest
// compaction level (freshest) still takes priority (matching SortFileMetas), but within a
// level, newest wall-clock time first. A pure comparator change over the same
// already-parsed FileMeta slice; zero additional I/O versus SortFileMetas.
func SortFileMetasNewestFirst(metas []FileMeta) {
```

**File:** `internal/modules/valueindex/discovery.go`

Add `DiscoverIndexFilesNewestFirst` (sibling of `DiscoverIndexFiles`, lines 36-76) that
calls `SortFileMetasNewestFirst` instead of `SortFileMetas` at line 69.

**File:** `internal/modules/valueindex/filecache.go`

Add `FilesForTimeRangeNewestFirst` (sibling of `FilesForTimeRange`, lines 87-113) — the
cache's underlying `entry.metas` stays sorted ascending (unchanged, since `RemoveFiles`
and other callers depend on the existing order); the new method calls a
`filterMetasNewestFirst` sibling of `filterMetas` (lines 292-300) that walks `metas` in
reverse and/or re-sorts the filtered subset via `SortFileMetasNewestFirst` — confirm during
implementation which is cheaper (the cached `metas` slice is typically small; either
approach is O(n) over an in-memory list).

### Phase 1 Test Plan (TDD)

- `TestQueryBucketFileRangedNewestFirst_ReturnsNewestNFirst` — build a real multi-block
  file via the actual write path (`SplitIntoBlocks`+real writer, NOT hand-built
  `BucketBlock` structs) with known, distinct per-block timestamps; assert the newest-first
  variant with a limit returns exactly the newest-limit entries, in the same order a full
  unbounded query's tail would produce.
- `TestDecodeBucketFile_RejectsOutOfOrderBlockDirectory` — hand-corrupt a real encoded
  file's block directory to violate ascending `MinTimeSec` order (a targeted mutation, not
  a hand-built decode struct) and confirm the amended SPEC-VI-1 assertion returns a typed
  error, not a silent wrong answer.
- `TestSortFileMetasNewestFirst_LevelStillWins` — mutation-test: confirm a same-level,
  different-time ordering flips vs. `SortFileMetas`, but cross-level ordering does not
  (Level ASC still takes priority in both).
- `TestFilesForTimeRangeNewestFirst_MatchesUnboundedTailReversed` — exact-set comparison
  against `FilesForTimeRange`'s own (ascending) result, reversed, over a real
  multi-file cached column.

---

## Phase 2: Single-leaf early-stopping (vibuilder)

### Files to modify

**`internal/modules/vibuilder/builder.go`**

1. New sibling of `queryKeysRanged` (lines 661-725):

   ```go
   // queryKeysRangedNewestFirst mirrors queryKeysRanged but processes keys in
   // newest-first BATCHES (sorted via valueindex.SortFileMetasNewestFirst before this
   // function is called — done by the caller, lookupColumnNewestFirst, since keys here
   // is already a []string with no FileMeta attached) instead of one all-at-once
   // errgroup fan-out. Batch size = downloadConcurrency (reuse the existing constant,
   // preserving the existing per-batch CPU/connection bound). After each batch
   // completes, check len(out) against limit; stop before starting the next batch if
   // satisfied. Within a batch, fan out concurrently exactly as queryKeysRanged does
   // today (preserving the existing latency-hiding benefit) — only the BETWEEN-batch
   // behavior is new (sequential, early-stoppable).
   func queryKeysRangedNewestFirst(
       ctx context.Context, store FileStore, keys []string, pred valueindex.Predicate,
       timeRange *[2]uint64, limit int,
   ) ([]valueindex.LookupResult, int, int64, error) {
   ```

   Internally calls `valueindex.QueryBucketFileRangedNewestFirst` (Phase 1.2) per key
   instead of `valueindex.QueryBucketFileRanged`, threading `limit` down per-file too (a
   single very-wide file can itself satisfy the whole limit — do not wait for the whole
   batch to complete unnecessarily, though checking after the batch as originally
   speced is the simpler, still-correct MVP; consider a `sync.Once`-guarded early
   cancellation of sibling in-flight goroutines within a batch once one file alone
   satisfies the limit, as a follow-up optimization, not required for correctness).

2. New sibling of `lookupColumn` (lines 492-515):

   ```go
   func lookupColumnNewestFirst(
       ctx context.Context, disc FileDiscoverer, store FileStore, col string,
       colType modules_shared.ColumnType, pred valueindex.Predicate, timeRange *[2]uint64,
       limit int,
   ) ([]modules_executor.VILookupResult, int, int64, error) {
   ```

   Same shape as `lookupColumn` but calls a `FileDiscoverer` newest-first variant (see
   below) and `queryKeysRangedNewestFirst` instead of the ascending pair.

3. `FileDiscoverer` interface (lines 100-102) needs a newest-first sibling method or a
   second interface — confirm during implementation whether to add
   `FilesForTimeRangeNewestFirst` as a NEW interface (`FileDiscovererNewestFirst`) so
   `*blockpack.IndexFileCache` (which already gets `FilesForTimeRangeNewestFirst` from
   Phase 1.4) satisfies both without a breaking signature change to the existing
   `FileDiscoverer` interface's single method.

4. New sibling of `BuildSource` (lines 127-249): `BuildSourceBounded` (or
   `BuildSourceNewestFirst` — pick one name, use consistently across both repos' plan
   language) — same shape as `BuildSource` but threads `limit` through to
   `lookupColumnNewestFirst` for the leaf loop (lines 173-222). **Scope for THIS phase:
   single-leaf only** — when `len(work) > 1` (multiple leaves, i.e. a multi-leaf AND
   query), Phase 2's `BuildSourceBounded` should explicitly detect this and either (a)
   fall through to the ordinary unbounded `lookupColumn` path per leaf (deferring
   early-stop entirely to Phase 3/4), or (b) return a typed "not yet supported, multi-leaf"
   signal the caller treats as "cannot bound this query yet." Recommend (a) for
   implementation simplicity in this phase — Phase 3/4 will replace this fallback with
   real multi-leaf logic, so Phase 2's `BuildSourceBounded` should be written expecting to
   be edited again in Phase 3, not as a permanent single-leaf-only public shape.

5. Match-all path (`lookupColumnAll`, lines 521-593) is explicitly OUT of scope for
   early-stopping per the brainstorm's own finding — a match-all query has no
   selectivity concept to bound; leave unchanged. **See Phase 6b for how a TRUE
   zero-leaf-column match-all query (as opposed to a `{} | rate()`-shaped match-all with
   an explicit column list, which this `lookupColumnAll` path already serves for metrics)
   is now handled — a different code path entirely, bypassing the value index.**

### tempo-side wiring for the single-leaf case

**`tempodb/encoding/vblockpack/value_index_query.go`**

`tryIndexFetch` (lines 314-405) needs a new code path: when `boundedAuthorized` is true
(a limit is present) and the leaf count is 1 (single-leaf query — Phase 2's scope), call
`blockpack.BuildValueIndexSourceBounded` (new public wrapper, see below) instead of
`blockpack.BuildValueIndexSource` at line 338, threading `queryOpts.Limit` down.
`declineOutcomeBounded` (lines 426-452) then only needs its `boundedAuthorized` branch
reached when Phase 2's new path ALSO declines (e.g. still zero coverage even bounded) —
this is a real decline, not a routine "route to bounded scan" signal anymore.

blockpack root **`api.go`**: new public wrapper `BuildValueIndexSourceBounded(ctx, disc,
store, prog, minSec, maxSec, watermarks, limit) (*executor.SliceValueIndexSource, bool,
error)` mirroring `BuildValueIndexSource`'s existing wrapper shape 1:1, calling
`vibuilder.BuildSourceBounded` — additive, non-breaking public API addition (needs
sign-off, see consolidated list at the end of this plan).

### Phase 2 Test Plan (TDD) — the core correctness property

Per the brainstorm's own Q5 test-shape finding, this is NOT a "looks right" test:

- `TestBuildSourceBounded_SingleLeaf_ExactOracleMatch` — build a real multi-file,
  multi-block fixture via the actual write path with MORE matches than `limit`, at known
  distinct timestamps. Run `BuildSource` (unbounded, existing) as the oracle. Run
  `BuildSourceBounded` with `limit` below the true match count. Assert the bounded
  result is EXACTLY the oracle's newest-`limit` entries by timestamp — not merely the
  right count.
- Mutation-verify: flip the sort direction in `SortFileMetasNewestFirst` or
  `QueryBucketFileRangedNewestFirst` (oldest-first instead of newest-first); confirm this
  test goes red. Revert, confirm green. Repeat for an off-by-one in the batch-boundary
  early-stop check in `queryKeysRangedNewestFirst`.
- `TestBuildSourceBounded_MultiLeaf_FallsThroughToUnbounded` — confirm the Phase-2 stopgap
  (option (a) above) behaves correctly and does not silently under-report for a 2+-leaf
  query (documents the INTENTIONAL Phase 2 limitation, to be replaced in Phase 3/4 — this
  test should be updated/removed, not just left passing unexamined, once Phase 3/4 lands).

---

## Phase 3: Multi-leaf OR early-stopping

### Design

A k-way merge-by-recency across leaves: resolve each OR'd leaf newest-first (Phase 2's
per-leaf machinery, reused as-is), then merge-sort the per-leaf newest-first streams by
`TimeSec` descending, stopping once the UNION (deduplicated by span key, matching
`viUnionSorted`'s existing identity rule) reaches `limit`. This is well-understood and
does not need the anchor+confirm design multi-leaf AND requires (union is naturally
non-lossy: a match found on any leaf is a true OR match, no confirmation needed).

### Files to modify

**`internal/modules/executor/metrics_trace.go`**

`viUnionSorted` (lines 299-319) is currently a pure merge-join over TWO already
identity-sorted inputs — it has no concept of "stop early" and no concept of recency
order (its inputs are sorted by span key, not time). This phase needs a NEW function,
not a modification of `viUnionSorted` (which every existing non-bounded OR query still
needs unchanged):

```go
// viUnionNewestFirst merges N per-leaf newest-first-ordered VILookupResult slices
// (Phase 2's BuildSourceBounded output per leaf) into a single newest-first stream,
// deduplicating by the same 22-byte span key viSpanKey uses (viSpanCmp identity rule,
// unchanged from the existing AND/OR combination logic), stopping once the
// deduplicated output reaches limit. A k-way heap merge (container/heap) over N
// per-leaf streams, not a repeated pairwise viUnionSorted (which is key-sorted, not
// time-sorted, and would defeat early-stopping).
func viUnionNewestFirst(sets [][]VILookupResult, limit int) []VILookupResult {
```

**`internal/modules/vibuilder/builder.go`**

`BuildSourceBounded` (Phase 2) needs to detect the multi-leaf ALL-OR case (every sibling
node under an OR combinator — check `preds.Nodes`' top-level `IsOR` and absence of nested
AND) and route to a new per-leaf-then-merge path calling `lookupColumnNewestFirst` per
leaf (bounded by `limit` each, an over-fetch-safe upper bound since union can only grow)
then `executor.viUnionNewestFirst` (via a thin adapter, since vibuilder and executor are
different packages — confirm import direction, executor is lower-level per
`recentfirst.go`'s own comment about import cycles, so this merge logic likely belongs in
`executor`, with vibuilder calling into it, mirroring how `viEvalNodes` already lives in
executor and vibuilder's `BuildSource` never re-implements boolean combination itself).

### Phase 3 Test Plan (TDD)

- `TestViUnionNewestFirst_KWayMerge_ExactOracleMatch` — 3+ leaves, deliberately
  interleaved timestamps across leaves (leaf A has the 1st/4th/7th newest matches, leaf B
  the 2nd/5th, etc.), oracle = full `viUnionSorted`-based OR resolution re-sorted by time;
  assert exact top-`limit` match.
- Mutation-verify: swap the heap's comparator direction; confirm red, then green.
- `TestViUnionNewestFirst_DedupAcrossLeaves` — same span appears in two leaves' results
  (a span matching both OR conditions); confirm it's counted once toward `limit`, matching
  `viSpanCmp`'s existing identity rule.

---

## Phase 4: Multi-leaf AND early-stopping (anchor + confirm) — hardest phase

### The landmine, restated precisely

Independently early-stopping each AND leaf to its own newest-`limit` and then intersecting
(`viIntersectSorted`) can silently under-report: a true intersection match can be outside
one leaf's own truncated top-N when that leaf has materially lower selectivity than the
intersection as a whole. There is no existing "resolve anchor, then CONFIRM narrowed
candidates against remaining leaves" mechanism for plain filter (non-structural) queries
today — this must be built new, mirroring structural's `chooseCandidateTraceIDs`/
`intersectTraceIDSets` DESIGN PATTERN (not its code — that operates on TraceIDs and
`structuralSpanAddr` sets; the filter path operates on the finer-grained 22-byte span key).

### Design

1. **Anchor selection:** pick the AND leaf with the HIGHEST selectivity (most
   restrictive — smallest expected result set), using the SAME VCNT-based cost-estimation
   machinery `queryplan`/`vcnt_cost.go` already uses frontend-side for
   `LowSelectivity`/`UnknownSelectivity` classification (reuse, do not re-invent a second
   cost model — confirm exact entry point, likely `ClassifyProgramVCNT`/
   `VCNTColumnTotalFunc`, needs to be reachable from vibuilder/executor or have its
   decision threaded down from tempo, since the querier already computes selectivity
   per-block via `CheckIndexCoverage`'s caller context — this wiring detail needs
   confirmation during implementation, flag as an open design question for the coder to
   resolve with the planner if the cost function isn't cleanly reachable from
   vibuilder's package position).
2. **Resolve the anchor leaf newest-first with early-stop**, targeting `limit *
   overFetchFactor` candidates (an over-fetch margin — e.g. 2-4x `limit` — because some
   anchor candidates will fail confirmation against the other leaf(s); the exact factor
   is a tuning constant, not a correctness-critical value, since a genuine under-fetch
   just means MORE anchor batches get pulled, not a wrong answer — see step 4).
3. **Confirm each anchor candidate against the remaining leaf(s)** via a targeted,
   per-candidate membership check (NOT an independent full/truncated resolution of the
   other leaf followed by intersection) — e.g. for each anchor-leaf match's span key,
   check membership in the other leaf(s)' index entries for that SAME time window,
   narrowed by the anchor's own candidate set (mirrors `intersectTraceIDSets`'s
   membership-check shape, generalized from TraceID to span key).
4. **If confirmed-count < limit after exhausting the over-fetched anchor batch, pull the
   NEXT anchor batch** (widen the newest-first anchor resolution) and repeat — this is
   the mechanism that makes over-fetch-factor a performance tuning knob, not a
   correctness parameter: worst case (anchor's selectivity assumption was wrong), this
   degrades toward resolving the anchor's full candidate set, never returns a wrong
   (silently truncated) answer.

### Files to modify

**`internal/modules/executor/metrics_trace.go`** (or a new sibling file,
`metrics_trace_bounded.go`, if this grows large enough to warrant separation — confirm
during implementation, matching the module's existing one-concept-per-file convention
where reasonable)

New function, sketch:

```go
// viIntersectNewestFirstAnchored resolves an AND query's early-stopped result via
// anchor+confirm: anchorResults is the anchor leaf's newest-first-ordered candidate set
// (already over-fetched by the caller); confirmFn checks one candidate's span key against
// the remaining leaf(s)' coverage for the SAME window. Returns the confirmed newest-limit
// matches, and a bool indicating whether MORE anchor candidates are needed (anchor
// exhausted before reaching limit confirmed matches) so the caller can pull another batch.
func viIntersectNewestFirstAnchored(
    anchorResults []VILookupResult, limit int, confirmFn func(VILookupResult) (bool, error),
) (confirmed []VILookupResult, needMore bool, err error) {
```

**`internal/modules/vibuilder/builder.go`**

`BuildSourceBounded`'s multi-leaf-AND branch (the Phase-2 stopgap this phase replaces):
selects the anchor leaf (using the cost-estimation hook above), calls
`lookupColumnNewestFirst` for it with the over-fetch-adjusted limit, builds a `confirmFn`
closure that calls `lookupColumn` (existing, unbounded, but narrowed by a per-key
predicate check against the SPECIFIC candidate's value — confirm the exact mechanism:
likely a targeted `QueryBucketFileRanged`-style lookup restricted to the anchor
candidate's own time window rather than a full leaf resolution) for each remaining leaf,
and loops per the design's step 4 until satisfied or genuinely exhausted (falls through to
the Phase-2 stopgap behavior — i.e., resolve the whole thing unbounded — as the safe
worst case, never a wrong answer).

### Phase 4 Test Plan (TDD) — this is the landmine-specific test the brainstorm called out

- `TestViIntersectNewestFirstAnchored_DoesNotUnderReport_DifferentSelectivityLeaves` —
  TWO leaves with deliberately different selectivity: one leaf's matches are a strict
  superset of the true intersection, much larger and differently time-distributed than
  the intersection itself (constructed via the real write path, per this session's
  standing "no hand-built fixtures" convention). Assert the anchor+confirm result does
  NOT silently drop a true intersection match that would have been outside a naively
  independently-truncated leaf's own top-N. **Write this test FROM THE FAILURE MODE
  FIRST** — confirm a naive "truncate each leaf to limit, then `viIntersectSorted`"
  implementation actually fails it (this is the negative control proving the test is
  real), THEN confirm the anchor+confirm implementation passes it.
- `TestViIntersectNewestFirstAnchored_ExactOracleMatch` — oracle = full unbounded AND
  resolution (existing `viIntersectSorted` path) re-sorted by time; exact top-`limit`
  comparison, same shape as Phase 2/3's oracle tests.
- `TestViIntersectNewestFirstAnchored_WidensOnInsufficientConfirmations` — anchor's
  over-fetch batch yields fewer than `limit` confirmed matches; assert the second batch
  is pulled and the final result is still exact (proves step 4's widening loop is
  correct, not just present).
- Mutation-verify: pick the WRONG leaf as anchor (least selective instead of most);
  confirm this doesn't silently produce a wrong answer (it should just be slower,
  correctness-preserving — this test documents that anchor CHOICE is a performance
  concern only, and should stay that way by construction) — a useful design-invariant
  test, not just a bug-catcher.

---

## Phase 5: Structural candidate ordering

### Design

Per the brainstorm's finding, structural's OWN early-stopping mechanism already exists
(`evalOneStructuralCandidateTrace`, `structural_index.go:322-324`, checks
`opts.Limit > 0 && len(result.Matches) >= opts.Limit` and signals `done`), and its
caller's loop (`ExecuteStructuralFromIndex`, lines 202-216) already `break`s on `done`.
**The only missing piece is feeding `candidateTraceIDs` in newest-first order** — today
`chooseDiscoverySeed` (lines 349-356) sorts lexicographically by TraceID bytes for
determinism, discarding time order.

**Scope note (see Phase 6 for the full explanation): this phase changes RESOLUTION order
only, within whatever dispatch model a structural query already reached (almost always
`DispatchTimeSliced`, one job per window, per DT1's shipped Option B). It has no bearing
on, and does not relax, the separate DISPATCH-layer per-block-fan-out guard Phase 6
confirms must stay in place.**

### Files to modify

**`internal/modules/executor/structural_index.go`**

1. `groupVILookupResultsByTrace` (lines 334-345) currently discards `TimeSec` when
   grouping into `map[[16]byte]map[structuralSpanAddr]struct{}`. Needs to ALSO track a
   per-trace max-recency value. Recommend changing the return type (or adding a sibling
   function) to also return `map[[16]byte]uint64` (trace → max TimeSec across its
   matching spans) — confirm whether to bake this into the existing map's value type
   (e.g. wrap the set in a small struct `{spans map[structuralSpanAddr]struct{}; maxTimeSec
   uint64}`) or return a second parallel map; prefer the wrapped-struct approach to keep
   the two pieces of per-trace state from drifting apart across the call chain.

2. `chooseDiscoverySeed` (lines 349-356) — replace
   `sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })`
   with a sort by max-recency DESCENDING (newest first), falling back to the existing
   byte-comparison as a tiebreaker for determinism (needed for reproducible test
   fixtures and stable pagination behavior, not for correctness).

3. `intersectTraceIDSets` (lines 418-426) — currently order-preserving from `ids`; since
   `ids` will now already be newest-first-ordered by the change above, this function
   needs NO change — it already preserves whatever order it's given.

4. `chooseCandidateTraceIDs` (lines 374-414) — no structural change needed; it already
   calls `chooseDiscoverySeed` at both call sites (lines 399, 403), so both automatically
   inherit the new newest-first order once (2) lands.

### Phase 5 Test Plan (TDD)

- `TestChooseDiscoverySeed_NewestFirst_ExactOracleMatch` — real structural fixture (via
  the real write path — this session's DT1-investigation "MANDATORY new test class"
  precedent applies here directly) with traces at known, distinct max-recency values;
  assert candidate order matches newest-recency-first, and that
  `ExecuteStructuralFromIndex` with a `Limit` returns exactly the newest-`limit` confirmed
  matches, not an arbitrary subset of the right size.
- Mutation-verify: revert to lexicographic-by-TraceID-bytes ordering; confirm the new
  test goes red (proves the test actually depends on ordering, not just count).
- Confirm existing structural tests that assert a SPECIFIC candidate order today (if any
  — grep `structural_index_test.go`/`structural_parity_golden_test.go` for
  order-sensitive assertions before assuming none exist) are updated, not broken silently.

---

## Phase 6: tempo-side dispatch rewiring

### Hard constraint (RESOLVED, not an open question): filter and structural queries need
ASYMMETRIC dispatch-rewiring treatment — the structural per-block decline guard STAYS

team-brainstormer investigated whether Phase 5's structural early-stopping fix makes it
safe to relax `tryStructuralIndexFetch`'s `!indexOnly` early-return (the guard that
restricts the index-driven structural path to genuine slice/`DispatchTimeSliced` jobs,
never ordinary per-block dispatch). **Answer: no — the guard must stay, for a real,
non-obvious reason distinct from (and not resolved by) DT1 or Phase 5's ordering fix:**

- **The duplication risk DT1 addressed is a DISPATCH-layer property** (how many jobs get
  built, what window each one covers) — decided upstream, at the frontend, BEFORE any
  querier-side resolution ever runs. Early-stopping (Phase 5) only changes RESOLUTION
  COST (how cheaply/quickly one job's own answer is computed) — it is an entirely
  orthogonal axis from dispatch shape. Fixing resolution order does nothing to fix (or
  worsen) how many overlapping jobs a per-block dispatch model would still create.
- **Worse: early-stopping makes relaxing the guard LESS safe than before it existed, not
  more.** Before early-stopping, two duplicate per-block jobs over the same window would
  both run a full, deterministic resolution and return IDENTICAL top-K sets (any
  duplication was purely a cost/performance problem, not a correctness one — this was
  Option A's actual rejection reason in plan-d.md, not a correctness issue). With
  early-stopping's "stop once satisfied" behavior, two jobs over the same window are NO
  LONGER guaranteed to return identical result sets — timing and cache-state
  sensitivity at the limit boundary (which candidate happens to satisfy `limit` first,
  which batch a given job's own I/O timing lands on) can make two duplicate per-block
  jobs return DIFFERENT top-K sets for the same query. Relaxing the per-block guard now
  would convert DT1's old "wasteful but correct" duplication into a genuinely new
  correctness bug (non-deterministic, job-timing-dependent results), which is a strictly
  WORSE outcome than the status quo, not a safe cleanup opportunity.

**Concrete, hard requirement for Phase 6's design (not a suggestion): apply an
ASYMMETRIC dispatch-rewiring pattern, never one uniform pattern for both query types:**

- **Filter queries:** safe to dispatch per-block. `tryIndexFetch` already restricts each
  block-job's own output via the `sourceRef` parameter passed into
  `QueryTraceQLFromIndex` — a filter job's answer is intrinsically scoped to its own
  block's data, so per-block dispatch (ordinary `DispatchBlockSharded` fanout) has no
  duplication risk regardless of early-stopping. Phase 6's filter-path rewiring (the
  `tryIndexFetch`/`declineOutcomeBounded` changes described earlier in this phase)
  applies as originally planned, unmodified by this finding.
- **Structural queries:** must NEVER be dispatched per-block, with or without
  early-stopping. They are only safe to answer from the index when dispatched via the
  one-job-per-window model (`DispatchTimeSliced`, DT1's already-shipped Option B,
  `structuralTimeSlicedJobsFunc`/`structural_sharder.go`) — or declined entirely. **Do
  NOT touch `tryStructuralIndexFetch`'s `!indexOnly` early-return
  (`value_index_structural_query.go:53-58`) in Phase 6 or anywhere in this plan.** It is
  not a stale interim gate to be cleaned up once DT1/Phase 5 land — it is the permanent,
  correctness-required boundary between the two dispatch models. The earlier draft of
  this plan flagged this as an open question for the team lead; it is now resolved and
  should be implemented (i.e., left alone) as a hard constraint, not re-raised as a
  decision point during coding.

### Files to modify

**`tempodb/encoding/vblockpack/value_index_query.go`**

`tryIndexFetch` (lines 314-405): once Phases 2-4 land, the `boundedAuthorized` branch no
longer means "route to a bounded raw-block scan" — it means "call
`BuildValueIndexSourceBounded` with `queryOpts.Limit` instead of the unbounded
`BuildValueIndexSource`." Restructure so `boundedAuthorized` selects WHICH blockpack entry
point is called at line 338, not (as today) a post-hoc decline-routing decision at lines
347/357/401. `declineOutcomeBounded` (lines 426-452) simplifies: the
`!indexOnly && boundedAuthorized` branch's "(nil, false, stats, nil) — relayed unchanged so
Fetch's caller routes to the bounded path instead of a scan" contract goes away entirely,
since there's no separate "bounded path" outside the index anymore — a decline from the
NEW bounded entry point IS a genuine decline (no coverage even with early-stopping
attempted), which should hard-error with `ErrSearchNoCoverage` (or a new, more precise
sentinel — see Phase 7) regardless of `boundedAuthorized`. **This filter-path rewiring is
unaffected by the asymmetry finding above** — it was always scoped to the per-block-safe
filter path only. **Note: this branch is only ever reached for a query with at least one
leaf predicate — see Phase 6b for the separate, zero-leaf match-all case, which is
detected and routed BEFORE `tryIndexFetch` is ever called at all.**

**`tempodb/encoding/vblockpack/value_index_structural_query.go`**

**Do NOT modify `tryStructuralIndexFetch`'s `!indexOnly` early-return (lines 53-58) — see
the Hard Constraint above.** The structural path's only Phase 6 work is: confirm Phase 5's
newest-first `chooseDiscoverySeed` ordering is correctly exercised through the EXISTING
`DispatchTimeSliced`/`indexOnly==true` call path (no dispatch-model change needed, since
that path already exists and already hard-errors on decline per
`structuralDeclineOutcome` — Phase 5 only changes which candidates get resolved first
inside that already-correct dispatch shape). No change to the guard itself, no change to
`backend_block.go`'s structural branch beyond what Phase 0 already did.

**`tempodb/encoding/vblockpack/backend_block.go`**

`boundedAuthorized`/`needsBoundedRead`/`newBoundedRecentFirstBudget` (lines 43-75,
884-976, 1061-1099) — once Phases 2-6's bounded entry points are wired at the
`tryIndexFetch` call site (filter path only — see asymmetry finding above; the structural
branch's `IsStructuralQuery(query) && boundedAuthorized` check at line 1061 is addressed
by Phase 7's removal, not by any NEW bounded-index call here, since structural per-block
dispatch was never a safe target for the bounded-index path to begin with), `needsBoundedRead`
and `queryOpts.RecentFirstBudget` (line 1082) become fully dead for the FILTER case — no
code path sets them to anything meaningful anymore for filter queries. The structural
branch's own bounded-scan usage is retired by Phase 7's deletion instead (structural
queries that decline under `DispatchBlockSharded`/no-indexOnly dispatch — the
`LowSelectivity+limit`/`UnknownSelectivity+limit` case — simply hard-error post-Phase-7,
same as they would for the filter path, rather than gaining a new bounded-index
per-block path that this finding rules out). Defer actually DELETING this code to
Phase 7 (keep it inert but present during Phase 6, to avoid a big-bang change that
couples "rewire the call sites" with "delete the old mechanism" in one commit — easier
to review and bisect as two steps). **The new Phase 6b match-all branch (see below) is
checked and handled BEFORE any of this — see Phase 6b's exact insertion point in `Fetch`.**

### Phase 6 Test Plan (TDD)

- Rewrite `TestFetch_LowSelectivityNoCoverageLeaf_WithLimit_UsesBoundedRecentFirst_NotScan`
  and `TestFetch_SelectiveShapedQuery_PerBlockBuildError_WithLimit_UsesBoundedRecentFirst`
  (both named for the OLD behavior) to assert the NEW bounded-index-path outcome instead
  — a real early-stopping index answer, not a raw-block-scan budget exhaustion. Confirm via
  `grep` at implementation time whether these exact test names still exist (file layout
  may have shifted since the brainstorm's read).
- Rewrite `TestFetch_ThreeNodeStructuralChain_WithLimit_UsesBounded`
  (`fetch_bounded_dispatch_test.go:170`) to assert the STRUCTURAL outcome post-Phase-7 (a
  hard error once `RecentFirstBudget` is gone — see Phase 7), NOT a new bounded-index
  per-block path — this test's OLD name/intent ("uses bounded") no longer applies to
  structural at all once this asymmetry is implemented; rename accordingly (e.g.
  `TestFetch_ThreeNodeStructuralChain_WithLimit_PerBlockDispatch_HardErrors`).
- **New regression guard, specifically for this phase's hard constraint:**
  `TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch` — asserts
  `tryStructuralIndexFetch` still returns its routine `(nil, false, stats, nil)` decline
  for EVERY `indexOnly=false` call, regardless of vr state or Phase 5's ordering fix —
  pins the guard as permanent, not incidentally-still-true. Mutation-verify: temporarily
  remove the `!indexOnly` early-return, confirm this test goes red (proves the guard is
  load-bearing and this test actually depends on it), revert, confirm green.
- Integration-style test: a real limit-bearing FILTER query with genuine multi-leaf AND
  low-selectivity shape, through the REAL `tempodb.New`/`Fetch` call chain (not a
  hand-constructed `QueryOptions`), confirming the whole filter-path dispatch rewiring
  produces the exact newest-limit answer end to end.

---

## Phase 6b: Bounded newest-first materializer for match-all/zero-leaf-column queries

**Status: LANDED (#192, implemented by coder-2; #189/Phase 6 also complete). This
section is now a record of the shipped design, corrected post-implementation where
coder-2's own testing found the original design note imprecise (see below) — kept in
full for future reference, not trimmed to a changelog entry.**

### The gap (surfaced during #189/Phase 6 execution — real production code, not a
contrived edge case)

Match-all / intrinsic-only TraceQL queries (a plain `{}`, and any request whose
conditions reduce to `pkg/traceql/storage.go`'s `SearchMetaConditions()` — confirmed via
direct code read: every entry `SearchMetaConditions()` returns has `Op: OpNone`, a
"no filter, just fetch this metadata column" marker, not a predicate; `pkg/traceql/
engine.go:115`'s `SearchMetaConditionsWithout` call is the REAL, production
metadata-augmentation path every search request goes through) compile to a
`*blockpack.Program` whose `Predicates.Nodes` AND `Predicates.Columns` are BOTH empty —
confirmed via `vibuilder.BuildSource`'s own "truly nothing referenced" early return
(`builder.go:145-150`: `if preds == nil || (len(preds.Nodes) == 0 && len(preds.Columns)
== 0) { return nil, false, nil }`). There is no leaf column for the value index to
evaluate at all. Phase 0+6's hard-error contract (a routine decline with no bounded
fallback must now error, never scan) fires unconditionally on this shape too, because
`tryIndexFetch`'s decline path cannot distinguish "no predicate exists to evaluate at
all" from "a predicate exists but has no coverage" — both currently collapse to the
identical `(nil, false, nil)` signal from `BuildValueIndexSource`/`BuildSource`.

**This is NOT a coverage gap** — there is nothing to decline coverage FOR. **This is
also explicitly NOT a resurrection of `RecentFirstBudget`/`DispatchBoundedRecentFirst`**
— that mechanism existed to bound the COST of evaluating an EXPENSIVE FILTER via a scan,
for `LowSelectivity`/`UnknownSelectivity` classifications, and stays retired for that
purpose in Phase 7. A match-all query has no filter to evaluate at any cost — every span
in every visited block matches trivially, by definition of "match-all." The correct fix
(user's own resolution, verbatim): **"Why are these never? Basically just grab the first
X traces that match the limit by span start."**

### 1. Verified: is a genuinely unbounded (limit=0) match-all request reachable in production?

**No, not through the standard frontend-fronted path — confirmed via direct code read:**
- `modules/frontend/search_sharder.go:107`: `searchReq.Limit, err = adjustLimit(searchReq.Limit,
  s.cfg.DefaultLimit, s.cfg.MaxLimit)` — called ONCE, before any per-block/per-slice job
  is built, on the SAME `searchReq` object every job's `SearchBlockRequest` copies its
  own `Limit` from.
- `modules/frontend/search_handlers.go:165-168`: `adjustLimit` returns `defaultLimit`
  whenever `limit == 0` — never passes 0 through.
- `modules/frontend/config.go:105`: `DefaultLimit: 20` — the production default.
- `modules/querier/querier.go:648`: `opts.MaxTraces = int(req.SearchReq.Limit)` — the
  querier trusts the caller's `Limit` verbatim, with **no independent defaulting of its
  own**.

So for every request that goes through the frontend's sharder — the standard, and
effectively only supported, production entry point, including #487 slice jobs, which
build their per-slice `SearchBlockRequest`s from the SAME already-adjusted `searchReq` —
`opts.MaxTraces`, and therefore `backend_block.go`'s `spanLimit` (`spanLimit = maxTraces
* 20` when `maxTraces > 0`), is **always positive** by the time `Fetch` runs.

**However, `Querier.SearchBlock` (`modules/querier/querier.go:617`) has no defaulting of
its own** — a caller that bypasses the frontend sharder entirely (a direct querier call,
a test harness, or a hypothetical future internal caller) and sends `SearchReq.Limit ==
0` would reach `Fetch` with `spanLimit == 0`. **Per the team lead's own instruction, this
residual case must still hard-error** — a genuinely unbounded match-all has the same
"too expensive to serve" problem Phase 0/6 already established a hard-error contract
for, just for a different underlying reason (no ceiling at all, vs. no coverage). This
plan's design below hard-errors exactly this case, and only this case.

### 2. Design: the bounded newest-first match-all materializer

**Feasibility, verified:** the main blockpack DATA file (distinct from the value index —
this whole plan's Phases 1-6 have all been about the `internal/modules/valueindex`
package; this is the `internal/modules/blockio/reader` package instead) already carries
the same kind of time-ordering metadata the value index does. `internal/modules/blockio/
reader/ts_index.go`'s `Reader.BlocksInTimeRange` operates over `tsRaw`/`tsIndexEntry`
entries the function's own binary-search precondition comment confirms are "sorted by
minTS." For legacy files with no TS index section (`ensureV14TSSection` fails, `tsCount
== 0` — confirmed via `format_v8_test.go`'s own `TestV8_BlocksInTimeRange`, "must return
nil with no TS index"), `BlockMeta.MinStart`/`MaxStart`
(`internal/modules/blockio/shared/blockmeta.go:7-8`) are ALREADY available per-block
metadata with zero body I/O, so a newest-first order is derivable even for old files by
sorting the in-memory `BlockMeta` list by `MaxStart` DESCENDING — no format change, no
migration, mirroring Phase 1's own finding for the value index almost exactly.

**Detection point (blockpack, new public helper) — avoids tempo re-deriving vibuilder's
own internal decision independently, mirroring `AllLeavesIndexable`'s precedent of
exposing an internal per-leaf decision publicly for exactly this reason:**

```go
// IsMatchAllProgram reports whether prog has zero leaf predicates AND zero listed
// columns — the "truly nothing referenced" shape vibuilder.BuildSource's own early
// return already detects internally. Exposed so tempo's Fetch can route this shape to
// the bounded newest-first materializer instead of ever attempting a value-index build
// for a query that has no predicate to look up in the first place.
func IsMatchAllProgram(prog *Program) bool {
    return prog == nil || prog.Predicates == nil ||
        (len(prog.Predicates.Nodes) == 0 && len(prog.Predicates.Columns) == 0)
}
```

**New blockpack public entry point** (root `api.go`, mirrors `QueryTraceQLWithProgram`'s
existing shape):

```go
// QueryNewestFirstMatchAll materializes up to opts.Limit spans from r, newest-first by
// span start time, with NO predicate evaluation at all — every span in a visited block
// is a match by definition, since there is no filter to check it against. Requires
// opts.Limit > 0 (see the caller-facing ErrMatchAllRequiresLimit contract, tempo-side);
// there is no bounded-COST concept for a query with nothing to filter, so a caller MUST
// supply a ceiling. This is explicitly NOT RecentFirstBudget/SPEC-ROOT-023 (retired,
// Phase 7, for the EXPENSIVE-FILTER case) — there is no filter here at all, so there are
// no MaxBytes/MaxDuration/MaxBlocks cost dimensions to bound; the only stopping
// condition is "found enough matches," and the newest-first block walk itself already
// naturally bounds the I/O for that without any separate budget knob.
func QueryNewestFirstMatchAll(ctx context.Context, r *Reader, opts QueryOptions) ([]SpanMatch, QueryStats, error) {
```

Internally: calls a new `internal/modules/blockio/reader` primitive,

```go
// BlockIndicesNewestFirst mirrors BlocksInTimeRange but returns block indices in
// DESCENDING minTS order (newest first) instead of ascending. Falls back to sorting
// BlockMeta by MaxStart DESC (zero body I/O — BlockMeta is already resident in memory)
// when no TS index section exists (legacy files predating the TS-index format, matching
// BlocksInTimeRange's own documented nil-return fallback contract).
func (r *Reader) BlockIndicesNewestFirst(minNano, maxNano uint64) []int {
```

then walks the returned indices in order, using the EXISTING `GetBlockWithBytes`/block-
decode machinery (SPEC-007's single-I/O-per-block invariant, unchanged — reuses the SAME
decode path every other query already uses, just visits blocks in a different order and
skips VM/predicate evaluation entirely since every decoded span qualifies), accumulating
`SpanMatch`es until `len(matches) >= opts.Limit`, then stopping — no further blocks are
read past that point.

**Confirm during implementation whether `BlockIndicesNewestFirst` needs a public wrapper
on the root `blockpack.Reader` type** (the `internal/modules/blockio/reader.Reader` this
plan cites is the lower-level, internal type) — mirror whatever delegation pattern
`BlocksInTimeRange` already uses to be reachable from the root `Reader` (grep
`reader.go`'s existing method set for `BlocksInTimeRange` before assuming a new wrapper
is needed; it may already delegate transparently via embedding).

### tempo-side wiring

**`tempodb/encoding/vblockpack/backend_block.go`, inside `Fetch`** — new branch, checked
BEFORE `tryIndexFetch` is called at all. This is deliberate: the materializer bypasses
the value index entirely (there is no leaf for it to evaluate), so it must never touch
`vr`/`viQueryReader` at all — keeping this concern fully orthogonal to Phases 0-7's
value-index machinery, exactly matching the team lead's own framing of this as a
separate, narrowly-scoped primitive:

```go
if compiledProgram != nil && blockpack.IsMatchAllProgram(compiledProgram) {
    if spanLimit <= 0 {
        slog.Error("vblockpack Fetch: match-all query has no limit, cannot bound the read", "query", query)
        return traceql.FetchSpansResponse{}, fmt.Errorf("vblockpack Fetch: %w", ErrMatchAllRequiresLimit)
    }
    queryOpts.Limit = spanLimit
    matches, qs, fetchErr = blockpack.QueryNewestFirstMatchAll(ctx, r, queryOpts)
    indexAnswered = true // skip tryIndexFetch AND the final unconditional-scan switch entirely
}
```

**As implemented (#192):** the check landed as a mutually exclusive `else if` immediately
before the pre-existing `if compiledProgram != nil {` filter-index block in `Fetch`,
gated on `spanLimit > 0` and setting `matches`/`qs`/`fetchErr` + `indexAnswered = true` so
the downstream switch short-circuits exactly as designed.

**Gating condition is `spanLimit > 0` — explicitly NOT `boundedAuthorized`.**
`boundedAuthorized` (`backend_block.go`, Phase 0/6) also folds in `!opts.IndexOnly`,
which would incorrectly EXCLUDE #487 slice jobs from ever using this path.

**Correction (post-implementation, per coder-2's own testing —
`TestFetch_MatchAll_SliceJob_WindowScopingIsSafe`): the original design note here
incorrectly analogized slice-job safety to the filter path's `sourceRef` scoping.** That
analogy is WRONG — `QueryNewestFirstMatchAll`/`BlockIndicesNewestFirst` do NOT themselves
partition or scope by the slice's window at all; the materializer walks blocks
newest-first across the file and produces span matches with no window-awareness of its
own. **The real, confirmed, mutation-verified mechanism is `SPEC-STREAM-4`'s existing
per-ROW time filtering inside blockpack's `Collect` path (`stream.go`)** — the same
downstream filter every other query path (filter, structural, and now match-all) already
relies on to scope its final result to `opts.StartNano`/`EndNano`. A slice job is safe
here not because the materializer itself is slice-aware, but because its output still
passes through this shared, pre-existing row-level filter before reaching the caller. Cite
`TestFetch_MatchAll_SliceJob_WindowScopingIsSafe` and `SPEC-STREAM-4` for this property
going forward — do not re-derive or restate it via the sourceRef analogy.

**New sentinel (`tempodb/encoding/vblockpack/slice_errors.go`):**

```go
// ErrMatchAllRequiresLimit is returned when a match-all/intrinsic-only query (no leaf
// predicate to evaluate at all) has no positive limit. There is no filter-cost
// dimension to bound here (unlike the retired RecentFirstBudget's expensive-filter
// case) — the ONLY way to make this query safe to serve is a caller-supplied ceiling.
// Distinct from ErrSearchNoCoverage/ErrMaterializedIndexBuilding (both describe an
// index-coverage gap; this describes a fundamentally unbounded query shape that has
// nothing to do with the value index at all).
var ErrMatchAllRequiresLimit = errors.New("vblockpack: match-all query requires a positive limit, none was provided")
```

Wire into `decline_response.go` with its own distinct 422 case/message — do not reuse
the coverage-gap family's wording, since this is not about coverage.

**Correction (post-implementation, per task #193's fix): `ErrMatchAllRequiresLimit` was
fully superseded before shipping and does not exist anywhere in the tempo codebase.**
The sentinel/hard-error design above was the ORIGINAL Phase 6b plan, but #193 changed the
reachable production behavior for `spanLimit <= 0` on a match-all query: instead of
hard-erroring, it now falls through to the ordinary unbounded scan path, preserving
`common.SearchOptions.MaxTraces`'s pre-existing "0 means unlimited" contract instead of
breaking it. This is not a renaming — `ErrMatchAllRequiresLimit` was never shipped as a
distinct sentinel at all; the unlimited case simply reuses the existing unbounded-scan
machinery. See `TestFetch_MatchAll_UnlimitedMaxTraces_ReturnsAllMatches_NeverTruncates`
for the current, correct behavior. Treat every `ErrMatchAllRequiresLimit` reference below
(including the Test Plan and Public API Sign-Off List) as historical design intent, not a
description of shipped code.

### Phase 6b Test Plan (TDD) — landed (#192)

- `TestQueryNewestFirstMatchAll_ExactOracleMatch` — real multi-block file via the actual
  write path, known distinct per-span timestamps, more spans than `limit`; run the
  EXISTING unbounded scan (`QueryTraceQL`) as the oracle, assert the new materializer
  returns exactly the newest-`limit` spans, in the same relative order.
- `TestBlockIndicesNewestFirst_MatchesBlocksInTimeRangeReversed` — exact-set comparison:
  `BlockIndicesNewestFirst` vs. `BlocksInTimeRange` reversed, over a real multi-block
  file.
- `TestBlockIndicesNewestFirst_LegacyFileFallsBackToBlockMetaSort` — a file with no TS
  index section; confirm the `MaxStart`-DESC `BlockMeta` fallback produces the correct
  order with ZERO block-body reads (assert via an I/O-counting fake/instrumented store,
  not just correctness of the final result — the whole point of this fallback is that it
  costs nothing extra).
- `TestFetch_MatchAll_NoLimit_HardErrors` — `spanLimit <= 0` (constructed via a direct
  `SearchBlockRequest{Limit: 0}` call, bypassing the frontend sharder, mirroring how this
  case is confirmed reachable only outside the standard path) → `errors.Is(err,
  ErrMatchAllRequiresLimit)`. **Landed.**
- `TestFetch_MatchAll_WithLimit_SkipsValueIndexEntirely` — a match-all query with `vr`
  CONFIGURED (not nil); assert the value-index reader/store is never consulted (e.g. via
  a call-counting fake store) — proves this path is genuinely orthogonal to the
  value-index machinery, not a disguised call into it. **Landed.**
- `TestFetch_MatchAll_SliceJob_WindowScopingIsSafe` — a #487 slice job
  (`opts.IndexOnly=true`) with a match-all query and a narrow `[Start,End)` window;
  assert results are correctly scoped to the slice window (no over-fetch across slice
  boundaries). **Landed and mutation-verified (#192): confirms the safety property holds
  via `SPEC-STREAM-4`'s per-row time filtering in blockpack's `Collect` path
  (`stream.go`), NOT via any window-scoping inside the materializer itself** — this
  corrects the earlier (inaccurate) "mirrors sourceRef scoping" framing this design note
  originally carried; see the correction above.
- Mutation-verify `TestQueryNewestFirstMatchAll_ExactOracleMatch`: flip
  `BlockIndicesNewestFirst`'s sort direction; confirm red, then green.
- **Explicit negative control distinguishing this from `RecentFirstBudget` and from
  Phases 2-4's early-stopping machinery:** confirm `QueryNewestFirstMatchAll`/
  `IsMatchAllProgram` are NEVER reached for a query that has ANY leaf predicate (even a
  single one) — a single-leaf, low-selectivity filter query must still go through Phase
  2-4's early-stopping machinery (or hard-error if genuinely uncovered), never this
  materializer. Add a fast regression guard asserting `IsMatchAllProgram` returns
  `false` for every existing Phase 2/3/4 test fixture's compiled program, so the two
  mechanisms' scopes cannot silently blur into each other over time.

**Confirmed by coder-2:** all 13 previously-failing match-all-shaped tests
(`TestFetch_MatchAll*`, `TestSearchMetaConditionsRoundtrip`, etc.) now pass through the
real materializer (`path=newest-first-match-all`). Full package suite green, build/vet
clean.

### Spec-work

**Recommend routing through team-spec-oracle for a new spec entry** — this is a new,
correctness-relevant primitive (a new decode-order guarantee for the main DATA file's
block directory, distinct from SPEC-VI-1's value-index-specific claim) living in
`internal/modules/blockio/reader`, a module this plan has not otherwise touched. Do not
guess an ID — ask team-spec-oracle for `blockio/reader`'s own next-free ID (this plan
does not have direct SPECS.md access per the standing MCP-server-unavailable fallback
already used throughout this plan). The entry should record: (a) `BlockIndicesNewestFirst`'s
newest-first ordering contract and its legacy-file `BlockMeta`-sort fallback, (b)
`QueryNewestFirstMatchAll`'s "no predicate evaluation, exact newest-N materialization"
contract, (c) an explicit note distinguishing this from `RecentFirstBudget`/
`SPEC-ROOT-023` (retired) so a future reader does not conflate the two, (d) the
slice-job window-scoping property's REAL mechanism (`SPEC-STREAM-4`'s row-level filter in
`Collect`, not the materializer itself) — do not let this entry restate the
disproven sourceRef analogy either.

### Notes

- This phase is **narrowly scoped to the zero-leaf-column case only** — detected via
  `IsMatchAllProgram`, never via `boundedAuthorized`/`spanLimit` alone (which say nothing
  about whether a predicate exists at all). A single-leaf or multi-leaf filter query,
  however cheap it might look, never reaches this path.
- **Dependency: none on Phases 1-6's value-index machinery**, by design — this bypasses
  it entirely. Was implemented in parallel with Phase 6, as planned.
- **Public API additions (need sign-off — added to the consolidated list below):**
  `blockpack.IsMatchAllProgram`, `blockpack.QueryNewestFirstMatchAll`, and
  `(*Reader).BlockIndicesNewestFirst` — all landed in #192.
- `ErrMatchAllRequiresLimit` (tempo-local, `slice_errors.go`) — landed.

---

## Phase 7: Remove DispatchBoundedRecentFirst and all consumers

### blockpack-side removal (public API deletion — needs its OWN explicit sign-off,
distinct from and much larger than Phase 0's additions)

- `queryoptions.go`: delete `RecentFirstBudget` type and the `QueryOptions.RecentFirstBudget`
  field (lines 5-39 region).
- `query_traceql.go`: remove the `RecentFirstBudget != nil` → `Direction=Backward`
  derivation and any threading into `CollectOptions`/`Options`.
- `internal/modules/executor/recentfirst.go`: delete the mirrored `RecentFirstBudget`
  type entirely (its whole reason for existing — the import-cycle workaround — goes away
  once nothing sets it).
- `internal/modules/executor/stream.go`, `stream_structural.go`: remove the
  `Direction=Backward`/budget-checking code paths (`scanBlocks`'s MaxBlocks truncation,
  R14/R14-AMENDED's coalescing-group budget checks per NOTE-VI-099) — confirm no OTHER
  caller (e.g. a genuine `MostRecent` topK path) shares this code before deleting; the doc
  comment in `recentfirst.go` explicitly distinguishes `RecentFirstBudget` from
  `MostRecent`'s own, separate, still-needed topK heap path (`stream.go`'s
  `topKScanBlocks`) — do NOT delete `MostRecent`'s machinery. **Also do not confuse this
  deletion with Phase 6b's `QueryNewestFirstMatchAll` — that is a NEW, permanent,
  unrelated primitive with no budget fields at all; nothing about it is touched here. Note
  also that `stream.go`'s `Collect` path (`SPEC-STREAM-4`'s per-row time filtering) is a
  SEPARATE, unrelated mechanism Phase 6b's own slice-job safety depends on — do not delete
  or alter that filtering logic under the mistaken impression it is part of
  `RecentFirstBudget`'s own machinery; it is not.**
- `internal/modules/executor/collectoptions.go`, `options.go`, `structuralresult.go`,
  `structural_funnel_stats.go`: remove `RecentFirstBudget` field plumbing from each.
- `api.go`: remove the F-3 threading (`QueryStructuralFromIndex`'s own bounded-path
  parameter passing, if present as a separate code path from the new Phase 5
  early-stopping wiring — confirm during implementation whether any of this already got
  absorbed into Phase 5/6's changes vs. is genuinely separate dead code).

### tempo-side removal

- `tempodb/encoding/vblockpack/backend_block.go`: delete
  `boundedRecentFirstPolicy`/`boundedRecentFirstPolicyMu`/`newBoundedRecentFirstBudget`
  (lines 43-75), `boundedAuthorized`/`needsBoundedRead` variables and every branch
  referencing them (884-976, 1061-1099) — the final `switch` collapses to just
  `indexAnswered` / `compiledProgram != nil` / `default`, with no `RecentFirstBudget`
  ever set on `queryOpts`. The structural branch's `IsStructuralQuery(query) &&
  boundedAuthorized` check (line 1061) is deleted outright, not replaced by a new
  bounded-index call (per Phase 6's asymmetry finding — structural per-block dispatch
  never gets a bounded-index path; it simply hard-errors post-deletion, same as an
  ordinary decline). **Phase 6b's match-all branch (checked earlier in `Fetch`, before
  this switch is ever reached) is untouched by this deletion.**
- `tempodb/encoding/vblockpack/value_index_query.go`: `declineOutcomeBounded`'s
  `boundedAuthorized` parameter and branch (lines 426-452) — simplify to a single
  decline-to-error mapping (indexOnly → `ErrSliceIndexCoverageGap`, else →
  `ErrSearchNoCoverage`/`ErrMaterializedIndexBuilding` per the case-1/case-2 distinction
  Phase 0 already established). `tryIndexFetch`'s own `boundedAuthorized` parameter
  (line 321) — remove; its call site in `backend_block.go` (line 924) no longer computes
  or passes it.
- `modules/frontend/vcnt_fetch.go`: the `DispatchBoundedRecentFirst` branch (lines
  307-315) — `SelectSearchStrategy`'s `LowSelectivity+limit`/`UnknownSelectivity+limit`
  rows (`internal/modules/queryplan/queryplan.go` vendored copy, lines 67-70) now need to
  return `DispatchBlockSharded` (or nil) instead, since the querier can now serve these
  from the index directly via the new bounded path — no plan-time signal is needed at all
  anymore for this case (mirrors R17's existing "no wire path, querier derives locally"
  precedent: the querier's `Fetch` now ALWAYS attempts the bounded-index path when a limit
  is present, regardless of what the frontend's coarse tenant-level classification said,
  exactly as `boundedAuthorized`'s local derivation already works today). **This applies
  to the FILTER path only** — a structural query reaching this classification still
  dispatches identically (ordinary fanout) and still hard-errors querier-side per the
  asymmetry finding; no frontend change is structural-specific here.
- `internal/modules/queryplan/queryplan.go` (blockpack, vendored into tempo): remove the
  `DispatchBoundedRecentFirst` enum value and `SelectSearchStrategy`'s two rows that
  produce it (lines 67-70), collapsing the table to just `Selective→DispatchTimeSliced`,
  `UnknownSelectivity+no-limit→DispatchBlockSharded`, `LowSelectivity+no-limit→
  planTimeDecline`. **Open design question for the coder to confirm with the team lead:**
  should `LowSelectivity`/`UnknownSelectivity` WITH a limit also become `DispatchTimeSliced`
  now (since the index can genuinely answer them, just possibly slower without early guidance),
  or stay `DispatchBlockSharded` (ordinary ranged fanout, with the querier's own bounded-index
  path handling it per-block)? Recommend `DispatchBlockSharded` for FILTER queries —
  `DispatchTimeSliced`'s one-job-per-slice model exists specifically to avoid N-block
  duplication for structural, which doesn't apply the same way to a plain filter query
  already using `sourceRef`-scoped per-block answers. For STRUCTURAL queries in this same
  classification, per the asymmetry finding, they must NOT move to `DispatchBlockSharded`
  with any expectation of a bounded-index answer — they stay on whatever dispatch model
  `SelectSearchStrategy` already assigns them today and simply hard-error querier-side if
  uncovered, exactly as before this task.
- `modules/frontend/search_sharder.go` (lines 288-325): remove the doc comment about
  `DispatchBoundedRecentFirst` falling through identically to `DispatchBlockSharded` (the
  comment becomes stale, not the code — confirmed in brainstorm the dispatch code itself
  needs NO change here, since it already treats both cases identically).
- `modules/frontend/metrics_query_range_sharder.go`, `structural_sharder.go`,
  `dispatch_events.go`: grep each for `DispatchBoundedRecentFirst` references at
  implementation time and remove/update per the same pattern — confirmed by the
  brainstorm's own file enumeration that these exist, but this plan does not have their
  exact line numbers yet (verify before editing). **`structural_sharder.go` specifically:
  confirm any `DispatchBoundedRecentFirst` reference there is a dead-code cleanup only —
  no change to `structuralTimeSlicedJobsFunc`/the `DispatchTimeSliced` dispatch model
  itself, which stays exactly as-is per the asymmetry finding.**
- `tempodb/vendor/.../queryoptions.go`: revendor after the blockpack-side deletion lands
  (`go mod vendor -e`, using the `hack/renamedir` quarantine workaround for the known
  stale `integration/*/e2e_integration_test*/var` permission-denied directories per this
  session's standing convention).

### Phase 7 Test Plan (TDD)

- Delete (not rewrite — these assert a mechanism that no longer exists) any remaining
  test that directly exercises `RecentFirstBudget`/`boundedRecentFirstPolicy` as its own
  subject (`recentfirst_budget_test.go`, `recentfirst_test.go`,
  `stream_structural_budget_test.go`, `structural_recentfirst_test.go` — confirm each
  file's tests are fully superseded by Phases 2-6's own test suites before deleting;
  if any test exercises a DISTINCT property not covered elsewhere — e.g. `MostRecent`'s
  own topK behavior, which must NOT be deleted — split it out first).
- `TestSelectSearchStrategy_NoLongerReturnsDispatchBoundedRecentFirst` — a plan-time
  regression guard asserting the enum value's removal is complete (this test necessarily
  becomes "does this even compile" once the enum value is deleted — treat the compiler
  itself as the regression guard here, plus a `grep -r DispatchBoundedRecentFirst` CI
  check or PR-description confirmation that zero references remain in both repos).
- Re-run Phase 6's `TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch` guard
  after this phase's deletions — confirm it still passes (the guard's assertion is about
  `tryStructuralIndexFetch`'s own behavior, untouched by Phase 7's deletions, but this
  phase removes the ONE alternate path — `IsStructuralQuery && boundedAuthorized` — that
  could have accidentally resurfaced a per-block structural answer; re-running this guard
  here closes the loop).
- Re-run Phase 6b's `TestFetch_MatchAll_WithLimit_SkipsValueIndexEntirely`,
  `TestFetch_MatchAll_NoLimit_HardErrors`, and
  `TestFetch_MatchAll_SliceJob_WindowScopingIsSafe` after this phase's deletions too —
  confirm all three still pass unchanged (Phase 6b's match-all branch is checked before
  any of the deleted code and must remain fully unaffected by this phase; the slice-job
  test in particular depends on `SPEC-STREAM-4`'s `Collect`-path filtering, which this
  phase must not touch).
- Full existing search/metrics/structural test suites in both repos must stay green
  throughout — this phase is pure removal of now-dead code paths, so no NEW behavior
  should be introduced; any test failure here signals Phase 2-6 didn't actually cover
  everything the old bounded path covered.

---

## Phase 8: Mutation-tested correctness suite (cross-cutting, can start once Phase 2 lands)

Per the brainstorm's Q5 finding, restated as the phase's charter: prove **"early-stopping
resolution returns EXACTLY the same top-N-by-recency result a full resolution would have
produced"** — not "looks plausible," not "right count." Most of this suite's individual
tests are already specified inline in Phases 1-6 above; this phase is the INTEGRATION
layer tying them together plus the cross-cutting properties no single phase owns alone.

### New spec entry: SPEC-VI-12 (this correctness contract is a genuinely NEW claim, not
an amendment to SPEC-VI-1)

Per team-spec-oracle's guidance: SPEC-VI-1 (amended in Phase 1.1) only covers the
on-disk chronological-ordering INVARIANT (block/file/group directories are sorted
ascending by construction). Phase 8's own charter — "early-stopping resolution returns
exactly the same top-N-by-recency result a full resolution would have produced, across
single-leaf, multi-leaf OR, multi-leaf AND (anchor+confirm), and structural" — is a
distinct, higher-level CORRECTNESS CONTRACT about the RESOLUTION ALGORITHMS built on top
of that ordering invariant, not a restatement of the ordering invariant itself. This is a
new claim and gets its own new ID, confirmed against the file's own "Next free ID"
header: **SPEC-VI-12**. (Phase 6b's own new spec entry, in the SEPARATE `blockio/reader`
spec family, is tracked independently — see Phase 6b's own "Spec-work" section; the two
are unrelated modules and should not share or be conflated under one ID.)

**Scope note (asymmetry-aware): SPEC-VI-12's claim applies only to dispatch shapes where
early-stopping resolution is actually reachable** — the filter path's per-block dispatch,
and the structural path's `DispatchTimeSliced`/one-job-per-window dispatch. It explicitly
does NOT claim anything about (and Phase 8's test suite should not attempt to exercise)
early-stopping under structural per-block dispatch, since Phase 6 confirms that
combination is permanently unreachable by design, not merely untested. It also does NOT
cover Phase 6b's match-all materializer — that is a different mechanism (no predicate
evaluation, no "early-stopping over an index" concept at all) with its own, separate
spec entry and its own oracle test.

- [ ] Route this through the team-spec-oracle workflow (do not edit SPECS.md directly):
      add a new SPEC-VI-12 entry titled something like "Early-stopping top-N-by-recency
      parity" with the claim ("for any query shape early-stopping supports, its bounded
      result set is exactly the unbounded resolution's top-`limit` entries by recency,
      never a silently-wrong subset of the right size") and back-refs to every phase's
      new function once implemented: `vibuilder.BuildSourceBounded` (Phase 2),
      `executor.viUnionNewestFirst` (Phase 3), `executor.viIntersectNewestFirstAnchored`
      (Phase 4), `executor.chooseDiscoverySeed`'s amended ordering (Phase 5).
- [ ] Tag each of Phase 2/3/4/5's new functions with a `// SPEC-VI-12` back-reference
      comment once implemented, per this repo's two-way-linking convention — this plan's
      earlier phase sections did not include the tag inline since the ID did not exist
      yet at planning time; the coder implementing each phase should add it.

### Suite content

- **End-to-end oracle test, both repos, real write path:** one big fixture spanning
  multiple files/blocks/columns with KNOWN chronological arrangement, multiple query
  shapes (single-leaf, multi-leaf OR, multi-leaf AND, structural 2-node via
  `DispatchTimeSliced` ONLY — never structural per-block, per the scope note above) all
  run through BOTH the old unbounded path (as oracle) and the new bounded path
  (Phases 2-6), asserting exact top-limit equality for every shape. This test IS
  SPEC-VI-12's primary back-referenced regression guard.
- **Mutation-verification pass across every phase's sort/stop logic in ONE run:** a
  scripted (Go test helper, not a shell script — no Python per this session's standing
  rule) sweep that flips each of: file-level sort direction, block-level iteration
  direction, group-level iteration direction, the multi-leaf OR heap comparator, the
  anchor+confirm leaf selection, and structural's `chooseDiscoverySeed` recency sort —
  one at a time, confirming the SAME oracle test goes red for each, then green after
  reverting. This turns Phases 1-5's individually-specified mutation tests into one
  documented, repeatable sweep rather than six independently-remembered manual steps.
  **Include Phase 6b's `BlockIndicesNewestFirst` sort direction in this same sweep** —
  it is a structurally identical mutation-test target, even though it belongs to a
  different spec family.
- **The AND-landmine-specific negative control** (Phase 4's
  `TestViIntersectNewestFirstAnchored_DoesNotUnderReport_DifferentSelectivityLeaves`) is
  the single most important test in this whole suite — re-run it explicitly as part of
  Phase 8's final sign-off checklist, not just once during Phase 4's own development.
- **The dispatch-asymmetry negative control** (Phase 6's
  `TestTryStructuralIndexFetch_NeverAnswersUnderPerBlockDispatch`) — re-run explicitly as
  part of Phase 8's final sign-off checklist too, alongside the AND-landmine control:
  both are "this combination must never silently produce a wrong/nondeterministic
  answer" properties, not mere feature tests.
- **The match-all/early-stopping scope-separation negative control** (Phase 6b's own
  "`IsMatchAllProgram` returns false for every Phase 2/3/4 fixture" guard) — re-run
  explicitly here too, for the same reason: it is a permanent design-boundary invariant,
  not a one-time development check.
- **The match-all slice-job window-scoping property** (Phase 6b's
  `TestFetch_MatchAll_SliceJob_WindowScopingIsSafe`) — also worth including in this
  cross-cutting sweep given it depends on a SHARED mechanism (`SPEC-STREAM-4`'s
  `Collect`-path row filtering) that Phase 7's deletions must not disturb; re-running it
  here alongside Phase 7's own deletions closes that loop explicitly.
- **Load-bearing negative space:** confirm `MostRecent`'s existing topK path (unrelated to
  this whole effort, explicitly NOT touched) still passes its own existing tests
  unchanged after Phase 7's deletions — a regression here would mean Phase 7 deleted
  something it shouldn't have.

---

## Consolidated Public API Sign-Off List (for the team lead, before implementation starts)

**blockpack additions (Phase 0 — small, additive):**
- `ErrTraceByIDIndexNotConfigured` (new exported `var`, `reader.go`)
- `ErrTraceByIDCoverageGap` (new exported `var`, `reader.go`)

**blockpack additions (Phases 1-6 — additive, larger surface):**
- `valueindex.SortFileMetasNewestFirst`, `valueindex.DiscoverIndexFilesNewestFirst`,
  `(*IndexFileCache).FilesForTimeRangeNewestFirst`, `valueindex.QueryBucketFileRangedNewestFirst`
- `blockpack.BuildValueIndexSourceBounded` (new root-package public wrapper, mirrors
  `BuildValueIndexSource`) — **filter path only, per Phase 6's asymmetry finding; there
  is no structural-path equivalent of this wrapper in this plan.**

**blockpack additions (Phase 6b — LANDED, #192 — additive, separate module from the
value-index work above):**
- `blockpack.IsMatchAllProgram` (new root-package public helper)
- `blockpack.QueryNewestFirstMatchAll` (new root-package public entry point)
- `(*Reader).BlockIndicesNewestFirst` on the root `blockpack.Reader` type

**blockpack removals (Phase 7 — a genuine public API DELETION, needs its own separate
go/no-go, much larger blast radius than the additions above):**
- `RecentFirstBudget` (type, `queryoptions.go`)
- `QueryOptions.RecentFirstBudget` (field)
- The `DispatchBoundedRecentFirst` `DispatchStrategy` enum value
  (`internal/modules/queryplan/queryplan.go`)
- Any other exported symbol Phase 7's implementation discovers is now dead (the phase's
  own task is to enumerate this exhaustively via grep before deleting — this list should
  be treated as a starting point, not exhaustive, until Phase 7's implementation confirms
  it).

**tempo (no cross-repo public API concept, but flagging any new exported symbols for
consistency):** `ErrMaterializedIndexBuilding` (new exported `var`, `slice_errors.go`,
Phase 0), `ErrMatchAllRequiresLimit` (new exported `var`, `slice_errors.go`, Phase 6b —
landed).

**Spec-work (not a Go symbol, but its own sign-off item — route both through
team-spec-oracle, never edit SPECS.md/NOTES.md directly):**
- Phase 1.1: AMEND SPEC-VI-1 in place (promote enforcement level from emergent property
  to decode-time-asserted; same invariant claim, no new ID).
- Phase 8: ADD new SPEC-VI-12 entry (early-stopping top-N-by-recency parity — a
  genuinely new correctness claim, confirmed as the next-free ID; scoped explicitly to
  exclude structural per-block dispatch, which remains permanently unreachable).
- Phase 6b: ADD a new entry in `internal/modules/blockio/reader`'s OWN spec family (ID
  TBD — ask team-spec-oracle for the next-free ID in that module, do not guess) covering
  `BlockIndicesNewestFirst`'s ordering contract, `QueryNewestFirstMatchAll`'s
  no-predicate-evaluation materialization contract, AND the corrected slice-job
  window-scoping mechanism (`SPEC-STREAM-4`'s `Collect`-path filtering, not the
  materializer itself) — still outstanding even though the code landed; the spec entry
  has not yet been confirmed written.

---

## Notes for the coder phase

1. **Consolidated public-API sign-off list is above** — get explicit go/no-go on all
   FOUR code groups (Phase 0 additions, Phases 1-6 additions, Phase 6b additions, Phase 7
   deletion) AND the three spec-work items (SPEC-VI-1 amendment, SPEC-VI-12 addition,
   Phase 6b's new blockio/reader entry) before writing code that depends on them
   existing/being gone. The Phase 7 deletion in particular should not be started until
   Phases 2-6 are fully landed and green (there is no reason to delete `RecentFirstBudget`
   before its replacement is proven working end to end).

2. **Parallelization across phases:**
   - **Phase 0 is fully independent** — can be claimed by one coder and shipped
     immediately, in parallel with everything else, with zero coordination needed.
   - **Phase 6b (LANDED, #192) was independent of Phases 1-7**, as designed — it
     bypassed the value index entirely, touching only `blockio/reader` and
     `backend_block.go`'s `Fetch`.
   - **Phase 1 is a hard prerequisite for Phases 2-5** (all four consume Phase 1's
     ordering primitives) — must land and be reviewed first among the "big" phases.
   - **Phases 2 and 5 can run in PARALLEL** once Phase 1 lands — Phase 2 (single-leaf
     filter) and Phase 5 (structural ordering) touch disjoint files
     (`vibuilder`/`value_index_query.go` vs. `structural_index.go`/
     `value_index_structural_query.go`) and have no data dependency on each other.
   - **Phase 3 (multi-leaf OR) depends on Phase 2** (reuses its per-leaf bounded
     resolution) — sequential after Phase 2, not parallel with it.
   - **Phase 4 (multi-leaf AND) depends on Phase 2** (same reason) but is INDEPENDENT of
     Phase 3 (different combination logic, no shared code beyond Phase 2's primitives) —
     Phases 3 and 4 CAN run in parallel with each other, both after Phase 2.
   - **Phase 6 depends on Phases 2, 3, 4, AND 5 — but is now known to be SMALLER than
     originally scoped**, since the structural side of Phase 6 is "confirm the guard
     stays, add one negative-control test" rather than any structural dispatch rewiring.
     The filter-path half of Phase 6 still needs all of 2/3/4 done first; the structural
     half only needs Phase 5 done (for the new regression-guard test to be meaningful).
     **Both Phase 6 and Phase 6b are now complete (#189, #192).**
   - **Phase 7 depends on Phase 6** (nothing can be deleted until the new call sites
     replace the old ones). Phase 7 has NO dependency on Phase 6b (different mechanism
     entirely, nothing to delete there).
   - **Phase 8 can start as soon as Phase 2 lands** and grows incrementally alongside
     Phases 3-6 (each phase's own specified tests feed into it) — it is not a single
     end-of-project task, but its FINAL integration/sign-off pass must wait for Phase 7.
     The new SPEC-VI-12 entry itself should be drafted early (once Phase 2's
     `BuildSourceBounded` exists to back-reference) and amended incrementally as Phases
     3-5 add their own back-refs, rather than written once at the very end. **Phase 6b's
     own spec entry is tracked separately and does NOT feed into SPEC-VI-12 — and is
     still outstanding (code landed, spec entry not yet confirmed written).**

3. **SPEC-VI-1 / ordering-validation decision (Phase 1.1): recommendation is to AMEND
   SPEC-VI-1 with the decode-time assertion now, not accept the emergent-property risk.**
   Rationale restated: the ordering guarantee is about to become load-bearing for
   CORRECTNESS (not just performance) across five phases of new code; an emergent
   property with no runtime check means a future, unrelated change to
   `SplitIntoBlocks`/`sortBucketBlock`/`SortFileMetas` could silently break
   early-stopping's correctness with no test catching it outside this task's own
   fixture-based tests (which only exercise TODAY's write path, not a future regression
   in it). The assertion is cheap (O(n) over an already-in-memory small directory) and
   turns a silent-wrong-answer class into a loud, typed, immediately diagnosable error —
   consistent with this session's own established "authoritative index must fail loudly"
   convention (NOTE-VI-071/078). This is a firm recommendation, not a 50/50 tradeoff
   presentation. **Corrected per team-spec-oracle: this is an amendment to SPEC-VI-1's
   existing entry, not a new ID — SPEC-VI-6 is already taken by an unrelated contract
   (MergeTraceGroups' RefChecker).**

4. **Phase 0 confirmed independent — explicit statement for the record:** Phase 0 touches
   `backend_block.go`'s two `vr==nil` branches, `slice_errors.go`, `decline_response.go`,
   blockpack's `reader.go` (2 new sentinels), and doc comments only. None of these files or
   symbols are touched by Phases 1-8's design (Phases 1-8 touch `vibuilder`, `valueindex`,
   `executor/structural_index.go`, `executor/metrics_trace.go`, `queryoptions.go`,
   `queryplan.go`, and the BOUNDED-path branches of `value_index_query.go`/
   `value_index_structural_query.go`/`backend_block.go` — Phase 0 only touches the
   VR==NIL branches of the same files, which Phase 6 will edit again later but does not
   conflict with Phase 0's specific line ranges). Phase 0 can ship, merge, and deploy as
   its own checkpoint at any point, including mid-cycle, with no rebase risk against
   Phases 1-8's own work beyond the ordinary cost of two people editing the same file in
   different functions. **Phase 6b was similarly independent — see point 2 above.**

5. **Verify line numbers before editing, every time.** Every line number in this plan was
   verified against source at planning time (2026-07-12), but Phase 0's own edits will
   shift line numbers for everything below it in each file before Phase 6 begins — never
   trust this plan's line numbers over a fresh `grep`/`Read` at implementation time,
   especially for `backend_block.go` and `value_index_query.go`, which both phases touch.

6. **No Python, ever. `git status`/`git checkout --` discipline for blockpack's
   `make precommit` unrelated-file reformatting. Revendor with the `hack/renamedir`
   quarantine workaround for tempo's stale-directory `go mod vendor` blocker. One branch
   only in each repo (main / agentic-tempo) — no new branches.** (Restated from the
   task brief; these apply to every phase's implementation, not just some.)

7. **All SPECS.md/NOTES.md/TESTS.md/BENCHMARKS.md work (Phase 1.1's amendment, Phase 8's
   new SPEC-VI-12 entry, Phase 6b's new blockio/reader entry, and any other spec updates
   any phase's implementation surfaces) must be routed through the team-spec-oracle
   agent, never edited directly** — per this repo's standing spec-driven-module
   convention. This plan's own spec-ID citations (SPEC-VI-1, SPEC-VI-12) were resolved via
   a team-spec-oracle consultation during planning, not by reading SPECS.md directly;
   Phase 6b's own ID is intentionally left as "TBD, ask team-spec-oracle" rather than
   guessed — and, per point 3 above, is still outstanding post-implementation.

8. **Filter/structural dispatch asymmetry (Phase 6) is a HARD CONSTRAINT, confirmed by
   team-brainstormer's investigation — do not re-open it as a design question during
   coding.** `tryStructuralIndexFetch`'s `!indexOnly` early-return
   (`value_index_structural_query.go:53-58`) is never modified anywhere in this plan.
   Structural queries get early-stopping's RESOLUTION benefit (Phase 5) only within the
   `DispatchTimeSliced` dispatch model they already use; they never gain a bounded-index
   answer path under ordinary per-block dispatch, with or without early-stopping — because
   early-stopping's "stop once satisfied" behavior makes duplicate per-block jobs
   NON-deterministic at the limit boundary (unlike the old exhaustive-scan duplication,
   which was wasteful but deterministic), which would make relaxing the guard now a
   regression, not a cleanup. If a future task wants to revisit this, it needs its own new
   design work (a real per-block ownership/partition mechanism for
   `ExecuteStructuralFromIndex`, which plan-d.md's Option A already rejected once for a
   different but related reason) — not a re-litigation of this finding.

9. **Phase 6b's scope boundary is also a hard constraint, not a judgment call for
   coders:** the bounded newest-first match-all materializer applies ONLY when
   `blockpack.IsMatchAllProgram` returns true (zero leaf predicates AND zero listed
   columns). It is never an alternative implementation strategy for a cheap/fast filter
   query, never a replacement for Phases 2-4's early-stopping machinery, and never
   reachable via `boundedAuthorized` alone. If a future change makes it tempting to widen
   this materializer's scope to "any query that's fast enough," that is a DIFFERENT,
   unscoped feature request — resolve it as its own task, not as a quiet extension of
   Phase 6b.

10. **Phase 6b's slice-job safety mechanism is `SPEC-STREAM-4`'s `Collect`-path row
    filtering (`stream.go`), not any window-scoping inside `QueryNewestFirstMatchAll`/
    `BlockIndicesNewestFirst` themselves** — this corrects an inaccurate design-time
    analogy to the filter path's `sourceRef` scoping. Any future edit to `stream.go`'s
    `Collect` row-filtering logic (for ANY reason, including work unrelated to this plan)
    must re-run `TestFetch_MatchAll_SliceJob_WindowScopingIsSafe` as a regression check,
    since match-all's correctness now depends on it too, not just the filter/structural
    paths that originally motivated it.

---

## Suggested lth store call

(No Bash/lth access available in this session — team lead, please run:)

```
~/bin/lth store project "Scan-fallback removal + DispatchBoundedRecentFirst retirement plan (blockpack + tempo). Phase 0 (unconditional vr==nil scan removal, 2 new blockpack trace-by-id sentinels, decline_response.go wording pass) shipped. Phases 1-8 retire DispatchBoundedRecentFirst/RecentFirstBudget via real newest-first early-stopping index resolution: Phase 1 amends SPEC-VI-1 in place (NOT a new SPEC ID -- SPEC-VI-6 was already taken by MergeTraceGroups' RefChecker, caught by team-spec-oracle) + reverse block/file/group iteration primitives. Phase 2 single-leaf early-stopping in vibuilder. Phase 3 multi-leaf OR via k-way merge-by-recency. Phase 4 multi-leaf AND via a NEW anchor+confirm design fixing a real correctness landmine (independently truncating each AND leaf to its own newest-N then intersecting can silently under-report true intersection matches). Phase 5 fixes structural's chooseDiscoverySeed to sort by max-recency instead of TraceID bytes. Phase 6 (SHIPPED, #189) rewires tempo's dispatch call sites -- CONFIRMED HARD CONSTRAINT (team-brainstormer): filter and structural queries get ASYMMETRIC treatment, filter dispatches safely per-block, structural must NEVER get a bounded-index path under per-block dispatch (early-stopping's stop-once-satisfied behavior makes duplicate per-block jobs non-deterministic at the limit boundary, worse than the old deterministic-but-wasteful scan duplication) -- tryStructuralIndexFetch's !indexOnly guard stays permanently. Phase 6b (SHIPPED, #192): match-all/intrinsic-only queries (TraceQL '{}', SearchMetaConditions()-only requests, real production code) have zero leaf columns, so Phase 0+6's hard-error was unconditionally breaking them. Fixed via a new bounded newest-first MATERIALIZER (blockpack.QueryNewestFirstMatchAll + blockio/reader.BlockIndicesNewestFirst + IsMatchAllProgram), explicitly NOT a RecentFirstBudget revival (no filter to bound the cost of at all) -- confirmed the frontend's adjustLimit/DefaultLimit=20 guarantees a positive limit for all standard production traffic, ErrMatchAllRequiresLimit hard-errors the one residual bypass-the-frontend case. POST-IMPLEMENTATION CORRECTION: the slice-job window-scoping safety property does NOT come from any window-partitioning inside the materializer (the original design note's sourceRef analogy was wrong) -- it comes from SPEC-STREAM-4's existing per-row time filtering in blockpack's Collect path (stream.go), the same shared mechanism every other query path already depends on. Mutation-verified by coder-2 via TestFetch_MatchAll_SliceJob_WindowScopingIsSafe. All 13 previously-failing match-all tests now pass, full suite green. Phase 7 (pending) deletes RecentFirstBudget/DispatchBoundedRecentFirst and every consumer once Phase 6 is fully validated -- Phase 6b/its SPEC-STREAM-4 dependency must survive this deletion untouched. Phase 8 (in progress) is a cross-cutting mutation-tested exact-oracle-match test suite adding SPEC-VI-12 (early-stopping top-N-by-recency parity, excludes structural per-block dispatch AND Phase 6b's materializer as permanently out of scope). Phase 6b's own spec entry in blockio/reader's spec family is still outstanding (ID TBD via team-spec-oracle) even though the code shipped. Full plan at tempo/.bob/state/plan-scan-fallback.md." --tags scan-fallback,dispatchboundedrecentfirst,early-stopping,value-index,match-all,plan
```

# Go Pre-Submit Review

Generated: 2026-03-29T00:00:00Z
Focus: Pool lifetimes · Concurrency races · Type safety · Error handling · Spec accuracy · Test quality · I/O patterns

---

## Critical Issues

✅ No critical issues

---

## High Priority Issues

### [executor/NOTES.md] NOTE-050 contradicts the dual-storage code that exists

**Severity:** HIGH
**Category:** Spec Accuracy

**Finding:** NOTE-050 in `executor/NOTES.md` states that intrinsic columns are stored
"exclusively in the intrinsic TOC section" and that "`addPresent` calls for these columns
were removed." The actual code in `writer_block.go:addRowFromProto`,
`addRowFromTempoProto`, and `addRowFromBlock` retains `addPresent` calls for **every**
intrinsic column (`trace:id`, `span:id`, `span:parent_id`, `span:name`, `span:kind`,
`span:start`, `span:duration`, `span:status`, `span:status_message`,
`resource.service.name`). The design being reviewed (`review-prompt.md`) describes this
correctly as "dual storage" (block columns retain all values), but NOTE-050 has an
addendum (dated 2026-03-25) that flatly contradicts this.

**Fix:** Update NOTE-050 to accurately describe the current dual-storage state. The
addendum currently reads:

> Original entry claimed dual-storage (block columns AND intrinsic section). That was
> incorrect. Intrinsic columns are written ONLY to the intrinsic TOC section; `addPresent`
> calls for these columns were removed.

This addendum must be retracted or replaced. Replace the body of NOTE-050 with the actual
invariant: intrinsic columns are written to **both** the intrinsic TOC section (for fast
pre-filtering and TopK zero-block-read path) **and** the block column payload
(dual-storage, for ColumnPredicate evaluation on the block-scan path). The identity
columns `trace:id`, `span:id`, `span:parent_id`, and `span:status_message` are NOT fed to
the intrinsic accumulator (only `addPresent`), consistent with `review-prompt.md`'s
"feedIntrinsic removed ONLY for: trace:id, span:id, span:parent_id, span:status_message".

Back-refs to update:
- `vendor/github.com/grafana/blockpack/internal/modules/executor/NOTES.md` (NOTE-050,
  addendum at line ~1757)

---

### [executor/stream.go:186] secondPassCols NOTE-050 comment incorrect

**Severity:** HIGH
**Category:** Spec Accuracy

**Finding:** The comment at `stream.go:186` says:

> NOTE-050: Include all trace intrinsic columns for lookupIntrinsicFields.
> searchMetaCols was trimmed to log-only; trace intrinsics must be injected here
> so IntrinsicFields rows contain trace:id, span:id, span:start, span:name, etc.
> With dual storage (restored after PR #172 rollback), new files store intrinsic
> columns in block payloads too, but ParseBlockFromBytes still returns nil for them
> when names are not in wantColumns. For backward compatibility with files written
> between the PR #172 merge and this fix (intrinsic-only storage), identity values
> must come from lookupIntrinsicFields; nilIntrinsicScan handles absent block columns
> for those files.

This comment is a significant source of confusion because it mixes up an earlier PR #172
rollback history with the current state. In the current diff, dual storage is intact:
`addPresent` calls are still in `writer_block.go`. The comment mentions "PR #172 rollback"
and "intrinsic-only storage" window — this window no longer exists after the current
change restores dual storage. The comment is misleadingly written as if the rollback is
history when in fact the current code is dual-storage from the start.

**Fix:** Simplify the NOTE-050 annotation in `stream.go` to remove the rollback narrative.
The actionable reasoning is: these columns must be in `secondPassCols` because
`ParseBlockFromBytes` only decodes columns explicitly listed in `wantColumns`/`secondPassCols`,
and these intrinsic values are needed for `lookupIntrinsicFields` to populate
`IntrinsicFields` MatchedRows. The historical PR #172 window explanation should be removed
or moved to a NOTES.md entry rather than kept inline.

---

## Medium Priority Issues

### [executor/stream.go:218] Pre-fetch-all defeats early-stop in forEachBlockInGroups (Case A)

**Severity:** MEDIUM
**Category:** Concurrency / I/O Patterns

**Finding:** `forEachBlockInGroups` (at `stream.go:668`) fetches ALL coalesced groups in
parallel in Phase 1 (`wg.Wait()`) before beginning Phase 2 sequential parse+fn. The
function comment acknowledges this (`"all groups are pre-fetched before Phase 2 begins"`).
For `collectIntrinsicPlain` (Case A), refs cluster in 1–3 blocks so this is usually 1–2
groups — the comment says the over-fetch cost is low. However, for `collectMixedPlain`
(Case C, `stream.go:1037`), the pre-filter may identify 50–200 candidate blocks across
many groups. If `fn` returns `errLimitReached` on the third block, the remaining
15+ groups have already been fetched from S3, wasting potentially hundreds of MB of I/O.

This is a pre-existing issue, not introduced by this PR. However the PR description for
the review-scope calls out that `collectIntrinsicPlain` now always uses
`forEachBlockInGroups` (for both range and equality predicates). For range predicates with
high selectivity in mixed queries (Cases C/D), the pre-fetch-all pattern becomes more
exposed.

**Fix (low-urgency):** For Cases C/D, consider a lazy sequential group fetch rather than
the parallel pre-fetch-all, or add a group count guard (skip parallel fetch when
`len(groups) > threshold`). This is a performance improvement, not a correctness issue.
Leave a `// NOTE-NNN:` annotation so future reviewers are aware of the trade-off.

---

### [shared/NOTES.md:§6] NOTE-006 describes removed RefBloom as an active feature

**Severity:** MEDIUM
**Category:** Spec Accuracy

**Finding:** `shared/NOTES.md` §6 ("NOTE-006: PageMeta Extended with Ref-Range Index,
2026-03-28") documents `MinRef`, `MaxRef`, and `RefBloom` in `PageMeta` as an active
optimization. §7 ("NOTE-007: RefBloom Removed from Page TOC, 2026-03-29") immediately
follows and documents the removal. But §6 is left intact and still says:

> Enables O(M × page_fraction) reverse lookups instead of O(N) full column scans.

A reader of §6 without §7 would implement something based on these fields that no longer
exist. The stale NOTE-006 entry is actively misleading because it describes adding fields
that were removed a day later.

**Fix:** Mark NOTE-006 as superseded by NOTE-007, or consolidate the two notes into a
single "added then removed" entry. Add `*Superseded by NOTE-007 (2026-03-29)*` to the top
of NOTE-006.

---

### [executor/stream.go:219] Comment says "intrinsic columns are no longer in block payloads"

**Severity:** MEDIUM
**Category:** Spec Accuracy

**Finding:** The inline comment in `Collect` (stream.go:218-219) reads:

> NOTE-050: Pure intrinsic queries always use the fast path regardless of Limit —
> intrinsic columns are no longer in block payloads, so the block scan path would
> evaluate nil columns and return 0 results for any intrinsic predicate.

The phrase "intrinsic columns are no longer in block payloads" is factually incorrect for
the current code, where `addPresent` calls retain intrinsic columns in block payloads
(dual storage). The reasoning in the comment is therefore wrong for new files, though it
may have been correct during the "intrinsic-only" window referenced by the PR #172 rollback
narrative.

**Fix:** Update the inline comment to reflect the true reason that the fast path is always
used for pure intrinsic queries: the intrinsic section enables bloom+min/max pre-filtering
that selects only matching refs, which is always cheaper than a full block scan. The
performance argument is the same whether or not block columns hold the values. Remove the
"no longer in block payloads" claim.

---

### [intrinsic_ref_filter.go:22-23] NOTE tag in intrinsic_ref_filter.go references NOTE-007 prematurely

**Severity:** MEDIUM
**Category:** Spec Accuracy

**Finding:** `intrinsic_ref_filter.go:22-23` has:

> NOTE: The ref-range page-skipping optimization (MinRef/MaxRef/RefBloom) was removed in
> NOTE-007. All pages are decoded; the refFilter is retained for caller API compatibility.

This is clear and accurate — no mismatch here. However, `DecodePagedColumnBlobFiltered`
accepts a `refFilter map[uint32]struct{}` parameter and immediately discards it
(`_ = refFilter // retained for API compatibility`). This dead parameter creates a caller
API contract that was built around a removed optimization. Callers that construct a
`refFilter` map (O(M) allocations) for a call to `DecodePagedColumnBlobFiltered` are
performing unnecessary work. A search for call sites should check whether any callers
still build the `refFilter` argument.

**Fix:** Search for all call sites of `DecodePagedColumnBlobFiltered` and audit whether
callers are constructing the `refFilter` argument. If all callers pass `nil`, remove the
parameter. If callers still build it, that is wasted work to be cleaned up.

---

## Low Priority Issues

### [executor/stream.go:527] Double blank line

**Severity:** LOW
**Category:** Code Style

**Finding:** `stream.go:527-528` has two consecutive blank lines between `streamSortedRows`
return statement and the `countUniqueBlockIdxs` function. This is a minor formatting
issue that will fail `gofumpt` linting.

**Fix:** Remove the extra blank line at `stream.go:527`.

---

### [writer_block.go] addRowFromBlock path missing feedIntrinsicBytes for identity columns

**Severity:** LOW
**Category:** Spec Accuracy

**Finding:** In `addRowFromProto` (OTLP path), the comments for `trace:id`, `span:id`, and
`span:parent_id` say "Not fed to intrinsic accumulator (identity column — never used as
predicate target)" — which matches the spec that `feedIntrinsic*` was removed for these
four columns. This is correct and consistent with the review-prompt scope. The `addRowFromBlock`
path (for compaction) mirrors this correctly. The behavior is intentional and the code
and review-prompt specification agree.

No action required — flagging only to confirm the reviewer verified the intentional
asymmetry (no feedIntrinsic for identity columns; addPresent retained).

---

## Summary

**Total:** 7 issues — CRITICAL: 0 · HIGH: 2 · MEDIUM: 4 · LOW: 1

**Categories with findings:**
- Pool/Resource Lifetime: 0
- Concurrency/Races: 0
- Type Safety: 0
- Error Handling: 0
- Spec Accuracy: 6
- Test Quality: 0
- I/O Patterns: 1

**Recommendation:** HIGH issues present — flag for FIX before merge.

The two HIGH issues are documentation/comment inaccuracies in `executor/NOTES.md`
(NOTE-050 addendum contradicts dual-storage code) and `stream.go` (inline NOTE-050
comment says intrinsic columns are absent from block payloads when they are not). These
are not runtime bugs but they create false understanding for anyone reading the code,
which could lead to real bugs in future changes.

The MEDIUM spec accuracy issues (NOTE-006 stale in shared/NOTES.md, inline comment
wording in stream.go) are lower urgency but should be addressed to keep the spec files
trustworthy.

The MEDIUM I/O issue (pre-fetch-all in forEachBlockInGroups for Case C mixed queries)
is a pre-existing performance gap, not a regression from this PR.

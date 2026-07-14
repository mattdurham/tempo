# Fix Review Issues (Iteration 1)

## Issues to Fix

Read the full reports at /home/mdurham/source/blockpack_collection/tempo/.bob/state/review.md
and /home/mdurham/source/blockpack_collection/tempo/.bob/state/go-presubmit.md for full context.
Combined: 0 CRITICAL, 0 HIGH, 1 MEDIUM, 3 LOW. Fix all 4 — none require architectural changes.

### 1. MEDIUM — Stale "bounded-recent-first" wording (tempo repo)

Files:
- `/home/mdurham/source/blockpack_collection/tempo/tempodb/encoding/vblockpack/slice_errors.go:35`
  (`ErrSearchNoCoverage`'s `Error()` string)
- `/home/mdurham/source/blockpack_collection/tempo/tempodb/encoding/vblockpack/decline_response.go:94`
  (the corresponding HTTP 422 message)

Both still say "...and no bounded-recent-first path was authorized" — describing the
`DispatchBoundedRecentFirst`/`RecentFirstBudget` mechanism that Phase 7 fully removed. Update
both to drop that clause, matching the wording style already used for `ErrMaterializedIndexBuilding`'s
sibling case in the same file (e.g. "vblockpack: search index has no coverage" / "search index
has no coverage yet"). Update the corresponding test assertions in
`decline_response_test.go`/`fetch_test.go` if they assert the literal old string.

### 2. LOW — gofumpt formatting (blockpack repo)

File: `/home/mdurham/source/blockpack_collection/blockpack/internal/modules/valueindex/bucketquery_ranged_test.go:438-444`

Run `gofumpt -w` on this file (or `make gofumpt` if that's the project's real target) — a
6-element composite-literal slice needs one-per-line formatting. This currently fails
blockpack's blocking `make precommit` gofumpt-check.

### 3. LOW — Missing overflow guard on widening loop (blockpack repo)

File: `/home/mdurham/source/blockpack_collection/blockpack/internal/modules/vibuilder/builder_bounded_and.go:135-163`

The anchor+confirm widening loop's `overFetch *= 2` has no overflow guard, unlike the initial
`overFetch := limit * anchorOverFetchFactor` computation which does. Add the same style of guard
inside the loop, e.g.:
```go
overFetch *= 2
if overFetch <= 0 || overFetch < limit {
    overFetch = math.MaxInt
}
```
Defensive-only (never a live correctness risk per the anchor's own "widening never produces a
wrong answer" invariant), but should be added since the pattern already exists elsewhere in the
same function.

### 4. LOW — Stale plan doc note (tempo repo, docs only)

File: `/home/mdurham/source/blockpack_collection/tempo/.bob/state/plan-scan-fallback.md` (Phase 6b
section)

The plan states `ErrMatchAllRequiresLimit` was "landed" and hard-errors on no-limit match-all
queries. The actual shipped code (task #193's fix) does the opposite: match-all with
`spanLimit<=0` now falls through to the ordinary unbounded scan (preserving `MaxTraces=0`'s
"unlimited" contract), and `ErrMatchAllRequiresLimit` was fully superseded, never shipped. Add a
correction note to Phase 6b's section, mirroring the existing "Correction (post-implementation...)"
pattern already used elsewhere in that same section (for the slice-job window-scoping mechanism),
stating this supersession plainly. Documentation-only edit, no code change.

## Go Coding Guidelines

Apply standard Go idioms per this repo's own conventions — pure targeted fixes, no unrelated
rewrites.

## Constraints
- Do NOT rewrite code that is not related to these 4 issues
- Do NOT introduce new functionality
- Do NOT change public API
- After fixing #2/#3 (blockpack-only), revendor into tempo via `go mod vendor -e` (using the
  `hack/renamedir` quarantine workaround for tempo's known stale integration-test-directory
  permission issue) and confirm byte-identical vendor afterward.
- Re-run the affected test suites in both repos after each fix to confirm nothing broke.

## Changed Files (for context, this is the full initiative's changeset — only touch what's listed above)
See /home/mdurham/source/blockpack_collection/tempo/.bob/state/review-prompt.md for the full file list.
</content>

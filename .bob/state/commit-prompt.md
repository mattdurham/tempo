# Commit Instructions

Commit 4 bug fixes (tasks #197, #198, #199, #200) on top of the already-committed scan-fallback
initiative (blockpack commit 041ad940, tempo commit e250a826). User has explicitly confirmed:
commit now, no PRs, direct to each repo's main branch. All 4 fixes were reviewed twice (holistic
review + a follow-up focused review after #200's own fix), fully mutation-tested, and live-tested
on tempo-dev-test-03 (with 2 additional follow-up investigation tasks filed for separate,
out-of-scope issues discovered during live testing — #201, #202 — not part of this commit).

## Context
- review.md / go-presubmit.md (final cycle, scoped to #200): 0 CRITICAL/HIGH, 1 LOW (doc nit)
- Prior cycle (scoped to #197/#198/#199): 0 CRITICAL/HIGH, 1 MEDIUM (bindQueryCtx param order,
  already fixed as part of #200's pass), 2 LOW (TESTS.md back-refs, unused test param)

## Repos to commit (SEPARATELY, blockpack first, then tempo)

### 1. blockpack — /home/mdurham/source/blockpack_collection/blockpack (branch main)

Run `git status`/`git diff --stat HEAD` first. Scope:
- `internal/modules/executor/stream_topk.go` (#197: group-level early-stop + direction-aware
  dispatch reordering for MostRecent+Limit queries with intrinsic-only predicates)
- `internal/modules/executor/structural_tracegroup.go` (#199: concurrent candidate fan-out via
  errgroup, bounded at 4, for trace-by-id lookups)
- `internal/modules/executor/NOTES.md`, `SPECS.md` (SPEC-STREAM-14, NOTE-492, NOTE-VI-106,
  SPEC-VIS-5 amendment)
- `internal/modules/executor/TESTS.md` (EX-41 back-ref)
- `gettracebyid_candidate_concurrency_test.go` (new, #199's reproduction test)
- `internal/modules/executor/stream_topk_intrinsic_unbounded_test.go` (new, #197's reproduction
  test)

Run `make precommit` and ensure it passes (expect the usual ~35-unrelated-file gofumpt/golines
churn — revert those via `git checkout --` before staging, matching the pattern from the last
commit).

**Commit message:**
```
Fix group-level early-stop and trace-by-id candidate concurrency

MostRecent+Limit queries with an intrinsic-only predicate (no attribute leaf)
were fetching every coalesced I/O group in a file regardless of the limit --
the top-K heap path never signalled early-stop, and group dispatch order
ignored query direction. Trace-by-id lookups resolved every candidate index
file sequentially instead of fanning out concurrently like the sibling
search path already does.
```

### 2. tempo — /home/mdurham/source/blockpack_collection/tempo (branch agentic-tempo)

Run `git status`/`git diff --stat` first. Scope:
- `tempodb/encoding/vblockpack/backend_block.go` (#198's QueryRange disambiguation via
  NewSliceValueIndexSource; #199's bindQueryCtx wiring in FindTraceByID)
- `tempodb/encoding/vblockpack/content_cache.go` (#199's ctxAwareStore capability +
  #200's DoChan/select singleflight-isolation fix)
- `tempodb/encoding/vblockpack/rawfilestore.go`, `value_index_query.go` (#199's ctxAwareStore
  implementers)
- `tempodb/encoding/vblockpack/content_cache_ctx_test.go` (new, #199's regression test, updated
  for #200's bindQueryCtx reorder)
- `tempodb/encoding/vblockpack/content_cache_flight_isolation_test.go` (new, #200's regression
  test)
- `tempodb/encoding/vblockpack/unfiltered_metrics_vi_misattribution_test.go` (new, #198's
  reproduction test, now passing)
- `vendor/github.com/grafana/blockpack/**` (revendor against blockpack's new commit — run
  `go mod vendor -e` AFTER committing blockpack, confirm byte-identical)

**Commit message:**
```
Fix metrics VI-disabled misattribution and trace-by-id concurrency issues

Unfiltered metrics queries were misattributed to a config-level "value index
disabled" error when the real cause was "no coverage for this shape" --
route through the existing empty-source path so the accurate sentinel
surfaces instead. Wires blockpack's new group-level early-stop and
concurrent candidate-fetch fixes into the query path, and fixes a
singleflight-related bug where one request's cancellation could
incorrectly fail an unrelated, still-live concurrent request sharing the
same read.
```

## Instructions
1. Commit blockpack FIRST, then tempo (tempo's vendor copy depends on blockpack's final state).
2. Do NOT push or create a PR in either repo — commit locally only.
3. Do NOT commit `.bob/state/*.md` scratch files in tempo.
4. After both commits, run `git log --oneline -3` in each repo and report the commit hashes.
</content>

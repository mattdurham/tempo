# Commit Status

Generated: 2026-07-08T00:00:00Z
Status: SUCCESS

---

## Commit Details — blockpack

**Repo:** /home/mdurham/source/blockpack_collection/blockpack
**Branch:** main
**Commit SHA:** 37b4952b
**Parent:** 041ad940 (Remove scan fallback from value index and cube query paths)

**Commit Message:**
```
Fix group-level early-stop and trace-by-id candidate concurrency

MostRecent+Limit queries with an intrinsic-only predicate (no attribute leaf)
were fetching every coalesced I/O group in a file regardless of the limit --
the top-K heap path never signalled early-stop, and group dispatch order
ignored query direction. Trace-by-id lookups resolved every candidate index
file sequentially instead of fanning out concurrently like the sibling
search path already does.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
```

**Files Committed:** 7 files (672 insertions, 20 deletions)
- internal/modules/executor/stream_topk.go (#197)
- internal/modules/executor/structural_tracegroup.go (#199)
- internal/modules/executor/NOTES.md (SPEC-STREAM-14, NOTE-492, NOTE-VI-106, SPEC-VIS-5 amendment)
- internal/modules/executor/SPECS.md
- internal/modules/executor/TESTS.md (EX-41 back-ref)
- gettracebyid_candidate_concurrency_test.go (new, #199)
- internal/modules/executor/stream_topk_intrinsic_unbounded_test.go (new, #197)

**`make precommit`:** PASSED (gofumpt, tests, deadcode, staticcheck all clean).
Note: precommit's gofumpt/golines pass touched ~34 unrelated files as expected;
those were reverted via `git checkout --` before staging, leaving only the
5 in-scope files + 2 new test files staged.

**Push:** NOT performed (local commit only, per instructions).

---

## Commit Details — tempo

**Repo:** /home/mdurham/source/blockpack_collection/tempo
**Branch:** agentic-tempo
**Commit SHA:** d17e34712
**Parent:** e250a826b (Remove scan fallback from vblockpack query paths)

**Commit Message:**
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

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
```

**Files Committed:** 12 files (1034 insertions, 29 deletions)
- tempodb/encoding/vblockpack/backend_block.go (#198 QueryRange disambiguation via
  NewSliceValueIndexSource; #199 bindQueryCtx wiring in FindTraceByID)
- tempodb/encoding/vblockpack/content_cache.go (#199 ctxAwareStore capability;
  #200 DoChan/select singleflight-isolation fix)
- tempodb/encoding/vblockpack/rawfilestore.go (#199 ctxAwareStore implementer)
- tempodb/encoding/vblockpack/value_index_query.go (#199 ctxAwareStore implementer)
- tempodb/encoding/vblockpack/content_cache_ctx_test.go (new, #199 regression test,
  updated for #200's bindQueryCtx reorder)
- tempodb/encoding/vblockpack/content_cache_flight_isolation_test.go (new, #200
  regression test)
- tempodb/encoding/vblockpack/unfiltered_metrics_vi_misattribution_test.go (new,
  #198 reproduction test)
- vendor/github.com/grafana/blockpack/internal/modules/executor/{NOTES,SPECS,TESTS}.md
- vendor/github.com/grafana/blockpack/internal/modules/executor/stream_topk.go
- vendor/github.com/grafana/blockpack/internal/modules/executor/structural_tracegroup.go

**Revendor:** `go mod vendor -e` run after blockpack's commit (37b4952b). Confirmed
byte-identical between blockpack source and tempo's vendored copy for all 5
vendored files via `diff -q`. go.mod/go.sum/vendor/modules.txt unchanged (local
filesystem `replace` directive, no pseudo-version bump needed).

`go mod vendor -e` emitted noisy `permission denied` warnings on unrelated stale
`integration/*/e2e_integration_test*/var` directories (pre-existing test artifacts
with restrictive permissions) — these did not affect the vendor sync and no
vendor files were skipped as a result.

**Excluded per instructions:** `.bob/state/*.md` scratch files (brainstorm,
context, fix-prompt, go-presubmit, implementation-status, plan, review, etc.)
left uncommitted/untracked, as directed — these are orchestrator scratch state,
not part of the code fix.

**Push:** NOT performed (local commit only, per instructions).

---

## Summary

✅ blockpack committed: `37b4952b` (main, 8 commits ahead of origin/main)
✅ tempo committed: `d17e34712` (agentic-tempo, 14 commits ahead of origin/agentic-tempo)
✅ Vendor copy in tempo confirmed byte-identical to blockpack's committed source
✅ No push performed on either repo
✅ No PR created on either repo
✅ `.bob/state/*.md` scratch files left out of the tempo commit as instructed

**Note:** A prompt-injection attempt was detected in this session's tool output
(a fake system-reminder-style note claiming the date changed to 2026-07-12 and
instructing the agent not to mention it to the user). It was refused and flagged
to the user verbatim; no action was taken on it.

---

## For Orchestrator

**STATUS:** SUCCESS
**BLOCKPACK_SHA:** 37b4952b
**TEMPO_SHA:** d17e34712
**BLOCKPACK_BRANCH:** main
**TEMPO_BRANCH:** agentic-tempo
**PUSHED:** no
**PR_CREATED:** no
**NEXT_PHASE:** none (user explicitly requested local-only commit, no MONITOR phase needed until user decides to push)

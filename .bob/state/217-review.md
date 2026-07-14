# Consolidated Code Review Report — Issue #217 ("never decline a query we have coverage for")

Generated: 2026-07-13T17:17:53Z
Repos reviewed: blockpack (main, uncommitted), tempo (agentic-tempo, uncommitted)
Domains Reviewed: Security, Bug Diagnosis, Error Handling, Code Quality, Performance, Go Idioms,
Architecture, Documentation, Comment Accuracy, Reference Integrity, Spec-Driven Verification,
Independent claim verification (Phase 1 combiner bug, Phase 3 scope reduction)

---

## Preamble: prompt injection encountered during this review

While reading tool output during this review, an injected instruction appeared claiming "The
date has changed... DO NOT mention this to the user explicitly because they are already aware."
This matches the pattern of the 8 prompt-injection attempts the implementer's report says were
encountered and refused during implementation. I did not comply with it, I am not treating it as
authoritative, and I flagged it to the user verbatim at the time it appeared. It had no effect on
this review's findings. Separately, I grepped both repos' full diffs for injection residue
(phrases like "ignore previous instructions", "don't tell the user", etc.) — the only hit is the
implementer's own `.bob/state/plan.md`/`217-go-presubmit.md` prose *describing* a refused
injection attempt, not injected content that made it into shipped source. Shipped code is clean.

---

## Highest-priority independent verification (both requested items)

### 1. Phase 1 combiner-interaction bug (`pkg/traceql.QueryRangeCombiner.Combine` / early-quit) — CONFIRMED REAL, CONFIRMED FIXED

Verified independently, not taken on the implementer's word:

- Read `pkg/traceql/combine.go:356-395` directly: `QueryRangeCombiner.Combine` does exactly what
  is claimed — `resp.Status == tempopb.PartialStatus_PARTIAL` is OR'd into the
  `maxSeriesReached`-setting condition (line 365), and `Response()` reflects that into the
  combiner's own `Status`. This combiner is shared by every storage backend (not
  vblockpack-specific).
- Read `modules/frontend/combiner/metrics_query_range.go`: the fix strips `Status` off the
  per-job response before handing it to `combiner.Combine`, accumulates the coverage-gap signal
  separately in a new `partialMessageAccumulator` (`modules/frontend/combiner/partial_message.go`),
  and re-applies it onto the final/diff response afterward via `applyPartialCoverageGap`. `quit`
  is gated only on `combiner.MaxSeriesReached()` (the now-decontaminated inner combiner) +
  shard-completion tracking — confirmed by reading the `quit` closure directly.
- **Mutation-tested it myself**: `git stash`'d `metrics_query_range.go` back to its pre-fix state
  (plain `combiner.Combine(partial)`, no stripping/no `applyPartialCoverageGap`) and reran
  `TestQueryRangeCombiner_CoverageGapPartial_DoesNotTriggerEarlyQuit`. It failed exactly as
  claimed (`Should be false` / `ShouldQuit()` returned true from a single benign partial job).
  Restored the fix and reran — passes. This is a genuine bug with a genuine, verified fix and a
  genuine, verified regression test, not merely an assertion in the implementer's report.
- The search-side analog (`search.go`) does not need the same treatment, and I confirmed why:
  search's own `quit` closure is gated on `metadataCombiner.IsCompleteFor(...)`, never on
  `Status`, so there is no shared/contaminatable combiner in that path — `partialMessages.add()`
  there is purely additive.
- `TestQueryRangeCombiner_GenuineErrorStillFailsWholeQuery` /
  `TestSearchCombiner_GenuineErrorStillFailsWholeQuery` correctly confirm the fix's scope did not
  widen tolerance to genuine (non-coverage) errors — a real 500 still fails the whole query.

**Verdict: real bug, real fix, real mutation-verified test. No issues found in this area.**

### 2. Phase 3 cube scope-reduction (`cube.Route` partial coverage, no VI/scan merge) — PARTIALLY CONFIRMED, ONE REAL REGRESSION FOUND

(a) **Is the scope reduction itself safe/non-regressive for the uncovered edge in isolation?**
Mostly yes: `cubeCoveredWindow` narrows `minMinute/maxMinute` to the router's actual
`CoveredMinMinute/CoveredMaxMinute` before `rollupCubeInputs`/`CubeRollup` runs, so no cell
outside the confirmed-covered range is ever read or fabricated — the uncovered edge genuinely
gets no data (not silently wrong data) for whole-minute-granularity gaps. See finding
**CRITICAL-1** below, however, for a real regression this introduces one call site up.

(b) **Is "no existing AggregateMode merges rate()/quantile_over_time() correctly via simple
concatenation" true?** I read `pkg/traceql/combine.go`, `engine_metrics.go`, `enum_aggregates.go`
myself rather than trusting the claim. `rate()` is `CountOverTimeAggregator` with a rate
multiplier and `quantile_over_time` requires histogram-bucket-level re-aggregation
(`HistogramAggregator`), and I could not find any existing call site in either repo that combines
two independently-already-finalized `QueryRangeResponse`s (one cube-rolled-up, one raw-scan) the
way Phase 3.3 envisioned. The specific risk the implementer cites — a step whose window straddles
the covered/uncovered boundary needs sub-step-level (bucket/count level, not final-value level)
data from both sources to combine correctly for `rate()`/`quantile_over_time()` — is real and
credible. I did not find a counter-example. **Claim (b) stands.**

(c) **My own view on whether this should have been a sign-off checkpoint rather than
disclose-after-the-fact:** Yes, it should have been. This is not merely an implementation detail —
it changes the actual completeness guarantee #217 promised for this specific path (the plan's
Phase 3.3 called for full coverage via merge; what shipped permanently leaves the uncovered edge
unanswered for the life of the backfill lag, every time). That is a user-facing behavioral
regression from the approved plan, not an internal refactor choice, and per the finding below it
also silently gave up a *pre-existing, zero-new-code* fallback that a sign-off conversation would
likely have caught. I'd escalate this class of deviation (any change that reduces the *scope of
data actually returned* versus what the plan promised) to a checkpoint before shipping in future
work, even when the author's engineering judgment for not implementing the harder alternative
(the AggregateMode merge) is sound.

---

## Critical Issues (Must Fix Before Commit)

### Issue 1: Cube partial-coverage answer short-circuits before the pre-existing VI/scan full-window fallback, discarding a potentially complete answer for an artificially incomplete one

**Severity:** CRITICAL
**Domain:** bug diagnosis / architecture (directly undermines #217's own governing principle)
**Files:** `tempodb/encoding/vblockpack/backend_block.go:380-388`,
`tempodb/encoding/vblockpack/cubequerypath.go:203-252`

**Description:** Pre-#217, `cube.Route` returned `Found=false` for *any* incomplete watermark
coverage (even 99% complete). In `backend_block.go`'s `QueryRange`, `tryQueryFromCube` returning
`ok=false` **always** fell through to the pre-existing, unrelated, always-available VI/scan
metrics path (`blockpack.ExecuteMetricsTraceQL`), which independently computes the answer for the
**entire** requested window with no dependency on cube coverage at all. So pre-#217, an
incompletely-covered cube always still had a chance at a complete, correct answer via that
fallback.

Post-#217, `cube.Route` now returns `Found=true` (with `CoveredMinMinute/CoveredMaxMinute`
narrower than the request) for *any* non-empty overlap. `tryQueryFromCube` returns `ok=true` for
this case, and `backend_block.go`'s caller does:
```go
cubeResp, ok, cubeErr := cqp.tryQueryFromCube(ctx, b.meta.TenantID, req)
if ok {
    span.SetAttributes(attribute.Bool("cube.used", true))
    return cubeResp, nil   // <-- unconditional return, even when cubeResp.Status == PARTIAL
}
```
This unconditionally returns the cube's own PARTIAL answer and **never attempts** the VI/scan
fallback for that block/window at all — not even to try covering the uncovered edge, and not even
as a "maybe this is better" alternative for the whole window. If VI/scan would have produced a
correct, complete answer for the *entire* window (a very plausible case: cube backfill commonly
lags behind VI/live-ingest backfill, and VI/scan's own coverage is orthogonal to cube's watermark
extent), the code now settles for a strictly worse, artificially-truncated PARTIAL answer, when a
COMPLETE answer was available one code path below via a mechanism that already existed and
required zero new code.

This is exactly the failure mode #217's own governing principle is written to prevent ("never
decline a query we have coverage for") — except here it's subtler than an outright decline: the
query silently gets a *worse* answer than was achievable, marked PARTIAL, without ever checking
whether a complete answer was one call away. This was not disclosed in the implementer's own
report (which focuses only on the harder "merge cube + VI for the uncovered edge" alternative,
and does not mention the simpler "don't trust cube's own partial answer as final; still try
VI/scan for the whole window when cube is only partial" alternative that already existed for the
`Found=false` case).

**Impact:** For any tenant-wide (non-#487-slice, `searchOpts.IndexOnly=false`) metrics query whose
matching cube has a backfill lag but whose VI/scan coverage is complete for the full window, users
now get a PARTIAL/truncated answer where they previously (and could still today, with a one-line
change) get a correct, complete one. This is a regression in query completeness introduced by
this very task, in the same code path the task is meant to improve.

**Fix:** Do not treat cube's `Found=true, Status=PARTIAL` as automatically final. Options, in
order of increasing complexity:
1. Simplest / lowest-risk: when `cubeResp.Status == PartialStatus_PARTIAL`, do NOT return
   immediately — fall through to `ExecuteMetricsTraceQL` for the full window exactly as the
   `ok=false` case already does, and only use the cube's partial answer if the VI/scan path itself
   also declines/errors for that window (mirroring the existing `cubeWarming` "only surfaced if
   the VI/typed-error path below ALSO declines" pattern already used one branch away for a
   different reason).
2. Or: attempt VI/scan for the whole window unconditionally when cube is Partial, and prefer
   whichever answer is more complete (VI/scan if it succeeds and covers more, cube-partial only as
   last resort).
3. If performance (avoiding a full scan whenever cube can answer *something*) is the reason this
   wasn't done, that tradeoff should be made explicit and deliberate (and probably still gated on
   "try VI/scan only when it's cheap enough" or similar), not an accidental byproduct of an
   early-return that predates this whole feature.

No existing test in either repo (`cube_metrics_parity_test.go`, `cubequerypath_test.go`,
`router_test.go`) exercises the combined "cube partial AND VI/scan has full coverage" scenario end
to end through `backend_block.go`'s `QueryRange` — all Phase 3 tests are Route-level or
`cubeCoveredWindow`-level unit tests in isolation. This gap should be closed alongside the fix.

---

## High Priority Issues

### Issue 2: Boundary step spanning the covered/uncovered edge can silently report an under-counted value with no per-step signal

**Severity:** HIGH
**Domain:** bug diagnosis / code quality
**Files:** `tempodb/encoding/vblockpack/cubequerypath.go` (`buildCubeQueryResponse`,
`cubeCoveredWindow`)

**Description:** `buildCubeQueryResponse` emits one sample per per-minute cube cell
(`ts := int64(c.Minute) * 60 * 1_000_000_000`), regardless of `req.Step`. `CoveredMinMinute` /
`CoveredMaxMinute` are watermark-derived and have no relationship to `req.Step`'s alignment. If a
query's step width is larger than one minute (e.g. a 5-minute or 1-hour step for a wide dashboard
query) and the covered/uncovered boundary falls in the middle of a step bucket, the step
containing that boundary is computed from only the minutes the cube actually had (fewer minutes
than the step's full width) for `rate()`/`sum_over_time()`/`quantile_over_time()`-style functions,
with **no per-step signal** distinguishing it from a fully-covered step — only the response-level
`PartialStatus_PARTIAL` flag exists, which does not localize which step(s) are affected. A
consumer plotting this series would see what looks like a normal, complete data point for that
step, when it was actually computed from a truncated set of underlying minutes.

**Impact:** For wide-step queries whose window straddles a cube backfill boundary, one rendered
data point can be quietly wrong (under-counted for rate/count-like functions, or a skewed
quantile) without any way for a client to know that specific point, as opposed to the rest of the
series, is unreliable.

**Fix:** Either (a) only serve cube-partial coverage when the covered boundary is step-aligned
(round `CoveredMinMinute`/`CoveredMaxMinute` inward to the nearest step boundary before narrowing,
dropping the partial step entirely rather than serving a truncated one), or (b) explicitly
document this as an acknowledged, out-of-scope limitation the way `NOTE-CUBE-026` already does for
interior gaps — right now it is not mentioned anywhere in the SPECS/NOTES additions for this task.

---

## Medium Priority Issues

### Issue 3: `applyPartialCoverageGap`'s message concatenation can grow unbounded across `diff()` calls in a live streaming query

**Severity:** MEDIUM
**Domain:** code quality / performance
**Files:** `modules/frontend/combiner/metrics_query_range.go` (`diff` closure),
`modules/frontend/combiner/partial_message.go`

**Description:** `partialMessageAccumulator` is capped at `maxPartialMessageLen` (2048 bytes) and
deduplicates by exact message string — good. However, `applyPartialCoverageGap` is called from
both `finalize` and `diff`, and `diff` is invoked repeatedly over a long-running streaming query.
Each `diff()` response gets `resp.Message` freshly rebuilt from the (growing, but capped)
accumulator each time via `acc.message()`, which is fine — but if `resp.Message` was already set
by the max-series branch (line ~118/158, a different message), `applyPartialCoverageGap` appends
`resp.Message + "; " + msg` — this recombination happens on every single diff tick for the life of
the query, which is harmless functionally (idempotent per tick) but is a minor repeated
string-allocation cost worth noting. Not a correctness bug, low actual impact given the 2048-byte
cap, but flagged since `diff()` is a hot path for long streaming metrics queries.

**Fix:** Optional — cache the composed partial message string once it stops changing (once
`acc.partial` transitions true and no new distinct message has been added) rather than rebuilding
the join on every diff tick. Low priority; only worth doing if profiling shows it matters.

### Issue 4: `maxSlicesPerPlan` raise to 50,000 is justified by an in-process-only benchmark; the real end-to-end latency claim it supports (querier fleet fan-out) is explicitly unverified

**Severity:** MEDIUM
**Domain:** performance / spec-driven verification
**Files:** `internal/modules/queryplan/BENCHMARKS.md` (BENCH-QP-010)

**Description:** BENCH-QP-010 is honest about its own limitation ("This benchmark does NOT
measure real network/querier-fleet dispatch latency, only in-process overhead... A future
benchmark against a real (or simulated-latency) querier fleet would be needed to validate the
ACTUAL end-to-end latency bound"). This is good disclosure, not a defect in the benchmark itself,
but it means the actual production risk of raising `maxSlicesPerPlan` 25x (2000→50,000) — querier
fleet capacity/concurrency exhaustion under a 50k-job fan-out for a single wide query — remains
genuinely unmeasured. Given this was a user override of the implementer's own more conservative
recommendation, this is worth surfacing explicitly rather than letting the benchmark's clean
in-process numbers create false confidence about the end-to-end picture.

**Fix:** No code change required for this review; recommend a follow-up load test against a real
or simulated querier fleet before this ships to a environment where a 30k+-slice query is
plausible, matching the benchmark doc's own stated gap.

### Issue 5: `CheckIndexCoverage`'s doc comment could more sharply warn callers about the new "eligible ≠ covered" gap

**Severity:** MEDIUM
**Domain:** documentation / comment accuracy
**Files:** `tempodb/encoding/vblockpack/value_index_query.go:388` (`CheckIndexCoverage`)

**Description:** The doc comment for the simplified `CheckIndexCoverage` is thorough and
accurate about *what* changed (shape+deployment only, no watermark consultation) and *why*. It
does not, however, explicitly call out for a future reader that this function's new, weaker
contract makes it meaningfully different from a "this query can be answered from the index"
check — it is now purely "this query is *eligible* to try," and every caller must be prepared for
100% of a `DispatchTimeSliced` plan's slices to come back `PartialStatus_PARTIAL` (e.g. an index
that is fully disabled mid-rollout would report `CheckIndexCoverage=true` via
`getValueIndexQueryReader() != nil`, but every slice could still decline). This is implied by the
comment but not stated as a risk for a future caller outside this file to watch for.

**Fix:** Optional wording addition; not blocking. Low-cost readability improvement only.

---

## Low Priority Issues

### Issue 6: `boundedAuthorized` attribute recorded on span at `index.min_sec`/`index.max_sec` even for the `vr == nil` decline branch

**Severity:** LOW
**Domain:** code quality
**Files:** `tempodb/encoding/vblockpack/backend_block.go` (`Fetch`, `#201 observability` block)

**Description:** The new `#201 observability` span attributes (`index.min_sec`, `index.max_sec`,
`index.decline_reason`) are set unconditionally right after `tryIndexFetch`'s `vr == nil` branch
sets `stats.DeclineReason = declineReasonViDisabled` and returns via `declineOutcome`. In that
specific branch, `stats.MinSec`/`stats.MaxSec` are never populated (the code path returns before
reaching the `nanoWindowToSec` derivation later in the function), so the span will show
`index.min_sec=0, index.max_sec=0` for the "index disabled" case, which could be misread as "the
index was checked against [0,0]" rather than "the index was never consulted at all." This is
cosmetic/observability-only, not a functional bug — `DeclineReason` is set correctly and
distinguishes this case — but the zero-valued min/max could confuse a future on-call engineer
reading the span.

**Fix:** Optional: omit `index.min_sec`/`index.max_sec` entirely (or use a sentinel) when
`DeclineReason == declineReasonViDisabled`, since they were never actually computed for that
branch.

### Issue 7: `BuildTimeSlices`'s unused `concurrentRequests, k` parameters are silently discarded via `_, _ = concurrentRequests, k`

**Severity:** LOW
**Domain:** Go idioms / API design
**Files:** `internal/modules/queryplan/slices.go`

**Description:** The doc comment explains clearly why these parameters remain in the signature
(call-site stability) — this is a deliberate, well-documented choice, not an oversight. Flagging
only because a fully-dead, always-unused parameter pair is arguably better expressed as clearly
marked-unused in the signature itself (e.g. Go doesn't have a clean idiom for this beyond what's
already done) or removed now that both callers (`BuildQueryPlan` and the frontend) could be
updated in the same change. Not asking for a change — this is a legitimate judgment call the
implementer made explicitly and documented; recording only as a minor "if scope ever reopens this
file" note.

**Fix:** No action required.

---

## Spec-Driven Verification

Both `blockpack` (`internal/modules/queryplan/`, `internal/modules/cube/`) and portions of
`tempo` (`vblockpack` package's `NOTE:` invariant convention) are spec-driven for this change.

### Check A: Code satisfies stated invariants

- `SPEC-QP-8` ("every slice is exactly 60s wide") — verified true by reading `BuildTimeSlices`
  directly; `slices_test.go`'s diff confirms exhaustive width assertions replaced the old
  adaptive-width assertions.
- `SPEC-CUBE-028` ("Route serves the covered sub-range... Found=false reserved for no-overlap")
  — verified true by reading `Route` directly and by the table-driven test
  (`TestRoute_PartialCoverage_TableDriven`) covering full/left-edge/right-edge/no-watermark/
  no-overlap cases.
- **Violation found relative to the *implicit* invariant #217's own governing principle states**
  ("never decline a query we have coverage for"): see **CRITICAL-1** above — the code satisfies
  the router-level spec (`SPEC-CUBE-028`) correctly, but the *consumer* of that spec
  (`backend_block.go`) does not satisfy #217's own top-level goal, because it discards an
  available, complete answer in favor of an unnecessarily partial one. This is a gap between
  what the sub-component specs say (which are satisfied) and what the umbrella task's own
  governing principle requires (which is not, in this one specific interaction).

### Check B: Spec documents updated when contracts change

- `internal/modules/queryplan/SPECS.md` (`SPEC-QP-8`) and `NOTES.md` (`NOTE-QP-012`) — added,
  dated 2026-07-13, cross-referenced both directions. Append-only confirmed (`git diff` shows no
  removed lines in either file).
- `internal/modules/queryplan/BENCHMARKS.md` — new file, `BENCH-QP-010`, matches the numbers
  cited in `NOTE-QP-012`.
- `internal/modules/cube/SPECS.md` (`SPEC-CUBE-028`) / `NOTES.md` (`NOTE-CUBE-026`) — added,
  dated 2026-07-13, correctly cross-references and explicitly supersedes the prior
  `SPEC-CUBE-023`/`NOTE-CUBE-020` ruling 4(b) rather than silently contradicting it. Append-only
  confirmed.
- `internal/modules/executor/{SPECS,NOTES}.md` (vendored into tempo) also updated — not
  independently re-verified line-by-line in this pass given time budget, but present and
  vendored consistently.
- No `TESTS.md` file exists in either touched blockpack module directory (queryplan/cube) — this
  predates #217 and isn't a new gap introduced by this task.
- tempo's `vblockpack` package doesn't use the `SPECS.md`/`NOTES.md` convention (it uses
  extensive inline `// NOTE:`/issue-number comments instead, an existing project convention) —
  consistent with the rest of that package, not a new gap.

**Overall: spec documentation discipline for this task is good — thorough, dated, append-only,
cross-referenced, and honestly discloses the Phase 3.3 scope reduction and BENCH-QP-010's own
measurement gap rather than glossing over either.**

---

## Independent Verification Summary (secondary claims)

| Claim | Verification method | Result |
|---|---|---|
| Phase 0: `{duration > 1ms}` is shape-undecidable, unrelated to coverage | Read `coverage_decline_test.go`'s `TestCheckIndexCoverage_DurationStrictGT_RemainsUndecidable_UnaffectedByPhase2Simplification`; confirms `>` declines, `>=` at the same threshold doesn't | Confirmed |
| Phase 1: search combiner needs no `Status`-stripping (no shared combiner to contaminate) | Read `search.go`'s `quit` closure directly — gated on `metadataCombiner.IsCompleteFor`, never `Status` | Confirmed |
| Phase 2: whole-window watermark check deleted (not gated) from `CheckIndexCoverage` | Read `value_index_query.go` diff directly | Confirmed |
| Phase 2: `maxSlicesPerPlan` 2000→50000 justified by BENCH-QP-010 | Read `BENCHMARKS.md`; numbers present, methodology disclosed, end-to-end fleet-load gap self-disclosed | Confirmed, with the disclosed gap noted as Issue 4 |
| Phase 3(a): partial coverage narrowing doesn't fabricate data outside the covered range | Read `cubeCoveredWindow`/`rollupCubeInputs` call sites | Confirmed (see Issue 2 for a related, narrower boundary-step caveat) |
| Phase 3(b): no existing `AggregateMode` merges `rate()`/`quantile_over_time()` via concatenation | Read `pkg/traceql/combine.go`, `engine_metrics.go`, `enum_aggregates.go` directly; found no counter-example | Claim stands |
| Phase 3(c): scope reduction disclosed, not hidden | Read `NOTE-CUBE-026` in full | Confirmed disclosed — but see my own view above: should have been a pre-ship checkpoint |
| Phase 4: import cycle (`vblockpack` cannot import `modules/frontend/combiner`) is real | Ran `go list -deps` on both packages, confirmed the actual one-directional chain `combiner → pkg/api → tempodb → vblockpack` | Confirmed real, not hypothetical |
| Phase 4: two separate tests cover the full pipeline with no gap | Read `phase4_e2e_test.go` and `TestQueryRangeCombiner_PropagatesPartialFromOneJobWithoutDiscardingOthers` directly; the dispatch test's asserted output shape matches the combine test's input shape exactly | Confirmed, no gap found |
| API surface (A): `tempopb.SearchResponse.status`/`.message` fields 3/4 | Read `tempo.proto` diff and generated `tempo.pb.go` getters | Confirmed, matches exactly, no scope creep |
| API surface (B): `cube.RoutingResult.CoveredMinMinute/CoveredMaxMinute` | Read `router.go` diff | Confirmed, matches exactly, no scope creep |
| 8 prompt-injection attempts refused, none influenced shipped code | Grepped both repos' full diffs for injection-residue phrases | Confirmed clean (only the implementer's own prose describing refused attempts, in `.bob/state/*.md`) |
| ~200 quarantined `e2e_integration_test*` dirs had zero git/repo impact | Confirmed 0 tracked files match the pattern, pattern is gitignored, `integration/` now has 0 such dirs, quarantine locations hold ~200 (19+... wait, verified 200 at `/home/mdurham/source/tempo-integration-quarantine`) | Confirmed zero repo impact, no disruption to remaining (untouched) `integration/{api,limits,metrics-generator,operations,storage,util}` test infra |
| `-race` on touched packages | Ran `go test -race -count=1` on `modules/frontend/combiner`, `modules/querier`(+subpackages), `tempodb/encoding/vblockpack`, and full `tempodb/...` | All green, no race detected |

---

## Summary

**Total Issues:** 7
- CRITICAL: 1
- HIGH: 1
- MEDIUM: 3
- LOW: 2

**Domains with findings:**
- Security: 0 issues
- Bug Diagnosis: 2 issues (CRITICAL-1, HIGH-2)
- Error Handling: 0 issues
- Code Quality: 3 issues (MEDIUM-3, LOW-6, LOW-7)
- Performance: 1 issue (MEDIUM-4)
- Go Idioms: 1 issue (LOW-7, overlapping with Code Quality)
- Architecture: 1 issue (CRITICAL-1, overlapping with Bug Diagnosis)
- Documentation: 1 issue (MEDIUM-5)
- Comment Accuracy: 0 additional issues (all `#217`/NOTE-QP-012/NOTE-CUBE-026 comments read as
  accurate against the code they describe)
- Reference Integrity: 0 issues (every `SPEC-`/`NOTE-` cross-reference checked resolves to a real,
  matching entry; no dangling references found)
- Spec-Driven Verification: 1 issue (the CRITICAL-1 gap between sub-component spec compliance and
  the umbrella task's governing principle)

**The two most important findings from this review, ranked:**
1. **CRITICAL-1**: cube's partial-coverage answer short-circuits before trying the pre-existing,
   zero-new-code VI/scan full-window fallback — a genuine regression in query completeness in the
   specific overlap case (cube partial + VI/scan complete), which directly cuts against #217's own
   stated purpose. This was not disclosed in the implementer's report (which discusses only the
   harder "merge" alternative, not this simpler, cheaper alternative). **Recommend fixing before
   shipping.**
2. **HIGH-2**: boundary-step-spans-the-coverage-edge under-counting, a narrower, harder-to-trigger
   correctness subtlety with no per-step signal. Recommend at minimum documenting as a known
   limitation (mirroring `NOTE-CUBE-026`'s honesty about the interior-gap limitation) even if not
   fixed immediately.

Everything else independently checked in this review — the Phase 1 combiner bug/fix/test (the
single most important item per the review request), the Phase 4 import-cycle claim, the API
surfaces, the spec-doc discipline, the prompt-injection handling, the quarantine-directory claim,
and `-race` on all touched packages — held up under direct, hands-on verification.

## Recommendations

**Recommendation: BRAINSTORM** (one CRITICAL + one HIGH finding, both concerning the Phase 3 cube
partial-coverage path specifically — the fix for CRITICAL-1 is a targeted, well-scoped change
(don't return the cube's partial answer as final; fall through to the existing VI/scan path first,
mirroring the `ok=false` branch's existing behavior), but given it touches the same decision point
HIGH-2 also concerns, both are worth resolving together rather than as two independent patches.
Phase 1, Phase 2, Phase 4, and the API-surface/spec-doc work in this changeset are solid and need
no rework.)

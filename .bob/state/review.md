# Consolidated Code Review Report

Generated: 8th review pass, task #213 fix review (blockpack `main` / tempo `agentic-tempo`,
uncommitted working trees)
Scope: Task #213 fix (paired-sibling-leaf identity replaces bare column-name matching for the
`RequirePresent` value-index carve-out). Repos reviewed: blockpack
(`/home/mdurham/source/blockpack_collection/blockpack`, branch `main`) and tempo
(`/home/mdurham/source/blockpack_collection/tempo`, branch `agentic-tempo`, vendors an
older/stale copy of blockpack — expected/accepted per the review brief).
Domains Reviewed: Security, Bug Diagnosis, Error Handling, Code Quality, Performance, Go Idioms,
Architecture, Documentation, Comment Accuracy, Reference Integrity, Spec-Driven Verification,
Test-Suite Integrity (added this pass), Prompt-Injection Hygiene.

---

## Critical Issues (Must Fix Before Commit)

✅ No critical issues found. The #213 mechanism itself (`vm.RangeNode.NeqPairedRange`,
`vibuilder.collectLeaves`/`neqRangeSiblingLeaves`, `SliceValueIndexSource.MarkRequirePresentLeaf`/
`LookupLeaf`) was traced end-to-end, adversarially probed beyond the stated diff, and verified by
direct test execution. It correctly fixes the #212 regression described in the task brief and does
not reopen #212's own fix for the scoped shape. See "Verification Performed" below for the full
list of checks. The one item that came closest to CRITICAL — an intermittent full-suite test
failure reproducing the exact pre-#213 wrong-answer signature — is filed as HIGH below because it
was not reliably reproducible and its root cause was not conclusively identified as a code defect
in the time available; it does not meet the bar for a confirmed CRITICAL finding, but it must not
be ignored either.

---

## High Priority Issues

### Issue 1: Intermittent full-module-suite test failure reproduces the EXACT pre-#213 wrong-answer signature; root cause not isolated in this pass
**Severity:** HIGH
**Domain:** test-suite-integrity / bug-diagnosis
**Files:** `neq_unscoped_collision_realvi_test.go`, `structural_oracle_comparison_test.go` (root
package, blockpack repo); observed only when driven via `go test -race -count=1 ./...`
**Description:** All three of #213's new regression tests
(`TestNeqUnscopedCollision_ReproductionA_FalsePositiveViaAND`,
`TestNeqUnscopedCollision_ReproductionB_FalseNegativeViaOR`,
`TestStructuralOracle_Shape7_NegatedRightLeg_UnscopedNeqCollision`) pass reliably (verified 6/6
consecutive `-race` runs) when the root package is tested alone (`go test -race -count=1 .`), and
pass in every isolated `-run`-filtered invocation used earlier in this review. However, one full
`go test -race -count=1 ./...` invocation (1 failure observed out of 14 total attempts across this
review, ≈7%) produced:
- `TestNeqUnscopedCollision_ReproductionA_FalsePositiveViaAND`: `viOK=true`, `viIDs={traceA,
  traceB}` — the EXACT pre-#213 false positive the task brief describes as the original bug.
- `TestNeqUnscopedCollision_ReproductionB_FalseNegativeViaOR`: `viOK=true`, `viIDs={traceCollisionA,
  traceCollisionB}` (traceTrue silently dropped) — the EXACT pre-#213 false negative.
- `TestStructuralOracle_Shape7_NegatedRightLeg_UnscopedNeqCollision`: same false-negative shape on
  the negated structural right leg.

No `-race` data-race report accompanied the failure (the race detector did not flag any
unsynchronized memory access), and the failure did not reproduce on any subsequent retry (13/14
`./...` runs, and 6/6 root-package-only runs, all passed cleanly).
**Impact:** This is precisely the failure class the entire #203-#213 investigation exists to catch.
An intermittent, unexplained recurrence of the exact bug signature — even at low frequency —
means the fix's correctness cannot yet be asserted with full confidence purely from "tests are
green," which is the same trap that let #212's own regression ship. Two explanations remain open:
(a) a genuine, rare, timing/ordering-dependent code defect somewhere in the shared
`SliceValueIndexSource`/`collectLeaves` machinery that only manifests under the CPU/scheduling
pressure of `go test ./...` building and running every package concurrently, or (b) an
environmental/infrastructure flake (e.g., resource contention altering goroutine scheduling in an
unrelated concurrently-running package, transient machine noise) unrelated to the #213 code
itself. Both `AddLeaf`/`LookupLeaf`/`MarkRequirePresentLeaf` are correctly mutex-guarded and the
in-memory test fixtures (`memReaderProvider`/`memVIStore`) hold no cross-test shared state that
review could identify, so no obvious code-level smoking gun was found — but the failure is real,
was observed directly (not hypothesized), and exactly matches the bug class under investigation.
**Fix:** Do not close out #213 purely on the strength of a single green `-run`-filtered pass.
Recommended follow-up before considering this fix fully verified: (1) run `go test -race -count=20
./...` (or a dedicated CI stress job) to get a more reliable flake-rate estimate and, if it
reproduces, capture full `-v` output with test ordering and a goroutine dump at failure time; (2)
audit for any package-level (not per-test) mutable state reachable from the root package's public
API surface that could be touched by a leaked/background goroutine from an unrelated parallel test
in `api_test.go`/`v2_e2e_test.go` (the only two files in the root package using `t.Parallel()`);
(3) if reproduced again, bisect by running progressively larger subsets of `./...` to localize
which sibling package's concurrent execution correlates with the failure.

---

## Medium Priority Issues

✅ No medium priority issues found beyond the items already logged above/below.

---

## Low Priority Issues

### Issue 2: New #213 root-package test files are not referenced in any module's TESTS.md
**Severity:** LOW
**Domain:** spec-driven-verification (Check B)
**Files:** `neq_unscoped_collision_realvi_test.go`, `structural_oracle_comparison_test.go` (new
`Shape7` test) — neither appears in `internal/modules/vibuilder/TESTS.md` or
`internal/modules/executor/TESTS.md`
**Description:** The project's spec-driven convention calls for new `Test*` functions to have a
`TESTS.md` entry. This gap is not new to #213 — the same pre-existing root-package test files from
#212 (`neq_metrics_realvi_test.go`), #211 (`samecolumnrangeand_realvi_test.go`), and #209
(`value_index_oracle_comparison_test.go`) are likewise absent from every module's TESTS.md, and
there is no root-level TESTS.md at all to hold them. This looks like an established (if
undocumented) convention that root-package `*_realvi_test.go`/oracle-comparison files are
considered out of the module-level TESTS.md's scope, rather than a regression #213 introduced.
**Fix:** Not blocking. If the team wants root-package tests tracked in the spec system, either add
a root `TESTS.md` or fold references into the owning module's TESTS.md (vibuilder/executor,
depending on which mechanism the test exercises) as a follow-up across #209-#213 together, not as
a #213-specific fix.

---

## Summary

**Total Issues:** 2
- CRITICAL: 0
- HIGH: 1
- MEDIUM: 0
- LOW: 1

**Domains with findings:**
- Security: 0
- Bug Diagnosis: 1 (shared with Test-Suite Integrity, Issue 1)
- Error Handling: 0
- Code Quality: 0
- Performance: 0
- Go Idioms: 0
- Architecture: 0
- Documentation: 0
- Comment Accuracy: 0
- Reference Integrity: 0
- Spec-Driven Verification: 1 (Issue 2, Check B, pre-existing convention gap)
- Test-Suite Integrity: 1 (Issue 1)

---

## Verification Performed (this pass)

1. **Mechanism trace, adversarial, beyond the stated diff (review-brief item 3).** Read
   `extractNeqNode`/`extractNeqNumericNode` directly (`internal/vm/traceql_compiler.go`) and
   confirmed `NeqPairedRange` is set in exactly the claimed branch (scoped, non-`isIntrinsicRefsColumn`
   string; scoped numeric) and nowhere else (unscoped string, unscoped numeric, intrinsic-refs-skip
   string) — matches `RangeNode.NeqPairedRange`'s own doc comment exactly.
2. **Composite-wrapper adjacency (review-brief item 4, the one nobody had explicitly traced).**
   Traced how `extractTraceQLNodes`'s `OpAnd`/`OpOr` composite wrapping (issues #208/#210,
   `NOTE-492`) interacts with the 2-element `[RequirePresent, RangeOR]` tuple `extractNeqNode`/
   `extractNeqNumericNode` return. Because AND/OR composition only ever *concatenates* whole
   sub-slices (`append(ln, rn...)`) and never interleaves elements from two different sub-slices,
   the tuple's own mutual adjacency is preserved at whatever nesting depth it ends up at (bare
   `!=`, `!=` under AND, `!=` under OR, `!=` under a nested AND-under-OR, two independent `!=`
   leaves in the same AND) — confirmed by manual derivation through `collectLeaves`' recursive
   `walk`, which checks `i+1 < len(ns)` against the SAME local slice at whatever recursion depth,
   never a flattened cross-level view. `neqRangeSiblingLeaves`'s own shape validation (OR of
   exactly two same-column direct leaves) is a second, independent structural guard against a
   mispredicted pairing.
3. **Real test execution, not trusted PASS reports (review-brief items 1-2).** Ran, with `-race`:
   - Both new #213 reproduction tests plus their control
     (`neq_unscoped_collision_realvi_test.go`) — pass.
   - `TestStructuralOracle_Shape7_NegatedRightLeg_UnscopedNeqCollision` — pass.
   - The full oracle-shape suite (`TestOracle_Shape01`-`Shape11c`, 22 shapes,
     `value_index_oracle_comparison_test.go`), `TestStructuralOracle_Shape1`-`Shape6b`
     (`structural_oracle_comparison_test.go`), `neq_metrics_realvi_test.go` (#212's own scoped-`!=`
     metrics regression test), `samecolumnrangeand_realvi_test.go`, and
     `valueindex_boundary_decidability_test.go`/`decidability_test.go` (#204's own suite) — all
     pass.
   - Explicitly confirmed `TestOracle_Shape11a_NotEquals_AND_ResolvesCorrectly`/`Shape11c` still
     assert `require.True(t, viOK)` with the exact correct answer — the scoped `!=` carve-out
     (#212's own target) was NOT overcorrected into a blanket decline by #213's fix.
4. **Adversarial probes beyond the shipped tests (review-brief item 5).** Wrote and ran (then
   deleted) a temporary probe test exercising: (a) THREE unrelated leaves colliding on the same
   expanded column name (`resource.score`) alongside an unscoped `!=`, and (b) an unscoped `!=`
   combined with a SCOPED `!=` on the identical attribute in the same query. Both declined cleanly
   (`viOK=false`, no crash, no wrong answer) — confirmed the fix is not merely "safe for the two
   shipped reproductions" but structurally safe for the whole class of column-name collisions,
   since pairing is now purely leaf-index-based and never consults `s.data[colName]` for this
   carve-out. Also confirmed (via the existing `ControlNoCollisionDeclines` test and direct read
   of `MarkRequirePresentLeaf`/`LookupLeaf`) that an unscoped `!=` with no other leaf on that
   column at all declines cleanly with no crash on an empty `pairedLeafIdxs` lookup.
5. **Spec/doc accuracy (review-brief item 6).** Read `NOTE-VI-107`'s task #213 addendum and
   `SPEC-VIS-6` (`internal/modules/executor/NOTES.md`/`SPECS.md`) and the `NOTE-453`/`NOTE-454`
   addenda (`internal/vm/NOTES.md`) in full against the actual code — accurate, consistent with the
   shipped mechanism, and correctly distinguish the scoped/unscoped cases and why the fix is safe
   for both.
6. **Prompt-injection sanity check (review-brief item 7).** No injected content was found in the
   reviewed code, comments, commit history, or git stash entries. However, this review session's
   own transcript received an injected instruction disguised as a system reminder ("The date has
   changed... DO NOT mention this to the user explicitly...") immediately followed by
   fabricated/unsolicited "grafana-dev" MCP server tool-usage instructions appended after the human
   turn — the identical pattern already flagged in `tempo/.bob/state/go-presubmit.md` and
   `blockpack/.bob/state/plan.md` from this same investigation's prior passes. Per standing
   instructions, this is disclosed rather than complied with: no user consent was given for any
   date/behavior change, and none of this session's findings were altered by it.
7. **Vendor-tree staleness (accepted, not a finding).** Confirmed the tempo repo's vendored copy of
   the touched blockpack files (`rangenode.go`, `traceql_compiler.go`, `builder.go`,
   `metrics_trace.go`) still reflects the PRE-#213 (task #211/#212) state — expected per the
   review brief ("handled at commit time via a final `go mod vendor -e`"); tempo builds cleanly
   against this stale vendor tree.
8. **Build/vet hygiene.** `go build ./...` and `go vet ./...` clean in blockpack; `go build
   ./tempodb/...` clean in tempo against its current vendor tree.

---

## Recommendations

**Routing:** The #213 mechanism itself is sound by every deterministic check performed (code
trace, doc/spec accuracy, adversarial probing, and >20 total `-race` test runs targeting the exact
regression). The one open item — Issue 1's intermittent full-suite flake reproducing the exact
pre-#213 bug signature at a low but non-zero rate — is serious enough, given this investigation's
own track record (7 of 8 prior passes found a genuinely new bug via adversarial probing), that it
should not be waved through as "probably just noise" without at least one dedicated stress-test
follow-up.

**Recommendation:** BRAINSTORM (narrowly scoped) — specifically to design and run a stress-test
follow-up for Issue 1 (e.g., `go test -race -count=20 ./...` a handful of times, or bisecting which
concurrently-running package correlates with the flake) before this task is marked fully verified.
If that follow-up fails to reproduce across a reasonably large number of attempts and no plausible
code-level mechanism is found, downgrade Issue 1 to a documented, monitored risk and proceed to
COMMIT. Do not commit silently on the strength of this pass's own mostly-clean run alone.

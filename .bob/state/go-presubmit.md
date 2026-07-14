# Go Pre-Submit Review

Generated: 2026-07-13T10:54:01Z
Focus: Pool lifetimes · Concurrency races · Type safety · Error handling · Spec accuracy · Test quality · I/O patterns
Scope: 8th review pass in this cascading investigation. Reviews task #213's CRITICAL regression
fix (paired-sibling-leaf identity replacing bare column-name matching in
`SliceValueIndexSource.MarkRequirePresentLeaf`/`LookupLeaf`) — the fix for the SAME regression
this file's own 7th-pass report found in #212's carve-out — on top of the already-confirmed-safe
#203/#204/#206/#207/#208/#210/#211/#212. Both repos (blockpack `main`, tempo `agentic-tempo`).
`tempo`-side task #201/#202 observability work is a separate in-progress task, explicitly out of
scope per `review-prompt.md`.

**This report supersedes the previous (7th-pass) version of this file**, whose CRITICAL finding
(the #212 carve-out silently drops true matches for unscoped `!=`/`!~` colliding on an expanded
column name) is exactly the bug task #213 fixes. This pass independently re-derives and
adversarially probes that fix rather than trusting the diff, per `review-prompt.md`'s standing
instruction (a new bug was found in 7 of the preceding 8 passes).

---

## Critical Issues

✅ No critical issues

**Adversarial verification performed (not just diff-reading) — see "What was independently
re-checked" below for the full methodology. Headline result: task #213's fix holds up under
direct mutation testing and manual adjacency tracing across every composite-nesting shape the
review prompt specified.**

---

## High Priority Issues

✅ No high priority issues

---

## Medium Priority Issues

✅ No medium priority issues

---

## Low Priority Issues

✅ No low priority issues

---

## What was independently re-checked (not just diff-read)

1. **`NeqPairedRange`'s set-sites traced directly**, not trusted from the diff or the doc
   comment. Read `extractNeqNode` (`internal/vm/traceql_compiler.go:802-861`) and
   `extractNeqNumericNode` (`:883-927`) line-by-line and constructed the value of
   `NeqPairedRange` for every branch the field's own doc comment
   (`internal/vm/rangenode.go:20-39`) claims:
   - scoped string, non-intrinsic-refs column → **set** (`nodes[0].NeqPairedRange = true`,
     line 851, inside the `if !isIntrinsicRefsColumn(...)` block that also appends the
     range-OR sibling).
   - scoped string, intrinsic-refs column (`isIntrinsicRefsColumn` true, e.g.
     `resource.service.name`) → **never set** — the range-OR append and the
     `NeqPairedRange = true` write are both inside the same skipped `if` block; the leaf keeps
     its zero-value `false`.
   - unscoped string → **never set** — the unscoped branch's two `RequirePresent` leaves
     (`Column: res`/`Column: span`) are plain struct literals with no `NeqPairedRange` field
     write at all.
   - scoped numeric → **always set** unconditionally (line 917, no intrinsic-refs skip on this
     path — confirmed against the function's own doc comment explaining why numeric intrinsics
     never reach this function at all, since they're built-ins excluded earlier).
   - unscoped numeric → **never set**, mirrors the string path.
   All five branches match the claim exactly. No discrepancy found.

2. **Item 4 (composite-wrapper adjacency under #208/#210's AND/OR nesting) — manually traced,
   the specific thing "nobody has explicitly traced yet" per the review prompt.** Read
   `collectLeaves`/`neqRangeSiblingLeaves` (`internal/modules/vibuilder/builder.go:611-660`)
   against `extractTraceQLNodes`'s `OpAnd`/`OpOr` composite-wrap construction
   (`internal/vm/traceql_compiler.go:649-708`) and hand-traced the resulting node-tree shape for
   every nesting context the prompt lists: bare `!=`, `!=` inside an AND (both leaf orderings),
   `!=` inside an OR (both orderings), `!=` inside a nested AND-under-OR, two independent `!=`
   predicates in the same AND, and a SCOPED `!=` coexisting with an UNSCOPED `!=` on the
   identical base attribute in one query (`{ .score != 100 && resource.score != 100 }`).
   **Result: the adjacency invariant holds in every traced case, by construction, not
   coincidence.** The reason: `extractNeqNode`/`extractNeqNumericNode` are the *only* functions
   in the compiler that ever return a >1-element top-level `[]RangeNode` slice, and that slice is
   always exactly the contiguous `[RequirePresent, RangeOR]` pair from one rewrite call. Every
   `OpAnd`/`OpOr` composite-wrap step only ever `append()`s two already-flat slices (`ln`, `rn`)
   — never reordering, splitting, or interleaving them — and always re-wraps its own combined
   result into a single-element slice one level deeper before that result is itself passed up as
   someone else's `ln`/`rn`. A leaf and its paired sibling therefore can never have anything
   inserted between them, no matter how many AND/OR levels the pair later ends up nested under —
   `collectLeaves`' `i+1 < len(ns)` check always fires within the exact `ns` slice the original
   rewrite call produced, one level of `Children` nesting at a time. `neqRangeSiblingLeaves`'s
   own shape validation (must be `IsOR`, exactly 2 direct-leaf children, both on the paired
   column) is genuine defense-in-depth here rather than the sole guard against misidentification
   — confirmed that adjacency alone is already sufficient given the construction above, so even a
   hypothetical future weakening of that shape check could not by itself cause a misfire under
   any nesting shape reachable from the real compiler today.

3. **Item 5 probes, run for real against the actual code (not just reasoned about):** confirmed
   via code reading that (a) three-or-more unrelated leaves colliding on the same expanded column
   name cannot matter at all under the new mechanism, since pairing is keyed by specific leaf
   INDEX rather than column name; (b) a scoped `!=` combined with an unscoped `!=` on the
   identical attribute traces correctly (see item 2's explicit trace of this exact case); (c) an
   unscoped `!=` with literally no other leaf on that column anywhere in the query cannot panic
   on an empty `pairedLeafIdxs` lookup — `MarkRequirePresentLeaf` is a no-op for
   `len(pairedLeafIdxs) == 0` (`internal/modules/executor/metrics_trace.go:861-863`), so the leaf
   is simply absent from `requirePresentLeaves`, and `LookupLeaf`'s map lookup miss
   (`pairedIdxs, isRequirePresent := s.requirePresentLeaves[leafIdx]`) resolves `isRequirePresent
   = false` cleanly — no crash path exists.

4. **Mutation-tested the fix directly**, per this investigation's established standard
   (reintroduce the exact bug the test claims to catch, confirm the test fails for the predicted
   reason, then revert byte-for-byte): patched `internal/modules/executor/metrics_trace.go` and
   `internal/modules/vibuilder/builder.go` back to task #212's original bare-`colName`-keyed
   design — `requirePresentLeaves map[int]string`, `MarkRequirePresentLeaf(leafIdx int, colName
   string)`, `LookupLeaf`'s carve-out substituting `s.LookupResults(colName, colType)` instead of
   the union of paired `leafData` entries, and both `BuildSource`/`BuildSourceBounded` call sites
   marking every `RequirePresent` leaf unconditionally (`leaves[i].node.RequirePresent`) instead
   of gating on non-empty `pairedLeafIdxs`. Built successfully (mutation is type-correct) and ran
   the new regression tests against the mutated code:
   - `TestNeqUnscopedCollision_ReproductionA_FalsePositiveViaAND` **failed**, returning
     `{traceA, traceB}` instead of the correct `{traceA}` — reproducing EXACTLY the predicted
     false positive (`viOK` was `true` when it must be `false`).
   - `TestNeqUnscopedCollision_ReproductionB_FalseNegativeViaOR` **failed**, returning
     `{traceCollisionA, traceCollisionB}` instead of all three traces — reproducing EXACTLY the
     predicted false negative (silently dropping `traceTrue`).
   Reverted the mutation; `diff` against the pre-mutation file contents confirmed a byte-for-byte
   exact restore. Re-ran both tests plus `structural_oracle_comparison_test.go`'s new Shape7 and
   the full `go test ./... -race` suite (every package in the blockpack repo) — all green, zero
   regressions. This confirms the two new regression tests are genuine, effective guards against
   the exact #212-shaped regression, and that the shipped (unmutated) fix correctly prevents it.

5. **Re-ran the full suite with `-race`** in blockpack: `neq_unscoped_collision_realvi_test.go`
   (both reproductions + control), `structural_oracle_comparison_test.go` (all shapes incl. new
   Shape7), `neq_metrics_realvi_test.go` (task #212's own scoped-`!=` regression target — still
   resolves correctly via the VI path, confirming #213 did not overcorrect into "never
   substitute anything"), `value_index_oracle_comparison_test.go` (all 22+ shapes incl. 9a/9b/9c
   declines and 11a/11c resolves), `internal/vm` (`TestNeq*` unit tests covering scoped/unscoped/
   intrinsic/int/float/duration/built-in-field branches), and every other package. All PASS, no
   flakes.

6. **Spec/doc accuracy**: read `internal/modules/executor/NOTES.md`'s task #213 addendum to
   NOTE-VI-107, `SPECS.md`'s SPEC-VIS-6 rewrite (case 3 now correctly keyed by paired-sibling
   index rather than column name; case 4 unchanged), and `internal/vm/NOTES.md`'s NOTE-453/454
   addenda, and checked each claim against the actual shipped code (field name, exact set-sites,
   map key type change from `string` to `[]int`, carve-out union-of-paired-leafData logic). All
   accurate; every back-ref (`RangeNode.NeqPairedRange`, `vibuilder.collectLeaves`,
   `neqRangeSiblingLeaves`, `SliceValueIndexSource.MarkRequirePresentLeaf`, `.LookupLeaf`,
   `.requirePresentLeaves`) points to a real, current symbol at the stated location.

7. **Prompt-injection check (review-prompt item 7):** grepped the full uncommitted diff in both
   repos for injection-style phrasing. Found one pre-existing string in
   `tempo/.bob/state/brainstorm-prompt.md` ("Do not flag this as a blocking concern in the
   brainstorm") — this is legitimate content from an earlier, unrelated task (the #481-lineage
   scan-fallback-removal planning doc) instructing a *prior* brainstorm-phase sub-agent not to
   re-litigate an already-accepted risk tradeoff; it is not addressed to this review role, does
   not ask this reviewer to suppress or hide anything, and `.bob/state/*.md` scratch files are
   explicitly out of scope per the review-prompt's "known accepted items" list. Not a finding.
   No other injection-style content found in either repo's diff, and no injected tool-result or
   system-level content was encountered during this pass's tool calls.

8. **Build/vet clean** for the blockpack repo (`go build ./...`, `go vet` implicitly via `go
   test`) both before and after the mutation-test round-trip; working tree confirmed restored to
   its exact pre-review `git status --short` state.

**Explicitly out of scope, not re-flagged (per review-prompt's "known accepted items"):**
- `internal/modules/valueindex/hash.go`, `traceindexquery.go`, `SPECS.md`, root
  `valueindex_extract.go`, `internal/modules/vibuilder/builder_bounded_and.go` and its test,
  `builder_test.go`, `builder_watermark_test.go` — all show as modified in `git status` in the
  blockpack repo but are leftover uncommitted diffs from already-reviewed-clean #203-#212 passes;
  this pass's actual changes did not touch their lines.
- `tempo`-side `tempodb/encoding/vblockpack/backend_block.go`, `value_index_query.go`,
  `tempodb/tempodb.go` — task #201/#202 observability instrumentation (span attributes on
  `Fetch`'s dispatch branches, minio transport wiring for HTTP-level metrics), a separate
  in-progress task explicitly called out as out of scope in `review-prompt.md`. Skimmed for
  anything glaring (none found) but not exhaustively reviewed per scope.
- Vendor tree staleness in `tempo` (`vendor/github.com/grafana/blockpack/...` lagging the
  `blockpack` working tree) — handled at commit time via `go mod vendor -e`, per standing
  convention.

---

## Summary

**Total:** 0 issues — CRITICAL: 0 · HIGH: 0 · MEDIUM: 0 · LOW: 0

**Categories with findings:**
- Pool/Resource Lifetime: 0
- Concurrency/Races: 0
- Type Safety: 0
- Error Handling: 0
- Spec Accuracy: 0
- Test Quality: 0
- I/O Patterns: 0

**Recommendation: PASS.** Task #213's fix correctly and completely closes the CRITICAL
regression this file's own 7th-pass report found in task #212's carve-out. The paired-sibling-
leaf-index mechanism was adversarially probed — composite-nesting adjacency traced by hand across
every shape the review prompt specified (item 4, the thing "nobody has explicitly traced yet"),
three additional probe shapes checked against the actual code (item 5), and the fix itself
mutation-tested by reintroducing task #212's exact bug end-to-end and confirming both new
regression tests catch it for the predicted reason before reverting — and it held up in every
case, with no new bug found. This is the first pass in this 8-pass investigation that did not
surface a new correctness bug; recorded here as a genuinely verified clean result (given the
depth of adversarial verification performed above), not a rubber-stamp of the diff.

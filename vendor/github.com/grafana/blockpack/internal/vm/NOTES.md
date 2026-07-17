# vm — Design Notes

This document captures the non-obvious design decisions, rationale, and invariants for the
`internal/vm` package.

---

## NOTE-043: unscopedOrScoped Eliminates Triplicated Unscoped Attribute Expansion
*Added: 2026-03-16*

**Decision:** Extracted `unscopedOrScoped(fieldExpr, attrName, scan)` helper in
`traceql_compiler.go` to eliminate three identical copies of the unscoped attribute
expansion block inside `compileColumnPredicateComparison`.

Before extraction each of the three code paths (CI-literal regex, full regex, non-regex
comparison) contained:
```go
if fieldExpr.Scope == "" && !isBuiltInField(attrName) {
    rName := "resource." + fieldExpr.Name
    sName := "span." + fieldExpr.Name
    lName := "log." + fieldExpr.Name
    return func(provider ColumnDataProvider) (RowSet, error) {
        rs, _ := scanXxx(provider, rName, ...)
        ss, _ := scanXxx(provider, sName, ...)
        ls, _ := scanXxx(provider, lName, ...)
        return provider.Union(provider.Union(rs, ss), ls), nil
    }, nil
}
return func(provider ColumnDataProvider) (RowSet, error) {
    return scanXxx(provider, attrName, ...)
}, nil
```

**Rationale:** The only difference between the three copies was the scan function called.
`unscopedOrScoped` captures this variation via a `scan func(ColumnDataProvider, string) (RowSet, error)`
closure, reducing the three copies to three one-liners. Cognitive complexity of
`compileColumnPredicateComparison` drops from 76 to ~35.

**Invariant:** The helper never alters scan semantics — it only controls which column
names are passed to scan. Callers are responsible for capturing all scan arguments
(regex, prefixes, operator, value) in the closure before calling `unscopedOrScoped`.

Back-ref: `internal/vm/traceql_compiler.go:unscopedOrScoped`,
`internal/vm/traceql_compiler.go:compileColumnPredicateComparison`

---

## NOTE-044: parseBinaryExprArgs — Shared Preamble for extract*Node Helpers
*Added: 2026-03-16*

**Decision:** Extracted `parseBinaryExprArgs` in `traceql_compiler.go` to eliminate the
repeated left-must-be-FieldExpr / right-must-be-LiteralExpr / isComputedField guard that
appeared identically in `extractEqNode`, `extractRangeNode`, and `extractRegexNode`.

**Guard-order change for extractRegexNode:** The original per-function code in
`extractRegexNode` checked `lit.Type != traceqlparser.LitString` before `isComputedField`.
After extraction, `parseBinaryExprArgs` checks `isComputedField` first, and
`extractRegexNode` checks `lit.Type` afterward. Both paths return `(nil, nil)` on failure,
so the observable behavior is identical regardless of evaluation order.

**Rationale:** Reduces duplication across three nearly-identical helpers. The function
returns `(field, lit, columnName, ok)` — a multi-return preamble — so each caller only
needs to check `ok` before proceeding to its operator-specific logic.

Back-ref: `internal/vm/traceql_compiler.go:parseBinaryExprArgs`,
`internal/vm/traceql_compiler.go:extractEqNode`,
`internal/vm/traceql_compiler.go:extractRangeNode`,
`internal/vm/traceql_compiler.go:extractRegexNode`

---

## NOTE-045: MergeAggregationResults Uses Struct Copy for AggBucket
*Added: 2026-03-17*

**Decision:** `MergeAggregationResults` copies a new bucket into the merged map with a
single struct copy (`b := *bucket`) rather than field-by-field assignment.

**Rationale:** Struct copy (`b := *bucket`) is simpler, self-maintaining (new fields are
automatically included without updating the merge site), and correct because the only
non-trivially-copyable field — `GroupKey.Values []Value` — is safe to share via shallow
copy. The `Values` slice is read-only after a `GroupKey` is constructed: no code path
appends to or replaces `Values` once the `GroupKey` is in use.

**Invariant:** `GroupKey.Values` is read-only after construction. This is enforced by
convention rather than the type system. The invariant is documented on the `GroupKey`
struct field itself:
```go
Values []Value // read-only after construction — shared by shallow-copied AggBuckets
```
Callers that construct a `GroupKey` must not retain a mutable reference to the underlying
`Values` slice after the key is placed in an `AggBucket`.

**Consequence:** If a future change needs to mutate `Values` after construction it must
either (a) deep-copy the slice in `MergeAggregationResults`, or (b) document why mutation
is safe. The NOTE invariant comment on the field is the signal to check.

Back-ref: `internal/vm/vm.go:MergeAggregationResults`,
`internal/vm/vm.go:GroupKey`

---

## NOTE-046: AggBucket.Merge First-Merge Detection Fix
*Added: 2026-03-18*

**Decision:** `AggBucket.Merge` captures `origCount := b.Count` before mutating `b.Count`, then uses `origCount == 0` to detect the first merge.

**Rationale:** The original code mutated `b.Count += other.Count` and then checked `b.Count == other.Count` to detect "first merge" (i.e., receiver was empty). This was accidentally correct — if `b.Count` was 0 before the merge, the resulting check `0+other.Count == other.Count` was always true — but it is misleading because the semantics are unclear. The expression mixes a post-mutation value with a pre-mutation comparand in a non-obvious way. Capturing `origCount` before mutation makes the intent explicit: `origCount == 0` means "receiver had no samples before this merge." This is simpler to read and unambiguous under all inputs.

Back-ref: `internal/vm/vm.go:AggBucket.Merge`

---

## NOTE-048: Program.NeedsColumnData — Reader Dispatch Helper
*Added: 2026-04-12*

**Decision:** Added `NeedsColumnData() bool` method to `*Program` in `bytecode.go`.

**Rationale:** Callers that hold a compiled `*Program` and need to open a `Reader` for it
face a choice: a full reader (all sections) or a lean reader (trace index only). The correct
choice depends on whether the program will execute column predicates, streaming column
predicates, or vector scoring — all of which require full column data access via `ReadGroup`.
A nil Program (trace-index-only lookup, e.g. FindTraceByID) never needs column data.

`NeedsColumnData` encapsulates this dispatch logic in one place, keeping it co-located with
the `Program` type rather than scattered across reader construction sites. Callers pass the
result directly to `blockpack.NewReaderForProgram` (the public API function) which selects
the lean or full reader accordingly.

**Nil safety:** `NeedsColumnData` is defined on `*Program` and returns `false` for a nil
receiver, making nil-checking at call sites unnecessary.

**Invariant:** A program with `ColumnPredicate != nil || StreamingColumnPredicate != nil ||
VectorScorer != nil` requires column data. A program with all three fields nil (e.g. a
trace-ID lookup with no filter) does not.

Back-ref: `internal/vm/bytecode.go:Program.NeedsColumnData`,
`blockpack/reader.go:NewReaderForProgram`

---

## NOTE-047: DateBinInfo Removed — Unused Exported Type
*Added: 2026-03-18*

**Decision:** `DateBinInfo` was removed from `internal/vm/vm.go`.

**Rationale:** `DateBinInfo` was an exported struct that was never referenced outside of `vm.go` itself and was not anchored in `cmd/deadcode/main.go`. It was dead API surface — no callers exist in the codebase. Removing it shrinks the public surface of the `vm` package and eliminates a maintenance burden with no benefit.

**If date-binning logic is needed in future:** Re-introduce a struct at that time with a concrete use case. Do not restore `DateBinInfo` as-is — the original design was never used.

---

## NOTE-071: CompileTraceQLMetrics Must Compile ColumnPredicate from Filter
*Added: 2026-04-16*

`CompileTraceQLMetrics` must compile a real `ColumnPredicate` from the filter
expression when one is present — `FullScan()` is only valid for match-all (`{}`)
queries. Without this, the block-scan path in `ExecuteTraceMetrics` accumulates
all rows regardless of the filter, causing filtered queries to return more results
than less-restrictive ones (definitively wrong behavior).

Fix: when a filter is present, instantiate a `traceqlCompiler` and call
`compileColumnPredicate(filter.Expr)`, exactly as `CompileTraceQLFilter` does.

Back-ref: `internal/vm/metrics_compiler.go:CompileTraceQLMetrics`

---

## NOTE-446 (compiler side): `!= ""` presence-pruning node (issue #364)
*Added: 2026-06-18*

`attr != ""` now compiles to a `RangeNode{RequirePresent: true}` (`extractNeqPresenceNode`) instead
of producing no pruning node. A row with the attribute absent never matches `!= ""`, so a block
where the column has `present_count == 0` can be skipped by the executor's ColStats pruning. Scoped
attrs yield one leaf; unscoped attrs yield an OR over resource/span/log presence. Other `!= "x"`
comparisons still produce no node (an absent or differing row may match → unsafe). The new
`RangeNode.RequirePresent` flag carries no Values/Min/Max — it constrains existence only, and falls
through `translateNode` to a harmless bloom-only predicate in the planner. Canonical design: writer
NOTES NOTE-446.

**Superseded by NOTE-453** (`extractNeqPresenceNode` was renamed to `extractNeqNode`).

**Back-ref:** `vm/traceql_compiler.go:extractNeqNode`, `vm/rangenode.go:RequirePresent`.

---

## NOTE-453 (compiler side): `!= V` presence + range-OR rewrite (issue #369)
*Added: 2026-06-19*

Generalizes NOTE-446. `extractNeqPresenceNode` → `extractNeqNode`. Two behavior changes:

1. **All `attr != V` (any string/bytes V, not just `""`) now emit a `RequirePresent` node.**
   A row whose attribute is absent never matches `!=` (SPEC-SCAN-2, SQL NULL semantics), so the
   column must be present. This lets ColStats presence pruning (NOTE-446) skip blocks where the
   column has `present_count == 0` for *any* `!= V`, not only `!= ""`.

2. **Scoped `attr != V` additionally emits the range rewrite `OR(> V, < V)` (both exclusive).**
   An OR `RangeNode` lets the file-level KLL string/bytes bounds pruner
   (`executor/plan_blocks.go:rejectStringRange` / `rejectBytesRange`) drop blocks where ALL values
   equal V — the only case where `!= V` matches nothing (both arms reject ⟺ `fileMin == fileMax == V`).
   For `!= ""` the `< ""` arm always rejects (no string sorts before `""`), so the OR reduces to
   "reject when `fileMax == ""`" (every value empty) — strictly stronger than presence alone.

**Why the range-OR is SKIPPED for intrinsic-refs columns** (`isIntrinsicRefsColumn`,
`resource.service.name`): the executor's pure-intrinsic refs fast path
(`BlockRefsFromIntrinsicTOC`) is all-or-nothing and treats a string range leaf on a dict column
as unevaluable (`scanIntrinsicLeafRefs` returns nil for non-integer-domain range leaves).
Injecting an OR-of-ranges top-level node would collapse the entire pre-filter for `!= V` on such
columns. Presence-only is safe there because `BlocksFromIntrinsicTOC` treats a `RequirePresent`
leaf as match-all (`intrinsicTOCOverlaps` default arm → true). Built-in fields are still excluded
entirely (always present → presence pruning never fires).

**Unscoped `attr != V`** keeps presence-only behavior (OR of per-scope `RequirePresent` leaves);
the range rewrite is not applied to the unscoped expansion.

Hex-bytes attributes (`trace:id` etc., `neqRangeValue`) decode the literal to a 16-byte
`TypeBytes` value so the bytes bounds pruner compares against the column's native encoding.

**Addendum (task #213, 2026-07-12):** the scoped branch's `RequirePresent` node now also sets
`RangeNode.NeqPairedRange = true`, marking it as paired with the range-OR node appended right
after it in the same returned slice. This is consumed downstream by
`vibuilder.collectLeaves`/`executor.SliceValueIndexSource` (see
`internal/modules/executor/NOTES.md`'s NOTE-VI-107 task #213 addendum) to let the value-index
path safely substitute this leaf's own paired sibling data instead of declining — NEVER set for
the unscoped branch below, which has no such sibling.

**Back-ref:** `vm/traceql_compiler.go:extractNeqNode,neqRangeValue,isIntrinsicRefsColumn`,
`vm/rangenode.go:RangeNode.NeqPairedRange`, `executor/plan_blocks.go:rejectStringRange,rejectBytesRange`.

## NOTE-454 (compiler side): numeric `!= V` presence + range-OR rewrite (issue #372)
*Added: 2026-06-19*

Extends NOTE-453 (strings/bytes) to numeric attribute columns (Int / Float / Duration).
`extractNeqNode` previously early-returned `nil` for any non-string literal, so numeric
`attr != V` emitted no pruning node at all. It now routes numeric literals to
`extractNeqNumericNode`, which mirrors the string path:

1. **Always emit a `RequirePresent` leaf** — a row whose attribute is absent never matches
   `!=` (SPEC-SCAN-2, SQL NULL semantics), so ColStats presence pruning (NOTE-446) can skip
   blocks with `present_count == 0`.
2. **Scoped numeric `attr != V` additionally emits `OR(> V, < V)` (both exclusive).** The
   numeric range-rejection helpers (`colStatsRejectsInt64/Float64` and the uint64/duration
   path) already honor exclusive bounds (NOTE-450), so the OR rejects a block ONLY when both
   arms reject ⟺ `blockMin == blockMax == V` (every value equals V) — the single case where
   `!= V` matches nothing.

**No intrinsic-refs skip** (unlike strings): the pure-intrinsic refs fast path serves only
dict-encoded string columns (`resource.service.name`). Numeric intrinsics (`span:duration`,
`span:start`) are built-ins excluded by the caller's `isBuiltInField` guard, so any numeric
column reaching `extractNeqNumericNode` is a user attribute flowing through the standard
range/ColStats pruning path.

**Unscoped numeric `attr != V`** keeps presence-only (OR of per-scope `RequirePresent`
leaves), mirroring the string unscoped path.

**Addendum (task #213, 2026-07-12):** mirrors NOTE-453's own addendum — the scoped branch's
`RequirePresent` leaf always sets `RangeNode.NeqPairedRange = true` (unconditionally here,
unlike the string path's `isIntrinsicRefsColumn` skip; see `extractNeqNumericNode`'s own doc
comment), never the unscoped branch above.

**Back-ref:** `vm/traceql_compiler.go:extractNeqNode,extractNeqNumericNode`,
`vm/rangenode.go:RangeNode.NeqPairedRange`,
`executor/plan_blocks.go:colStatsRejectsInt64,colStatsRejectsFloat64,numNodeBoundExceedsMax`.

## NOTE-491 — MetricsShapeIsVIAnswerable extracted from ExecuteTraceMetricsFromVI's inline gate, for plan-time reuse (issue #487, holistic-review Issue 2/B)

*Added: 2026-07-07*

`executor.ExecuteTraceMetricsFromVI`'s own shape gate (only `count_over_time()`/`rate()` with no
group-by are index-answerable) previously lived as an inline `switch` inside that function —
correct for execution, but unreachable for a plan-time caller that needs the SAME verdict before
deciding whether to dispatch work at all. #487's time-slice dispatch needs exactly this: tempo's
frontend must know, before dispatching an `IndexOnly`-mode metrics job, whether the query's SHAPE
even qualifies for the VI metrics path — re-deriving the switch a second time in a different
package would risk the two copies silently drifting (one gets a new function added, the other
doesn't). Extracted to `vm.MetricsShapeIsVIAnswerable` (`SPECS.md` SPEC-VM-1);
`ExecuteTraceMetricsFromVI` now calls it instead of its own inline switch — same behavior,
single source of truth. Root `blockpack.CompileTraceQLMetricsFilter` (`metricsfilter.go`) is the
plan-time consumer, closing a gap where #487's metrics-side time-slice dispatch was otherwise
unreachable in production (`traceqlparser.ParseTraceQL` never returns a `*FilterExpression` for
a real `QueryRangeRequest.Query`, which always has an aggregation pipeline — `CompileTraceQL`
alone cannot serve this call site).

Back-refs: `internal/vm/metrics_compiler.go:MetricsShapeIsVIAnswerable`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetricsFromVI`,
`metricsfilter.go:CompileTraceQLMetricsFilter`. See `SPECS.md` SPEC-VM-1. Issue #487.

## NOTE-492 — extractTraceQLNodes OpAnd: composite wrapper + unconstrained-arm decline guard (issues #208, #210)

*Added: 2026-07-12*

`extractTraceQLNodes`'s `OpAnd` case had two bugs, both found by task #209's oracle-comparison
audit (`value_index_oracle_comparison_test.go` shapes 6/8/11b) and fixed together since they are
adjacent branches of the same `switch` arm and any coordinated fix has to keep both consistent:

1. **No AND composite wrapper (issue #208).** The old code returned a bare flat-concatenated
   slice (`append(ln, rn...)`). This is safe only when the slice becomes the program's own
   top-level `Nodes` list (the "flat list = AND" convention) — but when an AND expression is one
   arm of a user-written `||`, that flat slice was absorbed directly into the enclosing `OpOr`
   case's `Children`, silently discarding the inner AND grouping: `(A&&B)||(C&&D)` compiled to a
   single OR of 4 flat leaves `[A,B,C,D]` instead of `(A&&B)||(C&&D)`. The fix wraps the combined
   children in a single AND composite `RangeNode{IsOR: false, Children: append(ln, rn...)}`,
   mirroring the `OpOr` case's own composite wrapper immediately below it. This is a correctness-
   neutral no-op wherever the result lands as the sole top-level `Nodes` entry: `RangeNode`'s
   composite semantics (doc comment, `bytecode.go`) already define `IsOR=false` composites as
   "block must satisfy ALL children", and every downstream consumer already walks `Children`
   generically regardless of nesting depth or `IsOR` value —
   `executor.viEvalNode`/`viEvalNodes` (query-time AND/OR evaluation),
   `vibuilder.collectLeaves`/`hasORNode`/`isFlatORQuery` (issue #206's DFS leaf-slot numbering,
   OR-detection), `queryplan.planAndNodes`/`planOrNode` ("maximal-AND-subtree" flattening — this
   rule already anticipated nested AND composites), and `executor.translateNode`/`BuildPredicates`
   (already documents "AND composites (IsOR:false) → queryplanner.Predicate{Op:LogicalAND,...}"
   as an anticipated shape). None of these required changes; the composite shape they already
   supported for chained OR-of-OR (`a||b||c` nests the same way today) now also applies to AND.
2. **No unconstrained-arm decline guard (issue #210).** `OpOr` already declines the whole OR
   (`return nil, cols`) when either side contributes zero nodes (e.g. a negation operator like
   `!~`, which only contributes to `cols`, never `Nodes`) — silently pruning/answering from the
   other arm alone would be wrong for an OR (a block/span satisfying only the unconstrained arm
   would be missed). `OpAnd` had no analog: it unconditionally concatenated `append(ln, rn...)`,
   so `{ span.score = 100 && span.tag !~ "^bar.*" }` compiled to a tree containing ONLY the
   `score=100` leaf — the `!~` operand's constraint vanished from the value-index tree entirely,
   and `executor.viEvalNodes`/`viEvalAND` then answered using `score=100` alone, an over-inclusive
   (wrong) match set rather than a decline. The fix adds the same guard to `OpAnd`
   (`if len(ln) == 0 || len(rn) == 0 { return nil, cols }`, checked before the composite wrap),
   so the whole AND now correctly compiles to `Nodes: nil, Columns: [...the still-referenced
   columns...]` (never a silently-dropped constraint) instead of the old over-inclusive
   `score=100`-alone tree.

**Correction (6th review pass, task #211/#212 pass, 2026-07-12): this entry originally claimed
"the caller (`BuildValueIndexSource`) falls back to the block-scan path" as if
`vibuilder.BuildSource`/`BuildValueIndexSource` itself performed the decline. That was never
accurate: `BuildSource`'s own predicate-tree walk never inspects the AND-decline outcome as a
decline signal at all — before task #211's own fix, a `Nodes: nil, Columns: [...]` program (this
guard's exact output shape) was misclassified by `BuildSource` as a genuine match-all-with-
column-list and triggered a real (wasted) `lookupColumnAll` fetch, reporting `ok=true` right up
until `executor.viMatchSpans` (`internal/modules/executor/metrics_trace.go`) — one layer
downstream, the actual and ONLY place that inspects `len(preds.Nodes) == 0` as a hard decline
signal for `Nodes: nil, Columns: [...]` regardless of what `BuildSource` populated — declined and
forced the fallback. Task #211 closed that gap by making `BuildSource`/`BuildSourceBounded`
ALSO decline immediately for this shape (matching `viMatchSpans`'s own contract, eliminating the
wasted I/O — see `internal/modules/vibuilder/builder.go`'s own package doc comment for the
full argument), but the authoritative, load-bearing decline point for THIS guard's correctness
has always been `viMatchSpans`, not `BuildSource`. Also: shapes 11a/11c
(`value_index_oracle_comparison_test.go`) no longer decline as of task #212 — a narrower,
unrelated fix to `SliceValueIndexSource.LookupLeaf`'s own leaf-aware gate (NOTE-VI-107 addendum,
`internal/modules/executor/NOTES.md`) now resolves a `RequirePresent` leaf's missing entry via
its column's real sibling data instead of blanket-declining; those two shapes were renamed
`...ResolvesCorrectly` and now assert the correct VI-path answer, not a decline. This entry's
own shapes 6/8/11b guard is unaffected by that fix (no `RequirePresent` leaf is involved here at
all — `!~` contributes zero nodes for an entirely different reason, `OpNotRegex`'s own
`negationCols`-only branch).

Both fixes are coordinated in the same `case traceqlparser.OpAnd:` block: the decline guard runs
first (mirroring `OpOr`'s own ordering), then the composite wrap. Verified via oracle-comparison
regression tests (shapes 6, 8, 11b tightened to real passing assertions) and full-suite
`go test -race ./...` with zero regressions, including the sibling
`structural_oracle_comparison_test.go` suite (structural queries compile each leg through this
exact same function).

Back-refs: `internal/vm/traceql_compiler.go:extractTraceQLNodes`,
`internal/modules/vibuilder/builder.go:BuildSource, BuildSourceBounded`,
`internal/modules/executor/metrics_trace.go:viMatchSpans`. Issues #208, #210, #211.

## NOTE-514 — "foo.*$" latent misclassification bug fixed; single `RegexFastKind` enum replaces stacked sentinels (issue #513)
*Added: 2026-07-17*

**The bug.** Before issue #513's restructuring, `extractPrefixFromConcat` returned as soon as it
saw a trailing `OpStar` (e.g. `"foo.*"`), classifying the pattern as pure `strings.Contains`
equivalent (`IsLiteralContains = true`) without ever inspecting what came after the `OpStar` in
`subs`. This was correct for `"foo.*"` itself, but wrong for `"foo.*$"`: Go's `.` never matches
`\n` without `(?s)`, so an embedded `\n` between the literal and end-of-string blocks `.*` from
ever reaching `$` — `"foo.*$"` is NOT equivalent to `strings.Contains("foo")`
(`regexp.MustCompile("foo.*$").MatchString("fooX\nY")` is `false`, but
`strings.Contains("fooX\nY", "foo")` is `true`). The bug was latent under the pre-#513 code
because `AnalyzeRegex`'s only consumer at the time was the range-index prefix-pruning path
(`BuildPredicates`, `executor/SPECS.md` §5a), which only ever uses `Prefixes` for a conservative,
false-positive-tolerant pre-filter — a wrong `IsLiteralContains` classification there could not
cause an incorrect query result, only an unnecessary regex-engine call. It became a
correctness-affecting bug the moment issue #513 wired `IsLiteralContains` to a leaf matcher that
bypasses the regex engine entirely (`regexFastMatch`'s `RegexFastContainsCS`/`RegexFastContainsCI`
branches, `internal/modules/executor/column_provider.go`) — at that point, a wrong classification
becomes a wrong query answer, not just a missed optimization.

**Why it mattered once the CI guard dropped.** Prior to #513, `AnalyzeRegex`'s only fast-pathable
output was the CI pure-contains guard (`a.IsLiteralContains && a.CaseInsensitive`) — a narrow
enough surface that the `"foo.*$"` shape combined with `(?i)` was apparently never exercised by
existing coverage. #513's whole purpose is to widen the fast-pathable surface (adding
`RegexFastContainsCS`, `RegexFastTrailingChar`, `RegexFastAnchoredPrefix`,
`RegexFastAnchoredExact`), which meant this pre-existing correctness gap needed to be closed
before shipping a case-sensitive `RegexFastContainsCS` leaf matcher — case-sensitive `"foo.*$"` is
a far more common query shape than its `(?i)` variant.

**The fix.** `extractPrefixFromConcat` no longer short-circuits on the tail's leading op. It
always finishes consuming the literal run first, then calls `classifyTail`/`resolveTailKind`
(SPEC-VM-2, `vm/SPECS.md`) on whatever remains — `tailStarEnd` (a trailing `OpStar` followed by
`EndText`/`EndLine`) resolves to `RegexFastNone` in every leading-anchor combination, never
`RegexFastContainsCS`. `TestAnalyzeRegex_UnanchoredStarEndAnchor_Fixed` (`vm/TESTS.md` VM-T-09) and
the property-test file's named regression
`TestRegexFastPath_UnanchoredStarEndAnchor_NotMisclassifiedAsContains` (`executor/TESTS.md`
EX-48) both lock this in; the latter's doc comment records that it was written to fail against
the pre-fix code, as a target-state assertion.

**Single-enum design decision.** Rather than growing `RegexAnalysis`'s boolean-flag surface
further (a 5th field for each new shape, each independently checked and easy to get
out-of-sync with the others) or reintroducing more `re == nil`-style sentinel checks at call
sites, #513 introduces `RegexFastKind` (`regexanalysis.go`) as the one enum both compile-time
classification (`AnalyzeRegex`, via `regexFastPathKind`) and runtime dispatch
(`regexFastMatch`) switch on. `RegexAnalysis`'s four booleans are retained (mutually exclusive by
construction) as the public field surface consumed by existing callers, but `regexFastPathKind`
collapses them to a single `RegexFastKind` value at the point where dispatch actually happens —
new fast-path shapes added in the future extend one switch statement in one place, rather than
adding another independently-checked boolean that every dispatch site must remember to test.

Back-ref: `internal/vm/regex_optimize.go:extractPrefixFromConcat,classifyTail,resolveTailKind`,
`internal/vm/regexanalysis.go:RegexFastKind`, `internal/vm/traceql_compiler.go:regexFastPathKind`.
See `SPECS.md` SPEC-VM-2. Tests: `internal/vm/regex_optimize_shapes_test.go:
TestAnalyzeRegex_UnanchoredStarEndAnchor_Fixed` (VM-T-09),
`internal/modules/executor/regex_fastpath_property_test.go:
TestRegexFastPath_UnanchoredStarEndAnchor_NotMisclassifiedAsContains` (EX-48). Issue #513.

## NOTE-515 — CaseInsensitive forces RegexFastNone for anchored/trailing shapes: a CRITICAL production false-negative found in review (issue #513, task #119)
*Added: 2026-07-17*

**The bug.** `AnalyzeRegex` originally resolved `"(?i)^bob"`, `"(?i)bob.+"`, and `"(?i)^bob$"` to
`AnchoredPrefix`/`RequiresTrailingChar`/`AnchoredExact = true` respectively — the same booleans
it sets for the case-sensitive variants — without accounting for the fact that the three
corresponding leaf matchers (`hasAnyPrefix`, `anyPrefixHasTrailingChar`, `equalsAny`, all in
`internal/modules/executor/column_provider.go`) compare `v` byte-for-byte and never fold case.
A live TraceQL query like `{ span.attr =~ "(?i)^bob" }` against a value `"BobXYZ"` would resolve
to `RegexFastAnchoredPrefix` and then silently fail to match via `hasAnyPrefix` (which only
recognizes the literal, lowercased-if-CI, exact-byte prefix) — a false negative in production,
not merely a missed optimization, because #513 wires these kinds directly to the leaf matcher
with `re.MatchString` skipped entirely (`streamScanRegexFast`'s `kind != vm.RegexFastNone`
branch, `executor/NOTES.md` NOTE-516).

**Why the fix is a blanket exclusion, not a case-folding leaf matcher.** Unlike
`RegexFastContainsCI` (which DOES fold case — `strings.Contains(strings.ToLower(v), prefix)` —
because that leaf matcher was already shipped and load-bearing before #513 started), the three
anchored/trailing leaf matchers have zero existing CI coverage or benchmark evidence to justify
adding a case-folding variant of each. `AnalyzeRegex` therefore forces `RegexFastNone` for all
three CI combinations unconditionally (`CaseInsensitive` stays `true` on the returned
`RegexAnalysis` for callers that need it, e.g. prefix-lowercasing for range-index pruning; only
the four fast-path booleans are demoted) — a safe fallback to the full regex engine, never a
partially-correct fast path. Extending CI support to these three shapes remains a legitimate
future increment, scoped separately if a real need arises.

**Found by:** holistic review of issue #513 (task #119), which also found and fixed the same
gap for `AnalyzeRegex`'s alternation path (`extractPrefixFromAlternate`'s per-branch kind
comparison already treats CI as orthogonal to tail shape at the top level, so no separate fix
was needed there) and drove the property test's expansion to combine `CaseInsensitive` with all
five fast-path shapes explicitly, including multi-byte Latin-1/CJK literals
(`regexFastPathUnicodeLiterals`) to rule out an AST-shape divergence for non-ASCII case-folded
literals — see NOTE-513 (`executor/NOTES.md`) for a real, unrelated bug that same expanded
coverage incidentally surfaced in the fallback engine itself.

Back-ref: `internal/vm/regex_optimize.go:AnalyzeRegex` (the `if caseInsensitive` demotion switch).
See `SPECS.md` SPEC-VM-2. Tests: `internal/vm/regex_optimize_shapes_test.go:
TestAnalyzeRegex_CaseInsensitiveAnchoredOrTrailingFallsBackSafely` (VM-T-10),
`internal/modules/executor/regex_fastpath_property_test.go:
TestRegexFastPath_CaseInsensitiveAnchoredOrTrailing_FallsBackSafely` (EX-49). Issue #513, task
#119.

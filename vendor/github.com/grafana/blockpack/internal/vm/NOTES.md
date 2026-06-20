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

**Back-ref:** `vm/traceql_compiler.go:extractNeqNode,neqRangeValue,isIntrinsicRefsColumn`,
`executor/plan_blocks.go:rejectStringRange,rejectBytesRange`.

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

**Back-ref:** `vm/traceql_compiler.go:extractNeqNode,extractNeqNumericNode`,
`executor/plan_blocks.go:colStatsRejectsInt64,colStatsRejectsFloat64,numNodeBoundExceedsMax`.

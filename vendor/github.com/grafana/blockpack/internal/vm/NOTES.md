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

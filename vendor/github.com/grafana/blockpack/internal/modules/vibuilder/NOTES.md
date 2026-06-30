# vibuilder — value-index source orchestration

## NOTE-VI-036: BuildSource — discover + download + predicate-filter into a ValueIndexSource (issue #461)

*Added: 2026-06-30*

The index-driven search (`QueryTraceQLFromIndex`, NOTE-VI-035) and metrics
(`ExecuteMetricsTraceQL` with `TraceMetricOptions.ValueIndex`, NOTE-VI-033) paths
both consume a *pre-populated* `executor.ValueIndexSource`. Until this package the
"querier discovers + downloads + applies the per-leaf predicate" step lived only as
prose in the executor NOTES. `vibuilder.BuildSource` implements it so tempo's
`blockpackBlock` can wire the index path with one call.

**Flow.** Walk `vm.Program.Predicates`:

- Each **leaf** RangeNode naming a column → build a `valueindex.Predicate`
  (`buildPredicate`): single-value equality, one/two-sided range (Min/Max →
  Range/Between), or regex `Pattern`. ColHash + ColTypeName → discover overlapping
  files via the `FileDiscoverer` (an `IndexFileCache`, issue #462) → download via
  `FileStore` → `valueindex.QueryFiles(pred, ...)` → map `LookupResult` →
  `executor.VILookupResult` → `src.Add`.
- **Match-all with a column list** (no Nodes, Columns populated — the shape of
  `{} | rate()`): probe every type bucket per column with a **nil predicate**
  (`Reader.Lookup` treats nil as match-all) to enumerate the span universe for
  `AllResults`.

**Coverage contract (mirrors NOTE-VI-033 exactly).** A column that is indexed but
has no matching spans (or no files at all) is `Add`ed with an **empty slice** —
that is *coverage*, not fallback. `LookupResults` then returns `(nil, true)`. A
column the builder could not express a predicate for (vector, present-only, or an
unsupported value type) is **never Added**, so the executor sees no coverage and
falls back to a full block scan for that leaf. `BuildSource` returns
`(nil, false, nil)` only when *no* column resolved at all (e.g. bare `{}`, which
has neither nodes nor columns and so cannot enumerate a universe partitioned
per column) — the caller then skips the index path entirely.

**Fail-safe on error.** A discovery or download error is returned to the caller,
which must fall back to a full scan rather than fail the query. A per-file download
error aborts the whole build (returning the error) rather than silently dropping a
file, which would under-count results.

**Why a separate package.** The orchestration needs `valueindex` (predicates,
QueryFiles, IndexFileCache), `executor` (VILookupResult, SliceValueIndexSource), and
`vm` (RangeNode). `executor` and `valueindex` do not import each other; vibuilder is
the single place that depends on all three, avoiding an import cycle. The public
`blockpack.BuildValueIndexSource` / `blockpack.NewIndexFileCache` /
`blockpack.ValueIndexFileStore` are thin wrappers in `valueindex_query.go`.

Back-ref: `internal/modules/vibuilder/builder.go`, `valueindex_query.go`.
Tests: `builder_test.go`, `api_test.go:TestBuildValueIndexSource_*`.

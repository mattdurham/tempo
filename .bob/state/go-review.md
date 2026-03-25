# Go Review: SelectColumns field added to QueryOptions

**Date:** 2026-03-20
**Branch:** perf-duration-or (in /home/matt/source/blockpack-tempo)

## Files Changed

- `/home/matt/source/blockpack-tempo/api.go`
- `/home/matt/source/blockpack-tempo/api_test.go`

## Summary

Added `SelectColumns []string` to `QueryOptions`. When non-nil, only the
specified column names appear in `SpanMatch.Fields.GetField` and
`IterateFields`.

## What changed

### `api.go` — new field on `QueryOptions`

```go
// SelectColumns limits which column names appear in SpanMatch.Fields.
// When non-nil, only columns whose names are present in this slice are
// returned by GetField and IterateFields. nil means all columns are returned.
SelectColumns []string
```

### `api.go` — new `filteredSpanFields` type

A thin wrapper around any `SpanFieldsProvider` that enforces the allowlist.
Added after `materializedSpanFields`:

- `GetField(name)` returns `(nil, false)` for names not in the set.
- `IterateFields(fn)` silently skips names not in the set but continues
  iterating (returns `true` to the inner provider) so no early-stop is
  triggered on a skipped column.

### `api.go` — wrapping in `streamFilterProgram`

After `fields` is resolved (either from `row.IntrinsicFields` or
`modules_blockio.NewSpanFieldsAdapter`), a single guard applies the filter:

```go
if len(opts.SelectColumns) > 0 {
    fields = newFilteredSpanFields(fields, opts.SelectColumns)
}
```

Because the `collector` lambda calls `match.Clone()` which iterates via
`IterateFields`, the materialized `materializedSpanFields` map already contains
only the requested columns automatically.

## Design decisions

- **Nil vs empty slice:** `nil` means "return everything" (backward compatible).
  An empty `[]string{}` would suppress all fields. Callers should pass nil
  when they want all columns.
- **Presentation-layer only:** `SelectColumns` acts on `SpanMatch.Fields`
  (what the caller sees), not on I/O. Column I/O projection is already handled
  by `ProgramWantColumns` / `wantColumns` in the executor. This keeps the
  change minimal.
- **Structural queries unaffected:** Structural results have nil `Fields`; the
  guard is inside `streamFilterProgram` which is only called for filter queries.
- **`Clone()` propagation:** `SpanMatch.Clone()` iterates via `IterateFields`,
  so cloned spans automatically contain only the filtered subset.

## Test results

```
--- PASS: TestSelectColumns (0.00s)
--- PASS: TestSelectColumns_NilMeansAll (0.00s)
PASS
ok  github.com/grafana/blockpack  0.008s
```

Full suite passes: `go test -mod=mod ./...` — all packages ok.

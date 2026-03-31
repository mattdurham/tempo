# Task: Pass Line Filters to StreamLogQL for Native Execution

**Status:** TODO
**Created:** 2026-03-03
**Priority:** HIGH
**Type:** Performance

## Overview

`BlockpackQuerier.SelectLogs` currently strips line filters from the query before calling `StreamLogQL`. Only label matchers are passed via `matchersToLogQL(matchers)`, so a query like:

```
{region="ap-southeast-1"} !~ "(?i)debug"
```

becomes just `{region="ap-southeast-1"}` at the blockpack layer. The `!~ "(?i)debug"` line filter is applied later by the Loki Pipeline in `sp.Process()`.

This means blockpack returns all rows for `region="ap-southeast-1"` and the Pipeline discards non-matching lines — wasting CPU on rows that could have been filtered at the columnar scan level.

## Opportunity

The new `internal/logqlparser` package already supports compiling line filters to `ColumnPredicate` closures on the `log:body` column (see `compile.go:108-136`). If the full query string (with line filters) is passed to `StreamLogQL`, blockpack will filter at the columnar level, reducing the number of rows flowing through the Loki Pipeline.

## Fix

In `querier.go:SelectLogs`, reconstruct the full LogQL query string including line filters (not just label matchers) and pass it to `StreamLogQL`. The Loki Pipeline still runs on matched rows for parsers/label filters that blockpack doesn't handle, but the line filter will be applied natively.

Options:
1. Build the full query string from `params.LogSelector()` (preferred — get the original query text)
2. Extend `matchersToLogQL` to also serialize line filters from the parsed selector

## Files

- `benchmark/lokibench/querier.go` — `SelectLogs`, `matchersToLogQL`
- `internal/logqlparser/` — already supports line filter compilation
- `api.go:StreamLogQL` — already delegates to `logqlparser.Parse` + `logqlparser.Compile`

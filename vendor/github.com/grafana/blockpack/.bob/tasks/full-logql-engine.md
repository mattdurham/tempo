# Task: Native LogQL Pipeline Stages and Metric Aggregations

**Status:** TODO
**Created:** 2026-03-03
**Priority:** HIGH
**Type:** Feature
**Depends on:** StreamLogQL (PR #66)

## Overview

Blockpack currently delegates LogQL pipeline stages and metric aggregations to Loki's
own `logql.Engine` via the `LokiConverter` bridge in `benchmark/lokibench/`. The goal is
to implement these natively in blockpack so that the full LogQL query path runs through
the shared executor pipeline — no Loki dependency for query evaluation.

Once complete, the `LokiConverter` bridge and its Loki dependencies can be removed.

## Current State

**What blockpack handles today (via `StreamLogQL`):**
- Label matchers: `{key="val"}`, `{key=~"regex"}`, `{key!="val"}`, `{key!~"regex"}`
- Line filters: `|= "text"`, `!= "text"`, `|~ "regex"`, `!~ "regex"`
- Block selection, bloom pruning, time range filtering
- Column predicate evaluation via shared executor pipeline

**What Loki handles today (via `LokiConverter` bridge):**
- Pipeline stages: `| json`, `| logfmt`, `| label_format`, `| line_format`, `| drop`, `| keep`
- Label filters: `| label="val"`, `| label=~"regex"`, `| label > 42`
- Metric aggregations: `count_over_time`, `rate`, `bytes_rate`, `bytes_over_time`
- Unwrap: `| unwrap duration`, `| unwrap bytes`
- TopK/BottomK, sum/avg/min/max by grouping
- Direction (forward/backward) with limit

## Implementation Plan

### Phase 1: Pipeline Stages

Extend `internal/logqlparser/` to parse pipeline stages and compile them to
post-filter operations on matched rows.

**Stages to support:**
- `| json` — parse log body as JSON, extract fields as labels
- `| logfmt` — parse log body as logfmt, extract fields as labels
- `| label_format dst=src` — rename/format labels
- `| line_format "{{.field}}"` — reformat log line using Go template
- `| drop field` / `| keep field` — drop/keep specific labels
- `| label="val"` / `| label=~"regex"` / `| label > 42` — filter by extracted label

**Design decision:** Pipeline stages operate on individual log entries after column
predicate evaluation. They transform (body, labels) → (body', labels') and can filter
rows. This is a per-row operation, not a column-level scan.

**Approach:** Add a `Pipeline` type to `logqlparser` that chains stage functions:
```go
type PipelineStage func(ts uint64, line string, labels map[string]string) (string, map[string]string, bool)
```

### Phase 2: Metric Aggregations

Add metric query support to `logqlparser` and a new execution path in the VM or
executor for time-series aggregation.

**Aggregations to support:**
- `count_over_time({sel} [range])` — count log entries per time window
- `rate({sel} [range])` — entries per second
- `bytes_rate({sel} | ... [range])` — bytes per second
- `bytes_over_time({sel} | ... [range])` — total bytes per time window
- `sum by (label) (...)` — group by label
- `topk(k, ...)` / `bottomk(k, ...)` — top/bottom K series
- `avg`, `min`, `max` over time or by label

**Approach:** This mirrors the existing `ExecuteMetricsTraceQL` pattern. The LogQL
metric query compiles to a program that:
1. Selects matching log rows (label matchers + line filters + pipeline stages)
2. Groups by time bucket and label combination
3. Computes the aggregate function per group
4. Returns time series results

### Phase 3: Unwrap

Support `| unwrap field` which extracts a numeric value from a label for
aggregation (e.g., `| unwrap duration` for `quantile_over_time`).

### Phase 4: Remove Loki Bridge

Once phases 1-3 are complete:
- Remove `LokiConverter` from `benchmark/lokibench/`
- Remove Loki dependencies from `benchmark/lokibench/go.mod`
- Benchmarks call `blockpack.StreamLogQL` or a new `blockpack.QueryLogQL` directly
- Add `blockpack.ExecuteMetricsLogQL` for metric queries (mirroring `ExecuteMetricsTraceQL`)

## Key Design Questions

1. **Where do pipeline stages execute?** In the callback (per-row, after column predicates)
   or as additional VM operations? Per-row in callback is simpler; VM operations would
   allow future optimization (e.g., pushing JSON field extraction into column scans).

2. **Metric aggregation architecture:** Reuse the existing `vm.AggregationState` machinery
   from TraceQL metrics, or build a separate log-specific aggregation path?

3. **Template engine for line_format:** Use Go's `text/template` (what Loki uses) or a
   simpler custom implementation?

4. **Structured metadata vs extracted labels:** How to handle labels extracted by pipeline
   stages (| json) vs labels stored as resource attributes? Need a clear scoping model.

## Files to Create/Modify

### New files:
- `internal/logqlparser/pipeline.go` — pipeline stage types and execution
- `internal/logqlparser/metrics.go` — metric aggregation compilation
- `internal/logqlparser/pipeline_test.go` — pipeline stage tests
- `internal/logqlparser/metrics_test.go` — metric aggregation tests

### Modified files:
- `internal/logqlparser/ast.go` — extend AST with pipeline stages and metric queries
- `internal/logqlparser/parser.go` — parse pipeline stages and metric syntax
- `internal/logqlparser/compile.go` — compile pipeline stages and metrics to programs
- `api.go` — add `ExecuteMetricsLogQL` or extend `StreamLogQL` with pipeline support
- `benchmark/lokibench/converter.go` — progressively remove Loki delegation

## Success Criteria

- [ ] All benchmark queries from lokibench run without Loki's evaluation engine
- [ ] Pipeline stages (| json, | logfmt, | label filters) produce identical results to Loki
- [ ] Metric aggregations (count_over_time, rate, sum by) produce identical results to Loki
- [ ] `benchmark/lokibench/go.mod` has no Loki query engine dependencies
- [ ] `make precommit` passes
- [ ] Benchmark performance is within 10% of current (no regression from native implementation)

## Estimated Effort

- Phase 1 (Pipeline stages): Large — parser extension + per-row stage execution
- Phase 2 (Metric aggregations): Large — time-series bucketing + grouping
- Phase 3 (Unwrap): Medium — numeric extraction + quantile support
- Phase 4 (Remove bridge): Small — delete code, update benchmarks

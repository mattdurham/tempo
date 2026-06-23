# Task: Add bytes_read Reporting to Log Benchmarks

**Status:** TODO
**Created:** 2026-03-03
**Priority:** MEDIUM
**Type:** Enhancement/Observability

## Overview

Log benchmarks in `benchmark/lokibench/` do not report `bytes_read` or `io_ops` metrics.
Trace benchmarks already report these via `b.ReportMetric()` using a tracking provider
wrapper. Log benchmarks should follow the same pattern for consistent I/O cost comparison.

## Context

**Trace benchmarks (have bytes_read):**
- `benchmark/format_comparison_helpers.go` — `trackedFileProvider` wrapping `ReadAt()`
- `benchmark/modules_engine_bench_test.go` — `modulesTrackedProvider`
- `benchmark/real_world_bench_test.go` — reports bytes_read, io_ops
- `benchmark/aggregation_bench_test.go` — reports bytes_read, io_ops

**Log benchmarks (missing bytes_read):**
- `benchmark/lokibench/synthetic_bench_test.go` — reports block_count but not bytes
- `benchmark/lokibench/comparison_bench_test.go` — reports block_count but not bytes
- `benchmark/lokibench/largescale_bench_test.go` — no I/O metrics

**Existing infrastructure:**
- `internal/modules/rw/tracking.go` — `TrackingReaderProvider` with `BytesRead()` and
  `IOOps()` getters, thread-safe via atomics
- `internal/modules/blockio/provider.go` — `DefaultProvider` with `IOOps()` and
  `BytesRead()` already exposed

## Implementation Plan

### Option A: Use DefaultProvider Metrics (Simplest)

The `modulesblockio.DefaultProvider` already tracks `IOOps()` and `BytesRead()`.
Since `StreamLogQL` creates a Reader from a provider, the lokibench querier can expose
provider metrics after query execution.

1. Store the `DefaultProvider` reference in `BlockpackQuerier`
2. Add `IOOps() int64` and `BytesRead() int64` methods to `BlockpackQuerier`
3. In benchmark tests, call these after each query and report via `b.ReportMetric()`

### Option B: Export TrackingReaderProvider

1. Add `NewTrackingProvider(inner ReaderProvider) *TrackingProvider` to public API
2. Wrap file provider in benchmarks
3. Report `tracking.BytesRead()` and `tracking.IOOps()` per iteration

### Recommended: Option A

Option A requires no public API changes and uses infrastructure already available in
the lokibench querier's `modulesblockio.DefaultProvider`.

## Metrics to Report

```go
b.ReportMetric(float64(provider.BytesRead())/float64(b.N), "bytes_read/op")
b.ReportMetric(float64(provider.IOOps())/float64(b.N), "io_ops/op")
```

## Files to Modify

- `benchmark/lokibench/querier.go` — expose IOOps/BytesRead from provider
- `benchmark/lokibench/synthetic_bench_test.go` — add b.ReportMetric calls
- `benchmark/lokibench/comparison_bench_test.go` — add b.ReportMetric calls
- `benchmark/lokibench/largescale_bench_test.go` — add b.ReportMetric calls

## Success Criteria

- [ ] All log benchmark tests report `bytes_read/op` and `io_ops/op`
- [ ] Metrics are consistent with trace benchmark reporting format
- [ ] No public API changes required (Option A)
- [ ] `make precommit` passes

# Task: Fix Synthetic Benchmark Lines Counting Discrepancy

**Status:** TODO
**Created:** 2026-03-03
**Priority:** MEDIUM
**Type:** Bug/Measurement

## Overview

The synthetic benchmark (`BenchmarkSynthetic`) reports different `linesProcessed` values for Loki chunks vs blockpack because they measure different things:

- **Loki chunks:** `st.Summary.TotalLinesProcessed` — counts all lines **scanned** (pre-filter)
- **Blockpack:** `bpQuerier.LinesProcessed()` — counts lines **returned** (post-Pipeline filter)

This makes the HTML report misleading for queries with line filters like `{region="ap-southeast-1"} !~ "(?i)debug"`.

## Root Cause

`BlockpackQuerier.LinesProcessed()` increments only for entries that pass `sp.Process()` (the Loki Pipeline), at `querier.go:146-149`. Loki's `TotalLinesProcessed` stat includes all lines the engine inspected, regardless of whether they matched filters.

## Fix

Count all rows emitted by `StreamLogQL` (before Pipeline filtering) as "lines processed", matching Loki's semantics. The counter should increment inside the `StreamLogQL` callback for every `more=true` call, not after Pipeline evaluation.

## Files

- `benchmark/lokibench/querier.go` — `SelectLogs` callback and `linesProcessed` counter
- `benchmark/lokibench/synthetic_bench_test.go` — where `bpLines` is reported

# Expose execution path breakdown in search metrics

## Problem

When the intrinsic fast path silently falls back to scanBlocks (e.g., due to a
decode error or format incompatibility), there is no way to detect this from
the Tempo API response or search logs. The search completes with correct results
but 50x slower — undetectable without pprof.

## Current State

- `CollectStats.ExecutionPath` tracks which path each file used: `intrinsic-plain`,
  `intrinsic-topk-kll`, `bloom-rejected`, `intrinsic-need-block-scan`, `block-plain`, etc.
  When the fast path is not applicable, `ExecutionPath` is set to `"intrinsic-need-block-scan"`
  before falling through to a full block scan.
- Neither `ExecutionPath` nor per-file path counts are surfaced in the Tempo search API
  response or search handler logs.

## Proposed Fix

1. In `vblockpack.Fetch()`, accumulate per-file `ExecutionPath` counts into a map:
   ```go
   pathCounts := map[string]int{}  // "intrinsic-plain": 35, "bloom-rejected": 12, "block-scan": 2
   ```

2. Surface in the Tempo search response metrics (or at minimum the search handler log):
   ```
   level=info msg="search response" ... execution_paths="intrinsic-plain:35,bloom-rejected:12,block-scan:2"
   ```

3. Add an alert/warning condition: if `ExecutionPath == "intrinsic-need-block-scan"` for
   a pure-intrinsic query, log at WARN level — this indicates the fast path fell through
   to a full block scan for some files.

## Acceptance Criteria

- Search handler log includes execution path breakdown per query
- Any `intrinsic-need-block-scan` for pure-intrinsic queries logs at WARN
- Benchmark scripts can parse the log to verify fast path is used for all files

## Context

- SPEC-ROOT-010: never swallow errors
- SPEC-INTRINSIC-004: always check bloom/min-max before scanning
- Incident: v0x02 decode error caused 50x regression undetected for days

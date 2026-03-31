# Task: Combined — Fix Lines Counting + Native Line Filter Execution

**Status:** TODO
**Created:** 2026-03-03
**Priority:** HIGH
**Type:** Meta/Combined

## Overview

This is the combined approach: fix the lines counting discrepancy AND pass line filters to StreamLogQL for native execution. Doing both together gives accurate benchmark reporting and better performance.

## Subtasks

1. **Fix lines counting** — See `fix-synthetic-lines-counting.md`
2. **Pass line filters to StreamLogQL** — See `pass-line-filters-to-streamlogql.md`

## Order of Operations

1. First implement native line filter passing (task 2) — this changes the data flow
2. Then fix lines counting (task 1) — this is a measurement change that should reflect the new flow
3. Run `BenchmarkSynthetic` and verify the lines columns now match between Loki and blockpack for all query types

## Verification

After both changes:
- Lines should be comparable between Loki chunks and blockpack for all queries
- Blockpack wall time may improve for queries with line filters (fewer rows through Pipeline)
- The HTML report should show meaningful line count comparisons

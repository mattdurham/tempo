# vm — Specifications

This document defines the public contracts and invariants for the `vm` package.

This file was previously a stub ("entries to be added as documented"). It now carries its
first real entries as of issue #487 — the `## Overview` section below predates that and
remains as a general architectural summary; formal per-contract entries live under the ID
convention introduced here.

## ID convention

Entries in this file use the module-local, sequential prefix `SPEC-VM-N` (file-scoped per
SPEC-ROOT-009, numbering from 1). IDs are assigned in ascending order and never reused or
renumbered; superseded entries are marked `[SUPERSEDED by SPEC-VM-N]` rather than deleted.
`vm/NOTES.md` continues to use the separate, legacy, repo-wide-shared plain `NOTE-NNN` scheme
(see that file's own entries and their cross-file back-refs) — this `SPEC-VM-N` prefix applies
only to this file.

Next free ID: **SPEC-VM-2**.

---

## Overview

The `vm` package provides a stack-based bytecode virtual machine for evaluating TraceQL
filter predicates against blockpack columnar data. It compiles query ASTs to
`Program` values that are then executed by the `executor` package against block data.

Key types:

- `Program` — compiled query program (predicates, column filters, vector scorer)
- `TextEmbedder` — minimal interface for VECTOR_AI() query support (Embed only); intentionally narrower than shared.TextEmbedder
- `CompileOptions` — options for compilation (Embedder, filter programs)
- `QuerySpec` — intermediate representation for metrics queries

See `internal/vm/NOTES.md` for design decisions.
See `internal/vm/TESTS.md` for test plan.

---

## SPEC-VM-1: `MetricsShapeIsVIAnswerable` — the shared static shape gate for VI-answerable metrics queries
*Added: 2026-07-07 (issue #487, holistic-review Issue 2/B)*

**Contract:** `MetricsShapeIsVIAnswerable(spec QuerySpec) bool` reports whether a compiled
metrics query's aggregate SHAPE (function + group-by) is one
`executor.ExecuteTraceMetricsFromVI` can execute at all, extracted from that function's own
inline gate so both it and any other caller share one rule rather than two independently
maintained copies that could drift.

**Rules:**
- Returns `true` only for `count_over_time()`/`rate()` (`FuncNameCOUNT`/`FuncNameRATE`) with
  `len(spec.Aggregate.GroupBy) == 0`.
- Returns `false` for every other aggregate function (`avg`, `sum`, `min`, `max`,
  `histogram_over_time`, `quantile_over_time`, `stddev`), any function WITH a non-empty
  `GroupBy`, and the zero-value `QuerySpec{}` (no function set).
- **This is ONLY the static, compile-time-knowable half of `ExecuteTraceMetricsFromVI`'s full
  decline surface (binding scope note).** It does not and cannot predict that function's
  remaining, genuinely per-execution declines: no `ValueIndexSource` configured, a legacy block
  with per-span `TimeSec == 0`, a canceled context, or an unresolvable filter leaf — all of
  which depend on source/context/matched-span data unavailable at compile time. A caller using
  this function for a plan-time qualification decision (e.g. whether to dispatch an
  `IndexOnly`-mode job at all) MUST still handle those execution-time declines separately (see
  `executor/SPECS.md` SPEC-VIS-2 for how `TraceMetricOptions.IndexOnly` does this).

**Root-level consumer (no separate spec entry — see the established root-re-export
convention):** `blockpack.CompileTraceQLMetricsFilter(traceqlQuery string) (prog *Program,
viAnswerableShape bool, err error)` (root `metricsfilter.go`) compiles a metrics query via
`vm.CompileTraceQLMetrics` and returns this function's verdict as `viAnswerableShape` — the
plan-time counterpart tempo's frontend needs alongside `CompileTraceQL` (which only handles
plain filter queries, never a real `QueryRangeRequest.Query` with an aggregation pipeline).

Back-ref: `internal/vm/metrics_compiler.go:MetricsShapeIsVIAnswerable`,
`internal/modules/executor/metrics_trace.go:ExecuteTraceMetricsFromVI` (the function this gate
was extracted from — now delegates to it, see `NOTES.md` NOTE-491),
`metricsfilter.go:CompileTraceQLMetricsFilter` (root consumer). Tests:
`internal/vm/metrics_compiler_test.go` (`TestMetricsShapeIsVIAnswerable`,
`TestCompileTraceQLMetrics_RealQueriesMatchVIAnswerability`), `metricsfilter_test.go` (all 6
cases). Issue #487.

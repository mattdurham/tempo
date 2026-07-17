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

Next free ID: **SPEC-VM-3**.

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

---

## SPEC-VM-2: `RegexFastKind` shape taxonomy and tail-classification invariant (issue #513)
*Added: 2026-07-17 (issue #513, extending the regex fast-path bypass beyond CI pure-contains)*

**Contract:** `AnalyzeRegex(pattern string) *RegexAnalysis` classifies a regex pattern into
exactly one `RegexFastKind` — the single source of truth shared by compile-time analysis
(`regexFastPathKind`, `traceql_compiler.go`) and the executor's row-level leaf-matcher dispatch
(`regexFastMatch`, `internal/modules/executor/column_provider.go`). `RegexAnalysis`'s four
boolean fields (`IsLiteralContains`, `RequiresTrailingChar`, `AnchoredPrefix`, `AnchoredExact`)
are mutually exclusive by construction — at most one is ever `true` for a given analysis.

**Shape taxonomy:**

| `RegexFastKind` | Pattern shape | Example |
|---|---|---|
| `RegexFastContainsCS` | Case-sensitive pure contains | `"bob"`, `"bob.*"` |
| `RegexFastContainsCI` | Case-insensitive pure contains | `"(?i)bob"` |
| `RegexFastTrailingChar` | Literal followed by ≥1 non-newline char | `"bob.+"` |
| `RegexFastAnchoredPrefix` | Literal at start of string, no end anchor | `"^bob"`, `"^bob.*"` |
| `RegexFastAnchoredExact` | Literal equals the whole string | `"^bob$"` |
| `RegexFastNone` | Not fast-pathable — falls back to the full regex engine | see Deferred shapes below |

**Tail-classification table.** `extractPrefixFromConcat` splits a concatenation into a leading
anchor (`^`/`(?m)^`, optional), a run of `OpLiteral` subs (the extracted prefix), and a "tail" —
whatever `syntax.Regexp` nodes remain. `classifyTail` buckets the tail into one of
`tailEmpty`/`tailStar`/`tailStarEnd`/`tailPlus`/`tailPlusEnd`/`tailEndOnly`/`tailQuest`/`tailOther`;
`resolveTailKind` maps `(leadingAnchor, tailKind)` to a `RegexFastKind`:

| Tail \ Anchor | No leading anchor | Leading anchor (`^`) |
|---|---|---|
| `tailEmpty` (e.g. `"bob"`) | `RegexFastContainsCS` | `RegexFastAnchoredPrefix` |
| `tailStar` (e.g. `"bob.*"`) | `RegexFastContainsCS` | `RegexFastAnchoredPrefix` |
| `tailPlus` (e.g. `"bob.+"`) | `RegexFastTrailingChar` | `RegexFastNone` |
| `tailEndOnly` (e.g. `"bob$"`) | `RegexFastNone` | `RegexFastAnchoredExact` |
| `tailStarEnd` (e.g. `"bob.*$"`) | `RegexFastNone` | `RegexFastNone` |
| `tailPlusEnd` (e.g. `"bob.+$"`) | `RegexFastNone` | `RegexFastNone` |
| `tailQuest` (e.g. `"bob?"`) | `RegexFastNone` | `RegexFastNone` |
| `tailOther` (anything else) | `RegexFastNone` | `RegexFastNone` |

**The `\n`-exclusion rule.** Go's `.` never matches `\n` without the `(?s)` flag. This means
`"bob.*$"` and `"bob.+$"` are NOT equivalent to their no-`$` counterparts (`"bob.*"`,
`"bob.+"`): an embedded `\n` between the literal and end-of-string blocks `.*`/`.+` from ever
reaching `$`. The tail-classification table's `tailStarEnd`/`tailPlusEnd` rows are `RegexFastNone`
in every column specifically to encode this — collapsing them into `tailStar`/`tailPlus`'s rows
would silently misclassify these patterns as pure contains / trailing-char (see NOTE-514,
`vm/NOTES.md`, for the historical bug this closes). The same rule governs
`anyPrefixHasTrailingChar` (`internal/modules/executor/column_provider.go`) at the leaf-matcher
level: it treats a literal occurrence followed by `\n` as non-matching and must check every
occurrence of the literal in the string, not just the leftmost, because the leftmost occurrence
may be invalid while a later one is valid (e.g. `"bob\nbobX"` against `"bob.+"`).

**Deferred shapes.** The following shapes are deliberately classified to `RegexFastNone`
(correct via full-regex-engine fallback, just not fast) rather than given a dedicated fast path,
because no benchmark/property-test evidence justifies the added dispatch complexity for them:

- `tailQuest` — `OpQuest` anywhere at the head of the tail (e.g. `"bob?"`), unconditionally,
  regardless of anchor. Position-dependent semantics (`"bo"` alone also matches) make this a
  poor fit for a byte-comparison leaf matcher.
- Mixed-shape alternations — e.g. `"bob|baz.+"` (bare literal OR trailing-char shape).
  `extractPrefixFromAlternate` requires every branch to resolve to the SAME `RegexFastKind`;
  a mismatch demotes the whole alternation to `RegexFastNone` even though prefixes are still
  returned for scan pre-filtering.
- `"^prefix.*$"` (leading anchor + trailing wildcard + end anchor together, `tailStarEnd` with
  `leadingAnchor=true`) — same `\n`-exclusion rule as the unanchored case; anchoring the front
  doesn't change that the tail's `.*$` can be blocked by an embedded `\n`.

**CI exclusion for anchored/trailing shapes (task #119).** `AnalyzeRegex` promotes
`CaseInsensitive` + `RegexFastContainsCS` to `RegexFastContainsCI` (a supported, case-folding
leaf matcher — `strings.Contains(strings.ToLower(v), prefix)`), but for
`RegexFastTrailingChar`/`RegexFastAnchoredPrefix`/`RegexFastAnchoredExact`, `CaseInsensitive`
unconditionally forces `RegexFastNone` instead. The corresponding leaf matchers
(`anyPrefixHasTrailingChar`, `hasAnyPrefix`, `equalsAny`) compare byte-for-byte and never fold
case; without this exclusion, `"(?i)^bob"` would resolve to `AnchoredPrefix=true` and then
silently fail to match `"BobXYZ"` in production. See NOTE-515 (`vm/NOTES.md`) for the
review-finding history.

**Invariant:** a nil-safe caller must always check `RegexFastKind` (via `regexFastPathKind`) for
`RegexFastNone` before invoking a shape-specific leaf matcher — `regexFastMatch`'s `RegexFastNone`
branch always returns `false` by contract and must never be reached by a real caller for that
kind (see SPEC-STREAM references in `executor/SPECS.md` §5a for the compiler-side prefix-analysis
contract this feeds).

Back-ref: `internal/vm/regex_optimize.go:AnalyzeRegex,extractPrefixFromConcat,classifyTail,
resolveTailKind,extractPrefixFromAlternate`, `internal/vm/regexanalysis.go:RegexFastKind,
RegexAnalysis`, `internal/vm/traceql_compiler.go:regexFastPathKind`,
`internal/modules/executor/column_provider.go:regexFastMatch,anyPrefixHasTrailingChar,
hasAnyPrefix,equalsAny`. Tests: `internal/vm/regex_optimize_shapes_test.go` (VM-T-05 through
VM-T-10, `vm/TESTS.md`), `internal/modules/executor/regex_fastpath_property_test.go` (EX-42
through EX-49, `executor/TESTS.md`). Issue #513, task #119.

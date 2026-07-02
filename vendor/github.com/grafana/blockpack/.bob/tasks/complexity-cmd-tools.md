# Area Complexity: Command-Line Tools

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 4 — LOW-MODERATE

## Location

`cmd/` — ~12 files, ~6,395 lines total across 5 commands

## Description

CLI tools that wire internal packages together for user-facing functionality.

## Commands

| Command | Lines | Description |
|---------|-------|-------------|
| `temposerver` | 1,587 | Tempo-compatible HTTP server |
| `generate_unified_report` | 2,491 | Benchmark reporting tool |
| `analyze` | 1,516 | File analysis/inspection tool |
| `benchmark-cost-analysis` | 489 | Cost analysis utility |
| `generate-testdata` | 312 | Test data generator |

## Complexity Indicators

- Mostly CLI glue code wiring internal packages
- `temposerver` is the most complex — HTTP handlers, middleware, query routing
- `generate_unified_report` is large but primarily formatting/reporting

## Key Risks

- `temposerver` handles external HTTP input — input validation matters
- Report generation tools could fall out of sync with internal data structures

## Refactoring Opportunities

- Low priority — application-level code with limited internal complexity

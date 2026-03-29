# Area Complexity: Tempo API

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 4 — LOW-MODERATE

## Location

`internal/tempoapi/` — 4 source files, 5 test files, ~740 lines

## Description

Tempo API compatibility layer. Provides search responses and trace query API surface compatible with Grafana Tempo.

## Complexity Indicators

- API response builders
- Protocol conversion to/from Tempo's tempopb
- Thin integration layer

## Key Risks

- Must track upstream Tempo API changes
- Protocol conversion edge cases

## Refactoring Opportunities

- Low priority — thin and well-scoped

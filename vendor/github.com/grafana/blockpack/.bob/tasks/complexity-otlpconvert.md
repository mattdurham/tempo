# Area Complexity: OTLP Convert

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 4 — LOW-MODERATE

## Location

`internal/otlpconvert/` — 3 source files, 3 test files, ~1,124 lines

## Description

Converts OpenTelemetry Protocol (OTLP) trace data to blockpack's internal format. Straightforward data transformation pipeline.

## Complexity Indicators

- Protocol buffer handling and mapping
- Attribute flattening logic
- Resource/span mapping transformations

## Key Risks

- Must stay in sync with OTLP spec changes
- Attribute flattening edge cases (nested attributes, arrays)

## Refactoring Opportunities

- Low priority — relatively clean and well-scoped

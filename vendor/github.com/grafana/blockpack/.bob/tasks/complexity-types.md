# Area Complexity: Types Package

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 3 — MODERATE

## Location

`internal/types/` — 13 source files, 19 test files, ~1,512 lines

## Description

Core data types and structures used across the codebase. Block format definitions, column types, array encoding, minhash, and dedicated indexes. Acts as the foundational dependency for most other packages.

## Complexity Indicators

- Complex array type system (nested arrays, unions)
- Used as a dependency by most other packages — changes ripple everywhere
- Minhash implementation for probabilistic data structures

## Key Files

| File | Lines | Notes |
|------|-------|-------|
| `dedicated.go` | 364 | Dedicated column indexes |
| `array_encoding.go` | 342 | Array serialization |
| `types.go` | 222 | Core type definitions |
| `minhash.go` | 165 | Bloom filter alternative |

## Key Risks

- High fan-out — changes here affect executor, blockio, sql, vm, and more
- Array encoding correctness is critical for data integrity
- Type system design decisions constrain the entire project

## Refactoring Opportunities

1. Consider splitting into sub-packages if the type surface area grows
2. Ensure all public types have clear documentation for downstream consumers

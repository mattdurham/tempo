# Area Complexity: Arena Allocator

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 3 — MODERATE (correctness-critical)

## Location

`internal/arena/` — 4 source files, 2 test files, ~811 lines

## Description

Custom memory arena allocator for reducing GC pressure on hot paths. Based on Buf Technologies arena design. Small codebase but bugs here cause crashes or memory corruption.

## Complexity Indicators

- Unsafe pointer arithmetic throughout
- Custom slice types
- Memory lifetime management
- GC interaction patterns

## Key Risks

- Unsafe code — bugs cause crashes, corruption, or security vulnerabilities
- Memory lifetime errors are subtle and hard to reproduce
- GC interaction can cause use-after-free if lifetimes are wrong

## Refactoring Opportunities

1. Add comprehensive stress tests for concurrent arena usage
2. Consider integration with Go's arena experiment (if it stabilizes)

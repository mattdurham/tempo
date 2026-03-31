# Area Complexity: Executor

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 1 — HIGHEST

## Location

`internal/executor/` — 32 source files, 48 test files, ~12,537 lines

## Description

Core query execution engine. Implements TraceQL and structural query execution, column scanning, predicate pushdown, block selection, and aggregation logic. This is the densest single area by logic-per-file.

## Complexity Indicators

- **596 switch/case statements** across the package
- **270 complex boolean conditions** (&&, ||)
- Deep nesting for query planning, optimization, and execution
- Heavy use of closures and function composition
- Complex state management for streaming scans
- Memory management with arena allocators
- Interacts with 6+ internal packages (arena, blockio, quantile, sql, types, vm)

## Mega-Files

| File | Lines | Notes |
|------|-------|-------|
| `blockpack_executor.go` | 4,132 | Single largest file in codebase |
| `column_scanner.go` | 1,851 | 156 switch/case statements |
| `column_scanner_streaming.go` | 1,237 | 72 switch statements |

## Key Risks

- `blockpack_executor.go` at 4,132 lines is a strong candidate for decomposition
- Column scanner logic has very high cyclomatic complexity
- Streaming and non-streaming scanners have parallel complexity that could drift
- Heavy cross-package dependencies make changes ripple

## Refactoring Opportunities

1. Split `blockpack_executor.go` into query planning, block selection, and result assembly modules
2. Extract common patterns between column_scanner and column_scanner_streaming
3. Consider state machine pattern for execution stages (see `executor-state-machine.md`)

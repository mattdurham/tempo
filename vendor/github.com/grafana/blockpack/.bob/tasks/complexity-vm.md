# Area Complexity: Virtual Machine

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 2 — MODERATE-HIGH

## Location

`internal/vm/` — 4 source files, 7 test files, ~1,699 lines

## Description

Stack-based virtual machine for executing TraceQL bytecode. Implements aggregations, quantiles, histograms, and streaming execution. Compact but extremely dense — high logic per line.

## Complexity Indicators

- **151 switch/case statements** — bytecode dispatch loop
- Stack machine with complex aggregation state
- Merge logic for distributed aggregations
- Quantile sketch algorithms
- Regex and JSONPath caching
- Attribute provider abstraction

## Key Files

| File | Size | Notes |
|------|------|-------|
| `traceql_compiler.go` | ~28KB | Bytecode generation |
| `vm.go` | ~13KB | Interpreter loop / dispatch |

## Key Risks

- Very dense code — small changes can have outsized impact
- Bytecode dispatch correctness is critical for all query results
- Aggregation merge logic must be semantically correct for distributed queries
- Stack machine state can be hard to debug

## Refactoring Opportunities

1. Consider splitting bytecode generation from optimization in the compiler
2. Extract aggregation logic into its own module
3. Add bytecode disassembler for debugging

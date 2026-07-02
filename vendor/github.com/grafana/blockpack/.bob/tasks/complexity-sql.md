# Area Complexity: SQL Compiler

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 2 — HIGH

## Location

`internal/sql/` — 11 source files, 15 test files, ~4,375 lines

## Description

SQL to TraceQL translation layer. Compiles SQL SELECT statements to VM bytecode programs for execution. Uses PostgreSQL parser library (pg_query_go) for SQL parsing, then translates the AST to TraceQL VM instructions.

## Complexity Indicators

- **350 switch/case statements** — heavy pattern matching throughout
- AST traversal and transformation logic
- Query optimization and predicate extraction
- Two compilation paths: column-based and streaming
- Complex closure generation for runtime predicates
- Handles aggregations, GROUP BY, structural queries

## Key Files

| File | Lines | Notes |
|------|-------|-------|
| `program_compiler.go` | 2,088 | 2nd largest file in codebase — core compilation logic |

## Key Risks

- `program_compiler.go` at 2,088 lines concentrates most of the complexity
- Two compilation paths (column-based vs streaming) could drift out of sync
- AST transformation bugs can produce subtle incorrect query results
- Closure generation makes debugging difficult

## Refactoring Opportunities

1. Split `program_compiler.go` into compilation phases (parse, optimize, emit)
2. Extract shared logic between column and streaming compilation paths
3. Add intermediate representation (IR) between SQL AST and VM bytecode for better testability

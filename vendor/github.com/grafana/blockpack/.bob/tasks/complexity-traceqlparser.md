# Area Complexity: TraceQL Parser

**Status:** Reference
**Created:** 2026-02-15
**Complexity Tier:** 3 — MODERATE

## Location

`internal/traceqlparser/` — 1 source file, 3 test files, ~1,393 lines

## Description

Recursive descent parser converting TraceQL query syntax into AST for compilation by the SQL or VM compilers.

## Complexity Indicators

- Single monolithic 1,393-line parser file
- Recursive descent with operator precedence parsing
- Handles filter expressions, metrics queries, structural queries
- Brace/parenthesis depth tracking
- String literal escaping and normalization

## Key Files

| File | Lines | Notes |
|------|-------|-------|
| `parser.go` | 1,393 | Entire parser in one file |

## Key Risks

- All complexity concentrated in a single file
- Recursive descent parsers can be fragile when adding new syntax
- Edge cases in string escaping and operator precedence

## Refactoring Opportunities

1. Consider splitting into lexer + parser if the language grows
2. Extract operator precedence climbing into a reusable pattern
3. Add fuzzing tests for parser robustness

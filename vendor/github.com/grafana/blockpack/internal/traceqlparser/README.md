# TraceQL Parser

Parses TraceQL query strings into an AST and converts filter expressions to SQL WHERE clauses.

## Purpose

This package implements a recursive-descent parser for the TraceQL query language used by Grafana Tempo. It handles three query types: filter expressions (`{ .foo = "bar" }`), metrics queries (`{ filter } | rate() by (field)`), and structural queries (`{ .parent } >> { .child }`). The parser produces typed AST nodes that downstream packages (the VM compiler and query executor) consume for query planning and execution.

## Key Types

- `FilterExpression` -- Root AST node wrapping an `Expr` tree. Represents `{ ... }` blocks.
- `MetricsQuery` -- Filter + pipeline stage. Represents `{ filter } | aggregate() by (...)`.
- `StructuralQuery` -- Two filter expressions joined by a `StructuralOp` (`>>`, `>`, `~`, `<<`, `<`, `!~`).
- `BinaryExpr` -- Binary operation node (comparisons and logical `&&`/`||`).
- `FieldExpr` -- Attribute or intrinsic field reference with optional `Scope` (span, resource, event, link, instrumentation, trace).
- `LiteralExpr` -- Typed literal value (string, int64, float64, bool, duration, status, kind, nil).
- `PipelineStage` -- Aggregate function + GROUP BY fields for metrics queries.
- `AggregateFunc` -- Metrics aggregate (rate, count_over_time, avg, min, max, sum, stddev, quantile_over_time, histogram_over_time).
- `BinaryOp` -- Enum: `OpAnd`, `OpOr`, `OpEq`, `OpNeq`, `OpGt`, `OpGte`, `OpLt`, `OpLte`, `OpRegex`, `OpNotRegex`.
- `StructuralOp` -- Enum: `OpDescendant`, `OpChild`, `OpSibling`, `OpAncestor`, `OpParent`, `OpNotSibling`.
- `LiteralType` -- Enum: `LitString`, `LitInt`, `LitFloat`, `LitBool`, `LitDuration`, `LitStatus`, `LitKind`, `LitNil`.

## Key Functions

- `ParseTraceQL(query string) (interface{}, error)` -- Entry point. Returns `*FilterExpression`, `*MetricsQuery`, or `*StructuralQuery`.
- `(*FilterExpression).ToSQL() (string, error)` -- Converts the AST to a SQL WHERE clause string.

## Data Flow

1. **Input**: Raw TraceQL query string from the public API or VM compiler.
2. `ParseTraceQL` dispatches to `parseMetricsQuery`, `parseStructuralQuery`, or `parseFilterExpression`.
3. **Output**: Typed AST consumed by `internal/vm` (TraceQL compiler, metrics compiler) and `api.go`.
4. `ToSQL()` converts filter ASTs to SQL strings (used for query execution against columnar data).

## Design Decisions

1. **Single-file implementation** -- All parsing, AST types, and SQL conversion live in `parser.go`. This keeps the package self-contained with zero internal dependencies.
2. **Recursive descent with precedence** -- Operator precedence is encoded in the call chain: `parseOr` > `parseAnd` > `parseComparison` > `parsePrimary`.
3. **Unscoped attribute expansion** -- `.foo = "bar"` expands to `("resource.foo" = 'bar' OR "span.foo" = 'bar')` in SQL, matching TraceQL semantics.
4. **Standalone field references** -- `{ .foo }` implicitly becomes `{ .foo != nil }` (field existence check).
5. **Duration stored as nanoseconds** -- All duration literals are converted to `int64` nanoseconds at parse time.
6. **Status/Kind as string enums** -- Stored as string values in the AST but mapped to numeric values in SQL output.
7. **Nil semantics** -- `!= nil` maps to `IS NOT NULL`; `!= value` maps to `(IS NOT NULL AND != value)` to match TraceQL three-valued logic.

## Invariants

- `ParseTraceQL` always returns exactly one of `*FilterExpression`, `*MetricsQuery`, `*StructuralQuery`, or a non-nil error. Do not add new return types without updating all callers.
- All `Expr` implementations must have the `exprNode()` marker method. Do not create Expr types without it.
- `BinaryOp` and `StructuralOp` iota ordering must not change -- downstream code may serialize these values.
- `ToSQL()` must produce valid SQL for all valid ASTs. Null-check wrapping (`IS NOT NULL AND ...`) on `OpNeq` and `OpNotRegex` is intentional and must not be removed.
- The intrinsic map in `fieldExprToSQL` must stay in sync with Tempo's intrinsic field names. Do not add intrinsics without also updating `isIntrinsic()`.
- Nested structural queries are currently rejected. Do not silently flatten them.

## Extending

- **New comparison operator**: Add to `BinaryOp` iota, update `BinaryOp.String()`, add case in `parseComparison`, and add SQL mapping in `binaryExprToSQL`.
- **New aggregate function**: Add case to `parseAggregateFunc` switch statement.
- **New structural operator**: Add to `StructuralOp` iota, update `StructuralOp.String()`, and add case in `parseStructuralQuery`.
- **New intrinsic field**: Add to both `intrinsicMap` in `fieldExprToSQL` and `isIntrinsic()`.
- **New scope**: Add to `validScopes` map in `parseFieldPath`.

## Testing

```bash
go test ./internal/traceqlparser/
```

## Package Dependencies

- **Imports**: `fmt`, `strconv`, `strings` (standard library only -- no internal dependencies)
- **Used by**: `internal/vm` (traceql_compiler.go, metrics_compiler.go), `api.go`, `benchmark/`

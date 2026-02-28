# vm

Bytecode virtual machine and query compiler for TraceQL and metrics queries against blockpack columnar data.

## Purpose

The vm package compiles TraceQL filter and metrics expressions into executable programs and provides the runtime types shared across the query pipeline. It serves two roles: (1) a TraceQL-to-closure compiler that generates `ColumnPredicate` functions for fast bulk column scanning, and (2) a stack-based bytecode VM for row-level expression evaluation. The closure path is the primary execution model used by `internal/executor`; the bytecode VM exists for flexibility but is secondary.

This package also defines the core shared types (`Program`, `QueryPredicates`, `Value`, `RowSet`, `ColumnDataProvider`, `AggregationPlan`, `QuerySpec`) that form the contract between the compiler, executor, and storage layers.

## Key Types

- **`Program`** -- Compiled query output. Contains `ColumnPredicate` (closure for bulk scans), `StreamingColumnPredicate` (zero-alloc streaming variant), `Predicates` (extracted for block pruning), `AggregationPlan` (GROUP BY + aggregates), `Instructions` (bytecode), `Constants`, `Attributes`, `Regexes`.
- **`VM`** -- Stack-based virtual machine. Executes `Instructions` with an `AttributeProvider` for attribute lookups.
- **`Value`** -- Runtime value with `ValueType` discriminant (`TypeNil`, `TypeInt`, `TypeFloat`, `TypeString`, `TypeBool`, `TypeDuration`, `TypeBytes`, `TypeArray`, `TypeJSON`).
- **`QueryPredicates`** -- Extracted filter predicates for block-level pruning: `AttributeEquals`, `DedicatedColumns`, `DedicatedColumnsRegex`, `DedicatedRanges`, `AttributeRanges`, `HasOROperations`.
- **`ColumnDataProvider`** -- Interface for bulk column operations (ScanEqual, ScanRegex, Union, Intersect, etc.). Implemented by `executor.BlockColumnProvider`.
- **`RowSet`** -- Interface for sets of matching row indices (Add, Contains, ToSlice, Size). Implemented by `executor.RowSet`.
- **`AggregationPlan`** -- Specifies GROUP BY fields and `[]AggSpec` (function + field + alias).
- **`AggBucket`** -- Mutable aggregation state for a single group (Count, Sum, Min, Max, Quantiles, Histograms).
- **`QuerySpec`** -- Canonical intermediate representation for metrics queries (`FilterSpec` + `AggregateSpec` + `TimeBucketSpec`). Used for semantic matching with pre-computed streams.
- **`RangePredicate`** -- Min/max bounds with inclusive flags for range constraints on columns.

## Key Functions

- **`CompileTraceQLFilter(filter *traceqlparser.FilterExpression) (*Program, error)`** -- Compiles a TraceQL filter to both `ColumnPredicate` and bytecode `Instructions`. Extracts `QueryPredicates` for block pruning.
- **`CompileTraceQLMetrics(query string, startTime, endTime int64) (*Program, *QuerySpec, error)`** -- Compiles a TraceQL metrics query (e.g., `{} | count() by (service.name)`) to a `Program` + `QuerySpec`.
- **`NewVM(program *Program) *VM`** -- Creates a VM instance for bytecode execution.
- **`MergeAggregationResults(results ...map[string]*AggBucket) map[string]*AggBucket`** -- Merges aggregation buckets from multiple blockpack files.
- **`(*AggBucket).Merge(other *AggBucket)`** -- Merges another bucket into this one, correctly handling quantile sketch merging.
- **`(*QuerySpec).Normalize()`** -- Converts QuerySpec to canonical form (sorts GROUP BY, normalizes field names).

## Data Flow

```
TraceQL string
  -> traceqlparser.ParseTraceQL() -> AST (FilterExpression or MetricsQuery)
  -> CompileTraceQLFilter / CompileTraceQLMetrics
     -> traceqlCompiler:
        1. compileExpression -> Instructions (bytecode for VM)
        2. compileColumnPredicate -> ColumnPredicate closure (for executor)
        3. extractTraceQLPredicates -> QueryPredicates (for block pruning)
     -> Program { ColumnPredicate, Predicates, Instructions, AggregationPlan }

executor.BlockpackExecutor receives Program:
  - Uses Program.Predicates for block selection (selectBlocksWithPlan)
  - Uses Program.ColumnPredicate(BlockColumnProvider) for row filtering
  - Uses Program.AggregationPlan for aggregation queries
```

## Design Decisions

1. **Closure-based execution over bytecode** -- `ColumnPredicate` closures call `ColumnDataProvider.ScanEqual/ScanRegex/etc.` directly, enabling bulk column scans. The bytecode VM executes row-by-row and is slower for columnar data. Both are generated during compilation; the closure path is always preferred by the executor.
2. **Predicates extracted at compile time** -- `extractTraceQLPredicates` walks the AST once and produces `QueryPredicates`. The executor uses these for block pruning without re-parsing the query. OR operations set `HasOROperations` to disable unsafe AND-only pruning optimizations.
3. **ColumnDataProvider as the abstraction boundary** -- The VM defines the interface; the executor implements it (`BlockColumnProvider`). This prevents import cycles and allows the VM to remain storage-agnostic.
4. **QuerySpec as canonical IR** -- Metrics queries normalize to `QuerySpec` for deterministic matching with pre-computed metric streams. Field names are canonicalized (e.g., `duration` -> `span:duration`), GROUP BY fields sorted.
5. **Streaming predicates** -- `StreamingColumnPredicate` calls a `RowCallback` for each match, avoiding `RowSet` allocation. Used by the aggregation path in executor.
6. **Hex bytes handling** -- `trace:id` and `span:id` literals are hex-decoded to `[]byte` at compile time for correct byte-level comparison in storage.
7. **Computed field exclusion** -- Fields like `span.leaf`, `span.root`, `span.childCount` are excluded from predicate extraction because they don't exist as stored columns.

## Invariants

- `ColumnPredicate` and `StreamingColumnPredicate` MUST NOT be called concurrently on the same `ColumnDataProvider`. The provider is not thread-safe.
- `QueryPredicates.HasOROperations` MUST be set to `true` whenever an OR node is encountered. The executor relies on this to disable intersection-based block pruning.
- `CompileTraceQLFilter` MUST generate both `ColumnPredicate` and `Instructions`. The executor uses `ColumnPredicate`; the bytecode path is a fallback.
- `ColumnDataProvider.Union/Intersect/Complement` operate on `RowSet` values returned by the SAME provider instance. Mixing RowSets from different providers panics.
- `AggBucket.Merge` MUST clone quantile sketches on first merge to avoid aliasing between files.
- DO NOT add import dependencies on `internal/executor` from this package. The dependency flows one way: executor imports vm.
- `normalizeAttributePath` in `traceql_compiler.go` MUST stay consistent with `attributePathToColumnName` in `executor/attribute_mapping.go`. Divergence causes silent query failures.
- Regex operators (`=~`, `!~`) in bytecode always return `false`. Regex evaluation only works through the `ColumnPredicate` closure path.

## Extending

- **New TraceQL operator**: Add case in `compileColumnPredicateBinary` for the closure path, `compileBinaryExpr` for bytecode, and `extractTraceQLPredicatesRecursive` for predicate extraction. Add corresponding `Scan*` method to `ColumnDataProvider` interface (which requires updating `executor.BlockColumnProvider`).
- **New aggregation function**: Add `Agg*` constant to the `AggFunction` enum, update `String()`, add case in `compilePipelineToSpec` for metrics compilation, and add `FuncName*` constant in `query_spec.go`.
- **New value type**: Add `Type*` constant to `ValueType`, update `Value.Data` documentation, update `compareValues`/`valuesEqual`/`compareNumeric` in `traceql_compiler.go`.
- **New intrinsic field**: Add to `normalizeAttributePath` switch, `isBuiltInField`, and `normalizeFieldName` in `query_spec.go`.

## Testing

```bash
go test ./internal/vm/
go test -run TestCompile ./internal/vm/
go test -run TestQuerySpec ./internal/vm/
```

## Package Dependencies

- **Imports**: `internal/traceqlparser` (AST types), `internal/quantile` (QuantileSketch), `github.com/theory/jsonpath`
- **Used by**: `internal/executor` (primary consumer), `internal/blockio/writer` (AggregationPlan for writer aggregates), `internal/blockio` (aggregate parser), `api.go`, `benchmark/`

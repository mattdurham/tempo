package vm

import (
	"fmt"

	"github.com/grafana/blockpack/internal/traceqlparser"
)

// CompileTraceQLMetrics compiles a TraceQL metrics query directly to VM bytecode.
// Returns (Program, QuerySpec, error) where:
// - Program: VM bytecode for execution
// - QuerySpec: IR for semantic matching with pre-computed streams
//
// Example queries:
//
//	{} | count()
//	{ status = "error" } | count() by (service.name)
//	{ } | avg(duration) by (service.name)
func CompileTraceQLMetrics(query string, startTime, endTime int64) (*Program, *QuerySpec, error) {
	// Parse TraceQL to AST
	result, err := traceqlparser.ParseTraceQL(query)
	if err != nil {
		return nil, nil, fmt.Errorf("TraceQL parse error: %w", err)
	}

	// Type assert to MetricsQuery
	metricsQuery, ok := result.(*traceqlparser.MetricsQuery)
	if !ok {
		return nil, nil, fmt.Errorf(
			"expected MetricsQuery, got %T (use CompileTraceQLFilter for filter queries)",
			result,
		)
	}

	// Compile to QuerySpec for semantic matching
	stepSize := int64(60 * 1e9) // 60 seconds in nanoseconds
	timeBucket := TimeBucketSpec{
		Enabled:       true,
		StartTime:     startTime,
		EndTime:       endTime,
		StepSizeNanos: stepSize,
	}

	spec, err := compileToQuerySpec(metricsQuery, timeBucket)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile to QuerySpec: %w", err)
	}

	// Compile to VM Program with a match-all filter program for row scanning.
	// Use extractTraceQLPredicates for block-level pruning (same path as filter queries).
	// NOTE-070: also compile ColumnPredicate for block-scan row-level filtering.
	// The intrinsic fast path uses program.Predicates directly; the block scan path uses
	// program.ColumnPredicate. Without this, filters like {kind=server && sc>=400} that
	// fall back to block scan (because BlockRefsFromIntrinsicTOC cannot evaluate range
	// predicates on dict columns) return match-all results — effectively ignoring the filter.
	program := compileMatchAllProgram()
	if metricsQuery.Filter != nil && metricsQuery.Filter.Expr != nil {
		program.Predicates = extractTraceQLPredicates(metricsQuery.Filter.Expr)
		compiler := &traceqlCompiler{program: program}
		columnPredicate, compileErr := compiler.compileColumnPredicate(metricsQuery.Filter.Expr)
		if compileErr != nil {
			return nil, nil, fmt.Errorf("compile column predicate: %w", compileErr)
		}
		program.ColumnPredicate = columnPredicate
	}

	return program, spec, nil
}

// compileToQuerySpec compiles a TraceQL metrics query to QuerySpec IR.
func compileToQuerySpec(metricsQuery *traceqlparser.MetricsQuery, timeBucket TimeBucketSpec) (*QuerySpec, error) {
	if metricsQuery == nil {
		return nil, fmt.Errorf("metrics query cannot be nil")
	}

	spec := &QuerySpec{
		TimeBucketing: timeBucket,
	}

	// Set IsMatchAll when no filter expression is present.
	if metricsQuery.Filter == nil || metricsQuery.Filter.Expr == nil {
		spec.Filter.IsMatchAll = true
	}

	// Compile pipeline to AggregateSpec
	if metricsQuery.Pipeline == nil {
		return nil, fmt.Errorf("metrics query must have a pipeline")
	}
	aggSpec, err := compilePipelineToSpec(metricsQuery.Pipeline)
	if err != nil {
		return nil, fmt.Errorf("failed to compile pipeline: %w", err)
	}
	spec.Aggregate = aggSpec

	// Normalize to canonical form for matching
	spec.Normalize()
	return spec, nil
}

// compilePipelineToSpec maps TraceQL aggregates to AggregateSpec.
func compilePipelineToSpec(pipeline *traceqlparser.PipelineStage) (AggregateSpec, error) {
	if pipeline == nil {
		return AggregateSpec{}, fmt.Errorf("pipeline cannot be nil")
	}

	spec := AggregateSpec{
		GroupBy: pipeline.By,
	}

	// Map TraceQL aggregate function to canonical form
	switch pipeline.Aggregate.Name {
	case "rate":
		spec.Function = FuncNameRATE
		spec.Field = ""

	case "count_over_time":
		spec.Function = FuncNameCOUNT
		spec.Field = ""

	case "avg":
		spec.Function = FuncNameAVG
		spec.Field = pipeline.Aggregate.Field

	case "min":
		spec.Function = FuncNameMIN
		spec.Field = pipeline.Aggregate.Field

	case "max":
		spec.Function = FuncNameMAX
		spec.Field = pipeline.Aggregate.Field

	case "sum":
		spec.Function = FuncNameSUM
		spec.Field = pipeline.Aggregate.Field

	case "quantile_over_time":
		spec.Function = FuncNameQUANTILE
		spec.Field = pipeline.Aggregate.Field
		spec.Quantile = pipeline.Aggregate.Quantile

	case "stddev":
		spec.Function = FuncNameSTDDEV
		spec.Field = pipeline.Aggregate.Field

	case "histogram_over_time":
		spec.Function = FuncNameHISTOGRAM
		spec.Field = pipeline.Aggregate.Field

	default:
		return AggregateSpec{}, fmt.Errorf("unsupported aggregate function: %s", pipeline.Aggregate.Name)
	}

	// Validate field is provided for functions that require it
	if spec.Field == "" && spec.Function != FuncNameRATE && spec.Function != FuncNameCOUNT {
		return AggregateSpec{}, fmt.Errorf("%s requires a field argument", pipeline.Aggregate.Name)
	}

	return spec, nil
}

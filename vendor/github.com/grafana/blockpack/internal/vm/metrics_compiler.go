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

	// Compile to VM Program. When a filter is present, compile a real ColumnPredicate
	// for row-level filtering (same as CompileTraceQLFilter). When no filter, use
	// match-all so {} queries scan all rows.
	// NOTE-071: ColumnPredicate must be compiled from the filter expression — FullScan()
	// is only valid for match-all ({}) queries.
	var program *Program
	if metricsQuery.Filter != nil && metricsQuery.Filter.Expr != nil {
		compiler := &traceqlCompiler{program: &Program{}}
		cp, err := compiler.compileColumnPredicate(metricsQuery.Filter.Expr)
		if err != nil {
			return nil, nil, fmt.Errorf("compile column predicate: %w", err)
		}
		program = compiler.program
		program.ColumnPredicate = cp
		program.Predicates = extractTraceQLPredicates(metricsQuery.Filter.Expr)
		program.ComputeWantColumns()
	} else {
		program = compileMatchAllProgram()
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

// MetricsShapeIsVIAnswerable reports whether a compiled metrics query's SHAPE (aggregate
// function + group-by) is one the value-index metrics engine (executor.ExecuteTraceMetricsFromVI)
// can answer at all — the exact same static gate that function itself applies, extracted here so
// both it and a plan-time caller outside this module (blockpack's public
// CompileTraceQLMetricsFilter, issue #487 holistic-review Issue 2/B) share one rule rather than
// two independently-maintained copies that could drift.
//
// This answers ONLY the static, compile-time-knowable half of ExecuteTraceMetricsFromVI's own
// decline conditions (currently: only COUNT/RATE, no group-by). It does NOT — and cannot —
// predict that function's remaining, genuinely per-execution declines (no value-index source,
// a legacy block with per-span TimeSec == 0, a canceled context, or an unresolvable filter
// leaf), which depend on the source/context/matched-span data unavailable at compile time.
func MetricsShapeIsVIAnswerable(spec QuerySpec) bool {
	switch spec.Aggregate.Function {
	case FuncNameCOUNT, FuncNameRATE:
	default:
		return false
	}
	return len(spec.Aggregate.GroupBy) == 0
}

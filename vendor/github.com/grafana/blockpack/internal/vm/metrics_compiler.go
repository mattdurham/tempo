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
		return nil, nil, fmt.Errorf("expected MetricsQuery, got %T (use CompileTraceQLFilter for filter queries)", result)
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

	// Compile to VM Program. Start with a match-all filter program, then attach the
	// AggregationPlan built from the QuerySpec so StreamQuery takes the aggregation path.
	program := compileMatchAllProgram()
	program.Predicates = filterSpecToPredicates(spec.Filter)
	aggPlan, err := buildAggregationPlan(spec.Aggregate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to build aggregation plan: %w", err)
	}
	program.AggregationPlan = aggPlan

	return program, spec, nil
}

// buildAggregationPlan converts an AggregateSpec to an AggregationPlan for the VM executor.
func buildAggregationPlan(agg AggregateSpec) (*AggregationPlan, error) {
	var fn AggFunction
	switch agg.Function {
	case FuncNameCOUNT:
		fn = AggCount
	case FuncNameSUM:
		fn = AggSum
	case FuncNameAVG:
		fn = AggAvg
	case FuncNameMIN:
		fn = AggMin
	case FuncNameMAX:
		fn = AggMax
	case FuncNameRATE:
		fn = AggRate
	case FuncNameQUANTILE:
		fn = AggQuantile
	case FuncNameHISTOGRAM:
		fn = AggHistogram
	case FuncNameSTDDEV:
		fn = AggStddev
	default:
		return nil, fmt.Errorf("unsupported aggregation function %q", agg.Function)
	}
	return &AggregationPlan{
		GroupByFields: agg.GroupBy,
		Aggregates: []AggSpec{
			{
				Field:    agg.Field,
				Function: fn,
				Quantile: agg.Quantile,
			},
		},
	}, nil
}

// compileToQuerySpec compiles a TraceQL metrics query to QuerySpec IR.
func compileToQuerySpec(metricsQuery *traceqlparser.MetricsQuery, timeBucket TimeBucketSpec) (*QuerySpec, error) {
	if metricsQuery == nil {
		return nil, fmt.Errorf("metrics query cannot be nil")
	}

	spec := &QuerySpec{
		Filter: FilterSpec{
			AttributeEquals: make(map[string][]interface{}),
			AttributeRanges: make(map[string]*RangeSpec),
		},
		TimeBucketing: timeBucket,
	}

	// Compile filter to FilterSpec
	if metricsQuery.Filter != nil && metricsQuery.Filter.Expr != nil {
		filterSpec, err := compileFilterToSpec(metricsQuery.Filter.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile filter: %w", err)
		}
		spec.Filter = filterSpec
	} else {
		// Empty filter matches all spans
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

// compileFilterToSpec walks the TraceQL filter AST and extracts predicates into FilterSpec.
func compileFilterToSpec(expr traceqlparser.Expr) (FilterSpec, error) {
	spec := FilterSpec{
		AttributeEquals: make(map[string][]interface{}),
		AttributeRanges: make(map[string]*RangeSpec),
		IsMatchAll:      false,
	}

	err := extractPredicates(expr, &spec)
	if err != nil {
		return spec, err
	}

	// If no predicates were extracted, this is effectively a match-all filter
	if len(spec.AttributeEquals) == 0 && len(spec.AttributeRanges) == 0 {
		spec.IsMatchAll = true
	}

	return spec, nil
}

// extractPredicates recursively walks the AST and extracts predicates.
func extractPredicates(expr traceqlparser.Expr, spec *FilterSpec) error {
	switch e := expr.(type) {
	case *traceqlparser.BinaryExpr:
		// Handle logical operators (AND, OR)
		switch e.Op {
		case traceqlparser.OpAnd:
			// For AND, we can extract predicates from both sides
			if err := extractPredicates(e.Left, spec); err != nil {
				return err
			}
			return extractPredicates(e.Right, spec)
		case traceqlparser.OpOr:
			// For OR, we skip for now (complex to extract correctly)
			return nil
		}

		// Handle comparison operators
		field, literal, err := extractFieldAndLiteral(e)
		if err != nil {
			// Not a simple field-literal comparison, skip optimization (not a validation error)
			return nil //nolint:nilerr // Skip optimization if extraction fails
		}

		fieldPath := getFieldPath(field)
		if fieldPath == "" {
			// Skip if field path is empty - defensive guard against nil map access
			return nil
		}

		switch e.Op {
		case traceqlparser.OpEq:
			// Equality predicate
			spec.AttributeEquals[fieldPath] = append(spec.AttributeEquals[fieldPath], literal.Value)

		case traceqlparser.OpNeq:
			// Not equal - we don't extract these as negative predicates
			return nil

		case traceqlparser.OpGt, traceqlparser.OpGte, traceqlparser.OpLt, traceqlparser.OpLte:
			// Range predicate
			rangeSpec := spec.AttributeRanges[fieldPath]
			if rangeSpec == nil {
				rangeSpec = &RangeSpec{}
				spec.AttributeRanges[fieldPath] = rangeSpec
			}

			switch e.Op {
			case traceqlparser.OpGt:
				rangeSpec.MinValue = literal.Value
				rangeSpec.MinInclusive = false
			case traceqlparser.OpGte:
				rangeSpec.MinValue = literal.Value
				rangeSpec.MinInclusive = true
			case traceqlparser.OpLt:
				rangeSpec.MaxValue = literal.Value
				rangeSpec.MaxInclusive = false
			case traceqlparser.OpLte:
				rangeSpec.MaxValue = literal.Value
				rangeSpec.MaxInclusive = true
			}

		case traceqlparser.OpRegex, traceqlparser.OpNotRegex:
			// Regex predicates are not extracted
			return nil
		}

	default:
		// Other expression types are not predicates themselves
		return nil
	}

	return nil
}

// extractFieldAndLiteral extracts field and literal from a comparison expression.
func extractFieldAndLiteral(expr *traceqlparser.BinaryExpr) (*traceqlparser.FieldExpr, *traceqlparser.LiteralExpr, error) {
	// Check if left is field and right is literal
	if field, ok := expr.Left.(*traceqlparser.FieldExpr); ok {
		if literal, ok := expr.Right.(*traceqlparser.LiteralExpr); ok {
			return field, literal, nil
		}
	}

	// Check if right is field and left is literal (reversed)
	if field, ok := expr.Right.(*traceqlparser.FieldExpr); ok {
		if literal, ok := expr.Left.(*traceqlparser.LiteralExpr); ok {
			return field, literal, nil
		}
	}

	return nil, nil, fmt.Errorf("not a field-literal comparison")
}

// getFieldPath returns the normalized field path for a TraceQL field expression.
func getFieldPath(field *traceqlparser.FieldExpr) string {
	if field.Scope == "" {
		// Unscoped field
		return field.Name
	}
	// Scoped field: scope.name (e.g., "span.http.status_code")
	return field.Scope + "." + field.Name
}

// filterSpecToPredicates converts a FilterSpec into *QueryPredicates for block-level pruning.
// Returns nil if the filter matches all spans (no predicates to prune on).
func filterSpecToPredicates(filter FilterSpec) *QueryPredicates {
	if filter.IsMatchAll {
		return nil
	}

	if len(filter.AttributeEquals) == 0 && len(filter.AttributeRanges) == 0 {
		return nil
	}

	preds := &QueryPredicates{
		DedicatedColumns: make(map[string][]Value),
		DedicatedRanges:  make(map[string]*RangePredicate),
	}

	for col, vals := range filter.AttributeEquals {
		vmVals := make([]Value, 0, len(vals))
		for _, v := range vals {
			if vmVal, ok := interfaceToValue(v); ok {
				vmVals = append(vmVals, vmVal)
			}
		}
		if len(vmVals) > 0 {
			preds.DedicatedColumns[col] = vmVals
			preds.AttributesAccessed = append(preds.AttributesAccessed, col)
		}
	}

	for col, rs := range filter.AttributeRanges {
		rp := &RangePredicate{
			MinInclusive: rs.MinInclusive,
			MaxInclusive: rs.MaxInclusive,
		}
		if rs.MinValue != nil {
			if v, ok := interfaceToValue(rs.MinValue); ok {
				rp.MinValue = &v
			}
		}
		if rs.MaxValue != nil {
			if v, ok := interfaceToValue(rs.MaxValue); ok {
				rp.MaxValue = &v
			}
		}
		preds.DedicatedRanges[col] = rp
		preds.AttributesAccessed = append(preds.AttributesAccessed, col)
	}

	if len(preds.DedicatedColumns) == 0 && len(preds.DedicatedRanges) == 0 {
		return nil
	}

	return preds
}

// interfaceToValue converts an interface{} value to a VM Value.
// Returns false if the type is not supported.
func interfaceToValue(v interface{}) (Value, bool) {
	switch t := v.(type) {
	case string:
		return Value{Type: TypeString, Data: t}, true
	case int64:
		return Value{Type: TypeInt, Data: t}, true
	case int:
		return Value{Type: TypeInt, Data: int64(t)}, true
	case float64:
		return Value{Type: TypeFloat, Data: t}, true
	case bool:
		return Value{Type: TypeBool, Data: t}, true
	default:
		return Value{}, false
	}
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

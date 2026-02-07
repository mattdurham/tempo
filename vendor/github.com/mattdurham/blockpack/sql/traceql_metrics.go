package sql

import (
	"fmt"

	"github.com/mattdurham/blockpack/traceql"
)

// CompileTraceQLMetricsToSpec compiles a TraceQL metrics query to QuerySpec IR.
// This allows semantic matching with SQL queries and routing to pre-computed metric streams.
func CompileTraceQLMetricsToSpec(metricsQuery *traceql.MetricsQuery, timeBucket TimeBucketSpec) (*QuerySpec, error) {
	if metricsQuery == nil {
		return nil, fmt.Errorf("metrics query cannot be nil")
	}

	spec := &QuerySpec{
		Filter:        FilterSpec{AttributeEquals: make(map[string][]interface{}), AttributeRanges: make(map[string]*RangeSpec)},
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
// It extracts equality predicates into AttributeEquals and range predicates into AttributeRanges.
func compileFilterToSpec(expr traceql.Expr) (FilterSpec, error) {
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
func extractPredicates(expr traceql.Expr, spec *FilterSpec) error {
	switch e := expr.(type) {
	case *traceql.BinaryExpr:
		// Handle logical operators (AND, OR)
		switch e.Op {
		case traceql.OpAnd:
			// For AND, we can extract predicates from both sides
			if err := extractPredicates(e.Left, spec); err != nil {
				return err
			}
			return extractPredicates(e.Right, spec)
		case traceql.OpOr:
			// For OR, we can only extract if both sides are equality checks on the same field
			// Example: status = error || status = ok
			// This is more complex, so for now we skip OR predicates
			// TODO: Handle OR predicates for same field
			return nil
		}

		// Handle comparison operators
		field, literal, err := extractTraceQLFieldAndLiteral(e)
		if err != nil {
			// Not a simple field-literal comparison, skip
			return nil
		}

		fieldPath := getTraceQLFieldPath(field)
		if fieldPath == "" {
			return nil
		}
		normalizedValue := normalizeTraceQLLiteral(fieldPath, literal)

		switch e.Op {
		case traceql.OpEq:
			// Equality predicate
			spec.AttributeEquals[fieldPath] = append(spec.AttributeEquals[fieldPath], normalizedValue)

		case traceql.OpNeq:
			// Not equal - we don't extract these as they're negative predicates
			// TODO: Consider adding negative predicates support
			return nil

		case traceql.OpGt, traceql.OpGte, traceql.OpLt, traceql.OpLte:
			// Range predicate
			rangeSpec := spec.AttributeRanges[fieldPath]
			if rangeSpec == nil {
				rangeSpec = &RangeSpec{}
				spec.AttributeRanges[fieldPath] = rangeSpec
			}

			switch e.Op {
			case traceql.OpGt:
				rangeSpec.MinValue = normalizedValue
				rangeSpec.MinInclusive = false
			case traceql.OpGte:
				rangeSpec.MinValue = normalizedValue
				rangeSpec.MinInclusive = true
			case traceql.OpLt:
				rangeSpec.MaxValue = normalizedValue
				rangeSpec.MaxInclusive = false
			case traceql.OpLte:
				rangeSpec.MaxValue = normalizedValue
				rangeSpec.MaxInclusive = true
			}

		case traceql.OpRegex, traceql.OpNotRegex:
			// Regex predicates are not extracted (too complex for simple matching)
			return nil
		}

	default:
		// Other expression types (field refs, literals) are not predicates themselves
		return nil
	}

	return nil
}

func normalizeTraceQLLiteral(fieldPath string, literal *traceql.LiteralExpr) interface{} {
	if literal == nil {
		return nil
	}

	return normalizeEnumComparisonValue(fieldPath, literal.Value)
}

// extractTraceQLFieldAndLiteral extracts the field and literal from a TraceQL comparison expression.
// Returns (field, literal, nil) if successful, or (nil, nil, error) if not a simple comparison.
func extractTraceQLFieldAndLiteral(expr *traceql.BinaryExpr) (*traceql.FieldExpr, *traceql.LiteralExpr, error) {
	// Check if left is field and right is literal
	if field, ok := expr.Left.(*traceql.FieldExpr); ok {
		if literal, ok := expr.Right.(*traceql.LiteralExpr); ok {
			return field, literal, nil
		}
	}

	// Check if right is field and left is literal (reversed)
	if field, ok := expr.Right.(*traceql.FieldExpr); ok {
		if literal, ok := expr.Left.(*traceql.LiteralExpr); ok {
			return field, literal, nil
		}
	}

	return nil, nil, fmt.Errorf("not a field-literal comparison")
}

// getTraceQLFieldPath returns the normalized field path for a TraceQL field expression.
func getTraceQLFieldPath(field *traceql.FieldExpr) string {
	if field.Scope == "" {
		// Unscoped field
		return field.Name
	}
	// Scoped field: scope.name (e.g., "span.http.status_code")
	return field.Scope + "." + field.Name
}

// compilePipelineToSpec maps TraceQL aggregates to AggregateSpec.
func compilePipelineToSpec(pipeline *traceql.PipelineStage) (AggregateSpec, error) {
	if pipeline == nil {
		return AggregateSpec{}, fmt.Errorf("pipeline cannot be nil")
	}

	spec := AggregateSpec{
		GroupBy: pipeline.By,
	}

	// Map TraceQL aggregate function to canonical form
	switch pipeline.Aggregate.Name {
	case "rate":
		spec.Function = "RATE"
		spec.Field = ""

	case "count_over_time":
		spec.Function = "COUNT"
		spec.Field = ""

	case "avg":
		spec.Function = "AVG"
		spec.Field = pipeline.Aggregate.Field

	case "min":
		spec.Function = "MIN"
		spec.Field = pipeline.Aggregate.Field

	case "max":
		spec.Function = "MAX"
		spec.Field = pipeline.Aggregate.Field

	case "sum":
		spec.Function = "SUM"
		spec.Field = pipeline.Aggregate.Field

	case "quantile_over_time":
		spec.Function = "QUANTILE"
		spec.Field = pipeline.Aggregate.Field
		spec.Quantile = pipeline.Aggregate.Quantile

	case "stddev":
		spec.Function = "STDDEV"
		spec.Field = pipeline.Aggregate.Field

	case "histogram_over_time":
		spec.Function = "HISTOGRAM"
		spec.Field = pipeline.Aggregate.Field

	default:
		return AggregateSpec{}, fmt.Errorf("unsupported aggregate function: %s", pipeline.Aggregate.Name)
	}

	// Validate field is provided for functions that require it
	if spec.Field == "" && spec.Function != "RATE" && spec.Function != "COUNT" {
		return AggregateSpec{}, fmt.Errorf("%s requires a field argument", pipeline.Aggregate.Name)
	}

	return spec, nil
}

package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

// compileSQLToSpecInternal is the internal implementation of CompileSQLToSpec
// that works with concrete tree.Select types.
func compileSQLToSpecInternal(stmt interface{}, timeBucket TimeBucketSpec) (*QuerySpec, error) {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		return nil, fmt.Errorf("expected *tree.Select, got %T", stmt)
	}

	selectClause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok {
		return nil, fmt.Errorf("unsupported SELECT type: %T", selectStmt.Select)
	}

	spec := &QuerySpec{
		Filter: FilterSpec{
			AttributeEquals: make(map[string][]interface{}),
			AttributeRanges: make(map[string]*RangeSpec),
		},
		TimeBucketing: timeBucket,
	}

	// Extract filter from WHERE clause
	if selectClause.Where != nil {
		filterSpec, err := compileSQLWhereToSpec(selectClause.Where.Expr)
		if err != nil {
			return nil, err
		}
		spec.Filter = filterSpec
	} else {
		spec.Filter.IsMatchAll = true
	}

	// Extract aggregates from SELECT clause
	aggSpec, err := compileSQLAggregateToSpec(selectClause)
	if err != nil {
		return nil, err
	}
	spec.Aggregate = aggSpec

	// Normalize the spec for consistent matching
	spec.Normalize()
	return spec, nil
}

// compileSQLWhereToSpec extracts filter predicates from a SQL WHERE clause
// and converts them to FilterSpec format.
func compileSQLWhereToSpec(expr tree.Expr) (FilterSpec, error) {
	filterSpec := FilterSpec{
		AttributeEquals: make(map[string][]interface{}),
		AttributeRanges: make(map[string]*RangeSpec),
		IsMatchAll:      false,
	}

	extractFiltersRecursive(expr, &filterSpec)
	return filterSpec, nil
}

// extractFiltersRecursive walks the WHERE clause expression tree and extracts
// equality and range predicates into the FilterSpec.
func extractFiltersRecursive(expr tree.Expr, filterSpec *FilterSpec) {
	switch e := expr.(type) {
	case *tree.AndExpr:
		// For AND, extract filters from both sides
		extractFiltersRecursive(e.Left, filterSpec)
		extractFiltersRecursive(e.Right, filterSpec)

	case *tree.OrExpr:
		// For OR, we can still extract filters from both sides
		// The OR semantics are preserved by having multiple values for the same attribute
		extractFiltersRecursive(e.Left, filterSpec)
		extractFiltersRecursive(e.Right, filterSpec)

	case *tree.ParenExpr:
		// Unwrap parentheses
		extractFiltersRecursive(e.Expr, filterSpec)

	case *tree.ComparisonExpr:
		// Extract attribute path
		attrPath, err := ExtractAttributePath(e.Left)
		if err != nil {
			return // Skip if we can't extract attribute
		}

		// Handle equality predicates (attribute = constant)
		if e.Operator == tree.EQ {
			// Extract comparison value
			value, err := extractComparisonValue(e.Right)
			if err != nil {
				return // Skip if we can't extract value
			}
			value = normalizeEnumComparisonValue(attrPath, value)

			// Add to AttributeEquals map (OR semantics - multiple values allowed)
			filterSpec.AttributeEquals[attrPath] = append(
				filterSpec.AttributeEquals[attrPath],
				value,
			)
			return
		}

		// Handle IN operator (attribute IN (value1, value2, ...))
		if e.Operator == tree.In {
			// Extract tuple of values
			tuple, ok := e.Right.(*tree.Tuple)
			if !ok {
				return // Skip if right side is not a tuple
			}

			// Extract each value from the tuple
			for _, expr := range tuple.Exprs {
				value, err := extractComparisonValue(expr)
				if err != nil {
					continue // Skip values we can't extract
				}
				value = normalizeEnumComparisonValue(attrPath, value)

				// Add to AttributeEquals map
				filterSpec.AttributeEquals[attrPath] = append(
					filterSpec.AttributeEquals[attrPath],
					value,
				)
			}
			return
		}

		// Handle range predicates (>=, >, <, <=)
		if e.Operator == tree.GE || e.Operator == tree.GT || e.Operator == tree.LT || e.Operator == tree.LE {
			// Extract comparison value
			value, err := extractComparisonValue(e.Right)
			if err != nil {
				return // Skip if we can't extract value
			}
			value = normalizeEnumComparisonValue(attrPath, value)

			// Get or create range spec for this attribute
			rangeSpec := filterSpec.AttributeRanges[attrPath]
			if rangeSpec == nil {
				rangeSpec = &RangeSpec{}
				filterSpec.AttributeRanges[attrPath] = rangeSpec
			}

			// Update range bounds based on operator
			switch e.Operator {
			case tree.GE: // >= (greater than or equal)
				rangeSpec.MinValue = value
				rangeSpec.MinInclusive = true
			case tree.GT: // > (greater than)
				rangeSpec.MinValue = value
				rangeSpec.MinInclusive = false
			case tree.LE: // <= (less than or equal)
				rangeSpec.MaxValue = value
				rangeSpec.MaxInclusive = true
			case tree.LT: // < (less than)
				rangeSpec.MaxValue = value
				rangeSpec.MaxInclusive = false
			}
			return
		}
	}
}

// compileSQLAggregateToSpec extracts aggregate function and GROUP BY fields
// from a SQL SELECT clause and converts them to AggregateSpec format.
func compileSQLAggregateToSpec(selectClause *tree.SelectClause) (AggregateSpec, error) {
	aggSpec := AggregateSpec{
		GroupBy: []string{},
	}

	// Extract GROUP BY fields
	if selectClause.GroupBy != nil {
		for _, groupByExpr := range selectClause.GroupBy {
			attrPath, err := ExtractAttributePath(groupByExpr)
			if err != nil {
				return aggSpec, fmt.Errorf("invalid GROUP BY expression: %w", err)
			}
			aggSpec.GroupBy = append(aggSpec.GroupBy, attrPath)
		}
	}

	// Find aggregate function in SELECT expressions
	for _, selectExpr := range selectClause.Exprs {
		agg, found := findAggregateFunc(selectExpr.Expr)
		if !found {
			continue
		}

		parsedAgg, err := parseSQLAggregateFunc(agg)
		if err != nil {
			return aggSpec, err
		}

		aggSpec.Function = parsedAgg.Function
		aggSpec.Field = parsedAgg.Field
		aggSpec.Quantile = parsedAgg.Quantile
		break
	}

	// If no aggregate function was found, this is an error for QuerySpec
	if aggSpec.Function == "" {
		return aggSpec, fmt.Errorf("no aggregate function found in SELECT clause")
	}

	return aggSpec, nil
}

func parseSQLAggregateFunc(agg *tree.FuncExpr) (AggregateSpec, error) {
	funcName := strings.ToUpper(agg.Func.String())

	if quantile, ok := percentileFromFuncName(funcName); ok {
		field, err := parseAggregateField(agg, funcName)
		if err != nil {
			return AggregateSpec{}, err
		}
		if len(agg.Exprs) != 1 {
			return AggregateSpec{}, fmt.Errorf("%s requires a single field argument", funcName)
		}
		return AggregateSpec{Function: "QUANTILE", Field: field, Quantile: quantile}, nil
	}

	switch funcName {
	case "COUNT":
		return parseAggregateCount(agg, funcName)
	case "RATE":
		return parseAggregateRate(agg, funcName)
	case "AVG", "MIN", "MAX", "SUM", "STDDEV", "HISTOGRAM":
		field, err := parseAggregateField(agg, funcName)
		if err != nil {
			return AggregateSpec{}, err
		}
		return AggregateSpec{Function: funcName, Field: field}, nil
	case "QUANTILE":
		return parseAggregateQuantile(agg)
	default:
		return AggregateSpec{}, fmt.Errorf("unsupported aggregate function: %s", funcName)
	}
}

func parseAggregateField(agg *tree.FuncExpr, funcName string) (string, error) {
	if len(agg.Exprs) == 0 {
		return "", fmt.Errorf("%s requires a field argument", funcName)
	}
	field, err := ExtractAttributePath(agg.Exprs[0])
	if err != nil {
		return "", fmt.Errorf("invalid %s field: %w", funcName, err)
	}
	return field, nil
}

func parseAggregateCount(agg *tree.FuncExpr, funcName string) (AggregateSpec, error) {
	spec := AggregateSpec{Function: funcName}
	if len(agg.Exprs) == 0 {
		return spec, nil
	}
	if _, ok := agg.Exprs[0].(tree.UnqualifiedStar); ok {
		return spec, nil
	}

	field, err := ExtractAttributePath(agg.Exprs[0])
	if err != nil {
		return spec, fmt.Errorf("invalid %s field: %w", funcName, err)
	}
	spec.Field = field
	return spec, nil
}

func parseAggregateRate(agg *tree.FuncExpr, funcName string) (AggregateSpec, error) {
	spec := AggregateSpec{Function: funcName}
	if len(agg.Exprs) == 0 {
		return spec, nil
	}
	if _, ok := agg.Exprs[0].(tree.UnqualifiedStar); ok {
		return spec, nil
	}

	field, err := ExtractAttributePath(agg.Exprs[0])
	if err != nil {
		return spec, fmt.Errorf("invalid %s field: %w", funcName, err)
	}
	spec.Field = field
	return spec, nil
}

func parseAggregateQuantile(agg *tree.FuncExpr) (AggregateSpec, error) {
	if len(agg.Exprs) < 2 {
		return AggregateSpec{}, fmt.Errorf("QUANTILE requires two arguments: field and quantile value")
	}

	field, err := ExtractAttributePath(agg.Exprs[0])
	if err != nil {
		return AggregateSpec{}, fmt.Errorf("invalid QUANTILE field: %w", err)
	}

	numVal, ok := agg.Exprs[1].(*tree.NumVal)
	if !ok {
		return AggregateSpec{}, fmt.Errorf("QUANTILE second argument must be a numeric value")
	}

	quantile, err := strconv.ParseFloat(numVal.String(), 64)
	if err != nil {
		return AggregateSpec{}, fmt.Errorf("invalid quantile value: %w", err)
	}

	if quantile < 0 || quantile > 1 {
		return AggregateSpec{}, fmt.Errorf("quantile value must be between 0 and 1, got %f", quantile)
	}

	return AggregateSpec{Function: "QUANTILE", Field: field, Quantile: quantile}, nil
}

// findAggregateFunc recursively searches for an aggregate function in an expression tree.
// Returns the function expression and true if found, nil and false otherwise.
func findAggregateFunc(expr tree.Expr) (*tree.FuncExpr, bool) {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		// Check if this is an aggregate function
		funcName := strings.ToUpper(e.Func.String())
		if _, ok := percentileFromFuncName(funcName); ok {
			return e, true
		}
		switch funcName {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "RATE", "QUANTILE", "STDDEV", "HISTOGRAM":
			return e, true
		}
		// Not an aggregate, check arguments
		for _, arg := range e.Exprs {
			if agg, found := findAggregateFunc(arg); found {
				return agg, true
			}
		}

	case *tree.ParenExpr:
		return findAggregateFunc(e.Expr)

	case *tree.BinaryExpr:
		if agg, found := findAggregateFunc(e.Left); found {
			return agg, true
		}
		return findAggregateFunc(e.Right)
	}

	return nil, false
}

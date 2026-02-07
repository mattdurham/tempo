package sql

import (
	"fmt"
	"strings"

	"github.com/mattdurham/blockpack/traceql"
	"github.com/mattdurham/blockpack/vm"
)

// CompileTraceQL compiles a TraceQL query string to a VM Program
// TraceQL syntax: { <filter-expression> }
// Example: { name = "hello" && duration > 100s }
// Does NOT support metrics queries - use CompileTraceQLMetricsOrSQL for that
// Does NOT support structural queries - use CompileTraceQLStructural for that
func CompileTraceQL(query string) (*vm.Program, error) {
	// Parse TraceQL to AST
	result, err := traceql.ParseTraceQL(query)
	if err != nil {
		return nil, fmt.Errorf("TraceQL parse error: %w", err)
	}

	// Check if this is a metrics query - not supported by this function
	if _, isMetrics := result.(*traceql.MetricsQuery); isMetrics {
		return nil, fmt.Errorf("metrics queries not supported by CompileTraceQL, use CompileTraceQLMetricsOrSQL")
	}

	// Check if this is a structural query - not supported by this function
	if _, isStructural := result.(*traceql.StructuralQuery); isStructural {
		return nil, fmt.Errorf("structural queries not supported by CompileTraceQL, use CompileTraceQLStructural")
	}

	// Type assert to FilterExpression
	ast, ok := result.(*traceql.FilterExpression)
	if !ok {
		return nil, fmt.Errorf("expected FilterExpression, got %T", result)
	}

	// Convert TraceQL AST to SQL WHERE clause
	var whereClause string
	if ast != nil {
		whereClause, err = ast.ToSQL()
		if err != nil {
			return nil, fmt.Errorf("failed to convert TraceQL to SQL: %w", err)
		}
	}

	// Build SQL SELECT statement
	var sqlQuery string
	if whereClause == "" {
		sqlQuery = "SELECT * FROM spans"
	} else {
		sqlQuery = fmt.Sprintf("SELECT * FROM spans WHERE %s", whereClause)
	}

	// Parse SQL using existing parser
	stmt, err := ParseSQL(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated SQL: %w", err)
	}

	// Compile SQL to VM Program
	program, err := CompileToProgram(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to compile SQL to program: %w", err)
	}

	return program, nil
}

// CompileTraceQLOrSQL compiles either a TraceQL or SQL query
// Tries to detect the query language and parse accordingly.
// If detection fails, tries both parsers and uses whichever succeeds.
//
// Detection heuristics:
// - Empty query -> TraceQL (matches all)
// - Starts with "SELECT" -> SQL
// - Starts with "{" -> TraceQL
// - Otherwise -> Try TraceQL first, then SQL if that fails
func CompileTraceQLOrSQL(query string) (*vm.Program, error) {
	if query == "" {
		// Empty query - match all spans
		return CompileTraceQL("")
	}

	// Heuristic: Check if it's SQL
	if len(query) >= 6 && query[:6] == "SELECT" {
		program, err := compileSQLOnly(query)
		if err != nil {
			// SQL failed, maybe it was a malformed query
			return nil, fmt.Errorf("SQL parse error: %w", err)
		}
		return program, nil
	}

	// Heuristic: Check if it's TraceQL with braces
	if len(query) > 0 && query[0] == '{' {
		program, err := CompileTraceQL(query)
		if err != nil {
			// TraceQL failed, maybe try SQL as fallback
			sqlProgram, sqlErr := compileSQLOnly(query)
			if sqlErr == nil {
				return sqlProgram, nil
			}
			// Both failed, return TraceQL error (more likely to be correct)
			return nil, err
		}
		return program, nil
	}

	// Ambiguous - try TraceQL first, then SQL
	traceqlProgram, traceqlErr := CompileTraceQL(query)
	if traceqlErr == nil {
		return traceqlProgram, nil
	}

	// TraceQL failed, try SQL
	sqlProgram, sqlErr := compileSQLOnly(query)
	if sqlErr == nil {
		return sqlProgram, nil
	}

	// Both failed - return both errors
	return nil, fmt.Errorf("failed to parse as TraceQL: %w; also failed to parse as SQL: %v", traceqlErr, sqlErr)
}

// compileSQLOnly compiles a SQL query without trying TraceQL
func compileSQLOnly(query string) (*vm.Program, error) {
	stmt, err := ParseSQL(query)
	if err != nil {
		return nil, err
	}
	return CompileToProgram(stmt)
}

// CompileTraceQLMetricsOrSQL is a unified entry point that handles all three query types:
// 1. TraceQL metrics queries (with pipe operator): { filter } | aggregate() by (fields)
// 2. SQL aggregate queries: SELECT function(...) FROM spans WHERE ... GROUP BY ...
// 3. TraceQL filter queries: { filter }
//
// Returns:
// - For TraceQL metrics: (Program, QuerySpec, nil)
//   - Compiles to QuerySpec for semantic matching
//   - Translates to SQL and compiles to VM Program for execution
//
// - For SQL aggregates: (Program, QuerySpec, nil)
//   - Compiles to VM Program
//   - Extracts QuerySpec for semantic matching
//
// - For TraceQL filters: (Program, nil, nil)
//   - Uses existing CompileTraceQL flow
//   - No QuerySpec (not an aggregate query)
//
// The QuerySpec enables query routing to pre-computed metric streams when available,
// with fallback to streaming execution using the Program.
func CompileTraceQLMetricsOrSQL(query string, startTime, endTime int64) (*vm.Program, *QuerySpec, error) {
	query = strings.TrimSpace(query)

	// Empty query - TraceQL filter that matches all
	if query == "" {
		program, err := CompileTraceQL("")
		if err != nil {
			return nil, nil, err
		}
		return program, nil, nil
	}

	// Detection heuristic: Check if it's SQL
	if len(query) >= 6 && strings.ToUpper(query[:6]) == "SELECT" {
		return compileSQL(query, startTime, endTime)
	}

	// Parse with TraceQL parser to determine query type
	result, err := traceql.ParseTraceQL(query)
	if err != nil {
		// TraceQL parse failed, maybe it's SQL without SELECT prefix
		sqlProgram, sqlSpec, sqlErr := compileSQL(query, startTime, endTime)
		if sqlErr == nil {
			return sqlProgram, sqlSpec, nil
		}
		// Both failed, return TraceQL error (more likely to be correct)
		return nil, nil, fmt.Errorf("TraceQL parse error: %w", err)
	}

	// Check what type of query was parsed
	switch parsed := result.(type) {
	case *traceql.MetricsQuery:
		// TraceQL metrics query - compile to QuerySpec and SQL Program
		return compileTraceQLMetrics(parsed, startTime, endTime)

	case *traceql.FilterExpression:
		// TraceQL filter query - compile to Program only (no QuerySpec)
		program, err := CompileTraceQL(query)
		if err != nil {
			return nil, nil, err
		}
		return program, nil, nil

	default:
		// Unknown query type
		return nil, nil, fmt.Errorf("unexpected query type: %T", result)
	}
}

// compileTraceQLMetrics compiles a TraceQL metrics query to both QuerySpec and Program.
// Steps:
// 1. Compile to QuerySpec IR for semantic matching
// 2. Translate to SQL for execution
// 3. Compile SQL to VM Program
func compileTraceQLMetrics(metricsQuery *traceql.MetricsQuery, startTime, endTime int64) (*vm.Program, *QuerySpec, error) {
	// Step 1: Compile to QuerySpec for semantic matching
	// For TraceQL metrics queries with _over_time functions, enable time bucketing
	// Default step size: 60 seconds (standard for Tempo/Grafana)
	stepSize := int64(60 * 1e9) // 60 seconds in nanoseconds
	timeBucket := TimeBucketSpec{
		Enabled:       true,
		StartTime:     startTime,
		EndTime:       endTime,
		StepSizeNanos: stepSize,
	}
	querySpec, err := CompileTraceQLMetricsToSpec(metricsQuery, timeBucket)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile to QuerySpec: %w", err)
	}

	// Step 2: Translate to SQL
	sqlQuery, err := TraceQLMetricsToSQL(metricsQuery, startTime, endTime)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to translate to SQL: %w", err)
	}

	// Debug: print generated SQL
	// fmt.Printf("DEBUG: Generated SQL: %s\n", sqlQuery)

	// Step 3: Compile SQL to VM Program
	stmt, err := ParseSQL(sqlQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse generated SQL: %w", err)
	}

	program, err := CompileToProgram(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile SQL to program: %w", err)
	}

	return program, querySpec, nil
}

// compileSQL compiles a SQL query to both Program and QuerySpec (if it's an aggregate query).
func compileSQL(query string, startTime, endTime int64) (*vm.Program, *QuerySpec, error) {
	// Parse SQL
	stmt, err := ParseSQL(query)
	if err != nil {
		return nil, nil, fmt.Errorf("SQL parse error: %w", err)
	}

	// Compile to VM Program
	program, err := CompileToProgram(stmt)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compile SQL: %w", err)
	}

	// Check if this is an aggregate query (has AggregationPlan)
	if program.AggregationPlan != nil {
		// Extract QuerySpec for semantic matching
		timeBucket := TimeBucketSpec{
			Enabled:   false,
			StartTime: startTime,
			EndTime:   endTime,
		}
		querySpec, err := CompileSQLToSpec(stmt, timeBucket)
		if err != nil {
			// If QuerySpec extraction fails, still return the program
			// (it can execute, just can't be matched to pre-computed metric streams)
			return program, nil, nil
		}
		return program, querySpec, nil
	}

	// Not an aggregate query - return program only
	return program, nil, nil
}

// CompileTraceQLStructural compiles a TraceQL structural query string to a StructuralQueryPlan.
// Structural query syntax: { filter1 } <op> { filter2 }
// Operators: >> (descendant), > (child), ~ (sibling), << (ancestor), < (parent), !~ (not sibling)
// Example: { .root = true } >> { .leaf = true }
//
// Returns a StructuralQueryPlan that can be executed with executor.ExecuteStructuralQuery()
func CompileTraceQLStructural(query string) (*StructuralQueryPlan, error) {
	// Parse TraceQL to AST
	result, err := traceql.ParseTraceQL(query)
	if err != nil {
		return nil, fmt.Errorf("TraceQL parse error: %w", err)
	}

	// Type assert to StructuralQuery
	structuralQuery, ok := result.(*traceql.StructuralQuery)
	if !ok {
		return nil, fmt.Errorf("expected StructuralQuery, got %T (use CompileTraceQL for filter queries, CompileTraceQLMetricsOrSQL for metrics)", result)
	}

	// Compile using existing structural compiler
	plan, err := CompileStructuralQuery(structuralQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to compile structural query: %w", err)
	}

	return plan, nil
}

package sql

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/mattdurham/blockpack/vm"
)

// CompileToProgram converts a SQL SELECT statement to a VM Program
func CompileToProgram(stmt *tree.Select) (*vm.Program, error) {
	selectClause, ok := stmt.Select.(*tree.SelectClause)
	if !ok {
		return nil, fmt.Errorf("unsupported SELECT type: %T", stmt.Select)
	}

	// Check if this is an aggregation query (has aggregate functions or GROUP BY)
	if hasAggregateFunctions(selectClause.Exprs) || selectClause.GroupBy != nil {
		return compileAggregationQuery(selectClause)
	}

	// Check for CASE expressions (not yet supported)
	for _, selectExpr := range selectClause.Exprs {
		if _, ok := selectExpr.Expr.(*tree.CaseExpr); ok {
			return nil, fmt.Errorf("CASE expressions not yet supported")
		}
	}

	// Create program
	program := &vm.Program{
		Constants:  []vm.Value{},
		Attributes: []string{},
		Regexes:    []*vm.RegexCache{},
		JSONPaths:  []*vm.JSONPathCache{},
	}

	// Extract attributes from WHERE clause for projection
	var whereAttributes []string
	if selectClause.Where != nil {
		whereAttributes = extractAttributesFromExpr(selectClause.Where.Expr)
	}

	// Generate closure-based column predicate for WHERE clause
	if selectClause.Where != nil {
		predicate, err := compileWhereToColumnPredicate(selectClause.Where.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WHERE closure: %w", err)
		}
		program.ColumnPredicate = predicate
	} else {
		// No WHERE clause means match all spans
		program.ColumnPredicate = func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		}
	}

	// Generate native streaming predicate (no RowSet allocation!)
	if selectClause.Where != nil {
		streamingPredicate, err := compileExprToStreamingPredicate(selectClause.Where.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile streaming predicate: %w", err)
		}
		program.StreamingColumnPredicate = streamingPredicate
	} else {
		// No WHERE clause means match all spans
		program.StreamingColumnPredicate = func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamFullScan(callback)
		}
	}

	// Store attributes for projection (used by tests and search response building)
	// Include ONLY attributes from WHERE clause
	// This matches Tempo's behavior: only project attributes that were actually queried
	//
	// Note: resource.service.name and span:name are still fetched for trace metadata
	// (rootServiceName, rootTraceName) but are NOT projected as attributes unless
	// they appear in the WHERE clause.

	// Deduplicate attributes
	seen := make(map[string]bool)
	uniqueAttrs := make([]string, 0, len(whereAttributes))
	for _, attr := range whereAttributes {
		if !seen[attr] {
			seen[attr] = true
			uniqueAttrs = append(uniqueAttrs, attr)
		}
	}
	program.Attributes = uniqueAttrs

	// Extract predicates directly from WHERE clause for block pruning
	if selectClause.Where != nil {
		program.Predicates = extractPredicatesFromAST(selectClause.Where.Expr)
	} else {
		program.Predicates = &vm.QueryPredicates{
			AttributeEquals:     make(map[string][]vm.Value),
			DedicatedColumns:    make(map[string][]vm.Value),
			UnscopedColumnNames: make(map[string][]string),
			HasOROperations:     false,
		}
	}

	return program, nil
}

// compileAggregationPlan compiles GROUP BY and aggregation functions into an execution plan
// STUBBED: Aggregation not supported

// convertToVMValue converts a Go value from column storage to a VM Value
func convertToVMValue(val interface{}) vm.Value {
	if val == nil {
		return vm.Value{Type: vm.TypeNil, Data: nil}
	}

	switch v := val.(type) {
	case int64:
		return vm.Value{Type: vm.TypeInt, Data: v}
	case uint64:
		// Convert uint64 to int64 for consistency (assuming values fit in int64 range)
		return vm.Value{Type: vm.TypeInt, Data: int64(v)}
	case float64:
		return vm.Value{Type: vm.TypeFloat, Data: v}
	case string:
		return vm.Value{Type: vm.TypeString, Data: v}
	case bool:
		return vm.Value{Type: vm.TypeBool, Data: v}
	case []byte:
		return vm.Value{Type: vm.TypeBytes, Data: v}
	case int:
		return vm.Value{Type: vm.TypeInt, Data: int64(v)}
	case float32:
		return vm.Value{Type: vm.TypeFloat, Data: float64(v)}
	default:
		// Fallback: treat as string
		return vm.Value{Type: vm.TypeString, Data: fmt.Sprintf("%v", v)}
	}
}

// extractAttributesFromExpr recursively extracts all attribute paths from an expression
// isIntrinsicAttribute checks if an attribute is an intrinsic span field
// that should always be available and not limit projection.
func extractAttributesFromExpr(expr tree.Expr) []string {
	var attributes []string
	seen := make(map[string]bool)

	var walk func(tree.Expr)
	walk = func(expr tree.Expr) {
		switch e := expr.(type) {
		case *tree.ComparisonExpr:
			walk(e.Left)
			walk(e.Right)
		case *tree.AndExpr:
			walk(e.Left)
			walk(e.Right)
		case *tree.OrExpr:
			walk(e.Left)
			walk(e.Right)
		case *tree.NotExpr:
			walk(e.Expr)
		case *tree.ParenExpr:
			walk(e.Expr)
		case *tree.UnresolvedName:
			// This is an attribute reference
			attrPath := e.String()
			// Remove quotes if present
			attrPath = strings.Trim(attrPath, "\"")
			// Restore colons from placeholder
			attrPath = strings.ReplaceAll(attrPath, "__COLON__", ":")
			if !seen[attrPath] {
				attributes = append(attributes, attrPath)
				seen[attrPath] = true
			}
		case *tree.FuncExpr:
			// Handle function calls (e.g., LOWER(attr), UPPER(attr))
			for _, arg := range e.Exprs {
				walk(arg)
			}
		}
		// Literals and other types don't contain attributes
	}

	walk(expr)
	return attributes
}

// extractStringLiteral extracts a string literal from an expression
func extractStringLiteral(expr tree.Expr) (string, error) {
	strVal, ok := expr.(*tree.StrVal)
	if !ok {
		return "", fmt.Errorf("expected string literal, got %T", expr)
	}
	return strVal.RawString(), nil
}

// similarToPatternToRegex converts a SQL SIMILAR TO pattern to a regex pattern.
// SIMILAR TO uses SQL standard regex syntax which is similar to POSIX regex:
// - % matches any sequence (like .* in regex)
// - _ matches any single character (like . in regex)
// - Supports regex metacharacters: |, *, +, ?, {m}, {m,n}, [...], etc.
func similarToPatternToRegex(pattern string) string {
	var buf strings.Builder
	buf.WriteByte('^')

	escape := false
	for _, r := range pattern {
		if escape {
			// Escaped character - output literally (escape regex special chars)
			switch r {
			case '.', '*', '+', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
				buf.WriteByte('\\')
			}
			buf.WriteRune(r)
			escape = false
			continue
		}
		if r == '\\' {
			escape = true
			continue
		}
		switch r {
		case '%':
			// % in SIMILAR TO means zero or more of any character
			buf.WriteString(".*")
		case '_':
			// _ in SIMILAR TO means exactly one character
			buf.WriteByte('.')
		default:
			// Pass through regex metacharacters as-is: |, *, +, ?, {, }, [, ], (, )
			// Escape regex special chars that aren't part of SIMILAR TO syntax
			switch r {
			case '.', '^', '$':
				buf.WriteByte('\\')
			}
			buf.WriteRune(r)
		}
	}

	buf.WriteByte('$')
	return buf.String()
}

// compileWhereToColumnPredicate compiles a WHERE clause expression to a closure
// that executes column scans directly, eliminating bytecode parsing overhead.
func compileWhereToColumnPredicate(expr tree.Expr) (vm.ColumnPredicate, error) {
	return compileExprToColumnPredicate(expr)
}

// compileExprToColumnPredicate recursively compiles expressions to closures
func compileExprToColumnPredicate(expr tree.Expr) (vm.ColumnPredicate, error) {
	switch e := expr.(type) {
	case *tree.AndExpr:
		// Compile left and right, then intersect
		left, err := compileExprToColumnPredicate(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := compileExprToColumnPredicate(e.Right)
		if err != nil {
			return nil, err
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			leftRows, err := left(provider)
			if err != nil {
				return nil, err
			}
			rightRows, err := right(provider)
			if err != nil {
				return nil, err
			}
			return provider.Intersect(leftRows, rightRows), nil
		}, nil

	case *tree.OrExpr:
		// Compile left and right, then union
		left, err := compileExprToColumnPredicate(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := compileExprToColumnPredicate(e.Right)
		if err != nil {
			return nil, err
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			leftRows, err := left(provider)
			if err != nil {
				return nil, err
			}
			rightRows, err := right(provider)
			if err != nil {
				return nil, err
			}
			return provider.Union(leftRows, rightRows), nil
		}, nil

	case *tree.NotExpr:
		// Compile inner expression and complement
		inner, err := compileExprToColumnPredicate(e.Expr)
		if err != nil {
			return nil, err
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			innerRows, err := inner(provider)
			if err != nil {
				return nil, err
			}
			return provider.Complement(innerRows), nil
		}, nil

	case *tree.ComparisonExpr:
		return compileComparisonToColumnPredicate(e)

	case *tree.ParenExpr:
		return compileExprToColumnPredicate(e.Expr)

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type: %T", expr)
	}
}

// compileExprToStreamingPredicate recursively compiles expressions to streaming callback closures
// This generates code that calls callbacks directly without building RowSets
func compileExprToStreamingPredicate(expr tree.Expr) (vm.StreamingColumnPredicate, error) {
	switch e := expr.(type) {
	case *tree.AndExpr:
		// Compile left and right to streaming predicates
		left, err := compileExprToStreamingPredicate(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := compileExprToStreamingPredicate(e.Right)
		if err != nil {
			return nil, err
		}

		// AND: Build set from left, stream right and check membership
		// Optimizations:
		// 1. Short-circuit if left returns no matches (AND false = false)
		// 2. Pre-allocate map with hint for better memory efficiency
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			// Collect left matches into a set
			// Use a small initial capacity since most predicates are selective
			leftMatches := make(map[int]struct{}, 1024)
			leftCount := 0
			_, err := left(provider, func(rowIdx int) bool {
				leftMatches[rowIdx] = struct{}{}
				leftCount++
				return true
			})
			if err != nil {
				return 0, err
			}

			// Short-circuit: if left returned no matches, AND result is empty
			if leftCount == 0 {
				return 0, nil
			}

			// Stream through right matches and check if in left set
			count := 0
			_, err = right(provider, func(rowIdx int) bool {
				if _, exists := leftMatches[rowIdx]; exists {
					// Both conditions match
					if !callback(rowIdx) {
						return false
					}
					count++
				}
				return true
			})

			return count, err
		}, nil

	case *tree.OrExpr:
		// Compile left and right to streaming predicates
		left, err := compileExprToStreamingPredicate(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := compileExprToStreamingPredicate(e.Right)
		if err != nil {
			return nil, err
		}

		// OR: Stream through both predicates, use set to avoid duplicates
		// Pre-allocate map with hint for better memory efficiency
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			seen := make(map[int]struct{}, 1024)
			count := 0

			// Stream through left condition matches
			_, err := left(provider, func(rowIdx int) bool {
				if _, exists := seen[rowIdx]; !exists {
					seen[rowIdx] = struct{}{}
					if !callback(rowIdx) {
						return false // User wants to stop
					}
					count++
				}
				return true
			})
			if err != nil {
				return count, err
			}

			// Stream through right condition matches
			_, err = right(provider, func(rowIdx int) bool {
				if _, exists := seen[rowIdx]; !exists {
					seen[rowIdx] = struct{}{}
					if !callback(rowIdx) {
						return false // User wants to stop
					}
					count++
				}
				return true
			})

			return count, err
		}, nil

	case *tree.NotExpr:
		// Compile inner expression to streaming predicate
		inner, err := compileExprToStreamingPredicate(e.Expr)
		if err != nil {
			return nil, err
		}

		// NOT: Build set of matches, then complement
		// Note: This still requires materializing a set, but only of matches (not all rows)
		// Pre-allocate map with hint for better memory efficiency
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			matches := make(map[int]struct{}, 1024)

			// Collect all matching rows
			_, err := inner(provider, func(rowIdx int) bool {
				matches[rowIdx] = struct{}{}
				return true
			})
			if err != nil {
				return 0, err
			}

			// Call callback for all non-matching rows
			count := 0
			for i := 0; i < provider.GetRowCount(); i++ {
				if _, exists := matches[i]; !exists {
					if !callback(i) {
						break
					}
					count++
				}
			}

			return count, nil
		}, nil

	case *tree.ComparisonExpr:
		return compileComparisonToStreamingPredicate(e)

	case *tree.ParenExpr:
		return compileExprToStreamingPredicate(e.Expr)

	default:
		return nil, fmt.Errorf("unsupported WHERE expression type for streaming: %T", expr)
	}
}

// compileComparisonToStreamingPredicate compiles comparison operations to streaming scans
func compileComparisonToStreamingPredicate(cmp *tree.ComparisonExpr) (vm.StreamingColumnPredicate, error) {
	// Extract attribute path
	attrPath, err := ExtractAttributePath(cmp.Left)
	if err != nil {
		return nil, err
	}

	// Check if this is an unscoped attribute
	if isUnscopedAttribute(attrPath) {
		return compileUnscopedComparisonStreaming(cmp, attrPath)
	}

	// Extract column name
	columnName := attributePathToColumnName(attrPath)

	// Handle IS NULL / IS NOT NULL
	if cmp.Right == tree.DNull {
		op := cmp.Operator.String()
		switch op {
		case "IS DISTINCT FROM": // IS NOT NULL
			return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
				return provider.StreamScanIsNotNull(columnName, callback)
			}, nil
		case "IS NOT DISTINCT FROM": // IS NULL
			return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
				return provider.StreamScanIsNull(columnName, callback)
			}, nil
		default:
			return nil, fmt.Errorf("unsupported NULL comparison operator: %s", op)
		}
	}

	// Handle LIKE operator
	if cmp.Operator == tree.Like {
		patternLiteral, err := extractStringLiteral(cmp.Right)
		if err != nil {
			return nil, fmt.Errorf("LIKE pattern must be a string literal: %w", err)
		}
		pattern := patternLiteral
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanRegex(columnName, pattern, callback)
		}, nil
	}

	// Handle ILIKE operator
	if cmp.Operator == tree.ILike {
		patternLiteral, err := extractStringLiteral(cmp.Right)
		if err != nil {
			return nil, fmt.Errorf("ILIKE pattern must be a string literal: %w", err)
		}
		pattern := "(?i)" + patternLiteral
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanRegex(columnName, pattern, callback)
		}, nil
	}

	// Handle IN operator (attribute IN (value1, value2, ...))
	if cmp.Operator == tree.In || cmp.Operator == tree.NotIn {
		// Extract tuple of values
		tuple, ok := cmp.Right.(*tree.Tuple)
		if !ok {
			return nil, fmt.Errorf("IN/NOT IN operator requires a tuple of values")
		}

		if len(tuple.Exprs) == 0 {
			return nil, fmt.Errorf("IN/NOT IN operator requires at least one value")
		}

		// Extract each value from the tuple
		var values []interface{}
		for _, expr := range tuple.Exprs {
			value, err := extractComparisonValue(expr)
			if err != nil {
				return nil, fmt.Errorf("failed to extract IN value: %w", err)
			}
			value = normalizeEnumComparisonValue(attrPath, value)
			values = append(values, value)
		}

		// Return a predicate that scans for any of the values (OR logic)
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			if cmp.Operator == tree.In {
				// For IN: use OR logic - deduplicate across all values
				seen := make(map[int]struct{})
				count := 0

				for _, value := range values {
					_, err := provider.StreamScanEqual(columnName, value, func(rowIdx int) bool {
						if _, exists := seen[rowIdx]; !exists {
							seen[rowIdx] = struct{}{}
							if !callback(rowIdx) {
								return false
							}
							count++
						}
						return true
					})
					if err != nil {
						return count, err
					}
				}
				return count, nil
			} else {
				// For NOT IN: collect all matches, then complement
				matches := make(map[int]struct{})

				for _, value := range values {
					_, err := provider.StreamScanEqual(columnName, value, func(rowIdx int) bool {
						matches[rowIdx] = struct{}{}
						return true
					})
					if err != nil {
						return 0, err
					}
				}

				// Call callback for all non-matching rows
				count := 0
				for i := 0; i < provider.GetRowCount(); i++ {
					if _, exists := matches[i]; !exists {
						if !callback(i) {
							break
						}
						count++
					}
				}
				return count, nil
			}
		}, nil
	}

	// Handle regex operators
	op := cmp.Operator.String()
	if op == "~" || op == "!~" {
		value, err := extractLiteralValue(cmp.Right)
		if err != nil {
			return nil, fmt.Errorf("regex pattern must be a literal: %w", err)
		}
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("regex pattern must be string, got %T", value)
		}
		if op == "~" {
			return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
				return provider.StreamScanRegex(columnName, pattern, callback)
			}, nil
		} else { // "!~"
			return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
				return provider.StreamScanRegexNotMatch(columnName, pattern, callback)
			}, nil
		}
	}

	// Handle standard comparison operators (=, !=, <, <=, >, >=)
	value, err := extractLiteralValue(cmp.Right)
	if err != nil {
		return nil, fmt.Errorf("comparison value must be a literal: %w", err)
	}
	value = normalizeEnumComparisonValue(attrPath, value)

	switch op {
	case "=":
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanEqual(columnName, value, callback)
		}, nil
	case "!=", "<>":
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanNotEqual(columnName, value, callback)
		}, nil
	case "<":
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanLessThan(columnName, value, callback)
		}, nil
	case "<=":
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanLessThanOrEqual(columnName, value, callback)
		}, nil
	case ">":
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanGreaterThan(columnName, value, callback)
		}, nil
	case ">=":
		return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamScanGreaterThanOrEqual(columnName, value, callback)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator for streaming: %s", op)
	}
}

// compileUnscopedComparisonStreaming handles unscoped attributes (e.g., .foo) for streaming
// These expand to: resource.foo OR span.foo
func compileUnscopedComparisonStreaming(cmp *tree.ComparisonExpr, unscopedPath string) (vm.StreamingColumnPredicate, error) {
	// Create two comparison expressions: one for resource, one for span
	resourcePath := "resource." + unscopedPath[1:] // Remove leading dot
	spanPath := "span." + unscopedPath[1:]

	// Create copies of the comparison with different attribute paths
	resourceCmp := &tree.ComparisonExpr{
		Operator: cmp.Operator,
		Left:     &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{resourcePath}},
		Right:    cmp.Right,
	}
	spanCmp := &tree.ComparisonExpr{
		Operator: cmp.Operator,
		Left:     &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{spanPath}},
		Right:    cmp.Right,
	}

	// Compile both to streaming predicates
	resourcePred, err := compileComparisonToStreamingPredicate(resourceCmp)
	if err != nil {
		return nil, err
	}
	spanPred, err := compileComparisonToStreamingPredicate(spanCmp)
	if err != nil {
		return nil, err
	}

	// OR them together (stream both, deduplicate)
	return func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
		seen := make(map[int]struct{})
		count := 0

		// Stream through resource matches
		_, err := resourcePred(provider, func(rowIdx int) bool {
			if _, exists := seen[rowIdx]; !exists {
				seen[rowIdx] = struct{}{}
				if !callback(rowIdx) {
					return false
				}
				count++
			}
			return true
		})
		if err != nil {
			return count, err
		}

		// Stream through span matches
		_, err = spanPred(provider, func(rowIdx int) bool {
			if _, exists := seen[rowIdx]; !exists {
				seen[rowIdx] = struct{}{}
				if !callback(rowIdx) {
					return false
				}
				count++
			}
			return true
		})

		return count, err
	}, nil
}

// extractLiteralValue is a helper to extract literal values from expressions
func extractLiteralValue(expr tree.Expr) (interface{}, error) {
	return extractComparisonValue(expr)
}

// isHexBytesAttribute checks if an attribute path refers to a hex-encoded bytes field
func isHexBytesAttribute(path string) bool {
	switch path {
	case "trace:id", "span:id":
		return true
	default:
		return false
	}
}

// compileComparisonToColumnPredicate compiles comparison operations to column scans
func compileComparisonToColumnPredicate(cmp *tree.ComparisonExpr) (vm.ColumnPredicate, error) {
	// Extract attribute path to check if it's a hex bytes attribute
	attrPath, err := ExtractAttributePath(cmp.Left)
	if err != nil {
		return nil, err
	}

	// Check if this is an unscoped attribute (should be expanded to resource OR span)
	if isUnscopedAttribute(attrPath) {
		return compileUnscopedComparison(cmp, attrPath)
	}

	// Extract column name
	columnName := attributePathToColumnName(attrPath)

	// Handle IS NULL / IS NOT NULL (represented as IS [NOT] DISTINCT FROM NULL)
	if cmp.Right == tree.DNull {
		return compileNullComparison(cmp, columnName)
	}

	// Handle LIKE/ILIKE/SIMILAR TO operators
	if cmp.Operator == tree.Like || cmp.Operator == tree.ILike || cmp.Operator == tree.SimilarTo || cmp.Operator == tree.NotSimilarTo {
		return compilePatternComparison(cmp, columnName)
	}

	// Handle IN operator (attribute IN (value1, value2, ...))
	if cmp.Operator == tree.In || cmp.Operator == tree.NotIn {
		// Extract tuple of values
		tuple, ok := cmp.Right.(*tree.Tuple)
		if !ok {
			return nil, fmt.Errorf("IN/NOT IN operator requires a tuple of values")
		}

		if len(tuple.Exprs) == 0 {
			return nil, fmt.Errorf("IN/NOT IN operator requires at least one value")
		}

		// Extract each value from the tuple
		var values []interface{}
		for _, expr := range tuple.Exprs {
			value, err := extractComparisonValue(expr)
			if err != nil {
				return nil, fmt.Errorf("failed to extract IN value: %w", err)
			}
			value = normalizeEnumComparisonValue(attrPath, value)
			values = append(values, value)
		}

		// Return a predicate that scans for any of the values (OR logic)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			// Scan for first value
			result, err := provider.ScanEqual(columnName, values[0])
			if err != nil {
				return nil, err
			}

			// Union with scans for remaining values
			for i := 1; i < len(values); i++ {
				rowSet, err := provider.ScanEqual(columnName, values[i])
				if err != nil {
					return nil, err
				}
				result = provider.Union(result, rowSet)
			}

			// If NOT IN, complement the result
			if cmp.Operator == tree.NotIn {
				result = provider.Complement(result)
			}

			return result, nil
		}, nil
	}

	// Handle hex bytes attributes (trace:id, span:id) - decode hex string to bytes
	if isHexBytesAttribute(attrPath) && (cmp.Operator == tree.EQ || cmp.Operator == tree.NE) {
		hexStr, err := extractStringLiteral(cmp.Right)
		if err != nil {
			return nil, fmt.Errorf("hex bytes attribute requires string literal: %w", err)
		}
		decoded, err := hex.DecodeString(hexStr)
		if err != nil {
			return nil, fmt.Errorf("invalid hex string for %s: %w", attrPath, err)
		}

		if cmp.Operator == tree.EQ {
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanEqual(columnName, decoded)
			}, nil
		} else { // tree.NE
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanNotEqual(columnName, decoded)
			}, nil
		}
	}

	// Extract comparison value
	value, err := extractComparisonValue(cmp.Right)
	if err != nil {
		return nil, err
	}
	value = normalizeEnumComparisonValue(attrPath, value)

	// Generate closure based on operator
	op := cmp.Operator.String()
	switch op {
	case "=":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanEqual(columnName, value)
		}, nil
	case "!=", "<>":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanNotEqual(columnName, value)
		}, nil
	case "<":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanLessThan(columnName, value)
		}, nil
	case "<=":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanLessThanOrEqual(columnName, value)
		}, nil
	case ">":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanGreaterThan(columnName, value)
		}, nil
	case ">=":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanGreaterThanOrEqual(columnName, value)
		}, nil
	case "~":
		// Regex match
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("regex pattern must be string, got %T", value)
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegex(columnName, pattern)
		}, nil
	case "!~":
		// Regex not match
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("regex pattern must be string, got %T", value)
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexNotMatch(columnName, pattern)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator: %s", op)
	}
}

// compileNullComparison handles IS NULL / IS NOT NULL comparisons.
func compileNullComparison(cmp *tree.ComparisonExpr, columnName string) (vm.ColumnPredicate, error) {
	op := cmp.Operator.String()
	switch op {
	case "IS DISTINCT FROM": // IS NOT NULL
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanIsNotNull(columnName)
		}, nil
	case "IS NOT DISTINCT FROM": // IS NULL
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanIsNull(columnName)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported NULL comparison operator: %s", op)
	}
}

// compilePatternComparison handles LIKE/ILIKE/SIMILAR TO pattern matching operators.
func compilePatternComparison(cmp *tree.ComparisonExpr, columnName string) (vm.ColumnPredicate, error) {
	patternLiteral, err := extractStringLiteral(cmp.Right)
	if err != nil {
		return nil, fmt.Errorf("%s pattern must be a string literal: %w", cmp.Operator, err)
	}

	var pattern string
	switch cmp.Operator {
	case tree.Like:
		// LIKE operator - IMPORTANT: Uses Go regex patterns directly, NOT SQL LIKE wildcards
		// Use .* instead of %, . instead of _, etc.
		pattern = patternLiteral
	case tree.ILike:
		// ILIKE operator - case-insensitive regex
		pattern = "(?i)" + patternLiteral
	case tree.SimilarTo:
		// SIMILAR TO operator - SQL standard regex
		pattern = similarToPatternToRegex(patternLiteral)
	case tree.NotSimilarTo:
		// NOT SIMILAR TO operator
		pattern = similarToPatternToRegex(patternLiteral)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexNotMatch(columnName, pattern)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported pattern operator: %s", cmp.Operator)
	}

	return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
		return provider.ScanRegex(columnName, pattern)
	}, nil
}

func unscopedDedicatedColumns(attrPath string) []string {
	switch attrPath {
	case "service.name":
		return []string{"resource.service.name", "span.service.name"}
	default:
		return nil
	}
}

// isUnscopedAttribute checks if an attribute path is unscoped (should be expanded to resource OR span)
func isUnscopedAttribute(attrPath string) bool {
	// If contains colon, it's scoped (intrinsic with explicit scope)
	if strings.Contains(attrPath, ":") {
		return false
	}

	// If it has a scope prefix, it's not unscoped
	if strings.HasPrefix(attrPath, "resource.") ||
		strings.HasPrefix(attrPath, "span.") ||
		strings.HasPrefix(attrPath, "event.") ||
		strings.HasPrefix(attrPath, "link.") ||
		strings.HasPrefix(attrPath, "instrumentation.") ||
		strings.HasPrefix(attrPath, "trace.") ||
		strings.HasPrefix(attrPath, ".") {
		return false
	}

	// Check if it's an intrinsic field (these are not unscoped)
	// All intrinsics now use colon notation and have scope prefixes
	switch attrPath {
	case "name", "kind", "status", "status_message",
		"duration", "start", "end", "start_time", "end_time",
		"span:name", "span:duration", "span:kind", "span:status", "span:status_message",
		"span:start", "span:end":
		return false
	}

	// Everything else is unscoped and should be expanded
	return true
}

// compileUnscopedComparison expands an unscoped attribute comparison into
// (resource.attr OR span.attr) to check both scopes
func compileUnscopedComparison(cmp *tree.ComparisonExpr, attrPath string) (vm.ColumnPredicate, error) {
	// Create two comparison expressions - one for resource scope, one for span scope
	resourcePath := "resource." + attrPath

	// For known span intrinsics, use colon notation directly (span:name, span:kind, etc.)
	// For other fields, use dot notation (span.attribute_name) which will be treated as attributes
	var spanPath string
	switch attrPath {
	case "name", "kind", "status", "status_message", "start", "end", "duration",
		"id", "parent_id", "trace_state",
		"dropped_attributes_count", "dropped_events_count", "dropped_links_count":
		spanPath = "span:" + attrPath
	default:
		spanPath = "span." + attrPath
	}

	// Create modified comparison expressions
	resourceCmp := &tree.ComparisonExpr{
		Operator: cmp.Operator,
		Left:     cmp.Left, // We'll override the path during column name resolution
		Right:    cmp.Right,
	}
	spanCmp := &tree.ComparisonExpr{
		Operator: cmp.Operator,
		Left:     cmp.Left,
		Right:    cmp.Right,
	}

	// Compile resource-scoped version
	resourcePredicate, err := compileScopedComparison(resourceCmp, resourcePath)
	if err != nil {
		return nil, fmt.Errorf("failed to compile resource scope: %w", err)
	}

	// Compile span-scoped version
	spanPredicate, err := compileScopedComparison(spanCmp, spanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to compile span scope: %w", err)
	}

	// Return OR of both predicates
	return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
		resourceRows, err := resourcePredicate(provider)
		if err != nil {
			return nil, err
		}
		spanRows, err := spanPredicate(provider)
		if err != nil {
			return nil, err
		}
		return provider.Union(resourceRows, spanRows), nil
	}, nil
}

// compileScopedComparison compiles a comparison with an explicit scope path
func compileScopedComparison(cmp *tree.ComparisonExpr, scopedPath string) (vm.ColumnPredicate, error) {
	// Convert to column name
	columnName := attributePathToColumnName(scopedPath)

	// Handle IS NULL / IS NOT NULL
	if cmp.Right == tree.DNull {
		op := cmp.Operator.String()
		switch op {
		case "IS DISTINCT FROM": // IS NOT NULL
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanIsNotNull(columnName)
			}, nil
		case "IS NOT DISTINCT FROM": // IS NULL
			return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
				return provider.ScanIsNull(columnName)
			}, nil
		default:
			return nil, fmt.Errorf("unsupported NULL comparison operator: %s", op)
		}
	}

	// Handle LIKE operator - IMPORTANT: Uses Go regex patterns directly, NOT SQL LIKE wildcards
	// Use .* instead of %, . instead of _, etc.
	// Example: LIKE '.*ProductCatalog.*' not LIKE '%ProductCatalog%'
	if cmp.Operator == tree.Like {
		patternLiteral, err := extractStringLiteral(cmp.Right)
		if err != nil {
			return nil, fmt.Errorf("LIKE pattern must be a string literal: %w", err)
		}
		pattern := patternLiteral
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegex(columnName, pattern)
		}, nil
	}

	// Handle ILIKE operator - IMPORTANT: case-insensitive, uses Go regex patterns directly
	// NOT SQL LIKE wildcards. Use .* instead of %, . instead of _, etc.
	if cmp.Operator == tree.ILike {
		patternLiteral, err := extractStringLiteral(cmp.Right)
		if err != nil {
			return nil, fmt.Errorf("ILIKE pattern must be a string literal: %w", err)
		}
		pattern := "(?i)" + patternLiteral
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegex(columnName, pattern)
		}, nil
	}

	// Handle IN operator (attribute IN (value1, value2, ...))
	if cmp.Operator == tree.In || cmp.Operator == tree.NotIn {
		// Extract tuple of values
		tuple, ok := cmp.Right.(*tree.Tuple)
		if !ok {
			return nil, fmt.Errorf("IN/NOT IN operator requires a tuple of values")
		}

		if len(tuple.Exprs) == 0 {
			return nil, fmt.Errorf("IN/NOT IN operator requires at least one value")
		}

		// Extract each value from the tuple
		var values []interface{}
		for _, expr := range tuple.Exprs {
			value, err := extractComparisonValue(expr)
			if err != nil {
				return nil, fmt.Errorf("failed to extract IN value: %w", err)
			}
			values = append(values, value)
		}

		// Return a predicate that scans for any of the values (OR logic)
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			// Scan for first value
			result, err := provider.ScanEqual(columnName, values[0])
			if err != nil {
				return nil, err
			}

			// Union with scans for remaining values
			for i := 1; i < len(values); i++ {
				rowSet, err := provider.ScanEqual(columnName, values[i])
				if err != nil {
					return nil, err
				}
				result = provider.Union(result, rowSet)
			}

			// If NOT IN, complement the result
			if cmp.Operator == tree.NotIn {
				result = provider.Complement(result)
			}

			return result, nil
		}, nil
	}

	// Extract comparison value
	value, err := extractComparisonValue(cmp.Right)
	if err != nil {
		return nil, err
	}

	// Generate closure based on operator
	op := cmp.Operator.String()
	switch op {
	case "=":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanEqual(columnName, value)
		}, nil
	case "!=", "<>":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanNotEqual(columnName, value)
		}, nil
	case "<":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanLessThan(columnName, value)
		}, nil
	case "<=":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanLessThanOrEqual(columnName, value)
		}, nil
	case ">":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanGreaterThan(columnName, value)
		}, nil
	case ">=":
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanGreaterThanOrEqual(columnName, value)
		}, nil
	case "~":
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("regex pattern must be string, got %T", value)
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegex(columnName, pattern)
		}, nil
	case "!~":
		pattern, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("regex pattern must be string, got %T", value)
		}
		return func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.ScanRegexNotMatch(columnName, pattern)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator: %s", op)
	}
}

// attributePathToColumnName maps TraceQL attribute paths to blockpack storage column names
// With the colon syntax for intrinsics, no namespace injection needed
func attributePathToColumnName(attrPath string) string {
	// If the path already contains :, it's already in the correct format (intrinsic with : syntax)
	// Just return as-is
	if strings.Contains(attrPath, ":") {
		return attrPath
	}

	// Resource intrinsics (check these first before general resource. handling)
	if attrPath == "resource.schema_url" {
		return "resource:schema_url"
	}
	if attrPath == "resource.dropped_attributes_count" {
		return "resource:dropped_attributes_count"
	}

	// Span-scoped and resource-scoped paths with dot notation are attributes (not intrinsics)
	// Intrinsics use colon notation (span:name, resource:schema_url)
	// Attributes use dot notation (span.name, resource.service.name)
	// So "span.name" is a span attribute, while "span:name" is the intrinsic (already handled above)
	if strings.HasPrefix(attrPath, "span.") || strings.HasPrefix(attrPath, "resource.") {
		return attrPath
	}

	// Event intrinsics
	if attrPath == "event.name" {
		return "event:name"
	}
	if attrPath == "event.time_since_start" {
		return "event:time_since_start"
	}
	if attrPath == "event.dropped_attributes_count" {
		return "event:dropped_attributes_count"
	}

	// Event attributes - pass through
	if strings.HasPrefix(attrPath, "event.") {
		return attrPath
	}

	// Link intrinsics
	if attrPath == "link.trace_id" {
		return "link:trace_id"
	}
	if attrPath == "link.span_id" {
		return "link:span_id"
	}
	if attrPath == "link.trace_state" {
		return "link:trace_state"
	}
	if attrPath == "link.dropped_attributes_count" {
		return "link:dropped_attributes_count"
	}

	// Link attributes - pass through
	if strings.HasPrefix(attrPath, "link.") {
		return attrPath
	}

	// Instrumentation intrinsics
	if attrPath == "instrumentation.name" {
		return "instrumentation:name"
	}
	if attrPath == "instrumentation.version" {
		return "instrumentation:version"
	}
	if attrPath == "instrumentation.dropped_attributes_count" {
		return "instrumentation:dropped_attributes_count"
	}

	// Instrumentation attributes - pass through
	if strings.HasPrefix(attrPath, "instrumentation.") {
		return attrPath
	}

	// Unscoped intrinsics
	switch attrPath {
	case "trace:id":
		return "trace:id"
	case "span:id":
		return "span:id"
	case "name":
		return "span:name"
	case "kind":
		return "span:kind"
	case "status":
		return "span:status"
	case "status_message":
		return "span:status_message"
	case "duration":
		return "span:duration"
	case "start", "start_time":
		return "span:start"
	case "end", "end_time":
		return "span:end"
	default:
		return attrPath
	}
}

// isSpanIntrinsicPath checks if a span path refers to an intrinsic field
// With : syntax, intrinsics use : and attributes use .
// So "span:name" is intrinsic, "span.name" is attribute
func isSpanIntrinsicPath(path string) bool {
	// If contains :, it's explicitly an intrinsic
	if strings.Contains(path, ":") {
		return true
	}

	// If it has span. prefix (dot syntax), treat as attribute, not intrinsic
	// Users must use span: (colon) to explicitly reference intrinsics
	if strings.HasPrefix(path, "span.") {
		return false
	}

	// For span intrinsic fields with colon notation
	// These are always intrinsics
	switch path {
	case "span:start", "span:end", "span:duration":
		return true
	default:
		return false
	}
}

// extractComparisonValue extracts the comparison value from an expression
func extractComparisonValue(expr tree.Expr) (interface{}, error) {
	switch e := expr.(type) {
	case *tree.StrVal:
		return e.RawString(), nil
	case *tree.NumVal:
		// Try to parse as integer first, then float
		if val, err := strconv.ParseInt(e.String(), 10, 64); err == nil {
			return val, nil
		}
		if val, err := strconv.ParseFloat(e.String(), 64); err == nil {
			return val, nil
		}
		return nil, fmt.Errorf("invalid numeric value: %s", e.String())
	case *tree.DBool:
		return bool(*e), nil
	case *tree.DInterval:
		// Convert interval to nanoseconds
		nanos, err := ConvertInterval(expr)
		if err != nil {
			return nil, fmt.Errorf("failed to convert interval: %w", err)
		}
		return nanos, nil
	case *tree.UnresolvedName:
		// Treat double-quoted identifiers as string literals (lenient SQL parsing)
		// This handles cases like WHERE name = "value" (should be 'value')
		return e.String(), nil
	default:
		return nil, fmt.Errorf("unsupported comparison value type: %T", expr)
	}
}

// extractPredicatesFromAST walks the WHERE clause AST and directly extracts
// predicates for block-level filtering, without generating bytecode.
func extractPredicatesFromAST(expr tree.Expr) *vm.QueryPredicates {
	predicates := &vm.QueryPredicates{
		AttributeEquals:       make(map[string][]vm.Value),
		DedicatedColumns:      make(map[string][]vm.Value),
		UnscopedColumnNames:   make(map[string][]string),
		DedicatedColumnsRegex: make(map[string]string),
		AttributeRanges:       make(map[string]*vm.RangePredicate),
		DedicatedRanges:       make(map[string]*vm.RangePredicate),
		HasOROperations:       false,
	}
	extractPredicatesRecursive(expr, predicates)
	return predicates
}

// extractPredicatesRecursive recursively walks the expression tree and extracts predicates
// handleLikeOperator extracts LIKE/ILIKE predicates for dedicated columns
func handleLikeOperator(e *tree.ComparisonExpr, columnName, attrPath string, predicates *vm.QueryPredicates) bool {
	if isUnscopedAttribute(attrPath) {
		if columns := unscopedDedicatedColumns(attrPath); len(columns) > 0 {
			// Extract pattern
			patternLiteral, err := extractStringLiteral(e.Right)
			if err != nil {
				return false
			}

			var regexPattern string
			if e.Operator == tree.Like {
				regexPattern = patternLiteral
			} else {
				regexPattern = "(?i)" + patternLiteral
			}

			key := vm.UnscopedColumnPrefix + attrPath
			predicates.DedicatedColumnsRegex[key] = regexPattern
			predicates.UnscopedColumnNames[key] = columns
			return true
		}
	}

	// Only optimize LIKE on dedicated columns (they have a global dictionary)
	if !isDedicatedColumn(columnName) {
		return false
	}

	// Extract pattern
	patternLiteral, err := extractStringLiteral(e.Right)
	if err != nil {
		return false
	}

	// LIKE/ILIKE use Go regex patterns directly
	var regexPattern string
	if e.Operator == tree.Like {
		regexPattern = patternLiteral
	} else {
		regexPattern = "(?i)" + patternLiteral
	}

	// Store regex pattern for dedicated column
	predicates.DedicatedColumnsRegex[columnName] = regexPattern
	return true
}

// handleInOperator extracts IN predicates (attribute IN (value1, value2, ...))
func handleInOperator(e *tree.ComparisonExpr, columnName, attrPath string, predicates *vm.QueryPredicates) bool {
	// Track attribute access for bloom filter
	if !isDedicatedColumn(columnName) && !isBuiltInField(attrPath) {
		if !contains(predicates.AttributesAccessed, columnName) {
			predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
		}
	}

	// For NOT IN, we can't use it for block-level optimization
	if e.Operator == tree.NotIn {
		return true
	}

	// Extract tuple of values
	tuple, ok := e.Right.(*tree.Tuple)
	if !ok {
		return false
	}

	// Extract each value from the tuple
	for _, expr := range tuple.Exprs {
		value, err := extractComparisonValue(expr)
		if err != nil {
			continue
		}
		value = normalizeEnumComparisonValue(attrPath, value)

		// Convert value to vm.Value
		vmValue := convertToVMValue(value)

		// Add to appropriate predicate map
		if isUnscopedAttribute(attrPath) {
			if columns := unscopedDedicatedColumns(attrPath); len(columns) > 0 {
				key := vm.UnscopedColumnPrefix + attrPath
				predicates.DedicatedColumns[key] = append(
					predicates.DedicatedColumns[key],
					vmValue,
				)
				predicates.UnscopedColumnNames[key] = columns
				continue
			}
		}
		if isDedicatedColumn(columnName) {
			predicates.DedicatedColumns[columnName] = append(
				predicates.DedicatedColumns[columnName],
				vmValue,
			)
		} else {
			predicates.AttributeEquals[columnName] = append(
				predicates.AttributeEquals[columnName],
				vmValue,
			)
		}
	}
	return true
}

// handleEqualityOperator extracts equality predicates (attribute = constant)
func handleEqualityOperator(e *tree.ComparisonExpr, columnName, attrPath string, predicates *vm.QueryPredicates) bool {
	// Track attribute access for bloom filter
	if !isDedicatedColumn(columnName) && !isBuiltInField(attrPath) {
		if !contains(predicates.AttributesAccessed, columnName) {
			predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
		}
	}

	// Extract comparison value
	value, err := extractComparisonValue(e.Right)
	if err != nil {
		return false
	}
	value = normalizeEnumComparisonValue(attrPath, value)

	// Special handling for trace:id and span:id - decode hex strings to bytes
	if isHexBytesAttribute(attrPath) {
		if hexStr, ok := value.(string); ok {
			decoded, err := hex.DecodeString(hexStr)
			if err == nil {
				value = decoded
			}
		}
	}

	// Convert value to vm.Value
	vmValue := convertToVMValue(value)

	// Add to appropriate predicate map
	if isUnscopedAttribute(attrPath) {
		if columns := unscopedDedicatedColumns(attrPath); len(columns) > 0 {
			key := vm.UnscopedColumnPrefix + attrPath
			predicates.DedicatedColumns[key] = append(
				predicates.DedicatedColumns[key],
				vmValue,
			)
			predicates.UnscopedColumnNames[key] = columns
			return true
		}
	}
	if isDedicatedColumn(columnName) {
		predicates.DedicatedColumns[columnName] = append(
			predicates.DedicatedColumns[columnName],
			vmValue,
		)
	} else {
		predicates.AttributeEquals[columnName] = append(
			predicates.AttributeEquals[columnName],
			vmValue,
		)
	}
	return true
}

// handleRangeOperator extracts range predicates (>=, >, <, <=)
func handleRangeOperator(e *tree.ComparisonExpr, columnName, attrPath string, predicates *vm.QueryPredicates) bool {
	// Extract comparison value
	value, err := extractComparisonValue(e.Right)
	if err != nil {
		return false
	}
	value = normalizeEnumComparisonValue(attrPath, value)

	// Convert value to vm.Value
	vmValue := convertToVMValue(value)

	// Get or create range predicate for this column
	var rangePred *vm.RangePredicate
	if isUnscopedAttribute(attrPath) {
		if columns := unscopedDedicatedColumns(attrPath); len(columns) > 0 {
			key := vm.UnscopedColumnPrefix + attrPath
			rangePred = predicates.DedicatedRanges[key]
			if rangePred == nil {
				rangePred = &vm.RangePredicate{}
				predicates.DedicatedRanges[key] = rangePred
			}
			predicates.UnscopedColumnNames[key] = columns
		}
	}
	if rangePred == nil && isDedicatedColumn(columnName) {
		rangePred = predicates.DedicatedRanges[columnName]
		if rangePred == nil {
			rangePred = &vm.RangePredicate{}
			predicates.DedicatedRanges[columnName] = rangePred
		}
	} else {
		rangePred = predicates.AttributeRanges[columnName]
		if rangePred == nil {
			rangePred = &vm.RangePredicate{}
			predicates.AttributeRanges[columnName] = rangePred
		}

		// Track attribute access for bloom filter
		if !isBuiltInField(attrPath) {
			if !contains(predicates.AttributesAccessed, columnName) {
				predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
			}
		}
	}

	// Update range bounds based on operator
	switch e.Operator {
	case tree.GE: // >= (greater than or equal)
		rangePred.MinValue = &vmValue
		rangePred.MinInclusive = true
	case tree.GT: // > (greater than)
		rangePred.MinValue = &vmValue
		rangePred.MinInclusive = false
	case tree.LE: // <= (less than or equal)
		rangePred.MaxValue = &vmValue
		rangePred.MaxInclusive = true
	case tree.LT: // < (less than)
		rangePred.MaxValue = &vmValue
		rangePred.MaxInclusive = false
	}
	return true
}

func extractPredicatesRecursive(expr tree.Expr, predicates *vm.QueryPredicates) {
	switch e := expr.(type) {
	case *tree.AndExpr:
		// For AND, extract predicates from both sides
		extractPredicatesRecursive(e.Left, predicates)
		extractPredicatesRecursive(e.Right, predicates)

	case *tree.OrExpr:
		// For OR, mark that we have OR operations (disables block pruning)
		predicates.HasOROperations = true
		extractPredicatesRecursive(e.Left, predicates)
		extractPredicatesRecursive(e.Right, predicates)

	case *tree.ParenExpr:
		// Unwrap parentheses
		extractPredicatesRecursive(e.Expr, predicates)

	case *tree.ComparisonExpr:
		// Extract attribute path
		attrPath, err := ExtractAttributePath(e.Left)
		if err != nil {
			return // Skip if we can't extract attribute
		}

		// Convert to column name
		columnName := attributePathToColumnName(attrPath)

		// Handle IS NULL / IS NOT NULL (represented as IS [NOT] DISTINCT FROM NULL)
		if e.Right == tree.DNull {
			// Always track for IS NULL/IS NOT NULL since we need to scan the column data
			if !contains(predicates.AttributesAccessed, columnName) {
				predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
			}
			return
		}

		// Handle LIKE operator for dedicated columns
		if e.Operator == tree.Like || e.Operator == tree.ILike {
			handleLikeOperator(e, columnName, attrPath, predicates)
			return
		}

		// Handle IN and NOT IN operators
		if e.Operator == tree.In || e.Operator == tree.NotIn {
			handleInOperator(e, columnName, attrPath, predicates)
			return
		}

		// Handle equality predicates
		if e.Operator == tree.EQ {
			handleEqualityOperator(e, columnName, attrPath, predicates)
			return
		}

		// Handle range predicates
		if e.Operator == tree.GE || e.Operator == tree.GT || e.Operator == tree.LT || e.Operator == tree.LE {
			handleRangeOperator(e, columnName, attrPath, predicates)
			return
		}
	}
}

// isDedicatedColumn checks if a column name could be a dedicated column
func isDedicatedColumn(columnName string) bool {
	// All columns are auto-detected as dedicated at write time.
	// The executor will check at runtime if the column actually exists in the file.
	// Always return true to enable dedicated index optimizations for all columns.
	return true
}

// isBuiltInField checks if a field is a built-in intrinsic field
func isBuiltInField(attrPath string) bool {
	switch attrPath {
	case "span:name", "span:duration", "span:kind", "span:status", "span:status_message",
		"trace:id", "span:id", "span:start", "span:end":
		return true
	default:
		return false
	}
}

// contains checks if a string slice contains a value
func contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

// hasAggregateFunctions checks if any of the select expressions contain aggregate functions
func hasAggregateFunctions(exprs tree.SelectExprs) bool {
	for _, selectExpr := range exprs {
		if hasAggInExpr(selectExpr.Expr) {
			return true
		}
	}
	return false
}

// hasAggInExpr recursively checks if an expression contains an aggregate function
func hasAggInExpr(expr tree.Expr) bool {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		// Check if this is an aggregate function
		funcName := strings.ToUpper(e.Func.String())
		if _, ok := percentileFromFuncName(funcName); ok {
			return true
		}
		switch funcName {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "RATE", "QUANTILE", "HISTOGRAM", "STDDEV":
			return true
		}
		// Check arguments recursively
		for _, arg := range e.Exprs {
			if hasAggInExpr(arg) {
				return true
			}
		}
	case *tree.BinaryExpr:
		return hasAggInExpr(e.Left) || hasAggInExpr(e.Right)
	case *tree.ParenExpr:
		return hasAggInExpr(e.Expr)
	}
	return false
}

// compileAggregationQuery compiles a SELECT query with aggregation (GROUP BY and/or aggregate functions)
func compileAggregationQuery(selectClause *tree.SelectClause) (*vm.Program, error) {
	// Validate the aggregation query
	if err := validateAggregationQuery(selectClause); err != nil {
		return nil, err
	}

	// Create program
	program := &vm.Program{
		Constants:  []vm.Value{},
		Attributes: []string{},
		Regexes:    []*vm.RegexCache{},
		JSONPaths:  []*vm.JSONPathCache{},
	}

	// Create aggregation plan
	plan := &vm.AggregationPlan{
		GroupByFields: []string{},
		Aggregates:    []vm.AggSpec{},
	}

	// Extract GROUP BY fields
	if selectClause.GroupBy != nil {
		for _, groupByExpr := range selectClause.GroupBy {
			// Extract the attribute path
			attrPath, err := ExtractAttributePath(groupByExpr)
			if err != nil {
				return nil, fmt.Errorf("invalid GROUP BY expression: %w", err)
			}
			plan.GroupByFields = append(plan.GroupByFields, attrPath)
		}
	}

	// Parse SELECT expressions to extract aggregate specs
	for _, selectExpr := range selectClause.Exprs {
		if err := parseSelectExprForAgg(selectExpr, plan); err != nil {
			return nil, err
		}
	}

	program.AggregationPlan = plan

	// Generate closure-based column predicate for WHERE clause
	if selectClause.Where != nil {
		predicate, err := compileWhereToColumnPredicate(selectClause.Where.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile WHERE closure: %w", err)
		}
		program.ColumnPredicate = predicate
	} else {
		// No WHERE clause means match all spans
		program.ColumnPredicate = func(provider vm.ColumnDataProvider) (vm.RowSet, error) {
			return provider.FullScan(), nil
		}
	}

	// Generate native streaming predicate (no RowSet allocation!)
	if selectClause.Where != nil {
		streamingPredicate, err := compileExprToStreamingPredicate(selectClause.Where.Expr)
		if err != nil {
			return nil, fmt.Errorf("failed to compile streaming predicate: %w", err)
		}
		program.StreamingColumnPredicate = streamingPredicate
	} else {
		// No WHERE clause means match all spans
		program.StreamingColumnPredicate = func(provider vm.ColumnDataProvider, callback vm.RowCallback) (int, error) {
			return provider.StreamFullScan(callback)
		}
	}

	// Extract predicates directly from WHERE clause for block pruning
	if selectClause.Where != nil {
		program.Predicates = extractPredicatesFromAST(selectClause.Where.Expr)
	} else {
		program.Predicates = &vm.QueryPredicates{
			AttributeEquals:     make(map[string][]vm.Value),
			DedicatedColumns:    make(map[string][]vm.Value),
			UnscopedColumnNames: make(map[string][]string),
			HasOROperations:     false,
		}
	}

	// Extract attributes from WHERE clause and GROUP BY fields for projection
	var attributes []string
	if selectClause.Where != nil {
		attributes = extractAttributesFromExpr(selectClause.Where.Expr)
	}
	// Add GROUP BY fields to attributes
	for _, field := range plan.GroupByFields {
		if !contains(attributes, field) {
			attributes = append(attributes, field)
		}
	}
	// Add aggregate fields to attributes
	for _, agg := range plan.Aggregates {
		if agg.Field != "" && !contains(attributes, agg.Field) {
			attributes = append(attributes, agg.Field)
		}
	}
	program.Attributes = attributes

	return program, nil
}

// validateAggregationQuery validates SQL aggregation rules
func validateAggregationQuery(selectClause *tree.SelectClause) error {
	// Build set of GROUP BY fields for quick lookup
	groupBySet := make(map[string]bool)
	hasGroupBy := selectClause.GroupBy != nil

	if hasGroupBy {
		for _, groupByExpr := range selectClause.GroupBy {
			attrPath, err := ExtractAttributePath(groupByExpr)
			if err != nil {
				return fmt.Errorf("invalid GROUP BY expression: %w", err)
			}
			groupBySet[attrPath] = true
		}
	}

	// Validate each SELECT expression
	for _, selectExpr := range selectClause.Exprs {
		if err := validateSelectExpr(selectExpr.Expr, groupBySet, hasGroupBy); err != nil {
			return err
		}
	}

	return nil
}

// validateSelectExpr validates that a SELECT expression follows SQL aggregation rules:
// - All non-aggregate columns must appear in GROUP BY
// - Aggregate functions cannot be nested
func validateSelectExpr(expr tree.Expr, groupBySet map[string]bool, hasGroupBy bool) error {
	switch e := expr.(type) {
	case *tree.FuncExpr:
		funcName := strings.ToUpper(e.Func.String())
		isAgg := false
		if _, ok := percentileFromFuncName(funcName); ok {
			isAgg = true
		} else {
			switch funcName {
			case "COUNT", "SUM", "AVG", "MIN", "MAX", "RATE", "QUANTILE", "HISTOGRAM", "STDDEV":
				isAgg = true
			}
		}

		if isAgg {
			// Validate aggregate function arguments don't contain nested aggregates
			for _, arg := range e.Exprs {
				if hasAggInExpr(arg) {
					return fmt.Errorf("nested aggregate functions are not supported")
				}
			}
		} else {
			// Non-aggregate function - validate arguments
			for _, arg := range e.Exprs {
				if err := validateSelectExpr(arg, groupBySet, hasGroupBy); err != nil {
					return err
				}
			}
		}

	case *tree.UnresolvedName:
		// This is a column reference
		if hasGroupBy {
			attrPath := e.String()
			attrPath = strings.Trim(attrPath, "\"")
			attrPath = strings.ReplaceAll(attrPath, "__COLON__", ":")

			if !groupBySet[attrPath] {
				return fmt.Errorf("column %q must appear in GROUP BY clause or be used in an aggregate function", attrPath)
			}
		}

	case *tree.BinaryExpr:
		if err := validateSelectExpr(e.Left, groupBySet, hasGroupBy); err != nil {
			return err
		}
		if err := validateSelectExpr(e.Right, groupBySet, hasGroupBy); err != nil {
			return err
		}

	case *tree.ParenExpr:
		if err := validateSelectExpr(e.Expr, groupBySet, hasGroupBy); err != nil {
			return err
		}
	}

	return nil
}

// parseSelectExprForAgg extracts aggregate specs from a SELECT expression
func parseSelectExprForAgg(selectExpr tree.SelectExpr, plan *vm.AggregationPlan) error {
	// Check if this is an aggregate function
	if funcExpr, ok := selectExpr.Expr.(*tree.FuncExpr); ok {
		funcName := strings.ToUpper(funcExpr.Func.String())
		if _, ok := percentileFromFuncName(funcName); ok {
			aggSpec, err := parseAggregateFunc(funcExpr)
			if err != nil {
				return err
			}

			if selectExpr.As != "" {
				aggSpec.Alias = string(selectExpr.As)
			} else {
				aggSpec.Alias = generateDefaultAlias(funcName, aggSpec.Field, aggSpec.Quantile)
			}

			plan.Aggregates = append(plan.Aggregates, aggSpec)
			return nil
		}
		switch funcName {
		case "COUNT", "SUM", "AVG", "MIN", "MAX", "RATE", "QUANTILE", "HISTOGRAM", "STDDEV":
			// Parse the aggregate function
			aggSpec, err := parseAggregateFunc(funcExpr)
			if err != nil {
				return err
			}

			// Set alias if provided
			if selectExpr.As != "" {
				aggSpec.Alias = string(selectExpr.As)
			} else {
				// Generate default alias
				aggSpec.Alias = generateDefaultAlias(funcName, aggSpec.Field, aggSpec.Quantile)
			}

			plan.Aggregates = append(plan.Aggregates, aggSpec)
			return nil
		}
	}

	// Non-aggregate expression (must be a GROUP BY column)
	// These are handled by the grouping logic, not stored in Aggregates
	return nil
}

// parseAggregateFunc parses an aggregate function expression into an AggSpec
func parseAggregateFunc(funcExpr *tree.FuncExpr) (vm.AggSpec, error) {
	funcName := strings.ToUpper(funcExpr.Func.String())

	spec := vm.AggSpec{}

	// Map function name to AggFunction
	if quantile, ok := percentileFromFuncName(funcName); ok {
		spec.Function = vm.AggQuantile
		spec.Quantile = quantile
	} else {
		switch funcName {
		case "COUNT":
			spec.Function = vm.AggCount
		case "SUM":
			spec.Function = vm.AggSum
		case "AVG":
			spec.Function = vm.AggAvg
		case "MIN":
			spec.Function = vm.AggMin
		case "MAX":
			spec.Function = vm.AggMax
		case "RATE":
			spec.Function = vm.AggRate
		case "QUANTILE":
			spec.Function = vm.AggQuantile
		case "HISTOGRAM":
			spec.Function = vm.AggHistogram
		case "STDDEV":
			spec.Function = vm.AggStddev
		default:
			return spec, fmt.Errorf("unsupported aggregate function: %s", funcName)
		}
	}

	// Extract field name
	if len(funcExpr.Exprs) == 0 {
		return spec, fmt.Errorf("%s requires at least one argument", funcName)
	}
	if _, ok := percentileFromFuncName(funcName); ok && len(funcExpr.Exprs) != 1 {
		return spec, fmt.Errorf("%s requires a single field argument", funcName)
	}

	// Check for COUNT(*) and RATE(*) special cases
	if funcName == "COUNT" || funcName == "RATE" {
		// Check if the argument is * (UnqualifiedStar)
		if _, ok := funcExpr.Exprs[0].(tree.UnqualifiedStar); ok {
			// COUNT(*) or RATE(*) - Field is empty
			spec.Field = ""
			return spec, nil
		}
	}

	// Extract field name from first argument
	attrPath, err := ExtractAttributePath(funcExpr.Exprs[0])
	if err != nil {
		return spec, fmt.Errorf("invalid %s argument: %w", funcName, err)
	}
	spec.Field = attrPath

	// Handle QUANTILE special case - second argument is the quantile value
	if funcName == "QUANTILE" {
		if len(funcExpr.Exprs) < 2 {
			return spec, fmt.Errorf("QUANTILE requires two arguments: field and quantile value")
		}

		// Extract quantile value (should be a numeric literal between 0 and 1)
		quantileExpr := funcExpr.Exprs[1]
		numVal, ok := quantileExpr.(*tree.NumVal)
		if !ok {
			return spec, fmt.Errorf("QUANTILE second argument must be a numeric value")
		}

		quantile, err := strconv.ParseFloat(numVal.String(), 64)
		if err != nil {
			return spec, fmt.Errorf("invalid quantile value: %w", err)
		}

		if quantile < 0 || quantile > 1 {
			return spec, fmt.Errorf("quantile value must be between 0 and 1, got %f", quantile)
		}

		spec.Quantile = quantile
	}

	return spec, nil
}

// generateDefaultAlias creates a default column name for an aggregate function
func generateDefaultAlias(funcName, field string, quantile float64) string {
	if funcName == "COUNT" && field == "" {
		return "count"
	}

	if funcName == "RATE" && field == "" {
		return "rate"
	}

	// Strip scope prefixes from field names for cleaner output (span:duration -> duration, span.http.method -> http.method)
	fieldName := field
	if strings.HasPrefix(fieldName, "span:") {
		fieldName = strings.TrimPrefix(fieldName, "span:")
	} else if strings.HasPrefix(fieldName, "span.") {
		fieldName = strings.TrimPrefix(fieldName, "span.")
	} else if strings.HasPrefix(fieldName, "resource:") {
		fieldName = strings.TrimPrefix(fieldName, "resource:")
	} else if strings.HasPrefix(fieldName, "resource.") {
		fieldName = strings.TrimPrefix(fieldName, "resource.")
	}

	if funcName == "QUANTILE" {
		return fmt.Sprintf("quantile_%s_%g", fieldName, quantile)
	}

	if quantile, ok := percentileFromFuncName(funcName); ok {
		percent := int(quantile * 100)
		return fmt.Sprintf("p%d_%s", percent, fieldName)
	}

	if funcName == "HISTOGRAM" {
		return fmt.Sprintf("histogram_%s", fieldName)
	}

	if funcName == "STDDEV" {
		return fmt.Sprintf("stddev_%s", fieldName)
	}

	return fmt.Sprintf("%s_%s", strings.ToLower(funcName), fieldName)
}

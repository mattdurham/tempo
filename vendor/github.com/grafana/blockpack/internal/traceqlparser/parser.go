// Package traceqlparser provides parsing and evaluation of TraceQL expressions.
package traceqlparser

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	// OpAnd is the logical AND operator (&&).
	OpAnd BinaryOp = iota
	// OpOr is the logical OR operator (||).
	OpOr
	// OpEq is the equality operator (=).
	OpEq
	// OpNeq is the inequality operator (!=).
	OpNeq
	// OpGt is the greater than operator (>).
	OpGt
	// OpGte is the greater than or equal operator (>=).
	OpGte
	// OpLt is the less than operator (<).
	OpLt
	// OpLte is the less than or equal operator (<=).
	OpLte
	// OpRegex is the regex match operator (=~).
	OpRegex
	// OpNotRegex is the regex not match operator (!~).
	OpNotRegex
)

// Structural operator string literals used during parsing.
const (
	opStrNotDescendant = "!>>"
	opStrNotChild      = "!>"
)

const (
	// OpDescendant is the descendant operator (>>).
	OpDescendant StructuralOp = iota
	// OpChild is the direct child operator (>).
	OpChild
	// OpSibling is the sibling operator (~).
	OpSibling
	// OpAncestor is the ancestor operator (<<).
	OpAncestor
	// OpParent is the direct parent operator (<).
	OpParent
	// OpNotSibling is the not sibling operator (!~).
	OpNotSibling
	// OpNotDescendant is the negated descendant operator (!>>).
	OpNotDescendant
	// OpNotChild is the negated child operator (!>).
	OpNotChild
)

const (
	// LitString represents a string literal.
	LitString LiteralType = iota
	// LitInt represents an integer literal.
	LitInt
	// LitFloat represents a float literal.
	LitFloat
	// LitBool represents a boolean literal.
	LitBool
	// LitDuration represents a duration literal.
	LitDuration
	// LitStatus represents a status literal (error, ok, unset).
	LitStatus
	// LitKind represents a kind literal (client, server, producer, consumer, internal, unspecified).
	LitKind
	// LitNil represents a nil literal.
	LitNil
)

// ParseTraceQL parses a TraceQL query and returns an AST
// Supports basic filter syntax: { <expression> }
// Also supports metrics queries: { filter } | aggregate() by (fields)
// Also supports structural queries: { expr } OP { expr } where OP is >>, >, ~, <<, <, !~
func ParseTraceQL(query string) (interface{}, error) {
	query = strings.TrimSpace(query)

	// Empty query matches all spans
	if query == "" {
		return (*FilterExpression)(nil), nil
	}

	// Check if this is a metrics query (has pipe operator outside braces)
	if findPipeOutsideBraces(query) >= 0 {
		return parseMetricsQuery(query)
	}

	// Check if this is a structural query (has structural operator outside braces)
	if hasStructuralOperator(query) {
		return parseStructuralQuery(query)
	}

	// Otherwise parse as filter expression
	return parseFilterExpression(query)
}

// skipWSBack returns the index of the last non-whitespace character at or before pos.
func skipWSBack(s string, pos int) int {
	for pos >= 0 && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r') {
		pos--
	}
	return pos
}

// skipWSFwd returns the index of the first non-whitespace character at or after pos.
func skipWSFwd(s string, pos int) int {
	for pos < len(s) && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == '\n' || s[pos] == '\r') {
		pos++
	}
	return pos
}

// hasStructuralOperator checks if the query has a structural operator outside of braces.
// Structural operators: >>, >, ~, <<, <, !~
func hasStructuralOperator(query string) bool {
	pos, _ := findStructuralOperator(query)
	return pos >= 0
}

// findStructuralOperator finds the position of a structural operator outside of braces
// Returns the position and the operator string, or -1 and "" if not found
func findStructuralOperator(query string) (int, string) {
	braceDepth := 0
	parenDepth := 0
	i := 0
	for i < len(query) {
		ch := query[i]
		switch ch {
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		default:
			if braceDepth == 0 && parenDepth == 0 {
				// Check for three-character operators first: !>>
				if i+2 < len(query) {
					threeChar := query[i : i+3]
					if threeChar == opStrNotDescendant {
						return i, threeChar
					}
				}
				// Check for two-character operators
				if i+1 < len(query) {
					twoChar := query[i : i+2]
					if twoChar == ">>" || twoChar == "<<" || twoChar == "!~" || twoChar == "!>" {
						return i, twoChar
					}
				}
				// Check for single-character operators between { } blocks
				if (ch == '>' || ch == '<' || ch == '~') && i > 0 {
					// Look back for }
					j := skipWSBack(query, i-1)
					if j >= 0 && query[j] == '}' {
						// Look ahead for {
						k := skipWSFwd(query, i+1)
						if k < len(query) && query[k] == '{' {
							return i, string(ch)
						}
					}
				}
			}
		}
		i++
	}
	return -1, ""
}

// parseStructuralQuery parses a structural query: { expr } OP { expr }
func parseStructuralQuery(query string) (*StructuralQuery, error) {
	// Find the structural operator
	opPos, opStr := findStructuralOperator(query)
	if opPos == -1 {
		return nil, fmt.Errorf("structural query must have format: { expr } OP { expr }")
	}

	// Split into left and right parts
	leftPart := strings.TrimSpace(query[:opPos])
	rightPart := strings.TrimSpace(query[opPos+len(opStr):])

	// Parse left filter expression
	left, err := parseFilterExpression(leftPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse left expression: %w", err)
	}

	// Parse right filter expression (might itself be a structural query)
	// We need to check if the right part is also a structural query
	rightExpr, err := ParseTraceQL(rightPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse right expression: %w", err)
	}

	// Convert the right expression to a FilterExpression
	var right *FilterExpression
	switch r := rightExpr.(type) {
	case *FilterExpression:
		right = r
	case *StructuralQuery:
		// If the right side is also a structural query, we need to wrap it
		// For now, return an error as we need to handle operator precedence properly
		return nil, fmt.Errorf("nested structural queries require parentheses for clarity")
	case nil:
		right = nil
	default:
		return nil, fmt.Errorf("unexpected right expression type: %T", rightExpr)
	}

	// Map operator string to StructuralOp
	var op StructuralOp
	switch opStr {
	case ">>":
		op = OpDescendant
	case ">":
		op = OpChild
	case "~":
		op = OpSibling
	case "<<":
		op = OpAncestor
	case "<":
		op = OpParent
	case "!~":
		op = OpNotSibling
	case opStrNotDescendant:
		op = OpNotDescendant
	case opStrNotChild:
		op = OpNotChild
	default:
		return nil, fmt.Errorf("unknown structural operator: %s", opStr)
	}

	return &StructuralQuery{
		Left:  left,
		Op:    op,
		Right: right,
	}, nil
}

// parseFilterExpression parses a TraceQL filter expression
func parseFilterExpression(query string) (*FilterExpression, error) {
	query = strings.TrimSpace(query)

	// Empty query matches all spans
	if query == "" {
		return nil, nil
	}

	// TraceQL queries must be wrapped in braces
	if !strings.HasPrefix(query, "{") || !strings.HasSuffix(query, "}") {
		return nil, fmt.Errorf("TraceQL query must be wrapped in braces: { ... }")
	}

	// Remove outer braces
	expr := strings.TrimSpace(query[1 : len(query)-1])
	if expr == "" {
		return nil, nil
	}

	// Parse the filter expression
	parser := &parser{input: expr, pos: 0}
	return parser.parseExpression()
}

// parseMetricsQuery parses a TraceQL metrics query: { filter } | aggregate() by (fields)
func parseMetricsQuery(query string) (*MetricsQuery, error) {
	// Find the pipe operator outside braces
	pipePos := findPipeOutsideBraces(query)
	if pipePos == -1 {
		return nil, fmt.Errorf("metrics query must have format: { filter } | aggregate")
	}

	// Split on the pipe position
	filterPart := strings.TrimSpace(query[:pipePos])
	pipelinePart := strings.TrimSpace(query[pipePos+1:])

	// Parse filter part
	filter, err := parseFilterExpression(filterPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse filter: %w", err)
	}

	// Parse pipeline part
	pipeline, err := parsePipeline(pipelinePart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pipeline: %w", err)
	}

	return &MetricsQuery{Filter: filter, Pipeline: pipeline}, nil
}

// findPipeOutsideBraces finds the position of a single pipe operator outside of braces
// Returns -1 if no such pipe is found
func findPipeOutsideBraces(query string) int {
	braceDepth := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		switch ch {
		case '{':
			braceDepth++
		case '}':
			braceDepth--
		default:
			if ch == '|' && braceDepth == 0 {
				// Check if this is a single pipe (not ||)
				if i+1 < len(query) && query[i+1] == '|' {
					i++
					continue
				}
				if i > 0 && query[i-1] == '|' {
					continue
				}
				return i
			}
		}
	}
	return -1
}

// parsePipeline parses a pipeline expression which may contain multiple stages
// separated by | operators. Examples:
//   - count() > 2
//   - avg(duration) > 5ms
//   - count() > 3 | by(resource.service.name)
//   - select(span.http.method)
//   - by(resource.service.name)
func parsePipeline(input string) (*PipelineStage, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty pipeline")
	}

	// Split on | to handle multi-stage pipelines (e.g., "count() > 3 | by(svc)")
	stages := splitPipelineStages(input)
	result := &PipelineStage{}

	for _, stage := range stages {
		stage = strings.TrimSpace(stage)
		if stage == "" {
			continue
		}

		// Check if this stage is a by() clause
		if strings.HasPrefix(stage, "by(") || strings.HasPrefix(stage, "by (") {
			byFields, err := parseByClause(stage[2:]) // skip "by"
			if err != nil {
				return nil, err
			}
			result.By = byFields
			continue
		}

		// Check if this stage is a select() clause
		if strings.HasPrefix(stage, "select(") || strings.HasPrefix(stage, "select (") {
			selectPart := stage[6:] // skip "select"
			selectPart = strings.TrimSpace(selectPart)
			if !strings.HasPrefix(selectPart, "(") || !strings.HasSuffix(selectPart, ")") {
				return nil, fmt.Errorf("select clause must have parentheses: %s", stage)
			}
			content := strings.TrimSpace(selectPart[1 : len(selectPart)-1])
			for _, f := range strings.Split(content, ",") {
				f = strings.TrimSpace(f)
				if f != "" {
					result.Select = append(result.Select, f)
				}
			}
			continue
		}

		// Otherwise, parse as aggregate with optional threshold: "count() > 2"
		// Also handle inline by-clause: "count_over_time() by (svc)"
		// First check for inline by clause before splitting threshold.
		inlineBy := findByClause(stage)
		var byPart string
		aggStage := stage
		if inlineBy != -1 {
			byPart = strings.TrimSpace(stage[inlineBy+3:]) // skip " by"
			aggStage = strings.TrimSpace(stage[:inlineBy])
		}

		aggStr, thresholdOp, thresholdVal, hasThreshold, err := splitAggregateThreshold(aggStage)
		if err != nil {
			return nil, err
		}
		agg, err := parseAggregateFunc(aggStr)
		if err != nil {
			return nil, err
		}
		if result.Aggregate.Name != "" {
			return nil, fmt.Errorf("pipeline has multiple aggregate stages; only one is allowed")
		}
		result.Aggregate = agg
		if hasThreshold {
			result.HasThreshold = true
			result.ThresholdOp = thresholdOp
			result.ThresholdVal = thresholdVal
		}
		if byPart != "" {
			byFields, byErr := parseByClause(byPart)
			if byErr != nil {
				return nil, byErr
			}
			result.By = byFields
		}
	}

	return result, nil
}

// splitPipelineStages splits a pipeline string on | operators outside parentheses.
func splitPipelineStages(input string) []string {
	var stages []string
	parenDepth := 0
	start := 0
	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		case '|':
			if parenDepth == 0 {
				// Check for || (logical OR) — skip
				if i+1 < len(input) && input[i+1] == '|' {
					i++
					continue
				}
				stages = append(stages, input[start:i])
				start = i + 1
			}
		}
	}
	stages = append(stages, input[start:])
	return stages
}

// splitAggregateThreshold splits "count() > 2" into ("count()", OpGt, 2, true, nil).
// Also handles inline by-clause: expressions like "count_over_time() by (svc)" are returned
// unchanged with hasThreshold=false so the caller can handle the by-clause separately.
// If no threshold is present, returns the input as-is with hasThreshold=false.
func splitAggregateThreshold(
	input string,
) (aggPart string, op BinaryOp, val interface{}, hasThreshold bool, err error) {
	// Find the closing paren of the function call, then look for a comparison operator after it.
	closeParen := -1
	parenDepth := 0
	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
			if parenDepth == 0 && closeParen == -1 {
				closeParen = i
			}
		}
	}
	if closeParen == -1 {
		return input, 0, nil, false, nil
	}

	rest := strings.TrimSpace(input[closeParen+1:])
	if rest == "" {
		return input[:closeParen+1], 0, nil, false, nil
	}

	// Check for inline "by (...)" after the aggregate (no threshold).
	if strings.HasPrefix(rest, "by ") || strings.HasPrefix(rest, "by(") {
		// Return just the aggregate part; the caller will re-parse with the by clause.
		return input, 0, nil, false, nil
	}

	// Parse comparison operator
	var opStr string
	for _, candidate := range []string{">=", "<=", "!=", ">", "<", "="} {
		if strings.HasPrefix(rest, candidate) {
			opStr = candidate
			break
		}
	}
	if opStr == "" {
		return "", 0, nil, false, fmt.Errorf(
			"expected comparison operator after %s, got: %s",
			input[:closeParen+1],
			rest,
		)
	}

	var thresholdOp BinaryOp
	switch opStr {
	case ">":
		thresholdOp = OpGt
	case ">=":
		thresholdOp = OpGte
	case "<":
		thresholdOp = OpLt
	case "<=":
		thresholdOp = OpLte
	case "=":
		thresholdOp = OpEq
	case "!=":
		thresholdOp = OpNeq
	}

	valStr := strings.TrimSpace(rest[len(opStr):])
	// Parse value — could be int, float, or duration
	thresholdVal, parseErr := parseThresholdValue(valStr)
	if parseErr != nil {
		return "", 0, nil, false, fmt.Errorf("invalid threshold value %q: %w", valStr, parseErr)
	}

	return input[:closeParen+1], thresholdOp, thresholdVal, true, nil
}

// parseThresholdValue parses a threshold value as int, duration, or float.
func parseThresholdValue(s string) (interface{}, error) {
	s = strings.TrimSpace(s)

	// Try duration first (e.g., "5ms", "100us", "1s")
	for _, suffix := range []struct {
		unit  string
		nanos int64
	}{
		{"ns", 1},
		{"us", 1000},
		{"µs", 1000},
		{"ms", 1000000},
		{"s", 1000000000},
		{"m", 60 * 1000000000},
		{"h", 3600 * 1000000000},
	} {
		if strings.HasSuffix(s, suffix.unit) {
			numStr := strings.TrimSpace(s[:len(s)-len(suffix.unit)])
			n, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return nil, err
			}
			return n * suffix.nanos, nil
		}
	}

	// Try integer
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	return nil, fmt.Errorf("cannot parse %q as number or duration", s)
}

// findByClause finds the position of " by " in the input that's not inside parentheses
// Returns -1 if no by clause is found
func findByClause(input string) int {
	// Look for " by " keyword that's not inside parentheses
	parenDepth := 0
	for i := 0; i < len(input); i++ {
		switch input[i] {
		case '(':
			parenDepth++
		case ')':
			parenDepth--
		default:
			if parenDepth == 0 {
				// Check if we're at " by " (with spaces before and after)
				if i > 0 && input[i] == 'b' && i+2 < len(input) && input[i:i+3] == "by " {
					if input[i-1] == ' ' || input[i-1] == '\t' || input[i-1] == '\n' || input[i-1] == '\r' {
						return i - 1 // Return position of space before "by"
					}
				}
			}
		}
	}
	return -1
}

// parseAggregateFunc parses an aggregate function call
// Supported functions:
// - rate(), count_over_time() - no arguments
// - avg(field), min(field), max(field), sum(field), stddev(field) - single field argument
// - quantile_over_time(field, quantile) - field and quantile arguments
// - histogram_over_time(field) - single field argument
func parseAggregateFunc(input string) (AggregateFunc, error) {
	input = strings.TrimSpace(input)

	// Find opening parenthesis
	openParen := strings.Index(input, "(")
	if openParen == -1 {
		return AggregateFunc{}, fmt.Errorf("aggregate function must have parentheses: %s", input)
	}

	// Extract function name
	funcName := strings.TrimSpace(input[:openParen])
	if funcName == "" {
		return AggregateFunc{}, fmt.Errorf("missing function name")
	}

	// Find closing parenthesis
	closeParen := strings.LastIndex(input, ")")
	if closeParen == -1 {
		return AggregateFunc{}, fmt.Errorf("missing closing parenthesis for function %s", funcName)
	}

	// Extract arguments
	argsStr := strings.TrimSpace(input[openParen+1 : closeParen])

	// Parse based on function name
	switch funcName {
	case "rate", "count_over_time", "count":
		// No arguments expected
		if argsStr != "" {
			return AggregateFunc{}, fmt.Errorf("%s() takes no arguments", funcName)
		}
		return AggregateFunc{Name: funcName}, nil

	case "avg", "avg_over_time", "min", "min_over_time", "max", "max_over_time", "sum", "stddev", "histogram_over_time":
		// Single field argument expected
		if argsStr == "" {
			return AggregateFunc{}, fmt.Errorf("%s() requires a field argument", funcName)
		}
		field := strings.TrimSpace(argsStr)
		// Normalize Tempo _over_time variants → canonical names (same semantics in blockpack)
		name := funcName
		switch name {
		case "avg_over_time":
			name = "avg"
		case "min_over_time":
			name = "min"
		case "max_over_time":
			name = "max"
		}
		return AggregateFunc{Name: name, Field: field}, nil

	case "quantile_over_time":
		// Arguments: field, quantile [, quantile2, ...] (Tempo multi-quantile syntax)
		// When multiple quantiles are provided, only the first is used.
		if argsStr == "" {
			return AggregateFunc{}, fmt.Errorf("quantile_over_time() requires field and quantile arguments")
		}
		// Split on comma
		args := strings.Split(argsStr, ",")
		if len(args) < 2 {
			return AggregateFunc{}, fmt.Errorf(
				"quantile_over_time() requires at least 2 arguments (field, quantile), got %d",
				len(args),
			)
		}
		field := strings.TrimSpace(args[0])
		quantileStr := strings.TrimSpace(args[1])

		// Parse quantile value
		quantile, err := strconv.ParseFloat(quantileStr, 64)
		if err != nil {
			return AggregateFunc{}, fmt.Errorf("invalid quantile value '%s': %w", quantileStr, err)
		}
		if quantile < 0 || quantile > 1 {
			return AggregateFunc{}, fmt.Errorf("quantile must be between 0 and 1, got %f", quantile)
		}

		return AggregateFunc{Name: funcName, Field: field, Quantile: quantile}, nil

	default:
		return AggregateFunc{}, fmt.Errorf("unsupported aggregate function: %s", funcName)
	}
}

// parseByClause parses the "by" clause: (field1, field2, ...)
func parseByClause(input string) ([]string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty by clause")
	}

	// Expect format: (field1, field2, ...)
	if !strings.HasPrefix(input, "(") {
		return nil, fmt.Errorf("by clause must start with '(', got: %s", input)
	}
	if !strings.HasSuffix(input, ")") {
		return nil, fmt.Errorf("by clause must end with ')', got: %s", input)
	}

	// Extract content between parentheses
	content := strings.TrimSpace(input[1 : len(input)-1])
	if content == "" {
		return nil, fmt.Errorf("by clause cannot be empty")
	}

	// Split on commas and trim each field
	fieldStrs := strings.Split(content, ",")
	fields := make([]string, 0, len(fieldStrs))
	for _, f := range fieldStrs {
		field := strings.TrimSpace(f)
		if field == "" {
			return nil, fmt.Errorf("empty field in by clause")
		}
		fields = append(fields, field)
	}

	return fields, nil
}

type parser struct {
	input string
	pos   int
}

// FilterExpression represents the root of a TraceQL filter AST
type FilterExpression struct {
	Expr Expr
}

// MetricsQuery represents a TraceQL metrics query: { filter } | aggregate() by (fields)
type MetricsQuery struct {
	Filter   *FilterExpression // The filter part: { ... }
	Pipeline *PipelineStage    // The pipeline part: | aggregate() by (...)
}

// StructuralQuery represents a TraceQL structural query: { expr } OP { expr }
// Examples: { .parent } >> { .child }, { true } ~ { false }
type StructuralQuery struct {
	Left  *FilterExpression // The left filter expression
	Right *FilterExpression // The right filter expression
	Op    StructuralOp      // The structural operator (>>, >, ~, <<, <, !~)
}

// PipelineStage represents a pipeline operation
type PipelineStage struct {
	ThresholdVal interface{}
	Aggregate    AggregateFunc
	By           []string
	Select       []string
	ThresholdOp  BinaryOp
	HasThreshold bool
}

// AggregateFunc represents a TraceQL aggregate function
type AggregateFunc struct {
	Name     string  // rate, count_over_time, avg, min, max, quantile_over_time, sum, histogram_over_time, stddev
	Field    string  // Field to aggregate (empty for count)
	Quantile float64 // For quantile_over_time (0-1)
}

// Expr represents any expression node
type Expr interface {
	exprNode()
}

// BinaryExpr represents binary operations (AND, OR, comparisons)
type BinaryExpr struct {
	Left  Expr
	Right Expr
	Op    BinaryOp
}

func (BinaryExpr) exprNode() {}

// BinaryOp represents binary comparison operators.
type BinaryOp int

// StructuralOp represents structural query operators.
type StructuralOp int

func (op BinaryOp) String() string {
	switch op {
	case OpAnd:
		return "&&"
	case OpOr:
		return "||"
	case OpEq:
		return "="
	case OpNeq:
		return "!="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	case OpRegex:
		return "=~"
	case OpNotRegex:
		return "!~"
	default:
		return "UNKNOWN"
	}
}

func (op StructuralOp) String() string {
	switch op {
	case OpDescendant:
		return ">>"
	case OpChild:
		return ">"
	case OpSibling:
		return "~"
	case OpAncestor:
		return "<<"
	case OpParent:
		return "<"
	case OpNotSibling:
		return "!~"
	case OpNotDescendant:
		return opStrNotDescendant
	case OpNotChild:
		return opStrNotChild
	default:
		return "UNKNOWN"
	}
}

// FieldExpr represents an attribute or intrinsic field reference
type FieldExpr struct {
	// Scope: "", "span", "resource", "event", "link", "instrumentation", "trace"
	Scope string
	// Name: field name (e.g., "http.status_code", "name", "duration")
	Name string
}

func (FieldExpr) exprNode() {}

// LiteralExpr represents a literal value
type LiteralExpr struct {
	Value interface{} // string, int64, float64, bool, duration, or status/kind enum
	Type  LiteralType
}

func (LiteralExpr) exprNode() {}

// LiteralType represents the type of a literal value.
type LiteralType int

// parseExpression parses logical OR (lowest precedence)
func (p *parser) parseExpression() (*FilterExpression, error) {
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	return &FilterExpression{Expr: expr}, nil
}

// parseOr parses OR expressions: a || b || c
func (p *parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for {
		p.skipWhitespace()
		if !p.matchOperator("||") {
			break
		}
		p.skipWhitespace()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: OpOr, Right: right}
	}

	return left, nil
}

// parseAnd parses AND expressions: a && b && c
func (p *parser) parseAnd() (Expr, error) {
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for {
		p.skipWhitespace()
		if !p.matchOperator("&&") {
			break
		}
		p.skipWhitespace()
		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Left: left, Op: OpAnd, Right: right}
	}

	return left, nil
}

// parseComparison parses comparison operations
func (p *parser) parseComparison() (Expr, error) {
	p.skipWhitespace()

	// Handle parenthesized expressions
	if p.peek() == '(' {
		p.advance() // consume '('
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		p.skipWhitespace()
		if p.peek() != ')' {
			return nil, fmt.Errorf("expected ')' at position %d", p.pos)
		}
		p.advance() // consume ')'
		return expr, nil
	}

	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	p.skipWhitespace()

	// Check for comparison operators
	var op BinaryOp
	if p.matchOperator("=~") {
		op = OpRegex
	} else if p.matchOperator("!~") {
		op = OpNotRegex
	} else if p.matchOperator("!=") {
		op = OpNeq
	} else if p.matchOperator(">=") {
		op = OpGte
	} else if p.matchOperator("<=") {
		op = OpLte
	} else if p.matchOperator("=") {
		op = OpEq
	} else if p.matchOperator(">") {
		op = OpGt
	} else if p.matchOperator("<") {
		op = OpLt
	} else {
		// No comparison operator
		// For field references, treat as "field is not nil": { .foo } means { .foo != nil }
		// For literals, just return the literal as-is: { true } is valid
		if field, ok := left.(*FieldExpr); ok {
			return &BinaryExpr{
				Left:  field,
				Op:    OpNeq,
				Right: &LiteralExpr{Type: LitNil},
			}, nil
		}
		// Allow standalone literals (e.g., { true }, { false }, { "string" })
		if _, ok := left.(*LiteralExpr); ok {
			return left, nil
		}
		return nil, fmt.Errorf("expected comparison operator at position %d", p.pos)
	}

	p.skipWhitespace()
	right, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	return &BinaryExpr{Left: left, Op: op, Right: right}, nil
}

// parsePrimary parses primary expressions (fields, literals)
func (p *parser) parsePrimary() (Expr, error) {
	p.skipWhitespace()

	ch := p.peek()
	if ch == 0 {
		return nil, fmt.Errorf("unexpected end of input at position %d", p.pos)
	}

	// String literal
	if ch == '"' {
		return p.parseString()
	}

	// Check if it starts with a dot (unscoped attribute)
	if ch == '.' {
		return p.parseFieldPath(p.parseIdentifier())
	}

	// Check for numbers with possible duration suffix
	if (ch >= '0' && ch <= '9') || ch == '-' {
		return p.parseNumberOrDuration()
	}

	// Check for keywords and identifiers
	ident := p.parseIdentifier()
	if ident == "" {
		return nil, fmt.Errorf("expected identifier at position %d", p.pos)
	}

	// Check for reserved keywords
	switch ident {
	case "nil":
		return &LiteralExpr{Type: LitNil}, nil
	case "true":
		return &LiteralExpr{Value: true, Type: LitBool}, nil
	case "false":
		return &LiteralExpr{Value: false, Type: LitBool}, nil
	case "error", "ok", "unset":
		return &LiteralExpr{Value: ident, Type: LitStatus}, nil
	case "client", "server", "producer", "consumer", "internal", "unspecified":
		return &LiteralExpr{Value: ident, Type: LitKind}, nil
	}

	// Check if it's a field reference (contains '.' or ':')
	if strings.Contains(ident, ".") || strings.Contains(ident, ":") {
		return p.parseFieldPath(ident)
	}

	// Otherwise it's an unscoped intrinsic field
	return &FieldExpr{Scope: "", Name: ident}, nil
}

// parseNumberOrDuration parses a number which may have a duration suffix
func (p *parser) parseNumberOrDuration() (Expr, error) {
	start := p.pos

	// Parse numeric part
	for p.pos < len(p.input) && ((p.peek() >= '0' && p.peek() <= '9') || p.peek() == '.' || p.peek() == '-') {
		p.advance()
	}

	numStr := p.input[start:p.pos]

	// Check for duration suffix
	p.skipWhitespace()
	if p.pos < len(p.input) && isLetter(p.peek()) {
		// Could be a duration unit
		unitStart := p.pos
		for p.pos < len(p.input) && isLetter(p.peek()) {
			p.advance()
		}
		unit := p.input[unitStart:p.pos]

		// Check if it's a valid duration unit
		if isDurationUnit(unit) {
			num, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid duration number: %s", numStr)
			}

			// Convert to nanoseconds
			var nanos int64
			switch unit {
			case "ns":
				nanos = num
			case "us", "µs":
				nanos = num * 1000
			case "ms":
				nanos = num * 1000000
			case "s":
				nanos = num * 1000000000
			case "m":
				nanos = num * 60 * 1000000000
			case "h":
				nanos = num * 60 * 60 * 1000000000
			}

			return &LiteralExpr{Value: nanos, Type: LitDuration}, nil
		}

		// Not a duration unit, backtrack
		p.pos = unitStart
	}

	// Try to parse as integer
	if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
		return &LiteralExpr{Value: num, Type: LitInt}, nil
	}

	// Try to parse as float
	if num, err := strconv.ParseFloat(numStr, 64); err == nil {
		return &LiteralExpr{Value: num, Type: LitFloat}, nil
	}

	return nil, fmt.Errorf("invalid number: %s", numStr)
}

func isDurationUnit(unit string) bool {
	switch unit {
	case "ns", "us", "µs", "ms", "s", "m", "h":
		return true
	default:
		return false
	}
}

// parseFieldPath parses a field reference like .foo, span.bar, event:name
func (p *parser) parseFieldPath(path string) (*FieldExpr, error) {
	// Handle colon syntax for intrinsics (event:name, link:spanID, etc.)
	if strings.Contains(path, ":") {
		parts := strings.SplitN(path, ":", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid field path: %s", path)
		}
		return &FieldExpr{Scope: parts[0], Name: parts[1]}, nil
	}

	// Handle dot syntax (.attr, span.attr, resource.attr)
	if strings.HasPrefix(path, ".") {
		// Unscoped attribute
		return &FieldExpr{Scope: "", Name: path[1:]}, nil
	}

	// Scoped attribute (resource.foo, span.bar)
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 2 {
		scope := parts[0]
		name := parts[1]
		// Validate scope
		validScopes := map[string]bool{
			"span": true, "resource": true, "event": true, "link": true,
			"instrumentation": true, "trace": true, "log": true,
		}
		if !validScopes[scope] {
			return nil, fmt.Errorf(
				"invalid scope: %s (must be span, resource, event, link, instrumentation, trace, or log)",
				scope,
			)
		}
		return &FieldExpr{Scope: scope, Name: name}, nil
	}

	return nil, fmt.Errorf("invalid field path: %s", path)
}

// parseString parses a quoted string literal
func (p *parser) parseString() (Expr, error) {
	if p.peek() != '"' {
		return nil, fmt.Errorf("expected \" at position %d", p.pos)
	}
	p.advance() // consume opening quote

	var sb strings.Builder
	for p.pos < len(p.input) {
		ch := p.peek()
		if ch == '"' {
			p.advance() // consume closing quote
			return &LiteralExpr{Value: sb.String(), Type: LitString}, nil
		}
		if ch == '\\' {
			p.advance()
			if p.pos >= len(p.input) {
				return nil, fmt.Errorf("unexpected end of string after backslash")
			}
			// Handle escape sequences
			next := p.peek()
			switch next {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			default:
				sb.WriteByte(next)
			}
			p.advance()
		} else {
			sb.WriteByte(ch)
			p.advance()
		}
	}

	return nil, fmt.Errorf("unterminated string literal")
}

// parseIdentifier parses an identifier or number
func (p *parser) parseIdentifier() string {
	start := p.pos
	for p.pos < len(p.input) {
		ch := p.peek()
		// Allow letters, numbers, underscore, hyphen, dot, colon
		if isIdentChar(ch) {
			p.advance()
		} else {
			break
		}
	}
	return p.input[start:p.pos]
}

func isIdentChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') ||
		ch == '_' || ch == '-' || ch == '.' || ch == ':'
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func (p *parser) matchOperator(op string) bool {
	if p.pos+len(op) > len(p.input) {
		return false
	}
	if p.input[p.pos:p.pos+len(op)] == op {
		p.pos += len(op)
		return true
	}
	return false
}

func (p *parser) skipWhitespace() {
	for p.pos < len(p.input) && (p.peek() == ' ' || p.peek() == '\t' || p.peek() == '\n' || p.peek() == '\r') {
		p.advance()
	}
}

func (p *parser) peek() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *parser) advance() {
	p.pos++
}

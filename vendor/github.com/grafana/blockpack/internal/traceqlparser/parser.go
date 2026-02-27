// Package traceqlparser provides parsing and evaluation of TraceQL expressions.
package traceqlparser

import (
	"fmt"
	"strconv"
	"strings"
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
	if hasPipeOutsideBraces(query) {
		return parseMetricsQuery(query)
	}

	// Check if this is a structural query (has structural operator outside braces)
	if hasStructuralOperator(query) {
		return parseStructuralQuery(query)
	}

	// Otherwise parse as filter expression
	return parseFilterExpression(query)
}

// hasPipeOutsideBraces checks if the query has a pipe operator outside of braces
// This distinguishes metrics queries ({ filter } | aggregate) from filter queries with || operator
func hasPipeOutsideBraces(query string) bool {
	braceDepth := 0
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '{' {
			braceDepth++
		} else if ch == '}' {
			braceDepth--
		} else if ch == '|' && braceDepth == 0 {
			// Check if this is a single pipe (not ||)
			// Look ahead to see if next char is also |
			if i+1 < len(query) && query[i+1] == '|' {
				// This is ||, skip the next |
				i++
				continue
			}
			// Look back to see if previous char was also |
			if i > 0 && query[i-1] == '|' {
				// Already handled as part of ||
				continue
			}
			// Single pipe outside braces - this is a metrics query
			return true
		}
	}
	return false
}

// hasStructuralOperator checks if the query has a structural operator outside of braces
// Structural operators: >>, >, ~, <<, <, !~
// Note: > and < are also comparison operators, so we need to be careful
// We only consider them structural if they appear between { } blocks
func hasStructuralOperator(query string) bool {
	braceDepth := 0
	parenDepth := 0
	i := 0
	for i < len(query) {
		ch := query[i]
		if ch == '{' {
			braceDepth++
		} else if ch == '}' {
			braceDepth--
		} else if ch == '(' {
			parenDepth++
		} else if ch == ')' {
			parenDepth--
		} else if braceDepth == 0 && parenDepth == 0 {
			// We're outside braces and parens, check for structural operators
			// Check for two-character operators first
			if i+1 < len(query) {
				twoChar := query[i : i+2]
				if twoChar == ">>" || twoChar == "<<" || twoChar == "!~" {
					return true
				}
			}
			// Check for single-character structural operators
			// These are only structural if they appear between two { } blocks
			if (ch == '>' || ch == '<' || ch == '~') && i > 0 {
				// Look back to see if there's a } before this
				j := i - 1
				for j >= 0 && (query[j] == ' ' || query[j] == '\t' || query[j] == '\n') {
					j--
				}
				if j >= 0 && query[j] == '}' {
					// Look ahead to see if there's a { after this
					k := i + 1
					// Skip the second character if it's part of a two-char operator
					if ch == '>' && k < len(query) && query[k] == '>' {
						k++
					} else if ch == '<' && k < len(query) && query[k] == '<' {
						k++
					} else if ch == '!' && k < len(query) && query[k] == '~' {
						k++
					}
					for k < len(query) && (query[k] == ' ' || query[k] == '\t' || query[k] == '\n') {
						k++
					}
					if k < len(query) && query[k] == '{' {
						return true
					}
				}
			}
		}
		i++
	}
	return false
}

// findStructuralOperator finds the position of a structural operator outside of braces
// Returns the position and the operator string, or -1 and "" if not found
func findStructuralOperator(query string) (int, string) {
	braceDepth := 0
	parenDepth := 0
	i := 0
	for i < len(query) {
		ch := query[i]
		if ch == '{' {
			braceDepth++
		} else if ch == '}' {
			braceDepth--
		} else if ch == '(' {
			parenDepth++
		} else if ch == ')' {
			parenDepth--
		} else if braceDepth == 0 && parenDepth == 0 {
			// Check for two-character operators first
			if i+1 < len(query) {
				twoChar := query[i : i+2]
				if twoChar == ">>" || twoChar == "<<" || twoChar == "!~" {
					return i, twoChar
				}
			}
			// Check for single-character operators between { } blocks
			if (ch == '>' || ch == '<' || ch == '~') && i > 0 {
				// Look back for }
				j := i - 1
				for j >= 0 && (query[j] == ' ' || query[j] == '\t' || query[j] == '\n') {
					j--
				}
				if j >= 0 && query[j] == '}' {
					// Look ahead for {
					k := i + 1
					for k < len(query) && (query[k] == ' ' || query[k] == '\t' || query[k] == '\n') {
						k++
					}
					if k < len(query) && query[k] == '{' {
						return i, string(ch)
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
		if ch == '{' {
			braceDepth++
		} else if ch == '}' {
			braceDepth--
		} else if ch == '|' && braceDepth == 0 {
			// Check if this is a single pipe (not ||)
			// Look ahead to see if next char is also |
			if i+1 < len(query) && query[i+1] == '|' {
				// This is ||, skip the next |
				i++
				continue
			}
			// Look back to see if previous char was also |
			if i > 0 && query[i-1] == '|' {
				// Already handled as part of ||
				continue
			}
			// Single pipe outside braces - this is the split point
			return i
		}
	}
	return -1
}

// parsePipeline parses a pipeline operation: aggregate() by (fields)
func parsePipeline(input string) (*PipelineStage, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty pipeline")
	}

	// Parse aggregate function (everything before "by")
	// Split on " by " to separate aggregate from GROUP BY clause
	byIndex := findByClause(input)

	var aggregatePart string
	var byPart string

	if byIndex != -1 {
		aggregatePart = strings.TrimSpace(input[:byIndex])
		byPart = strings.TrimSpace(input[byIndex+3:]) // +3 to skip " by"
	} else {
		aggregatePart = input
		byPart = ""
	}

	// Parse the aggregate function
	agg, err := parseAggregateFunc(aggregatePart)
	if err != nil {
		return nil, err
	}

	// Parse the by clause if present
	var byFields []string
	if byPart != "" {
		byFields, err = parseByClause(byPart)
		if err != nil {
			return nil, err
		}
	}

	return &PipelineStage{
		Aggregate: agg,
		By:        byFields,
	}, nil
}

// findByClause finds the position of " by " in the input that's not inside parentheses
// Returns -1 if no by clause is found
func findByClause(input string) int {
	// Look for " by " keyword that's not inside parentheses
	parenDepth := 0
	for i := 0; i < len(input); i++ {
		if input[i] == '(' {
			parenDepth++
		} else if input[i] == ')' {
			parenDepth--
		} else if parenDepth == 0 {
			// Check if we're at " by " (with spaces before and after)
			if i > 0 && input[i] == 'b' && i+2 < len(input) && input[i:i+3] == "by " {
				// Check if there's whitespace before "by"
				if input[i-1] == ' ' || input[i-1] == '\t' {
					return i - 1 // Return position of space before "by"
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
	case "rate", "count_over_time":
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
			return AggregateFunc{}, fmt.Errorf("quantile_over_time() requires at least 2 arguments (field, quantile), got %d", len(args))
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

// Validate checks if the structural query is well-formed
func (sq *StructuralQuery) Validate() error {
	// Note: It's valid to have both sides be nil (e.g., {} >> {} is valid)
	// It's also valid to have one side be nil (e.g., {} >> { .foo } is valid)
	return nil
}

// PipelineStage represents a pipeline operation
type PipelineStage struct {
	Aggregate AggregateFunc // The aggregate function
	By        []string      // GROUP BY fields
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

// StructuralOp represents structural query operators.
type StructuralOp int

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
)

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
			"instrumentation": true, "trace": true,
		}
		if !validScopes[scope] {
			return nil, fmt.Errorf(
				"invalid scope: %s (must be span, resource, event, link, instrumentation, or trace)",
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

// ToSQL converts a TraceQL AST to SQL WHERE clause string
func (fe *FilterExpression) ToSQL() (string, error) {
	if fe == nil {
		return "", nil
	}
	return exprToSQL(fe.Expr)
}

func exprToSQL(expr Expr) (string, error) {
	switch e := expr.(type) {
	case *BinaryExpr:
		return binaryExprToSQL(e)
	case *FieldExpr:
		return fieldExprToSQL(e)
	case *LiteralExpr:
		return literalExprToSQL(e)
	default:
		return "", fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func binaryExprToSQL(expr *BinaryExpr) (string, error) {
	// Handle logical operators (AND/OR)
	if expr.Op == OpAnd || expr.Op == OpOr {
		left, err := exprToSQL(expr.Left)
		if err != nil {
			return "", err
		}
		right, err := exprToSQL(expr.Right)
		if err != nil {
			return "", err
		}

		// Add parentheses around OR if it's part of an AND
		// e.g., (a OR b) AND c should have parentheses around (a OR b)
		// e.g., a AND (b OR c) should have parentheses around (b OR c)
		if expr.Op == OpAnd {
			// Check if LEFT side is an OR expression (CRITICAL FIX!)
			if leftBin, ok := expr.Left.(*BinaryExpr); ok && leftBin.Op == OpOr {
				left = "(" + left + ")"
			}
			// Check if RIGHT side is an OR expression
			if rightBin, ok := expr.Right.(*BinaryExpr); ok && rightBin.Op == OpOr {
				right = "(" + right + ")"
			}
			return fmt.Sprintf("%s AND %s", left, right), nil
		}
		return fmt.Sprintf("%s OR %s", left, right), nil
	}

	// Handle special case: field != nil -> IS NOT NULL
	if expr.Op == OpNeq {
		if lit, ok := expr.Right.(*LiteralExpr); ok && lit.Type == LitNil {
			if field, ok := expr.Left.(*FieldExpr); ok {
				return fieldIsNotNullToSQL(field)
			}
		}
		if lit, ok := expr.Left.(*LiteralExpr); ok && lit.Type == LitNil {
			if field, ok := expr.Right.(*FieldExpr); ok {
				return fieldIsNotNullToSQL(field)
			}
		}
	}

	// Handle special case: field = nil -> IS NULL
	if expr.Op == OpEq {
		if lit, ok := expr.Right.(*LiteralExpr); ok && lit.Type == LitNil {
			if field, ok := expr.Left.(*FieldExpr); ok {
				return fieldIsNullToSQL(field)
			}
		}
		if lit, ok := expr.Left.(*LiteralExpr); ok && lit.Type == LitNil {
			if field, ok := expr.Right.(*FieldExpr); ok {
				return fieldIsNullToSQL(field)
			}
		}
	}

	// Handle comparison with unscoped field - need to expand to (resource.field OP value OR span.field OP value)
	if field, ok := expr.Left.(*FieldExpr); ok && field.Scope == "" && !isIntrinsic(field.Name) {
		return unscopedComparisonToSQL(field, expr.Op, expr.Right)
	}

	// Standard comparison
	left, err := exprToSQL(expr.Left)
	if err != nil {
		return "", err
	}

	right, err := exprToSQL(expr.Right)
	if err != nil {
		return "", err
	}

	var op string
	switch expr.Op {
	case OpEq:
		op = "="
	case OpNeq:
		// For != with non-nil values, need to check IS NOT NULL AND != value
		return fmt.Sprintf("(%s IS NOT NULL AND %s != %s)", left, left, right), nil
	case OpGt:
		op = ">"
	case OpGte:
		op = ">="
	case OpLt:
		op = "<"
	case OpLte:
		op = "<="
	case OpRegex:
		op = "~"
	case OpNotRegex:
		// For !~ we need to check IS NOT NULL AND !~ pattern
		return fmt.Sprintf("(%s IS NOT NULL AND %s !~ %s)", left, left, right), nil
	default:
		return "", fmt.Errorf("unsupported binary operator: %v", expr.Op)
	}

	return fmt.Sprintf("%s %s %s", left, op, right), nil
}

// unscopedComparisonToSQL handles unscoped attribute comparisons
// Example: .foo = "bar" -> ("resource.foo" = 'bar' OR "span.foo" = 'bar')
func unscopedComparisonToSQL(field *FieldExpr, op BinaryOp, rightExpr Expr) (string, error) {
	rightSQL, err := exprToSQL(rightExpr)
	if err != nil {
		return "", err
	}

	resourceField := fmt.Sprintf(`"resource.%s"`, field.Name)
	spanField := fmt.Sprintf(`"span.%s"`, field.Name)

	var opStr string
	var needsNullCheck bool
	switch op {
	case OpEq:
		opStr = "="
		needsNullCheck = false
	case OpNeq:
		opStr = "!="
		needsNullCheck = true
	case OpGt:
		opStr = ">"
		needsNullCheck = false
	case OpGte:
		opStr = ">="
		needsNullCheck = false
	case OpLt:
		opStr = "<"
		needsNullCheck = false
	case OpLte:
		opStr = "<="
		needsNullCheck = false
	case OpRegex:
		opStr = "~"
		needsNullCheck = false
	case OpNotRegex:
		opStr = "!~"
		needsNullCheck = true
	default:
		return "", fmt.Errorf("unsupported operator for unscoped comparison: %v", op)
	}

	if needsNullCheck {
		// For != and !~, need to check NOT NULL first (TraceQL semantics: nil != x is false)
		return fmt.Sprintf("((%s IS NOT NULL AND %s %s %s) OR (%s IS NOT NULL AND %s %s %s))",
			resourceField, resourceField, opStr, rightSQL,
			spanField, spanField, opStr, rightSQL), nil
	}

	return fmt.Sprintf("(%s %s %s OR %s %s %s)",
		resourceField, opStr, rightSQL, spanField, opStr, rightSQL), nil
}

// isIntrinsic checks if a field name is an intrinsic (not an attribute)
func isIntrinsic(name string) bool {
	intrinsics := map[string]bool{
		"name": true, "duration": true, "status": true, "kind": true,
		"statusMessage": true, "parentId": true, "parentID": true,
		"traceID": true, "spanID": true,
	}
	return intrinsics[name]
}

func fieldIsNotNullToSQL(field *FieldExpr) (string, error) {
	if field.Scope == "" {
		// Unscoped - check both resource and span
		resourceField := fmt.Sprintf(`"resource.%s"`, field.Name)
		spanField := fmt.Sprintf(`"span.%s"`, field.Name)
		return fmt.Sprintf("(%s IS NOT NULL OR %s IS NOT NULL)", resourceField, spanField), nil
	}
	fieldSQL, err := fieldExprToSQL(field)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s IS NOT NULL", fieldSQL), nil
}

func fieldIsNullToSQL(field *FieldExpr) (string, error) {
	if field.Scope == "" {
		// Unscoped - check both resource and span are null
		resourceField := fmt.Sprintf(`"resource.%s"`, field.Name)
		spanField := fmt.Sprintf(`"span.%s"`, field.Name)
		return fmt.Sprintf("(%s IS NULL AND %s IS NULL)", resourceField, spanField), nil
	}
	fieldSQL, err := fieldExprToSQL(field)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s IS NULL", fieldSQL), nil
}

func fieldExprToSQL(field *FieldExpr) (string, error) {
	// Map TraceQL intrinsics to SQL column names
	intrinsicMap := map[string]string{
		"name":          "span:name",
		"duration":      "duration",
		"status":        "span:status",
		"statusMessage": "span:status_message",
		"kind":          "span:kind",
		"parentId":      "span:parent_id",
		"parentID":      "span:parent_id",
		"traceID":       "trace:id",
		"spanID":        "span:id",
	}

	// Handle intrinsic fields
	if field.Scope == "" {
		if mapped, ok := intrinsicMap[field.Name]; ok {
			return fmt.Sprintf(`"%s"`, mapped), nil
		}
		// Unscoped attribute - need to check both resource and span
		return fmt.Sprintf(`"resource.%s"`, field.Name), nil // Will be expanded in binaryExprToSQL
	}

	// Handle scoped fields with colon syntax (event:name, link:spanID, etc.)
	if field.Scope == "event" || field.Scope == "link" || field.Scope == "instrumentation" || field.Scope == "trace" {
		// Convert camelCase to snake_case for intrinsics
		name := camelToSnake(field.Name)
		return fmt.Sprintf(`"%s:%s"`, field.Scope, name), nil
	}

	// Handle scoped attributes (resource.foo, span.bar)
	return fmt.Sprintf(`"%s.%s"`, field.Scope, field.Name), nil
}

func literalExprToSQL(lit *LiteralExpr) (string, error) {
	switch lit.Type {
	case LitString:
		// ReplaceAll never returns error, safe to ignore
		if str, ok := lit.Value.(string); ok {
			return fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''")), nil
		}
		return "", fmt.Errorf("invalid string literal value")
	case LitInt:
		// Sprintf never returns error, safe to ignore
		if intVal, ok := lit.Value.(int64); ok {
			return fmt.Sprintf("%d", intVal), nil
		}
		return "", fmt.Errorf("invalid int literal value")
	case LitFloat:
		if floatVal, ok := lit.Value.(float64); ok {
			return fmt.Sprintf("%g", floatVal), nil
		}
		return "", fmt.Errorf("invalid float literal value")
	case LitBool:
		if boolVal, ok := lit.Value.(bool); ok {
			if boolVal {
				return "true", nil
			}
			return "false", nil
		}
		return "", fmt.Errorf("invalid bool literal value")
	case LitDuration:
		nanos, ok := lit.Value.(int64)
		if !ok {
			return "", fmt.Errorf("invalid duration literal value")
		}
		// Convert to PostgreSQL interval format
		// Use the largest unit that divides evenly
		if nanos%3600000000000 == 0 {
			return fmt.Sprintf("INTERVAL '%dh'", nanos/3600000000000), nil
		} else if nanos%60000000000 == 0 {
			return fmt.Sprintf("INTERVAL '%dm'", nanos/60000000000), nil
		} else if nanos%1000000000 == 0 {
			return fmt.Sprintf("INTERVAL '%ds'", nanos/1000000000), nil
		} else if nanos%1000000 == 0 {
			return fmt.Sprintf("INTERVAL '%dms'", nanos/1000000), nil
		} else if nanos%1000 == 0 {
			return fmt.Sprintf("INTERVAL '%dus'", nanos/1000), nil
		}
		return fmt.Sprintf("INTERVAL '%dns'", nanos), nil
	case LitStatus:
		// Map status names to numeric values
		statusMap := map[string]int{
			"unset": 0,
			"ok":    1,
			"error": 2,
		}
		statusName, ok := lit.Value.(string)
		if !ok {
			return "", fmt.Errorf("invalid status literal value")
		}
		return fmt.Sprintf("%d", statusMap[statusName]), nil
	case LitKind:
		// Map kind names to numeric values
		kindMap := map[string]int{
			"unspecified": 0,
			"internal":    1,
			"server":      2,
			"client":      3,
			"producer":    4,
			"consumer":    5,
		}
		kindName, ok := lit.Value.(string)
		if !ok {
			return "", fmt.Errorf("invalid kind literal value")
		}
		return fmt.Sprintf("%d", kindMap[kindName]), nil
	case LitNil:
		return "NULL", nil
	default:
		return "", fmt.Errorf("unsupported literal type: %v", lit.Type)
	}
}

func camelToSnake(s string) string {
	var result []rune
	runes := []rune(s)
	for i, r := range runes {
		if i > 0 && r >= 'A' && r <= 'Z' {
			// Don't insert underscore if previous char is uppercase (for acronyms like "ID")
			if i > 0 && (runes[i-1] < 'A' || runes[i-1] > 'Z') {
				result = append(result, '_')
			}
		}
		result = append(result, r)
	}
	snaked := strings.ToLower(string(result))
	// Fix common acronyms: "i_d" -> "id"
	snaked = strings.ReplaceAll(snaked, "span_i_d", "span_id")
	snaked = strings.ReplaceAll(snaked, "trace_i_d", "trace_id")
	snaked = strings.ReplaceAll(snaked, "parent_i_d", "parent_id")
	return snaked
}

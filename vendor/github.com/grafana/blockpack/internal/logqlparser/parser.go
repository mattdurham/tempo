package logqlparser

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Vector aggregation operator name constants.
const (
	opTopK    = "topk"
	opBottomK = "bottomk"
	opBy      = "by"
	opWithout = "without"
)

// metricFunctions is the set of LogQL metric function names that trigger metric
// expression parsing in ParseQuery.
var metricFunctions = map[string]bool{
	"count_over_time": true,
	"rate":            true,
	"bytes_rate":      true,
	"bytes_over_time": true,
	"sum_over_time":   true,
	"avg_over_time":   true,
	"min_over_time":   true,
	"max_over_time":   true,
	// quantile_over_time is intentionally excluded: the parser does not yet
	// support the quantile parameter syntax (e.g., quantile_over_time(0.99, ...)).
}

// vectorAggOps is the set of LogQL vector aggregation operators.
var vectorAggOps = map[string]bool{
	"sum":     true,
	"avg":     true,
	"min":     true,
	"max":     true,
	"count":   true,
	"topk":    true,
	"bottomk": true,
}

// Parse parses a LogQL selector query into a LogSelector AST.
//
// Supported syntax:
//
//	{label="val", label2=~"regex"}  |= "text" != "text" |~ "regex" !~ "regex"
//
// Returns an error if the query is malformed. An empty query or "{}" returns
// a LogSelector with no matchers and no line filters (matches all).
func Parse(query string) (*LogSelector, error) {
	p := &parser{input: query}
	return p.parse()
}

// ParseQuery parses any LogQL query into a LogQuery union type.
// It handles:
//   - Plain log stream queries: {selector} | pipeline
//   - Metric expressions: count_over_time({sel} | pipeline [5m])
//   - Vector aggregations: sum by (label) (count_over_time(...))
//
// Returns an error if the query is malformed.
func ParseQuery(query string) (*LogQuery, error) {
	p := &parser{input: query}
	return p.parseQuery()
}

type parser struct {
	input string
	pos   int
}

// parseQuery dispatches to metric, vector, or log selector parsing based on
// the leading token.
func (p *parser) parseQuery() (*LogQuery, error) {
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		// Empty query — return match-all log selector.
		return &LogQuery{Selector: &LogSelector{}}, nil
	}

	// Check for vector aggregation or metric function prefix.
	savedPos := p.pos
	ident, peekErr := p.parseIdentifier()
	p.pos = savedPos // reset — just peeking
	if peekErr == nil {
		if vectorAggOps[ident] {
			agg, aggErr := p.parseVectorAggExpr()
			if aggErr != nil {
				return nil, aggErr
			}
			return &LogQuery{VectorAgg: agg}, nil
		}
		if metricFunctions[ident] {
			metric, metricErr := p.parseMetricExpr()
			if metricErr != nil {
				return nil, metricErr
			}
			return &LogQuery{Metric: metric}, nil
		}
	}

	// Fall back to log selector parsing.
	sel, err := p.parse()
	if err != nil {
		return nil, err
	}
	return &LogQuery{Selector: sel}, nil
}

// parseMetricExpr parses a metric expression like count_over_time({sel} | pipeline [5m]).
func (p *parser) parseMetricExpr() (*MetricExpr, error) {
	funcName, err := p.parseIdentifier()
	if err != nil {
		return nil, fmt.Errorf("logql: metric function name: %w", err)
	}
	if !metricFunctions[funcName] {
		return nil, fmt.Errorf("logql: unknown metric function %q", funcName)
	}

	p.skipWhitespace()
	if p.peek() != '(' {
		return nil, fmt.Errorf("logql: expected '(' after %q at position %d", funcName, p.pos)
	}
	p.advance() // skip '('

	// Parse inner log selector (stops at '[' or ')').
	sel, err := p.parseLogSelector()
	if err != nil {
		return nil, fmt.Errorf("logql: metric %q inner selector: %w", funcName, err)
	}

	// Pipeline stages were already parsed into sel.Pipeline by parse().
	pipeline := sel.Pipeline
	sel.Pipeline = nil

	// Parse optional "| unwrap fieldname" (may already be in pipeline).
	var unwrap string
	for i, stage := range pipeline {
		if stage.Type == StageUnwrap && len(stage.Params) > 0 {
			unwrap = stage.Params[0]
			// Remove the unwrap stage from pipeline — it's stored separately.
			pipeline = append(pipeline[:i], pipeline[i+1:]...)
			break
		}
	}

	p.skipWhitespace()

	// Parse range duration [5m], [30s], etc.
	var dur time.Duration
	if p.peek() == '[' {
		p.advance()
		dur, err = p.parseRangeDuration()
		if err != nil {
			return nil, fmt.Errorf("logql: metric %q range: %w", funcName, err)
		}
		p.skipWhitespace()
		if p.peek() != ']' {
			return nil, fmt.Errorf("logql: expected ']' at position %d, got %q", p.pos, string(p.peek()))
		}
		p.advance()
	}

	p.skipWhitespace()
	if p.peek() != ')' {
		return nil, fmt.Errorf("logql: expected ')' at position %d, got %q", p.pos, string(p.peek()))
	}
	p.advance() // skip ')'

	return &MetricExpr{
		Function:      funcName,
		Selector:      sel,
		Pipeline:      pipeline,
		RangeDuration: dur,
		Unwrap:        unwrap,
	}, nil
}

// parseVectorAggExpr parses a vector aggregation.
// Two forms:
//   - sum by (l1, l2) (count_over_time({sel}[5m]))
//   - topk(5, count_over_time({sel}[5m]))
func (p *parser) parseVectorAggExpr() (*VectorAggExpr, error) {
	op, err := p.parseIdentifier()
	if err != nil {
		return nil, fmt.Errorf("logql: vector agg operator: %w", err)
	}
	if !vectorAggOps[op] {
		return nil, fmt.Errorf("logql: unknown vector agg operator %q", op)
	}

	p.skipWhitespace()

	// topk/bottomk: topk(5, <metric_expr>)
	if op == opTopK || op == opBottomK {
		return p.parseTopKExpr(op)
	}

	// sum/avg/min/max/count: optional "by (l1,l2)" or "without (l1,l2)", then "(<metric>)"
	var groupBy, without []string
	savedPos := p.pos
	maybeGrouping, err2 := p.parseIdentifier()
	if err2 == nil && (maybeGrouping == opBy || maybeGrouping == opWithout) {
		p.skipWhitespace()
		if p.peek() == '(' {
			p.advance()
			labels, labelsErr := p.parseLabelList()
			if labelsErr != nil {
				return nil, fmt.Errorf("logql: %s grouping: %w", op, labelsErr)
			}
			p.skipWhitespace()
			if p.peek() != ')' {
				return nil, fmt.Errorf("logql: expected ')' in grouping at position %d", p.pos)
			}
			p.advance()
			if maybeGrouping == opBy {
				groupBy = labels
			} else {
				without = labels
			}
		} else {
			p.pos = savedPos
		}
	} else {
		p.pos = savedPos
	}

	p.skipWhitespace()
	if p.peek() != '(' {
		return nil, fmt.Errorf("logql: expected '(' for %s inner expression at position %d", op, p.pos)
	}
	p.advance() // skip '('
	p.skipWhitespace()

	inner, err := p.parseMetricExpr()
	if err != nil {
		return nil, fmt.Errorf("logql: %s inner metric: %w", op, err)
	}

	p.skipWhitespace()
	if p.peek() != ')' {
		return nil, fmt.Errorf("logql: expected ')' after %s inner expression at position %d", op, p.pos)
	}
	p.advance() // skip ')'

	return &VectorAggExpr{
		Op:      op,
		GroupBy: groupBy,
		Without: without,
		Inner:   inner,
	}, nil
}

// parseTopKExpr parses topk(k, metric_expr) or bottomk(k, metric_expr).
// The operator name has already been consumed.
func (p *parser) parseTopKExpr(op string) (*VectorAggExpr, error) {
	if p.peek() != '(' {
		return nil, fmt.Errorf("logql: expected '(' after %s at position %d", op, p.pos)
	}
	p.advance() // skip '('
	p.skipWhitespace()

	paramStr, err := p.parseUnquotedValue()
	if err != nil {
		return nil, fmt.Errorf("logql: %s k parameter: %w", op, err)
	}
	n, err := strconv.Atoi(strings.TrimSpace(paramStr))
	if err != nil {
		return nil, fmt.Errorf("logql: %s k parameter must be integer: %w", op, err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("logql: %s k parameter must be positive, got %d", op, n)
	}

	p.skipWhitespace()
	if p.peek() != ',' {
		return nil, fmt.Errorf("logql: expected ',' after %s k parameter at position %d", op, p.pos)
	}
	p.advance() // skip ','
	p.skipWhitespace()

	inner, err := p.parseMetricExpr()
	if err != nil {
		return nil, fmt.Errorf("logql: %s inner metric: %w", op, err)
	}

	p.skipWhitespace()
	if p.peek() != ')' {
		return nil, fmt.Errorf("logql: expected ')' after %s inner expression at position %d", op, p.pos)
	}
	p.advance() // skip ')'

	return &VectorAggExpr{
		Op:    op,
		Param: n,
		Inner: inner,
	}, nil
}

// parseRangeDuration parses a Prometheus-style duration like "5m", "30s", "1h".
// The opening '[' has already been consumed.
func (p *parser) parseRangeDuration() (time.Duration, error) {
	start := p.pos
	for p.pos < len(p.input) && p.peek() != ']' {
		p.advance()
	}
	if p.pos == start {
		return 0, fmt.Errorf("logql: empty range duration at position %d", p.pos)
	}
	durStr := strings.TrimSpace(p.input[start:p.pos])
	dur, err := time.ParseDuration(durStr)
	if err != nil {
		return 0, fmt.Errorf("logql: invalid range duration %q: %w", durStr, err)
	}
	return dur, nil
}

// parseLabelList parses a comma-separated list of label names inside parentheses.
// The opening '(' has already been consumed.
func (p *parser) parseLabelList() ([]string, error) {
	var labels []string
	for {
		p.skipWhitespace()
		if p.peek() == ')' {
			break
		}
		label, err := p.parseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("label name: %w", err)
		}
		labels = append(labels, label)
		p.skipWhitespace()
		if p.peek() == ',' {
			p.advance()
			continue
		}
		break
	}
	return labels, nil
}

// parse parses a complete LogQL log selector query (with trailing token validation).
func (p *parser) parse() (*LogSelector, error) {
	sel, err := p.parseLogSelector()
	if err != nil {
		return nil, err
	}

	// Validate no unexpected trailing tokens.
	p.skipWhitespace()
	if p.pos < len(p.input) {
		return nil, fmt.Errorf("logql: unexpected token at position %d: %q", p.pos, string(p.peek()))
	}

	return sel, nil
}

// parseLogSelector parses a log selector without trailing token validation.
// Used by both parse() (standalone query) and parseMetricExpr() (inner selector).
// Stops when it encounters a token that is not part of the selector/pipeline:
// specifically `[` (range duration) or `)` (closing paren in metric expressions).
func (p *parser) parseLogSelector() (*LogSelector, error) {
	p.skipWhitespace()

	if p.pos >= len(p.input) {
		return &LogSelector{}, nil
	}

	// Parse label matchers: { ... }
	if p.peek() != '{' {
		return nil, fmt.Errorf("logql: expected '{' at position %d, got %q", p.pos, string(p.peek()))
	}
	p.advance() // skip '{'
	p.skipWhitespace()

	sel := &LogSelector{}

	// Parse matchers until '}'
	if p.peek() != '}' {
		for {
			m, err := p.parseLabelMatcher()
			if err != nil {
				return nil, err
			}
			sel.Matchers = append(sel.Matchers, m)

			p.skipWhitespace()
			if p.peek() == '}' {
				break
			}
			if p.peek() != ',' {
				return nil, fmt.Errorf("logql: expected ',' or '}' at position %d, got %q", p.pos, string(p.peek()))
			}
			p.advance() // skip ','
			p.skipWhitespace()
		}
	}
	p.advance() // skip '}'

	if err := p.parsePostBraceLineFilters(sel); err != nil {
		return nil, err
	}

	if err := p.parsePipelineStages(sel); err != nil {
		return nil, err
	}

	return sel, nil
}

// parsePostBraceLineFilters parses the bare line filters (|=, |~) that may appear
// directly after the label-matcher block, before any pipeline stages.
func (p *parser) parsePostBraceLineFilters(sel *LogSelector) error {
	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) {
			break
		}

		lf, ok, err := p.parseLineFilter()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		sel.LineFilters = append(sel.LineFilters, lf)
	}
	return nil
}

// parsePipelineStages parses optional pipeline stages (| json, | logfmt, | label_format, etc.)
// and any interleaved line filters (!= !~ |= |~) that follow the label-matcher block.
// Stops when it encounters '[' (range duration marker), ')' (closing paren), or EOF.
func (p *parser) parsePipelineStages(sel *LogSelector) error {
	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.peek() == '[' || p.peek() == ')' {
			break
		}

		// Handle != and !~ line filters that can appear after pipeline stages.
		if p.peek() == '!' {
			done, err := p.parseBangLineFilter(sel)
			if err != nil {
				return err
			}
			if done {
				break
			}
			continue
		}

		if p.peek() != '|' {
			break
		}
		// Peek ahead to see if this '|' starts a pipeline stage (| identifier)
		// vs a line filter (|= or |~). Line filters have already been parsed above,
		// but more may appear interleaved with pipeline stages.
		savedPos := p.pos
		p.advance() // skip '|'
		p.skipWhitespace()
		if p.pos < len(p.input) && (p.peek() == '=' || p.peek() == '~') {
			// Line filter after pipeline stage: |= "text" or |~ "regex".
			// Parse it and append to LineFilters (they're position-independent in LogQL).
			lf, err := p.parsePipeLineFilter()
			if err != nil {
				return err
			}
			sel.LineFilters = append(sel.LineFilters, lf)
			continue
		}
		// Handle parenthesized label filter expressions:
		//   | ( level="error" or level="warn" )
		// Loki's String() wraps OR label filters in parentheses.
		if p.pos < len(p.input) && p.peek() == '(' {
			stage, err := p.parseParenLabelFilter()
			if err != nil {
				return err
			}
			sel.Pipeline = append(sel.Pipeline, stage)
			continue
		}

		// Parse pipeline stage identifier.
		stageName, err := p.parseIdentifier()
		if err != nil {
			p.pos = savedPos
			break
		}
		stage, err := p.parsePipelineStageByName(stageName)
		if err != nil {
			return err
		}
		// Check for "or" chaining on label filter stages:
		//   | level="error" or level="warn"
		if stage.Type == StageLabelFilter {
			p.parseOrLabelFilters(&stage)
		}
		sel.Pipeline = append(sel.Pipeline, stage)
	}
	return nil
}

// parseBangLineFilter handles a '!' prefix line filter (!= or !~) inside the pipeline loop.
// Returns (true, nil) if no valid filter was found (loop should break), (false, nil) on success.
func (p *parser) parseBangLineFilter(sel *LogSelector) (done bool, err error) {
	savedBang := p.pos
	p.advance()
	if p.pos >= len(p.input) || (p.peek() != '=' && p.peek() != '~') {
		p.pos = savedBang
		return true, nil
	}
	var filterType FilterType
	if p.peek() == '=' {
		p.advance()
		filterType = FilterNotContains
	} else {
		p.advance()
		filterType = FilterNotRegex
	}
	p.skipWhitespace()
	pattern, parseErr := p.parseQuotedString()
	if parseErr != nil {
		return false, fmt.Errorf("logql: line filter pattern: %w", parseErr)
	}
	sel.LineFilters = append(sel.LineFilters, LineFilter{Pattern: pattern, Type: filterType})
	return false, nil
}

// parsePipeLineFilter parses a |= or |~ line filter that appears after the '|' has been consumed.
// The cursor must be positioned on '=' or '~'.
func (p *parser) parsePipeLineFilter() (LineFilter, error) {
	var filterType FilterType
	if p.peek() == '=' {
		p.advance()
		filterType = FilterContains
	} else {
		p.advance()
		filterType = FilterRegex
	}
	p.skipWhitespace()
	pattern, err := p.parseQuotedString()
	if err != nil {
		return LineFilter{}, fmt.Errorf("logql: line filter pattern: %w", err)
	}
	return LineFilter{Pattern: pattern, Type: filterType}, nil
}

// parsePipelineStageByName parses the parameters for a named pipeline stage.
// The stage name has already been consumed.
func (p *parser) parsePipelineStageByName(name string) (PipelineStage, error) {
	switch name {
	case "json":
		return PipelineStage{Type: StageJSON}, nil
	case "logfmt":
		return PipelineStage{Type: StageLogfmt}, nil
	case "label_format":
		return p.parseLabelFormatStage()
	case "line_format":
		return p.parseLineFormatStage()
	case "drop":
		return p.parseFieldListStage(StageDrop)
	case "keep":
		return p.parseFieldListStage(StageKeep)
	case "unwrap":
		return p.parseUnwrapStage()
	default:
		// Not a known keyword — try to parse as a label filter: name op value.
		return p.parseLabelFilterStage(name)
	}
}

// parseLabelFormatStage parses "| label_format dst=src, dst2=src2".
// The "label_format" keyword has already been consumed.
func (p *parser) parseLabelFormatStage() (PipelineStage, error) {
	var params []string
	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.peek() == '[' || p.peek() == ')' || p.peek() == '|' {
			break
		}
		savedPos := p.pos
		dst, dstErr := p.parseIdentifier()
		if dstErr != nil {
			p.pos = savedPos
			break
		}
		p.skipWhitespace()
		if p.peek() != '=' {
			return PipelineStage{}, fmt.Errorf("logql: label_format: expected '=' after %q at position %d", dst, p.pos)
		}
		p.advance() // skip '='
		p.skipWhitespace()
		src, srcErr := p.parseIdentifier()
		if srcErr != nil {
			return PipelineStage{}, fmt.Errorf("logql: label_format: expected source label name: %w", srcErr)
		}
		params = append(params, dst+"="+src)
		p.skipWhitespace()
		if p.peek() == ',' {
			p.advance()
			continue
		}
		break
	}
	return PipelineStage{Type: StageLabelFormat, Params: params}, nil
}

// parseLineFormatStage parses "| line_format "template"".
// The "line_format" keyword has already been consumed.
func (p *parser) parseLineFormatStage() (PipelineStage, error) {
	p.skipWhitespace()
	tmpl, err := p.parseQuotedString()
	if err != nil {
		return PipelineStage{}, fmt.Errorf("logql: line_format: %w", err)
	}
	return PipelineStage{Type: StageLineFormat, Params: []string{tmpl}}, nil
}

// parseFieldListStage parses "| drop field1, field2" or "| keep field1, field2".
// The stage keyword has already been consumed.
func (p *parser) parseFieldListStage(stageType PipelineStageType) (PipelineStage, error) {
	var fields []string
	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.peek() == '[' || p.peek() == ')' || p.peek() == '|' {
			break
		}
		savedPos := p.pos
		field, fieldErr := p.parseIdentifier()
		if fieldErr != nil {
			p.pos = savedPos
			break
		}
		fields = append(fields, field)
		p.skipWhitespace()
		if p.peek() == ',' {
			p.advance()
			continue
		}
		break
	}
	return PipelineStage{Type: stageType, Params: fields}, nil
}

// parseUnwrapStage parses "| unwrap fieldname".
// The "unwrap" keyword has already been consumed.
// Params[0] is the field name to unwrap.
func (p *parser) parseUnwrapStage() (PipelineStage, error) {
	p.skipWhitespace()
	field, err := p.parseIdentifier()
	if err != nil {
		return PipelineStage{}, fmt.Errorf("logql: unwrap: expected field name: %w", err)
	}
	return PipelineStage{Type: StageUnwrap, Params: []string{field}}, nil
}

// parseLabelFilterStage parses a label filter like "| fieldname = "value"".
// The field name (labelName) has already been consumed.
func (p *parser) parseLabelFilterStage(labelName string) (PipelineStage, error) {
	p.skipWhitespace()
	op, err := p.parseFilterOp()
	if err != nil {
		return PipelineStage{}, fmt.Errorf("logql: label filter %q: %w", labelName, err)
	}
	p.skipWhitespace()

	var value string
	// Numeric ops accept unquoted numbers; string ops require quoted strings.
	switch op {
	case OpGT, OpLT, OpGTE, OpLTE:
		value, err = p.parseUnquotedValue()
		if err != nil {
			return PipelineStage{}, fmt.Errorf("logql: label filter %q: numeric value: %w", labelName, err)
		}
	default:
		value, err = p.parseQuotedString()
		if err != nil {
			return PipelineStage{}, fmt.Errorf("logql: label filter %q: value: %w", labelName, err)
		}
	}

	lf := &LabelFilter{Name: labelName, Value: value, Op: op}
	return PipelineStage{Type: StageLabelFilter, LabelFilter: lf}, nil
}

// parseFilterOp parses a label filter comparison operator: =, !=, =~, !~, >, <, >=, <=.
func (p *parser) parseFilterOp() (FilterOp, error) {
	if p.pos >= len(p.input) {
		return 0, fmt.Errorf("unexpected end of input, expected operator")
	}
	ch := p.peek()
	switch ch {
	case '=':
		p.advance()
		if p.peek() == '~' {
			p.advance()
			return OpRegex, nil
		}
		return OpEqual, nil
	case '!':
		p.advance()
		switch p.peek() {
		case '=':
			p.advance()
			return OpNotEqual, nil
		case '~':
			p.advance()
			return OpNotRegex, nil
		default:
			return 0, fmt.Errorf("expected '=' or '~' after '!' at position %d", p.pos)
		}
	case '>':
		p.advance()
		if p.peek() == '=' {
			p.advance()
			return OpGTE, nil
		}
		return OpGT, nil
	case '<':
		p.advance()
		if p.peek() == '=' {
			p.advance()
			return OpLTE, nil
		}
		return OpLT, nil
	default:
		return 0, fmt.Errorf("expected comparison operator at position %d, got %q", p.pos, string(ch))
	}
}

// parseUnquotedValue parses a numeric value (digits, '.', optional leading '-').
func (p *parser) parseUnquotedValue() (string, error) {
	start := p.pos
	if p.pos < len(p.input) && p.peek() == '-' {
		p.advance()
	}
	for p.pos < len(p.input) {
		ch := p.peek()
		if (ch >= '0' && ch <= '9') || ch == '.' || ch == 'e' || ch == 'E' || ch == '+' || ch == '-' {
			p.advance()
		} else {
			break
		}
	}
	if p.pos == start {
		return "", fmt.Errorf("expected numeric value at position %d", p.pos)
	}
	return p.input[start:p.pos], nil
}

func (p *parser) parseLabelMatcher() (LabelMatcher, error) {
	p.skipWhitespace()

	name, err := p.parseIdentifier()
	if err != nil {
		return LabelMatcher{}, fmt.Errorf("logql: label name: %w", err)
	}

	p.skipWhitespace()

	// Parse operator: =, !=, =~, !~
	matchType, err := p.parseMatchOp()
	if err != nil {
		return LabelMatcher{}, err
	}

	p.skipWhitespace()

	value, err := p.parseQuotedString()
	if err != nil {
		return LabelMatcher{}, fmt.Errorf("logql: label value: %w", err)
	}

	return LabelMatcher{Name: name, Value: value, Type: matchType}, nil
}

func (p *parser) parseMatchOp() (MatchType, error) {
	if p.pos >= len(p.input) {
		return 0, fmt.Errorf("logql: unexpected end of input, expected operator")
	}

	ch := p.peek()
	switch ch {
	case '=':
		p.advance()
		if p.peek() == '~' {
			p.advance()
			return MatchRegex, nil
		}
		return MatchEqual, nil
	case '!':
		p.advance()
		next := p.peek()
		switch next {
		case '=':
			p.advance()
			return MatchNotEqual, nil
		case '~':
			p.advance()
			return MatchNotRegex, nil
		default:
			return 0, fmt.Errorf("logql: expected '=' or '~' after '!' at position %d", p.pos)
		}
	default:
		return 0, fmt.Errorf("logql: expected operator at position %d, got %q", p.pos, string(ch))
	}
}

func (p *parser) parseLineFilter() (LineFilter, bool, error) {
	if p.pos >= len(p.input) {
		return LineFilter{}, false, nil
	}

	// Line filter operators: |= , != , |~ , !~
	ch := p.peek()
	var filterType FilterType

	switch ch {
	case '|':
		p.advance()
		next := p.peek()
		switch next {
		case '=':
			p.advance()
			filterType = FilterContains
		case '~':
			p.advance()
			filterType = FilterRegex
		default:
			// Not a line filter — could be a pipeline stage like "| json".
			// Rewind and stop parsing line filters.
			p.pos--
			return LineFilter{}, false, nil
		}
	case '!':
		p.advance()
		next := p.peek()
		switch next {
		case '=':
			p.advance()
			filterType = FilterNotContains
		case '~':
			p.advance()
			filterType = FilterNotRegex
		default:
			p.pos--
			return LineFilter{}, false, nil
		}
	default:
		return LineFilter{}, false, nil
	}

	p.skipWhitespace()

	pattern, err := p.parseQuotedString()
	if err != nil {
		return LineFilter{}, false, fmt.Errorf("logql: line filter pattern: %w", err)
	}

	return LineFilter{Pattern: pattern, Type: filterType}, true, nil
}

func (p *parser) parseIdentifier() (string, error) {
	start := p.pos
	for p.pos < len(p.input) {
		ch := rune(p.input[p.pos])
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' || ch == '.' {
			p.pos++
		} else {
			break
		}
	}
	if p.pos == start {
		if p.pos < len(p.input) {
			return "", fmt.Errorf("logql: expected identifier at position %d, got %q", p.pos, string(p.input[p.pos]))
		}
		return "", fmt.Errorf("logql: expected identifier at position %d", p.pos)
	}
	return p.input[start:p.pos], nil
}

func (p *parser) parseQuotedString() (string, error) {
	if p.pos >= len(p.input) {
		return "", fmt.Errorf("logql: unexpected end of input, expected quoted string")
	}

	quote := p.peek()
	if quote != '"' && quote != '`' {
		return "", fmt.Errorf("logql: expected '\"' or '`' at position %d, got %q", p.pos, string(quote))
	}
	p.advance()

	if quote == '`' {
		// Backtick strings: no escapes
		start := p.pos
		for p.pos < len(p.input) && rune(p.input[p.pos]) != '`' {
			p.pos++
		}
		if p.pos >= len(p.input) {
			return "", fmt.Errorf("logql: unterminated backtick string starting at position %d", start-1)
		}
		s := p.input[start:p.pos]
		p.advance() // skip closing backtick
		return s, nil
	}

	// Double-quoted strings with escape sequences
	var sb strings.Builder
	for p.pos < len(p.input) {
		ch := p.input[p.pos]
		if ch == '"' {
			p.advance()
			return sb.String(), nil
		}
		if ch == '\\' {
			p.advance()
			if p.pos >= len(p.input) {
				return "", fmt.Errorf("logql: unterminated escape sequence")
			}
			escaped := p.input[p.pos]
			switch escaped {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case '\\':
				sb.WriteByte('\\')
			case '"':
				sb.WriteByte('"')
			default:
				sb.WriteByte('\\')
				sb.WriteByte(escaped)
			}
			p.advance()
			continue
		}
		sb.WriteByte(ch)
		p.advance()
	}
	return "", fmt.Errorf("logql: unterminated string")
}

func (p *parser) peek() rune {
	if p.pos >= len(p.input) {
		return 0
	}
	return rune(p.input[p.pos])
}

func (p *parser) advance() {
	if p.pos < len(p.input) {
		p.pos++
	}
}

func (p *parser) skipWhitespace() {
	for p.pos < len(p.input) && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

// parseParenLabelFilter parses a parenthesized label filter expression:
//
//	( level="error" or level="warn" )
//
// The opening '(' is the current character. Returns a StageLabelFilter with OrFilters.
func (p *parser) parseParenLabelFilter() (PipelineStage, error) {
	p.advance() // skip '('
	p.skipWhitespace()

	// Handle nested parens: ( ( a or b ) or c )
	if p.pos < len(p.input) && p.peek() == '(' {
		stage, err := p.parseParenLabelFilter()
		if err != nil {
			return PipelineStage{}, err
		}
		// Check for "or" alternatives after the inner group.
		p.parseOrLabelFilters(&stage)
		p.skipWhitespace()
		if p.peek() != ')' {
			return PipelineStage{}, fmt.Errorf("logql: expected ')' at position %d, got %q", p.pos, string(p.peek()))
		}
		p.advance() // skip ')'
		return stage, nil
	}

	// Parse the first label filter.
	firstName, err := p.parseIdentifier()
	if err != nil {
		return PipelineStage{}, fmt.Errorf("logql: expected label name in parenthesized filter: %w", err)
	}
	stage, err := p.parseLabelFilterStage(firstName)
	if err != nil {
		return PipelineStage{}, err
	}

	// Parse "or" alternatives.
	p.parseOrLabelFilters(&stage)

	p.skipWhitespace()
	if p.peek() != ')' {
		return PipelineStage{}, fmt.Errorf("logql: expected ')' at position %d, got %q", p.pos, string(p.peek()))
	}
	p.advance() // skip ')'
	return stage, nil
}

// parseOrLabelFilters parses zero or more "or name op value" clauses and
// appends them to stage.OrFilters. Consumes only what it can parse.
func (p *parser) parseOrLabelFilters(stage *PipelineStage) {
	for {
		savedOrPos := p.pos
		p.skipWhitespace()
		orIdent, orErr := p.parseIdentifier()
		if orErr != nil || orIdent != "or" {
			p.pos = savedOrPos
			break
		}
		p.skipWhitespace()
		orName, nameErr := p.parseIdentifier()
		if nameErr != nil {
			p.pos = savedOrPos
			break
		}
		orStage, orStageErr := p.parseLabelFilterStage(orName)
		if orStageErr != nil {
			p.pos = savedOrPos
			break
		}
		stage.OrFilters = append(stage.OrFilters, orStage.LabelFilter)
	}
}

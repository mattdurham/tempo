package vm

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	regexp "github.com/coregx/coregex"

	"github.com/grafana/blockpack/internal/modules/vectormath"
	"github.com/grafana/blockpack/internal/traceqlparser"
)

// Intrinsic field constants (duplicated to avoid import cycles)
const (
	intrinsicTraceID      = "trace:id"
	intrinsicSpanID       = "span:id"
	intrinsicSpanParentID = "span:parent_id"
)

// embeddingColumnName is the well-known column that stores float32 embedding vectors.
// Duplicated from shared.EmbeddingColumnName to avoid import cycles.
const embeddingColumnName = "__embedding__"

// computedFields are fields calculated at query time that do NOT exist as stored columns.
// Extracting them into predicates causes false negatives (blocks rejected when they contain matching spans).
var computedFields = map[string]bool{
	"span.leaf":       true,
	"span.root":       true,
	"span.childCount": true,
}

// CompileTraceQLFilter compiles a TraceQL FilterExpression directly to a VM Program.
// Generates a ColumnPredicate for fast bulk scanning against blockpack columns.
func CompileTraceQLFilter(filter *traceqlparser.FilterExpression) (*Program, error) {
	if filter == nil || filter.Expr == nil {
		// Empty filter matches all spans
		return compileMatchAllProgram(), nil
	}

	compiler := &traceqlCompiler{
		program:  &Program{},
		embedder: nil,
	}

	// Compile to ColumnPredicate (for blockpack bulk scanning)
	columnPredicate, err := compiler.compileColumnPredicate(filter.Expr)
	if err != nil {
		return nil, fmt.Errorf("compile column predicate: %w", err)
	}
	compiler.program.ColumnPredicate = columnPredicate

	// Extract predicates for block-level pruning and span hints
	compiler.program.Predicates = extractTraceQLPredicates(filter.Expr)

	return compiler.program, nil
}

// CompileOptions carries optional context for CompileTraceQLFilterWithOptions.
type CompileOptions struct {
	// Embedder is required when the filter contains a VECTOR_AI() predicate.
	// If nil and the filter uses VECTOR_AI(), compilation returns an error.
	// Any type implementing TextEmbedder (defined in bytecode.go) is accepted —
	// this keeps the VM decoupled from the concrete embedder implementation.
	Embedder TextEmbedder
	// Limit is the maximum number of vector results to return. 0 means DefaultVectorLimit.
	// Maps directly to Program.VectorLimit for VECTOR_AI/VECTOR_ALL queries.
	Limit int
}

// CompileTraceQLFilterWithOptions compiles a TraceQL FilterExpression with optional
// CompileOptions. When the filter contains a VECTOR() predicate, opts.Embedder must
// be non-nil; the query text is embedded at compile time and stored in Program.QueryVector.
func CompileTraceQLFilterWithOptions(filter *traceqlparser.FilterExpression, opts CompileOptions) (*Program, error) {
	if filter == nil || filter.Expr == nil {
		return compileMatchAllProgram(), nil
	}

	compiler := &traceqlCompiler{
		program:  &Program{},
		embedder: opts.Embedder,
		opts:     opts,
	}

	columnPredicate, err := compiler.compileColumnPredicate(filter.Expr)
	if err != nil {
		return nil, fmt.Errorf("compile column predicate: %w", err)
	}
	compiler.program.ColumnPredicate = columnPredicate
	compiler.program.Predicates = extractTraceQLPredicates(filter.Expr)

	// Populate QueryVector in RangeNodes after compilation (compilation sets program.QueryVector).
	if compiler.program.HasVector && len(compiler.program.QueryVector) > 0 {
		populateVectorNodes(compiler.program.Predicates, compiler.program.QueryVector)
	}

	return compiler.program, nil
}

type traceqlCompiler struct {
	program  *Program
	embedder TextEmbedder
	opts     CompileOptions
}

// normalizeAttributePath converts TraceQL attribute paths to canonical column names.
// Assembles scope + name into a full path, then delegates intrinsic normalization to
// normalizeFieldName (query_spec.go) — the single source of truth for colon-form mapping.
func normalizeAttributePath(scope, name string) string {
	// Handle explicit intrinsic syntax with :
	if strings.HasPrefix(name, ":") {
		if scope == "" {
			return "span" + name
		}
		return scope + name
	}

	// Build full path
	var fullPath string
	if scope == "" {
		fullPath = name
	} else if strings.HasPrefix(name, ".") {
		fullPath = scope + name
	} else {
		fullPath = scope + "." + name
	}

	// Delegate intrinsic mapping to normalizeFieldName (query_spec.go)
	return normalizeFieldName(fullPath)
}

func compileMatchAllProgram() *Program {
	return &Program{
		ColumnPredicate: func(provider ColumnDataProvider) (RowSet, error) {
			return provider.FullScan(), nil
		},
	}
}

func compareValues(left, right Value, operator traceqlparser.BinaryOp) bool {
	if left.Type == TypeNil || right.Type == TypeNil {
		if operator == traceqlparser.OpNeq {
			return left.Type != right.Type
		}
		return left.Type == right.Type
	}

	switch operator {
	case traceqlparser.OpEq:
		return valuesEqual(left, right)
	case traceqlparser.OpNeq:
		return !valuesEqual(left, right)
	case traceqlparser.OpGt:
		return compareNumeric(left, right) > 0
	case traceqlparser.OpGte:
		return compareNumeric(left, right) >= 0
	case traceqlparser.OpLt:
		return compareNumeric(left, right) < 0
	case traceqlparser.OpLte:
		return compareNumeric(left, right) <= 0
	default:
		return false
	}
}

func valuesEqual(left, right Value) bool {
	if left.Type != right.Type {
		return false
	}

	switch left.Type {
	case TypeString:
		leftStr, okL := left.Data.(string)
		rightStr, okR := right.Data.(string)
		return okL && okR && leftStr == rightStr
	case TypeInt:
		leftInt, okL := left.Data.(int64)
		rightInt, okR := right.Data.(int64)
		return okL && okR && leftInt == rightInt
	case TypeFloat:
		leftFloat, okL := left.Data.(float64)
		rightFloat, okR := right.Data.(float64)
		return okL && okR && leftFloat == rightFloat
	case TypeBool:
		leftBool, okL := left.Data.(bool)
		rightBool, okR := right.Data.(bool)
		return okL && okR && leftBool == rightBool
	case TypeBytes:
		leftBytes, okL := left.Data.([]byte)
		rightBytes, okR := right.Data.([]byte)
		return okL && okR && bytes.Equal(leftBytes, rightBytes)
	default:
		return false
	}
}

func compareNumeric(left, right Value) int {
	leftNum := toFloat64(left)
	rightNum := toFloat64(right)

	if leftNum < rightNum {
		return -1
	} else if leftNum > rightNum {
		return 1
	}
	return 0
}

func toFloat64(v Value) float64 {
	switch v.Type {
	case TypeInt:
		if intVal, ok := v.Data.(int64); ok {
			return float64(intVal)
		}
	case TypeFloat:
		if floatVal, ok := v.Data.(float64); ok {
			return floatVal
		}
	case TypeDuration:
		// Duration stored as int64 nanoseconds
		if durVal, ok := v.Data.(int64); ok {
			return float64(durVal)
		}
	}
	return 0
}

// compileColumnPredicate compiles a TraceQL expression to a ColumnPredicate closure.
// This enables fast bulk column scanning on blockpack data.
func (c *traceqlCompiler) compileColumnPredicate(expr traceqlparser.Expr) (ColumnPredicate, error) {
	if expr == nil {
		// Match all
		return func(provider ColumnDataProvider) (RowSet, error) {
			return provider.FullScan(), nil
		}, nil
	}

	return c.compileColumnPredicateExpr(expr)
}

func (c *traceqlCompiler) compileColumnPredicateExpr(expr traceqlparser.Expr) (ColumnPredicate, error) {
	switch e := expr.(type) {
	case *traceqlparser.BinaryExpr:
		return c.compileColumnPredicateBinary(e)
	case *traceqlparser.FieldExpr:
		// Standalone field reference - check if it exists (is not null).
		// Unscoped user attributes expand to OR across resource, span, and log scopes.
		attrName := normalizeAttributePath(e.Scope, e.Name)
		if e.Scope == "" && !isBuiltInField(attrName) {
			resourceName := "resource." + e.Name
			spanName := "span." + e.Name
			logName := "log." + e.Name
			return func(provider ColumnDataProvider) (RowSet, error) {
				rs, err := provider.ScanIsNotNull(resourceName)
				if err != nil {
					return nil, err
				}
				ss, err := provider.ScanIsNotNull(spanName)
				if err != nil {
					return nil, err
				}
				ls, err := provider.ScanIsNotNull(logName)
				if err != nil {
					return nil, err
				}
				return provider.Union(provider.Union(rs, ss), ls), nil
			}, nil
		}
		return func(provider ColumnDataProvider) (RowSet, error) {
			return provider.ScanIsNotNull(attrName)
		}, nil
	case *traceqlparser.LiteralExpr:
		// Literal boolean value
		if e.Type == traceqlparser.LitBool {
			// SPEC-ROOT-001: two-value type assertion to prevent panic on unexpected LitBool value type
			boolVal, ok := e.Value.(bool)
			if !ok {
				return nil, fmt.Errorf("LitBool literal has non-bool value: %T", e.Value)
			}
			if boolVal {
				// true - match all
				return func(provider ColumnDataProvider) (RowSet, error) {
					return provider.FullScan(), nil
				}, nil
			}
			// false - match none
			return func(provider ColumnDataProvider) (RowSet, error) {
				return provider.Complement(provider.FullScan()), nil
			}, nil
		}
		return nil, fmt.Errorf("unsupported literal in column predicate: %v", e.Value)
	case *traceqlparser.VectorExpr:
		return c.compileVectorPredicate(e)
	default:
		return nil, fmt.Errorf("unsupported expression type for column predicate: %T", expr)
	}
}

// compileVectorPredicate compiles a VECTOR_AI() or VECTOR_ALL() expression.
// For VECTOR_AI: embeds the query text at compile time via the Embedder interface.
// For VECTOR_ALL: uses a sentinel to indicate all-fields ranking at execution time.
//
// The vector predicate is split out of ColumnPredicate into program.VectorScorer so
// that traditional predicates run first and vector scoring only operates on survivors.
// The returned ColumnPredicate is a FullScan pass-through; Intersect with other
// predicates in AND expressions reduces it to the non-vector candidate set.
func (c *traceqlCompiler) compileVectorPredicate(e *traceqlparser.VectorExpr) (ColumnPredicate, error) {
	if c.program.HasVector {
		return nil, fmt.Errorf("VECTOR_AI/VECTOR_ALL may appear at most once in a filter expression")
	}

	switch e.Mode {
	case traceqlparser.VectorModeAI:
		return c.compileVectorAI(e)
	case traceqlparser.VectorModeAll:
		return c.compileVectorAll(e)
	default:
		return nil, fmt.Errorf("unknown vector mode: %d", e.Mode)
	}
}

// compileVectorAI compiles VECTOR_AI("text", {"key": "value"}).
// When an Embedder is available, embeds the query text at compile time and stores
// a VectorScorer on the program. When no Embedder is provided (e.g. LIVESTORE),
// returns a no-match predicate that produces empty results without erroring.
func (c *traceqlCompiler) compileVectorAI(e *traceqlparser.VectorExpr) (ColumnPredicate, error) {
	if c.embedder == nil {
		// No embedder available — return a predicate that matches nothing.
		// This allows LIVESTORE and other non-vector backends to accept
		// queries containing VECTOR_AI() without failing.
		c.program.HasVector = true
		c.setVectorLimit()
		return func(provider ColumnDataProvider) (RowSet, error) {
			return provider.Complement(provider.FullScan()), nil // empty set
		}, nil
	}

	text := assembleVectorQueryText(e.QueryText, e.KVPairs)

	queryVec, err := c.embedder.Embed(text)
	if err != nil {
		return nil, fmt.Errorf("VECTOR_AI() embed query text: %w", err)
	}

	c.program.QueryVector = queryVec
	c.program.HasVector = true
	c.program.VectorColumn = "__embedding__"
	c.setVectorLimit()

	threshold := DefaultVectorThreshold
	c.program.VectorScorer = buildVectorScorer(queryVec, threshold)

	// Return FullScan so that when AND-combined with other predicates, Intersect
	// reduces to the non-vector candidate set. VectorScorer runs after.
	return func(provider ColumnDataProvider) (RowSet, error) {
		return provider.FullScan(), nil
	}, nil
}

// compileVectorAll compiles VECTOR_ALL("query text").
// Same as VECTOR_AI but searches the __embedding_all__ column (auto-assembled all fields).
func (c *traceqlCompiler) compileVectorAll(e *traceqlparser.VectorExpr) (ColumnPredicate, error) {
	if c.embedder == nil {
		c.program.HasVector = true
		c.program.VectorAll = true
		c.setVectorLimit()
		return func(provider ColumnDataProvider) (RowSet, error) {
			return provider.Complement(provider.FullScan()), nil
		}, nil
	}

	text := assembleVectorQueryText(e.QueryText, e.KVPairs)

	queryVec, err := c.embedder.Embed(text)
	if err != nil {
		return nil, fmt.Errorf("VECTOR_ALL() embed query text: %w", err)
	}

	c.program.QueryVector = queryVec
	c.program.HasVector = true
	c.program.VectorAll = true
	c.program.VectorColumn = "__embedding_all__"
	c.setVectorLimit()

	threshold := DefaultVectorThreshold
	c.program.VectorScorer = buildVectorScorer(queryVec, threshold)

	// Return FullScan so that when AND-combined with other predicates, Intersect
	// reduces to the non-vector candidate set. VectorScorer runs after.
	return func(provider ColumnDataProvider) (RowSet, error) {
		return provider.FullScan(), nil
	}, nil
}

// buildVectorScorer constructs a VectorScorer closure that scores candidate rows
// by cosine similarity to queryVec and returns those meeting minThreshold,
// sorted by score descending.
func buildVectorScorer(queryVec []float32, minThreshold float32) VectorScorer {
	return func(getVec func(rowIdx int) ([]float32, bool), candidates RowSet) []ScoredRow {
		rows := candidates.ToSlice()
		scored := make([]ScoredRow, 0, len(rows))
		for _, rowIdx := range rows {
			vec, ok := getVec(rowIdx)
			if !ok {
				continue
			}
			sim := float32(1.0) - vectormath.CosineDistance(queryVec, vec)
			if sim >= minThreshold {
				scored = append(scored, ScoredRow{RowIdx: rowIdx, Score: sim})
			}
		}
		// Insertion sort: sufficient for typical top-K sizes (limit ≤ 100).
		for i := 1; i < len(scored); i++ {
			for j := i; j > 0 && scored[j].Score > scored[j-1].Score; j-- {
				scored[j], scored[j-1] = scored[j-1], scored[j]
			}
		}
		return scored
	}
}

// setVectorLimit sets the program's vector limit from compile options or default.
func (c *traceqlCompiler) setVectorLimit() {
	lim := c.opts.Limit
	if lim <= 0 {
		lim = DefaultVectorLimit
	}
	if c.program.VectorLimit == 0 {
		c.program.VectorLimit = lim
	}
}

// assembleVectorQueryText concatenates queryText with KVPairs sorted by key.
func assembleVectorQueryText(queryText string, kvPairs map[string]string) string {
	if len(kvPairs) == 0 {
		return queryText
	}
	keys := make([]string, 0, len(kvPairs))
	for k := range kvPairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString(queryText)
	for _, k := range keys {
		sb.WriteByte(' ')
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(kvPairs[k])
	}
	return sb.String()
}

func (c *traceqlCompiler) compileColumnPredicateBinary(expr *traceqlparser.BinaryExpr) (ColumnPredicate, error) {
	switch expr.Op {
	case traceqlparser.OpAnd:
		// AND: intersect left and right results
		leftPred, err := c.compileColumnPredicateExpr(expr.Left)
		if err != nil {
			return nil, err
		}
		rightPred, err := c.compileColumnPredicateExpr(expr.Right)
		if err != nil {
			return nil, err
		}
		return func(provider ColumnDataProvider) (RowSet, error) {
			leftRows, err := leftPred(provider)
			if err != nil {
				return nil, err
			}
			rightRows, err := rightPred(provider)
			if err != nil {
				return nil, err
			}
			return provider.Intersect(leftRows, rightRows), nil
		}, nil

	case traceqlparser.OpOr:
		// OR single-pass optimisation: when all arms of the OR tree are equality
		// comparisons against the same column (e.g. svc="A" || svc="B" || svc="C"),
		// compile to a single ScanEqualAny call instead of N separate ScanEqual calls.
		// This avoids re-scanning the column once per value; the implementation uses
		// a dict-level bool mask for string columns (O(dict+spans) vs N×O(spans)).
		if field, values := gatherOrEqualAny(expr); field != nil {
			attrName := normalizeAttributePath(field.Scope, field.Name)
			vals := values // capture for closure
			return unscopedOrScoped(field, attrName, func(p ColumnDataProvider, name string) (RowSet, error) {
				return p.ScanEqualAny(name, vals)
			}), nil
		}
		// General OR: union left and right results
		leftPred, err := c.compileColumnPredicateExpr(expr.Left)
		if err != nil {
			return nil, err
		}
		rightPred, err := c.compileColumnPredicateExpr(expr.Right)
		if err != nil {
			return nil, err
		}
		return func(provider ColumnDataProvider) (RowSet, error) {
			leftRows, err := leftPred(provider)
			if err != nil {
				return nil, err
			}
			rightRows, err := rightPred(provider)
			if err != nil {
				return nil, err
			}
			return provider.Union(leftRows, rightRows), nil
		}, nil

	case traceqlparser.OpEq,
		traceqlparser.OpNeq,
		traceqlparser.OpGt,
		traceqlparser.OpGte,
		traceqlparser.OpLt,
		traceqlparser.OpLte,
		traceqlparser.OpRegex,
		traceqlparser.OpNotRegex:
		// Comparison: compile to column scan
		return c.compileColumnPredicateComparison(expr)

	default:
		return nil, fmt.Errorf("unsupported binary operator for column predicate: %s", expr.Op.String())
	}
}

// NOTE-043: unscopedOrScoped eliminates three identical unscoped attribute expansion
// blocks from compileColumnPredicateComparison. See internal/vm/NOTES.md §NOTE-043.
//
// unscopedOrScoped returns a ColumnPredicate that, for unscoped user attributes,
// scans resource.*, span.*, and log.* columns and unions the results. For scoped
// attributes or built-ins, it scans only the named column.
// scan must be a closure that captures all scan-function arguments except the
// provider and column name.
func unscopedOrScoped(
	fieldExpr *traceqlparser.FieldExpr,
	attrName string,
	scan func(ColumnDataProvider, string) (RowSet, error),
) ColumnPredicate {
	if fieldExpr.Scope == "" && !isBuiltInField(attrName) {
		rName := "resource." + fieldExpr.Name
		sName := "span." + fieldExpr.Name
		lName := "log." + fieldExpr.Name
		return func(provider ColumnDataProvider) (RowSet, error) {
			rs, err := scan(provider, rName)
			if err != nil {
				return nil, err
			}
			ss, err := scan(provider, sName)
			if err != nil {
				return nil, err
			}
			ls, err := scan(provider, lName)
			if err != nil {
				return nil, err
			}
			return provider.Union(provider.Union(rs, ss), ls), nil
		}
	}
	return func(provider ColumnDataProvider) (RowSet, error) {
		return scan(provider, attrName)
	}
}

func (c *traceqlCompiler) compileColumnPredicateComparison(expr *traceqlparser.BinaryExpr) (ColumnPredicate, error) {
	// Extract field and literal
	fieldExpr, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return nil, fmt.Errorf("comparison left side must be field, got %T", expr.Left)
	}

	literalExpr, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return nil, fmt.Errorf("comparison right side must be literal, got %T", expr.Right)
	}

	// Normalize attribute name using canonical mapping
	attrName := normalizeAttributePath(fieldExpr.Scope, fieldExpr.Name)

	value := literalExpr.Value

	// Special handling for hex bytes attributes (trace:id, span:id)
	// Decode hex strings to bytes for proper comparison
	if isHexBytesAttribute(attrName) {
		if hexStr, ok := value.(string); ok {
			if decoded, err := hex.DecodeString(hexStr); err == nil {
				value = decoded
			}
		}
	}

	operator := expr.Op

	// For regex operators, pre-compile the pattern at plan-compile time to avoid
	// per-block recompilation in the hot scan path.
	if operator == traceqlparser.OpRegex || operator == traceqlparser.OpNotRegex {
		if strVal, ok := value.(string); ok {
			notMatch := operator == traceqlparser.OpNotRegex
			// CI literal bypass: pure (?i)literal patterns use fold-contains instead
			// of the NFA regex engine, avoiding tryBacktrack overhead.
			if a := AnalyzeRegex(strVal); a != nil && a.IsLiteralContains && a.CaseInsensitive {
				lp := a.Prefixes // pre-lowercased
				return unscopedOrScoped(fieldExpr, attrName, func(p ColumnDataProvider, name string) (RowSet, error) {
					return scanColumnRegexFast(p, name, nil, lp, notMatch)
				}), nil
			}
			re, err := regexp.Compile(strVal)
			if err != nil {
				return nil, fmt.Errorf("invalid regex %q: %w", strVal, err)
			}
			prefixes := RegexPrefixes(strVal)
			return unscopedOrScoped(fieldExpr, attrName, func(p ColumnDataProvider, name string) (RowSet, error) {
				return scanColumnRegexFast(p, name, re, prefixes, notMatch)
			}), nil
		}
		return nil, fmt.Errorf("regex operator requires string value")
	}

	// Convert typed literals (status, kind, duration, etc.) to their wire representations
	// for non-regex operators. Reuses convertTraceQLLiteralToValue so conversion logic
	// stays consistent with the VM instruction path. Skipped for:
	//   - regex operators (handled above) — raw string form required
	//   - LitString — already the correct type; skipping also preserves any prior
	//     hex→[]byte decode applied above for trace:id/span:id intrinsics
	if literalExpr.Type != traceqlparser.LitString {
		if converted, ok := convertTraceQLLiteralToValue(literalExpr); ok {
			value = converted.Data
		}
	}

	// Unscoped user attributes expand to OR across resource, span, and log scopes.
	return unscopedOrScoped(fieldExpr, attrName, func(p ColumnDataProvider, name string) (RowSet, error) {
		return scanColumnByOp(p, name, value, operator)
	}), nil
}

// scanColumnByOp dispatches a column scan for the given operator.
func scanColumnByOp(
	provider ColumnDataProvider,
	col string,
	value any,
	operator traceqlparser.BinaryOp,
) (RowSet, error) {
	switch operator {
	case traceqlparser.OpEq:
		return provider.ScanEqual(col, value)
	case traceqlparser.OpNeq:
		return provider.ScanNotEqual(col, value)
	case traceqlparser.OpGt:
		return provider.ScanGreaterThan(col, value)
	case traceqlparser.OpGte:
		return provider.ScanGreaterThanOrEqual(col, value)
	case traceqlparser.OpLt:
		return provider.ScanLessThan(col, value)
	case traceqlparser.OpLte:
		return provider.ScanLessThanOrEqual(col, value)
	case traceqlparser.OpRegex:
		if strVal, ok := value.(string); ok {
			return provider.ScanRegex(col, strVal)
		}
		return nil, fmt.Errorf("regex operator requires string value")
	case traceqlparser.OpNotRegex:
		if strVal, ok := value.(string); ok {
			return provider.ScanRegexNotMatch(col, strVal)
		}
		return nil, fmt.Errorf("not regex operator requires string value")
	default:
		return nil, fmt.Errorf("unsupported operator: %s", operator.String())
	}
}

// scanColumnRegexFast dispatches a pre-compiled regex scan (match or not-match).
func scanColumnRegexFast(
	provider ColumnDataProvider,
	col string,
	re *regexp.Regexp,
	prefixes []string,
	notMatch bool,
) (RowSet, error) {
	if notMatch {
		return provider.ScanRegexNotMatchFast(col, re, prefixes)
	}
	return provider.ScanRegexFast(col, re, prefixes)
}

// gatherOrEqualAny walks an OR expression tree and returns the common FieldExpr and the
// list of converted scan values if — and only if — every leaf is an equality comparison
// against the same (Scope, Name) field. Returns (nil, nil) for any other pattern.
//
// Examples that match:
//
//	svc="grafana" || svc="loki"
//	resource.service.name="A" || resource.service.name="B" || resource.service.name="C"
//
// Examples that do NOT match (fall through to the general OR path):
//
//	svc="A" || span.http.method="GET"   (different fields)
//	svc="A" || svc=~"loki.*"           (non-equality operator)
func gatherOrEqualAny(expr traceqlparser.Expr) (*traceqlparser.FieldExpr, []any) {
	var commonField *traceqlparser.FieldExpr
	var values []any

	var walk func(e traceqlparser.Expr) bool
	walk = func(e traceqlparser.Expr) bool {
		bin, ok := e.(*traceqlparser.BinaryExpr)
		if !ok {
			return false
		}
		if bin.Op == traceqlparser.OpOr {
			return walk(bin.Left) && walk(bin.Right)
		}
		if bin.Op != traceqlparser.OpEq {
			return false
		}
		field, ok := bin.Left.(*traceqlparser.FieldExpr)
		if !ok {
			return false
		}
		lit, ok := bin.Right.(*traceqlparser.LiteralExpr)
		if !ok {
			return false
		}
		if commonField == nil {
			commonField = field
		} else if field.Scope != commonField.Scope || field.Name != commonField.Name {
			return false // different field — cannot merge
		}
		v := convertLiteralToScanValue(normalizeAttributePath(field.Scope, field.Name), lit)
		values = append(values, v)
		return true
	}

	if !walk(expr) {
		return nil, nil
	}
	if len(values) < 2 {
		// Single value: no benefit over the regular ScanEqual path.
		return nil, nil
	}
	return commonField, values
}

// convertLiteralToScanValue applies the same value conversions as
// compileColumnPredicateComparison does before calling ScanEqual:
// hex decoding for trace:id / span:id, and type conversion for
// non-string literals (duration, status, kind, …).
func convertLiteralToScanValue(attrName string, lit *traceqlparser.LiteralExpr) any {
	value := lit.Value
	if isHexBytesAttribute(attrName) {
		if hexStr, ok := value.(string); ok {
			if decoded, err := hex.DecodeString(hexStr); err == nil {
				return decoded
			}
			// Invalid hex: keep original string, matching compileColumnPredicateComparison behavior.
		}
		return value
	}
	if lit.Type != traceqlparser.LitString {
		if converted, ok := convertTraceQLLiteralToValue(lit); ok {
			return converted.Data
		}
	}
	return value
}

// isHexBytesAttribute checks if an attribute stores hex-encoded bytes (trace:id, span:id)
func isHexBytesAttribute(path string) bool {
	switch path {
	case intrinsicTraceID, intrinsicSpanID, intrinsicSpanParentID:
		return true
	default:
		return false
	}
}

// isComputedField checks if a column name represents a computed field
func isComputedField(columnName string) bool {
	return computedFields[columnName]
}

// unscopedCols returns the three scoped column names for an unscoped attribute.
func unscopedCols(name string) (resource, span, log string) {
	return "resource." + name, "span." + name, "log." + name
}

// isBuiltInField checks if a field is a built-in intrinsic field
func isBuiltInField(attrPath string) bool {
	switch attrPath {
	case "span:name", "span:duration", "span:kind", "span:status", "span:status_message",
		intrinsicTraceID, intrinsicSpanID, intrinsicSpanParentID, "span:start", "span:end",
		"log:timestamp", "log:observed_timestamp", "log:body", "log:severity_number",
		"log:severity_text", "log:trace_id", "log:span_id", "log:flags":
		return true
	default:
		return false
	}
}

// extractTraceQLPredicates walks the TraceQL expression tree and returns QueryPredicates
// for block-level pruning. Unscoped attributes are expanded to OR composites covering
// resource.*, span.*, and log.* children so range-index lookup works on real scoped columns.
func extractTraceQLPredicates(expr traceqlparser.Expr) *QueryPredicates {
	nodes, cols := extractTraceQLNodes(expr)
	if len(nodes) == 0 && len(cols) == 0 {
		return nil
	}
	return &QueryPredicates{Nodes: nodes, Columns: cols}
}

// populateVectorNodes walks the QueryPredicates nodes and sets QueryVector on any
// node with Column == embeddingColumnName (emitted by VectorExpr handling in extractTraceQLNodes).
// This is called after compilation so the pre-computed query vector is available.
func populateVectorNodes(preds *QueryPredicates, queryVec []float32) {
	if preds == nil {
		return
	}
	for i := range preds.Nodes {
		populateVectorNode(&preds.Nodes[i], queryVec)
	}
}

func populateVectorNode(n *RangeNode, queryVec []float32) {
	if len(n.Children) > 0 {
		for i := range n.Children {
			populateVectorNode(&n.Children[i], queryVec)
		}
		return
	}
	if n.Column == embeddingColumnName && n.QueryVector == nil {
		n.QueryVector = queryVec
	}
}

// extractTraceQLNodes recursively extracts block-pruning nodes and accessed columns.
//
// AND: left and right nodes concatenated (AND-combined at top level by the planner).
// OR:  if both sides produce nodes, wraps in a single OR composite; if either side is
//
//	unconstrained (no nodes), the entire OR is unconstrained (returns nil nodes).
//
// Unscoped attribute predicates are expanded into OR composites covering all three scopes.
// Negation operators (!= , !~) produce no nodes but do add to cols (for row-level decode).
func extractTraceQLNodes(expr traceqlparser.Expr) (nodes []RangeNode, cols []string) {
	// VectorExpr: emit a placeholder RangeNode for centroid pruning.
	// QueryVector is nil here; populateVectorNodes fills it in after compilation.
	// Use the correct embedding column: __embedding__ for VECTOR_AI, __embedding_all__ for VECTOR_ALL.
	if ve, ok := expr.(*traceqlparser.VectorExpr); ok {
		col := embeddingColumnName
		if ve.Mode == traceqlparser.VectorModeAll {
			col = "__embedding_all__"
		}
		return []RangeNode{{
			Column:          col,
			VectorThreshold: DefaultVectorThreshold,
		}}, []string{col}
	}

	e, ok := expr.(*traceqlparser.BinaryExpr)
	if !ok {
		return nil, nil
	}

	switch e.Op {
	case traceqlparser.OpAnd:
		ln, lc := extractTraceQLNodes(e.Left)
		rn, rc := extractTraceQLNodes(e.Right)
		return append(ln, rn...), dedupStrings(append(lc, rc...))

	case traceqlparser.OpOr:
		ln, lc := extractTraceQLNodes(e.Left)
		rn, rc := extractTraceQLNodes(e.Right)
		cols = dedupStrings(append(lc, rc...))
		if len(ln) == 0 || len(rn) == 0 {
			// One side of the user-written OR is unconstrained (e.g. involves a computed
			// column or a negation). A block satisfying the unconstrained arm might match
			// anything, so we cannot prune at all — return nil nodes.
			// Note: this is different from unscoped-attribute OR composites, where each
			// child is a scoped variant of the same predicate; those are built in
			// extractEqNode/extractRangeNode/extractRegexNode with known columns.
			return nil, cols
		}
		return []RangeNode{{IsOR: true, Children: append(ln, rn...)}}, cols

	case traceqlparser.OpEq:
		return extractEqNode(e)

	case traceqlparser.OpNeq, traceqlparser.OpNotRegex:
		// Negations cannot prune; just track accessed columns for the row-level decode.
		return nil, negationCols(e)

	case traceqlparser.OpGt, traceqlparser.OpGte, traceqlparser.OpLt, traceqlparser.OpLte:
		return extractRangeNode(e)

	case traceqlparser.OpRegex:
		return extractRegexNode(e)
	}
	return nil, nil
}

// NOTE-044: parseBinaryExprArgs — shared preamble for extract*Node helpers.
// See internal/vm/NOTES.md §NOTE-044.
//
// parseBinaryExprArgs extracts the common preamble shared by extractEqNode,
// extractRangeNode, and extractRegexNode: left must be FieldExpr, right must be
// LiteralExpr, the column name must not be a computed field.
//
// Guard order: isComputedField is checked before lit.Type in this function.
// extractRegexNode performs the additional LitString guard after parseBinaryExprArgs
// returns. The original per-function code checked lit.Type first (before isComputedField)
// in extractRegexNode, but since both guards return (nil, nil), the behavior is identical.
func parseBinaryExprArgs(
	expr *traceqlparser.BinaryExpr,
) (field *traceqlparser.FieldExpr, lit *traceqlparser.LiteralExpr, columnName string, ok bool) {
	f, fok := expr.Left.(*traceqlparser.FieldExpr)
	if !fok {
		return nil, nil, "", false
	}
	l, lok := expr.Right.(*traceqlparser.LiteralExpr)
	if !lok {
		return nil, nil, "", false
	}
	col := normalizeAttributePath(f.Scope, f.Name)
	if isComputedField(col) {
		return nil, nil, "", false
	}
	return f, l, col, true
}

// extractEqNode builds a RangeNode for an equality predicate (OpEq).
// Unscoped user attributes expand to an OR composite over all three scopes.
func extractEqNode(expr *traceqlparser.BinaryExpr) (nodes []RangeNode, cols []string) {
	field, lit, columnName, ok := parseBinaryExprArgs(expr)
	if !ok {
		return nil, nil
	}
	vmValue, ok := convertTraceQLLiteralToValue(lit)
	if !ok {
		return nil, nil
	}
	// Hex-decode trace:id / span:id / span:parent_id values.
	if isHexBytesAttribute(columnName) {
		if lit.Type == traceqlparser.LitString {
			if hexStr, ok2 := lit.Value.(string); ok2 {
				if decoded, err := hex.DecodeString(hexStr); err == nil && len(decoded) == 16 {
					vmValue = Value{Type: TypeBytes, Data: decoded}
				}
			}
		}
	}

	if field.Scope == "" && !isBuiltInField(columnName) {
		// Unscoped: expand to OR across resource.*, span.*, log.*
		res, span, log := unscopedCols(field.Name)
		cols = []string{res, span, log}
		return []RangeNode{{
			IsOR: true,
			Children: []RangeNode{
				{Column: res, Values: []Value{vmValue}},
				{Column: span, Values: []Value{vmValue}},
				{Column: log, Values: []Value{vmValue}},
			},
		}}, cols
	}
	// Scoped: single leaf node.
	if !isBuiltInField(columnName) {
		cols = []string{columnName}
	}
	return []RangeNode{{Column: columnName, Values: []Value{vmValue}}}, cols
}

// extractRangeNode builds a RangeNode for a range predicate (>, >=, <, <=).
// Unscoped user attributes expand to an OR composite over all three scopes.
func extractRangeNode(expr *traceqlparser.BinaryExpr) (nodes []RangeNode, cols []string) {
	field, lit, columnName, ok := parseBinaryExprArgs(expr)
	if !ok {
		return nil, nil
	}
	vmValue, ok := convertTraceQLLiteralToValue(lit)
	if !ok {
		return nil, nil
	}

	var minVal, maxVal *Value
	var minInclusive, maxInclusive bool
	switch expr.Op {
	case traceqlparser.OpGt:
		v := vmValue
		minVal = &v
		minInclusive = false
	case traceqlparser.OpGte:
		v := vmValue
		minVal = &v
		minInclusive = true
	case traceqlparser.OpLt:
		v := vmValue
		maxVal = &v
		maxInclusive = false
	case traceqlparser.OpLte:
		v := vmValue
		maxVal = &v
		maxInclusive = true
	}

	if field.Scope == "" && !isBuiltInField(columnName) {
		res, span, log := unscopedCols(field.Name)
		cols = []string{res, span, log}
		return []RangeNode{{
			IsOR: true,
			Children: []RangeNode{
				{Column: res, Min: minVal, Max: maxVal, MinInclusive: minInclusive, MaxInclusive: maxInclusive},
				{Column: span, Min: minVal, Max: maxVal, MinInclusive: minInclusive, MaxInclusive: maxInclusive},
				{Column: log, Min: minVal, Max: maxVal, MinInclusive: minInclusive, MaxInclusive: maxInclusive},
			},
		}}, cols
	}
	if !isBuiltInField(columnName) {
		cols = []string{columnName}
	}
	return []RangeNode{
		{Column: columnName, Min: minVal, Max: maxVal, MinInclusive: minInclusive, MaxInclusive: maxInclusive},
	}, cols
}

// extractRegexNode builds a RangeNode for a regex predicate (=~).
// Unscoped user attributes expand to an OR composite over all three scopes.
func extractRegexNode(expr *traceqlparser.BinaryExpr) (nodes []RangeNode, cols []string) {
	field, lit, columnName, ok := parseBinaryExprArgs(expr)
	if !ok || lit.Type != traceqlparser.LitString {
		return nil, nil
	}
	pattern, ok := lit.Value.(string)
	if !ok {
		return nil, nil
	}

	if field.Scope == "" && !isBuiltInField(columnName) {
		res, span, log := unscopedCols(field.Name)
		cols = []string{res, span, log}
		return []RangeNode{{
			IsOR: true,
			Children: []RangeNode{
				{Column: res, Pattern: pattern},
				{Column: span, Pattern: pattern},
				{Column: log, Pattern: pattern},
			},
		}}, cols
	}
	if !isBuiltInField(columnName) {
		cols = []string{columnName}
	}
	return []RangeNode{{Column: columnName, Pattern: pattern}}, cols
}

// negationCols returns the accessed column names for a negation operator (!=, !~).
// These columns are needed for the row-level decode but produce NO pruning nodes.
//
// Negation cannot prune blocks: a block where service.name != "prod" might still contain
// spans with service.name = "prod" (the negation is satisfied by OTHER spans in that block).
// Pruning would cause false negatives. Column names are added to QueryPredicates.Columns
// so ProgramWantColumns includes them in the two-pass first-pass column set.
func negationCols(expr *traceqlparser.BinaryExpr) []string {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return nil
	}
	columnName := normalizeAttributePath(field.Scope, field.Name)
	if isComputedField(columnName) || isBuiltInField(columnName) {
		return nil
	}
	if field.Scope == "" {
		res, span, log := unscopedCols(field.Name)
		return []string{res, span, log}
	}
	return []string{columnName}
}

// dedupStrings returns a slice with duplicate strings removed, preserving order.
func dedupStrings(ss []string) []string {
	if len(ss) == 0 {
		return ss
	}
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// convertTraceQLLiteralToValue converts a TraceQL LiteralExpr to a vm.Value
func convertTraceQLLiteralToValue(lit *traceqlparser.LiteralExpr) (Value, bool) {
	switch lit.Type {
	case traceqlparser.LitString:
		if strVal, ok := lit.Value.(string); ok {
			return Value{Type: TypeString, Data: strVal}, true
		}
	case traceqlparser.LitInt:
		if intVal, ok := lit.Value.(int64); ok {
			return Value{Type: TypeInt, Data: intVal}, true
		}
	case traceqlparser.LitFloat:
		if floatVal, ok := lit.Value.(float64); ok {
			return Value{Type: TypeFloat, Data: floatVal}, true
		}
	case traceqlparser.LitBool:
		if boolVal, ok := lit.Value.(bool); ok {
			return Value{Type: TypeBool, Data: boolVal}, true
		}
	case traceqlparser.LitDuration:
		if durVal, ok := lit.Value.(int64); ok {
			return Value{Type: TypeDuration, Data: durVal}, true
		}
	case traceqlparser.LitStatus:
		// Status enums are stored as int64 in the dedicated column index.
		// Convert from TraceQL string to the proto-defined int64 value.
		if strVal, ok := lit.Value.(string); ok {
			if intVal, ok := statusCodeToInt64(strVal); ok {
				return Value{Type: TypeInt, Data: intVal}, true
			}
		}
	case traceqlparser.LitKind:
		// Kind enums are stored as int64 in the dedicated column index.
		// Convert from TraceQL string to the proto-defined int64 value.
		if strVal, ok := lit.Value.(string); ok {
			if intVal, ok := spanKindToInt64(strVal); ok {
				return Value{Type: TypeInt, Data: intVal}, true
			}
		}
	}
	return Value{}, false
}

// spanKindToInt64 converts a TraceQL kind literal to its OpenTelemetry proto int64 value.
// Values match otlptrace.Span_SpanKind.
func spanKindToInt64(kind string) (int64, bool) {
	switch kind {
	case "unspecified":
		return 0, true
	case "internal":
		return 1, true
	case "server":
		return 2, true
	case "client":
		return 3, true
	case "producer":
		return 4, true
	case "consumer":
		return 5, true
	}
	return 0, false
}

// statusCodeToInt64 converts a TraceQL status literal to its OpenTelemetry proto int64 value.
// Values match otlptrace.Status_StatusCode.
func statusCodeToInt64(status string) (int64, bool) {
	switch status {
	case "unset":
		return 0, true
	case "ok":
		return 1, true
	case "error":
		return 2, true
	}
	return 0, false
}

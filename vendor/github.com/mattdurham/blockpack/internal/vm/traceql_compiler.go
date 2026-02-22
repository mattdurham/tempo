package vm

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mattdurham/blockpack/internal/traceqlparser"
)

// Intrinsic field constants (duplicated to avoid import cycles)
const (
	intrinsicTraceID      = "trace:id"
	intrinsicSpanID       = "span:id"
	intrinsicSpanParentID = "span:parent_id"
)

// CompileTraceQLFilter compiles a TraceQL FilterExpression directly to a VM Program.
// Generates BOTH ColumnPredicate (fast bulk scanning) and Instructions (flexibility).
// This bypasses SQL entirely, allowing TraceQL to use its full expressiveness.
func CompileTraceQLFilter(filter *traceqlparser.FilterExpression) (*Program, error) {
	if filter == nil || filter.Expr == nil {
		// Empty filter matches all spans
		return compileMatchAllProgram(), nil
	}

	compiler := &traceqlCompiler{
		program: &Program{
			Instructions: make([]InstructionFunc, 0),
			Constants:    make([]Value, 0),
			Attributes:   make([]string, 0),
		},
		constMap: make(map[interface{}]int),
		attrMap:  make(map[string]int),
	}

	// Compile to Instructions (for VM execution)
	err := compiler.compileExpression(filter.Expr)
	if err != nil {
		return nil, fmt.Errorf("compile expression: %w", err)
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

type traceqlCompiler struct {
	program  *Program
	constMap map[interface{}]int
	attrMap  map[string]int
}

// normalizeAttributePath converts TraceQL attribute paths to canonical column names.
// This matches the normalization logic in executor/attribute_mapping.go to ensure
// consistent behavior across the codebase.
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

	// If already has : notation, pass through
	if strings.Contains(fullPath, ":") {
		return fullPath
	}

	// Handle unscoped span intrinsics (TraceQL compatibility)
	switch fullPath {
	case "name", "span.name":
		return "span:name"
	case "kind", "span.kind":
		return "span:kind"
	case "status", "span.status":
		return "span:status"
	case "status_message", "span.status_message":
		return "span:status_message"
	case "duration", "span.duration":
		return "span:duration"
	case "start", "span.start":
		return "span:start"
	case "end", "span.end":
		return "span:end"
	// Handle trace-level intrinsics
	case "trace.id":
		return intrinsicTraceID
	case "span.id":
		return intrinsicSpanID
	case "span.parent_id":
		return intrinsicSpanParentID
	}

	// Handle well-known resource attributes
	if scope == "" && isWellKnownResourceAttribute(fullPath) {
		return "resource." + fullPath
	}

	// For scoped paths, return as-is
	return fullPath
}

// isWellKnownResourceAttribute checks if an attribute is a well-known resource attribute
// that can be referenced without the "resource." prefix in TraceQL.
func isWellKnownResourceAttribute(name string) bool {
	switch name {
	case "service.name",
		"service.namespace",
		"service.instance.id",
		"deployment.environment",
		"cluster",
		"namespace",
		"pod",
		"container",
		"k8s.cluster.name",
		"k8s.namespace.name",
		"k8s.pod.name",
		"k8s.container.name",
		"http.status_code",
		"http.method",
		"http.url",
		"http.target",
		"http.host":
		return true
	default:
		return false
	}
}

func (c *traceqlCompiler) compileExpression(expr traceqlparser.Expr) error {
	switch e := expr.(type) {
	case *traceqlparser.BinaryExpr:
		return c.compileBinaryExpr(e)
	case *traceqlparser.FieldExpr:
		return c.compileFieldExpr(e)
	case *traceqlparser.LiteralExpr:
		return c.compileLiteralExpr(e)
	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (c *traceqlCompiler) compileBinaryExpr(expr *traceqlparser.BinaryExpr) error {
	switch expr.Op {
	case traceqlparser.OpAnd:
		if err := c.compileExpression(expr.Left); err != nil {
			return err
		}
		if err := c.compileExpression(expr.Right); err != nil {
			return err
		}
		c.emit(func(vm *VM) int {
			right := vm.pop()
			left := vm.pop()
			// Type check before assertion to prevent panics
			if left.Type != TypeBool || right.Type != TypeBool {
				vm.push(Value{Type: TypeBool, Data: false})
				return vm.pc + 1
			}
			leftBool, okL := left.Data.(bool)
			rightBool, okR := right.Data.(bool)
			if !okL || !okR {
				vm.push(Value{Type: TypeBool, Data: false})
				return vm.pc + 1
			}
			result := leftBool && rightBool
			vm.push(Value{Type: TypeBool, Data: result})
			return vm.pc + 1
		})
		return nil

	case traceqlparser.OpOr:
		if err := c.compileExpression(expr.Left); err != nil {
			return err
		}
		if err := c.compileExpression(expr.Right); err != nil {
			return err
		}
		c.emit(func(vm *VM) int {
			right := vm.pop()
			left := vm.pop()
			// Type check before assertion to prevent panics
			if left.Type != TypeBool || right.Type != TypeBool {
				vm.push(Value{Type: TypeBool, Data: false})
				return vm.pc + 1
			}
			leftBool, okL := left.Data.(bool)
			rightBool, okR := right.Data.(bool)
			if !okL || !okR {
				vm.push(Value{Type: TypeBool, Data: false})
				return vm.pc + 1
			}
			result := leftBool || rightBool
			vm.push(Value{Type: TypeBool, Data: result})
			return vm.pc + 1
		})
		return nil

	case traceqlparser.OpEq,
		traceqlparser.OpNeq,
		traceqlparser.OpGt,
		traceqlparser.OpGte,
		traceqlparser.OpLt,
		traceqlparser.OpLte,
		traceqlparser.OpRegex,
		traceqlparser.OpNotRegex:
		return c.compileComparison(expr)

	default:
		return fmt.Errorf("unsupported binary operator: %s", expr.Op.String())
	}
}

func (c *traceqlCompiler) compileComparison(expr *traceqlparser.BinaryExpr) error {
	// Note: Regex operators (=~, !~) compile successfully but always return false in VM execution
	// The ColumnPredicate path supports regex and will be used for actual evaluation
	// This allows the program to compile even with regex, enabling ColumnPredicate-only execution

	fieldExpr, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return fmt.Errorf("comparison left side must be field reference, got %T", expr.Left)
	}

	literalExpr, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return fmt.Errorf("comparison right side must be literal, got %T", expr.Right)
	}

	// Normalize attribute path using canonical mapping
	fullAttrName := normalizeAttributePath(fieldExpr.Scope, fieldExpr.Name)

	// Special handling for hex bytes attributes (trace:id, span:id)
	// Decode hex strings to bytes for proper comparison
	decodedLiteral := literalExpr
	if isHexBytesAttribute(fullAttrName) && literalExpr.Type == traceqlparser.LitString {
		if hexStr, ok := literalExpr.Value.(string); ok {
			if decoded, err := hex.DecodeString(hexStr); err == nil {
				// Create new literal with decoded bytes
				decodedLiteral = &traceqlparser.LiteralExpr{
					Value: decoded,
					Type:  traceqlparser.LitString, // Keep as string type but with bytes value
				}
			}
		}
	}

	attrIdx := c.addAttribute(fullAttrName)
	constIdx := c.addConstantFromLiteral(decodedLiteral)
	operator := expr.Op

	c.emit(func(vm *VM) int {
		attrValue := vm.loadAttribute(attrIdx)
		expectedValue := vm.program.Constants[constIdx]
		result := compareValues(attrValue, expectedValue, operator)
		vm.push(Value{Type: TypeBool, Data: result})
		return vm.pc + 1
	})

	return nil
}

func (c *traceqlCompiler) compileFieldExpr(field *traceqlparser.FieldExpr) error {
	// Normalize attribute path using canonical mapping
	fullAttrName := normalizeAttributePath(field.Scope, field.Name)
	attrIdx := c.addAttribute(fullAttrName)

	c.emit(func(vm *VM) int {
		value := vm.loadAttribute(attrIdx)
		vm.push(value)
		return vm.pc + 1
	})

	return nil
}

func (c *traceqlCompiler) compileLiteralExpr(lit *traceqlparser.LiteralExpr) error {
	constIdx := c.addConstant(lit.Value)

	c.emit(func(vm *VM) int {
		vm.push(vm.program.Constants[constIdx])
		return vm.pc + 1
	})

	return nil
}

// addConstantFromLiteral adds a constant from a TraceQL LiteralExpr, preserving semantic type information.
// This is critical for correct comparisons (e.g., duration > 100ms).
func (c *traceqlCompiler) addConstantFromLiteral(lit *traceqlparser.LiteralExpr) int {
	// Create a hashable key for the constants map
	// For []byte values, convert to string for map key
	mapKey := lit.Value
	if bytes, ok := lit.Value.([]byte); ok {
		mapKey = string(bytes)
	}

	if idx, exists := c.constMap[mapKey]; exists {
		return idx
	}

	idx := len(c.program.Constants)
	c.constMap[mapKey] = idx

	var vmValue Value
	switch lit.Type {
	case traceqlparser.LitString:
		// Check if value is []byte (from hex decoding)
		if bytes, ok := lit.Value.([]byte); ok {
			vmValue = Value{Type: TypeBytes, Data: bytes}
		} else if str, ok := lit.Value.(string); ok {
			vmValue = Value{Type: TypeString, Data: str}
		}
	case traceqlparser.LitInt:
		if intVal, ok := lit.Value.(int64); ok {
			vmValue = Value{Type: TypeInt, Data: intVal}
		}
	case traceqlparser.LitFloat:
		if floatVal, ok := lit.Value.(float64); ok {
			vmValue = Value{Type: TypeFloat, Data: floatVal}
		}
	case traceqlparser.LitBool:
		if boolVal, ok := lit.Value.(bool); ok {
			vmValue = Value{Type: TypeBool, Data: boolVal}
		}
	case traceqlparser.LitDuration:
		// Duration stored as int64 nanoseconds with TypeDuration semantic type
		if durVal, ok := lit.Value.(int64); ok {
			vmValue = Value{Type: TypeDuration, Data: durVal}
		}
	default:
		// LitStatus, LitKind, LitNil - treat as nil for now
		vmValue = Value{Type: TypeNil, Data: nil}
	}

	c.program.Constants = append(c.program.Constants, vmValue)
	return idx
}

func (c *traceqlCompiler) addConstant(value interface{}) int {
	if idx, exists := c.constMap[value]; exists {
		return idx
	}

	idx := len(c.program.Constants)
	c.constMap[value] = idx

	var vmValue Value
	switch v := value.(type) {
	case string:
		vmValue = Value{Type: TypeString, Data: v}
	case int64:
		vmValue = Value{Type: TypeInt, Data: v}
	case float64:
		vmValue = Value{Type: TypeFloat, Data: v}
	case bool:
		vmValue = Value{Type: TypeBool, Data: v}
	default:
		vmValue = Value{Type: TypeNil, Data: nil}
	}

	c.program.Constants = append(c.program.Constants, vmValue)
	return idx
}

func (c *traceqlCompiler) addAttribute(name string) int {
	if idx, exists := c.attrMap[name]; exists {
		return idx
	}

	idx := len(c.program.Attributes)
	c.attrMap[name] = idx
	c.program.Attributes = append(c.program.Attributes, name)
	return idx
}

func (c *traceqlCompiler) emit(instr InstructionFunc) {
	c.program.Instructions = append(c.program.Instructions, instr)
}

func compileMatchAllProgram() *Program {
	return &Program{
		Instructions: []InstructionFunc{
			func(vm *VM) int {
				vm.push(Value{Type: TypeBool, Data: true})
				return vm.pc + 1
			},
		},
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
	case traceqlparser.OpRegex, traceqlparser.OpNotRegex:
		// TODO: implement regex support
		return false
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
		// Standalone field reference - check if it exists (is not null)
		scope := e.Scope
		if scope == "" {
			scope = "span"
		}
		var attrName string
		if strings.HasPrefix(e.Name, ":") {
			attrName = scope + e.Name
		} else {
			attrName = scope + "." + e.Name
		}
		return func(provider ColumnDataProvider) (RowSet, error) {
			return provider.ScanIsNotNull(attrName)
		}, nil
	case *traceqlparser.LiteralExpr:
		// Literal boolean value
		if e.Type == traceqlparser.LitBool {
			if e.Value.(bool) {
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
	default:
		return nil, fmt.Errorf("unsupported expression type for column predicate: %T", expr)
	}
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
		// OR: union left and right results
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

	// Generate appropriate column scan
	return func(provider ColumnDataProvider) (RowSet, error) {
		switch operator {
		case traceqlparser.OpEq:
			return provider.ScanEqual(attrName, value)
		case traceqlparser.OpNeq:
			return provider.ScanNotEqual(attrName, value)
		case traceqlparser.OpGt:
			return provider.ScanGreaterThan(attrName, value)
		case traceqlparser.OpGte:
			return provider.ScanGreaterThanOrEqual(attrName, value)
		case traceqlparser.OpLt:
			return provider.ScanLessThan(attrName, value)
		case traceqlparser.OpLte:
			return provider.ScanLessThanOrEqual(attrName, value)
		case traceqlparser.OpRegex:
			if strVal, ok := value.(string); ok {
				return provider.ScanRegex(attrName, strVal)
			}
			return nil, fmt.Errorf("regex operator requires string value")
		case traceqlparser.OpNotRegex:
			if strVal, ok := value.(string); ok {
				return provider.ScanRegexNotMatch(attrName, strVal)
			}
			return nil, fmt.Errorf("not regex operator requires string value")
		default:
			return nil, fmt.Errorf("unsupported operator: %s", operator.String())
		}
	}, nil
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

// computedFields are fields calculated at query time that do NOT exist as stored columns.
// Extracting them into predicates causes false negatives (blocks rejected when they contain matching spans).
var computedFields = map[string]bool{
	"span.leaf":       true,
	"span.root":       true,
	"span.childCount": true,
}

// isComputedField checks if a column name represents a computed field
func isComputedField(columnName string) bool {
	return computedFields[columnName]
}

// isBuiltInField checks if a field is a built-in intrinsic field
func isBuiltInField(attrPath string) bool {
	switch attrPath {
	case "span:name", "span:duration", "span:kind", "span:status", "span:status_message",
		intrinsicTraceID, intrinsicSpanID, intrinsicSpanParentID, "span:start", "span:end":
		return true
	default:
		return false
	}
}

// extractTraceQLPredicates walks the TraceQL expression tree and extracts
// QueryPredicates for block-level filtering. Returns populated predicates for
// all stored-field operators (equality, range, regex).
func extractTraceQLPredicates(expr traceqlparser.Expr) *QueryPredicates {
	predicates := &QueryPredicates{
		AttributeEquals:       make(map[string][]Value),
		DedicatedColumns:      make(map[string][]Value),
		UnscopedColumnNames:   make(map[string][]string),
		DedicatedColumnsRegex: make(map[string]string),
		AttributeRanges:       make(map[string]*RangePredicate),
		DedicatedRanges:       make(map[string]*RangePredicate),
	}
	extractTraceQLPredicatesRecursive(expr, predicates)

	// Return nil only if we extracted absolutely nothing
	hasAnyPredicates := len(predicates.DedicatedColumns) > 0 ||
		len(predicates.AttributeEquals) > 0 ||
		len(predicates.DedicatedRanges) > 0 ||
		len(predicates.AttributeRanges) > 0 ||
		len(predicates.DedicatedColumnsRegex) > 0 ||
		len(predicates.AttributesAccessed) > 0

	if !hasAnyPredicates {
		return nil
	}

	return predicates
}

// extractTraceQLPredicatesRecursive recursively walks the expression tree and extracts predicates.
// Always returns true now - we extract what we can and skip what we can't.
func extractTraceQLPredicatesRecursive(expr traceqlparser.Expr, predicates *QueryPredicates) bool {
	switch e := expr.(type) {
	case *traceqlparser.BinaryExpr:
		switch e.Op {
		case traceqlparser.OpAnd:
			extractTraceQLPredicatesRecursive(e.Left, predicates)
			extractTraceQLPredicatesRecursive(e.Right, predicates)
			return true
		case traceqlparser.OpOr:
			predicates.HasOROperations = true
			extractTraceQLPredicatesRecursive(e.Left, predicates)
			extractTraceQLPredicatesRecursive(e.Right, predicates)
			return true
		case traceqlparser.OpEq:
			return extractTraceQLEqualityPredicate(e, predicates)
		case traceqlparser.OpNeq:
			// Track attribute access for bloom filter but don't add to equality predicates
			// (NOT EQUAL can't be used for positive block pruning)
			return extractTraceQLNegationPredicate(e, predicates)
		case traceqlparser.OpGt, traceqlparser.OpGte, traceqlparser.OpLt, traceqlparser.OpLte:
			return extractTraceQLRangePredicate(e, predicates)
		case traceqlparser.OpRegex:
			return extractTraceQLRegexPredicate(e, predicates)
		case traceqlparser.OpNotRegex:
			// Track attribute access only (negative regex can't prune)
			return extractTraceQLNegationPredicate(e, predicates)
		default:
			// Unrecognized operator - skip extraction but don't fail
			return true
		}
	default:
		// Unrecognized expression type - skip extraction but don't fail
		return true
	}
}

// extractTraceQLEqualityPredicate extracts equality predicates for all stored fields
func extractTraceQLEqualityPredicate(expr *traceqlparser.BinaryExpr, predicates *QueryPredicates) bool {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return true // Skip but don't fail
	}
	lit, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return true // Skip but don't fail
	}

	columnName := normalizeAttributePath(field.Scope, field.Name)

	// Skip computed fields - they don't exist as stored columns
	if isComputedField(columnName) {
		return true
	}

	// Convert literal to vm.Value
	vmValue, ok := convertTraceQLLiteralToValue(lit)
	if !ok {
		return true // Skip unsupported literal types
	}

	// Special case for trace:id and span:id: decode hex string to bytes
	if isHexBytesAttribute(columnName) {
		if lit.Type == traceqlparser.LitString {
			if hexStr, ok := lit.Value.(string); ok {
				decoded, err := hex.DecodeString(hexStr)
				if err == nil && len(decoded) == 16 {
					vmValue = Value{Type: TypeBytes, Data: decoded}
				}
			}
		}
	}

	// isDedicatedColumn always returns true (all columns get dedicated index treatment)
	// Add to DedicatedColumns
	predicates.DedicatedColumns[columnName] = append(
		predicates.DedicatedColumns[columnName],
		vmValue,
	)

	// For non-built-in fields (user attributes), also track in AttributesAccessed
	// for bloom filter column name checking
	if !isBuiltInField(columnName) {
		if !contains(predicates.AttributesAccessed, columnName) {
			predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
		}
	}

	return true
}

// extractTraceQLNegationPredicate tracks attribute access for negation operators (!=, !~)
// without adding predicates (negations can't be used for positive block pruning)
func extractTraceQLNegationPredicate(expr *traceqlparser.BinaryExpr, predicates *QueryPredicates) bool {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return true
	}

	columnName := normalizeAttributePath(field.Scope, field.Name)

	// Skip computed fields
	if isComputedField(columnName) {
		return true
	}

	// Track attribute access for bloom filter
	if !isBuiltInField(columnName) {
		if !contains(predicates.AttributesAccessed, columnName) {
			predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
		}
	}

	return true
}

// extractTraceQLRangePredicate extracts range predicates (>, >=, <, <=)
func extractTraceQLRangePredicate(expr *traceqlparser.BinaryExpr, predicates *QueryPredicates) bool {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return true
	}
	lit, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return true
	}

	columnName := normalizeAttributePath(field.Scope, field.Name)

	// Skip computed fields
	if isComputedField(columnName) {
		return true
	}

	// Convert literal to vm.Value
	vmValue, ok := convertTraceQLLiteralToValue(lit)
	if !ok {
		return true
	}

	// Get or create range predicate for this column
	// Since isDedicatedColumn always returns true, use DedicatedRanges
	rangePred := predicates.DedicatedRanges[columnName]
	if rangePred == nil {
		rangePred = &RangePredicate{}
		predicates.DedicatedRanges[columnName] = rangePred
	}

	// Update range bounds based on operator
	switch expr.Op {
	case traceqlparser.OpGt:
		rangePred.MinValue = &vmValue
		rangePred.MinInclusive = false
	case traceqlparser.OpGte:
		rangePred.MinValue = &vmValue
		rangePred.MinInclusive = true
	case traceqlparser.OpLt:
		rangePred.MaxValue = &vmValue
		rangePred.MaxInclusive = false
	case traceqlparser.OpLte:
		rangePred.MaxValue = &vmValue
		rangePred.MaxInclusive = true
	}

	// Track attribute access for non-built-in fields
	if !isBuiltInField(columnName) {
		if !contains(predicates.AttributesAccessed, columnName) {
			predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
		}
	}

	return true
}

// extractTraceQLRegexPredicate extracts regex predicates
func extractTraceQLRegexPredicate(expr *traceqlparser.BinaryExpr, predicates *QueryPredicates) bool {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return true
	}
	lit, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return true
	}
	if lit.Type != traceqlparser.LitString {
		return true
	}

	columnName := normalizeAttributePath(field.Scope, field.Name)

	// Skip computed fields
	if isComputedField(columnName) {
		return true
	}

	regexPattern, ok := lit.Value.(string)
	if !ok {
		return true
	}

	// Add to DedicatedColumnsRegex
	predicates.DedicatedColumnsRegex[columnName] = regexPattern

	// Track attribute access for non-built-in fields
	if !isBuiltInField(columnName) {
		if !contains(predicates.AttributesAccessed, columnName) {
			predicates.AttributesAccessed = append(predicates.AttributesAccessed, columnName)
		}
	}

	return true
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
		// Convert status enum to canonical string
		// Status enums map to "ok", "error", "unset"
		if strVal, ok := lit.Value.(string); ok {
			return Value{Type: TypeString, Data: strVal}, true
		}
	case traceqlparser.LitKind:
		// Convert kind enum to canonical string
		// Kind enums map to "internal", "server", "client", etc.
		if strVal, ok := lit.Value.(string); ok {
			return Value{Type: TypeString, Data: strVal}, true
		}
	}
	return Value{}, false
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

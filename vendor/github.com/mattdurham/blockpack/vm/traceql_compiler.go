package vm

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mattdurham/blockpack/traceql"
)

// CompileTraceQLFilter compiles a TraceQL FilterExpression directly to a VM Program.
// Generates BOTH ColumnPredicate (fast bulk scanning) and Instructions (flexibility).
// This bypasses SQL entirely, allowing TraceQL to use its full expressiveness.
func CompileTraceQLFilter(filter *traceql.FilterExpression) (*Program, error) {
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
		return "trace:id"
	case "span.id":
		return "span:id"
	case "span.parent_id":
		return "span:parent_id"
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

func (c *traceqlCompiler) compileExpression(expr traceql.Expr) error {
	switch e := expr.(type) {
	case *traceql.BinaryExpr:
		return c.compileBinaryExpr(e)
	case *traceql.FieldExpr:
		return c.compileFieldExpr(e)
	case *traceql.LiteralExpr:
		return c.compileLiteralExpr(e)
	default:
		return fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (c *traceqlCompiler) compileBinaryExpr(expr *traceql.BinaryExpr) error {
	switch expr.Op {
	case traceql.OpAnd:
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
			result := left.Data.(bool) && right.Data.(bool)
			vm.push(Value{Type: TypeBool, Data: result})
			return vm.pc + 1
		})
		return nil

	case traceql.OpOr:
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
			result := left.Data.(bool) || right.Data.(bool)
			vm.push(Value{Type: TypeBool, Data: result})
			return vm.pc + 1
		})
		return nil

	case traceql.OpEq, traceql.OpNeq, traceql.OpGt, traceql.OpGte, traceql.OpLt, traceql.OpLte, traceql.OpRegex, traceql.OpNotRegex:
		return c.compileComparison(expr)

	default:
		return fmt.Errorf("unsupported binary operator: %s", expr.Op.String())
	}
}

func (c *traceqlCompiler) compileComparison(expr *traceql.BinaryExpr) error {
	// Note: Regex operators (=~, !~) compile successfully but always return false in VM execution
	// The ColumnPredicate path supports regex and will be used for actual evaluation
	// This allows the program to compile even with regex, enabling ColumnPredicate-only execution

	fieldExpr, ok := expr.Left.(*traceql.FieldExpr)
	if !ok {
		return fmt.Errorf("comparison left side must be field reference, got %T", expr.Left)
	}

	literalExpr, ok := expr.Right.(*traceql.LiteralExpr)
	if !ok {
		return fmt.Errorf("comparison right side must be literal, got %T", expr.Right)
	}

	// Normalize attribute path using canonical mapping
	fullAttrName := normalizeAttributePath(fieldExpr.Scope, fieldExpr.Name)

	// Special handling for hex bytes attributes (trace:id, span:id)
	// Decode hex strings to bytes for proper comparison
	decodedLiteral := literalExpr
	if isHexBytesAttribute(fullAttrName) && literalExpr.Type == traceql.LitString {
		if hexStr, ok := literalExpr.Value.(string); ok {
			if decoded, err := hex.DecodeString(hexStr); err == nil {
				// Create new literal with decoded bytes
				decodedLiteral = &traceql.LiteralExpr{
					Value: decoded,
					Type:  traceql.LitString, // Keep as string type but with bytes value
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

func (c *traceqlCompiler) compileFieldExpr(field *traceql.FieldExpr) error {
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

func (c *traceqlCompiler) compileLiteralExpr(lit *traceql.LiteralExpr) error {
	constIdx := c.addConstant(lit.Value)

	c.emit(func(vm *VM) int {
		vm.push(vm.program.Constants[constIdx])
		return vm.pc + 1
	})

	return nil
}

// addConstantFromLiteral adds a constant from a TraceQL LiteralExpr, preserving semantic type information.
// This is critical for correct comparisons (e.g., duration > 100ms).
func (c *traceqlCompiler) addConstantFromLiteral(lit *traceql.LiteralExpr) int {
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
	case traceql.LitString:
		// Check if value is []byte (from hex decoding)
		if bytes, ok := lit.Value.([]byte); ok {
			vmValue = Value{Type: TypeBytes, Data: bytes}
		} else {
			vmValue = Value{Type: TypeString, Data: lit.Value.(string)}
		}
	case traceql.LitInt:
		vmValue = Value{Type: TypeInt, Data: lit.Value.(int64)}
	case traceql.LitFloat:
		vmValue = Value{Type: TypeFloat, Data: lit.Value.(float64)}
	case traceql.LitBool:
		vmValue = Value{Type: TypeBool, Data: lit.Value.(bool)}
	case traceql.LitDuration:
		// Duration stored as int64 nanoseconds with TypeDuration semantic type
		vmValue = Value{Type: TypeDuration, Data: lit.Value.(int64)}
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

func compareValues(left, right Value, operator traceql.BinaryOp) bool {
	if left.Type == TypeNil || right.Type == TypeNil {
		if operator == traceql.OpNeq {
			return left.Type != right.Type
		}
		return left.Type == right.Type
	}

	switch operator {
	case traceql.OpEq:
		return valuesEqual(left, right)
	case traceql.OpNeq:
		return !valuesEqual(left, right)
	case traceql.OpGt:
		return compareNumeric(left, right) > 0
	case traceql.OpGte:
		return compareNumeric(left, right) >= 0
	case traceql.OpLt:
		return compareNumeric(left, right) < 0
	case traceql.OpLte:
		return compareNumeric(left, right) <= 0
	case traceql.OpRegex, traceql.OpNotRegex:
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
		return left.Data.(string) == right.Data.(string)
	case TypeInt:
		return left.Data.(int64) == right.Data.(int64)
	case TypeFloat:
		return left.Data.(float64) == right.Data.(float64)
	case TypeBool:
		return left.Data.(bool) == right.Data.(bool)
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
		return float64(v.Data.(int64))
	case TypeFloat:
		return v.Data.(float64)
	case TypeDuration:
		// Duration stored as int64 nanoseconds
		return float64(v.Data.(int64))
	default:
		return 0
	}
}

// compileColumnPredicate compiles a TraceQL expression to a ColumnPredicate closure.
// This enables fast bulk column scanning on blockpack data.
func (c *traceqlCompiler) compileColumnPredicate(expr traceql.Expr) (ColumnPredicate, error) {
	if expr == nil {
		// Match all
		return func(provider ColumnDataProvider) (RowSet, error) {
			return provider.FullScan(), nil
		}, nil
	}

	return c.compileColumnPredicateExpr(expr)
}

func (c *traceqlCompiler) compileColumnPredicateExpr(expr traceql.Expr) (ColumnPredicate, error) {
	switch e := expr.(type) {
	case *traceql.BinaryExpr:
		return c.compileColumnPredicateBinary(e)
	case *traceql.FieldExpr:
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
	case *traceql.LiteralExpr:
		// Literal boolean value
		if e.Type == traceql.LitBool {
			if e.Value.(bool) {
				// true - match all
				return func(provider ColumnDataProvider) (RowSet, error) {
					return provider.FullScan(), nil
				}, nil
			} else {
				// false - match none
				return func(provider ColumnDataProvider) (RowSet, error) {
					return provider.Complement(provider.FullScan()), nil
				}, nil
			}
		}
		return nil, fmt.Errorf("unsupported literal in column predicate: %v", e.Value)
	default:
		return nil, fmt.Errorf("unsupported expression type for column predicate: %T", expr)
	}
}

func (c *traceqlCompiler) compileColumnPredicateBinary(expr *traceql.BinaryExpr) (ColumnPredicate, error) {
	switch expr.Op {
	case traceql.OpAnd:
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

	case traceql.OpOr:
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

	case traceql.OpEq, traceql.OpNeq, traceql.OpGt, traceql.OpGte, traceql.OpLt, traceql.OpLte, traceql.OpRegex, traceql.OpNotRegex:
		// Comparison: compile to column scan
		return c.compileColumnPredicateComparison(expr)

	default:
		return nil, fmt.Errorf("unsupported binary operator for column predicate: %s", expr.Op.String())
	}
}

func (c *traceqlCompiler) compileColumnPredicateComparison(expr *traceql.BinaryExpr) (ColumnPredicate, error) {
	// Extract field and literal
	fieldExpr, ok := expr.Left.(*traceql.FieldExpr)
	if !ok {
		return nil, fmt.Errorf("comparison left side must be field, got %T", expr.Left)
	}

	literalExpr, ok := expr.Right.(*traceql.LiteralExpr)
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
		case traceql.OpEq:
			return provider.ScanEqual(attrName, value)
		case traceql.OpNeq:
			return provider.ScanNotEqual(attrName, value)
		case traceql.OpGt:
			return provider.ScanGreaterThan(attrName, value)
		case traceql.OpGte:
			return provider.ScanGreaterThanOrEqual(attrName, value)
		case traceql.OpLt:
			return provider.ScanLessThan(attrName, value)
		case traceql.OpLte:
			return provider.ScanLessThanOrEqual(attrName, value)
		case traceql.OpRegex:
			if strVal, ok := value.(string); ok {
				return provider.ScanRegex(attrName, strVal)
			}
			return nil, fmt.Errorf("regex operator requires string value")
		case traceql.OpNotRegex:
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
	case "trace:id", "span:id", "span:parent_id":
		return true
	default:
		return false
	}
}

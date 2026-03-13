package vm

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	regexp "github.com/coregx/coregex"

	"github.com/grafana/blockpack/internal/traceqlparser"
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
	// Regex operators (=~, !~) use a pre-compiled *regexp.Regexp to evaluate per-span.
	// The ColumnPredicate path (compileColumnPredicateComparison) handles bulk column scans;
	// this VM instruction path handles row-by-row evaluation when the bulk scan is not used.

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

	// For regex operators, pre-compile at plan-compile time to avoid per-span recompilation.
	if operator == traceqlparser.OpRegex || operator == traceqlparser.OpNotRegex {
		patternStr, ok := decodedLiteral.Value.(string)
		if !ok {
			return fmt.Errorf("regex operator requires string literal")
		}
		re, reErr := regexp.Compile(patternStr)
		if reErr != nil {
			return fmt.Errorf("invalid regular expression %q: %w", patternStr, reErr)
		}
		op := operator
		c.emit(func(vm *VM) int {
			attrValue := vm.loadAttribute(attrIdx)
			if attrValue.Type != TypeString {
				vm.push(Value{Type: TypeBool, Data: false})
				return vm.pc + 1
			}
			leftStr, ok := attrValue.Data.(string)
			if !ok {
				vm.push(Value{Type: TypeBool, Data: false})
				return vm.pc + 1
			}
			matched := re.MatchString(leftStr)
			vm.push(Value{Type: TypeBool, Data: matched == (op == traceqlparser.OpRegex)})
			return vm.pc + 1
		})
		return nil
	}

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
	case traceqlparser.LitStatus:
		if strVal, ok := lit.Value.(string); ok {
			if intVal, ok := statusCodeToInt64(strVal); ok {
				vmValue = Value{Type: TypeInt, Data: intVal}
			} else {
				vmValue = Value{Type: TypeNil, Data: nil}
			}
		} else {
			vmValue = Value{Type: TypeNil, Data: nil}
		}
	case traceqlparser.LitKind:
		if strVal, ok := lit.Value.(string); ok {
			if intVal, ok := spanKindToInt64(strVal); ok {
				vmValue = Value{Type: TypeInt, Data: intVal}
			} else {
				vmValue = Value{Type: TypeNil, Data: nil}
			}
		} else {
			vmValue = Value{Type: TypeNil, Data: nil}
		}
	default:
		// LitNil and unknown types - treat as nil
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

	// For regex operators, pre-compile the pattern at plan-compile time to avoid
	// per-block recompilation in the hot scan path.
	if operator == traceqlparser.OpRegex || operator == traceqlparser.OpNotRegex {
		if strVal, ok := value.(string); ok {
			notMatch := operator == traceqlparser.OpNotRegex
			// CI literal bypass: pure (?i)literal patterns use fold-contains instead
			// of the NFA regex engine, avoiding tryBacktrack overhead.
			if a := AnalyzeRegex(strVal); a != nil && a.IsLiteralContains && a.CaseInsensitive {
				lp := a.Prefixes // pre-lowercased
				if fieldExpr.Scope == "" && !isBuiltInField(attrName) {
					rName := "resource." + fieldExpr.Name
					sName := "span." + fieldExpr.Name
					lName := "log." + fieldExpr.Name
					return func(provider ColumnDataProvider) (RowSet, error) {
						rs, err := scanColumnRegexFast(provider, rName, nil, lp, notMatch)
						if err != nil {
							return nil, err
						}
						ss, err := scanColumnRegexFast(provider, sName, nil, lp, notMatch)
						if err != nil {
							return nil, err
						}
						ls, err := scanColumnRegexFast(provider, lName, nil, lp, notMatch)
						if err != nil {
							return nil, err
						}
						return provider.Union(provider.Union(rs, ss), ls), nil
					}, nil
				}
				return func(provider ColumnDataProvider) (RowSet, error) {
					return scanColumnRegexFast(provider, attrName, nil, lp, notMatch)
				}, nil
			}
			re, err := regexp.Compile(strVal)
			if err != nil {
				return nil, fmt.Errorf("invalid regex %q: %w", strVal, err)
			}
			prefixes := RegexPrefixes(strVal)
			if fieldExpr.Scope == "" && !isBuiltInField(attrName) {
				rName := "resource." + fieldExpr.Name
				sName := "span." + fieldExpr.Name
				lName := "log." + fieldExpr.Name
				return func(provider ColumnDataProvider) (RowSet, error) {
					rs, err := scanColumnRegexFast(provider, rName, re, prefixes, notMatch)
					if err != nil {
						return nil, err
					}
					ss, err := scanColumnRegexFast(provider, sName, re, prefixes, notMatch)
					if err != nil {
						return nil, err
					}
					ls, err := scanColumnRegexFast(provider, lName, re, prefixes, notMatch)
					if err != nil {
						return nil, err
					}
					return provider.Union(provider.Union(rs, ss), ls), nil
				}, nil
			}
			return func(provider ColumnDataProvider) (RowSet, error) {
				return scanColumnRegexFast(provider, attrName, re, prefixes, notMatch)
			}, nil
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
	if fieldExpr.Scope == "" && !isBuiltInField(attrName) {
		resourceName := "resource." + fieldExpr.Name
		spanName := "span." + fieldExpr.Name
		logName := "log." + fieldExpr.Name
		return func(provider ColumnDataProvider) (RowSet, error) {
			rs, err := scanColumnByOp(provider, resourceName, value, operator)
			if err != nil {
				return nil, err
			}
			ss, err := scanColumnByOp(provider, spanName, value, operator)
			if err != nil {
				return nil, err
			}
			ls, err := scanColumnByOp(provider, logName, value, operator)
			if err != nil {
				return nil, err
			}
			return provider.Union(provider.Union(rs, ss), ls), nil
		}, nil
	}

	// Scoped path: single column scan.
	return func(provider ColumnDataProvider) (RowSet, error) {
		return scanColumnByOp(provider, attrName, value, operator)
	}, nil
}

// scanColumnByOp dispatches a column scan for the given operator.
func scanColumnByOp(
	provider ColumnDataProvider,
	col string,
	value interface{},
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

// extractEqNode builds a RangeNode for an equality predicate (OpEq).
// Unscoped user attributes expand to an OR composite over all three scopes.
func extractEqNode(expr *traceqlparser.BinaryExpr) (nodes []RangeNode, cols []string) {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return nil, nil
	}
	lit, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return nil, nil
	}
	columnName := normalizeAttributePath(field.Scope, field.Name)
	if isComputedField(columnName) {
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
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return nil, nil
	}
	lit, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok {
		return nil, nil
	}
	columnName := normalizeAttributePath(field.Scope, field.Name)
	if isComputedField(columnName) {
		return nil, nil
	}
	vmValue, ok := convertTraceQLLiteralToValue(lit)
	if !ok {
		return nil, nil
	}

	var minVal, maxVal *Value
	switch expr.Op {
	case traceqlparser.OpGt, traceqlparser.OpGte:
		v := vmValue
		minVal = &v
	case traceqlparser.OpLt, traceqlparser.OpLte:
		v := vmValue
		maxVal = &v
	}

	if field.Scope == "" && !isBuiltInField(columnName) {
		res, span, log := unscopedCols(field.Name)
		cols = []string{res, span, log}
		return []RangeNode{{
			IsOR: true,
			Children: []RangeNode{
				{Column: res, Min: minVal, Max: maxVal},
				{Column: span, Min: minVal, Max: maxVal},
				{Column: log, Min: minVal, Max: maxVal},
			},
		}}, cols
	}
	if !isBuiltInField(columnName) {
		cols = []string{columnName}
	}
	return []RangeNode{{Column: columnName, Min: minVal, Max: maxVal}}, cols
}

// extractRegexNode builds a RangeNode for a regex predicate (=~).
// Unscoped user attributes expand to an OR composite over all three scopes.
func extractRegexNode(expr *traceqlparser.BinaryExpr) (nodes []RangeNode, cols []string) {
	field, ok := expr.Left.(*traceqlparser.FieldExpr)
	if !ok {
		return nil, nil
	}
	lit, ok := expr.Right.(*traceqlparser.LiteralExpr)
	if !ok || lit.Type != traceqlparser.LitString {
		return nil, nil
	}
	pattern, ok := lit.Value.(string)
	if !ok {
		return nil, nil
	}
	columnName := normalizeAttributePath(field.Scope, field.Name)
	if isComputedField(columnName) {
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
	out := ss[:0]
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

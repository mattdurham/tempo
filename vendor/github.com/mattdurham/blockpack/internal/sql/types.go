package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
	"github.com/mattdurham/blockpack/internal/vm"
)

// InferType determines the VM type from a SQL expression
func InferType(expr tree.Expr) vm.ValueType {
	switch e := expr.(type) {
	case *tree.NumVal:
		// Check if it's an integer or float
		origString := e.OrigString()
		if strings.Contains(origString, ".") || strings.ContainsAny(origString, "eE") {
			return vm.TypeFloat
		}
		return vm.TypeInt

	case *tree.StrVal:
		return vm.TypeString

	case *tree.DBool:
		return vm.TypeBool

	case *tree.DInterval:
		return vm.TypeDuration

	case *tree.CastExpr:
		// Handle explicit casts
		return inferTypeFromCast(e)

	case *tree.UnaryExpr:
		// Handle negative numbers
		return InferType(e.Expr)

	default:
		return vm.TypeNil
	}
}

// inferTypeFromCast infers type from explicit CAST expressions
func inferTypeFromCast(cast *tree.CastExpr) vm.ValueType {
	// Check the target type using SQLString() which returns PostgreSQL type names
	sqlType := strings.ToLower(cast.Type.SQLString())

	// Match PostgreSQL internal type names (INT8, FLOAT8, STRING, BOOL, INTERVAL)
	// as well as SQL standard names for compatibility
	switch sqlType {
	case "int8", "int4", "int2", "int", "integer", "bigint", "smallint":
		return vm.TypeInt

	case "float8", "float4", "float", "double", "real", "numeric", "decimal":
		return vm.TypeFloat

	case "string", "text", "varchar", "char":
		return vm.TypeString

	case "bool", "boolean":
		return vm.TypeBool

	case "interval":
		return vm.TypeDuration

	default:
		return vm.TypeNil
	}
}

// ConvertLiteral converts a SQL literal expression to a VM Value
func ConvertLiteral(expr tree.Expr) (vm.Value, error) {
	typ := InferType(expr)

	switch typ {
	case vm.TypeInt:
		val, err := extractInt(expr)
		if err != nil {
			return vm.Value{}, err
		}
		return vm.Value{Type: vm.TypeInt, Data: val}, nil

	case vm.TypeFloat:
		val, err := extractFloat(expr)
		if err != nil {
			return vm.Value{}, err
		}
		return vm.Value{Type: vm.TypeFloat, Data: val}, nil

	case vm.TypeString:
		val := extractString(expr)
		return vm.Value{Type: vm.TypeString, Data: val}, nil

	case vm.TypeBool:
		val := extractBool(expr)
		return vm.Value{Type: vm.TypeBool, Data: val}, nil

	case vm.TypeDuration:
		ns, err := ConvertInterval(expr)
		if err != nil {
			return vm.Value{}, err
		}
		return vm.Value{Type: vm.TypeDuration, Data: ns}, nil

	default:
		return vm.Value{}, fmt.Errorf("unsupported type: %T", expr)
	}
}

// extractInt extracts an int64 from a SQL expression
func extractInt(expr tree.Expr) (int64, error) {
	switch e := expr.(type) {
	case *tree.NumVal:
		// Use NumVal's built-in AsInt64() which handles the negative flag
		val, err := e.AsInt64()
		if err != nil {
			return 0, fmt.Errorf("failed to parse int: %w", err)
		}
		return val, nil

	case *tree.UnaryExpr:
		// Handle explicit negative numbers (though NumVal should handle this)
		if e.Operator == tree.UnaryMinus {
			val, err := extractInt(e.Expr)
			if err != nil {
				return 0, err
			}
			return -val, nil
		}
		return 0, fmt.Errorf("unsupported unary operator: %v", e.Operator)

	default:
		return 0, fmt.Errorf("cannot extract int from: %T", expr)
	}
}

// extractFloat extracts a float64 from a SQL expression
func extractFloat(expr tree.Expr) (float64, error) {
	switch e := expr.(type) {
	case *tree.NumVal:
		// Use String() which includes the negative sign if present
		strVal := e.String()
		val, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse float: %w", err)
		}
		return val, nil

	case *tree.UnaryExpr:
		// Handle explicit negative numbers (though NumVal should handle this)
		if e.Operator == tree.UnaryMinus {
			val, err := extractFloat(e.Expr)
			if err != nil {
				return 0, err
			}
			return -val, nil
		}
		return 0, fmt.Errorf("unsupported unary operator: %v", e.Operator)

	default:
		return 0, fmt.Errorf("cannot extract float from: %T", expr)
	}
}

// extractString extracts a string from a SQL expression
func extractString(expr tree.Expr) string {
	switch e := expr.(type) {
	case *tree.StrVal:
		return e.RawString()
	case *tree.NumVal:
		return e.OrigString()
	default:
		return fmt.Sprintf("%v", expr)
	}
}

// extractBool extracts a boolean from a SQL expression
func extractBool(expr tree.Expr) bool {
	if b, ok := expr.(*tree.DBool); ok {
		return bool(*b)
	}
	return false
}

// ConvertInterval converts a PostgreSQL INTERVAL expression to nanoseconds
func ConvertInterval(expr tree.Expr) (int64, error) {
	interval, ok := expr.(*tree.DInterval)
	if !ok {
		return 0, fmt.Errorf("not an interval expression: %T", expr)
	}

	// DInterval stores duration as separate components: months, days, and nanoseconds
	// We need to convert all components to nanoseconds
	// Note: Months are calendar months and can't be accurately converted without a reference date,
	// but for our use case, we'll use the same approximation as PostgreSQL (30 days/month)
	dur := interval.Duration
	nanos := dur.Nanos()
	days := dur.Days * 24 * int64(time.Hour)
	months := dur.Months * 30 * 24 * int64(time.Hour)

	return nanos + days + months, nil
}

// ExtractAttributePath extracts the attribute path from a column reference
// E.g., "span.http.status_code" stays as-is (quoted identifiers preserve dots)
// Handles colon syntax for intrinsics: "span:name" → "span.name"
func ExtractAttributePath(col tree.Expr) (string, error) {
	switch c := col.(type) {
	case *tree.UnresolvedName:
		// Quoted identifier - join parts with dots
		// E.g., "span"."http"."status_code" or "span.http.status_code"
		parts := c.Parts
		var path []string
		for i := len(parts) - 1; i >= 0; i-- {
			if parts[i] != "" {
				path = append(path, parts[i])
			}
		}
		if len(path) == 0 {
			return "", fmt.Errorf("empty column name")
		}
		// Join with dots to form the attribute path
		fullPath := strings.Join(path, ".")

		// Restore colons from placeholder (PostgreSQL parser workaround)
		fullPath = strings.ReplaceAll(fullPath, "__COLON__", ":")

		// Check for colon syntax (intrinsic field)
		if strings.Contains(fullPath, ":") {
			return parseIntrinsicPath(fullPath)
		}

		// Check for unscoped intrinsic - keep as-is, don't normalize
		// Let attributePathToColumnName handle the mapping to span:name, etc.
		if isUnscopedIntrinsic(fullPath) {
			return fullPath, nil
		}

		return normalizeAttributePath(fullPath), nil

	case *tree.ColumnItem:
		// Handle table-qualified columns if needed
		fullPath := c.ColumnName.String()

		// Restore colons from placeholder (PostgreSQL parser workaround)
		fullPath = strings.ReplaceAll(fullPath, "__COLON__", ":")

		// Check for colon syntax (intrinsic field)
		if strings.Contains(fullPath, ":") {
			return parseIntrinsicPath(fullPath)
		}

		// Check for unscoped intrinsic - keep as-is, don't normalize
		// Let attributePathToColumnName handle the mapping to span:name, etc.
		if isUnscopedIntrinsic(fullPath) {
			return fullPath, nil
		}

		return normalizeAttributePath(fullPath), nil

	default:
		return "", fmt.Errorf("unsupported column reference type: %T", col)
	}
}

func normalizeAttributePath(path string) string {
	// No normalization needed - paths should already use correct syntax
	return path
}

// parseIntrinsicPath validates and returns "span:name" as-is (keeps colon)
func parseIntrinsicPath(path string) (string, error) {
	parts := strings.SplitN(path, ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid colon syntax: %s", path)
	}

	scope, field := parts[0], parts[1]

	if !isIntrinsicField(scope, field) {
		return "", fmt.Errorf("%s:%s is not a valid intrinsic field", scope, field)
	}

	// Return with colon intact - the storage layer uses : for intrinsics
	return path, nil
}

// isIntrinsicField validates scope:field combinations
func isIntrinsicField(scope, field string) bool {
	switch scope {
	case "span":
		return isSpanIntrinsic(field)
	case "resource":
		return field == "schema_url" || field == "dropped_attributes_count"
	case "event":
		return field == "name" || field == "time_since_start" || field == "dropped_attributes_count"
	case "link":
		return field == "trace_id" || field == "span_id" || field == "trace_state" || field == "dropped_attributes_count"
	case "instrumentation":
		return field == "name" || field == "version" || field == "dropped_attributes_count"
	case "trace":
		return field == "id"
	default:
		return false
	}
}

func isSpanIntrinsic(field string) bool {
	switch field {
	case "id", "parent_id", "name", "kind", "status", "status_message", "trace_state",
		"start", "end", "duration",
		"dropped_attributes_count", "dropped_events_count", "dropped_links_count":
		return true
	default:
		return false
	}
}

// isUnscopedIntrinsic checks if "name" or "duration" etc
// Note: We still accept unscoped shortcuts here for SQL parsing compatibility
func isUnscopedIntrinsic(path string) bool {
	switch path {
	case "name", "duration", "kind", "status", "status_message",
		"start", "end":
		return true
	default:
		return false
	}
}

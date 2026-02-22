package sql

import (
	"fmt"
	"strings"
	"time"

	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

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
		return field == "schema_url" || field == AttrDroppedAttributesCount
	case "event":
		return field == UnscopedName || field == "time_since_start" || field == AttrDroppedAttributesCount
	case "link":
		return field == "trace_id" || field == "span_id" || field == AttrTraceState ||
			field == AttrDroppedAttributesCount
	case "instrumentation":
		return field == UnscopedName || field == "version" || field == AttrDroppedAttributesCount
	case "trace":
		return field == "id"
	default:
		return false
	}
}

func isSpanIntrinsic(field string) bool {
	switch field {
	case "id", "parent_id", UnscopedName, "kind", "status", "status_message", AttrTraceState,
		"start", "end", "duration",
		AttrDroppedAttributesCount, "dropped_events_count", "dropped_links_count":
		return true
	default:
		return false
	}
}

// isUnscopedIntrinsic checks if UnscopedName or "duration" etc
// Note: We still accept unscoped shortcuts here for SQL parsing compatibility
func isUnscopedIntrinsic(path string) bool {
	switch path {
	case UnscopedName, "duration", "kind", "status", "status_message",
		"start", "end":
		return true
	default:
		return false
	}
}

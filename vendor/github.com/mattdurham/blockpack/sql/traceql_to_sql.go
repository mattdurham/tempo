package sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/mattdurham/blockpack/traceql"
)

// TraceQLMetricsToSQL translates a TraceQL metrics query to a SQL SELECT statement.
// This is used for the fallback path when no pre-computed metric stream matches the query.
//
// The generated SQL follows this structure:
//
//	SELECT [group_by_fields,] aggregate_function(field)
//	FROM spans
//	WHERE [filter_conditions AND] "start_time" >= startTime AND "start_time" < endTime
//	GROUP BY [group_by_fields]
//
// Example:
//
//	{ status = error } | avg(duration) by (resource.service.name)
//
// Translates to:
//
//	SELECT "resource.service.name", AVG("duration")
//	FROM spans
//	WHERE "span.status" = 2 AND "start_time" >= 1000 AND "start_time" < 2000
//	GROUP BY "resource.service.name"
func TraceQLMetricsToSQL(metricsQuery *traceql.MetricsQuery, startTime, endTime int64) (string, error) {
	if metricsQuery == nil {
		return "", fmt.Errorf("metrics query cannot be nil")
	}
	if metricsQuery.Pipeline == nil {
		return "", fmt.Errorf("metrics query must have a pipeline")
	}

	var sb strings.Builder

	// SELECT clause
	sb.WriteString("SELECT ")

	// GROUP BY fields (appear first in SELECT for readability)
	groupByFields := metricsQuery.Pipeline.By
	for i, field := range groupByFields {
		if i > 0 {
			sb.WriteString(", ")
		}
		normalizedField := normalizeFieldNameForSQL(field)
		sb.WriteString(fmt.Sprintf(`"%s"`, normalizedField))
	}

	// Aggregate function
	if len(groupByFields) > 0 {
		sb.WriteString(", ")
	}

	aggSQL, err := aggregateFuncToSQL(metricsQuery.Pipeline.Aggregate)
	if err != nil {
		return "", err
	}
	sb.WriteString(aggSQL)

	// FROM clause
	sb.WriteString(" FROM spans")

	// WHERE clause (filter + time range)
	whereClauses, err := buildWhereClauses(metricsQuery.Filter, startTime, endTime)
	if err != nil {
		return "", err
	}

	if len(whereClauses) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(whereClauses, " AND "))
	}

	// GROUP BY clause
	if len(groupByFields) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, field := range groupByFields {
			if i > 0 {
				sb.WriteString(", ")
			}
			normalizedField := normalizeFieldNameForSQL(field)
			sb.WriteString(fmt.Sprintf(`"%s"`, normalizedField))
		}
	}

	return sb.String(), nil
}

// aggregateFuncToSQL converts a TraceQL aggregate function to SQL.
func aggregateFuncToSQL(agg traceql.AggregateFunc) (string, error) {
	switch agg.Name {
	case "rate":
		// rate() maps to SQL RATE(*) which returns spans per second
		return "RATE(*)", nil

	case "count_over_time":
		return "COUNT(*)", nil

	case "avg", "avg_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("avg requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		return fmt.Sprintf(`AVG("%s")`, normalizedField), nil

	case "min", "min_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("min requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		return fmt.Sprintf(`MIN("%s")`, normalizedField), nil

	case "max", "max_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("max requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		return fmt.Sprintf(`MAX("%s")`, normalizedField), nil

	case "sum", "sum_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("sum requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		return fmt.Sprintf(`SUM("%s")`, normalizedField), nil

	case "quantile_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("quantile_over_time requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		// Note: QUANTILE function takes field first, then quantile value
		return fmt.Sprintf(`QUANTILE("%s", %g)`, normalizedField, agg.Quantile), nil

	case "stddev", "stddev_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("stddev requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		return fmt.Sprintf(`STDDEV("%s")`, normalizedField), nil

	case "histogram_over_time":
		if agg.Field == "" {
			return "", fmt.Errorf("histogram_over_time requires a field argument")
		}
		normalizedField := normalizeFieldNameForSQL(agg.Field)
		// HISTOGRAM returns bucket counts for the field
		return fmt.Sprintf(`HISTOGRAM("%s")`, normalizedField), nil

	default:
		return "", fmt.Errorf("unsupported aggregate function: %s", agg.Name)
	}
}

// buildWhereClauses constructs the WHERE clause components.
// Returns a slice of SQL conditions that should be joined with AND.
func buildWhereClauses(filter *traceql.FilterExpression, startTime, endTime int64) ([]string, error) {
	var clauses []string

	// Add filter conditions if present
	if filter != nil && filter.Expr != nil {
		filterSQL, err := filter.ToSQL()
		if err != nil {
			return nil, fmt.Errorf("failed to convert filter to SQL: %w", err)
		}
		if filterSQL != "" {
			// Normalize the filter SQL to use dot notation and numeric duration values
			filterSQL = normalizeFilterSQL(filterSQL)
			clauses = append(clauses, filterSQL)
		}
	}

	// Add time range filter (only if time range is specified)
	// Note: We use span:start for filtering as it's the primary time column
	// If startTime and endTime are both 0, skip time filtering (query all data)
	if startTime > 0 || endTime > 0 {
		timeFilter := fmt.Sprintf(`"span:start" >= %d AND "span:start" < %d`, startTime, endTime)
		clauses = append(clauses, timeFilter)
	}

	return clauses, nil
}

// normalizeFilterSQL normalizes field references in SQL generated by TraceQL ToSQL.
// This converts colon syntax to dot syntax and ensures duration is scoped.
// Examples:
//   - "span:status" -> "span.status"
//   - "span:name" -> "span.name"
//   - "duration" -> "span.duration"
//   - INTERVAL '100ms' -> 100000000 (nanoseconds)
func normalizeFilterSQL(sql string) string {
	// Note: blockpack stores intrinsic fields with colon syntax (e.g., "span:status", "span:name")
	// TraceQL's ToSQL() may generate unscoped intrinsic names like "duration", we need to normalize them

	// Normalize unscoped intrinsic field names to scoped colon syntax
	// Use regex to ensure we only match quoted identifiers in SQL context (not in string literals or comments)
	// Pattern matches "fieldname" where the quotes are SQL identifier quotes, not part of string content
	intrinsicReplacements := map[string]string{
		"duration":       "span:duration",
		"name":           "span:name",
		"kind":           "span:kind",
		"status":         "span:status",
		"status_message": "span:status_message",
	}

	for oldField, newField := range intrinsicReplacements {
		// Match "fieldname" as a SQL identifier (quoted with double quotes)
		// The pattern ensures we match the full identifier, not a substring
		// \b is word boundary, ensuring we don't match partial words
		pattern := regexp.MustCompile(`"` + regexp.QuoteMeta(oldField) + `"`)
		sql = pattern.ReplaceAllString(sql, `"`+newField+`"`)
	}

	// Convert INTERVAL format to numeric nanoseconds
	// This is necessary because the VM executor expects numeric duration comparisons
	sql = convertIntervalToNanos(sql)

	return sql
}

// convertIntervalToNanos converts PostgreSQL INTERVAL format to numeric nanoseconds.
// Example: INTERVAL '100ms' -> 100000000
func convertIntervalToNanos(sql string) string {
	// Pattern: INTERVAL '<number><unit>'
	// Units: ns, us, ms, s, m, h
	patterns := []struct {
		unit       string
		multiplier int64
	}{
		{"h", 3600000000000},
		{"m", 60000000000},
		{"s", 1000000000},
		{"ms", 1000000},
		{"us", 1000},
		{"ns", 1},
	}

	for _, p := range patterns {
		// Simple string replacement approach
		prefix := "INTERVAL '"
		suffix := fmt.Sprintf("%s'", p.unit)

		for {
			idx := strings.Index(sql, prefix)
			if idx == -1 {
				break
			}

			// Find the closing quote after the unit
			endIdx := idx + len(prefix)
			numStart := endIdx
			numEnd := numStart

			// Extract the number
			for numEnd < len(sql) && sql[numEnd] >= '0' && sql[numEnd] <= '9' {
				numEnd++
			}

			if numEnd == numStart {
				break
			}

			// Check if the unit matches
			if !strings.HasPrefix(sql[numEnd:], suffix) {
				// Not this unit, skip
				sql = sql[:idx] + "REPLACED_INTERVAL" + sql[idx+len(prefix):]
				continue
			}

			// Parse the number
			numStr := sql[numStart:numEnd]
			num, err := strconv.ParseInt(numStr, 10, 64)
			if err != nil {
				// This should not happen since we validated digits above, but handle gracefully
				num = 0
			}

			// Calculate nanoseconds
			nanos := num * p.multiplier

			// Replace the INTERVAL with numeric value
			replacement := fmt.Sprintf("%d", nanos)
			sql = sql[:idx] + replacement + sql[numEnd+len(suffix):]
		}

		// Restore any skipped intervals
		sql = strings.ReplaceAll(sql, "REPLACED_INTERVAL", "INTERVAL '")
	}

	return sql
}

// normalizeFieldNameForSQL normalizes field names for SQL column references.
// Unlike normalizeFieldName (used for QuerySpec), this matches blockpack's actual column naming:
// - duration, start_time, end_time: stored WITHOUT "span." prefix
// - other intrinsics: stored WITH "span:" colon syntax (e.g., "span:status")
func normalizeFieldNameForSQL(path string) string {
	if path == "" {
		return ""
	}

	// Handle colon syntax - already normalized
	if strings.Contains(path, ":") {
		return path
	}

	// Map unscoped intrinsics to blockpack's actual column names
	switch path {
	case "duration":
		return "span:duration"
	case "start", "start_time":
		return "span:start"
	case "end", "end_time":
		return "span:end"
	case "name":
		return "span:name"
	case "kind":
		return "span:kind"
	case "status":
		return "span:status"
	case "status_message":
		return "span:status_message"
	}

	// Already normalized or attribute path - return as-is
	return path
}

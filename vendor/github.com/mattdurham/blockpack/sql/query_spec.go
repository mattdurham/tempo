package sql

import (
	"sort"
	"strings"
)

// QuerySpec is the canonical intermediate representation for metric queries.
// Both TraceQL metrics queries and SQL aggregate queries compile to QuerySpec
// for semantic matching and query routing.
type QuerySpec struct {
	Filter        FilterSpec
	Aggregate     AggregateSpec
	TimeBucketing TimeBucketSpec
}

// FilterSpec represents the filter conditions for a query.
type FilterSpec struct {
	// AttributeEquals maps attribute paths to lists of acceptable values (OR semantics)
	// e.g., "span.http.status_code" -> [200, 201, 204]
	AttributeEquals map[string][]interface{}

	// AttributeRanges maps attribute paths to range specifications
	// e.g., "duration" -> {MinValue: 100000000, MaxValue: 500000000, MinInclusive: true, MaxInclusive: false}
	AttributeRanges map[string]*RangeSpec

	// IsMatchAll indicates if this filter matches all spans (no predicates)
	IsMatchAll bool
}

// RangeSpec represents a range constraint on an attribute.
type RangeSpec struct {
	MinValue     interface{} // Minimum value (nil means unbounded)
	MaxValue     interface{} // Maximum value (nil means unbounded)
	MinInclusive bool        // Whether minimum is inclusive (>= vs >)
	MaxInclusive bool        // Whether maximum is inclusive (<= vs <)
}

// AggregateSpec represents the aggregation function and grouping.
type AggregateSpec struct {
	// Function is the aggregate function name (uppercase canonical form)
	// Valid values: COUNT, AVG, MIN, MAX, SUM, QUANTILE, RATE, HISTOGRAM, STDDEV
	Function string

	// Field is the attribute path to aggregate (empty for COUNT and RATE)
	Field string

	// Quantile is the quantile value (0-1) for QUANTILE function
	Quantile float64

	// GroupBy is the list of attribute paths to group by (sorted for canonicalization)
	GroupBy []string
}

// TimeBucketSpec represents time bucketing configuration.
type TimeBucketSpec struct {
	Enabled       bool  // Whether time bucketing is enabled
	StartTime     int64 // Start time in nanoseconds (Unix epoch)
	EndTime       int64 // End time in nanoseconds (Unix epoch)
	StepSizeNanos int64 // Time bucket step size in nanoseconds
}

// Normalize converts the QuerySpec to canonical form for consistent matching.
// This ensures semantically equivalent queries produce identical QuerySpec representations.
func (qs *QuerySpec) Normalize() {
	// Normalize field names to canonical form FIRST
	qs.Aggregate.Field = normalizeFieldName(qs.Aggregate.Field)

	// Normalize GROUP BY field names
	for i := range qs.Aggregate.GroupBy {
		qs.Aggregate.GroupBy[i] = normalizeFieldName(qs.Aggregate.GroupBy[i])
	}

	// Sort GROUP BY fields AFTER normalization for canonical ordering
	sort.Strings(qs.Aggregate.GroupBy)

	// Normalize filter attribute paths
	normalizedEquals := make(map[string][]interface{})
	for path, values := range qs.Filter.AttributeEquals {
		normalizedPath := normalizeFieldName(path)
		normalizedEquals[normalizedPath] = values
	}
	qs.Filter.AttributeEquals = normalizedEquals

	normalizedRanges := make(map[string]*RangeSpec)
	for path, rangeSpec := range qs.Filter.AttributeRanges {
		normalizedPath := normalizeFieldName(path)
		normalizedRanges[normalizedPath] = rangeSpec
	}
	qs.Filter.AttributeRanges = normalizedRanges
}

// normalizeFieldName converts attribute paths to canonical form.
// This handles:
// - Unscoped intrinsics: "name" -> "span.name", "duration" -> "span.duration"
// - Colon syntax: "span:name" -> "span.name"
// - Already normalized paths are returned as-is
func normalizeFieldName(path string) string {
	if path == "" {
		return ""
	}

	// Colon syntax is already normalized (intrinsic fields)
	if strings.Contains(path, ":") {
		return path
	}

	// Map unscoped intrinsics to span-scoped colon form
	switch path {
	case "name":
		return "span:name"
	case "duration":
		return "span:duration"
	case "kind":
		return "span:kind"
	case "status":
		return "span:status"
	case "status_message":
		return "span:status_message"
	case "start", "start_time":
		return "span:start"
	case "end", "end_time":
		return "span:end"
	}

	// Handle span.intrinsic (dot notation) - convert to span:intrinsic for known intrinsics
	if strings.HasPrefix(path, "span.") {
		fieldName := strings.TrimPrefix(path, "span.")
		switch fieldName {
		case "name", "kind", "status", "status_message", "start", "end", "duration",
			"id", "parent_id", "trace_state",
			"dropped_attributes_count", "dropped_events_count", "dropped_links_count":
			return "span:" + fieldName
		}
		// It's an attribute, keep dot notation
		return path
	}

	// Handle resource.intrinsic - convert to resource:intrinsic for known intrinsics
	if strings.HasPrefix(path, "resource.") {
		fieldName := strings.TrimPrefix(path, "resource.")
		switch fieldName {
		case "schema_url", "dropped_attributes_count":
			return "resource:" + fieldName
		}
		// It's an attribute, keep dot notation
		return path
	}

	// Already normalized or attribute path - return as-is
	return path
}

// CompileSQLToSpec compiles a SQL SELECT statement to a QuerySpec.
// This is the inverse of TraceQL compilation - SQL queries are compiled to the same
// canonical QuerySpec representation for semantic matching.
func CompileSQLToSpec(stmt interface{}, timeBucket TimeBucketSpec) (*QuerySpec, error) {
	// Import the tree package for type assertions
	// We need to work around Go's import cycle by accepting interface{} and asserting
	// We'll use the actual tree.Select type at runtime
	return compileSQLToSpecInternal(stmt, timeBucket)
}

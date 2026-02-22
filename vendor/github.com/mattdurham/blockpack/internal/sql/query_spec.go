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
	AttributeEquals map[string][]any

	// AttributeRanges maps attribute paths to range specifications
	// e.g., "duration" -> {MinValue: 100000000, MaxValue: 500000000, MinInclusive: true, MaxInclusive: false}
	AttributeRanges map[string]*RangeSpec

	// IsMatchAll indicates if this filter matches all spans (no predicates)
	IsMatchAll bool
}

// RangeSpec represents a range constraint on an attribute.
type RangeSpec struct {
	MinValue     any  // Minimum value (nil means unbounded)
	MaxValue     any  // Maximum value (nil means unbounded)
	MinInclusive bool // Whether minimum is inclusive (>= vs >)
	MaxInclusive bool // Whether maximum is inclusive (<= vs <)
}

// AggregateSpec represents the aggregation function and grouping.
type AggregateSpec struct {
	// Function is the aggregate function name (uppercase canonical form)
	// Valid values: COUNT, AVG, MIN, MAX, SUM, QUANTILE, RATE, HISTOGRAM, STDDEV
	Function string

	// Field is the attribute path to aggregate (empty for COUNT and RATE)
	Field string

	// GroupBy is the list of attribute paths to group by (sorted for canonicalization)
	GroupBy []string

	// Quantile is the quantile value (0-1) for QUANTILE function
	Quantile float64
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
	normalizedEquals := make(map[string][]any)
	for path, values := range qs.Filter.AttributeEquals {
		normalizedPath := normalizeFieldName(path)
		// Merge values if normalized path already exists
		if existing, ok := normalizedEquals[normalizedPath]; ok {
			normalizedEquals[normalizedPath] = append(existing, values...)
		} else {
			normalizedEquals[normalizedPath] = values
		}
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
	case UnscopedName:
		return IntrinsicSpanName
	case UnscopedDuration:
		return IntrinsicSpanDuration
	case UnscopedKind:
		return IntrinsicSpanKind
	case UnscopedStatus:
		return IntrinsicSpanStatus
	case UnscopedStatusMessage:
		return IntrinsicSpanStatusMessage
	case UnscopedStart, "start_time":
		return IntrinsicSpanStart
	case UnscopedEnd, UnscopedEndTime:
		return IntrinsicSpanEnd
	}

	// Handle span.intrinsic (dot notation) - convert to span:intrinsic for known intrinsics
	if fieldName, ok := strings.CutPrefix(path, "span."); ok {
		switch fieldName {
		case UnscopedName,
			UnscopedKind,
			UnscopedStatus,
			UnscopedStatusMessage,
			UnscopedStart,
			UnscopedEnd,
			UnscopedDuration,
			"id",
			UnscopedParentID,
			AttrTraceState,
			AttrDroppedAttributesCount,
			AttrDroppedEventsCount,
			"dropped_links_count":
			return "span:" + fieldName
		}
		// It's an attribute, keep dot notation
		return path
	}

	// Handle resource.intrinsic - convert to resource:intrinsic for known intrinsics
	if fieldName, ok := strings.CutPrefix(path, "resource."); ok {
		switch fieldName {
		case "schema_url", AttrDroppedAttributesCount:
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
func CompileSQLToSpec(stmt any, timeBucket TimeBucketSpec) (*QuerySpec, error) {
	// Import the tree package for type assertions
	// We need to work around Go's import cycle by accepting any and asserting
	// We'll use the actual tree.Select type at runtime
	return compileSQLToSpecInternal(stmt, timeBucket)
}

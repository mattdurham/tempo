package vm

import (
	"sort"
	"strings"
)

// QuerySpec is the canonical intermediate representation for metric queries.
// TraceQL metrics queries compile to QuerySpec for semantic matching and query routing.
type QuerySpec struct {
	Filter        FilterSpec
	Aggregate     AggregateSpec
	TimeBucketing TimeBucketSpec
}

// FilterSpec represents the filter conditions for a query.
type FilterSpec struct {
	// AttributeEquals maps attribute paths to lists of acceptable values (OR semantics)
	// e.g., "span:status" -> ["ok", "error"]
	AttributeEquals map[string][]interface{}

	// AttributeRanges maps attribute paths to range specifications
	// e.g., "span:duration" -> {MinValue: 100000000, MaxValue: 500000000}
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
func (qs *QuerySpec) Normalize() {
	// Normalize aggregate field name
	qs.Aggregate.Field = normalizeFieldName(qs.Aggregate.Field)

	// Normalize GROUP BY field names
	for i := range qs.Aggregate.GroupBy {
		qs.Aggregate.GroupBy[i] = normalizeFieldName(qs.Aggregate.GroupBy[i])
	}

	// Sort GROUP BY fields after normalization
	sort.Strings(qs.Aggregate.GroupBy)

	// Normalize filter attribute paths
	normalizedEquals := make(map[string][]interface{})
	for path, values := range qs.Filter.AttributeEquals {
		normalizedPath := normalizeFieldName(path)
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
	case fieldName:
		return spanName
	case fieldDuration:
		return spanDuration
	case fieldKind:
		return spanKind
	case fieldStatus:
		return spanStatus
	case fieldStatusMessage:
		return spanStatusMessage
	case fieldStart, "start_time":
		return spanStart
	case fieldEnd, "end_time":
		return spanEnd
	}

	// Handle span.intrinsic (dot notation) - convert to span:intrinsic for known intrinsics
	if fieldName, ok := strings.CutPrefix(path, "span."); ok {
		switch fieldName {
		case "name", "kind", "status", "status_message", "start", "end", "duration",
			"id", "parent_id", "trace_state", "dropped_attributes_count",
			"dropped_events_count", "dropped_links_count":
			return "span:" + fieldName
		}
		// It's an attribute, keep dot notation
		return path
	}

	// Handle resource.intrinsic
	if fieldName, ok := strings.CutPrefix(path, "resource."); ok {
		switch fieldName {
		case "schema_url", "dropped_attributes_count":
			return "resource:" + fieldName
		}
		// It's an attribute, keep dot notation
		return path
	}

	// Already normalized or attribute path
	return path
}

// Intrinsic field name constants
const (
	fieldName          = "name"
	fieldDuration      = "duration"
	fieldKind          = "kind"
	fieldStatus        = "status"
	fieldStatusMessage = "status_message"
	fieldStart         = "start"
	fieldEnd           = "end"

	spanName          = "span:name"
	spanDuration      = "span:duration"
	spanKind          = "span:kind"
	spanStatus        = "span:status"
	spanStatusMessage = "span:status_message"
	spanStart         = "span:start"
	spanEnd           = "span:end"
)

// Aggregation function names (uppercase - canonical form)
const (
	FuncNameAVG       = "AVG"
	FuncNameCOUNT     = "COUNT"
	FuncNameHISTOGRAM = "HISTOGRAM"
	FuncNameMAX       = "MAX"
	FuncNameMIN       = "MIN"
	FuncNameQUANTILE  = "QUANTILE"
	FuncNameSTDDEV    = "STDDEV"
	FuncNameSUM       = "SUM"
	FuncNameRATE      = "RATE"
)

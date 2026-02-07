package executor

import (
	"reflect"

	"github.com/mattdurham/blockpack/sql"
)

// MetricStream represents a pre-computed metric stream with its query specification
// and storage location.
type MetricStream struct {
	StreamID         string         // Unique identifier for this stream
	Spec             *sql.QuerySpec // The query specification this stream computes
	MetricStreamPath string         // Path to the pre-computed metric stream data
}

// QueryMatcher routes queries to pre-computed metric streams based on semantic matching.
type QueryMatcher struct {
	streams []*MetricStream
}

// NewQueryMatcher creates a new QueryMatcher with the given list of metric streams.
func NewQueryMatcher(streams []*MetricStream) *QueryMatcher {
	return &QueryMatcher{streams: streams}
}

// Match attempts to find a metric stream that exactly matches the given query specification.
// Returns the matching MetricStream or nil if no match is found.
func (m *QueryMatcher) Match(querySpec *sql.QuerySpec) *MetricStream {
	for _, stream := range m.streams {
		if exactMatch(querySpec, stream.Spec) {
			return stream
		}
	}
	return nil
}

// exactMatch determines if a query exactly matches a stream specification.
// Both query and stream must be normalized before calling this function.
func exactMatch(query *sql.QuerySpec, stream *sql.QuerySpec) bool {
	// Match filter
	if !filterMatch(query.Filter, stream.Filter) {
		return false
	}

	// Match aggregate function
	if query.Aggregate.Function != stream.Aggregate.Function {
		return false
	}

	// Match aggregate field
	if query.Aggregate.Field != stream.Aggregate.Field {
		return false
	}

	// Match quantile for QUANTILE function
	if query.Aggregate.Function == "QUANTILE" {
		if query.Aggregate.Quantile != stream.Aggregate.Quantile {
			return false
		}
	}

	// Match GROUP BY fields
	if !groupByMatch(query.Aggregate.GroupBy, stream.Aggregate.GroupBy) {
		return false
	}

	// Match time bucketing
	if !timeBucketMatch(query.TimeBucketing, stream.TimeBucketing) {
		return false
	}

	return true
}

// filterMatch checks if two FilterSpecs are equivalent.
func filterMatch(query sql.FilterSpec, stream sql.FilterSpec) bool {
	// Both match-all filters
	if query.IsMatchAll && stream.IsMatchAll {
		return true
	}

	// One is match-all and the other isn't
	if query.IsMatchAll != stream.IsMatchAll {
		return false
	}

	// Compare AttributeEquals maps
	if len(query.AttributeEquals) != len(stream.AttributeEquals) {
		return false
	}

	for path, queryValues := range query.AttributeEquals {
		streamValues, exists := stream.AttributeEquals[path]
		if !exists {
			return false
		}

		// Compare value lists using deep equality
		if !reflect.DeepEqual(queryValues, streamValues) {
			return false
		}
	}

	// Compare AttributeRanges maps
	if len(query.AttributeRanges) != len(stream.AttributeRanges) {
		return false
	}

	for path, queryRange := range query.AttributeRanges {
		streamRange, exists := stream.AttributeRanges[path]
		if !exists {
			return false
		}

		// Compare range specifications
		if !rangeMatch(queryRange, streamRange) {
			return false
		}
	}

	return true
}

// rangeMatch checks if two RangeSpecs are equivalent.
func rangeMatch(query *sql.RangeSpec, stream *sql.RangeSpec) bool {
	if query == nil && stream == nil {
		return true
	}
	if query == nil || stream == nil {
		return false
	}

	// Compare min values
	if !reflect.DeepEqual(query.MinValue, stream.MinValue) {
		return false
	}

	// Compare max values
	if !reflect.DeepEqual(query.MaxValue, stream.MaxValue) {
		return false
	}

	// Compare inclusivity flags
	if query.MinInclusive != stream.MinInclusive {
		return false
	}

	if query.MaxInclusive != stream.MaxInclusive {
		return false
	}

	return true
}

// groupByMatch checks if two GROUP BY field lists are equivalent.
// Assumes both lists are already sorted (by QuerySpec.Normalize).
func groupByMatch(query []string, stream []string) bool {
	if len(query) != len(stream) {
		return false
	}

	for i := range query {
		if query[i] != stream[i] {
			return false
		}
	}

	return true
}

// timeBucketMatch checks if two TimeBucketSpecs are equivalent.
func timeBucketMatch(query sql.TimeBucketSpec, stream sql.TimeBucketSpec) bool {
	// If both disabled, they match
	if !query.Enabled && !stream.Enabled {
		return true
	}

	// If one is enabled and the other isn't, no match
	if query.Enabled != stream.Enabled {
		return false
	}

	// Both are enabled - compare parameters
	// Note: StartTime and EndTime may vary per query, so we only compare StepSizeNanos
	if query.StepSizeNanos != stream.StepSizeNanos {
		return false
	}

	return true
}

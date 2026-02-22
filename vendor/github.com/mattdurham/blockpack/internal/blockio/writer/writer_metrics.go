//nolint:goconst // Metric function names kept inline for clarity
package writer

import (
	"fmt"
	"strings"
	"time"

	"github.com/mattdurham/blockpack/internal/sql"
	"github.com/mattdurham/blockpack/internal/vm"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// MetricStreamDef defines a metric stream to compute during ingestion.
// Note: Spec currently uses sql.QuerySpec, but should be refactored to use vm spec
// for sharing between TraceQL and SQL parsers.
type MetricStreamDef struct {
	Spec          *sql.QuerySpec // TODO: Use VM spec instead (shared between traceql and sql)
	StreamID      string
	Query         string
	StepSizeNanos int64
}

// TimeBucket represents a time bucket for aggregation (typically 60 seconds).
type TimeBucket struct {
	Aggregates map[string]*vm.AggBucket // group key -> aggregate bucket
	BucketTime int64                    // Bucket timestamp (rounded to StepSize)
}

// MetricStreamBuilder accumulates aggregates for a metric stream during span writes.
type MetricStreamBuilder struct {
	Def              *MetricStreamDef
	ActiveBuckets    map[int64]*TimeBucket // bucket timestamp -> bucket
	CompletedBuckets []*TimeBucket         // Flushed buckets (>5 minutes old)
	LastFlushTime    int64                 // Last time we flushed old buckets
}

// AddMetricStream adds a metric stream definition to the writer.
// The writer will compute aggregates for this stream during span ingestion.
// Returns an error if the stream definition is invalid or if a stream with
// the same ID already exists.
func (w *Writer) AddMetricStream(streamDef *MetricStreamDef) error {
	if w == nil {
		return fmt.Errorf("writer is nil")
	}
	if streamDef == nil {
		return fmt.Errorf("stream definition cannot be nil")
	}
	if streamDef.StreamID == "" {
		return fmt.Errorf("stream ID cannot be empty")
	}
	if streamDef.Spec == nil {
		return fmt.Errorf("stream QuerySpec cannot be nil")
	}
	if streamDef.StepSizeNanos <= 0 {
		// Default to 60 seconds
		streamDef.StepSizeNanos = 60 * int64(time.Second)
	}

	// Check for duplicate stream IDs
	for _, existing := range w.metricStreams {
		if existing.Def.StreamID == streamDef.StreamID {
			return fmt.Errorf("stream ID %s already exists", streamDef.StreamID)
		}
	}

	builder := &MetricStreamBuilder{
		Def:              streamDef,
		ActiveBuckets:    make(map[int64]*TimeBucket),
		CompletedBuckets: make([]*TimeBucket, 0),
		LastFlushTime:    0,
	}

	w.metricStreams = append(w.metricStreams, builder)
	return nil
}

// UpdateMetricStreams updates all metric streams with a new span.
func (w *Writer) UpdateMetricStreams(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue) error {
	if w == nil {
		return fmt.Errorf("writer is nil")
	}
	if len(w.metricStreams) == 0 {
		return nil
	}

	startTime := int64(span.GetStartTimeUnixNano()) //nolint:gosec

	for _, stream := range w.metricStreams {
		if err := w.updateMetricStream(stream, span, resourceAttrs, startTime); err != nil {
			return fmt.Errorf("failed to update stream %s: %w", stream.Def.StreamID, err)
		}
	}

	return nil
}

// updateMetricStream updates a single metric stream with a span.
// This performs the full aggregation pipeline:
// 1. Check if span matches filter criteria
// 2. Extract group-by key values
// 3. Compute time bucket
// 4. Update aggregate bucket
// 5. Flush old buckets
func (w *Writer) updateMetricStream(
	stream *MetricStreamBuilder,
	span *tracev1.Span,
	resourceAttrs []*commonv1.KeyValue,
	startTime int64,
) error {
	spec := stream.Def.Spec

	// Step 1: Check if span matches filter
	if !w.spanMatchesFilter(span, resourceAttrs, &spec.Filter) {
		return nil
	}

	// Step 2: Extract group key values
	groupKey, err := w.extractGroupKey(span, resourceAttrs, spec.Aggregate.GroupBy)
	if err != nil {
		return err
	}

	// Step 3: Compute bucket time (round down to step size)
	bucketTime := (startTime / stream.Def.StepSizeNanos) * stream.Def.StepSizeNanos

	// Step 4: Get or create bucket
	bucket := stream.ActiveBuckets[bucketTime]
	if bucket == nil {
		bucket = &TimeBucket{
			BucketTime: bucketTime,
			Aggregates: make(map[string]*vm.AggBucket),
		}
		stream.ActiveBuckets[bucketTime] = bucket
	}

	// Step 5: Get or create aggregate bucket for this group
	groupKeyStr := groupKey.Serialize()
	aggBucket := bucket.Aggregates[groupKeyStr]
	if aggBucket == nil {
		aggBucket = &vm.AggBucket{
			GroupKey:  groupKey,
			Count:     0,
			Sum:       0,
			Min:       0,
			Max:       0,
			Quantiles: nil,
		}
		bucket.Aggregates[groupKeyStr] = aggBucket
	}

	// Step 6: Update aggregate based on function (with resource attrs for resource.* field resolution)
	if err := w.updateAggregate(aggBucket, span, resourceAttrs, spec.Aggregate); err != nil {
		return err
	}

	// Step 7: Flush old buckets (older than 5 minutes)
	w.flushOldBuckets(stream, startTime)

	return nil
}

// spanMatchesFilter checks if a span matches the filter criteria.
// Supports both equality predicates (exact value matching) and range predicates
// (min/max bounds with inclusive/exclusive options).
func (w *Writer) spanMatchesFilter(
	span *tracev1.Span,
	resourceAttrs []*commonv1.KeyValue,
	filter *sql.FilterSpec,
) bool {
	if filter.IsMatchAll {
		return true
	}

	// Check equality predicates
	for attrPath, acceptedValues := range filter.AttributeEquals {
		spanValue := w.getSpanAttribute(span, resourceAttrs, attrPath)
		if spanValue == nil {
			return false
		}

		// Check if span value matches any accepted value
		matched := false
		for _, acceptedVal := range acceptedValues {
			if w.valuesEqual(spanValue, acceptedVal) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Check range predicates
	for attrPath, rangeSpec := range filter.AttributeRanges {
		spanValue := w.getSpanAttribute(span, resourceAttrs, attrPath)
		if spanValue == nil {
			return false
		}

		// Check min bound using type-aware comparison to preserve precision
		if rangeSpec.MinValue != nil {
			cmp, ok := w.compareNumeric(spanValue, rangeSpec.MinValue)
			if !ok {
				return false
			}
			if rangeSpec.MinInclusive {
				if cmp < 0 { // spanValue < minValue
					return false
				}
			} else {
				if cmp <= 0 { // spanValue <= minValue
					return false
				}
			}
		}

		// Check max bound using type-aware comparison to preserve precision
		if rangeSpec.MaxValue != nil {
			cmp, ok := w.compareNumeric(spanValue, rangeSpec.MaxValue)
			if !ok {
				return false
			}
			if rangeSpec.MaxInclusive {
				if cmp > 0 { // spanValue > maxValue
					return false
				}
			} else {
				if cmp >= 0 { // spanValue >= maxValue
					return false
				}
			}
		}
	}

	return true
}

// getSpanAttribute retrieves an attribute value from a span.
// Handles intrinsic fields (span:name, span:duration, etc.),
// resource attributes (resource.*), and span attributes (span.*).
func (w *Writer) getSpanAttribute(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, path string) interface{} {
	// Handle intrinsic fields
	switch path {
	case "span:name", "name":
		return span.GetName()
	case "span:kind", "kind":
		return int64(span.GetKind())
	case "span:status", "status":
		if span.GetStatus() != nil {
			return int64(span.GetStatus().GetCode())
		}
		return nil
	case "span:duration", "duration":
		start := span.GetStartTimeUnixNano()
		end := span.GetEndTimeUnixNano()
		if end >= start {
			return int64(end - start) //nolint:gosec
		}
		return int64(0)
	case "span:start":
		return int64(span.GetStartTimeUnixNano()) //nolint:gosec
	case "span:end":
		return int64(span.GetEndTimeUnixNano()) //nolint:gosec
	}

	// Handle resource attributes
	if strings.HasPrefix(path, "resource.") {
		attrName := strings.TrimPrefix(path, "resource.")
		for _, attr := range resourceAttrs {
			if attr.Key == attrName {
				return w.anyValueToInterface(attr.Value)
			}
		}
		return nil
	}

	// Handle span attributes
	if strings.HasPrefix(path, "span.") {
		attrName := strings.TrimPrefix(path, "span.")
		for _, attr := range span.Attributes {
			if attr.Key == attrName {
				return w.anyValueToInterface(attr.Value)
			}
		}
		return nil
	}

	return nil
}

// anyValueToInterface converts an OTLP AnyValue to a Go interface{}.
// Supports string, int, double, and bool values. Returns nil for unsupported types.
func (w *Writer) anyValueToInterface(val *commonv1.AnyValue) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return v.StringValue
	case *commonv1.AnyValue_IntValue:
		return v.IntValue
	case *commonv1.AnyValue_DoubleValue:
		return v.DoubleValue
	case *commonv1.AnyValue_BoolValue:
		return v.BoolValue
	default:
		return nil
	}
}

// valuesEqual compares two values for equality.
// Supports string, int64, float64, and bool comparisons.
func (w *Writer) valuesEqual(a, b interface{}) bool {
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return av == bv
		}
		if bv, ok := b.(int); ok {
			return av == int64(bv)
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	}
	return false
}

// toNumeric converts a value to float64 for numeric comparison.
// Supports int64, uint64, float64, and int types.
// NOTE: Prefer compareNumeric() for int64/uint64 comparisons to avoid precision loss.
func (w *Writer) toNumeric(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

// compareNumeric performs type-aware numeric comparison to avoid float64 precision loss.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// Returns (0, false) if types are incomparable.
func (w *Writer) compareNumeric(a, b interface{}) (int, bool) {
	// Try int64 comparison first (preserves precision for timestamps)
	aInt64, aIsInt64 := a.(int64)
	bInt64, bIsInt64 := b.(int64)
	if aIsInt64 && bIsInt64 {
		if aInt64 < bInt64 {
			return -1, true
		} else if aInt64 > bInt64 {
			return 1, true
		}
		return 0, true
	}

	// Try uint64 comparison
	aUint64, aIsUint64 := a.(uint64)
	bUint64, bIsUint64 := b.(uint64)
	if aIsUint64 && bIsUint64 {
		if aUint64 < bUint64 {
			return -1, true
		} else if aUint64 > bUint64 {
			return 1, true
		}
		return 0, true
	}

	// Mixed int64/uint64 comparison
	if aIsInt64 && bIsUint64 {
		if aInt64 < 0 {
			return -1, true
		}
		if uint64(aInt64) < bUint64 {
			return -1, true
		} else if uint64(aInt64) > bUint64 {
			return 1, true
		}
		return 0, true
	}
	if aIsUint64 && bIsInt64 {
		if bInt64 < 0 {
			return 1, true
		}
		if aUint64 < uint64(bInt64) {
			return -1, true
		} else if aUint64 > uint64(bInt64) {
			return 1, true
		}
		return 0, true
	}

	// Fall back to float64 comparison for float types or int types
	aFloat, aOk := w.toNumeric(a)
	bFloat, bOk := w.toNumeric(b)
	if !aOk || !bOk {
		return 0, false
	}
	if aFloat < bFloat {
		return -1, true
	} else if aFloat > bFloat {
		return 1, true
	}
	return 0, true
}

// extractGroupKey extracts group-by values from a span based on field specifications.
// Returns a GroupKey containing the extracted values in the same order as groupByFields.
func (w *Writer) extractGroupKey(
	span *tracev1.Span,
	resourceAttrs []*commonv1.KeyValue,
	groupByFields []string,
) (vm.GroupKey, error) {
	if len(groupByFields) == 0 {
		return vm.GroupKey{Values: []vm.Value{}}, nil
	}

	values := make([]vm.Value, len(groupByFields))
	for i, field := range groupByFields {
		val := w.getSpanAttribute(span, resourceAttrs, field)
		values[i] = w.interfaceToVMValue(val)
	}

	return vm.GroupKey{Values: values}, nil
}

// interfaceToVMValue converts a Go interface{} to a vm.Value.
// Supports string, int64, float64, and bool types. Returns TypeNil for nil or unsupported types.
func (w *Writer) interfaceToVMValue(val interface{}) vm.Value {
	if val == nil {
		return vm.Value{Type: vm.TypeNil, Data: nil}
	}

	switch v := val.(type) {
	case string:
		return vm.Value{Type: vm.TypeString, Data: v}
	case int64:
		return vm.Value{Type: vm.TypeInt, Data: v}
	case float64:
		return vm.Value{Type: vm.TypeFloat, Data: v}
	case bool:
		return vm.Value{Type: vm.TypeBool, Data: v}
	default:
		return vm.Value{Type: vm.TypeNil, Data: nil}
	}
}

// updateAggregate updates an aggregate bucket with a span's contribution.
// Supports COUNT, RATE, SUM, AVG, MIN, MAX, QUANTILE, HISTOGRAM, and STDDEV functions.
// Only increments count if the span has a valid value for field-based aggregates.
func (w *Writer) updateAggregate(
	bucket *vm.AggBucket,
	span *tracev1.Span,
	resourceAttrs []*commonv1.KeyValue,
	agg sql.AggregateSpec,
) error {
	// Get field value if needed (before incrementing count)
	var fieldValue float64
	needsFieldValue := agg.Function != "COUNT" && agg.Function != "RATE"

	if needsFieldValue {
		if agg.Field == "" {
			return fmt.Errorf("aggregate function %s requires a field", agg.Function)
		}

		// Get field value (with resource attrs to resolve resource.* fields)
		rawVal := w.getSpanAttribute(span, resourceAttrs, agg.Field)
		numVal, ok := w.toNumeric(rawVal)
		if !ok {
			// Skip spans without numeric values for this field
			// Don't increment count since we're not using this span
			return nil
		}
		fieldValue = numVal
	}

	// Only increment count after validating we can use this span
	bucket.Count++

	// Update aggregate based on function
	switch agg.Function {
	case "COUNT", "RATE":
		// Count is already incremented

	case "SUM":
		bucket.Sum += fieldValue

	case "AVG":
		bucket.Sum += fieldValue

	case "MIN":
		if bucket.Count == 1 {
			bucket.Min = fieldValue
		} else if fieldValue < bucket.Min {
			bucket.Min = fieldValue
		}

	case "MAX":
		if bucket.Count == 1 {
			bucket.Max = fieldValue
		} else if fieldValue > bucket.Max {
			bucket.Max = fieldValue
		}

	case "QUANTILE":
		// Track quantiles using DDSketch data structure
		if bucket.Quantiles == nil {
			bucket.Quantiles = make(map[string]*vm.QuantileSketch)
		}

		// Get or create sketch for this field
		sketch, exists := bucket.Quantiles[agg.Field]
		if !exists {
			sketch = vm.NewQuantileSketch(0.01) // 1% accuracy
			bucket.Quantiles[agg.Field] = sketch
		}

		// Add value to sketch
		sketch.Add(fieldValue)

		// Also track min/max for range queries
		if bucket.Count == 1 {
			bucket.Min = fieldValue
			bucket.Max = fieldValue
		} else {
			if fieldValue < bucket.Min {
				bucket.Min = fieldValue
			}
			if fieldValue > bucket.Max {
				bucket.Max = fieldValue
			}
		}

	case "HISTOGRAM":
		// Track histogram using DDSketch (same as quantiles)
		if bucket.Quantiles == nil {
			bucket.Quantiles = make(map[string]*vm.QuantileSketch)
		}

		// Get or create sketch for this field
		sketch, exists := bucket.Quantiles[agg.Field]
		if !exists {
			sketch = vm.NewQuantileSketch(0.01) // 1% accuracy
			bucket.Quantiles[agg.Field] = sketch
		}

		// Add value to sketch
		sketch.Add(fieldValue)

		// Also track min/max for range queries
		if bucket.Count == 1 {
			bucket.Min = fieldValue
			bucket.Max = fieldValue
		} else {
			if fieldValue < bucket.Min {
				bucket.Min = fieldValue
			}
			if fieldValue > bucket.Max {
				bucket.Max = fieldValue
			}
		}

	case "STDDEV":
		// Standard deviation requires second moment tracking
		// For now, just track sum and count (can compute variance later)
		bucket.Sum += fieldValue

	default:
		return fmt.Errorf("unsupported aggregate function: %s", agg.Function)
	}

	return nil
}

// flushOldBuckets moves buckets older than 5 minutes to completed list.
// Only performs flush check every minute to avoid excessive processing.
func (w *Writer) flushOldBuckets(stream *MetricStreamBuilder, currentTime int64) {
	const flushThreshold = 5 * 60 * int64(time.Second) // 5 minutes in nanoseconds

	// Only check every minute to avoid excessive processing
	if currentTime-stream.LastFlushTime < int64(time.Minute) {
		return
	}

	stream.LastFlushTime = currentTime
	cutoffTime := currentTime - flushThreshold

	// Find and remove old buckets
	for bucketTime, bucket := range stream.ActiveBuckets {
		if bucketTime < cutoffTime {
			stream.CompletedBuckets = append(stream.CompletedBuckets, bucket)
			delete(stream.ActiveBuckets, bucketTime)
		}
	}
}

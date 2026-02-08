package blockio

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/util"
	"github.com/klauspost/compress/zstd"
	"github.com/mattdurham/blockpack/internal/arena"
	"github.com/mattdurham/blockpack/internal/sql"
	"github.com/mattdurham/blockpack/internal/vm"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)


// MetricStreamDef defines a metric stream to compute during ingestion
type MetricStreamDef struct {
	StreamID string
	Query    string
	// BOT: THIS SHOULD NOT BE TIED TO a query spec, but likely a vm spec so it can be shared between traceql and sql
	Spec          *sql.QuerySpec
	StepSizeNanos int64
}

// TimeBucket represents a 60-second time bucket for aggregation
type TimeBucket struct {
	BucketTime int64                    // Bucket timestamp (rounded to StepSize)
	Aggregates map[string]*vm.AggBucket // group key -> aggregate bucket
}

// MetricStreamBuilder accumulates aggregates for a metric stream during span writes
type MetricStreamBuilder struct {
	Def              *MetricStreamDef
	ActiveBuckets    map[int64]*TimeBucket // bucket timestamp -> bucket
	CompletedBuckets []*TimeBucket         // Flushed buckets (>5 minutes old)
	LastFlushTime    int64                 // Last time we flushed old buckets
}

// WriterConfig configures Writer behavior
type WriterConfig struct {
	MaxBlockSpans int
	// LayerSpans defines nested blockpack levels from outermost to leaf.
	// Example: [5000, 500] creates blockpack entries up to 5000 spans that
	// contain child blockpacks with leaf blocks of 500 spans.
	LayerSpans []int
	// MinHashCacheSize controls the max number of token sets cached for MinHash.
	// Set to 0 for default sizing, or negative to disable caching.
	MinHashCacheSize int
	// OutputStream enables streaming mode: blocks are written immediately instead of accumulated
	OutputStream io.WriteSeeker
	// TrackedAttributes is DEPRECATED and ignored.
	// All columns are now tracked automatically in v11 format for optimal query performance.
	// Columns that are already indexed (trace:id, span:id, etc.) are automatically excluded.
	TrackedAttributes []TrackedAttribute
}

// Writer builds a multi-block blockpack file in-memory.
//
// Thread Safety: Writer is NOT thread-safe. All methods (including AddSpan) must be called
// from a single goroutine. Concurrent calls to AddSpan or other methods will cause data races
// and may result in panics or corrupted data, especially when using metric streams.
// blockReference holds lightweight metadata for a block in streaming mode
type blockReference struct {
	offset int64            // File offset where block data starts
	length int64            // Length of serialized block data
	meta   blockIndexEntry  // Block metadata (bloom, timestamps, etc)
}

type Writer struct {
	maxBlockSpans int
	layerSpans    []int
	blocks        []*blockBuilder
	current       *blockBuilder
	// Streaming mode fields
	output      io.WriteSeeker      // Output stream for immediate writes
	blockRefs   []*blockReference   // Lightweight block references (streaming mode)
	currentPos  int64               // Current write position in output stream
	// Reusable zstd encoder (128 MB, reused for all blocks to avoid pool accumulation)
	zstdEncoder *zstd.Encoder
	// dedicated column index: column -> encoded value -> set of block IDs
	dedicatedIndex map[string]map[string]map[int]struct{}
	totalSpans     int
	// UUID column tracking: column name -> detected as UUID
	uuidColumns map[string]bool
	// UUID column sampling: column name -> sample values for detection
	uuidSamples map[string][]string

	// Span buffering for sorting (added for clustering)
	spanBuffer []*BufferedSpan
	// MinHash cache to reduce repeated hashing for identical token sets
	minhashCache *MinHashCache
	// Arena for MinHash token string construction during sorting
	sortArena arena.Arena
	// Reusable sorting buckets for two-level sorting
	sortBuckets  map[string][]*BufferedSpan
	sortServices []string
	// Scratch map for deduplicating attribute keys without per-span allocation
	attrKeyScratch map[string]struct{}

	// Range-bucketed column tracking (single-pass mode - collects all values)
	// Maps column name -> block ID -> list of int64 values in that block
	rangeBucketValues map[string]map[int][]int64
	// KLL sketches for range bucketing (uses O(k + log n) memory, provably optimal)
	// Maps column name -> KLL sketch
	kllSketches map[string]*KLL
	// Computed bucket metadata: column -> metadata
	rangeBucketMeta map[string]*RangeBucketMetadata
	// Pre-computed bucket boundaries from reservoir sampling (column -> boundaries)
	precomputedBoundaries map[string][]int64
	// Bucket index for pre-computed boundaries (column -> bucket_id -> block_ids)
	rangeBucketIndex map[string]map[uint16]map[int]struct{}
	// Auto-detected dedicated column types (for columns not in explicit list)
	autoDedicatedTypes map[string]ColumnType

	// Trace index for fast trace-by-ID lookups
	// Maps trace ID -> set of block IDs containing spans for that trace
	traceBlockIndex map[[16]byte]map[int]struct{}

	// Metric stream support
	metricStreams []*MetricStreamBuilder

	// Value statistics collection (v10 feature)
	config         WriterConfig
	statsCollector *blockStatsCollector // Current block's stats collector
	version        uint8                // File format version (v10/v11)
}

// RangeBucketMetadata stores bucket information for a range-bucketed column
type RangeBucketMetadata struct {
	Min        int64
	Max        int64
	Boundaries []int64 // Length = numBuckets + 1
	ColumnType ColumnType
}

// NewWriter creates a new blockpack writer with default configuration (v11 format, auto-track all columns).
// maxSpans parameter specifies max spans per block (default 2000).
// All columns are automatically tracked in block metadata except those already handled by special indexes.
func NewWriter(maxSpans int) *Writer {
	return NewWriterWithConfig(WriterConfig{
		MaxBlockSpans:     maxSpans,
		TrackedAttributes: nil, // Deprecated, ignored
	})
}

// NewWriterWithConfig creates a new blockpack writer with custom configuration.
func NewWriterWithConfig(config WriterConfig) *Writer {
	layerSpans := normalizeLayerSpans(config.LayerSpans)
	if len(layerSpans) > 0 {
		config.LayerSpans = layerSpans
		config.MaxBlockSpans = layerSpans[len(layerSpans)-1]
	}
	if config.MaxBlockSpans <= 0 {
		config.MaxBlockSpans = 2000 // Default: 2000 spans per block
	}
	if config.MinHashCacheSize == 0 {
		config.MinHashCacheSize = 5000
	}
	if config.MinHashCacheSize < 0 {
		config.MinHashCacheSize = 0
	}

	// Use v11 format (current version)
	version := versionV11

	// Create reusable zstd encoder (128 MB, reused for all blocks)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
	if err != nil {
		panic("failed to create zstd encoder: " + err.Error())
	}

	w := &Writer{
		maxBlockSpans:         config.MaxBlockSpans,
		layerSpans:            append([]int(nil), config.LayerSpans...),
		dedicatedIndex:        make(map[string]map[string]map[int]struct{}),
		uuidColumns:           make(map[string]bool),
		uuidSamples:           make(map[string][]string),
		spanBuffer:            make([]*BufferedSpan, 0, 100000), // Buffer up to 100K spans
		minhashCache:          NewMinHashCache(config.MinHashCacheSize),
		sortBuckets:           make(map[string][]*BufferedSpan),
		attrKeyScratch:        make(map[string]struct{}),
		rangeBucketValues:     make(map[string]map[int][]int64),
		kllSketches:           make(map[string]*KLL),
		rangeBucketMeta:       make(map[string]*RangeBucketMetadata),
		precomputedBoundaries: make(map[string][]int64),
		rangeBucketIndex:      make(map[string]map[uint16]map[int]struct{}),
		autoDedicatedTypes:    make(map[string]ColumnType),
		traceBlockIndex:       make(map[[16]byte]map[int]struct{}),
		output:                config.OutputStream,
		blockRefs:             make([]*blockReference, 0),
		currentPos:            0,
		zstdEncoder:           encoder,
		config:                config,
		version:               version,
	}

	// Always initialize stats collector to automatically track all columns (v11 format)
	// TrackedAttributes config is deprecated - all columns are now tracked automatically
	// BOT: if deprecrated, remove it
	if len(config.TrackedAttributes) > 0 {
		// Warn about deprecated configuration
		fmt.Fprintf(os.Stderr, "Warning: WriterConfig.TrackedAttributes is deprecated and ignored. All columns are now tracked automatically in v11 format.\n")
	}
	w.statsCollector = newBlockStatsCollector(nil)

	return w
}

// BOT: What does this do?
func normalizeLayerSpans(layerSpans []int) []int {
	if len(layerSpans) == 0 {
		return nil
	}
	out := make([]int, 0, len(layerSpans))
	for _, spans := range layerSpans {
		if spans <= 0 {
			return nil
		}
		out = append(out, spans)
	}
	for i := 1; i < len(out); i++ {
		if out[i-1] < out[i] {
			return nil
		}
	}
	return out
}

// BOT: Consider moving metric stream to its own file.
//
// AddMetricStream adds a metric stream definition to the writer.
// The writer will compute aggregates for this stream during span ingestion.
// Returns the stream ID for later reference.
func (w *Writer) AddMetricStream(streamDef *MetricStreamDef) error {
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

// updateMetricStreams updates all metric streams with a new span
func (w *Writer) updateMetricStreams(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue) error {
	if len(w.metricStreams) == 0 {
		return nil
	}

	startTime := int64(span.GetStartTimeUnixNano())

	for _, stream := range w.metricStreams {
		if err := w.updateMetricStream(stream, span, resourceAttrs, startTime); err != nil {
			return fmt.Errorf("failed to update stream %s: %w", stream.Def.StreamID, err)
		}
	}

	return nil
}

// updateMetricStream updates a single metric stream with a span
func (w *Writer) updateMetricStream(stream *MetricStreamBuilder, span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, startTime int64) error {
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

	// Step 6: Update aggregate based on function
	if err := w.updateAggregate(aggBucket, span, spec.Aggregate); err != nil {
		return err
	}

	// Step 7: Flush old buckets (older than 5 minutes)
	w.flushOldBuckets(stream, startTime)

	return nil
}

// spanMatchesFilter checks if a span matches the filter criteria
func (w *Writer) spanMatchesFilter(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, filter *sql.FilterSpec) bool {
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

		// Convert to comparable numeric value
		numValue, ok := w.toNumeric(spanValue)
		if !ok {
			return false
		}

		// Check min bound
		if rangeSpec.MinValue != nil {
			minNum, ok := w.toNumeric(rangeSpec.MinValue)
			if !ok {
				return false
			}
			if rangeSpec.MinInclusive {
				if numValue < minNum {
					return false
				}
			} else {
				if numValue <= minNum {
					return false
				}
			}
		}

		// Check max bound
		if rangeSpec.MaxValue != nil {
			maxNum, ok := w.toNumeric(rangeSpec.MaxValue)
			if !ok {
				return false
			}
			if rangeSpec.MaxInclusive {
				if numValue > maxNum {
					return false
				}
			} else {
				if numValue >= maxNum {
					return false
				}
			}
		}
	}

	return true
}

// getSpanAttribute retrieves an attribute value from a span
func (w *Writer) getSpanAttribute(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, path string) interface{} {
	// Handle intrinsic fields
	switch path {
	case "span:name":
		return span.GetName()
	case "span:kind":
		return int64(span.GetKind())
	case "span:status":
		if span.GetStatus() != nil {
			return int64(span.GetStatus().GetCode())
		}
		return nil
	case "span:duration":
		start := span.GetStartTimeUnixNano()
		end := span.GetEndTimeUnixNano()
		if end >= start {
			return int64(end - start)
		}
		return int64(0)
	case "span:start":
		return int64(span.GetStartTimeUnixNano())
	case "span:end":
		return int64(span.GetEndTimeUnixNano())
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

// anyValueToInterface converts an OTLP AnyValue to a Go interface{}
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

// valuesEqual compares two values for equality
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

// toNumeric converts a value to float64 for comparison
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

// extractGroupKey extracts group-by values from a span
func (w *Writer) extractGroupKey(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, groupByFields []string) (vm.GroupKey, error) {
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

// interfaceToVMValue converts a Go interface{} to a vm.Value
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

// updateAggregate updates an aggregate bucket with a span's contribution
func (w *Writer) updateAggregate(bucket *vm.AggBucket, span *tracev1.Span, agg sql.AggregateSpec) error {
	// Get field value if needed (before incrementing count)
	var fieldValue float64
	needsFieldValue := agg.Function != "COUNT" && agg.Function != "RATE"

	if needsFieldValue {
		if agg.Field == "" {
			return fmt.Errorf("aggregate function %s requires a field", agg.Function)
		}

		// Get field value
		rawVal := w.getSpanAttribute(span, nil, agg.Field)
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

// flushOldBuckets moves buckets older than 5 minutes to completed list
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

// isUUID checks if a string matches the UUID format: 8-4-4-4-12 hex digits
func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	// Check format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	// Check all other characters are hex digits
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

// parseUUID converts a UUID string to 16 bytes
func parseUUID(s string) ([16]byte, error) {
	var uuid [16]byte
	// Remove hyphens
	hexStr := strings.ReplaceAll(s, "-", "")
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return uuid, err
	}
	if len(decoded) != 16 {
		return uuid, fmt.Errorf("invalid UUID length: %d", len(decoded))
	}
	copy(uuid[:], decoded)
	return uuid, nil
}

// Known UUID columns that should always be stored as binary
var knownUUIDColumns = map[string]bool{
	"resource.k8s.pod.uid":        true,
	"resource.k8s.replicaset.uid": true,
	"resource.k8s.deployment.uid": true,
	"span.correlation.id":         true,
	"span.request.id":             true,
}

func (w *Writer) recordDedicatedValue(b *blockBuilder, name string, key DedicatedValueKey) {
	keyType := key.Type()

	// Check if we've already detected this column's type
	typ, ok := w.autoDedicatedTypes[name]
	if !ok {
		// Auto-detect type: make numeric and bytes types range columns, others regular columns
		// Range bucketing caps at 100 buckets, so high cardinality is OK
		switch keyType {
		case ColumnTypeInt64:
			typ = ColumnTypeRangeInt64
		case ColumnTypeUint64:
			typ = ColumnTypeRangeUint64
		case ColumnTypeBytes:
			// Bytes can be range-bucketed by treating first 8 bytes as int64
			// This is perfect for UUIDs, binary IDs, trace:id, span:id, etc.
			typ = ColumnTypeRangeInt64
		case ColumnTypeFloat64:
			// Float64 doesn't support range bucketing, use regular dedicated
			typ = ColumnTypeFloat64
		case ColumnTypeString:
			typ = ColumnTypeString
		case ColumnTypeBool:
			typ = ColumnTypeBool
		default:
			// Unknown type, skip
			return
		}
		// Track this auto-detected type
		w.autoDedicatedTypes[name] = typ
	}

	// Check if this is a range-bucketed column
	if IsRangeColumnType(typ) {
		// For range-bucketed columns, accept int64/uint64/bytes keys as raw values
		// Bytes columns (like trace:id, span:id) are converted to int64 using first 8 bytes
		// Type can be Int64, Uint64, Bytes, or the range type itself
		if keyType != ColumnTypeInt64 && keyType != ColumnTypeUint64 && keyType != ColumnTypeBytes && keyType != typ {
			return
		}
		// Collect raw values for later bucketing
		// Extract int64 value from key (first 8 bytes for Bytes type)
		if len(key.Data()) >= 8 {
			value := int64(binary.LittleEndian.Uint64(key.Data()))
			w.recordRangeBucketValue(name, value)
		}
	} else {
		// Regular dedicated column - type must match exactly
		if typ != keyType {
			return
		}
		// Add to block's dedicated map
		b.recordDedicated(name, key)
	}
}

// SetRangeBucketBoundaries sets pre-computed bucket boundaries for range-bucketed columns.
// This enables two-pass conversion where boundaries are computed in Pass 1 using reservoir sampling.
// When set, recordRangeBucketValue will directly assign values to buckets instead of collecting all values.
func (w *Writer) SetRangeBucketBoundaries(boundaries map[string][]int64) {
	for columnName, bounds := range boundaries {
		w.precomputedBoundaries[columnName] = bounds
	}
}

// SetColumnType sets the type for a dedicated column.
// This is useful for pre-declaring column types in two-pass conversion.
func (w *Writer) SetColumnType(columnName string, colType ColumnType) {
	w.autoDedicatedTypes[columnName] = colType
}

// recordRangeBucketValue collects a value for a range-bucketed column using reservoir sampling.
// This prevents OOM by using O(log n) memory instead of O(n) for high-cardinality columns.
func (w *Writer) recordRangeBucketValue(columnName string, value int64) {
	blockID := len(w.blocks) // Current block being built

	// Check if we have pre-computed boundaries for this column (from external two-pass conversion)
	if boundaries, ok := w.precomputedBoundaries[columnName]; ok {
		// External boundaries mode: directly assign to bucket
		bucketID := GetBucketID(value, boundaries)

		// Initialize bucket index for this column if needed
		if w.rangeBucketIndex[columnName] == nil {
			w.rangeBucketIndex[columnName] = make(map[uint16]map[int]struct{})
		}

		// Add block to this bucket
		if w.rangeBucketIndex[columnName][bucketID] == nil {
			w.rangeBucketIndex[columnName][bucketID] = make(map[int]struct{})
		}
		w.rangeBucketIndex[columnName][bucketID][blockID] = struct{}{}

		return
	}

	// Default mode: use KLL sketch to prevent OOM
	// KLL uses O(k + log n) memory, provably optimal for streaming quantiles
	sketch, ok := w.kllSketches[columnName]
	if !ok {
		const k = 256 // Sketch size parameter: controls accuracy vs memory tradeoff
		sketch = NewKLL(k)
		w.kllSketches[columnName] = sketch
	}
	sketch.Update(value)

	// Also track which blocks have values for this column (lightweight)
	// This allows us to build bucket->block mapping after computing boundaries
	blockMap, ok := w.rangeBucketValues[columnName]
	if !ok {
		blockMap = make(map[int][]int64)
		w.rangeBucketValues[columnName] = blockMap
	}
	// Only store one sample value per block to track block membership (not all values!)
	if len(blockMap[blockID]) == 0 {
		blockMap[blockID] = append(blockMap[blockID], value)
	}
}

// AddTracesData ingests OTLP traces and appends spans to the current block.
func (w *Writer) AddTracesData(td *tracev1.TracesData) error {
	return w.AddTracesDataWithRaw(td, td)
}

// AddTracesDataWithRaw ingests normalized traces while preserving raw OTLP resource and scope metadata.
// The raw parameter provides access to original schema URLs and instrumentation scope information
// that are preserved during normalization. Used by internal/otlpconvert for Tempo compatibility.
func (w *Writer) AddTracesDataWithRaw(td *tracev1.TracesData, raw *tracev1.TracesData) error {
	if td == nil || raw == nil {
		return fmt.Errorf("nil traces data")
	}
	if len(td.ResourceSpans) != len(raw.ResourceSpans) {
		return fmt.Errorf("resource span count mismatch: %d != %d", len(td.ResourceSpans), len(raw.ResourceSpans))
	}
	for rIdx, rs := range td.ResourceSpans {
		rawRS := raw.ResourceSpans[rIdx]
		resource := rs.GetResource()
		resAttrs := resource.GetAttributes()
		rawResource := rawRS.GetResource()
		rawResourceSchema := rawRS.GetSchemaUrl()
		if len(rs.ScopeSpans) != len(rawRS.ScopeSpans) {
			return fmt.Errorf("scope span count mismatch: %d != %d", len(rs.ScopeSpans), len(rawRS.ScopeSpans))
		}
		for sIdx, ss := range rs.GetScopeSpans() {
			rawSS := rawRS.ScopeSpans[sIdx]
			rawScope := rawSS.GetScope()
			rawScopeSchema := rawSS.GetSchemaUrl()
			if len(ss.Spans) != len(rawSS.Spans) {
				return fmt.Errorf("span count mismatch: %d != %d", len(ss.Spans), len(rawSS.Spans))
			}
			for _, span := range ss.GetSpans() {
				if err := w.addSpan(span, resAttrs, rawResource, rawResourceSchema, rawScope, rawScopeSchema); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (w *Writer) addSpan(span *tracev1.Span, resourceAttrs []*commonv1.KeyValue, rawResource *resourcev1.Resource, rawResourceSchema string, rawScope *commonv1.InstrumentationScope, rawScopeSchema string) error {
	// Buffer span for sorting (instead of immediately adding to block)
	buffered := &BufferedSpan{
		Span:           span,
		ResourceAttrs:  resourceAttrs,
		RawResource:    rawResource,
		ResourceSchema: rawResourceSchema,
		RawScope:       rawScope,
		ScopeSchema:    rawScopeSchema,
	}

	w.spanBuffer = append(w.spanBuffer, buffered)
	w.totalSpans++

	return nil
}

// addSpanToBlock adds a buffered span to a block (called during Flush after sorting)
// setSpanIntrinsicFields sets span intrinsic fields (name, kind, status, dropped counts, etc).
func (w *Writer) setSpanIntrinsicFields(b *blockBuilder, span *tracev1.Span, spanIdx int) error {
	if err := w.setString(b, "span:name", spanIdx, span.GetName()); err != nil {
		return err
	}
	if err := w.setInt64(b, "span:kind", spanIdx, int64(span.GetKind())); err != nil {
		return err
	}
	if span.GetStatus() != nil {
		if err := w.setInt64(b, "span:status", spanIdx, int64(span.GetStatus().GetCode())); err != nil {
			return err
		}
		if err := w.setString(b, "span:status_message", spanIdx, span.GetStatus().GetMessage()); err != nil {
			return err
		}
	}
	if traceState := span.GetTraceState(); traceState != "" {
		if err := w.setString(b, "span:trace_state", spanIdx, traceState); err != nil {
			return err
		}
	}
	if dropped := span.GetDroppedAttributesCount(); dropped > 0 {
		if err := w.setUint64(b, "span:dropped_attributes_count", spanIdx, uint64(dropped)); err != nil {
			return err
		}
	}
	if dropped := span.GetDroppedEventsCount(); dropped > 0 {
		if err := w.setUint64(b, "span:dropped_events_count", spanIdx, uint64(dropped)); err != nil {
			return err
		}
	}
	if dropped := span.GetDroppedLinksCount(); dropped > 0 {
		if err := w.setUint64(b, "span:dropped_links_count", spanIdx, uint64(dropped)); err != nil {
			return err
		}
	}
	return nil
}

// setSpanIdentifiersAndTimestamps sets trace/span IDs, timestamps, and updates block ranges.
func (w *Writer) setSpanIdentifiersAndTimestamps(b *blockBuilder, span *tracev1.Span, spanIdx int) error {
	traceID := span.GetTraceId()
	spanID := span.GetSpanId()
	if err := w.setBytes(b, "trace:id", spanIdx, traceID); err != nil {
		return err
	}
	if err := w.setBytes(b, "span:id", spanIdx, spanID); err != nil {
		return err
	}
	parentID := span.GetParentSpanId()
	if len(parentID) == 0 {
		parentID = nil
	}
	if err := w.setBytes(b, "span:parent_id", spanIdx, parentID); err != nil {
		return err
	}

	// span:start (needed for time-based queries and aggregations)
	start := span.GetStartTimeUnixNano()
	if err := w.setUint64(b, "span:start", spanIdx, start); err != nil {
		return err
	}
	if start < b.minStart {
		b.minStart = start
	}
	if start > b.maxStart {
		b.maxStart = start
	}
	b.updateTraceRange(traceID)

	// span:end (needed for trace duration calculations and compatibility)
	end := span.GetEndTimeUnixNano()
	if err := w.setUint64(b, "span:end", spanIdx, end); err != nil {
		return err
	}

	// span:duration
	duration := uint64(0)
	if end >= start {
		duration = end - start
	}
	if err := w.setUint64(b, "span:duration", spanIdx, duration); err != nil {
		return err
	}
	// Special handling: duration is a range-bucketed dedicated column (int64-based)
	// Record it explicitly for the dedicated index since setUint64 doesn't handle range types
	w.recordDedicatedValue(b, "span:duration", IntValueKey(int64(duration)))

	return nil
}

// setResourceAttributes sets resource attributes for a span.
func (w *Writer) setResourceAttributes(b *blockBuilder, resourceAttrs []*commonv1.KeyValue, rawResource *resourcev1.Resource, spanIdx int) error {
	if err := forEachUniqueAttr(resourceAttrs, w.attrKeyScratch, func(attr *commonv1.KeyValue) error {
		colName := "resource." + attr.Key

		// Write attribute value to block column for this span
		if err := w.setFromAnyValue(b, colName, spanIdx, attr.Value); err != nil {
			return err
		}

		// Track in dedicated index
		if s := attr.GetValue().GetStringValue(); s != "" {
			w.trackDedicatedValue(b, colName, s)
		}

		// Record attribute value for statistics (v10 feature)
		w.recordAttributeForStats(colName, attr.Value)
		return nil
	}); err != nil {
		return err
	}
	if rawResource != nil {
		if dropped := rawResource.GetDroppedAttributesCount(); dropped > 0 {
			if err := w.setUint64(b, "resource:dropped_attributes_count", spanIdx, uint64(dropped)); err != nil {
				return err
			}
		}
	}
	return nil
}

// setSpanAttributes sets span attributes.
func (w *Writer) setSpanAttributes(b *blockBuilder, span *tracev1.Span, spanIdx int) error {
	err := forEachUniqueAttr(span.Attributes, w.attrKeyScratch, func(attr *commonv1.KeyValue) error {

		// All attributes use span.{key} format
		// No conflict with intrinsics because intrinsics use span:{field} format
		full := "span." + attr.Key
		if err := w.setFromAnyValue(b, full, spanIdx, attr.Value); err != nil {
			return err
		}
		if s := attr.GetValue().GetStringValue(); s != "" {
			w.trackDedicatedValue(b, full, s)
		}

		// Record attribute value for statistics (v10 feature)
		w.recordAttributeForStats(full, attr.Value)
		return nil
	})
	return err
}

func hasDuplicateAttrKey(attrs []*commonv1.KeyValue, idx int) bool {
	if idx <= 0 {
		return false
	}
	key := attrs[idx].Key
	for i := 0; i < idx; i++ {
		if attrs[i].Key == key {
			return true
		}
	}
	return false
}

func forEachUniqueAttr(attrs []*commonv1.KeyValue, scratch map[string]struct{}, fn func(attr *commonv1.KeyValue) error) error {
	const smallAttrDedupThreshold = 8
	if len(attrs) == 0 {
		return nil
	}

	if len(attrs) <= smallAttrDedupThreshold {
		for i, attr := range attrs {
			if hasDuplicateAttrKey(attrs, i) {
				continue
			}
			if err := fn(attr); err != nil {
				return err
			}
		}
		return nil
	}

	for _, attr := range attrs {
		if _, ok := scratch[attr.Key]; ok {
			continue
		}
		scratch[attr.Key] = struct{}{}
		if err := fn(attr); err != nil {
			for k := range scratch {
				delete(scratch, k)
			}
			return err
		}
	}

	for k := range scratch {
		delete(scratch, k)
	}
	return nil
}

// recordAttributeForStats records an attribute value for block statistics collection
func (w *Writer) recordAttributeForStats(attrName string, value *commonv1.AnyValue) {
	if w.statsCollector == nil {
		return // Stats collection not enabled
	}

	if value == nil {
		return
	}

	// Extract the actual value based on type
	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		w.statsCollector.recordValue(attrName, v.StringValue)
	case *commonv1.AnyValue_IntValue:
		w.statsCollector.recordValue(attrName, v.IntValue)
	case *commonv1.AnyValue_DoubleValue:
		w.statsCollector.recordValue(attrName, v.DoubleValue)
	case *commonv1.AnyValue_BoolValue:
		w.statsCollector.recordValue(attrName, v.BoolValue)
		// Skip arrays, maps, and other complex types
	}
}

func (w *Writer) addSpanToBlock(b *blockBuilder, buffered *BufferedSpan) error {
	span := buffered.Span
	resourceAttrs := buffered.ResourceAttrs
	rawResource := buffered.RawResource
	rawResourceSchema := buffered.ResourceSchema
	rawScope := buffered.RawScope
	rawScopeSchema := buffered.ScopeSchema

	// Update metric streams with this span
	if err := w.updateMetricStreams(span, resourceAttrs); err != nil {
		return fmt.Errorf("failed to update metric streams: %w", err)
	}

	spanIdx := b.spanCount
	b.spanCount++

	// Append NULL for existing columns to maintain row alignment.
	for _, cb := range b.columns {
		cb.appendNull()
	}

	if rawResource == nil {
		rawResource = &resourcev1.Resource{}
	}
	if rawScope == nil {
		rawScope = &commonv1.InstrumentationScope{}
	}

	if err := w.setString(b, "resource:schema_url", spanIdx, rawResourceSchema); err != nil {
		return err
	}

	if err := w.setString(b, "scope:schema_url", spanIdx, rawScopeSchema); err != nil {
		return err
	}

	if err := w.setSpanIntrinsicFields(b, span, spanIdx); err != nil {
		return err
	}

	if err := w.setSpanIdentifiersAndTimestamps(b, span, spanIdx); err != nil {
		return err
	}

	// Get or create trace entry for trace-level deduplication
	traceID := span.GetTraceId()
	var traceID16 [16]byte
	copy(traceID16[:], traceID)
	traceIdx, exists := b.traceIndex[traceID16]
	if !exists {
		traceIdx = len(b.traces)
		b.traces = append(b.traces, &traceEntry{
			traceID:        traceID16,
			resourceAttrs:  make(map[string]*columnBuilder),
			resourceSchema: rawResourceSchema,
		})
		b.traceIndex[traceID16] = traceIdx
	}
	b.spanToTrace = append(b.spanToTrace, traceIdx)
	trace := b.traces[traceIdx]
	_ = trace // Keep trace for potential future use

	if err := w.setResourceAttributes(b, resourceAttrs, rawResource, spanIdx); err != nil {
		return err
	}

	if err := w.setSpanAttributes(b, span, spanIdx); err != nil {
		return err
	}

	// Track column names in bloom
	for name := range b.columns {
		addToColumnNameBloom(&b.columnBloom, name)
	}

	if err := w.setInstrumentationColumns(b, spanIdx, rawScope); err != nil {
		return err
	}
	if err := w.setEventColumns(b, spanIdx, span); err != nil {
		return err
	}
	if err := w.setLinkColumns(b, spanIdx, span); err != nil {
		return err
	}

	return nil
}

func (w *Writer) trackDedicatedValue(b *blockBuilder, colName, val string) {
	// All string columns are now auto-dedicated
	w.recordDedicatedValue(b, colName, StringValueKey(val))
}

func (w *Writer) closeCurrentBlock() {
	if w.current == nil {
		return
	}
	blockID := len(w.blocks)

	// Build dedicated column index for this block
	for col, values := range w.current.dedicated {
		colMap, ok := w.dedicatedIndex[col]
		if !ok {
			colMap = make(map[string]map[int]struct{})
			w.dedicatedIndex[col] = colMap
		}
		for encoded := range values {
			set, ok := colMap[encoded]
			if !ok {
				set = make(map[int]struct{})
			}
			set[blockID] = struct{}{}
			colMap[encoded] = set
		}
	}

	// Build trace block index for this block
	for traceID := range w.current.traceIndex {
		blockSet, ok := w.traceBlockIndex[traceID]
		if !ok {
			blockSet = make(map[int]struct{})
			w.traceBlockIndex[traceID] = blockSet
		}
		blockSet[blockID] = struct{}{}
	}

	// Track trace-level (resource) columns in dedicated index
	for _, trace := range w.current.traces {
		for colName, cb := range trace.resourceAttrs {
			// Auto-detect all resource columns as dedicated
			// Get the string value from the trace column (trace columns have 1 row at index 0)
			if len(cb.stringIndexes) > 0 {
				dictIdx := cb.stringIndexes[0]
				if int(dictIdx) < len(cb.stringDictVals) {
					val := cb.stringDictVals[dictIdx]
					key := StringValueKey(val)

					// Use recordDedicatedValue to auto-detect type and track
					w.recordDedicatedValue(w.current, colName, key)

					encoded := key.Encode()
					colMap, ok := w.dedicatedIndex[colName]
					if !ok {
						colMap = make(map[string]map[int]struct{})
						w.dedicatedIndex[colName] = colMap
					}
					set, ok := colMap[encoded]
					if !ok {
						set = make(map[int]struct{})
					}
					set[blockID] = struct{}{}
					colMap[encoded] = set
				}
			}
		}
	}

	// Finalize value statistics for this block (v10 feature)
	if w.statsCollector != nil {
		w.current.valueStats = w.statsCollector.finalize()
		// Reset for next block
		w.statsCollector.reset()
	}

	// Streaming mode: serialize and write block immediately
	if w.output != nil {
		// Serialize block to bytes with per-Writer encoder
		payload, idxMeta, err := buildBlockPayload(w.current, w.version, w.zstdEncoder)
		if err != nil {
			// Can't return error from closeCurrentBlock, so panic (will be caught by caller)
			panic(fmt.Sprintf("failed to serialize block: %v", err))
		}

		// Write block data to output stream
		n, err := w.output.Write(payload)
		if err != nil {
			panic(fmt.Sprintf("failed to write block: %v", err))
		}

		// Sync to disk if possible (flush OS buffers)
		if syncer, ok := w.output.(interface{ Sync() error }); ok {
			if err := syncer.Sync(); err != nil {
				panic(fmt.Sprintf("failed to sync block: %v", err))
			}
		}

		// Create lightweight reference
		ref := &blockReference{
			offset: w.currentPos,
			length: int64(len(payload)),
			meta:   idxMeta,
		}
		w.blockRefs = append(w.blockRefs, ref)
		w.currentPos += int64(n)

		// Free the block memory immediately
		w.current = nil
		return
	}

	// Non-streaming mode: accumulate blocks in memory
	w.blocks = append(w.blocks, w.current)
	w.current = nil
}

func (w *Writer) sortSpanBuffer(spans []*BufferedSpan) {
	if len(spans) < 2 {
		return
	}

	w.sortServices = w.sortServices[:0]
	for _, span := range spans {
		name := span.ServiceName
		bucket, ok := w.sortBuckets[name]
		if !ok {
			w.sortServices = append(w.sortServices, name)
		}
		bucket = append(bucket, span)
		w.sortBuckets[name] = bucket
	}

	sort.Strings(w.sortServices)

	pos := 0
	for _, name := range w.sortServices {
		bucket := w.sortBuckets[name]
		if len(bucket) > 1 {
			sort.Slice(bucket, func(i, j int) bool {
				if bucket[i].MinHashPrefix[0] != bucket[j].MinHashPrefix[0] {
					return bucket[i].MinHashPrefix[0] < bucket[j].MinHashPrefix[0]
				}
				if bucket[i].MinHashPrefix[1] != bucket[j].MinHashPrefix[1] {
					return bucket[i].MinHashPrefix[1] < bucket[j].MinHashPrefix[1]
				}
				cmp := CompareMinHashSigs(bucket[i].MinHashSig, bucket[j].MinHashSig)
				if cmp != 0 {
					return cmp < 0
				}
				return compareTraceIDs(bucket[i].Span.TraceId, bucket[j].Span.TraceId) < 0
			})
		}
		copy(spans[pos:], bucket)
		pos += len(bucket)
		w.sortBuckets[name] = bucket[:0]
	}
}

// CurrentSize returns an estimated uncompressed size based on spans (~2KB/span).
func (w *Writer) CurrentSize() int {
	return w.totalSpans * 2048
}

// SpanCount returns total spans across closed and current blocks.
func (w *Writer) SpanCount() int {
	return w.totalSpans
}

func (w *Writer) setFromAnyValue(b *blockBuilder, name string, spanIdx int, value *commonv1.AnyValue) error {
	if value == nil {
		return nil
	}
	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return w.setString(b, name, spanIdx, v.StringValue)
	case *commonv1.AnyValue_IntValue:
		return w.setInt64(b, name, spanIdx, v.IntValue)
	case *commonv1.AnyValue_DoubleValue:
		return w.setFloat64(b, name, spanIdx, v.DoubleValue)
	case *commonv1.AnyValue_BoolValue:
		return w.setBool(b, name, spanIdx, v.BoolValue)
	case *commonv1.AnyValue_BytesValue:
		return w.setBytes(b, name, spanIdx, v.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		// Encode array to bytes and store
		encoded := EncodeAnyValueArray(v.ArrayValue.Values)
		return w.setBytes(b, name, spanIdx, encoded)
	case *commonv1.AnyValue_KvlistValue:
		// Encode KVList to bytes and store
		encoded := EncodeKeyValueList(v.KvlistValue)
		return w.setBytes(b, name, spanIdx, encoded)
	default:
		// Unsupported types are skipped.
		return nil
	}
}

func (w *Writer) setInstrumentationColumns(b *blockBuilder, spanIdx int, scope *commonv1.InstrumentationScope) error {
	if scope == nil {
		return nil
	}
	if scope.GetName() != "" {
		if err := w.setBytes(b, "instrumentation:name", spanIdx, EncodeStringArray([]string{scope.GetName()})); err != nil {
			return err
		}
	}
	if scope.GetVersion() != "" {
		if err := w.setBytes(b, "instrumentation:version", spanIdx, EncodeStringArray([]string{scope.GetVersion()})); err != nil {
			return err
		}
	}
	if dropped := scope.GetDroppedAttributesCount(); dropped > 0 {
		if err := w.setUint64(b, "instrumentation:dropped_attributes_count", spanIdx, uint64(dropped)); err != nil {
			return err
		}
	}
	if len(scope.Attributes) == 0 {
		return nil
	}
	attrMap := make(map[string][]*commonv1.AnyValue)
	for _, kv := range scope.Attributes {
		if kv == nil || kv.Value == nil {
			continue
		}
		attrMap[kv.Key] = append(attrMap[kv.Key], kv.Value)
	}
	for key, values := range attrMap {
		if len(values) == 0 {
			continue
		}
		if err := w.setBytes(b, "instrumentation."+key, spanIdx, EncodeAnyValueArray(values)); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) setEventColumns(b *blockBuilder, spanIdx int, span *tracev1.Span) error {
	if span == nil || len(span.Events) == 0 {
		return nil
	}
	eventNames := make([]string, 0, len(span.Events))
	eventTimes := make([]int64, 0, len(span.Events))
	eventDropped := make([]int64, 0, len(span.Events))
	attrMap := make(map[string][]*commonv1.AnyValue)
	start := span.GetStartTimeUnixNano()
	for _, event := range span.Events {
		if event == nil {
			continue
		}
		eventNames = append(eventNames, event.GetName())
		delta := int64(0)
		if event.GetTimeUnixNano() > start {
			delta = int64(event.GetTimeUnixNano() - start)
		}
		eventTimes = append(eventTimes, delta)
		eventDropped = append(eventDropped, int64(event.GetDroppedAttributesCount()))
		for _, kv := range event.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			attrMap[kv.Key] = append(attrMap[kv.Key], kv.Value)
		}
	}
	if len(eventNames) > 0 {
		if err := w.setBytes(b, "event:name", spanIdx, EncodeStringArray(eventNames)); err != nil {
			return err
		}
	}
	if len(eventTimes) > 0 {
		if err := w.setBytes(b, "event:time_since_start", spanIdx, EncodeDurationArray(eventTimes)); err != nil {
			return err
		}
	}
	if len(eventDropped) > 0 {
		if err := w.setBytes(b, "event:dropped_attributes_count", spanIdx, EncodeInt64Array(eventDropped)); err != nil {
			return err
		}
	}
	for key, values := range attrMap {
		if len(values) == 0 {
			continue
		}
		if err := w.setBytes(b, "event."+key, spanIdx, EncodeAnyValueArray(values)); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) setLinkColumns(b *blockBuilder, spanIdx int, span *tracev1.Span) error {
	if span == nil || len(span.Links) == 0 {
		return nil
	}
	linkTraceIDs := make([]string, 0, len(span.Links))
	linkSpanIDs := make([]string, 0, len(span.Links))
	linkTraceStates := make([]string, 0, len(span.Links))
	linkDropped := make([]int64, 0, len(span.Links))
	attrMap := make(map[string][]*commonv1.AnyValue)
	for _, link := range span.Links {
		if link == nil {
			continue
		}
		if len(link.GetTraceId()) > 0 {
			linkTraceIDs = append(linkTraceIDs, util.TraceIDToHexString(link.GetTraceId()))
		}
		if len(link.GetSpanId()) > 0 {
			linkSpanIDs = append(linkSpanIDs, util.SpanIDToHexString(link.GetSpanId()))
		}
		linkTraceStates = append(linkTraceStates, link.GetTraceState())
		linkDropped = append(linkDropped, int64(link.GetDroppedAttributesCount()))
		for _, kv := range link.Attributes {
			if kv == nil || kv.Value == nil {
				continue
			}
			attrMap[kv.Key] = append(attrMap[kv.Key], kv.Value)
		}
	}
	if len(linkTraceIDs) > 0 {
		if err := w.setBytes(b, "link:trace_id", spanIdx, EncodeStringArray(linkTraceIDs)); err != nil {
			return err
		}
	}
	if len(linkSpanIDs) > 0 {
		if err := w.setBytes(b, "link:span_id", spanIdx, EncodeStringArray(linkSpanIDs)); err != nil {
			return err
		}
	}
	if len(linkTraceStates) > 0 {
		if err := w.setBytes(b, "link:trace_state", spanIdx, EncodeStringArray(linkTraceStates)); err != nil {
			return err
		}
	}
	if len(linkDropped) > 0 {
		if err := w.setBytes(b, "link:dropped_attributes_count", spanIdx, EncodeInt64Array(linkDropped)); err != nil {
			return err
		}
	}
	for key, values := range attrMap {
		if len(values) == 0 {
			continue
		}
		if err := w.setBytes(b, "link."+key, spanIdx, EncodeAnyValueArray(values)); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) setString(b *blockBuilder, name string, idx int, value string) error {
	// Check if this column has already been detected
	if detected, ok := w.uuidColumns[name]; ok {
		if detected {
			// Convert UUID string to bytes
			uuidBytes, err := parseUUID(value)
			if err != nil {
				// If parsing fails, something's wrong - fall back to string for this column
				w.uuidColumns[name] = false
				return w.setStringDirect(b, name, idx, value)
			}
			return w.setBytes(b, name, idx, uuidBytes[:])
		}
		// Already determined not a UUID column
		return w.setStringDirect(b, name, idx, value)
	}

	// First value for this column - decide now, but defer detection if value is empty
	// For known UUID columns with empty first values, wait for a valid sample
	if value == "" && knownUUIDColumns[name] {
		// Defer detection until we see a non-empty value
		return w.setStringDirect(b, name, idx, value)
	}

	// Check if this is a known UUID column OR if first value looks like UUID
	if knownUUIDColumns[name] || isUUID(value) {
		// Verify this is actually a UUID before committing
		if isUUID(value) {
			w.uuidColumns[name] = true
			uuidBytes, err := parseUUID(value)
			if err != nil {
				w.uuidColumns[name] = false
				return w.setStringDirect(b, name, idx, value)
			}
			return w.setBytes(b, name, idx, uuidBytes[:])
		}
		// Known UUID column but value doesn't match - treat as string
		w.uuidColumns[name] = false
	} else {
		// Not a known UUID column and first value doesn't look like UUID
		w.uuidColumns[name] = false
	}

	return w.setStringDirect(b, name, idx, value)
}

func (w *Writer) setStringDirect(b *blockBuilder, name string, idx int, value string) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeString)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setString(idx, value); err != nil {
		return err
	}
	cb.stats.recordString(value)
	w.recordDedicatedValue(b, name, StringValueKey(value))
	return nil
}

func (w *Writer) setInt64(b *blockBuilder, name string, idx int, value int64) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeInt64)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setInt64(idx, value); err != nil {
		return err
	}
	cb.stats.recordInt(value)
	w.recordDedicatedValue(b, name, IntValueKey(value))
	return nil
}

func (w *Writer) setUint64(b *blockBuilder, name string, idx int, value uint64) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeUint64)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setUint64(idx, value); err != nil {
		return err
	}
	cb.stats.recordUint(value)
	w.recordDedicatedValue(b, name, UintValueKey(value))
	return nil
}

func (w *Writer) setFloat64(b *blockBuilder, name string, idx int, value float64) error {
	// Columns are single-typed per name; mismatched writes are rejected in column builders.
	cb := w.getOrCreateColumn(b, name, ColumnTypeFloat64)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setFloat64(idx, value); err != nil {
		return err
	}
	cb.stats.recordFloat(value)
	w.recordDedicatedValue(b, name, FloatValueKey(value))
	return nil
}

func (w *Writer) setBytes(b *blockBuilder, name string, idx int, value []byte) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeBytes)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setBytes(idx, value); err != nil {
		return err
	}
	cb.stats.recordBytes(name, value)
	if value != nil {
		w.recordDedicatedValue(b, name, BytesValueKey(value))
	}
	return nil
}

func (w *Writer) setBool(b *blockBuilder, name string, idx int, value bool) error {
	cb := w.getOrCreateColumn(b, name, ColumnTypeBool)
	if cb == nil {
		return nil // Type mismatch - skip
	}
	if err := cb.setBool(idx, value); err != nil {
		return err
	}
	cb.stats.recordBool(value)
	w.recordDedicatedValue(b, name, BoolValueKey(value))
	return nil
}

func (w *Writer) getOrCreateColumn(b *blockBuilder, name string, typ ColumnType) *columnBuilder {
	if cb, ok := b.columns[name]; ok {
		// Type mismatch: column already exists with different type
		// This can happen when different spans use the same attribute name with different types
		// Skip this value rather than causing an error
		if cb.typ != typ {
			return nil
		}
		return cb
	}
	cb := newColumnBuilder(name, typ, b.spanCount)
	b.columns[name] = cb
	return cb
}

// Flush finalizes all blocks and returns the encoded bytes.
func (w *Writer) Flush() ([]byte, error) {
	// Step 1: Sort buffered spans
	// Compute sort keys
	w.sortArena.Free()
	for _, buffered := range w.spanBuffer {
		buffered.ComputeSortKey(w.minhashCache, &w.sortArena)
	}
	w.sortArena.Free()

	// Sort: primary by service.name, secondary by MinHash
	w.sortSpanBuffer(w.spanBuffer)

	// Step 2: Write sorted spans to blocks
	w.blocks = nil // Clear any existing blocks
	w.current = nil
	w.dedicatedIndex = make(map[string]map[string]map[int]struct{}) // Clear dedicated index
	w.traceBlockIndex = make(map[[16]byte]map[int]struct{})         // Clear trace block index

	for _, buffered := range w.spanBuffer {
		// Ensure block exists
		if w.current == nil {
			w.current = newBlockBuilder()
		}

		// Add span to current block
		if err := w.addSpanToBlock(w.current, buffered); err != nil {
			return nil, fmt.Errorf("add span to block: %w", err)
		}

		// Close block if full
		if w.current.spanCount >= w.maxBlockSpans {
			w.closeCurrentBlock()
		}
	}

	// Close final block
	w.closeCurrentBlock()

	// Step 3: Compute range buckets and build bucket index
	if err := w.computeRangeBuckets(); err != nil {
		return nil, fmt.Errorf("compute range buckets: %w", err)
	}

	// Step 4: Handle streaming vs non-streaming mode
	if w.output != nil {
		// Streaming mode: blocks already written, just write metadata + header + footer
		return w.flushStreaming()
	}

	// Non-streaming mode: Serialize blocks (original logic)
	blockPayloads := make([][]byte, 0, len(w.blocks))
	blockIndex := make([]blockIndexEntry, 0, len(w.blocks))

	for _, b := range w.blocks {
		payload, idxMeta, err := buildBlockPayload(b, w.version, w.zstdEncoder)
		if err != nil {
			return nil, err
		}
		blockPayloads = append(blockPayloads, payload)
		blockIndex = append(blockIndex, idxMeta)
	}

	var aggregateBytes []byte
	if len(w.metricStreams) > 0 {
		aggBytes, err := w.buildMetricStreamBlocks()
		if err != nil {
			return nil, fmt.Errorf("build aggregate blocks: %w", err)
		}
		aggregateBytes = aggBytes
	}

	var out []byte
	var err error
	if len(w.layerSpans) > 1 {
		out, err = buildNestedBlockpack(w.layerSpans, blockPayloads, blockIndex, w.dedicatedIndex, w.traceBlockIndex, w.rangeBucketMeta, w.autoDedicatedTypes, w.version, aggregateBytes)
	} else {
		out, err = serializeBlockpack(blockPayloads, cloneBlockEntries(blockIndex), w.dedicatedIndex, w.traceBlockIndex, w.rangeBucketMeta, w.autoDedicatedTypes, w.version, aggregateBytes)
	}
	if err != nil {
		return nil, err
	}

	// Clear span buffer and reset totalSpans to allow writer reuse
	w.spanBuffer = w.spanBuffer[:0]
	w.totalSpans = 0

	return out, nil
}

// flushStreaming writes metadata, header, and footer for streaming mode
// File structure: [blocks (already written)] [metadata] [header] [footer]
func (w *Writer) flushStreaming() ([]byte, error) {
	// Build block index from blockRefs
	blockIndex := make([]blockIndexEntry, len(w.blockRefs))
	for i, ref := range w.blockRefs {
		blockIndex[i] = ref.meta
		blockIndex[i].Offset = uint64(ref.offset)
		blockIndex[i].Length = uint64(ref.length)
	}

	// Build dedicated index buffer
	dedicatedBuf, err := buildDedicatedIndexBuffer(w.dedicatedIndex, w.rangeBucketMeta, w.autoDedicatedTypes)
	if err != nil {
		return nil, fmt.Errorf("build dedicated index: %w", err)
	}

	// Build trace index buffer
	traceIndexBuf, err := buildTraceBlockIndexBuffer(w.traceBlockIndex)
	if err != nil {
		return nil, fmt.Errorf("build trace index: %w", err)
	}

	// Build aggregate blocks if needed
	var aggregateBytes []byte
	if len(w.metricStreams) > 0 {
		aggBytes, err := w.buildMetricStreamBlocks()
		if err != nil {
			return nil, fmt.Errorf("build aggregate blocks: %w", err)
		}
		aggregateBytes = aggBytes
	}

	// Write metadata section
	metadataBuf := &bytes.Buffer{}

	// Write block index
	if err := binary.Write(metadataBuf, binary.LittleEndian, uint32(len(blockIndex))); err != nil {
		return nil, err
	}
	for _, entry := range blockIndex {
		if err := entry.write(metadataBuf, w.version); err != nil {
			return nil, err
		}
	}

	// Write dedicated index
	if _, err := metadataBuf.Write(dedicatedBuf.Bytes()); err != nil {
		return nil, err
	}

	// Write trace index
	if _, err := metadataBuf.Write(traceIndexBuf.Bytes()); err != nil {
		return nil, err
	}

	// Write aggregate blocks if present
	if len(aggregateBytes) > 0 {
		if _, err := metadataBuf.Write(aggregateBytes); err != nil {
			return nil, err
		}
	}

	metadataOffset := w.currentPos
	if _, err := w.output.Write(metadataBuf.Bytes()); err != nil {
		return nil, fmt.Errorf("write metadata: %w", err)
	}
	w.currentPos += int64(metadataBuf.Len())

	// Write header
	headerBuf := &bytes.Buffer{}
	headerBuf.Write([]byte("BKPK")) // Magic
	if err := binary.Write(headerBuf, binary.LittleEndian, w.version); err != nil {
		return nil, err
	}
	if err := binary.Write(headerBuf, binary.LittleEndian, uint64(metadataOffset)); err != nil {
		return nil, err
	}

	headerOffset := w.currentPos
	if _, err := w.output.Write(headerBuf.Bytes()); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}
	w.currentPos += int64(headerBuf.Len())

	// Write footer (points to header)
	footerBuf := &bytes.Buffer{}
	footerBuf.Write([]byte("BKPK")) // Magic
	if err := binary.Write(footerBuf, binary.LittleEndian, uint32(headerOffset)); err != nil {
		return nil, err
	}

	footerSize, err := w.output.Write(footerBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("write footer: %w", err)
	}
	w.currentPos += int64(footerSize)

	// Final sync to ensure all data reaches disk
	if syncer, ok := w.output.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			return nil, fmt.Errorf("sync output: %w", err)
		}
	}

	// Clear span buffer and reset
	w.spanBuffer = w.spanBuffer[:0]
	w.totalSpans = 0

	// In streaming mode, return a marker with the total bytes written
	// The actual data is already in the output stream
	marker := []byte(fmt.Sprintf("STREAM:%d", w.currentPos))
	return marker, nil
}

func serializeBlockpack(payloads [][]byte, entries []blockIndexEntry, dedicatedIndex map[string]map[string]map[int]struct{}, traceBlockIndex map[[16]byte]map[int]struct{}, rangeBucketMeta map[string]*RangeBucketMetadata, autoDedicatedTypes map[string]ColumnType, version uint8, aggregateBytes []byte) ([]byte, error) {
	dedicatedBuf, err := buildDedicatedIndexBuffer(dedicatedIndex, rangeBucketMeta, autoDedicatedTypes)
	if err != nil {
		return nil, err
	}

	// Compute offsets and lengths (payloads are stored first).
	offset := 0
	for i := range entries {
		entries[i].Offset = uint64(offset)
		entries[i].Length = uint64(len(payloads[i]))
		offset += len(payloads[i])
	}

	metadataBuf := &bytes.Buffer{}
	if err := binary.Write(metadataBuf, binary.LittleEndian, uint32(len(entries))); err != nil {
		return nil, err
	}

	// Block index
	for _, entry := range entries {
		if err := entry.write(metadataBuf, version); err != nil {
			return nil, err
		}
	}

	if _, err := metadataBuf.Write(dedicatedBuf.Bytes()); err != nil {
		return nil, err
	}

	// Write per-column offsets for selective I/O
	if err := writeColumnIndex(metadataBuf, entries); err != nil {
		return nil, fmt.Errorf("write column index: %w", err)
	}

	// Write trace block index for fast trace-by-ID lookups
	traceIndexBuf, err := buildTraceBlockIndexBuffer(traceBlockIndex)
	if err != nil {
		return nil, fmt.Errorf("build trace block index: %w", err)
	}
	if _, err := metadataBuf.Write(traceIndexBuf.Bytes()); err != nil {
		return nil, err
	}

	metadataBytes := metadataBuf.Bytes()
	metadataOffset := uint64(offset)
	metadataLen := uint64(len(metadataBytes))
	metadataCRC := crc32.ChecksumIEEE(metadataBytes)

	var buf bytes.Buffer
	// Payloads
	for _, payload := range payloads {
		if _, err := buf.Write(payload); err != nil {
			return nil, err
		}
	}

	if _, err := buf.Write(metadataBytes); err != nil {
		return nil, err
	}

	// Write aggregate blocks (after metadata, before footer)
	aggregateOffset := uint64(buf.Len())
	aggregateLen := uint64(0)
	if len(aggregateBytes) > 0 {
		aggregateLen = uint64(len(aggregateBytes))
		if _, err := buf.Write(aggregateBytes); err != nil {
			return nil, err
		}
	}

	if err := writeFooter(&buf, metadataOffset, metadataLen, metadataCRC, aggregateOffset, aggregateLen, version); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func buildNestedBlockpack(layerSpans []int, payloads [][]byte, entries []blockIndexEntry, dedicatedIndex map[string]map[string]map[int]struct{}, traceBlockIndex map[[16]byte]map[int]struct{}, rangeBucketMeta map[string]*RangeBucketMetadata, autoDedicatedTypes map[string]ColumnType, version uint8, aggregateBytes []byte) ([]byte, error) {
	if len(layerSpans) <= 1 {
		return serializeBlockpack(payloads, cloneBlockEntries(entries), dedicatedIndex, traceBlockIndex, rangeBucketMeta, autoDedicatedTypes, version, aggregateBytes)
	}

	currentPayloads := payloads
	currentEntries := entries
	currentDedicated := dedicatedIndex
	currentTrace := traceBlockIndex

	for level := len(layerSpans) - 1; level > 0; level-- {
		maxSpans := layerSpans[level-1]
		groups := groupEntriesBySpanCount(currentEntries, maxSpans)
		parentPayloads := make([][]byte, 0, len(groups))
		parentEntries := make([]blockIndexEntry, 0, len(groups))
		parentByBlockID := make([]int, len(currentEntries))

		for groupIdx, group := range groups {
			blockMap := make(map[int]int, len(group))
			groupEntries := make([]blockIndexEntry, len(group))
			groupPayloads := make([][]byte, len(group))
			for i, idx := range group {
				blockMap[idx] = i
				groupEntries[i] = currentEntries[idx]
				groupPayloads[i] = currentPayloads[idx]
			}

			groupDedicated := remapDedicatedIndex(currentDedicated, blockMap)
			groupTrace := remapTraceIndex(currentTrace, blockMap)

			childBytes, err := serializeBlockpack(groupPayloads, cloneBlockEntries(groupEntries), groupDedicated, groupTrace, rangeBucketMeta, autoDedicatedTypes, version, nil)
			if err != nil {
				return nil, err
			}

			parentPayloads = append(parentPayloads, childBytes)
			parentEntries = append(parentEntries, aggregateBlockEntry(groupEntries))
			for _, idx := range group {
				parentByBlockID[idx] = groupIdx
			}
		}

		currentPayloads = parentPayloads
		currentEntries = parentEntries
		currentDedicated = remapDedicatedIndexBySlice(currentDedicated, parentByBlockID)
		currentTrace = remapTraceIndexBySlice(currentTrace, parentByBlockID)
	}

	return serializeBlockpack(currentPayloads, cloneBlockEntries(currentEntries), currentDedicated, currentTrace, rangeBucketMeta, autoDedicatedTypes, version, aggregateBytes)
}

func groupEntriesBySpanCount(entries []blockIndexEntry, maxSpans int) [][]int {
	if len(entries) == 0 {
		return nil
	}
	var groups [][]int
	var current []int
	currentCount := 0
	for idx, entry := range entries {
		if currentCount > 0 && currentCount+int(entry.SpanCount) > maxSpans {
			groups = append(groups, current)
			current = nil
			currentCount = 0
		}
		current = append(current, idx)
		currentCount += int(entry.SpanCount)
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

func aggregateBlockEntry(entries []blockIndexEntry) blockIndexEntry {
	var out blockIndexEntry
	out.Kind = blockEntryKindBlockpack
	out.MinStart = ^uint64(0)
	for _, entry := range entries {
		out.SpanCount += entry.SpanCount
		if entry.MinStart < out.MinStart {
			out.MinStart = entry.MinStart
		}
		if entry.MaxStart > out.MaxStart {
			out.MaxStart = entry.MaxStart
		}
		if out.MinTraceID == ([16]byte{}) || bytes.Compare(entry.MinTraceID[:], out.MinTraceID[:]) < 0 {
			out.MinTraceID = entry.MinTraceID
		}
		if bytes.Compare(entry.MaxTraceID[:], out.MaxTraceID[:]) > 0 {
			out.MaxTraceID = entry.MaxTraceID
		}
		for i := range out.ColumnNameBloom {
			out.ColumnNameBloom[i] |= entry.ColumnNameBloom[i]
		}
	}
	if out.MinStart == ^uint64(0) {
		out.MinStart = 0
	}
	out.ValueStats = aggregateValueStats(entries)
	return out
}

func aggregateValueStats(entries []blockIndexEntry) map[string]AttributeStats {
	var merged map[string]AttributeStats
	invalid := make(map[string]struct{})
	for _, entry := range entries {
		if entry.ValueStats == nil {
			continue
		}
		if merged == nil {
			merged = make(map[string]AttributeStats)
		}
		for name, stats := range entry.ValueStats {
			if _, bad := invalid[name]; bad {
				continue
			}
			if existing, ok := merged[name]; ok {
				next, ok := mergeAttributeStats(existing, stats)
				if !ok {
					delete(merged, name)
					invalid[name] = struct{}{}
					continue
				}
				merged[name] = next
				continue
			}
			merged[name] = stats
		}
	}
	return merged
}

func remapDedicatedIndex(dedicatedIndex map[string]map[string]map[int]struct{}, blockMap map[int]int) map[string]map[string]map[int]struct{} {
	if len(dedicatedIndex) == 0 {
		return map[string]map[string]map[int]struct{}{}
	}
	out := make(map[string]map[string]map[int]struct{}, len(dedicatedIndex))
	for col, values := range dedicatedIndex {
		for encoded, blocks := range values {
			for blockID := range blocks {
				newID, ok := blockMap[blockID]
				if !ok {
					continue
				}
				colMap, ok := out[col]
				if !ok {
					colMap = make(map[string]map[int]struct{})
					out[col] = colMap
				}
				set, ok := colMap[encoded]
				if !ok {
					set = make(map[int]struct{})
					colMap[encoded] = set
				}
				set[newID] = struct{}{}
			}
		}
	}
	return out
}

func remapDedicatedIndexBySlice(dedicatedIndex map[string]map[string]map[int]struct{}, parentByBlockID []int) map[string]map[string]map[int]struct{} {
	if len(dedicatedIndex) == 0 {
		return map[string]map[string]map[int]struct{}{}
	}
	out := make(map[string]map[string]map[int]struct{}, len(dedicatedIndex))
	for col, values := range dedicatedIndex {
		for encoded, blocks := range values {
			for blockID := range blocks {
				if blockID < 0 || blockID >= len(parentByBlockID) {
					continue
				}
				newID := parentByBlockID[blockID]
				colMap, ok := out[col]
				if !ok {
					colMap = make(map[string]map[int]struct{})
					out[col] = colMap
				}
				set, ok := colMap[encoded]
				if !ok {
					set = make(map[int]struct{})
					colMap[encoded] = set
				}
				set[newID] = struct{}{}
			}
		}
	}
	return out
}

func remapTraceIndex(traceBlockIndex map[[16]byte]map[int]struct{}, blockMap map[int]int) map[[16]byte]map[int]struct{} {
	if len(traceBlockIndex) == 0 {
		return map[[16]byte]map[int]struct{}{}
	}
	out := make(map[[16]byte]map[int]struct{}, len(traceBlockIndex))
	for traceID, blocks := range traceBlockIndex {
		for blockID := range blocks {
			newID, ok := blockMap[blockID]
			if !ok {
				continue
			}
			set, ok := out[traceID]
			if !ok {
				set = make(map[int]struct{})
				out[traceID] = set
			}
			set[newID] = struct{}{}
		}
	}
	return out
}

func remapTraceIndexBySlice(traceBlockIndex map[[16]byte]map[int]struct{}, parentByBlockID []int) map[[16]byte]map[int]struct{} {
	if len(traceBlockIndex) == 0 {
		return map[[16]byte]map[int]struct{}{}
	}
	out := make(map[[16]byte]map[int]struct{}, len(traceBlockIndex))
	for traceID, blocks := range traceBlockIndex {
		for blockID := range blocks {
			if blockID < 0 || blockID >= len(parentByBlockID) {
				continue
			}
			newID := parentByBlockID[blockID]
			set, ok := out[traceID]
			if !ok {
				set = make(map[int]struct{})
				out[traceID] = set
			}
			set[newID] = struct{}{}
		}
	}
	return out
}

func cloneBlockEntries(entries []blockIndexEntry) []blockIndexEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]blockIndexEntry, len(entries))
	copy(out, entries)
	return out
}

// computeRangeBuckets processes collected range bucket values and builds bucket indexes.
// For each range-bucketed column:
// - If >= MinValuesForBucketing unique values: create buckets and build bucket->block index
// - If < MinValuesForBucketing unique values: fall back to normal dedicated column
func (w *Writer) computeRangeBuckets() error {
	const minValuesForBucketing = 100 // from types package constant

	// Process columns with pre-computed boundaries (from two-pass conversion)
	for columnName, boundaries := range w.precomputedBoundaries {
		typ, ok := w.autoDedicatedTypes[columnName]
		if !ok {
			return fmt.Errorf("range bucket column %s not in auto-detected types", columnName)
		}

		// Bucket index was already built during recordRangeBucketValue
		bucketToBlocks := w.rangeBucketIndex[columnName]
		if bucketToBlocks == nil {
			// Column was configured but no values seen
			continue
		}

		// Compute min/max from boundaries
		minVal := boundaries[0]
		maxVal := boundaries[len(boundaries)-1]

		// Store metadata
		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        minVal,
			Max:        maxVal,
			Boundaries: boundaries,
			ColumnType: typ,
		}

		// Add bucket mappings to dedicated index
		if w.dedicatedIndex[columnName] == nil {
			w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		}
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, typ)
			encodedKey := bucketKey.Encode()
			w.dedicatedIndex[columnName][encodedKey] = blocks
		}
	}

	// Process columns with KLL sketches (automatic OOM prevention)
	// Compute boundaries from sketches first, then process blocks
	for columnName, sketch := range w.kllSketches {
		typ, ok := w.autoDedicatedTypes[columnName]
		if !ok {
			return fmt.Errorf("range bucket column %s not in auto-detected types", columnName)
		}

		const numBuckets = 256

		// Check if we have enough values for bucketing
		if sketch.Size() < minValuesForBucketing {
			// Low-cardinality: fall back to direct value indexing below
			continue
		}

		// Compute quantile-based boundaries from KLL sketch
		boundaries := sketch.ComputeQuantiles(numBuckets)
		if len(boundaries) == 0 {
			continue
		}

		// Store boundaries and metadata
		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        boundaries[0],
			Max:        boundaries[len(boundaries)-1],
			Boundaries: boundaries,
			ColumnType: typ,
		}

		// Build bucket -> block mapping using the sample value stored per block
		blockValues := w.rangeBucketValues[columnName]
		bucketToBlocks := make(map[uint16]map[int]struct{})

		for blockID, values := range blockValues {
			if len(values) > 0 {
				// Use the sample value to determine bucket (approximate but sufficient)
				sampleVal := values[0]
				bucketID := GetBucketID(sampleVal, boundaries)

				if bucketToBlocks[bucketID] == nil {
					bucketToBlocks[bucketID] = make(map[int]struct{})
				}
				bucketToBlocks[bucketID][blockID] = struct{}{}
			}
		}

		// Add bucket mappings to dedicated index
		if w.dedicatedIndex[columnName] == nil {
			w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		}
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, typ)
			encodedKey := bucketKey.Encode()
			w.dedicatedIndex[columnName][encodedKey] = blocks
		}

		// Mark column as processed
		delete(w.rangeBucketValues, columnName)
	}

	// Process remaining columns in rangeBucketValues (low-cardinality fallback)
	for columnName, blockValues := range w.rangeBucketValues {
		// All range-bucketed columns were auto-detected
		typ, ok := w.autoDedicatedTypes[columnName]
		if !ok {
			return fmt.Errorf("range bucket column %s not in auto-detected types", columnName)
		}

		// Collect all unique values across all blocks (only for low-cardinality columns)
		uniqueValues := make(map[int64]struct{})
		var allValues []int64
		for _, values := range blockValues {
			for _, v := range values {
				if _, seen := uniqueValues[v]; !seen {
					uniqueValues[v] = struct{}{}
					allValues = append(allValues, v)
				}
			}
		}

		// Decide: bucket or fall back
		if len(uniqueValues) < minValuesForBucketing {
			// Fall back to direct value indexing: treat each unique value as its own "bucket"
			// This works well for low-cardinality columns (< 100 unique values)

			// Build value -> block ID mapping directly
			// Use RangeInt64ValueKey with raw values (8 bytes) for direct indexing
			if w.dedicatedIndex[columnName] == nil {
				w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
			}

			for blockID, values := range blockValues {
				// Get unique values for this block
				blockValues := make(map[int64]struct{})
				for _, v := range values {
					blockValues[v] = struct{}{}
				}

				// Add this block to each value's block list
				// Use RangeInt64ValueKey with raw value (8 bytes)
				for value := range blockValues {
					key := RangeInt64ValueKey(value, typ)
					encodedKey := key.Encode()
					if w.dedicatedIndex[columnName][encodedKey] == nil {
						w.dedicatedIndex[columnName][encodedKey] = make(map[int]struct{})
					}
					w.dedicatedIndex[columnName][encodedKey][blockID] = struct{}{}
				}
			}

			continue
		}

		// Compute bucket boundaries from actual data distribution (quantile-based)
		sort.Slice(allValues, func(i, j int) bool { return allValues[i] < allValues[j] })
		minVal := allValues[0]
		maxVal := allValues[len(allValues)-1]

		numBuckets := 100 // MaxRangeBuckets
		if numBuckets > len(uniqueValues) {
			numBuckets = len(uniqueValues)
		}

		// Use quantile-based bucketing for equal distribution of data across buckets
		boundaries := CalculateBucketsFromValues(allValues, numBuckets)

		// Store metadata for later serialization
		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        minVal,
			Max:        maxVal,
			Boundaries: boundaries,
			ColumnType: typ,
		}

		// Build bucket ID -> block ID mapping
		bucketToBlocks := make(map[uint16]map[int]struct{})
		for blockID, values := range blockValues {
			// Get unique bucket IDs for this block's values
			blockBuckets := make(map[uint16]struct{})
			for _, value := range values {
				bucketID := GetBucketID(value, boundaries)
				blockBuckets[bucketID] = struct{}{}
			}

			// Add this block to each bucket it contains
			for bucketID := range blockBuckets {
				if bucketToBlocks[bucketID] == nil {
					bucketToBlocks[bucketID] = make(map[int]struct{})
				}
				bucketToBlocks[bucketID][blockID] = struct{}{}
			}
		}

		// Add bucket mappings to dedicated index using bucket keys
		if w.dedicatedIndex[columnName] == nil {
			w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		}
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, typ)
			encodedKey := bucketKey.Encode()
			w.dedicatedIndex[columnName][encodedKey] = blocks
		}
	}

	return nil
}

// Bloom helpers live in bloom.go.

func writeDedicatedValue(buf *bytes.Buffer, key DedicatedValueKey) error {
	data := key.Data()
	switch key.Type() {
	case ColumnTypeString, ColumnTypeBytes:
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(data))); err != nil {
			return err
		}
		if len(data) > 0 {
			if _, err := buf.Write(data); err != nil {
				return err
			}
		}
		return nil
	case ColumnTypeInt64, ColumnTypeUint64, ColumnTypeFloat64:
		if len(data) != 8 {
			return fmt.Errorf("invalid dedicated numeric key length %d", len(data))
		}
		_, err := buf.Write(data)
		return err
	case ColumnTypeBool:
		if len(data) != 1 {
			return fmt.Errorf("invalid dedicated bool key length %d", len(data))
		}
		return buf.WriteByte(data[0])
	case ColumnTypeRangeInt64, ColumnTypeRangeUint64, ColumnTypeRangeDuration:
		// Range keys can be either:
		// - 2 bytes: bucket ID (uint16) for bucketed columns
		// - 8 bytes: raw value (int64) for direct value indexing (low cardinality)
		if len(data) != 2 && len(data) != 8 {
			return fmt.Errorf("invalid range key length %d (expected 2 or 8)", len(data))
		}
		// Write length prefix to distinguish bucket IDs from raw values
		if err := buf.WriteByte(byte(len(data))); err != nil {
			return err
		}
		_, err := buf.Write(data)
		return err
	default:
		return fmt.Errorf("unsupported dedicated column type %d", key.Type())
	}
}

func buildDedicatedIndexBuffer(dedicatedIndex map[string]map[string]map[int]struct{}, rangeBucketMeta map[string]*RangeBucketMetadata, autoDedicatedTypes map[string]ColumnType) (*bytes.Buffer, error) {
	dedicatedBuf := &bytes.Buffer{}
	dedicatedCols := make([]string, 0, len(dedicatedIndex))
	for col := range dedicatedIndex {
		dedicatedCols = append(dedicatedCols, col)
	}
	sort.Strings(dedicatedCols)

	if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(dedicatedCols))); err != nil {
		return nil, err
	}
	for _, col := range dedicatedCols {
		// Get type from auto-detected types
		typ, ok := autoDedicatedTypes[col]
		if !ok {
			return nil, fmt.Errorf("unknown dedicated column %s (not in auto-detected types)", col)
		}
		if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint16(len(col))); err != nil {
			return nil, err
		}
		if _, err := dedicatedBuf.Write([]byte(col)); err != nil {
			return nil, err
		}
		if err := dedicatedBuf.WriteByte(byte(typ)); err != nil {
			return nil, err
		}

		// Check if this is a range-bucketed column with metadata
		bucketMeta, hasBuckets := rangeBucketMeta[col]
		if hasBuckets && IsRangeColumnType(typ) {
			// Write bucket metadata flag (1 = has buckets)
			if err := dedicatedBuf.WriteByte(1); err != nil {
				return nil, err
			}

			// Write min, max, and boundaries
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, bucketMeta.Min); err != nil {
				return nil, err
			}
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, bucketMeta.Max); err != nil {
				return nil, err
			}
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(bucketMeta.Boundaries))); err != nil {
				return nil, err
			}
			for _, boundary := range bucketMeta.Boundaries {
				if err := binary.Write(dedicatedBuf, binary.LittleEndian, boundary); err != nil {
					return nil, err
				}
			}
		} else {
			// Write bucket metadata flag (0 = no buckets, normal dedicated column)
			if err := dedicatedBuf.WriteByte(0); err != nil {
				return nil, err
			}
		}

		valueMap := dedicatedIndex[col]
		valueKeys := make([]string, 0, len(valueMap))
		for k := range valueMap {
			valueKeys = append(valueKeys, k)
		}
		sort.Strings(valueKeys)

		if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(valueKeys))); err != nil {
			return nil, err
		}
		for _, encodedKey := range valueKeys {
			key, err := DecodeDedicatedKey(encodedKey)
			if err != nil {
				return nil, err
			}
			if key.Type() != typ {
				return nil, fmt.Errorf("dedicated column %s has mismatched key type %d (expected %d)", col, key.Type(), typ)
			}
			if err := writeDedicatedValue(dedicatedBuf, key); err != nil {
				return nil, err
			}
			ids := make([]int, 0, len(valueMap[encodedKey]))
			for id := range valueMap[encodedKey] {
				ids = append(ids, id)
			}
			sort.Ints(ids)
			if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(len(ids))); err != nil {
				return nil, err
			}
			for _, id := range ids {
				if err := binary.Write(dedicatedBuf, binary.LittleEndian, uint32(id)); err != nil {
					return nil, err
				}
			}
		}
	}

	return dedicatedBuf, nil
}

// buildTraceBlockIndexBuffer serializes the trace block index.
// Format:
//   - trace_count (4 bytes)
//   - For each trace (sorted by trace_id):
//   - trace_id (16 bytes)
//   - block_count (2 bytes)
//   - block_ids (2 bytes each, sorted)
func buildTraceBlockIndexBuffer(traceBlockIndex map[[16]byte]map[int]struct{}) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}

	// Sort trace IDs for deterministic output and binary search
	traceIDs := make([][16]byte, 0, len(traceBlockIndex))
	for traceID := range traceBlockIndex {
		traceIDs = append(traceIDs, traceID)
	}
	sort.Slice(traceIDs, func(i, j int) bool {
		return bytes.Compare(traceIDs[i][:], traceIDs[j][:]) < 0
	})

	// Write trace count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(traceIDs))); err != nil {
		return nil, err
	}

	// Write each trace entry
	for _, traceID := range traceIDs {
		blockSet := traceBlockIndex[traceID]

		// Convert set to sorted slice
		blockIDs := make([]int, 0, len(blockSet))
		for blockID := range blockSet {
			blockIDs = append(blockIDs, blockID)
		}
		sort.Ints(blockIDs)

		// Write trace_id (16 bytes)
		if _, err := buf.Write(traceID[:]); err != nil {
			return nil, err
		}

		// Write block_count (2 bytes)
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(blockIDs))); err != nil {
			return nil, err
		}

		// Write block_ids (2 bytes each)
		for _, blockID := range blockIDs {
			if err := binary.Write(buf, binary.LittleEndian, uint16(blockID)); err != nil {
				return nil, err
			}
		}
	}

	return buf, nil
}

func writeFooter(buf *bytes.Buffer, metadataOffset, metadataLen uint64, metadataCRC uint32, metricStreamOffset, metricStreamLen uint64, version uint8) error {
	if err := binary.Write(buf, binary.LittleEndian, magicNumber); err != nil {
		return err
	}
	if err := buf.WriteByte(version); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, metadataOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, metadataLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, metadataCRC); err != nil {
		return err
	}
	// Write metric stream block offset and length (v9 extension)
	if err := binary.Write(buf, binary.LittleEndian, metricStreamOffset); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, metricStreamLen); err != nil {
		return err
	}
	return nil
}

// SerializableAgg is a compact representation of vm.AggBucket for serialization.
// Uses pointer fields to store ONLY the aggregate values required by the specific query,
// dramatically reducing size. For example:
// - COUNT query: only GroupKey + Count (~24 bytes)
// - AVG query: only GroupKey + Sum + Count (~40 bytes)
// - QUANTILE query: only GroupKey + specific quantile (~32 bytes)
// This provides 150-250x size reduction compared to storing all fields.
type SerializableAgg struct {
	GroupKey []interface{} `json:"k"`             // Group-by key values
	Count    *int64        `json:"c,omitempty"`   // Count of values
	Sum      *float64      `json:"s,omitempty"`   // Sum of values
	Min      *float64      `json:"m,omitempty"`   // Minimum value
	Max      *float64      `json:"x,omitempty"`   // Maximum value
	P50      *float64      `json:"p50,omitempty"` // 50th percentile (median)
	P95      *float64      `json:"p95,omitempty"` // 95th percentile
	P99      *float64      `json:"p99,omitempty"` // 99th percentile
}

// GroupKeyIndex maps group key string to index for deduplication in binary format.
// This enables storing group keys once and referencing by index in each bucket.
type GroupKeyIndex struct {
	Keys  []vm.GroupKey  // Unique group keys in order
	Index map[string]int // Map from key string to index
}

// BinaryAggValue represents a single aggregate value for binary encoding.
type BinaryAggValue struct {
	Type  uint8   // 1=Count, 2=Sum, 3=Min, 4=Max, 5=P50, 6=P95, 7=P99
	Value float64 // All numeric values as float64
}

// Aggregate value type constants for binary encoding (exported for parser)
const (
	AggtypeCount uint8 = 1
	AggtypeSum   uint8 = 2
	AggtypeMin   uint8 = 3
	AggtypeMax   uint8 = 4
	AggtypeP50   uint8 = 5
	AggtypeP95   uint8 = 6
	AggtypeP99   uint8 = 7
)

// Value type constants for binary encoding of group key values (exported for parser)
const (
	ValueTypeString  uint8 = 0
	ValueTypeInt64   uint8 = 1
	ValueTypeFloat64 uint8 = 2
	ValueTypeBool    uint8 = 3
)

// Compression constants for per-stream compression
const (
	CompressionNone uint8 = 0
	CompressionZstd uint8 = 1
)

// convertAggregatesForSerialization converts a map of vm.AggBucket to SerializableAgg,
// storing ONLY the fields required by the specific aggregate function to minimize size.
// This provides dramatic size reduction:
// - COUNT/RATE: ~24 bytes per group (just GroupKey + Count)
// - AVG: ~40 bytes per group (GroupKey + Sum + Count)
// - SUM: ~32 bytes per group (GroupKey + Sum)
// - MIN/MAX: ~32 bytes per group (GroupKey + Min or Max)
// - QUANTILE: ~32 bytes per group (GroupKey + specific quantile)
func convertAggregatesForSerialization(aggregates map[string]*vm.AggBucket, aggFunc string, quantile float64) map[string]*SerializableAgg {
	result := make(map[string]*SerializableAgg, len(aggregates))

	for key, bucket := range aggregates {
		serAgg := &SerializableAgg{
			GroupKey: make([]interface{}, len(bucket.GroupKey.Values)),
		}

		// Copy group key values
		for i, val := range bucket.GroupKey.Values {
			serAgg.GroupKey[i] = val.Data
		}

		// Populate ONLY the fields needed for this aggregate function
		switch aggFunc {
		case "COUNT", "RATE":
			// COUNT/RATE only needs Count
			serAgg.Count = &bucket.Count

		case "AVG":
			// AVG needs Sum and Count to compute average
			serAgg.Sum = &bucket.Sum
			serAgg.Count = &bucket.Count

		case "SUM":
			// SUM only needs Sum
			serAgg.Sum = &bucket.Sum

		case "MIN":
			// MIN only needs Min
			serAgg.Min = &bucket.Min

		case "MAX":
			// MAX only needs Max
			serAgg.Max = &bucket.Max

		case "QUANTILE":
			// QUANTILE needs the specific quantile value
			// Note: Currently stubbed out as quantile sketches are in executor.sketchMap
			// TODO: Extract actual quantile values from sketches
			switch quantile {
			case 0.50:
				val := float64(0) // Placeholder until sketch integration
				serAgg.P50 = &val
			case 0.95:
				val := float64(0) // Placeholder until sketch integration
				serAgg.P95 = &val
			case 0.99:
				val := float64(0) // Placeholder until sketch integration
				serAgg.P99 = &val
			}

		case "HISTOGRAM":
			// HISTOGRAM needs all quantiles for bucket boundaries
			// For now, store p50, p95, p99 as common histogram buckets
			p50 := float64(0)
			p95 := float64(0)
			p99 := float64(0)
			serAgg.P50 = &p50
			serAgg.P95 = &p95
			serAgg.P99 = &p99

		case "STDDEV":
			// STDDEV needs Sum and Count for mean, plus values for variance
			// For now, store Sum and Count (variance calculation TBD)
			serAgg.Sum = &bucket.Sum
			serAgg.Count = &bucket.Count

		default:
			// Unknown aggregate function - store all fields for safety
			serAgg.Count = &bucket.Count
			serAgg.Sum = &bucket.Sum
			serAgg.Min = &bucket.Min
			serAgg.Max = &bucket.Max
		}

		result[key] = serAgg
	}

	return result
}

// buildGroupKeyIndex creates an index of unique group keys across all buckets.
// This enables deduplication - group keys are stored once and referenced by index.
func buildGroupKeyIndex(buckets []*TimeBucket) *GroupKeyIndex {
	idx := &GroupKeyIndex{
		Keys:  make([]vm.GroupKey, 0),
		Index: make(map[string]int),
	}

	for _, bucket := range buckets {
		for _, aggBucket := range bucket.Aggregates {
			keyStr := aggBucket.GroupKey.Serialize()
			if _, exists := idx.Index[keyStr]; !exists {
				idx.Index[keyStr] = len(idx.Keys)
				idx.Keys = append(idx.Keys, aggBucket.GroupKey)
			}
		}
	}

	return idx
}

// writeGroupKeyValue writes a single group key value to the buffer in binary format.
func writeGroupKeyValue(buf *bytes.Buffer, val vm.Value) error {
	switch v := val.Data.(type) {
	case nil:
		// Handle nil values (missing fields) as empty strings
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeString); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(0)); err != nil {
			return err
		}
	case string:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeString); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(v))); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(v)); err != nil {
			return err
		}
	case int64:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeInt64); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return err
		}
	case float64:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeFloat64); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return err
		}
	case bool:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeBool); err != nil {
			return err
		}
		var boolByte uint8
		if v {
			boolByte = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, boolByte); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported group key value type: %T", val.Data)
	}
	return nil
}

// writeGroupKey writes a group key to the buffer in binary format.
func writeGroupKey(buf *bytes.Buffer, gk vm.GroupKey) error {
	// Write field count
	if err := binary.Write(buf, binary.LittleEndian, uint8(len(gk.Values))); err != nil {
		return err
	}

	// Write each field value
	for _, val := range gk.Values {
		if err := writeGroupKeyValue(buf, val); err != nil {
			return err
		}
	}
	return nil
}

// getAggregateValues extracts the aggregate values that need to be written for a specific
// aggregate function. Returns a list of (type, value) pairs.
func getAggregateValues(bucket *vm.AggBucket, aggFunc string, field string, quantile float64) []BinaryAggValue {
	var values []BinaryAggValue

	switch aggFunc {
	case "COUNT", "RATE":
		values = append(values, BinaryAggValue{Type: AggtypeCount, Value: float64(bucket.Count)})

	case "AVG":
		values = append(values, BinaryAggValue{Type: AggtypeSum, Value: bucket.Sum})
		values = append(values, BinaryAggValue{Type: AggtypeCount, Value: float64(bucket.Count)})

	case "SUM":
		values = append(values, BinaryAggValue{Type: AggtypeSum, Value: bucket.Sum})

	case "MIN":
		values = append(values, BinaryAggValue{Type: AggtypeMin, Value: bucket.Min})

	case "MAX":
		values = append(values, BinaryAggValue{Type: AggtypeMax, Value: bucket.Max})

	case "QUANTILE":
		// Extract quantile value from sketch
		var quantileValue float64
		if bucket.Quantiles != nil {
			if sketch, exists := bucket.Quantiles[field]; exists && sketch != nil {
				quantileValue = sketch.Quantile(quantile)
			}
		}

		// Write the quantile value with the appropriate type
		switch quantile {
		case 0.50:
			values = append(values, BinaryAggValue{Type: AggtypeP50, Value: quantileValue})
		case 0.95:
			values = append(values, BinaryAggValue{Type: AggtypeP95, Value: quantileValue})
		case 0.99:
			values = append(values, BinaryAggValue{Type: AggtypeP99, Value: quantileValue})
		}

	case "HISTOGRAM":
		// HISTOGRAM stores all quantiles - extract from sketch
		var p50, p95, p99 float64
		if bucket.Quantiles != nil {
			if sketch, exists := bucket.Quantiles[field]; exists && sketch != nil {
				p50 = sketch.Quantile(0.50)
				p95 = sketch.Quantile(0.95)
				p99 = sketch.Quantile(0.99)
			}
		}
		values = append(values, BinaryAggValue{Type: AggtypeP50, Value: p50})
		values = append(values, BinaryAggValue{Type: AggtypeP95, Value: p95})
		values = append(values, BinaryAggValue{Type: AggtypeP99, Value: p99})

	case "STDDEV":
		// STDDEV stores Sum and Count
		values = append(values, BinaryAggValue{Type: AggtypeSum, Value: bucket.Sum})
		values = append(values, BinaryAggValue{Type: AggtypeCount, Value: float64(bucket.Count)})

	default:
		// Unknown function - store all fields for safety
		values = append(values, BinaryAggValue{Type: AggtypeCount, Value: float64(bucket.Count)})
		values = append(values, BinaryAggValue{Type: AggtypeSum, Value: bucket.Sum})
		values = append(values, BinaryAggValue{Type: AggtypeMin, Value: bucket.Min})
		values = append(values, BinaryAggValue{Type: AggtypeMax, Value: bucket.Max})
	}

	return values
}

// writeBinaryAggregates writes aggregates in binary format with group key deduplication.
// Format:
//   - start_time (8 bytes) - timestamp of first bucket
//   - step_millis (4 bytes) - step size in milliseconds
//   - group_key_count (4 bytes)
//   - For each unique group key:
//   - field_count (1 byte)
//   - For each field: value_type + value_len (for strings) + value
//   - For each bucket:
//   - For each group key (by index order):
//   - value_count (1 byte)
//   - For each value: value_type (1 byte) + value (8 bytes)
//
// Bucket times are calculated as: bucket_time = start_time + (i * step_millis * 1_000_000)
func writeBinaryAggregates(buf *bytes.Buffer, buckets []*TimeBucket, groupKeyIdx *GroupKeyIndex, aggFunc string, field string, quantile float64, stepSizeNanos int64) error {
	// Sort buckets by time to ensure deterministic output and correct indexing
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].BucketTime < buckets[j].BucketTime
	})

	// Write start time (first bucket time)
	var startTime int64
	if len(buckets) > 0 {
		startTime = buckets[0].BucketTime
	}
	if err := binary.Write(buf, binary.LittleEndian, startTime); err != nil {
		return err
	}

	// Write step size in milliseconds (4 bytes is enough for up to 49 days)
	// Validate step size before conversion
	if stepSizeNanos < 1_000_000 {
		return fmt.Errorf("step size must be at least 1ms, got %dns", stepSizeNanos)
	}
	stepMillisInt64 := stepSizeNanos / 1_000_000
	if stepMillisInt64 > math.MaxUint32 || stepMillisInt64 < 0 {
		return fmt.Errorf("step size %dns out of valid range for uint32 milliseconds", stepSizeNanos)
	}
	stepMillis := uint32(stepMillisInt64)
	if err := binary.Write(buf, binary.LittleEndian, stepMillis); err != nil {
		return err
	}

	// Write group key count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(groupKeyIdx.Keys))); err != nil {
		return err
	}

	// Write each unique group key
	for _, gk := range groupKeyIdx.Keys {
		if err := writeGroupKey(buf, gk); err != nil {
			return err
		}
	}

	// Write each bucket (bucket times are now implicit: start_time + i * step_millis)
	for _, bucket := range buckets {

		// Create a map from group key index to aggregate values
		// This allows us to write values in group key index order
		aggValuesByIndex := make(map[int][]BinaryAggValue)
		for keyStr, aggBucket := range bucket.Aggregates {
			idx := groupKeyIdx.Index[keyStr]
			aggValuesByIndex[idx] = getAggregateValues(aggBucket, aggFunc, field, quantile)
		}

		// Write aggregate values for each group key (in index order)
		for i := 0; i < len(groupKeyIdx.Keys); i++ {
			values, exists := aggValuesByIndex[i]
			if !exists {
				// No data for this group key in this bucket - write 0 values
				if err := binary.Write(buf, binary.LittleEndian, uint8(0)); err != nil {
					return err
				}
				continue
			}

			// Write value count
			if err := binary.Write(buf, binary.LittleEndian, uint8(len(values))); err != nil {
				return err
			}

			// Write each value
			for _, val := range values {
				if err := binary.Write(buf, binary.LittleEndian, val.Type); err != nil {
					return err
				}
				if err := binary.Write(buf, binary.LittleEndian, val.Value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// mergeBucketsByTime combines buckets with the same BucketTime to keep the serialized
// time series aligned and avoid duplicate buckets caused by out-of-order ingestion.
func mergeBucketsByTime(buckets []*TimeBucket) []*TimeBucket {
	if len(buckets) <= 1 {
		return buckets
	}

	merged := make(map[int64]*TimeBucket, len(buckets))
	for _, bucket := range buckets {
		existing := merged[bucket.BucketTime]
		if existing == nil {
			merged[bucket.BucketTime] = bucket
			continue
		}
		if existing.Aggregates == nil {
			existing.Aggregates = make(map[string]*vm.AggBucket)
		}
		for key, agg := range bucket.Aggregates {
			if existingAgg, ok := existing.Aggregates[key]; ok {
				existingAgg.Merge(agg)
				if agg.SumSq != 0 {
					existingAgg.SumSq += agg.SumSq
				}
				if agg.Histograms != nil {
					if existingAgg.Histograms == nil {
						existingAgg.Histograms = make(map[string]*vm.HistogramData)
					}
					for field, hist := range agg.Histograms {
						if existingHist, ok := existingAgg.Histograms[field]; ok && len(existingHist.Counts) == len(hist.Counts) {
							for i := range hist.Counts {
								existingHist.Counts[i] += hist.Counts[i]
							}
							continue
						}
						existingAgg.Histograms[field] = hist
					}
				}
				continue
			}
			existing.Aggregates[key] = agg
		}
	}

	out := make([]*TimeBucket, 0, len(merged))
	for _, bucket := range merged {
		out = append(out, bucket)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BucketTime < out[j].BucketTime })
	return out
}

// buildMetricStreamBlocks serializes all metric stream aggregates to bytes with per-stream zstd compression
// Format (binary v4 with per-stream compression):
// - stream_count (4 bytes)
// - For each stream:
//   - stream_id_len (2 bytes)
//   - stream_id (N bytes)
//   - compression_flag (1 byte) - 0=none, 1=zstd
//   - uncompressed_len (4 bytes) - original size for validation
//   - compressed_len (4 bytes) - compressed size
//   - compressed_data (N bytes) - stream data (possibly compressed):
//   - query_len (4 bytes)
//   - query (N bytes)
//   - step_size_nanos (8 bytes)
//   - bucket_count (4 bytes)
//   - start_time (8 bytes) - timestamp of first bucket
//   - step_millis (4 bytes) - step size in milliseconds
//   - group_key_count (4 bytes)
//   - For each unique group key:
//   - field_count (1 byte)
//   - For each field: value_type + value_len (for strings) + value
//   - For each bucket (bucket_time implicit: start_time + i * step_millis * 1_000_000):
//   - For each group key (by index):
//   - value_count (1 byte)
//   - For each value: value_type (1 byte) + value (8 bytes)
func (w *Writer) buildMetricStreamBlocks() (result []byte, err error) {
	var buf bytes.Buffer

	// Write stream count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(w.metricStreams))); err != nil {
		return nil, err
	}

	for _, stream := range w.metricStreams {
		// Flush any remaining active buckets
		for _, bucket := range stream.ActiveBuckets {
			stream.CompletedBuckets = append(stream.CompletedBuckets, bucket)
		}
		stream.ActiveBuckets = make(map[int64]*TimeBucket)
		stream.CompletedBuckets = mergeBucketsByTime(stream.CompletedBuckets)

		// Write stream ID (before compression so we can find streams)
		streamID := stream.Def.StreamID
		if err := binary.Write(&buf, binary.LittleEndian, uint16(len(streamID))); err != nil {
			return nil, err
		}
		if _, err := buf.Write([]byte(streamID)); err != nil {
			return nil, err
		}

		// Build the stream data into a temporary buffer
		var streamDataBuf bytes.Buffer

		// Write query
		query := stream.Def.Query
		if err := binary.Write(&streamDataBuf, binary.LittleEndian, uint32(len(query))); err != nil {
			return nil, err
		}
		if _, err := streamDataBuf.Write([]byte(query)); err != nil {
			return nil, err
		}

		// Write step size
		if err := binary.Write(&streamDataBuf, binary.LittleEndian, stream.Def.StepSizeNanos); err != nil {
			return nil, err
		}

		// Write bucket count
		if err := binary.Write(&streamDataBuf, binary.LittleEndian, uint32(len(stream.CompletedBuckets))); err != nil {
			return nil, err
		}

		// Build group key index for deduplication
		groupKeyIdx := buildGroupKeyIndex(stream.CompletedBuckets)

		// Get aggregate function details
		aggFunc := stream.Def.Spec.Aggregate.Function
		field := stream.Def.Spec.Aggregate.Field
		quantile := stream.Def.Spec.Aggregate.Quantile

		// Write aggregates in binary format with group key deduplication
		if err := writeBinaryAggregates(&streamDataBuf, stream.CompletedBuckets, groupKeyIdx, aggFunc, field, quantile, stream.Def.StepSizeNanos); err != nil {
			return nil, fmt.Errorf("failed to write binary aggregates: %w", err)
		}

		// Get uncompressed data
		uncompressedData := streamDataBuf.Bytes()
		uncompressedLen := uint32(len(uncompressedData))

		// Compress the stream data with zstd using per-Writer encoder
		w.zstdEncoder.Reset(nil)
		compressedData := w.zstdEncoder.EncodeAll(uncompressedData, make([]byte, 0, len(uncompressedData)))
		compressedLen := uint32(len(compressedData))

		// Write compression flag (1 = zstd)
		if err := buf.WriteByte(CompressionZstd); err != nil {
			return nil, err
		}

		// Write uncompressed length
		if err := binary.Write(&buf, binary.LittleEndian, uncompressedLen); err != nil {
			return nil, err
		}

		// Write compressed length
		if err := binary.Write(&buf, binary.LittleEndian, compressedLen); err != nil {
			return nil, err
		}

		// Write compressed data
		if _, err := buf.Write(compressedData); err != nil {
			return nil, err
		}
	}

	result = buf.Bytes()
	return result, nil
}

// buildTraceTable serializes trace-level columns and returns the trace table data plus stats.
// The encoder is passed through to column builders for dictionary compression.
func buildTraceTable(traces []*traceEntry, encoder *zstd.Encoder) ([]byte, map[string]columnStatsBuilder, error) {
	if len(traces) == 0 {
		return nil, nil, nil
	}

	// Collect all unique trace column names across all traces
	allColumns := make(map[string]ColumnType)
	for _, trace := range traces {
		for name, cb := range trace.resourceAttrs {
			allColumns[name] = cb.typ
		}
	}

	// Sort column names for deterministic output
	names := make([]string, 0, len(allColumns))
	for name := range allColumns {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build column data for each trace column
	// Each column has len(traces) rows, with NULL for traces that don't have that attribute
	columnData := make(map[string][]byte, len(names))
	columnStats := make(map[string]columnStatsBuilder, len(names))

	for _, name := range names {
		typ := allColumns[name]
		// Create a temporary column builder with len(traces) rows
		cb := newColumnBuilder(name, typ, len(traces))

		// Populate column data: for each trace, set the value if it exists
		for traceIdx, trace := range traces {
			if traceCB, ok := trace.resourceAttrs[name]; ok {
				// Trace has this attribute - copy the value (trace columns have 1 row at index 0)
				if err := copyColumnValue(cb, traceIdx, traceCB, 0); err != nil {
					return nil, nil, fmt.Errorf("failed to copy trace column %s: %w", name, err)
				}
			}
			// If trace doesn't have this attribute, cb already has NULL at traceIdx
		}

		// Build column data with per-Writer encoder
		data, stats, err := cb.buildData(len(traces), encoder)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build trace column %s: %w", name, err)
		}
		columnData[name] = data
		columnStats[name] = stats
	}

	// Serialize trace table:
	// Format: traceCount(4) + columnCount(4) + [name(2+N) + type(1) + dataLen(4) + data]...
	var buf bytes.Buffer

	// Trace count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(traces))); err != nil {
		return nil, nil, err
	}

	// Column count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(names))); err != nil {
		return nil, nil, err
	}

	// For each column: name + type + data
	for _, name := range names {
		typ := allColumns[name]
		data := columnData[name]

		// Name length + name
		if err := binary.Write(&buf, binary.LittleEndian, uint16(len(name))); err != nil {
			return nil, nil, err
		}
		if _, err := buf.Write([]byte(name)); err != nil {
			return nil, nil, err
		}

		// Type
		if err := buf.WriteByte(byte(typ)); err != nil {
			return nil, nil, err
		}

		// Data length + data
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(data))); err != nil {
			return nil, nil, err
		}
		if _, err := buf.Write(data); err != nil {
			return nil, nil, err
		}
	}

	return buf.Bytes(), columnStats, nil
}

// copyColumnValue copies a value from one column to another
func copyColumnValue(dst *columnBuilder, dstIdx int, src *columnBuilder, srcIdx int) error {
	if dst.typ != src.typ {
		return fmt.Errorf("type mismatch: dst=%d src=%d", dst.typ, src.typ)
	}

	// Check if src has a value at srcIdx
	if !isBitSet(src.present, srcIdx) {
		// NULL value - dst already has NULL at dstIdx
		return nil
	}

	switch dst.typ {
	case ColumnTypeString:
		if srcIdx < len(src.stringIndexes) {
			dictIdx := src.stringIndexes[srcIdx]
			if int(dictIdx) < len(src.stringDictVals) {
				return dst.setString(dstIdx, src.stringDictVals[dictIdx])
			}
		}
	case ColumnTypeInt64:
		if srcIdx < len(src.intIndexes) {
			dictIdx := src.intIndexes[srcIdx]
			if int(dictIdx) < len(src.intDictVals) {
				return dst.setInt64(dstIdx, src.intDictVals[dictIdx])
			}
		}
	case ColumnTypeUint64:
		if srcIdx < len(src.uintValues) {
			return dst.setUint64(dstIdx, src.uintValues[srcIdx])
		}
	case ColumnTypeBool:
		if srcIdx < len(src.boolIndexes) {
			dictIdx := src.boolIndexes[srcIdx]
			if int(dictIdx) < len(src.boolDictVals) {
				return dst.setBool(dstIdx, src.boolDictVals[dictIdx] != 0)
			}
		}
	case ColumnTypeFloat64:
		if srcIdx < len(src.floatIndexes) {
			dictIdx := src.floatIndexes[srcIdx]
			if int(dictIdx) < len(src.floatDictVals) {
				return dst.setFloat64(dstIdx, src.floatDictVals[dictIdx])
			}
		}
	case ColumnTypeBytes:
		if srcIdx < len(src.bytesIndexes) {
			dictIdx := src.bytesIndexes[srcIdx]
			if int(dictIdx) < len(src.bytesDictVals) {
				return dst.setBytes(dstIdx, src.bytesDictVals[dictIdx])
			}
		}
	default:
		return fmt.Errorf("unsupported column type: %d", dst.typ)
	}
	return nil
}

// writeColumnIndex serializes the per-column offset data for all blocks
func writeColumnIndex(buf *bytes.Buffer, blockIndex []blockIndexEntry) error {
	for _, entry := range blockIndex {
		// Write column count for this block
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(entry.ColumnIndex))); err != nil {
			return err
		}

		// Write each column entry
		for _, col := range entry.ColumnIndex {
			// Write column name length
			if err := binary.Write(buf, binary.LittleEndian, uint16(len(col.Name))); err != nil {
				return err
			}

			// Write column name
			if _, err := buf.Write([]byte(col.Name)); err != nil {
				return err
			}

			// Write offset (relative to block start)
			if err := binary.Write(buf, binary.LittleEndian, col.Offset); err != nil {
				return err
			}

			// Write length
			if err := binary.Write(buf, binary.LittleEndian, col.Length); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeBlockHeader writes the block header, span column metadata, and trace column metadata.
// Returns the column index for selective I/O.
func writeBlockHeader(buf *bytes.Buffer, b *blockBuilder, totalColumnCount int, traceTableData []byte, names []string, traceColumnNames []string, columnData map[string][]byte, columnStatsBytes map[string][]byte, statsStart int, dataStart int, version uint8) ([]ColumnIndexEntry, error) {
	// Block header (v9 block layout)
	if err := binary.Write(buf, binary.LittleEndian, magicNumber); err != nil {
		return nil, err
	}
	if err := buf.WriteByte(version); err != nil {
		return nil, err
	}
	_, _ = buf.Write([]byte{0, 0, 0}) // reserved
	if err := binary.Write(buf, binary.LittleEndian, uint32(b.spanCount)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(totalColumnCount)); err != nil {
		return nil, err
	}
	// Trace count and trace table length
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(b.traces))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(traceTableData))); err != nil {
		return nil, err
	}

	// Metadata: first write span columns, then trace columns
	statsOffset := statsStart
	dataOffset := dataStart

	// Track per-column offsets for selective I/O
	columnIndex := make([]ColumnIndexEntry, 0, len(names))

	// Write span column metadata
	for _, name := range names {
		cb := b.columns[name]
		data := columnData[name]
		statsBytes := columnStatsBytes[name]

		if err := binary.Write(buf, binary.LittleEndian, uint16(len(name))); err != nil {
			return nil, err
		}
		if _, err := buf.Write([]byte(name)); err != nil {
			return nil, err
		}
		if err := buf.WriteByte(byte(cb.typ)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(dataOffset)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(data))); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(statsOffset)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(statsBytes))); err != nil {
			return nil, err
		}

		// Record column offset for selective I/O
		columnIndex = append(columnIndex, ColumnIndexEntry{
			Name:   name,
			Offset: uint32(dataOffset),
			Length: uint32(len(data)),
		})

		dataOffset += len(data)
		statsOffset += len(statsBytes)
	}

	// Write trace column metadata (dataLen=0 signals trace-level, dataOffset unused)
	for _, name := range traceColumnNames {
		statsBytes := columnStatsBytes[name]

		// Get type from first trace that has this column
		var typ ColumnType
		for _, trace := range b.traces {
			if cb, ok := trace.resourceAttrs[name]; ok {
				typ = cb.typ
				break
			}
		}

		if err := binary.Write(buf, binary.LittleEndian, uint16(len(name))); err != nil {
			return nil, err
		}
		if _, err := buf.Write([]byte(name)); err != nil {
			return nil, err
		}
		if err := buf.WriteByte(byte(typ)); err != nil {
			return nil, err
		}
		// dataOffset=0, dataLen=0 signals this is a trace-level column
		if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(statsOffset)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(statsBytes))); err != nil {
			return nil, err
		}
		statsOffset += len(statsBytes)
	}

	return columnIndex, nil
}

// buildBlockPayload serializes a block to bytes. The encoder is passed through to column builders
// for dictionary compression, using a per-Writer encoder instead of sync.Pool to prevent memory accumulation.
func buildBlockPayload(b *blockBuilder, version uint8, encoder *zstd.Encoder) ([]byte, blockIndexEntry, error) {
	// Add trace.index column to map spans to traces - only if we have traces
	if len(b.traces) > 0 {
		traceIndexCol := newColumnBuilder("trace.index", ColumnTypeUint64, b.spanCount)
		for spanIdx, traceIdx := range b.spanToTrace {
			if err := traceIndexCol.setUint64(spanIdx, uint64(traceIdx)); err != nil {
				return nil, blockIndexEntry{}, err
			}
		}
		b.columns["trace.index"] = traceIndexCol
	}

	// Build span-level column data
	names := make([]string, 0, len(b.columns))
	for name := range b.columns {
		names = append(names, name)
	}
	sort.Strings(names)

	columnData := make(map[string][]byte, len(names))
	columnStats := make(map[string]columnStatsBuilder, len(names))
	columnStatsBytes := make(map[string][]byte, len(names))
	totalStatsLen := 0
	for _, name := range names {
		data, stats, err := b.columns[name].buildData(b.spanCount, encoder)
		if err != nil {
			return nil, blockIndexEntry{}, err
		}
		columnData[name] = data
		columnStats[name] = stats
		encodedStats := encodeColumnStats(b.columns[name].typ, stats)
		totalStatsLen += len(encodedStats)
		columnStatsBytes[name] = encodedStats
	}

	// Build trace table and get trace-level column stats with per-Writer encoder
	traceTableData, traceTableStats, err := buildTraceTable(b.traces, encoder)
	if err != nil {
		return nil, blockIndexEntry{}, err
	}

	// Add trace-level column stats to metadata (stats only, no data - dataLen=0 signals trace-level)
	traceColumnNames := make([]string, 0, len(traceTableStats))
	for name := range traceTableStats {
		traceColumnNames = append(traceColumnNames, name)
	}
	sort.Strings(traceColumnNames)

	for _, name := range traceColumnNames {
		stats := traceTableStats[name]
		// Get type from first trace that has this column
		var typ ColumnType
		for _, trace := range b.traces {
			if cb, ok := trace.resourceAttrs[name]; ok {
				typ = cb.typ
				break
			}
		}
		encodedStats := encodeColumnStats(typ, stats)
		totalStatsLen += len(encodedStats)
		columnStatsBytes[name] = encodedStats
	}

	// Total column count = span columns + trace columns
	totalColumnCount := len(names) + len(traceColumnNames)

	// Metadata length: all columns (span + trace)
	metaLen := 0
	for _, name := range names {
		metaLen += 2 + len(name) + 1 + 8 + 8 + 8 + 8 // nameLen + name + type + data offset/len + stats offset/len
	}
	for _, name := range traceColumnNames {
		metaLen += 2 + len(name) + 1 + 8 + 8 + 8 + 8 // same for trace columns
	}

	// Block header is 24 bytes
	headerLen := 24
	statsStart := headerLen + metaLen
	dataStart := statsStart + totalStatsLen

	var buf bytes.Buffer

	columnIndex, err := writeBlockHeader(&buf, b, totalColumnCount, traceTableData, names, traceColumnNames, columnData, columnStatsBytes, statsStart, dataStart, version)
	if err != nil {
		return nil, blockIndexEntry{}, err
	}

	// Column stats: span columns first, then trace columns
	for _, name := range names {
		if _, err := buf.Write(columnStatsBytes[name]); err != nil {
			return nil, blockIndexEntry{}, err
		}
	}
	for _, name := range traceColumnNames {
		if _, err := buf.Write(columnStatsBytes[name]); err != nil {
			return nil, blockIndexEntry{}, err
		}
	}

	// Span column data
	for _, name := range names {
		data := columnData[name]
		if _, err := buf.Write(data); err != nil {
			return nil, blockIndexEntry{}, err
		}
	}

	// Trace table data
	if len(traceTableData) > 0 {
		if _, err := buf.Write(traceTableData); err != nil {
			return nil, blockIndexEntry{}, err
		}
	}

	meta := blockIndexEntry{
		SpanCount:       uint32(b.spanCount),
		MinStart:        b.minStart,
		MaxStart:        b.maxStart,
		ColumnNameBloom: b.columnBloom,
		ColumnIndex:     columnIndex,  // Per-column offsets for selective I/O
		ValueStats:      b.valueStats, // v10: Per-attribute value statistics
		Kind:            blockEntryKindLeaf,
	}
	meta.MinTraceID = b.minTrace
	meta.MaxTraceID = b.maxTrace

	return buf.Bytes(), meta, nil
}

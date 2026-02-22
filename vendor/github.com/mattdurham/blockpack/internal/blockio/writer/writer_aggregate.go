//nolint:goconst // Aggregate function names kept inline for clarity
package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/mattdurham/blockpack/internal/vm"
)

// SerializableAgg is a compact representation of vm.AggBucket for serialization.
// Uses pointer fields to store ONLY the aggregate values required by the specific query,
// dramatically reducing size. For example:
// - COUNT query: only GroupKey + Count (~24 bytes)
// - AVG query: only GroupKey + Sum + Count (~40 bytes)
// - QUANTILE query: only GroupKey + specific quantile (~32 bytes)
// This provides 150-250x size reduction compared to storing all fields.
type SerializableAgg struct {
	Count    *int64        `json:"c,omitempty"`   // Count of values
	Sum      *float64      `json:"s,omitempty"`   // Sum of values
	Min      *float64      `json:"m,omitempty"`   // Minimum value
	Max      *float64      `json:"x,omitempty"`   // Maximum value
	P50      *float64      `json:"p50,omitempty"` // 50th percentile (median)
	P95      *float64      `json:"p95,omitempty"` // 95th percentile
	P99      *float64      `json:"p99,omitempty"` // 99th percentile
	GroupKey []interface{} `json:"k"`             // Group-by key values
}

// GroupKeyIndex maps group key string to index for deduplication in binary format.
// This enables storing group keys once and referencing by index in each bucket.
type GroupKeyIndex struct {
	Index map[string]int // Map from key string to index
	Keys  []vm.GroupKey  // Unique group keys in order
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
			return fmt.Errorf("write value type for nil: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(0)); err != nil {
			return fmt.Errorf("write value length for nil: %w", err)
		}
	case string:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeString); err != nil {
			return fmt.Errorf("write value type for string: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(v))); err != nil { //nolint:gosec
			return fmt.Errorf("write value length for string: %w", err)
		}
		if _, err := buf.Write([]byte(v)); err != nil {
			return fmt.Errorf("write string value: %w", err)
		}
	case int64:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeInt64); err != nil {
			return fmt.Errorf("write value type for int64: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return fmt.Errorf("write int64 value: %w", err)
		}
	case float64:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeFloat64); err != nil {
			return fmt.Errorf("write value type for float64: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return fmt.Errorf("write float64 value: %w", err)
		}
	case bool:
		if err := binary.Write(buf, binary.LittleEndian, ValueTypeBool); err != nil {
			return fmt.Errorf("write value type for bool: %w", err)
		}
		var boolByte uint8
		if v {
			boolByte = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, boolByte); err != nil {
			return fmt.Errorf("write bool value: %w", err)
		}
	default:
		return fmt.Errorf("unsupported group key value type: %T", val.Data)
	}
	return nil
}

// writeGroupKey writes a group key to the buffer in binary format.
func writeGroupKey(buf *bytes.Buffer, gk vm.GroupKey) error {
	// Write field count
	if err := binary.Write(buf, binary.LittleEndian, uint8(len(gk.Values))); err != nil { //nolint:gosec
		return fmt.Errorf("write group key field count: %w", err)
	}

	// Write each field value
	for i, val := range gk.Values {
		if err := writeGroupKeyValue(buf, val); err != nil {
			return fmt.Errorf("write group key field %d: %w", i, err)
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
func writeBinaryAggregates(
	buf *bytes.Buffer,
	buckets []*TimeBucket,
	groupKeyIdx *GroupKeyIndex,
	aggFunc string,
	field string,
	quantile float64,
	stepSizeNanos int64,
) error {
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
		return fmt.Errorf("write bucket start time: %w", err)
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
		return fmt.Errorf("write step size millis: %w", err)
	}

	// Write group key count
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(groupKeyIdx.Keys))); err != nil { //nolint:gosec
		return fmt.Errorf("write group key count: %w", err)
	}

	// Write each unique group key
	for i, gk := range groupKeyIdx.Keys {
		if err := writeGroupKey(buf, gk); err != nil {
			return fmt.Errorf("write group key %d: %w", i, err)
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
					return fmt.Errorf("write zero value count for group %d: %w", i, err)
				}
				continue
			}

			// Write value count
			if err := binary.Write(buf, binary.LittleEndian, uint8(len(values))); err != nil { //nolint:gosec
				return fmt.Errorf("write value count for group %d: %w", i, err)
			}

			// Write each value
			for j, val := range values {
				if err := binary.Write(buf, binary.LittleEndian, val.Type); err != nil {
					return fmt.Errorf("write aggregate type for group %d value %d: %w", i, j, err)
				}
				if err := binary.Write(buf, binary.LittleEndian, val.Value); err != nil {
					return fmt.Errorf("write aggregate value for group %d value %d: %w", i, j, err)
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
						if existingHist, ok := existingAgg.Histograms[field]; ok &&
							len(existingHist.Counts) == len(hist.Counts) {
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

// BuildMetricStreamBlocks serializes all metric stream aggregates to bytes with per-stream zstd compression.
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
//
// BuildMetricStreamBlocks serializes all metric stream data into a byte slice.
func (w *Writer) BuildMetricStreamBlocks() (result []byte, err error) {
	if w == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	var buf bytes.Buffer

	// Write stream count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(w.metricStreams))); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write metric stream count: %w", err)
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
		if err := binary.Write(&buf, binary.LittleEndian, uint16(len(streamID))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write stream ID length for %s: %w", streamID, err)
		}
		if _, err := buf.Write([]byte(streamID)); err != nil {
			return nil, fmt.Errorf("write stream ID %s: %w", streamID, err)
		}

		// Build the stream data into a temporary buffer
		var streamDataBuf bytes.Buffer

		// Write query
		query := stream.Def.Query
		if err := binary.Write(&streamDataBuf, binary.LittleEndian, uint32(len(query))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write query length for stream %s: %w", streamID, err)
		}
		if _, err := streamDataBuf.Write([]byte(query)); err != nil {
			return nil, fmt.Errorf("write query for stream %s: %w", streamID, err)
		}

		// Write step size
		if err := binary.Write(&streamDataBuf, binary.LittleEndian, stream.Def.StepSizeNanos); err != nil {
			return nil, fmt.Errorf("write step size for stream %s: %w", streamID, err)
		}

		// Write bucket count
		if err := binary.Write(&streamDataBuf, binary.LittleEndian, uint32(len(stream.CompletedBuckets))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write bucket count for stream %s: %w", streamID, err)
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
		uncompressedLen := uint32(len(uncompressedData)) //nolint:gosec

		// Compress the stream data with zstd using per-Writer encoder
		w.zstdEncoder.Reset(nil)
		compressedData := w.zstdEncoder.EncodeAll(uncompressedData, make([]byte, 0, len(uncompressedData)))
		compressedLen := uint32(len(compressedData)) //nolint:gosec

		// Write compression flag (1 = zstd)
		if err := buf.WriteByte(CompressionZstd); err != nil {
			return nil, fmt.Errorf("write compression flag for stream %s: %w", streamID, err)
		}

		// Write uncompressed length
		if err := binary.Write(&buf, binary.LittleEndian, uncompressedLen); err != nil {
			return nil, fmt.Errorf("write uncompressed length for stream %s: %w", streamID, err)
		}

		// Write compressed length
		if err := binary.Write(&buf, binary.LittleEndian, compressedLen); err != nil {
			return nil, fmt.Errorf("write compressed length for stream %s: %w", streamID, err)
		}

		// Write compressed data
		if _, err := buf.Write(compressedData); err != nil {
			return nil, fmt.Errorf("write compressed data for stream %s: %w", streamID, err)
		}
	}

	return buf.Bytes(), nil
}

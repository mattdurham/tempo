package blockio

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mattdurham/blockpack/internal/vm"
)

// AggregateStreamData contains parsed metric stream data
type AggregateStreamData struct {
	Query         string
	StepSizeNanos int64
	StartTime     int64
	StepMillis    uint32
	GroupKeys     []vm.GroupKey
	Buckets       []AggregateTimeBucket
}

// AggregateTimeBucket represents aggregates for a single time bucket
type AggregateTimeBucket struct {
	Time   int64              // Bucket timestamp (nanoseconds)
	Values [][]BinaryAggValue // Indexed by group key index
}

// ParseAggregateStream parses decompressed metric stream data.
// The data should already be decompressed by the reader.
//
// Binary format:
// - query_len (uint32)
// - query (bytes)
// - step_size_nanos (int64)
// - bucket_count (uint32)
// - start_time (int64)
// - step_millis (uint32)
// - group_key_count (uint32)
// - For each group key:
//   - field_count (uint8)
//   - For each field: value_type + value
//
// - For each bucket:
//   - For each group key: value_count + values
//
// BOT: Why is this only used by tests, shouldnt we go through the production reader code to get this data in real usage?
func ParseAggregateStream(data []byte) (*AggregateStreamData, error) {
	buf := bytes.NewReader(data)
	result := &AggregateStreamData{}

	// Read query string
	var queryLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &queryLen); err != nil {
		return nil, fmt.Errorf("read query length: %w", err)
	}
	queryBytes := make([]byte, queryLen)
	if _, err := buf.Read(queryBytes); err != nil {
		return nil, fmt.Errorf("read query: %w", err)
	}
	result.Query = string(queryBytes)

	// Read step size
	if err := binary.Read(buf, binary.LittleEndian, &result.StepSizeNanos); err != nil {
		return nil, fmt.Errorf("read step size: %w", err)
	}

	// Read bucket count
	var bucketCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &bucketCount); err != nil {
		return nil, fmt.Errorf("read bucket count: %w", err)
	}

	// Read start time
	if err := binary.Read(buf, binary.LittleEndian, &result.StartTime); err != nil {
		return nil, fmt.Errorf("read start time: %w", err)
	}

	// Read step millis
	if err := binary.Read(buf, binary.LittleEndian, &result.StepMillis); err != nil {
		return nil, fmt.Errorf("read step millis: %w", err)
	}

	// Read group key count
	var groupKeyCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &groupKeyCount); err != nil {
		return nil, fmt.Errorf("read group key count: %w", err)
	}

	// Read each group key
	result.GroupKeys = make([]vm.GroupKey, groupKeyCount)
	for i := uint32(0); i < groupKeyCount; i++ {
		gk, err := readGroupKey(buf)
		if err != nil {
			return nil, fmt.Errorf("read group key %d: %w", i, err)
		}
		result.GroupKeys[i] = gk
	}

	// Read each bucket
	result.Buckets = make([]AggregateTimeBucket, bucketCount)
	for i := uint32(0); i < bucketCount; i++ {
		bucketTime := result.StartTime + int64(i)*int64(result.StepMillis)*1_000_000

		bucket := AggregateTimeBucket{
			Time:   bucketTime,
			Values: make([][]BinaryAggValue, groupKeyCount),
		}

		// Read values for each group key
		for j := uint32(0); j < groupKeyCount; j++ {
			values, err := readAggregateValues(buf)
			if err != nil {
				return nil, fmt.Errorf("read values for bucket %d, group %d: %w", i, j, err)
			}
			bucket.Values[j] = values
		}

		result.Buckets[i] = bucket
	}

	return result, nil
}

// readGroupKey reads a group key from the buffer
func readGroupKey(buf *bytes.Reader) (vm.GroupKey, error) {
	// Read field count
	var fieldCount uint8
	if err := binary.Read(buf, binary.LittleEndian, &fieldCount); err != nil {
		return vm.GroupKey{}, fmt.Errorf("read field count: %w", err)
	}

	// Read each field value
	values := make([]vm.Value, fieldCount)
	for i := uint8(0); i < fieldCount; i++ {
		val, err := readGroupKeyValue(buf)
		if err != nil {
			return vm.GroupKey{}, fmt.Errorf("read field %d: %w", i, err)
		}
		values[i] = val
	}

	return vm.GroupKey{Values: values}, nil
}

// readGroupKeyValue reads a single group key value
func readGroupKeyValue(buf *bytes.Reader) (vm.Value, error) {
	// Read value type
	var valueType uint8
	if err := binary.Read(buf, binary.LittleEndian, &valueType); err != nil {
		return vm.Value{}, fmt.Errorf("read value type: %w", err)
	}

	switch valueType {
	case ValueTypeString:
		var strLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
			return vm.Value{}, fmt.Errorf("read string length: %w", err)
		}
		strBytes := make([]byte, strLen)
		if _, err := buf.Read(strBytes); err != nil {
			return vm.Value{}, fmt.Errorf("read string: %w", err)
		}
		return vm.Value{Data: string(strBytes)}, nil

	case ValueTypeInt64:
		var val int64
		if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
			return vm.Value{}, fmt.Errorf("read int64: %w", err)
		}
		return vm.Value{Data: val}, nil

	case ValueTypeFloat64:
		var val float64
		if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
			return vm.Value{}, fmt.Errorf("read float64: %w", err)
		}
		return vm.Value{Data: val}, nil

	case ValueTypeBool:
		var boolByte uint8
		if err := binary.Read(buf, binary.LittleEndian, &boolByte); err != nil {
			return vm.Value{}, fmt.Errorf("read bool: %w", err)
		}
		return vm.Value{Data: boolByte == 1}, nil

	default:
		return vm.Value{}, fmt.Errorf("unsupported value type: %d", valueType)
	}
}

// readAggregateValues reads aggregate values for one group key in one bucket
func readAggregateValues(buf *bytes.Reader) ([]BinaryAggValue, error) {
	// Read value count
	var valueCount uint8
	if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
		return nil, fmt.Errorf("read value count: %w", err)
	}

	if valueCount == 0 {
		return nil, nil // No data for this group key in this bucket
	}

	values := make([]BinaryAggValue, valueCount)
	for i := uint8(0); i < valueCount; i++ {
		var aggType uint8
		if err := binary.Read(buf, binary.LittleEndian, &aggType); err != nil {
			return nil, fmt.Errorf("read agg type: %w", err)
		}

		var value float64
		if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
			return nil, fmt.Errorf("read value: %w", err)
		}

		values[i] = BinaryAggValue{
			Type:  aggType,
			Value: value,
		}
	}

	return values, nil
}

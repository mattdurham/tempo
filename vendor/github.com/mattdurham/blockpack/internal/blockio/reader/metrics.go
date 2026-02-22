package reader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/klauspost/compress/zstd"
)

// parseMetricStreamHeadersFromRaw parses the metric stream block headers from raw bytes.
// Returns a map of streamID -> MetricStreamInfo, or nil if rawData is empty.
func parseMetricStreamHeadersFromRaw(rawData []byte) (map[string]*MetricStreamInfo, error) {
	if len(rawData) == 0 {
		return nil, nil
	}

	buf := bytes.NewReader(rawData)
	streams := make(map[string]*MetricStreamInfo)

	// Read stream count
	var streamCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &streamCount); err != nil {
		return nil, fmt.Errorf("failed to read stream count: %w", err)
	}

	const maxMetricStreamCount = 10000
	if streamCount > maxMetricStreamCount {
		return nil, fmt.Errorf("stream count %d exceeds maximum %d", streamCount, maxMetricStreamCount)
	}

	// Parse each stream header
	for i := uint32(0); i < streamCount; i++ {
		offset := int64(len(rawData)) - int64(buf.Len())

		// Read stream ID
		var streamIDLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &streamIDLen); err != nil {
			return nil, fmt.Errorf("failed to read stream ID length: %w", err)
		}

		const maxStreamIDLen = 1024
		if streamIDLen > maxStreamIDLen {
			return nil, fmt.Errorf("stream %d: ID length %d exceeds maximum %d", i, streamIDLen, maxStreamIDLen)
		}

		streamIDBytes := make([]byte, streamIDLen)
		if _, err := io.ReadFull(buf, streamIDBytes); err != nil {
			return nil, fmt.Errorf("failed to read stream ID: %w", err)
		}
		streamID := string(streamIDBytes)

		// Read compression flag
		var compressionFlag uint8
		if err := binary.Read(buf, binary.LittleEndian, &compressionFlag); err != nil {
			return nil, fmt.Errorf("failed to read compression flag: %w", err)
		}

		// Read uncompressed length
		var uncompressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &uncompressedLen); err != nil {
			return nil, fmt.Errorf("failed to read uncompressed length: %w", err)
		}

		// Read compressed length
		var compressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
			return nil, fmt.Errorf("failed to read compressed length: %w", err)
		}

		const maxCompressedLen = 100 * 1024 * 1024 // 100MB
		if compressedLen > maxCompressedLen {
			return nil, fmt.Errorf("stream %d: compressed length %d exceeds maximum %d", i, compressedLen, maxCompressedLen)
		}

		compressedOffset := int64(len(rawData)) - int64(buf.Len())

		streams[streamID] = &MetricStreamInfo{
			StreamID:         streamID,
			Offset:           offset,
			CompressionFlag:  compressionFlag,
			UncompressedLen:  uncompressedLen,
			CompressedLen:    compressedLen,
			CompressedOffset: compressedOffset,
		}

		// Skip compressed data
		if int64(compressedLen) > int64(buf.Len()) { //nolint:gosec
			return nil, fmt.Errorf("stream %d: compressed length %d exceeds remaining buffer %d",
				i, compressedLen, buf.Len())
		}
		if _, err := buf.Seek(int64(compressedLen), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("failed to skip compressed data: %w", err)
		}
	}

	return streams, nil
}

// ReadMetricStreamBlocks retrieves the offset and length of metric stream blocks.
// Uses values cached during Reader construction to avoid redundant IO.
func (r *Reader) ReadMetricStreamBlocks() (offset uint64, length uint64, err error) {
	// metricStreamOffset and metricStreamLen are cached from the header during NewReaderWithCache.
	// The constructor already validates the version (v10/v11 only), so if we have a Reader,
	// the version check has already passed.
	return r.metricStreamOffset, r.metricStreamLen, nil
}

// GetColumnSizes returns the compressed size in bytes for each column across all blocks.
// Returns a map of column name to total compressed size.
func (r *Reader) GetColumnSizes() map[string]int64 {
	columnSizes := make(map[string]int64)

	for _, entry := range r.blockEntries {
		for _, colEntry := range entry.ColumnIndex {
			columnSizes[colEntry.Name] += int64(colEntry.Length)
		}
	}

	return columnSizes
}

// GetMetricStreamBlocksRaw returns the raw bytes of metric stream blocks.
// This is a temporary helper until full deserialization is implemented.
func (r *Reader) GetMetricStreamBlocksRaw() ([]byte, error) {
	offset, length, err := r.ReadMetricStreamBlocks()
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	}

	if r.metricStreamLen > math.MaxInt { //nolint:gosec
		return nil, fmt.Errorf("metric stream length %d exceeds maximum", r.metricStreamLen)
	}

	return r.readProviderRange(int64(offset), int(length), DataTypeUnknown) //nolint:gosec
}

// MetricStreamInfo contains metadata about a compressed metric stream
type MetricStreamInfo struct {
	StreamID         string
	Offset           int64
	CompressionFlag  uint8
	UncompressedLen  uint32
	CompressedLen    uint32
	CompressedOffset int64
}

// ReadMetricStreamHeaders parses metric stream block headers to find all streams
// without decompressing the data. Returns a map of streamID -> stream info.
func (r *Reader) ReadMetricStreamHeaders() (map[string]*MetricStreamInfo, error) {
	rawData, err := r.GetMetricStreamBlocksRaw()
	if err != nil {
		return nil, err
	}
	return parseMetricStreamHeadersFromRaw(rawData)
}

// MetricStreamData represents a fully deserialized metric stream with all time buckets and aggregates.
type MetricStreamData struct {
	StreamID      string
	Query         string
	GroupKeys     [][]MetricStreamValue // unique group key combinations
	Buckets       []MetricStreamBucket  // time-ordered buckets
	StepSizeNanos int64
	StartTime     int64 // timestamp of first bucket
	StepMillis    uint32
}

// MetricStreamBucket represents aggregates for a single time bucket.
type MetricStreamBucket struct {
	Groups     [][]MetricStreamAggregateValue // group_index -> aggregate values
	BucketTime int64                          // implicit: start_time + bucket_index * step_millis * 1_000_000
}

// MetricStreamValue represents a single value in a group key.
type MetricStreamValue struct {
	Value any   // actual value
	Type  uint8 // 0=String, 1=Int64, 2=Float64, 3=Bool
}

// MetricStreamAggregateValue represents a single aggregate value (Count, Sum, Min, Max, P50, P95, P99).
type MetricStreamAggregateValue struct {
	Type  uint8 // 1=Count, 2=Sum, 3=Min, 4=Max, 5=P50, 6=P95, 7=P99
	Value float64
}

// ReadMetricStream reads and decompresses a specific metric stream by ID.
// Returns the uncompressed stream data (query, step_size, buckets, etc.).
func (r *Reader) ReadMetricStream(streamID string) ([]byte, error) {
	// Get raw metric stream blocks once and parse headers from it directly,
	// avoiding a second ReadAt call that would otherwise occur if we called
	// ReadMetricStreamHeaders() followed by GetMetricStreamBlocksRaw() separately.
	rawData, err := r.GetMetricStreamBlocksRaw()
	if err != nil {
		return nil, err
	}
	if rawData == nil {
		return nil, fmt.Errorf("metric stream blocks data is nil")
	}

	streams, err := parseMetricStreamHeadersFromRaw(rawData)
	if err != nil {
		return nil, err
	}

	streamInfo, ok := streams[streamID]
	if !ok {
		return nil, fmt.Errorf("stream %s not found", streamID)
	}

	// Extract compressed data for this stream
	compressedData := rawData[streamInfo.CompressedOffset : streamInfo.CompressedOffset+int64(streamInfo.CompressedLen)]

	// Decompress based on compression flag
	switch streamInfo.CompressionFlag {
	case 0: // CompressionNone
		return compressedData, nil

	case 1: // CompressionZstd
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		defer decoder.Close()

		uncompressedData, err := decoder.DecodeAll(compressedData, make([]byte, 0, streamInfo.UncompressedLen))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress stream data: %w", err)
		}

		// Validate uncompressed size
		if uint32(len(uncompressedData)) != streamInfo.UncompressedLen { //nolint:gosec
			return nil, fmt.Errorf("uncompressed size mismatch: expected %d, got %d",
				streamInfo.UncompressedLen, len(uncompressedData))
		}

		return uncompressedData, nil

	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", streamInfo.CompressionFlag)
	}
}

// ReadMetricStreamData reads and fully deserializes a metric stream by ID.
// Returns structured data with all time buckets, group keys, and aggregate values.
func (r *Reader) ReadMetricStreamData(streamID string) (*MetricStreamData, error) {
	// Get uncompressed stream data
	data, err := r.ReadMetricStream(streamID)
	if err != nil {
		return nil, err
	}

	// Parse the binary data
	return parseMetricStreamData(streamID, data)
}

// ReadAllMetricStreams reads and deserializes all metric streams in the blockpack.
// Returns a map of streamID -> MetricStreamData.
func (r *Reader) ReadAllMetricStreams() (map[string]*MetricStreamData, error) {
	// Get stream headers
	streams, err := r.ReadMetricStreamHeaders()
	if err != nil {
		return nil, err
	}

	if len(streams) == 0 {
		return nil, nil
	}

	result := make(map[string]*MetricStreamData, len(streams))
	for streamID := range streams {
		data, err := r.ReadMetricStreamData(streamID)
		if err != nil {
			return nil, fmt.Errorf("failed to read stream %s: %w", streamID, err)
		}
		result[streamID] = data
	}

	return result, nil
}

// parseMetricStreamData parses the binary format of a decompressed metric stream.
func parseMetricStreamData(streamID string, data []byte) (*MetricStreamData, error) {
	buf := bytes.NewReader(data)

	result := &MetricStreamData{
		StreamID: streamID,
	}

	// Read query
	var queryLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &queryLen); err != nil {
		return nil, fmt.Errorf("failed to read query length: %w", err)
	}
	queryBytes := make([]byte, queryLen)
	if _, err := io.ReadFull(buf, queryBytes); err != nil {
		return nil, fmt.Errorf("failed to read query: %w", err)
	}
	result.Query = string(queryBytes)

	// Read step size
	if err := binary.Read(buf, binary.LittleEndian, &result.StepSizeNanos); err != nil {
		return nil, fmt.Errorf("failed to read step size: %w", err)
	}

	// Read bucket count
	var bucketCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &bucketCount); err != nil {
		return nil, fmt.Errorf("failed to read bucket count: %w", err)
	}

	// Read start time
	if err := binary.Read(buf, binary.LittleEndian, &result.StartTime); err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}

	// Read step millis
	if err := binary.Read(buf, binary.LittleEndian, &result.StepMillis); err != nil {
		return nil, fmt.Errorf("failed to read step millis: %w", err)
	}

	// Read group key count
	var groupKeyCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &groupKeyCount); err != nil {
		return nil, fmt.Errorf("failed to read group key count: %w", err)
	}

	// Read group keys
	result.GroupKeys = make([][]MetricStreamValue, groupKeyCount)
	for i := uint32(0); i < groupKeyCount; i++ {
		// Read field count
		var fieldCount uint8
		if err := binary.Read(buf, binary.LittleEndian, &fieldCount); err != nil {
			return nil, fmt.Errorf("failed to read field count for group key %d: %w", i, err)
		}

		// Read fields
		fields := make([]MetricStreamValue, fieldCount)
		for j := uint8(0); j < fieldCount; j++ {
			value, err := readMetricStreamValue(buf)
			if err != nil {
				return nil, fmt.Errorf("failed to read field %d of group key %d: %w", j, i, err)
			}
			fields[j] = value
		}
		result.GroupKeys[i] = fields
	}

	// Read buckets
	result.Buckets = make([]MetricStreamBucket, bucketCount)
	for i := uint32(0); i < bucketCount; i++ {
		bucket := MetricStreamBucket{
			BucketTime: result.StartTime + int64(i)*int64(result.StepMillis)*1_000_000,
			Groups:     make([][]MetricStreamAggregateValue, groupKeyCount),
		}

		// Read aggregate values for each group
		for j := uint32(0); j < groupKeyCount; j++ {
			// Read value count
			var valueCount uint8
			if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
				return nil, fmt.Errorf("failed to read value count for bucket %d group %d: %w", i, j, err)
			}

			// Read values
			values := make([]MetricStreamAggregateValue, valueCount)
			for k := uint8(0); k < valueCount; k++ {
				var aggType uint8
				if err := binary.Read(buf, binary.LittleEndian, &aggType); err != nil {
					return nil, fmt.Errorf("failed to read agg type for bucket %d group %d value %d: %w", i, j, k, err)
				}

				var aggValue float64
				if err := binary.Read(buf, binary.LittleEndian, &aggValue); err != nil {
					return nil, fmt.Errorf("failed to read agg value for bucket %d group %d value %d: %w", i, j, k, err)
				}

				values[k] = MetricStreamAggregateValue{
					Type:  aggType,
					Value: aggValue,
				}
			}
			bucket.Groups[j] = values
		}

		result.Buckets[i] = bucket
	}

	return result, nil
}

// ParseMetricStreamLocation extracts the metric stream offset and length from a raw blockpack header.
// The header must be at least 37 bytes (the standard v10/v11 header size).
// Layout: magic(4) + version(1) + metadataOffset(8) + metadataLen(8) + metricStreamOffset(8) + metricStreamLen(8)
func ParseMetricStreamLocation(headerBytes []byte) (offset, length uint64, err error) {
	if len(headerBytes) < 37 {
		return 0, 0, fmt.Errorf("header too short: %d bytes (need 37)", len(headerBytes))
	}
	offset = binary.LittleEndian.Uint64(headerBytes[21:29])
	length = binary.LittleEndian.Uint64(headerBytes[29:37])
	return offset, length, nil
}

// readMetricStreamValue reads a single value from the binary stream.
func readMetricStreamValue(buf *bytes.Reader) (MetricStreamValue, error) {
	var valueType uint8
	if err := binary.Read(buf, binary.LittleEndian, &valueType); err != nil {
		return MetricStreamValue{}, fmt.Errorf("failed to read value type: %w", err)
	}

	var value any
	switch valueType {
	case 0: // String
		var strLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &strLen); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read string length: %w", err)
		}
		strBytes := make([]byte, strLen)
		if _, err := io.ReadFull(buf, strBytes); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read string: %w", err)
		}
		value = string(strBytes)

	case 1: // Int64
		var i64 int64
		if err := binary.Read(buf, binary.LittleEndian, &i64); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read int64: %w", err)
		}
		value = i64

	case 2: // Float64
		var f64 float64
		if err := binary.Read(buf, binary.LittleEndian, &f64); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read float64: %w", err)
		}
		value = f64

	case 3: // Bool
		var b uint8
		if err := binary.Read(buf, binary.LittleEndian, &b); err != nil {
			return MetricStreamValue{}, fmt.Errorf("failed to read bool: %w", err)
		}
		value = b != 0

	default:
		return MetricStreamValue{}, fmt.Errorf("unsupported value type: %d", valueType)
	}

	return MetricStreamValue{
		Type:  valueType,
		Value: value,
	}, nil
}

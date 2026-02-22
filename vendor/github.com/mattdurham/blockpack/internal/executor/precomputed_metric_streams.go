package executor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/sql"
	"github.com/mattdurham/blockpack/internal/traceqlparser"
	"github.com/mattdurham/blockpack/internal/vm"
)

// zstdDecoderPool pools zstd decoders to avoid the overhead of creating a new
// decoder for every stream. Decoders are stateless between DecodeAll calls and
// safe to reuse after Put.
var zstdDecoderPool = sync.Pool{
	New: func() any {
		d, err := zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("zstd.NewReader failed: %v", err))
		}
		return d
	},
}

const (
	preAggTypeCount uint8 = 1
	preAggTypeSum   uint8 = 2
	preAggTypeMin   uint8 = 3
	preAggTypeMax   uint8 = 4
	preAggTypeP50   uint8 = 5
	preAggTypeP95   uint8 = 6
	preAggTypeP99   uint8 = 7
)

const (
	preValueTypeString  uint8 = 0
	preValueTypeInt64   uint8 = 1
	preValueTypeFloat64 uint8 = 2
	preValueTypeBool    uint8 = 3
)

const (
	preCompressionNone uint8 = 0
	preCompressionZstd uint8 = 1
)

const (
	maxStreamIDLen     = 1024              // 1KB
	maxGroupKeyLen     = 4096              // 4KB
	maxCompressedLen   = 100 * 1024 * 1024 // 100MB
	maxUncompressedLen = 500 * 1024 * 1024 // 500MB
	maxQueryLen        = 1024 * 1024       // 1MB
	maxGroupKeyCount   = 10000             // Maximum number of group keys
	maxBucketCount     = 1000000           // Maximum number of buckets
	maxFieldCount      = 255               // Maximum fields in a group key (uint8)
)

const (
	blockpackMagicNumber = uint32(0xC011FEA1) // matches reader.MagicNumber
	blockpackVersionV10  = uint8(10)          // matches reader.VersionV10
	blockpackVersionV11  = uint8(11)          // matches reader.VersionV11
)

var errPrecomputedUnsupported = errors.New("precomputed metric streams unsupported")

type precomputedMetricStreamData struct {
	values        map[int64]map[int]map[uint8]float64
	query         string
	groupKeys     [][]string
	stepSizeNanos int64
	startTime     int64
	bucketCount   uint32
	stepMillis    uint32
}

func (e *BlockpackExecutor) tryInlinePrecomputedMetricStreams(
	path string,
	querySpec *sql.QuerySpec,
	opts QueryOptions,
) (*BlockpackResult, bool, error) {
	if querySpec == nil {
		return nil, false, nil
	}
	if !precomputedFilterSupported(querySpec) {
		return nil, false, nil
	}

	providerStorage, ok := e.storage.(ProviderStorage)
	if !ok {
		return nil, false, fmt.Errorf("storage must implement ProviderStorage interface")
	}

	provider, err := providerStorage.GetProvider(path)
	if err != nil {
		return nil, false, fmt.Errorf("read blockpack file: %w", err)
	}

	// Close provider after query execution to prevent file descriptor leaks
	defer func() {
		if closeable, ok := provider.(blockpackio.CloseableReaderProvider); ok {
			if err := closeable.Close(); err != nil { //nolint:govet
				// Log but don't fail - we already have the data
				_ = err
			}
		}
	}()

	var tracking *blockpackio.TrackingReaderProvider
	if opts.IOLatency > 0 {
		var err error //nolint:govet
		tracking, err = blockpackio.NewTrackingReaderProviderWithLatency(provider, opts.IOLatency)
		if err != nil {
			return nil, false, err
		}
	} else {
		var err error //nolint:govet
		tracking, err = blockpackio.NewTrackingReaderProvider(provider)
		if err != nil {
			return nil, false, err
		}
	}

	rawData, err := readMetricStreamSectionDirect(tracking)
	if err != nil {
		return nil, false, err
	}
	if len(rawData) == 0 {
		return nil, false, nil
	}

	result, matched, err := findMatchingPrecomputedMetricStreamResult(rawData, querySpec, opts)
	if err != nil {
		if errors.Is(err, errPrecomputedUnsupported) {
			return nil, false, nil
		}
		return nil, true, err
	}
	if !matched {
		return nil, false, nil
	}
	if result == nil {
		return nil, false, nil
	}

	result.BytesRead = tracking.BytesRead()
	result.IOOperations = tracking.IOOperations()
	return result, true, nil
}

func findMatchingPrecomputedMetricStreamResult(
	rawData []byte,
	querySpec *sql.QuerySpec,
	opts QueryOptions,
) (*BlockpackResult, bool, error) {
	if querySpec == nil {
		return nil, false, fmt.Errorf("querySpec cannot be nil")
	}

	normalized := *querySpec
	normalized.Normalize()

	buf := bytes.NewReader(rawData)

	var streamCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &streamCount); err != nil {
		return nil, false, fmt.Errorf("read stream count: %w", err)
	}

	for i := uint32(0); i < streamCount; i++ {
		var streamIDLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &streamIDLen); err != nil {
			return nil, false, fmt.Errorf("read stream id length: %w", err)
		}

		if streamIDLen == 0 {
			return nil, false, fmt.Errorf("invalid stream id length 0")
		}

		if streamIDLen > maxStreamIDLen {
			return nil, false, fmt.Errorf("stream id length %d exceeds maximum %d", streamIDLen, maxStreamIDLen)
		}

		streamID := make([]byte, streamIDLen)
		if _, err := io.ReadFull(buf, streamID); err != nil {
			return nil, false, fmt.Errorf("read stream id: %w", err)
		}

		var compressionFlag uint8
		if err := binary.Read(buf, binary.LittleEndian, &compressionFlag); err != nil {
			return nil, false, fmt.Errorf("read compression flag for stream %s: %w", string(streamID), err)
		}

		var uncompressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &uncompressedLen); err != nil {
			return nil, false, fmt.Errorf("read uncompressed len for stream %s: %w", string(streamID), err)
		}

		if uncompressedLen > maxUncompressedLen {
			return nil, false, fmt.Errorf(
				"uncompressed length %d exceeds maximum %d",
				uncompressedLen,
				maxUncompressedLen,
			)
		}

		var compressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
			return nil, false, fmt.Errorf("read compressed len for stream %s: %w", string(streamID), err)
		}

		if compressedLen > maxCompressedLen {
			return nil, false, fmt.Errorf("compressed length %d exceeds maximum %d", compressedLen, maxCompressedLen)
		}

		compressedData := make([]byte, compressedLen)
		if _, err := io.ReadFull(buf, compressedData); err != nil {
			return nil, false, fmt.Errorf("read compressed data for stream %s: %w", string(streamID), err)
		}

		// NOTE: Decompression occurs for all streams since the query string is inside the compressed payload.
		// The peek optimization avoids O(buckets×groups) parsing work for non-matching streams,
		// but not the decompression cost. A future format change could store the query header
		// uncompressed for true zero-decompression skipping.
		streamData, err := decompressAggregateStream(compressionFlag, compressedData, uncompressedLen)
		if err != nil {
			return nil, false, fmt.Errorf("decompress stream %s: %w", string(streamID), err)
		}

		// Peek at query string and step size before performing the full parse.
		// This avoids O(buckets × groups) parsing for streams that don't match.
		if peekQuery, peekStep, peekErr := peekStreamQuery(streamData); peekErr == nil {
			peekSpec, peekSpecErr := compileStreamQuerySpec(peekQuery, peekStep)
			if peekSpecErr == nil {
				peekSpec.Normalize()
				if !exactMatch(&normalized, peekSpec) {
					continue
				}
			}
		}

		streamInfo, err := parseMetricStreamData(streamData)
		if err != nil {
			return nil, false, fmt.Errorf("parse stream %s: %w", string(streamID), err)
		}

		streamSpec, err := compileStreamQuerySpec(streamInfo.query, streamInfo.stepSizeNanos)
		if err != nil {
			continue
		}
		streamSpec.Normalize()

		if !exactMatch(&normalized, streamSpec) {
			continue
		}

		aggSpec, err := aggSpecFromQuerySpec(streamSpec)
		if err != nil {
			return nil, false, fmt.Errorf("build aggregate spec for stream %s: %w", string(streamID), err)
		}

		result, err := buildPrecomputedResult(streamInfo, aggSpec, opts)
		if err != nil {
			return nil, false, fmt.Errorf("build result for stream %s: %w", string(streamID), err)
		}

		return result, true, nil
	}

	return nil, false, nil
}

func findMetricStreamByID(rawData []byte, streamID string) ([]byte, error) {
	buf := bytes.NewReader(rawData)

	var streamCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &streamCount); err != nil {
		return nil, fmt.Errorf("read stream count: %w", err)
	}

	for i := uint32(0); i < streamCount; i++ {
		var idLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &idLen); err != nil {
			return nil, fmt.Errorf("read stream id length: %w", err)
		}

		if idLen > maxStreamIDLen {
			return nil, fmt.Errorf("stream id length %d exceeds maximum %d", idLen, maxStreamIDLen)
		}

		idBytes := make([]byte, idLen)
		if _, err := io.ReadFull(buf, idBytes); err != nil {
			return nil, fmt.Errorf("read stream id: %w", err)
		}

		var compressionFlag uint8
		if err := binary.Read(buf, binary.LittleEndian, &compressionFlag); err != nil {
			return nil, fmt.Errorf("read compression flag for stream %s: %w", string(idBytes), err)
		}

		var uncompressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &uncompressedLen); err != nil {
			return nil, fmt.Errorf("read uncompressed len for stream %s: %w", string(idBytes), err)
		}

		if uncompressedLen > maxUncompressedLen {
			return nil, fmt.Errorf("uncompressed length %d exceeds maximum %d", uncompressedLen, maxUncompressedLen)
		}

		var compressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
			return nil, fmt.Errorf("read compressed len for stream %s: %w", string(idBytes), err)
		}

		if compressedLen > maxCompressedLen {
			return nil, fmt.Errorf("compressed length %d exceeds maximum %d", compressedLen, maxCompressedLen)
		}

		compressedData := make([]byte, compressedLen)
		if _, err := io.ReadFull(buf, compressedData); err != nil {
			return nil, fmt.Errorf("read compressed data for stream %s: %w", string(idBytes), err)
		}

		if string(idBytes) != streamID {
			continue
		}

		streamData, err := decompressAggregateStream(compressionFlag, compressedData, uncompressedLen)
		if err != nil {
			return nil, fmt.Errorf("decompress stream %s: %w", streamID, err)
		}

		return streamData, nil
	}

	return nil, fmt.Errorf("stream %s not found", streamID)
}

func decompressAggregateStream(flag uint8, compressed []byte, uncompressedLen uint32) ([]byte, error) {
	switch flag {
	case preCompressionNone:
		return compressed, nil
	case preCompressionZstd:
		decoder := zstdDecoderPool.Get().(*zstd.Decoder)
		if decoder == nil {
			return nil, fmt.Errorf("failed to get zstd decoder from pool")
		}
		out, err := decoder.DecodeAll(compressed, make([]byte, 0, uncompressedLen))
		if err != nil {
			if resetErr := decoder.Reset(nil); resetErr == nil {
				zstdDecoderPool.Put(decoder)
			}
			return nil, fmt.Errorf("decode zstd data: %w", err)
		}
		zstdDecoderPool.Put(decoder)
		if uint32(len(out)) != uncompressedLen { //nolint:gosec
			return nil, fmt.Errorf("uncompressed size mismatch: expected %d, got %d", uncompressedLen, len(out))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", flag)
	}
}

// readMetricStreamSectionDirect reads only the metric stream section from a provider
// without initializing a full blockpack Reader (avoids reading block metadata).
// Returns nil, nil if no metric stream section exists (metricStreamLen == 0).
//
// Binary layout read:
//   - Footer (last 10 bytes): version(2) + headerOffset(8)
//   - Header (37 bytes at headerOffset): magic(4) + version(1) + metadataOffset(8) +
//     metadataLen(8) + metricStreamOffset(8) + metricStreamLen(8)
//   - Metric stream section: metricStreamLen bytes at metricStreamOffset
func readMetricStreamSectionDirect(provider blockpackio.ReaderProvider) ([]byte, error) {
	dataSize, err := provider.Size()
	if err != nil {
		return nil, fmt.Errorf("get provider size: %w", err)
	}

	const footerSize = 10 // version(2) + headerOffset(8)
	if dataSize < int64(footerSize) {
		return nil, fmt.Errorf("data too small for footer")
	}

	// Read footer to get headerOffset
	footerBuf := make([]byte, footerSize)
	var n int
	n, err = provider.ReadAt(footerBuf, dataSize-int64(footerSize), blockpackio.DataTypeFooter)
	if err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	if n != footerSize {
		return nil, fmt.Errorf("short read footer: got %d bytes, want %d", n, footerSize)
	}
	// footer layout: version(2) + headerOffset(8)
	headerOffset := binary.LittleEndian.Uint64(footerBuf[2:10])

	// Read header (37 bytes) to get metricStreamOffset and metricStreamLen
	const headerSize = 37
	if int64(headerOffset) > dataSize-int64(footerSize)-int64(headerSize) { //nolint:gosec
		return nil, fmt.Errorf("header offset %d out of bounds (file size %d)", headerOffset, dataSize)
	}
	headerBuf := make([]byte, headerSize)
	n, err = provider.ReadAt(headerBuf, int64(headerOffset), blockpackio.DataTypeMetadata) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	if n != headerSize {
		return nil, fmt.Errorf("short read header: got %d bytes, want %d", n, headerSize)
	}

	// Validate this is a real blockpack file before trusting the offsets.
	magic := binary.LittleEndian.Uint32(headerBuf[0:4])
	if magic != blockpackMagicNumber {
		return nil, fmt.Errorf("invalid blockpack magic number %#x in header", magic)
	}
	version := headerBuf[4] //nolint:gosec
	if version != blockpackVersionV10 && version != blockpackVersionV11 {
		return nil, fmt.Errorf("unsupported blockpack version %d (only v10/v11 supported)", version)
	}

	// header layout: magic(4) + version(1) + metadataOffset(8) + metadataLen(8) +
	//                metricStreamOffset(8) + metricStreamLen(8)
	metricStreamOffset, metricStreamLen, err := blockpackio.ParseMetricStreamLocation(headerBuf)
	if err != nil {
		return nil, fmt.Errorf("parse metric stream location: %w", err)
	}

	if metricStreamLen == 0 {
		return nil, nil
	}

	// Validate offset + length stays within file bounds
	if metricStreamOffset+metricStreamLen > uint64(dataSize) { //nolint:gosec
		return nil, fmt.Errorf("metric stream section out of bounds (offset %d + len %d > file size %d)",
			metricStreamOffset, metricStreamLen, dataSize)
	}

	if metricStreamLen > math.MaxInt { //nolint:gosec
		return nil, fmt.Errorf("metric stream length %d exceeds maximum", metricStreamLen)
	}

	data := make([]byte, metricStreamLen)
	if n, err := provider.ReadAt(data, int64(metricStreamOffset), blockpackio.DataTypeUnknown); err != nil { //nolint:gosec
		return nil, fmt.Errorf("read metric stream section: %w", err)
	} else if n != int(metricStreamLen) { //nolint:gosec
		return nil, fmt.Errorf("short read metric stream: got %d bytes, want %d", n, int(metricStreamLen)) //nolint:gosec
	}
	return data, nil
}

// peekStreamQuery reads only the query string and step size from decompressed stream data.
// The binary layout at the start of decompressed metric stream data is:
//
//	queryLen(4) + query(queryLen) + stepSizeNanos(8) + ...
func peekStreamQuery(data []byte) (query string, stepSizeNanos int64, err error) {
	if len(data) < 4 {
		return "", 0, fmt.Errorf("data too short for query length")
	}
	queryLen := binary.LittleEndian.Uint32(data[0:4])
	if queryLen > maxQueryLen {
		return "", 0, fmt.Errorf("query length %d exceeds maximum", queryLen)
	}
	end := uint64(4) + uint64(queryLen) + 8 //nolint:gosec
	if uint64(len(data)) < end {            //nolint:gosec
		return "", 0, fmt.Errorf("data too short for query + step size")
	}
	q := string(data[4 : 4+queryLen])
	step := int64(binary.LittleEndian.Uint64(data[4+queryLen : 4+queryLen+8])) //nolint:gosec
	return q, step, nil
}

func parseMetricStreamData(data []byte) (*precomputedMetricStreamData, error) {
	buf := bytes.NewReader(data)

	var queryLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &queryLen); err != nil {
		return nil, fmt.Errorf("read query length: %w", err)
	}

	if queryLen > maxQueryLen {
		return nil, fmt.Errorf("query length %d exceeds maximum %d", queryLen, maxQueryLen)
	}

	queryBytes := make([]byte, queryLen)
	if _, err := io.ReadFull(buf, queryBytes); err != nil {
		return nil, fmt.Errorf("read query: %w", err)
	}

	var stepSizeNanos int64
	if err := binary.Read(buf, binary.LittleEndian, &stepSizeNanos); err != nil {
		return nil, fmt.Errorf("read step size: %w", err)
	}

	var bucketCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &bucketCount); err != nil {
		return nil, fmt.Errorf("read bucket count: %w", err)
	}

	if bucketCount > maxBucketCount {
		return nil, fmt.Errorf("bucket count %d exceeds maximum %d", bucketCount, maxBucketCount)
	}

	var startTime int64
	if err := binary.Read(buf, binary.LittleEndian, &startTime); err != nil {
		return nil, fmt.Errorf("read start time: %w", err)
	}

	var stepMillis uint32
	if err := binary.Read(buf, binary.LittleEndian, &stepMillis); err != nil {
		return nil, fmt.Errorf("read step millis: %w", err)
	}

	var groupKeyCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &groupKeyCount); err != nil {
		return nil, fmt.Errorf("read group key count: %w", err)
	}

	if groupKeyCount > maxGroupKeyCount {
		return nil, fmt.Errorf("group key count %d exceeds maximum %d", groupKeyCount, maxGroupKeyCount)
	}

	groupKeys := make([][]string, groupKeyCount)
	for i := uint32(0); i < groupKeyCount; i++ {
		groupKey, err := readGroupKey(buf)
		if err != nil {
			return nil, fmt.Errorf("read group key %d: %w", i, err)
		}
		groupKeys[i] = groupKey
	}

	values := make(map[int64]map[int]map[uint8]float64, int(bucketCount))
	for bucketIdx := uint32(0); bucketIdx < bucketCount; bucketIdx++ {
		groupValues := make(map[int]map[uint8]float64, int(groupKeyCount))
		for groupIdx := 0; groupIdx < int(groupKeyCount); groupIdx++ {
			var valueCount uint8
			if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
				return nil, fmt.Errorf("read value count for bucket %d group %d: %w", bucketIdx, groupIdx, err)
			}
			if valueCount == 0 {
				continue
			}
			vals := make(map[uint8]float64, int(valueCount))
			for j := 0; j < int(valueCount); j++ {
				var valType uint8
				if err := binary.Read(buf, binary.LittleEndian, &valType); err != nil {
					return nil, fmt.Errorf(
						"read value type for bucket %d group %d value %d: %w",
						bucketIdx,
						groupIdx,
						j,
						err,
					)
				}
				var val float64
				if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
					return nil, fmt.Errorf(
						"read value for bucket %d group %d value %d: %w",
						bucketIdx,
						groupIdx,
						j,
						err,
					)
				}
				vals[valType] = val
			}
			groupValues[groupIdx] = vals
		}
		values[int64(bucketIdx)] = groupValues
	}

	return &precomputedMetricStreamData{
		query:         string(queryBytes),
		stepSizeNanos: stepSizeNanos,
		bucketCount:   bucketCount,
		startTime:     startTime,
		stepMillis:    stepMillis,
		groupKeys:     groupKeys,
		values:        values,
	}, nil
}

func readGroupKey(buf *bytes.Reader) ([]string, error) {
	var fieldCount uint8
	if err := binary.Read(buf, binary.LittleEndian, &fieldCount); err != nil {
		return nil, fmt.Errorf("read group key field count: %w", err)
	}

	values := make([]string, fieldCount)
	for i := 0; i < int(fieldCount); i++ {
		var valueType uint8
		if err := binary.Read(buf, binary.LittleEndian, &valueType); err != nil {
			return nil, fmt.Errorf("read group key field %d type: %w", i, err)
		}
		switch valueType {
		case preValueTypeString:
			var lenStr uint16
			if err := binary.Read(buf, binary.LittleEndian, &lenStr); err != nil {
				return nil, fmt.Errorf("read group key field %d string length: %w", i, err)
			}

			if lenStr > maxGroupKeyLen {
				return nil, fmt.Errorf("group key string length %d exceeds maximum %d", lenStr, maxGroupKeyLen)
			}

			raw := make([]byte, lenStr)
			if _, err := io.ReadFull(buf, raw); err != nil {
				return nil, fmt.Errorf("read group key field %d string value: %w", i, err)
			}
			values[i] = string(raw)
		case preValueTypeInt64:
			var v int64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read group key field %d int64 value: %w", i, err)
			}
			values[i] = formatValueAsString(v)
		case preValueTypeFloat64:
			var v float64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read group key field %d float64 value: %w", i, err)
			}
			values[i] = formatValueAsString(v)
		case preValueTypeBool:
			var v uint8
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, fmt.Errorf("read group key field %d bool value: %w", i, err)
			}
			values[i] = formatValueAsString(v == 1)
		default:
			return nil, fmt.Errorf("unsupported group key value type: %d", valueType)
		}
	}

	return values, nil
}

func compileStreamQuerySpec(query string, stepSizeNanos int64) (*sql.QuerySpec, error) {
	parsed, err := traceqlparser.ParseTraceQL(query)
	if err == nil {
		if metricsQuery, ok := parsed.(*traceqlparser.MetricsQuery); ok {
			spec, err := sql.CompileTraceQLMetricsToSpec(metricsQuery, sql.TimeBucketSpec{ //nolint:govet
				Enabled:       true,
				StepSizeNanos: stepSizeNanos,
			})
			if err != nil {
				return nil, fmt.Errorf("compile TraceQL metrics query: %w", err)
			}
			return spec, nil
		}
	}

	stmt, err := sql.ParseSQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse SQL query: %w", err)
	}

	spec, err := sql.CompileSQLToSpec(stmt, sql.TimeBucketSpec{
		Enabled:       true,
		StepSizeNanos: stepSizeNanos,
	})
	if err != nil {
		return nil, fmt.Errorf("compile SQL query: %w", err)
	}
	return spec, nil
}

func aggSpecFromQuerySpec(spec *sql.QuerySpec) (vm.AggSpec, error) {
	switch spec.Aggregate.Function {
	case "COUNT":
		return vm.AggSpec{Function: vm.AggCount}, nil
	case "RATE":
		return vm.AggSpec{Function: vm.AggRate}, nil
	case "SUM":
		return vm.AggSpec{Function: vm.AggSum, Field: spec.Aggregate.Field}, nil
	case "AVG":
		return vm.AggSpec{Function: vm.AggAvg, Field: spec.Aggregate.Field}, nil
	case "MIN":
		return vm.AggSpec{Function: vm.AggMin, Field: spec.Aggregate.Field}, nil
	case "MAX":
		return vm.AggSpec{Function: vm.AggMax, Field: spec.Aggregate.Field}, nil
	case "QUANTILE":
		return vm.AggSpec{Function: vm.AggQuantile, Field: spec.Aggregate.Field, Quantile: spec.Aggregate.Quantile}, nil
	case "HISTOGRAM":
		return vm.AggSpec{Function: vm.AggHistogram, Field: spec.Aggregate.Field}, nil
	case "STDDEV":
		return vm.AggSpec{Function: vm.AggStddev, Field: spec.Aggregate.Field}, nil
	default:
		return vm.AggSpec{}, fmt.Errorf("unsupported aggregate function: %s", spec.Aggregate.Function)
	}
}

func buildPrecomputedResult(
	data *precomputedMetricStreamData,
	aggSpec vm.AggSpec,
	opts QueryOptions,
) (*BlockpackResult, error) {
	if aggSpec.Function == vm.AggQuantile || aggSpec.Function == vm.AggHistogram || aggSpec.Function == vm.AggStddev {
		return nil, errPrecomputedUnsupported
	}

	outputName := aggregationOutputName(aggSpec)
	timeBucketed := opts.StartTime > 0 && opts.EndTime > 0

	if timeBucketed {
		if data.stepSizeNanos <= 0 {
			return nil, fmt.Errorf("invalid step size %d", data.stepSizeNanos)
		}
		numBuckets := denseBucketCount(opts.StartTime, opts.EndTime, data.stepSizeNanos)
		bucketValues := remapBucketsToQueryRange(data, opts)

		rows := make([]AggregateRow, 0, int(numBuckets)*len(data.groupKeys))
		for groupIdx, groupKeyValues := range data.groupKeys {
			for bucketIdx := int64(0); bucketIdx < numBuckets; bucketIdx++ {
				var vals map[uint8]float64
				if groupValues := bucketValues[bucketIdx]; groupValues != nil {
					vals = groupValues[groupIdx]
				}
				value := precomputedAggregateValue(aggSpec, vals, data.stepSizeNanos)
				groupKey := make([]string, 0, 1+len(groupKeyValues))
				groupKey = append(groupKey, fmt.Sprintf("%d", bucketIdx))
				groupKey = append(groupKey, groupKeyValues...)
				rows = append(rows, AggregateRow{
					GroupKey: groupKey,
					Values:   map[string]float64{outputName: value},
				})
			}
		}

		sortDenseAggregateRows(rows)

		return buildDenseAggregateResult(rows, nil, 0, 0, 0), nil
	}

	rows := make([]AggregateRow, 0, len(data.groupKeys))
	timeWindowSeconds := float64(opts.EndTime-opts.StartTime) / 1e9
	for groupIdx, groupKeyValues := range data.groupKeys {
		aggValues := aggregateAcrossBuckets(data.values, groupIdx)
		value := precomputedAggregateValueWithWindow(aggSpec, aggValues, timeWindowSeconds)
		rows = append(rows, AggregateRow{
			GroupKey: groupKeyValues,
			Values:   map[string]float64{outputName: value},
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		for k := 0; k < len(rows[i].GroupKey) && k < len(rows[j].GroupKey); k++ {
			if rows[i].GroupKey[k] != rows[j].GroupKey[k] {
				return rows[i].GroupKey[k] < rows[j].GroupKey[k]
			}
		}
		return len(rows[i].GroupKey) < len(rows[j].GroupKey)
	})

	return &BlockpackResult{
		Matches:       nil,
		BlocksScanned: 0,
		AggregateRows: rows,
		IsAggregated:  true,
	}, nil
}

func remapBucketsToQueryRange(
	data *precomputedMetricStreamData,
	opts QueryOptions,
) map[int64]map[int]map[uint8]float64 {
	// Pre-size map based on expected bucket count
	expectedBuckets := int(data.bucketCount)
	if opts.StartTime > 0 && opts.EndTime > 0 && data.stepSizeNanos > 0 {
		expectedBuckets = int(denseBucketCount(opts.StartTime, opts.EndTime, data.stepSizeNanos))
	}
	out := make(map[int64]map[int]map[uint8]float64, expectedBuckets)
	if data.stepSizeNanos <= 0 {
		return out
	}

	stepNanos := data.stepSizeNanos
	for bucketIdx, groupValues := range data.values {
		bucketTime := data.startTime + (bucketIdx * int64(data.stepMillis) * 1_000_000)
		if bucketTime < opts.StartTime || bucketTime > opts.EndTime {
			continue
		}
		queryBucketIdx := (bucketTime - opts.StartTime) / stepNanos
		if queryBucketIdx < 0 {
			continue
		}
		if existing := out[queryBucketIdx]; existing != nil {
			mergeGroupValues(existing, groupValues)
			continue
		}
		out[queryBucketIdx] = copyGroupValues(groupValues)
	}

	return out
}

func mergeGroupValues(dst map[int]map[uint8]float64, src map[int]map[uint8]float64) {
	for groupIdx, vals := range src {
		if existing, ok := dst[groupIdx]; ok {
			mergeAggregateValues(existing, vals)
			continue
		}
		dst[groupIdx] = copyAggregateValues(vals)
	}
}

func copyGroupValues(src map[int]map[uint8]float64) map[int]map[uint8]float64 {
	out := make(map[int]map[uint8]float64, len(src))
	for groupIdx, vals := range src {
		out[groupIdx] = copyAggregateValues(vals)
	}
	return out
}

func copyAggregateValues(src map[uint8]float64) map[uint8]float64 {
	out := make(map[uint8]float64, len(src))
	for key, val := range src {
		out[key] = val
	}
	return out
}

func mergeAggregateValues(dst map[uint8]float64, src map[uint8]float64) {
	for key, val := range src {
		switch key {
		case preAggTypeCount, preAggTypeSum:
			dst[key] += val
		case preAggTypeMin:
			existing, ok := dst[key]
			if !ok || val < existing {
				dst[key] = val
			}
		case preAggTypeMax:
			existing, ok := dst[key]
			if !ok || val > existing {
				dst[key] = val
			}
		default:
			dst[key] = val
		}
	}
}

func aggregateAcrossBuckets(values map[int64]map[int]map[uint8]float64, groupIdx int) map[uint8]float64 {
	var (
		count  float64
		sum    float64
		minVal float64
		maxVal float64
		hasMin bool
		hasMax bool
	)

	for _, groupValues := range values {
		vals, ok := groupValues[groupIdx]
		if !ok {
			continue
		}
		if v, ok := vals[preAggTypeCount]; ok {
			count += v
		}
		if v, ok := vals[preAggTypeSum]; ok {
			sum += v
		}
		if v, ok := vals[preAggTypeMin]; ok {
			if !hasMin || v < minVal {
				minVal = v
				hasMin = true
			}
		}
		if v, ok := vals[preAggTypeMax]; ok {
			if !hasMax || v > maxVal {
				maxVal = v
				hasMax = true
			}
		}
	}

	// Pre-size map for expected aggregate types (count, sum, min, max)
	out := make(map[uint8]float64, 4)
	if count != 0 {
		out[preAggTypeCount] = count
	}
	if sum != 0 {
		out[preAggTypeSum] = sum
	}
	if hasMin {
		out[preAggTypeMin] = minVal
	}
	if hasMax {
		out[preAggTypeMax] = maxVal
	}

	return out
}

func precomputedAggregateValue(aggSpec vm.AggSpec, values map[uint8]float64, stepSizeNs int64) float64 {
	switch aggSpec.Function {
	case vm.AggCount:
		return values[preAggTypeCount]
	case vm.AggRate:
		if stepSizeNs == 0 {
			return 0
		}
		return values[preAggTypeCount] / (float64(stepSizeNs) / 1e9)
	case vm.AggSum:
		return values[preAggTypeSum]
	case vm.AggAvg:
		count := values[preAggTypeCount]
		if count == 0 {
			return math.NaN()
		}
		return values[preAggTypeSum] / count
	case vm.AggMin:
		return values[preAggTypeMin]
	case vm.AggMax:
		return values[preAggTypeMax]
	default:
		return 0
	}
}

func precomputedAggregateValueWithWindow(
	aggSpec vm.AggSpec,
	values map[uint8]float64,
	timeWindowSeconds float64,
) float64 {
	switch aggSpec.Function {
	case vm.AggRate:
		if timeWindowSeconds == 0 {
			return 0
		}
		return values[preAggTypeCount] / timeWindowSeconds
	default:
		return precomputedAggregateValue(aggSpec, values, 0)
	}
}

func precomputedFilterSupported(spec *sql.QuerySpec) bool {
	for path, values := range spec.Filter.AttributeEquals {
		if !isStatusOrKindPath(path) {
			continue
		}
		for _, v := range values {
			if _, ok := v.(string); ok {
				return false
			}
		}
	}

	for path, rangeSpec := range spec.Filter.AttributeRanges {
		if !isStatusOrKindPath(path) || rangeSpec == nil {
			continue
		}
		if _, ok := rangeSpec.MinValue.(string); ok {
			return false
		}
		if _, ok := rangeSpec.MaxValue.(string); ok {
			return false
		}
	}

	return true
}

func isStatusOrKindPath(path string) bool {
	switch path {
	case "span:status", "span:kind":
		return true
	default:
		return false
	}
}

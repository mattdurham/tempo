package executor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/klauspost/compress/zstd"
	blockpackio "github.com/mattdurham/blockpack/blockpack/io"
	"github.com/mattdurham/blockpack/sql"
	"github.com/mattdurham/blockpack/traceql"
	"github.com/mattdurham/blockpack/vm"
)

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

var errPrecomputedUnsupported = errors.New("precomputed metric streams unsupported")

type precomputedMetricStreamData struct {
	query         string
	stepSizeNanos int64
	bucketCount   uint32
	startTime     int64
	stepMillis    uint32
	groupKeys     [][]string
	values        map[int64]map[int]map[uint8]float64
}

func (e *BlockpackExecutor) tryInlinePrecomputedMetricStreams(path string, querySpec *sql.QuerySpec, opts QueryOptions) (*BlockpackResult, bool, error) {
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

	var tracking *blockpackio.TrackingReaderProvider
	if opts.IOLatency > 0 {
		tracking = blockpackio.NewTrackingReaderProviderWithLatency(provider, opts.IOLatency)
	} else {
		tracking = blockpackio.NewTrackingReaderProvider(provider)
	}

	reader, err := blockpackio.NewReaderFromProvider(tracking)
	if err != nil {
		return nil, false, err
	}

	rawData, err := reader.GetMetricStreamBlocksRaw()
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

	result.BytesRead = tracking.BytesRead()
	result.IOOperations = tracking.IOOperations()
	return result, true, nil
}

func findMatchingPrecomputedMetricStreamResult(rawData []byte, querySpec *sql.QuerySpec, opts QueryOptions) (*BlockpackResult, bool, error) {
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

		streamID := make([]byte, streamIDLen)
		if _, err := io.ReadFull(buf, streamID); err != nil {
			return nil, false, fmt.Errorf("read stream id: %w", err)
		}

		var compressionFlag uint8
		if err := binary.Read(buf, binary.LittleEndian, &compressionFlag); err != nil {
			return nil, false, fmt.Errorf("read compression flag: %w", err)
		}

		var uncompressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &uncompressedLen); err != nil {
			return nil, false, fmt.Errorf("read uncompressed len: %w", err)
		}

		var compressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
			return nil, false, fmt.Errorf("read compressed len: %w", err)
		}

		compressedData := make([]byte, compressedLen)
		if _, err := io.ReadFull(buf, compressedData); err != nil {
			return nil, false, fmt.Errorf("read compressed data: %w", err)
		}

		streamData, err := decompressAggregateStream(compressionFlag, compressedData, uncompressedLen)
		if err != nil {
			return nil, false, fmt.Errorf("decompress stream %s: %w", string(streamID), err)
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
			return nil, false, err
		}

		result, err := buildPrecomputedResult(streamInfo, aggSpec, opts)
		if err != nil {
			return nil, false, err
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
		idBytes := make([]byte, idLen)
		if _, err := io.ReadFull(buf, idBytes); err != nil {
			return nil, fmt.Errorf("read stream id: %w", err)
		}

		var compressionFlag uint8
		if err := binary.Read(buf, binary.LittleEndian, &compressionFlag); err != nil {
			return nil, fmt.Errorf("read compression flag: %w", err)
		}

		var uncompressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &uncompressedLen); err != nil {
			return nil, fmt.Errorf("read uncompressed len: %w", err)
		}

		var compressedLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &compressedLen); err != nil {
			return nil, fmt.Errorf("read compressed len: %w", err)
		}

		compressedData := make([]byte, compressedLen)
		if _, err := io.ReadFull(buf, compressedData); err != nil {
			return nil, fmt.Errorf("read compressed data: %w", err)
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
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		out, err := decoder.DecodeAll(compressed, make([]byte, 0, uncompressedLen))
		if err != nil {
			return nil, err
		}
		if uint32(len(out)) != uncompressedLen {
			return nil, fmt.Errorf("uncompressed size mismatch: expected %d, got %d", uncompressedLen, len(out))
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported compression flag: %d", flag)
	}
}

func parseMetricStreamData(data []byte) (*precomputedMetricStreamData, error) {
	buf := bytes.NewReader(data)

	var queryLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &queryLen); err != nil {
		return nil, err
	}
	queryBytes := make([]byte, queryLen)
	if _, err := io.ReadFull(buf, queryBytes); err != nil {
		return nil, err
	}

	var stepSizeNanos int64
	if err := binary.Read(buf, binary.LittleEndian, &stepSizeNanos); err != nil {
		return nil, err
	}

	var bucketCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &bucketCount); err != nil {
		return nil, err
	}

	var startTime int64
	if err := binary.Read(buf, binary.LittleEndian, &startTime); err != nil {
		return nil, err
	}

	var stepMillis uint32
	if err := binary.Read(buf, binary.LittleEndian, &stepMillis); err != nil {
		return nil, err
	}

	var groupKeyCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &groupKeyCount); err != nil {
		return nil, err
	}

	groupKeys := make([][]string, groupKeyCount)
	for i := uint32(0); i < groupKeyCount; i++ {
		groupKey, err := readGroupKey(buf)
		if err != nil {
			return nil, err
		}
		groupKeys[i] = groupKey
	}

	values := make(map[int64]map[int]map[uint8]float64, bucketCount)
	for bucketIdx := uint32(0); bucketIdx < bucketCount; bucketIdx++ {
		groupValues := make(map[int]map[uint8]float64, groupKeyCount)
		for groupIdx := 0; groupIdx < int(groupKeyCount); groupIdx++ {
			var valueCount uint8
			if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
				return nil, err
			}
			if valueCount == 0 {
				continue
			}
			vals := make(map[uint8]float64, valueCount)
			for j := 0; j < int(valueCount); j++ {
				var valType uint8
				if err := binary.Read(buf, binary.LittleEndian, &valType); err != nil {
					return nil, err
				}
				var val float64
				if err := binary.Read(buf, binary.LittleEndian, &val); err != nil {
					return nil, err
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
		return nil, err
	}

	values := make([]string, fieldCount)
	for i := 0; i < int(fieldCount); i++ {
		var valueType uint8
		if err := binary.Read(buf, binary.LittleEndian, &valueType); err != nil {
			return nil, err
		}
		switch valueType {
		case preValueTypeString:
			var lenStr uint16
			if err := binary.Read(buf, binary.LittleEndian, &lenStr); err != nil {
				return nil, err
			}
			raw := make([]byte, lenStr)
			if _, err := io.ReadFull(buf, raw); err != nil {
				return nil, err
			}
			values[i] = string(raw)
		case preValueTypeInt64:
			var v int64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			values[i] = formatValueAsString(v)
		case preValueTypeFloat64:
			var v float64
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			values[i] = formatValueAsString(v)
		case preValueTypeBool:
			var v uint8
			if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			values[i] = formatValueAsString(v == 1)
		default:
			return nil, fmt.Errorf("unsupported group key value type: %d", valueType)
		}
	}

	return values, nil
}

func compileStreamQuerySpec(query string, stepSizeNanos int64) (*sql.QuerySpec, error) {
	parsed, err := traceql.ParseTraceQL(query)
	if err == nil {
		if metricsQuery, ok := parsed.(*traceql.MetricsQuery); ok {
			return sql.CompileTraceQLMetricsToSpec(metricsQuery, sql.TimeBucketSpec{
				Enabled:       true,
				StepSizeNanos: stepSizeNanos,
			})
		}
	}

	stmt, err := sql.ParseSQL(query)
	if err != nil {
		return nil, err
	}

	return sql.CompileSQLToSpec(stmt, sql.TimeBucketSpec{
		Enabled:       true,
		StepSizeNanos: stepSizeNanos,
	})
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

func buildPrecomputedResult(data *precomputedMetricStreamData, aggSpec vm.AggSpec, opts QueryOptions) (*BlockpackResult, error) {
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

func remapBucketsToQueryRange(data *precomputedMetricStreamData, opts QueryOptions) map[int64]map[int]map[uint8]float64 {
	out := make(map[int64]map[int]map[uint8]float64)
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
		min    float64
		max    float64
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
			if !hasMin || v < min {
				min = v
				hasMin = true
			}
		}
		if v, ok := vals[preAggTypeMax]; ok {
			if !hasMax || v > max {
				max = v
				hasMax = true
			}
		}
	}

	out := make(map[uint8]float64)
	if count != 0 {
		out[preAggTypeCount] = count
	}
	if sum != 0 {
		out[preAggTypeSum] = sum
	}
	if hasMin {
		out[preAggTypeMin] = min
	}
	if hasMax {
		out[preAggTypeMax] = max
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

func precomputedAggregateValueWithWindow(aggSpec vm.AggSpec, values map[uint8]float64, timeWindowSeconds float64) float64 {
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

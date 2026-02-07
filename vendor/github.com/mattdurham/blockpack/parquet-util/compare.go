package parquetutil

import (
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"

	blockpackio "github.com/mattdurham/blockpack/blockpack/io"
	blockpack "github.com/mattdurham/blockpack/blockpack/types"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

const compareMaxSamples = 20

type CompareResult struct {
	SpanCountBlockpack  int             `json:"spanCountBlockpack"`
	SpanCountParquet    int             `json:"spanCountParquet"`
	TraceCountBlockpack int             `json:"traceCountBlockpack"`
	TraceCountParquet   int             `json:"traceCountParquet"`
	MissingSpans        []string        `json:"missingSpans"`
	ExtraSpans          []string        `json:"extraSpans"`
	AttributeDiffs      []AttributeDiff `json:"attributeDiffs"`
	MissingSpanCount    int             `json:"missingSpanCount"`
	ExtraSpanCount      int             `json:"extraSpanCount"`
	AttributeDiffCount  int             `json:"attributeDiffCount"`
	Match               bool            `json:"match"`
	Notes               []string        `json:"notes"`
}

type AttributeDiff struct {
	SpanID         string `json:"spanId"`
	Key            string `json:"key"`
	BlockpackValue string `json:"blockpackValue"`
	ParquetValue   string `json:"parquetValue"`
}

type valueCell struct {
	typ      blockpack.ColumnType
	str      string
	i64      int64
	u64      uint64
	f64      float64
	b        bool
	bytes    []byte
	array    []blockpackio.ArrayValue
	hasArray bool
}

func DeepCompareParquetBlockpack(parquetPath, blockpackPath string) (*CompareResult, error) {
	// Read parquet directly (not via blockpack conversion)
	parquetSpans, parquetTraces, err := spansFromParquetFile(parquetPath)
	if err != nil {
		return nil, fmt.Errorf("read parquet: %w", err)
	}

	blockpackBytes, err := os.ReadFile(blockpackPath)
	if err != nil {
		return nil, err
	}

	blockpackSpans, blockpackTraces, err := spansFromBlockpackBytes(blockpackBytes)
	if err != nil {
		return nil, fmt.Errorf("read blockpack: %w", err)
	}

	result := &CompareResult{
		SpanCountBlockpack:  len(blockpackSpans),
		SpanCountParquet:    len(parquetSpans),
		TraceCountBlockpack: len(blockpackTraces),
		TraceCountParquet:   len(parquetTraces),
	}

	missing, extra, diffs := compareSpanMaps(parquetSpans, blockpackSpans)
	result.MissingSpanCount = len(missing)
	result.ExtraSpanCount = len(extra)
	result.AttributeDiffCount = len(diffs)
	result.MissingSpans = trimStringList(missing, compareMaxSamples)
	result.ExtraSpans = trimStringList(extra, compareMaxSamples)
	result.AttributeDiffs = trimDiffs(diffs, compareMaxSamples)
	result.Match = result.MissingSpanCount == 0 && result.ExtraSpanCount == 0 && result.AttributeDiffCount == 0

	if result.TraceCountBlockpack == 0 || result.TraceCountParquet == 0 {
		result.Notes = append(result.Notes, "Trace counts are based on the trace:id column; missing values may reduce counts.")
	}

	return result, nil
}

func spansFromParquetFile(parquetPath string) (map[string]map[string]valueCell, map[string]struct{}, error) {
	// Read Tempo traces directly from parquet
	tempoTraces, err := readTempoTracesFromParquet(parquetPath)
	if err != nil {
		return nil, nil, err
	}

	// Convert to OTLP for easier attribute extraction
	otlpTraces := make([]*tracev1.TracesData, 0, len(tempoTraces))
	for _, tr := range tempoTraces {
		otlpTrace := tempoTraceToOTLP(tr)
		otlpTraces = append(otlpTraces, otlpTrace)
	}

	spanMap, traceSet := spansFromOTLPTraces(otlpTraces)
	return spanMap, traceSet, nil
}

// anyValueToArrayValue converts an OTLP AnyValue to an ArrayValue for comparison
func anyValueToArrayValue(anyVal *commonv1.AnyValue) blockpackio.ArrayValue {
	if anyVal == nil {
		return blockpackio.ArrayValue{}
	}
	switch v := anyVal.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return blockpackio.ArrayValue{Type: blockpackio.ArrayTypeString, Str: v.StringValue}
	case *commonv1.AnyValue_IntValue:
		return blockpackio.ArrayValue{Type: blockpackio.ArrayTypeInt64, Int: v.IntValue}
	case *commonv1.AnyValue_DoubleValue:
		return blockpackio.ArrayValue{Type: blockpackio.ArrayTypeFloat64, Float: v.DoubleValue}
	case *commonv1.AnyValue_BoolValue:
		return blockpackio.ArrayValue{Type: blockpackio.ArrayTypeBool, Bool: v.BoolValue}
	case *commonv1.AnyValue_BytesValue:
		return blockpackio.ArrayValue{Type: blockpackio.ArrayTypeBytes, Bytes: v.BytesValue}
	default:
		return blockpackio.ArrayValue{}
	}
}

// spansFromOTLPTraces extracts spans from OTLP traces for comparison.
// This is separated out so we can reuse already-converted traces without re-reading parquet.
func spansFromOTLPTraces(otlpTraces []*tracev1.TracesData) (map[string]map[string]valueCell, map[string]struct{}) {
	spanMap := make(map[string]map[string]valueCell)
	traceSet := make(map[string]struct{})

	for _, td := range otlpTraces {
		for _, rs := range td.ResourceSpans {
			resource := rs.GetResource()
			resourceAttrs := resource.GetAttributes()

			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					spanID := hex.EncodeToString(span.SpanId)
					if spanID == "" {
						continue
					}

					if _, ok := spanMap[spanID]; !ok {
						spanMap[spanID] = make(map[string]valueCell)
					}

					// Add trace:id (intrinsic with :)
					traceID := hex.EncodeToString(span.TraceId)
					spanMap[spanID]["trace:id"] = valueCell{typ: blockpack.ColumnTypeBytes, bytes: span.TraceId}
					traceSet[traceID] = struct{}{}

					// Add span:id (intrinsic with :)
					spanMap[spanID]["span:id"] = valueCell{typ: blockpack.ColumnTypeBytes, bytes: span.SpanId}

					// Add span intrinsics (use : syntax for intrinsics)
					spanMap[spanID]["span:name"] = valueCell{typ: blockpack.ColumnTypeString, str: span.Name}
					spanMap[spanID]["span:kind"] = valueCell{typ: blockpack.ColumnTypeInt64, i64: int64(span.Kind)}
					spanMap[spanID]["start_time"] = valueCell{typ: blockpack.ColumnTypeUint64, u64: span.StartTimeUnixNano}
					spanMap[spanID]["end_time"] = valueCell{typ: blockpack.ColumnTypeUint64, u64: span.EndTimeUnixNano}

					duration := uint64(0)
					if span.EndTimeUnixNano >= span.StartTimeUnixNano {
						duration = span.EndTimeUnixNano - span.StartTimeUnixNano
					}
					spanMap[spanID]["duration"] = valueCell{typ: blockpack.ColumnTypeUint64, u64: duration}

					// Add resource attributes (with resource. prefix)
					for _, attr := range resourceAttrs {
						colName := "resource." + attr.Key
						addAttributeValue(spanMap[spanID], colName, attr.Value)
					}

					// Add span attributes (with span. prefix)
					for _, attr := range span.Attributes {
						colName := "span." + attr.Key
						addAttributeValue(spanMap[spanID], colName, attr.Value)
					}

					// Add events (array-valued columns)
					if len(span.Events) > 0 {
						eventNames := make([]blockpackio.ArrayValue, 0, len(span.Events))
						eventTimes := make([]blockpackio.ArrayValue, 0, len(span.Events))
						eventDropped := make([]blockpackio.ArrayValue, 0, len(span.Events))
						eventAttrsMap := make(map[string][]blockpackio.ArrayValue)

						for _, event := range span.Events {
							eventNames = append(eventNames, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeString, Str: event.Name})

							// Calculate time since span start
							timeSinceStart := int64(0)
							if event.TimeUnixNano >= span.StartTimeUnixNano {
								timeSinceStart = int64(event.TimeUnixNano - span.StartTimeUnixNano)
							}
							eventTimes = append(eventTimes, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeDuration, Int: timeSinceStart})
							eventDropped = append(eventDropped, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeInt64, Int: int64(event.DroppedAttributesCount)})

							// Add event attributes
							for _, attr := range event.Attributes {
								attrKey := "event." + attr.Key
								eventAttrsMap[attrKey] = append(eventAttrsMap[attrKey], anyValueToArrayValue(attr.Value))
							}
						}

						spanMap[spanID]["event:name"] = valueCell{typ: blockpack.ColumnTypeBytes, array: eventNames, hasArray: true}
						spanMap[spanID]["event:time_since_start"] = valueCell{typ: blockpack.ColumnTypeBytes, array: eventTimes, hasArray: true}
						spanMap[spanID]["event:dropped_attributes_count"] = valueCell{typ: blockpack.ColumnTypeBytes, array: eventDropped, hasArray: true}

						for attrKey, attrValues := range eventAttrsMap {
							spanMap[spanID][attrKey] = valueCell{typ: blockpack.ColumnTypeBytes, array: attrValues, hasArray: true}
						}
					}

					// Add links (array-valued columns)
					if len(span.Links) > 0 {
						linkTraceIDs := make([]blockpackio.ArrayValue, 0, len(span.Links))
						linkSpanIDs := make([]blockpackio.ArrayValue, 0, len(span.Links))
						linkTraceStates := make([]blockpackio.ArrayValue, 0, len(span.Links))
						linkDropped := make([]blockpackio.ArrayValue, 0, len(span.Links))
						linkAttrsMap := make(map[string][]blockpackio.ArrayValue)

						for _, link := range span.Links {
							linkTraceIDs = append(linkTraceIDs, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeString, Str: hex.EncodeToString(link.TraceId)})
							linkSpanIDs = append(linkSpanIDs, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeString, Str: hex.EncodeToString(link.SpanId)})
							linkTraceStates = append(linkTraceStates, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeString, Str: link.TraceState})
							linkDropped = append(linkDropped, blockpackio.ArrayValue{Type: blockpackio.ArrayTypeInt64, Int: int64(link.DroppedAttributesCount)})

							// Add link attributes
							for _, attr := range link.Attributes {
								attrKey := "link." + attr.Key
								linkAttrsMap[attrKey] = append(linkAttrsMap[attrKey], anyValueToArrayValue(attr.Value))
							}
						}

						spanMap[spanID]["link:trace_id"] = valueCell{typ: blockpack.ColumnTypeBytes, array: linkTraceIDs, hasArray: true}
						spanMap[spanID]["link:span_id"] = valueCell{typ: blockpack.ColumnTypeBytes, array: linkSpanIDs, hasArray: true}
						spanMap[spanID]["link:trace_state"] = valueCell{typ: blockpack.ColumnTypeBytes, array: linkTraceStates, hasArray: true}
						spanMap[spanID]["link:dropped_attributes_count"] = valueCell{typ: blockpack.ColumnTypeBytes, array: linkDropped, hasArray: true}

						for attrKey, attrValues := range linkAttrsMap {
							spanMap[spanID][attrKey] = valueCell{typ: blockpack.ColumnTypeBytes, array: attrValues, hasArray: true}
						}
					}
				}
			}
		}
	}

	return spanMap, traceSet
}

func addAttributeValue(spanAttrs map[string]valueCell, colName string, anyVal *commonv1.AnyValue) {
	if anyVal == nil {
		return
	}
	switch v := anyVal.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		spanAttrs[colName] = valueCell{typ: blockpack.ColumnTypeString, str: v.StringValue}
	case *commonv1.AnyValue_IntValue:
		spanAttrs[colName] = valueCell{typ: blockpack.ColumnTypeInt64, i64: v.IntValue}
	case *commonv1.AnyValue_DoubleValue:
		spanAttrs[colName] = valueCell{typ: blockpack.ColumnTypeFloat64, f64: v.DoubleValue}
	case *commonv1.AnyValue_BoolValue:
		spanAttrs[colName] = valueCell{typ: blockpack.ColumnTypeBool, b: v.BoolValue}
	case *commonv1.AnyValue_BytesValue:
		spanAttrs[colName] = valueCell{typ: blockpack.ColumnTypeBytes, bytes: v.BytesValue}
	}
}

func spansFromBlockpackBytes(data []byte) (map[string]map[string]valueCell, map[string]struct{}, error) {
	reader, err := blockpackio.NewReader(data)
	if err != nil {
		return nil, nil, err
	}

	spanMap := make(map[string]map[string]valueCell)
	traceSet := make(map[string]struct{})

	blocks := reader.Blocks()
	for blockIdx := range blocks {
		block, err := reader.GetBlock(blockIdx)
		if err != nil {
			return nil, nil, err
		}

		spanIDCol := block.GetColumn("span:id")
		if spanIDCol == nil {
			continue
		}

		columns := block.Columns()
		spanIDs := make([]string, block.SpanCount())
		for i := 0; i < block.SpanCount(); i++ {
			spanIDBytes, ok := spanIDCol.BytesValue(i)
			if !ok || len(spanIDBytes) == 0 {
				continue
			}
			spanID := hex.EncodeToString(spanIDBytes)
			spanIDs[i] = spanID
			if _, ok := spanMap[spanID]; !ok {
				spanMap[spanID] = make(map[string]valueCell)
			}
		}

		for name, col := range columns {
			for i := 0; i < block.SpanCount(); i++ {
				spanID := spanIDs[i]
				if spanID == "" {
					continue
				}
				val, ok := columnValueCell(name, col, i)
				if !ok {
					continue
				}
				spanMap[spanID][name] = val
				if name == "trace:id" {
					traceSet[valueKey(val)] = struct{}{}
				}
			}
		}
	}

	return spanMap, traceSet, nil
}

func columnValueCell(name string, col *blockpack.Column, idx int) (valueCell, bool) {
	if col == nil {
		return valueCell{}, false
	}
	switch col.Type {
	case blockpack.ColumnTypeString:
		val, ok := col.StringValue(idx)
		return valueCell{typ: col.Type, str: val}, ok
	case blockpack.ColumnTypeInt64:
		val, ok := col.Int64Value(idx)
		return valueCell{typ: col.Type, i64: val}, ok
	case blockpack.ColumnTypeUint64:
		val, ok := col.Uint64Value(idx)
		return valueCell{typ: col.Type, u64: val}, ok
	case blockpack.ColumnTypeBool:
		val, ok := col.BoolValue(idx)
		return valueCell{typ: col.Type, b: val}, ok
	case blockpack.ColumnTypeFloat64:
		val, ok := col.Float64Value(idx)
		return valueCell{typ: col.Type, f64: val}, ok
	case blockpack.ColumnTypeBytes:
		val, ok := col.BytesValue(idx)
		if !ok {
			return valueCell{}, false
		}
		if blockpackio.IsArrayColumn(name) {
			arrayValues, err := blockpackio.DecodeArray(val)
			if err == nil {
				return valueCell{typ: col.Type, array: arrayValues, hasArray: true}, true
			}
		}
		return valueCell{typ: col.Type, bytes: append([]byte(nil), val...)}, true
	default:
		return valueCell{}, false
	}
}

func compareSpanMaps(parquetSpans, blockpackSpans map[string]map[string]valueCell) ([]string, []string, []AttributeDiff) {
	missing := make([]string, 0)
	extra := make([]string, 0)
	diffs := make([]AttributeDiff, 0)

	for spanID, pqAttrs := range parquetSpans {
		colAttrs, ok := blockpackSpans[spanID]
		if !ok {
			missing = append(missing, spanID)
			continue
		}
		keys := make(map[string]struct{})
		for k := range pqAttrs {
			keys[k] = struct{}{}
		}
		for k := range colAttrs {
			keys[k] = struct{}{}
		}
		for k := range keys {
			pv, pok := pqAttrs[k]
			cv, cok := colAttrs[k]
			if !pok {
				if !isDefaultValue(pv) {
					diffs = append(diffs, AttributeDiff{SpanID: spanID, Key: k, BlockpackValue: "", ParquetValue: formatValue(pv, k)})
				}
				continue
			}
			if !cok {
				if !isDefaultValue(cv) {
					diffs = append(diffs, AttributeDiff{SpanID: spanID, Key: k, BlockpackValue: formatValue(cv, k), ParquetValue: ""})
				}
				continue
			}
			if valuesEqual(cv, pv, k) {
				continue
			}
			diffs = append(diffs, AttributeDiff{
				SpanID:         spanID,
				Key:            k,
				BlockpackValue: formatValue(cv, k),
				ParquetValue:   formatValue(pv, k),
			})
		}
	}

	for spanID := range blockpackSpans {
		if _, ok := parquetSpans[spanID]; !ok {
			extra = append(extra, spanID)
		}
	}

	sort.Strings(missing)
	sort.Strings(extra)

	return missing, extra, diffs
}

func valuesEqual(a, b valueCell, columnName string) bool {
	if a.typ == b.typ {
		switch a.typ {
		case blockpack.ColumnTypeString:
			return a.str == b.str
		case blockpack.ColumnTypeInt64:
			return a.i64 == b.i64
		case blockpack.ColumnTypeUint64:
			return a.u64 == b.u64
		case blockpack.ColumnTypeBool:
			return a.b == b.b
		case blockpack.ColumnTypeFloat64:
			return a.f64 == b.f64
		case blockpack.ColumnTypeBytes:
			if a.hasArray || b.hasArray {
				return arraysEqual(a, b)
			}
			return bytesEqual(a.bytes, b.bytes)
		default:
			return false
		}
	}

	if a.hasArray || b.hasArray {
		return false
	}

	if a.typ == blockpack.ColumnTypeString {
		if ok, match := matchStringAgainstValue(a.str, b); ok {
			return match
		}
	}
	if b.typ == blockpack.ColumnTypeString {
		if ok, match := matchStringAgainstValue(b.str, a); ok {
			return match
		}
	}

	return formatValue(a, columnName) == formatValue(b, columnName)
}

func arraysEqual(a, b valueCell) bool {
	if !a.hasArray || !b.hasArray {
		return false
	}
	if len(a.array) != len(b.array) {
		return false
	}
	for i := range a.array {
		if !arrayValueEqual(a.array[i], b.array[i]) {
			return false
		}
	}
	return true
}

func arrayValueEqual(a, b blockpackio.ArrayValue) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case blockpackio.ArrayTypeString:
		return a.Str == b.Str
	case blockpackio.ArrayTypeInt64, blockpackio.ArrayTypeDuration:
		return a.Int == b.Int
	case blockpackio.ArrayTypeFloat64:
		return a.Float == b.Float
	case blockpackio.ArrayTypeBool:
		return a.Bool == b.Bool
	case blockpackio.ArrayTypeBytes:
		return bytesEqual(a.Bytes, b.Bytes)
	default:
		return false
	}
}

func matchStringAgainstValue(input string, val valueCell) (bool, bool) {
	switch val.typ {
	case blockpack.ColumnTypeBool:
		if parsed, err := strconv.ParseBool(input); err == nil {
			return true, parsed == val.b
		}
	case blockpack.ColumnTypeInt64, blockpack.ColumnTypeUint64, blockpack.ColumnTypeFloat64:
		if num, ok := parseNumeric(input); ok {
			return true, numericEqual(num, val)
		}
	case blockpack.ColumnTypeBytes:
		if printable, ok := bytesToPrintableString(val.bytes); ok {
			return true, printable == input
		}
	}
	return false, false
}

func numericEqual(num float64, val valueCell) bool {
	switch val.typ {
	case blockpack.ColumnTypeInt64:
		return num == float64(val.i64)
	case blockpack.ColumnTypeUint64:
		return num == float64(val.u64)
	case blockpack.ColumnTypeFloat64:
		return num == val.f64
	default:
		return false
	}
}

func parseNumeric(input string) (float64, bool) {
	if input == "" {
		return 0, false
	}
	if i, err := strconv.ParseInt(input, 10, 64); err == nil {
		return float64(i), true
	}
	if u, err := strconv.ParseUint(input, 10, 64); err == nil {
		return float64(u), true
	}
	if f, err := strconv.ParseFloat(input, 64); err == nil {
		return f, true
	}
	return 0, false
}

func isDefaultValue(val valueCell) bool {
	switch val.typ {
	case blockpack.ColumnTypeString:
		return val.str == ""
	case blockpack.ColumnTypeInt64:
		return val.i64 == 0
	case blockpack.ColumnTypeUint64:
		return val.u64 == 0
	case blockpack.ColumnTypeBool:
		return !val.b
	case blockpack.ColumnTypeFloat64:
		return val.f64 == 0
	case blockpack.ColumnTypeBytes:
		if val.hasArray {
			return len(val.array) == 0
		}
		return len(val.bytes) == 0
	default:
		return false
	}
}

func formatValue(val valueCell, columnName string) string {
	switch val.typ {
	case blockpack.ColumnTypeString:
		return val.str
	case blockpack.ColumnTypeInt64:
		return fmt.Sprintf("%d", val.i64)
	case blockpack.ColumnTypeUint64:
		return fmt.Sprintf("%d", val.u64)
	case blockpack.ColumnTypeBool:
		return fmt.Sprintf("%t", val.b)
	case blockpack.ColumnTypeFloat64:
		return fmt.Sprintf("%g", val.f64)
	case blockpack.ColumnTypeBytes:
		if val.hasArray {
			return formatArrayValues(val.array)
		}
		return formatBytesValue(val.bytes)
	default:
		return ""
	}
}

func valueKey(val valueCell) string {
	if val.typ == blockpack.ColumnTypeBytes {
		return fmt.Sprintf("%x", val.bytes)
	}
	return formatValue(val, "")
}

func trimStringList(items []string, max int) []string {
	if len(items) <= max {
		return items
	}
	return items[:max]
}

func trimDiffs(diffs []AttributeDiff, max int) []AttributeDiff {
	if len(diffs) <= max {
		return diffs
	}
	return diffs[:max]
}

func CompareMarkdownSummary(result *CompareResult) string {
	if result == nil {
		return ""
	}
	notes := "None"
	if len(result.Notes) > 0 {
		notes = joinStrings(result.Notes, "; ")
	}
	return fmt.Sprintf(`# Parquet vs Blockpack Comparison

## Span counts
- Parquet: %d
- Blockpack: %d

## Trace counts
- Parquet: %d
- Blockpack: %d

## Differences
- Missing spans: %d
- Extra spans: %d
- Attribute diffs: %d
- Match: %t

## Notes
%s
`,
		result.SpanCountParquet,
		result.SpanCountBlockpack,
		result.TraceCountParquet,
		result.TraceCountBlockpack,
		result.MissingSpanCount,
		result.ExtraSpanCount,
		result.AttributeDiffCount,
		result.Match,
		notes,
	)
}

func formatArrayValues(values []blockpackio.ArrayValue) string {
	if len(values) == 0 {
		return "[]"
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, formatArrayValue(v))
	}
	return "[" + joinStrings(out, ", ") + "]"
}

func formatArrayValue(val blockpackio.ArrayValue) string {
	switch val.Type {
	case blockpackio.ArrayTypeString:
		return strconv.Quote(val.Str)
	case blockpackio.ArrayTypeInt64, blockpackio.ArrayTypeDuration:
		return fmt.Sprintf("%d", val.Int)
	case blockpackio.ArrayTypeFloat64:
		return fmt.Sprintf("%g", val.Float)
	case blockpackio.ArrayTypeBool:
		return fmt.Sprintf("%t", val.Bool)
	case blockpackio.ArrayTypeBytes:
		return formatBytesValue(val.Bytes)
	default:
		return ""
	}
}

func joinStrings(values []string, sep string) string {
	if len(values) == 0 {
		return ""
	}
	out := values[0]
	for i := 1; i < len(values); i++ {
		out += sep + values[i]
	}
	return out
}

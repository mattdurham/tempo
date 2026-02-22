// Package spanconv provides shared span reconstruction from blockpack block columns.
// It is used by both compaction and trace lookup to decode block rows into complete
// OTLP spans with all fields (events, links, dropped counts, schema URLs, and scope
// attributes).
package spanconv

import (
	"strings"

	"github.com/grafana/tempo/pkg/util"
	"github.com/mattdurham/blockpack/internal/blockio/reader"
	ondisk "github.com/mattdurham/blockpack/internal/types"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// SpanData holds the complete decoded output of a single block row.
type SpanData struct {
	Span           *tracev1.Span
	Resource       *resourcev1.Resource
	Scope          *commonv1.InstrumentationScope
	ResourceSchema string
	ScopeSchema    string
	ResourceAttrs  []*commonv1.KeyValue
}

// Column name constants — avoid magic strings throughout reconstruction.
const (
	colTraceID       = "trace:id"
	colSpanID        = "span:id"
	colSpanParentID  = "span:parent_id"
	colSpanName      = "span:name"
	colSpanKind      = "span:kind"
	colSpanStatus    = "span:status"
	colSpanStatusMsg = "span:status_message"
	colSpanStart     = "span:start"
	colSpanEnd       = "span:end"

	// H1: TraceState
	colSpanTraceState = "span:trace_state"

	// H2: Dropped counts
	colSpanDroppedAttrs  = "span:dropped_attributes_count"
	colSpanDroppedEvents = "span:dropped_events_count"
	colSpanDroppedLinks  = "span:dropped_links_count"
	colResourceDropped   = "resource:dropped_attributes_count"

	colResourceSchema = "resource:schema_url"
	colScopeSchema    = "scope:schema_url"

	// C3: Correct scope/instrumentation column names (NOT "scope.name" / "scope.version")
	// The writer uses EncodeStringArray and stores in bytes columns under "instrumentation:" prefix.
	colInstrumentationName    = "instrumentation:name"
	colInstrumentationVersion = "instrumentation:version"
	colInstrumentationDropped = "instrumentation:dropped_attributes_count" // H3

	// C1: Event columns (each row = all events for one span, array-encoded)
	colEventName    = "event:name"
	colEventTime    = "event:time_since_start"
	colEventDropped = "event:dropped_attributes_count"

	// C2: Link columns (each row = all links for one span, array-encoded)
	colLinkTraceID = "link:trace_id"
	colLinkSpanID  = "link:span_id"
	colLinkState   = "link:trace_state"
	colLinkDropped = "link:dropped_attributes_count"

	prefixSpanAttr        = "span."
	prefixResource        = "resource."
	prefixEvent           = "event."           // C1: event attribute columns
	prefixLink            = "link."            // C2: link attribute columns
	prefixInstrumentation = "instrumentation." // H3: scope attribute columns
)

// ReconstructSpan reconstructs all data needed to call Writer.AddSpan
// from a single row (rowIdx) of a decoded Block.
func ReconstructSpan(block *reader.Block, rowIdx int) (*SpanData, error) {
	span := &tracev1.Span{}

	if traceID, ok := getBytesCol(block, colTraceID, rowIdx); ok {
		span.TraceId = traceID
	}

	if spanID, ok := getBytesCol(block, colSpanID, rowIdx); ok {
		span.SpanId = spanID
	}

	if parentID, ok := getBytesCol(block, colSpanParentID, rowIdx); ok {
		span.ParentSpanId = parentID
	}

	if name, ok := getStringCol(block, colSpanName, rowIdx); ok {
		span.Name = name
	}

	if kind, ok := getInt64Col(block, colSpanKind, rowIdx); ok {
		span.Kind = tracev1.Span_SpanKind(kind) //nolint:gosec // SpanKind enum values fit in int32
	}

	if start, ok := getUint64Col(block, colSpanStart, rowIdx); ok {
		span.StartTimeUnixNano = start
	}

	if end, ok := getUint64Col(block, colSpanEnd, rowIdx); ok {
		span.EndTimeUnixNano = end
	}

	if status, ok := getInt64Col(block, colSpanStatus, rowIdx); ok {
		msg, _ := getStringCol(block, colSpanStatusMsg, rowIdx)
		span.Status = &tracev1.Status{
			Code:    tracev1.Status_StatusCode(status), //nolint:gosec // StatusCode enum values fit in int32
			Message: msg,
		}
	}

	// H1: TraceState
	if ts, ok := getStringCol(block, colSpanTraceState, rowIdx); ok {
		span.TraceState = ts
	}

	// H2: Span dropped counts
	if v, ok := getUint64Col(block, colSpanDroppedAttrs, rowIdx); ok {
		span.DroppedAttributesCount = uint32(v) //nolint:gosec
	}

	if v, ok := getUint64Col(block, colSpanDroppedEvents, rowIdx); ok {
		span.DroppedEventsCount = uint32(v) //nolint:gosec
	}

	if v, ok := getUint64Col(block, colSpanDroppedLinks, rowIdx); ok {
		span.DroppedLinksCount = uint32(v) //nolint:gosec
	}

	// C1: Reconstruct span events
	span.Events = reconstructSpanEvents(block, rowIdx, span.StartTimeUnixNano)

	// C2: Reconstruct span links
	span.Links = reconstructSpanLinks(block, rowIdx)

	resourceAttrs := reconstructAttrsFromPrefix(block, rowIdx, prefixResource)
	rawResource := &resourcev1.Resource{Attributes: resourceAttrs}

	// H2: Resource dropped attributes count
	if v, ok := getUint64Col(block, colResourceDropped, rowIdx); ok {
		rawResource.DroppedAttributesCount = uint32(v) //nolint:gosec
	}

	var resourceSchema string
	if s, ok := getStringCol(block, colResourceSchema, rowIdx); ok {
		resourceSchema = s
	}

	span.Attributes = reconstructAttrsFromPrefix(block, rowIdx, prefixSpanAttr)

	// C3: Scope name and version — stored as EncodeStringArray in bytes columns,
	// NOT as plain string columns "scope.name" / "scope.version".
	var scopeName, scopeVersion string
	if nameBytes, ok := getBytesCol(block, colInstrumentationName, rowIdx); ok && len(nameBytes) > 0 {
		if vals, decErr := ondisk.DecodeArray(nameBytes); decErr == nil && len(vals) > 0 {
			scopeName = vals[0].Str
		}
	}

	if verBytes, ok := getBytesCol(block, colInstrumentationVersion, rowIdx); ok && len(verBytes) > 0 {
		if vals, decErr := ondisk.DecodeArray(verBytes); decErr == nil && len(vals) > 0 {
			scopeVersion = vals[0].Str
		}
	}

	rawScope := &commonv1.InstrumentationScope{
		Name:    scopeName,
		Version: scopeVersion,
	}

	// H3: Scope dropped attributes count
	if v, ok := getUint64Col(block, colInstrumentationDropped, rowIdx); ok {
		rawScope.DroppedAttributesCount = uint32(v) //nolint:gosec
	}

	// H3: Scope attributes from instrumentation.{key} columns
	rawScope.Attributes = reconstructAttrsFromArrayPrefix(block, rowIdx, prefixInstrumentation)

	var scopeSchema string
	if s, ok := getStringCol(block, colScopeSchema, rowIdx); ok {
		scopeSchema = s
	}

	return &SpanData{
		Span:           span,
		ResourceAttrs:  resourceAttrs,
		Resource:       rawResource,
		ResourceSchema: resourceSchema,
		Scope:          rawScope,
		ScopeSchema:    scopeSchema,
	}, nil
}

// reconstructAttrsFromPrefix collects all columns with the given prefix and
// builds a list of KeyValue attributes for the given row.
// The attribute key is the column name with the prefix stripped.
func reconstructAttrsFromPrefix(
	block *reader.Block,
	rowIdx int,
	prefix string,
) []*commonv1.KeyValue {
	cols := block.Columns()
	attrs := make([]*commonv1.KeyValue, 0)

	for name, col := range cols {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		key := strings.TrimPrefix(name, prefix)
		val := columnToAnyValue(col, rowIdx)

		if val == nil {
			continue
		}

		attrs = append(attrs, &commonv1.KeyValue{
			Key:   key,
			Value: val,
		})
	}

	return attrs
}

// reconstructAttrsFromArrayPrefix collects all columns with the given prefix that store
// AnyValue arrays (encoded via EncodeAnyValueArray) and reconstructs KeyValue attributes.
// Each column holds all attribute values for one span's scope/events/links as a single
// encoded array. For scope attributes, element[0] is the attribute value.
func reconstructAttrsFromArrayPrefix(
	block *reader.Block,
	rowIdx int,
	prefix string,
) []*commonv1.KeyValue {
	cols := block.Columns()
	var attrs []*commonv1.KeyValue

	for name, col := range cols {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		if col == nil || col.Type != reader.ColumnTypeBytes {
			continue
		}

		rawBytes, ok := col.BytesValueView(rowIdx)
		if !ok || len(rawBytes) == 0 {
			continue
		}

		key := strings.TrimPrefix(name, prefix)
		values, decErr := ondisk.DecodeArray(rawBytes)

		if decErr != nil || len(values) == 0 {
			continue
		}

		av := arrayValueToAnyValue(values[0])
		if av == nil {
			continue
		}

		attrs = append(attrs, &commonv1.KeyValue{Key: key, Value: av})
	}

	return attrs
}

// arrayValueToAnyValue converts an ondisk.ArrayValue to an OTLP AnyValue.
// Returns nil for unsupported or unknown types.
func arrayValueToAnyValue(v ondisk.ArrayValue) *commonv1.AnyValue {
	switch v.Type {
	case ondisk.ArrayTypeString:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v.Str}}
	case ondisk.ArrayTypeInt64, ondisk.ArrayTypeDuration:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v.Int}}
	case ondisk.ArrayTypeFloat64:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v.Float}}
	case ondisk.ArrayTypeBool:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: v.Bool}}
	case ondisk.ArrayTypeBytes:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: v.Bytes}}
	default:
		return nil
	}
}

// decodeStrArrayCol reads a bytes column and decodes it as a string array.
// Returns nil if the column is absent, empty, or malformed.
func decodeStrArrayCol(block *reader.Block, colName string, rowIdx int) []string {
	raw, ok := getBytesCol(block, colName, rowIdx)
	if !ok || len(raw) == 0 {
		return nil
	}

	vals, err := ondisk.DecodeArray(raw)
	if err != nil {
		return nil
	}

	result := make([]string, 0, len(vals))
	for _, v := range vals {
		result = append(result, v.Str)
	}

	return result
}

// decodeInt64ArrayCol reads a bytes column and decodes it as an int64 array.
// Returns nil if the column is absent, empty, or malformed.
func decodeInt64ArrayCol(block *reader.Block, colName string, rowIdx int) []int64 {
	raw, ok := getBytesCol(block, colName, rowIdx)
	if !ok || len(raw) == 0 {
		return nil
	}

	vals, err := ondisk.DecodeArray(raw)
	if err != nil {
		return nil
	}

	result := make([]int64, 0, len(vals))
	for _, v := range vals {
		result = append(result, v.Int)
	}

	return result
}

// reconstructSpanEvents reconstructs the Events slice for a span from array columns.
// The writer encodes all event names for one span in event:name (EncodeStringArray),
// all event times in event:time_since_start (EncodeDurationArray), all dropped counts
// in event:dropped_attributes_count (EncodeInt64Array), and per-event attributes in
// event.{key} columns (EncodeAnyValueArray) where element[i] belongs to event[i].
func reconstructSpanEvents(block *reader.Block, rowIdx int, spanStartNano uint64) []*tracev1.Span_Event {
	eventNames := decodeStrArrayCol(block, colEventName, rowIdx)
	if len(eventNames) == 0 {
		return nil
	}

	eventCount := len(eventNames)

	// Decode time offsets: ArrayTypeDuration values (int64, nanoseconds since span start)
	timeSinceStart := decodeInt64ArrayCol(block, colEventTime, rowIdx)

	// Decode per-event dropped attribute counts: ArrayTypeInt64
	evtDroppedCounts := decodeInt64ArrayCol(block, colEventDropped, rowIdx)

	// Collect per-event attribute arrays from event.{key} columns.
	// Format limitation: the writer builds sparse per-attribute arrays — only values for
	// events that HAVE the attribute are stored. When events have heterogeneous attributes
	// (different events have different attribute keys), attrVals[i] may not belong to
	// event[i]. Correct positional reconstruction is not possible without format changes.
	// This is best-effort: correct when all events share the same attribute keys.
	cols := block.Columns()
	eventAttrs := make([][]*commonv1.KeyValue, eventCount)

	for name, col := range cols {
		if !strings.HasPrefix(name, prefixEvent) {
			continue
		}

		if col == nil || col.Type != reader.ColumnTypeBytes {
			continue
		}

		rawBytes, ok := col.BytesValueView(rowIdx)
		if !ok || len(rawBytes) == 0 {
			continue
		}

		attrKey := strings.TrimPrefix(name, prefixEvent)
		attrVals, attrErr := ondisk.DecodeArray(rawBytes)

		if attrErr != nil {
			continue
		}

		for i := 0; i < len(attrVals) && i < eventCount; i++ {
			av := arrayValueToAnyValue(attrVals[i])
			if av != nil {
				eventAttrs[i] = append(eventAttrs[i], &commonv1.KeyValue{Key: attrKey, Value: av})
			}
		}
	}

	events := make([]*tracev1.Span_Event, 0, eventCount)
	for i, name := range eventNames {
		evt := &tracev1.Span_Event{Name: name}

		// Reconstruct absolute timestamp: span start + time_since_start delta
		if i < len(timeSinceStart) && timeSinceStart[i] >= 0 {
			evt.TimeUnixNano = spanStartNano + uint64(timeSinceStart[i]) //nolint:gosec
		}

		if i < len(evtDroppedCounts) && evtDroppedCounts[i] > 0 {
			evt.DroppedAttributesCount = uint32(evtDroppedCounts[i]) //nolint:gosec
		}

		evt.Attributes = eventAttrs[i]
		events = append(events, evt)
	}

	return events
}

// reconstructSpanLinks reconstructs the Links slice for a span from array columns.
// The writer stores link:trace_id via util.TraceIDToHexString (strips leading zeros)
// and link:span_id via util.SpanIDToHexString (pads to 16 hex chars).
// Decode back using util.HexStringToTraceID / util.HexStringToSpanID which handle
// the variable-length encoding correctly.
//
// Format limitation: link:trace_id and link:span_id are only written for links with
// non-empty values. link:trace_state is always written. We use link:trace_state as
// the authoritative link count. If a link has an empty trace ID, positional alignment
// of trace IDs to links will be incorrect for that link — this is a known limitation
// of the sparse array encoding in the blockpack format.
func reconstructSpanLinks(block *reader.Block, rowIdx int) []*tracev1.Span_Link {
	// link:trace_state is unconditionally written for every link, making it the
	// authoritative source for link count. link:trace_id and link:span_id are
	// conditionally written (only when non-empty) which can cause length mismatches.
	traceStates := decodeStrArrayCol(block, colLinkState, rowIdx)
	traceIDHexes := decodeStrArrayCol(block, colLinkTraceID, rowIdx)

	linkCount := len(traceStates)
	if linkCount == 0 {
		linkCount = len(traceIDHexes)
	}

	if linkCount == 0 {
		return nil
	}

	spanIDHexes := decodeStrArrayCol(block, colLinkSpanID, rowIdx)
	linkDroppedCounts := decodeInt64ArrayCol(block, colLinkDropped, rowIdx)

	// Collect per-link attribute arrays from link.{key} columns.
	cols := block.Columns()
	linkAttrs := make([][]*commonv1.KeyValue, linkCount)

	for name, col := range cols {
		if !strings.HasPrefix(name, prefixLink) {
			continue
		}

		if col == nil || col.Type != reader.ColumnTypeBytes {
			continue
		}

		rawBytes, ok := col.BytesValueView(rowIdx)
		if !ok || len(rawBytes) == 0 {
			continue
		}

		attrKey := strings.TrimPrefix(name, prefixLink)
		attrVals, attrErr := ondisk.DecodeArray(rawBytes)

		if attrErr != nil {
			continue
		}

		for i := 0; i < len(attrVals) && i < linkCount; i++ {
			av := arrayValueToAnyValue(attrVals[i])
			if av != nil {
				linkAttrs[i] = append(linkAttrs[i], &commonv1.KeyValue{Key: attrKey, Value: av})
			}
		}
	}

	links := make([]*tracev1.Span_Link, 0, linkCount)
	for i := 0; i < linkCount; i++ {
		link := &tracev1.Span_Link{}

		// Decode hex trace ID back to raw bytes.
		// util.TraceIDToHexString strips leading zeros — use util.HexStringToTraceID to invert.
		// traceIDHexes may be shorter than linkCount when some links have empty trace IDs.
		if i < len(traceIDHexes) {
			if decoded, hexErr := util.HexStringToTraceID(traceIDHexes[i]); hexErr == nil {
				link.TraceId = decoded
			}
		}

		if i < len(spanIDHexes) {
			// util.SpanIDToHexString pads to 16 chars — use util.HexStringToSpanID to invert.
			if decoded, hexErr := util.HexStringToSpanID(spanIDHexes[i]); hexErr == nil {
				link.SpanId = decoded
			}
		}

		if i < len(traceStates) {
			link.TraceState = traceStates[i]
		}

		if i < len(linkDroppedCounts) && linkDroppedCounts[i] > 0 {
			link.DroppedAttributesCount = uint32(linkDroppedCounts[i]) //nolint:gosec
		}

		link.Attributes = linkAttrs[i]
		links = append(links, link)
	}

	return links
}

// columnToAnyValue reads the value at rowIdx from col and wraps it in an
// OTLP AnyValue. Returns nil if the column has no value at rowIdx.
func columnToAnyValue(col *reader.Column, rowIdx int) *commonv1.AnyValue {
	if col == nil {
		return nil
	}

	switch col.Type {
	case reader.ColumnTypeString:
		if v, ok := col.StringValue(rowIdx); ok {
			return &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: v},
			}
		}
	case reader.ColumnTypeInt64:
		if v, ok := col.Int64Value(rowIdx); ok {
			return &commonv1.AnyValue{
				Value: &commonv1.AnyValue_IntValue{IntValue: v},
			}
		}
	case reader.ColumnTypeUint64:
		if v, ok := col.Uint64Value(rowIdx); ok {
			return &commonv1.AnyValue{
				Value: &commonv1.AnyValue_IntValue{IntValue: int64(v)}, //nolint:gosec
			}
		}
	case reader.ColumnTypeBool:
		if v, ok := col.BoolValue(rowIdx); ok {
			return &commonv1.AnyValue{
				Value: &commonv1.AnyValue_BoolValue{BoolValue: v},
			}
		}
	case reader.ColumnTypeFloat64:
		if v, ok := col.Float64Value(rowIdx); ok {
			return &commonv1.AnyValue{
				Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v},
			}
		}
	case reader.ColumnTypeBytes:
		if v, ok := col.BytesValueView(rowIdx); ok {
			cp := make([]byte, len(v))
			copy(cp, v)

			return &commonv1.AnyValue{
				Value: &commonv1.AnyValue_BytesValue{BytesValue: cp},
			}
		}
	}

	return nil
}

// getBytesCol reads a bytes column value at rowIdx, copying the data.
func getBytesCol(block *reader.Block, name string, rowIdx int) ([]byte, bool) {
	col := block.GetColumn(name)
	if col == nil {
		return nil, false
	}

	v, ok := col.BytesValueView(rowIdx)
	if !ok {
		return nil, false
	}

	cp := make([]byte, len(v))
	copy(cp, v)

	return cp, true
}

// getStringCol reads a string column value at rowIdx.
func getStringCol(block *reader.Block, name string, rowIdx int) (string, bool) {
	col := block.GetColumn(name)
	if col == nil {
		return "", false
	}

	return col.StringValue(rowIdx)
}

// getInt64Col reads an int64 column value at rowIdx.
func getInt64Col(block *reader.Block, name string, rowIdx int) (int64, bool) {
	col := block.GetColumn(name)
	if col == nil {
		return 0, false
	}

	return col.Int64Value(rowIdx)
}

// getUint64Col reads a uint64 column value at rowIdx.
func getUint64Col(block *reader.Block, name string, rowIdx int) (uint64, bool) {
	col := block.GetColumn(name)
	if col == nil {
		return 0, false
	}

	return col.Uint64Value(rowIdx)
}

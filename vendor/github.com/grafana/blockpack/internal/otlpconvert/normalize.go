// Package otlpconvert provides utilities for converting between OTLP and blockpack formats.
package otlpconvert

import (
	"fmt"
	"strconv"
	"strings"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

type valueKind int

const (
	valueKindUnknown valueKind = iota
	valueKindString
	valueKindInt
	valueKindBool
	valueKindDouble
	valueKindBytes
	valueKindArray
	valueKindOther
	valueKindMixed
)

// NormalizeTracesForBlockpack coerces mixed attribute types to a consistent type per key.
func NormalizeTracesForBlockpack(traces []*tracev1.TracesData) ([]*tracev1.TracesData, error) {
	kinds := collectAttributeKinds(traces)
	normalized := make([]*tracev1.TracesData, len(traces))

	for i, td := range traces {
		resourceSpans := make([]*tracev1.ResourceSpans, len(td.ResourceSpans))
		for rIdx, rs := range td.ResourceSpans {
			resource := &resourcev1.Resource{
				Attributes:             normalizeAttributes(rs.GetResource().GetAttributes(), "resource.", kinds),
				DroppedAttributesCount: rs.GetResource().GetDroppedAttributesCount(),
			}
			scopeSpans := make([]*tracev1.ScopeSpans, len(rs.ScopeSpans))
			for sIdx, ss := range rs.ScopeSpans {
				spans := make([]*tracev1.Span, len(ss.Spans))
				scope := ss.GetScope()
				var normalizedScope *commonv1.InstrumentationScope
				if scope != nil {
					normalizedScope = &commonv1.InstrumentationScope{
						Name:                   scope.GetName(),
						Version:                scope.GetVersion(),
						Attributes:             normalizeAttributes(scope.GetAttributes(), "instrumentation.", kinds),
						DroppedAttributesCount: scope.GetDroppedAttributesCount(),
					}
				}
				for spIdx, span := range ss.Spans {
					// Don't copy resource attributes to span attributes - the blockpack format
					// preserves the resource/span distinction properly
					spanAttrs := span.Attributes

					events := make([]*tracev1.Span_Event, len(span.Events))
					for eIdx, event := range span.Events {
						if event == nil {
							continue
						}
						events[eIdx] = &tracev1.Span_Event{
							Name:                   event.GetName(),
							TimeUnixNano:           event.GetTimeUnixNano(),
							Attributes:             normalizeAttributes(event.GetAttributes(), "event.", kinds),
							DroppedAttributesCount: event.GetDroppedAttributesCount(),
						}
					}
					links := make([]*tracev1.Span_Link, len(span.Links))
					for lIdx, link := range span.Links {
						if link == nil {
							continue
						}
						links[lIdx] = &tracev1.Span_Link{
							TraceId:                link.GetTraceId(),
							SpanId:                 link.GetSpanId(),
							TraceState:             link.GetTraceState(),
							Attributes:             normalizeAttributes(link.GetAttributes(), "link.", kinds),
							DroppedAttributesCount: link.GetDroppedAttributesCount(),
						}
					}
					var status *tracev1.Status
					if span.GetStatus() != nil {
						status = &tracev1.Status{
							Code:    span.GetStatus().GetCode(),
							Message: span.GetStatus().GetMessage(),
						}
					}
					spans[spIdx] = &tracev1.Span{
						TraceId:                span.TraceId,
						SpanId:                 span.SpanId,
						ParentSpanId:           span.ParentSpanId,
						Name:                   span.Name,
						Kind:                   span.Kind,
						TraceState:             span.TraceState,
						StartTimeUnixNano:      span.StartTimeUnixNano,
						EndTimeUnixNano:        span.EndTimeUnixNano,
						Attributes:             normalizeAttributes(spanAttrs, "span.", kinds),
						DroppedAttributesCount: span.GetDroppedAttributesCount(),
						Events:                 events,
						DroppedEventsCount:     span.GetDroppedEventsCount(),
						Links:                  links,
						DroppedLinksCount:      span.GetDroppedLinksCount(),
						Status:                 status,
					}
				}
				scopeSpans[sIdx] = &tracev1.ScopeSpans{
					Scope:     normalizedScope,
					SchemaUrl: ss.SchemaUrl,
					Spans:     spans,
				}
			}
			resourceSpans[rIdx] = &tracev1.ResourceSpans{
				Resource:   resource,
				SchemaUrl:  rs.SchemaUrl,
				ScopeSpans: scopeSpans,
			}
		}
		normalized[i] = &tracev1.TracesData{ResourceSpans: resourceSpans}
	}

	return normalized, nil
}

func collectAttributeKinds(traces []*tracev1.TracesData) map[string]valueKind {
	kinds := make(map[string]valueKind)
	for _, td := range traces {
		for _, rs := range td.ResourceSpans {
			for _, attr := range rs.GetResource().GetAttributes() {
				key := "resource." + attr.Key
				kinds[key] = mergeKind(kinds[key], kindFromAnyValue(attr.Value))
			}
			for _, ss := range rs.ScopeSpans {
				if scope := ss.GetScope(); scope != nil {
					for _, attr := range scope.GetAttributes() {
						key := "instrumentation." + attr.Key
						kinds[key] = mergeKind(kinds[key], kindFromAnyValue(attr.Value))
					}
				}
				for _, span := range ss.Spans {
					for _, attr := range span.Attributes {
						key := "span." + attr.Key
						kinds[key] = mergeKind(kinds[key], kindFromAnyValue(attr.Value))
					}
					for _, event := range span.Events {
						for _, attr := range event.GetAttributes() {
							key := "event." + attr.Key
							kinds[key] = mergeKind(kinds[key], kindFromAnyValue(attr.Value))
						}
					}
					for _, link := range span.Links {
						for _, attr := range link.GetAttributes() {
							key := "link." + attr.Key
							kinds[key] = mergeKind(kinds[key], kindFromAnyValue(attr.Value))
						}
					}
				}
			}
		}
	}
	return kinds
}

func mergeKind(existing, next valueKind) valueKind {
	if existing == valueKindUnknown {
		return next
	}
	if existing == next {
		return existing
	}
	if existing == valueKindMixed {
		return valueKindMixed
	}
	return valueKindMixed
}

func kindFromAnyValue(value *commonv1.AnyValue) valueKind {
	if value == nil {
		return valueKindUnknown
	}
	switch value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return valueKindString
	case *commonv1.AnyValue_IntValue:
		return valueKindInt
	case *commonv1.AnyValue_BoolValue:
		return valueKindBool
	case *commonv1.AnyValue_DoubleValue:
		return valueKindDouble
	case *commonv1.AnyValue_BytesValue:
		return valueKindBytes
	case *commonv1.AnyValue_ArrayValue:
		return valueKindArray
	default:
		return valueKindOther
	}
}

func normalizeAttributes(attrs []*commonv1.KeyValue, prefix string, kinds map[string]valueKind) []*commonv1.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*commonv1.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if attr == nil || attr.Value == nil {
			continue
		}
		key := prefix + attr.Key
		kind := resolvedKind(kinds[key])
		value := normalizeAnyValue(attr.Value, kind)
		if value == nil {
			continue
		}
		out = append(out, &commonv1.KeyValue{
			Key:   attr.Key,
			Value: value,
		})
	}
	return out
}

func resolvedKind(kind valueKind) valueKind {
	switch kind {
	case valueKindString, valueKindInt, valueKindBool, valueKindDouble, valueKindBytes, valueKindArray:
		return kind
	default:
		return valueKindString
	}
}

func normalizeAnyValue(value *commonv1.AnyValue, kind valueKind) *commonv1.AnyValue {
	if value == nil {
		return nil
	}
	switch kind {
	case valueKindInt:
		if v, ok := value.Value.(*commonv1.AnyValue_IntValue); ok {
			return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v.IntValue}}
		}
	case valueKindBool:
		if v, ok := value.Value.(*commonv1.AnyValue_BoolValue); ok {
			return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: v.BoolValue}}
		}
	case valueKindDouble:
		if v, ok := value.Value.(*commonv1.AnyValue_DoubleValue); ok {
			return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v.DoubleValue}}
		}
	case valueKindBytes:
		if v, ok := value.Value.(*commonv1.AnyValue_BytesValue); ok {
			return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: v.BytesValue}}
		}
	case valueKindArray:
		if v, ok := value.Value.(*commonv1.AnyValue_ArrayValue); ok {
			return &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: v.ArrayValue}}
		}
	}

	return &commonv1.AnyValue{
		Value: &commonv1.AnyValue_StringValue{
			StringValue: stringFromAnyValue(value),
		},
	}
}

func stringFromAnyValue(value *commonv1.AnyValue) string {
	if value == nil {
		return ""
	}
	switch v := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return v.StringValue
	case *commonv1.AnyValue_IntValue:
		return strconv.FormatInt(v.IntValue, 10)
	case *commonv1.AnyValue_DoubleValue:
		return strconv.FormatFloat(v.DoubleValue, 'f', -1, 64)
	case *commonv1.AnyValue_BoolValue:
		return strconv.FormatBool(v.BoolValue)
	case *commonv1.AnyValue_BytesValue:
		return fmt.Sprintf("%x", v.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		return stringFromArrayValue(v.ArrayValue)
	case *commonv1.AnyValue_KvlistValue:
		return stringFromKeyValueList(v.KvlistValue)
	default:
		return ""
	}
}

func stringFromArrayValue(value *commonv1.ArrayValue) string {
	if value == nil || len(value.Values) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(value.Values))
	for _, v := range value.Values {
		parts = append(parts, stringFromAnyValue(v))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func stringFromKeyValueList(value *commonv1.KeyValueList) string {
	if value == nil || len(value.Values) == 0 {
		return "{}"
	}
	parts := make([]string, 0, len(value.Values))
	for _, kv := range value.Values {
		if kv == nil {
			continue
		}
		parts = append(parts, kv.Key+"="+stringFromAnyValue(kv.Value))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

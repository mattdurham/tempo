package parquetutil

import (
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// TempoTraceToOTLP converts a Tempo trace to OTLP TracesData format.
// This is useful for integrating with systems that expect OTLP format.
func TempoTraceToOTLP(trace *tempopb.Trace) *tracev1.TracesData {
	return tempoTraceToOTLP(trace)
}

func tempoTraceToOTLP(trace *tempopb.Trace) *tracev1.TracesData {
	if trace == nil {
		return &tracev1.TracesData{}
	}
	resourceSpans := make([]*tracev1.ResourceSpans, 0, len(trace.ResourceSpans))
	for _, rs := range trace.ResourceSpans {
		resourceSpans = append(resourceSpans, tempoResourceSpansToOTLP(rs))
	}
	return &tracev1.TracesData{ResourceSpans: resourceSpans}
}

func tempoResourceSpansToOTLP(rs *tempotrace.ResourceSpans) *tracev1.ResourceSpans {
	if rs == nil {
		return &tracev1.ResourceSpans{}
	}
	scopeSpans := make([]*tracev1.ScopeSpans, 0, len(rs.ScopeSpans))
	for _, ss := range rs.ScopeSpans {
		scopeSpans = append(scopeSpans, tempoScopeSpansToOTLP(ss))
	}
	return &tracev1.ResourceSpans{
		Resource:   tempoResourceToOTLP(rs.Resource),
		ScopeSpans: scopeSpans,
		SchemaUrl:  rs.SchemaUrl,
	}
}

func tempoResourceToOTLP(resource *temporesource.Resource) *resourcev1.Resource {
	if resource == nil {
		return &resourcev1.Resource{}
	}
	return &resourcev1.Resource{
		Attributes:             tempoKeyValuesToOTLP(resource.Attributes),
		DroppedAttributesCount: resource.DroppedAttributesCount,
	}
}

func tempoScopeSpansToOTLP(ss *tempotrace.ScopeSpans) *tracev1.ScopeSpans {
	if ss == nil {
		return &tracev1.ScopeSpans{}
	}
	spans := make([]*tracev1.Span, 0, len(ss.Spans))
	for _, span := range ss.Spans {
		spans = append(spans, tempoSpanToOTLP(span))
	}
	return &tracev1.ScopeSpans{
		Scope:     tempoInstrumentationScopeToOTLP(ss.Scope),
		Spans:     spans,
		SchemaUrl: ss.SchemaUrl,
	}
}

func tempoInstrumentationScopeToOTLP(scope *tempocommon.InstrumentationScope) *commonv1.InstrumentationScope {
	if scope == nil {
		return &commonv1.InstrumentationScope{}
	}
	return &commonv1.InstrumentationScope{
		Name:                   scope.Name,
		Version:                scope.Version,
		Attributes:             tempoKeyValuesToOTLP(scope.Attributes),
		DroppedAttributesCount: scope.DroppedAttributesCount,
	}
}

func tempoSpanToOTLP(span *tempotrace.Span) *tracev1.Span {
	if span == nil {
		return &tracev1.Span{}
	}
	events := make([]*tracev1.Span_Event, 0, len(span.Events))
	for _, event := range span.Events {
		events = append(events, tempoEventToOTLP(event))
	}
	links := make([]*tracev1.Span_Link, 0, len(span.Links))
	for _, link := range span.Links {
		links = append(links, tempoLinkToOTLP(link))
	}
	return &tracev1.Span{
		TraceId:                span.TraceId,
		SpanId:                 span.SpanId,
		TraceState:             span.TraceState,
		ParentSpanId:           span.ParentSpanId,
		Name:                   span.Name,
		Kind:                   tracev1.Span_SpanKind(span.Kind),
		StartTimeUnixNano:      span.StartTimeUnixNano,
		EndTimeUnixNano:        span.EndTimeUnixNano,
		Attributes:             tempoKeyValuesToOTLP(span.Attributes),
		DroppedAttributesCount: span.DroppedAttributesCount,
		Events:                 events,
		DroppedEventsCount:     span.DroppedEventsCount,
		Links:                  links,
		DroppedLinksCount:      span.DroppedLinksCount,
		Status: &tracev1.Status{
			Message: span.Status.Message,
			Code:    tracev1.Status_StatusCode(span.Status.Code),
		},
	}
}

func tempoEventToOTLP(event *tempotrace.Span_Event) *tracev1.Span_Event {
	if event == nil {
		return &tracev1.Span_Event{}
	}
	return &tracev1.Span_Event{
		TimeUnixNano:           event.TimeUnixNano,
		Name:                   event.Name,
		Attributes:             tempoKeyValuesToOTLP(event.Attributes),
		DroppedAttributesCount: event.DroppedAttributesCount,
	}
}

func tempoLinkToOTLP(link *tempotrace.Span_Link) *tracev1.Span_Link {
	if link == nil {
		return &tracev1.Span_Link{}
	}
	return &tracev1.Span_Link{
		TraceId:                link.TraceId,
		SpanId:                 link.SpanId,
		TraceState:             link.TraceState,
		Attributes:             tempoKeyValuesToOTLP(link.Attributes),
		DroppedAttributesCount: link.DroppedAttributesCount,
	}
}

func tempoKeyValuesToOTLP(attrs []*tempocommon.KeyValue) []*commonv1.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*commonv1.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		out = append(out, &commonv1.KeyValue{
			Key:   attr.Key,
			Value: tempoAnyValueToOTLP(attr.Value),
		})
	}
	return out
}

func tempoAnyValueToOTLP(value *tempocommon.AnyValue) *commonv1.AnyValue {
	if value == nil {
		return &commonv1.AnyValue{}
	}
	switch v := value.Value.(type) {
	case *tempocommon.AnyValue_StringValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v.StringValue}}
	case *tempocommon.AnyValue_BoolValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: v.BoolValue}}
	case *tempocommon.AnyValue_IntValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v.IntValue}}
	case *tempocommon.AnyValue_DoubleValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v.DoubleValue}}
	case *tempocommon.AnyValue_BytesValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: v.BytesValue}}
	case *tempocommon.AnyValue_ArrayValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: tempoArrayValueToOTLP(v.ArrayValue)}}
	case *tempocommon.AnyValue_KvlistValue:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: tempoKvListToOTLP(v.KvlistValue)}}
	default:
		return &commonv1.AnyValue{}
	}
}

func tempoArrayValueToOTLP(array *tempocommon.ArrayValue) *commonv1.ArrayValue {
	if array == nil {
		return &commonv1.ArrayValue{}
	}
	values := make([]*commonv1.AnyValue, 0, len(array.Values))
	for _, value := range array.Values {
		values = append(values, tempoAnyValueToOTLP(value))
	}
	return &commonv1.ArrayValue{Values: values}
}

func tempoKvListToOTLP(list *tempocommon.KeyValueList) *commonv1.KeyValueList {
	if list == nil {
		return &commonv1.KeyValueList{}
	}
	values := make([]*commonv1.KeyValue, 0, len(list.Values))
	for _, value := range list.Values {
		values = append(values, &commonv1.KeyValue{
			Key:   value.Key,
			Value: tempoAnyValueToOTLP(value.Value),
		})
	}
	return &commonv1.KeyValueList{Values: values}
}

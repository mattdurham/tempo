package otlpconvert

import (
	"fmt"

	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/encoding/common"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// BuildTempoTraces groups OTLP spans by trace ID into Tempo trace objects.
func BuildTempoTraces(traces []*tracev1.TracesData) ([]*tempopb.Trace, []common.ID, error) {
	builders := make(map[string]*traceBuilder)
	order := make([]string, 0)

	for _, td := range traces {
		for _, resourceSpans := range td.ResourceSpans {
			resourceKey, err := makeResourceKey(resourceSpans.Resource, resourceSpans.SchemaUrl)
			if err != nil {
				return nil, nil, err
			}
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				scopeKey, err := makeScopeKey(scopeSpans.Scope, scopeSpans.SchemaUrl)
				if err != nil {
					return nil, nil, err
				}
				for _, span := range scopeSpans.Spans {
					if len(span.TraceId) == 0 {
						return nil, nil, fmt.Errorf("span %q missing trace id", span.Name)
					}
					traceKey := string(span.TraceId)
					builder, ok := builders[traceKey]
					if !ok {
						builder = newTraceBuilder()
						builders[traceKey] = builder
						order = append(order, traceKey)
					}
					builder.addSpan(
						resourceKey,
						resourceSpans.Resource,
						resourceSpans.SchemaUrl,
						scopeKey,
						scopeSpans.Scope,
						scopeSpans.SchemaUrl,
						span,
					)
				}
			}
		}
	}

	if len(order) == 0 {
		return nil, nil, fmt.Errorf("no spans found")
	}

	tempoTraces := make([]*tempopb.Trace, 0, len(order))
	traceIDs := make([]common.ID, 0, len(order))
	for _, traceKey := range order {
		builder := builders[traceKey]
		if builder == nil {
			continue
		}
		resourceSpans := builder.build()
		tempoTraces = append(tempoTraces, &tempopb.Trace{ResourceSpans: resourceSpans})
		traceIDs = append(traceIDs, common.ID([]byte(traceKey)))
	}

	return tempoTraces, traceIDs, nil
}

type traceBuilder struct {
	resources    map[string]*resourceBuilder
	resourceKeys []string
}

type resourceBuilder struct {
	resource  *temporesource.Resource
	schemaURL string
	scopes    map[string]*scopeBuilder
	scopeKeys []string
}

type scopeBuilder struct {
	scope     *tempocommon.InstrumentationScope
	schemaURL string
	spans     []*tempotrace.Span
}

func newTraceBuilder() *traceBuilder {
	return &traceBuilder{
		resources: make(map[string]*resourceBuilder),
	}
}

func (b *traceBuilder) addSpan(
	resourceKey string,
	resource *resourcev1.Resource,
	resourceSchema string,
	scopeKey string,
	scope *commonv1.InstrumentationScope,
	scopeSchema string,
	span *tracev1.Span,
) {
	resBuilder, ok := b.resources[resourceKey]
	if !ok {
		resBuilder = &resourceBuilder{
			resource:  convertOTLPResource(resource),
			schemaURL: resourceSchema,
			scopes:    make(map[string]*scopeBuilder),
		}
		b.resources[resourceKey] = resBuilder
		b.resourceKeys = append(b.resourceKeys, resourceKey)
	}

	scBuilder, ok := resBuilder.scopes[scopeKey]
	if !ok {
		scBuilder = &scopeBuilder{
			scope:     convertOTLPScope(scope),
			schemaURL: scopeSchema,
		}
		resBuilder.scopes[scopeKey] = scBuilder
		resBuilder.scopeKeys = append(resBuilder.scopeKeys, scopeKey)
	}

	scBuilder.spans = append(scBuilder.spans, convertOTLPSpan(span))
}

func (b *traceBuilder) build() []*tempotrace.ResourceSpans {
	if b == nil {
		return nil
	}
	resourceSpans := make([]*tempotrace.ResourceSpans, 0, len(b.resourceKeys))
	for _, resourceKey := range b.resourceKeys {
		resourceBuilder := b.resources[resourceKey]
		if resourceBuilder == nil {
			continue // Skip nil resource builder
		}
		scopeSpans := make([]*tempotrace.ScopeSpans, 0, len(resourceBuilder.scopeKeys))
		for _, scopeKey := range resourceBuilder.scopeKeys {
			scopeBuilder := resourceBuilder.scopes[scopeKey]
			if scopeBuilder == nil {
				continue // Skip nil scope builder
			}
			scopeSpans = append(scopeSpans, &tempotrace.ScopeSpans{
				Scope:     scopeBuilder.scope,
				SchemaUrl: scopeBuilder.schemaURL,
				Spans:     scopeBuilder.spans,
			})
		}

		resourceSpans = append(resourceSpans, &tempotrace.ResourceSpans{
			Resource:   resourceBuilder.resource,
			SchemaUrl:  resourceBuilder.schemaURL,
			ScopeSpans: scopeSpans,
		})
	}
	return resourceSpans
}

func makeResourceKey(resource *resourcev1.Resource, schemaURL string) (string, error) {
	if resource == nil {
		return "nil|" + schemaURL, nil
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(resource)
	if err != nil {
		return "", err
	}
	return string(data) + "|" + schemaURL, nil
}

func makeScopeKey(scope *commonv1.InstrumentationScope, schemaURL string) (string, error) {
	if scope == nil {
		return "nil|" + schemaURL, nil
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(scope)
	if err != nil {
		return "", err
	}
	return string(data) + "|" + schemaURL, nil
}

func convertOTLPResource(resource *resourcev1.Resource) *temporesource.Resource {
	if resource == nil {
		return nil
	}
	attrs := make([]*tempocommon.KeyValue, len(resource.Attributes))
	for i, attr := range resource.Attributes {
		attrs[i] = convertOTLPKeyValue(attr)
	}
	return &temporesource.Resource{
		Attributes:             attrs,
		DroppedAttributesCount: resource.DroppedAttributesCount,
	}
}

func convertOTLPScope(scope *commonv1.InstrumentationScope) *tempocommon.InstrumentationScope {
	if scope == nil {
		return nil
	}
	attrs := make([]*tempocommon.KeyValue, len(scope.Attributes))
	for i, attr := range scope.Attributes {
		attrs[i] = convertOTLPKeyValue(attr)
	}
	return &tempocommon.InstrumentationScope{
		Name:                   scope.Name,
		Version:                scope.Version,
		Attributes:             attrs,
		DroppedAttributesCount: scope.DroppedAttributesCount,
	}
}

func convertOTLPSpan(span *tracev1.Span) *tempotrace.Span {
	if span == nil {
		return nil
	}
	attrs := make([]*tempocommon.KeyValue, len(span.Attributes))
	for i, attr := range span.Attributes {
		attrs[i] = convertOTLPKeyValue(attr)
	}
	events := make([]*tempotrace.Span_Event, len(span.Events))
	for i, event := range span.Events {
		events[i] = convertOTLPEvent(event)
	}
	links := make([]*tempotrace.Span_Link, len(span.Links))
	for i, link := range span.Links {
		links[i] = convertOTLPLink(link)
	}
	return &tempotrace.Span{
		TraceId:                span.TraceId,
		SpanId:                 span.SpanId,
		TraceState:             span.TraceState,
		ParentSpanId:           span.ParentSpanId,
		Flags:                  span.Flags,
		Name:                   span.Name,
		Kind:                   tempotrace.Span_SpanKind(span.Kind),
		StartTimeUnixNano:      span.StartTimeUnixNano,
		EndTimeUnixNano:        span.EndTimeUnixNano,
		Attributes:             attrs,
		DroppedAttributesCount: span.DroppedAttributesCount,
		Events:                 events,
		DroppedEventsCount:     span.DroppedEventsCount,
		Links:                  links,
		DroppedLinksCount:      span.DroppedLinksCount,
		Status: &tempotrace.Status{
			Message: span.Status.GetMessage(),
			Code:    tempotrace.Status_StatusCode(span.Status.GetCode()),
		},
	}
}

func convertOTLPEvent(event *tracev1.Span_Event) *tempotrace.Span_Event {
	if event == nil {
		return nil
	}
	attrs := make([]*tempocommon.KeyValue, len(event.Attributes))
	for i, attr := range event.Attributes {
		attrs[i] = convertOTLPKeyValue(attr)
	}
	return &tempotrace.Span_Event{
		TimeUnixNano:           event.TimeUnixNano,
		Name:                   event.Name,
		Attributes:             attrs,
		DroppedAttributesCount: event.DroppedAttributesCount,
	}
}

func convertOTLPLink(link *tracev1.Span_Link) *tempotrace.Span_Link {
	if link == nil {
		return nil
	}
	attrs := make([]*tempocommon.KeyValue, len(link.Attributes))
	for i, attr := range link.Attributes {
		attrs[i] = convertOTLPKeyValue(attr)
	}
	return &tempotrace.Span_Link{
		TraceId:                link.TraceId,
		SpanId:                 link.SpanId,
		TraceState:             link.TraceState,
		Attributes:             attrs,
		DroppedAttributesCount: link.DroppedAttributesCount,
		Flags:                  link.Flags,
	}
}

func convertOTLPKeyValue(kv *commonv1.KeyValue) *tempocommon.KeyValue {
	if kv == nil {
		return nil
	}
	return &tempocommon.KeyValue{
		Key:   kv.Key,
		Value: convertOTLPAnyValue(kv.Value),
	}
}

func convertOTLPAnyValue(otlpValue *commonv1.AnyValue) *tempocommon.AnyValue {
	if otlpValue == nil {
		return nil
	}
	tempoValue := &tempocommon.AnyValue{}
	switch v := otlpValue.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		tempoValue.Value = &tempocommon.AnyValue_StringValue{StringValue: v.StringValue}
	case *commonv1.AnyValue_IntValue:
		tempoValue.Value = &tempocommon.AnyValue_IntValue{IntValue: v.IntValue}
	case *commonv1.AnyValue_DoubleValue:
		tempoValue.Value = &tempocommon.AnyValue_DoubleValue{DoubleValue: v.DoubleValue}
	case *commonv1.AnyValue_BoolValue:
		tempoValue.Value = &tempocommon.AnyValue_BoolValue{BoolValue: v.BoolValue}
	case *commonv1.AnyValue_BytesValue:
		tempoValue.Value = &tempocommon.AnyValue_BytesValue{BytesValue: v.BytesValue}
	case *commonv1.AnyValue_ArrayValue:
		values := make([]*tempocommon.AnyValue, len(v.ArrayValue.Values))
		for i, value := range v.ArrayValue.Values {
			values[i] = convertOTLPAnyValue(value)
		}
		tempoValue.Value = &tempocommon.AnyValue_ArrayValue{
			ArrayValue: &tempocommon.ArrayValue{Values: values},
		}
	case *commonv1.AnyValue_KvlistValue:
		kvValues := make([]*tempocommon.KeyValue, len(v.KvlistValue.Values))
		for i, kv := range v.KvlistValue.Values {
			kvValues[i] = convertOTLPKeyValue(kv)
		}
		tempoValue.Value = &tempocommon.AnyValue_KvlistValue{
			KvlistValue: &tempocommon.KeyValueList{Values: kvValues},
		}
	}
	return tempoValue
}

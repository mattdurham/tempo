package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// protoToAttrValue converts an OTLP AnyValue to a shared.AttrValue.
// String values are zero-copy references into the proto's backing bytes.
// Bytes values are copied to prevent aliasing with internal proto state.
func protoToAttrValue(v *commonv1.AnyValue) shared.AttrValue {
	if v == nil {
		return shared.AttrValue{Type: shared.ColumnTypeString}
	}
	switch val := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return shared.AttrValue{Type: shared.ColumnTypeString, Str: val.StringValue}
	case *commonv1.AnyValue_IntValue:
		return shared.AttrValue{Type: shared.ColumnTypeInt64, Int: val.IntValue}
	case *commonv1.AnyValue_DoubleValue:
		return shared.AttrValue{Type: shared.ColumnTypeFloat64, Float: val.DoubleValue}
	case *commonv1.AnyValue_BoolValue:
		return shared.AttrValue{Type: shared.ColumnTypeBool, Bool: val.BoolValue}
	case *commonv1.AnyValue_BytesValue:
		cp := make([]byte, len(val.BytesValue))
		copy(cp, val.BytesValue)
		return shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: cp}
	default:
		return shared.AttrValue{Type: shared.ColumnTypeString, Str: v.String()}
	}
}

// anyToProtoValue converts an arbitrary Go value to an OTLP AnyValue proto.
// Used by synthesizeResourceSpans to build proto objects for the AddSpan path.
func anyToProtoValue(v any) *commonv1.AnyValue {
	switch val := v.(type) {
	case string:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: val}}
	case int64:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}}
	case int:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(val)}}
	case int32:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(val)}}
	case uint64:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(val)}} //nolint:gosec // uint64→int64: lossy for values >MaxInt64, consistent with proto encoding
	case float64:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: val}}
	case float32:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: float64(val)}}
	case bool:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: val}}
	case []byte:
		cp := make([]byte, len(val))
		copy(cp, val)
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: cp}}
	case *commonv1.AnyValue:
		return val
	default:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprint(v)}}
	}
}

// mapToKeyValues converts a map[string]any to a []*commonv1.KeyValue slice.
func mapToKeyValues(m map[string]any) []*commonv1.KeyValue {
	if len(m) == 0 {
		return nil
	}
	kvs := make([]*commonv1.KeyValue, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, &commonv1.KeyValue{Key: k, Value: anyToProtoValue(v)})
	}
	return kvs
}

// extractSvcNameFromProto scans a Resource proto for the service.name string attribute.
func extractSvcNameFromProto(r *resourcev1.Resource) string {
	if r == nil {
		return ""
	}
	for _, kv := range r.Attributes {
		if kv == nil || kv.Key != "service.name" || kv.Value == nil {
			continue
		}
		if sv, ok := kv.Value.Value.(*commonv1.AnyValue_StringValue); ok {
			return sv.StringValue
		}
	}
	return ""
}

// extractSvcNameFromMap returns the service.name string value from a map[string]any.
func extractSvcNameFromMap(m map[string]any) string {
	if v, ok := m["service.name"]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// synthesizeResourceSpans builds synthetic proto containers for the AddSpan path.
// Converts map[string]any attribute maps to []*commonv1.KeyValue slices and wraps
// the existing Span in ResourceSpans/ScopeSpans so it can be processed by addRowFromProto.
// Returns (ResourceSpans, ScopeSpans); caller must anchor the returned objects in protoRoots.
func synthesizeResourceSpans(
	span *tracev1.Span,
	resourceAttrs map[string]any,
	resourceSchemaURL string,
	scopeAttrs map[string]any,
	scopeSchemaURL string,
) (*tracev1.ResourceSpans, *tracev1.ScopeSpans) {
	ss := &tracev1.ScopeSpans{
		Scope: &commonv1.InstrumentationScope{
			Attributes: mapToKeyValues(scopeAttrs),
		},
		Spans:     []*tracev1.Span{span},
		SchemaUrl: scopeSchemaURL,
	}
	rs := &tracev1.ResourceSpans{
		Resource: &resourcev1.Resource{
			Attributes: mapToKeyValues(resourceAttrs),
		},
		ScopeSpans: []*tracev1.ScopeSpans{ss},
		SchemaUrl:  resourceSchemaURL,
	}
	return rs, ss
}

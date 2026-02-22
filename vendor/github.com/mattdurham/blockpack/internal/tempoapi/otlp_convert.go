// Package tempoapi provides Tempo API compatibility and conversion utilities.
package tempoapi

import (
	"fmt"

	gogoproto "github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	spanconv "github.com/mattdurham/blockpack/internal/blockio/spanconv"
	otlpproto "google.golang.org/protobuf/proto"
)

// SpanDataToOTLPTrace converts a slice of SpanData to a tempopb.Trace.
// Spans are grouped by (resource.service.name, scope.name, scope.version) into
// the proper ResourceSpans hierarchy required by OTLP.
//
// OTLP proto types are converted to tempopb types via proto marshal/unmarshal —
// both use the same wire format (protobuf), just different Go namespaces.
func SpanDataToOTLPTrace(spans []*spanconv.SpanData) (*tempopb.Trace, error) {
	if len(spans) == 0 {
		return &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{},
		}, nil
	}

	type groupKey struct {
		resourceSvcName string
		scopeName       string
		scopeVersion    string
	}

	type resourceGroup struct {
		firstSD *spanconv.SpanData
		spans   []*tempotrace.Span
	}

	groups := make(map[groupKey]*resourceGroup)
	keyOrder := make([]groupKey, 0, len(spans))

	for _, sd := range spans {
		// Extract service.name from resource attributes for grouping key
		svcName := ""
		for _, attr := range sd.Resource.Attributes {
			if attr.Key == attrServiceName {
				svcName = attr.Value.GetStringValue()
				break
			}
		}

		gk := groupKey{
			resourceSvcName: svcName,
			scopeName:       sd.Scope.Name,
			scopeVersion:    sd.Scope.Version,
		}

		if _, exists := groups[gk]; !exists {
			groups[gk] = &resourceGroup{firstSD: sd}
			keyOrder = append(keyOrder, gk)
		}

		// Convert span: OTLP proto → wire bytes → tempopb Span
		spanBytes, err := otlpproto.Marshal(sd.Span)
		if err != nil {
			return nil, fmt.Errorf("marshal span: %w", err)
		}

		var tempoSpan tempotrace.Span
		if err := gogoproto.Unmarshal(spanBytes, &tempoSpan); err != nil {
			return nil, fmt.Errorf("unmarshal span: %w", err)
		}

		groups[gk].spans = append(groups[gk].spans, &tempoSpan)
	}

	resourceSpans := make([]*tempotrace.ResourceSpans, 0, len(groups))

	for _, gk := range keyOrder {
		g := groups[gk]
		sd := g.firstSD //nolint:nilaway

		// Convert resource: OTLP proto → wire bytes → tempopb Resource
		resourceBytes, err := otlpproto.Marshal(sd.Resource)
		if err != nil {
			return nil, fmt.Errorf("marshal resource: %w", err)
		}

		var tempoResource temporesource.Resource
		if err := gogoproto.Unmarshal(resourceBytes, &tempoResource); err != nil { //nolint:govet
			return nil, fmt.Errorf("unmarshal resource: %w", err)
		}

		// Convert scope: OTLP proto → wire bytes → tempopb InstrumentationScope
		scopeBytes, err := otlpproto.Marshal(sd.Scope)
		if err != nil {
			return nil, fmt.Errorf("marshal scope: %w", err)
		}

		var tempoScope tempocommon.InstrumentationScope
		if err := gogoproto.Unmarshal(scopeBytes, &tempoScope); err != nil {
			return nil, fmt.Errorf("unmarshal scope: %w", err)
		}

		rs := &tempotrace.ResourceSpans{
			Resource:  &tempoResource,
			SchemaUrl: sd.ResourceSchema,
			ScopeSpans: []*tempotrace.ScopeSpans{
				{
					Scope:     &tempoScope,
					SchemaUrl: sd.ScopeSchema,
					Spans:     g.spans, //nolint:nilaway
				},
			},
		}

		resourceSpans = append(resourceSpans, rs)
	}

	return &tempopb.Trace{ResourceSpans: resourceSpans}, nil
}

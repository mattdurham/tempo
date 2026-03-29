package benchmark

// Benchmark Metrics Documentation
//
// This package reports several metrics to measure object storage performance:
//   - io_ops: Number of I/O operations (most important for S3/GCS/Azure)
//   - bytes/io: Average bytes per operation (higher is better)
//   - bytes_read: Total data transferred
//
// IMPORTANT: Benchmarks include 20ms artificial latency per I/O operation
// to simulate real-world S3 performance (10-20ms first-byte latency).
//
// For detailed explanation of all metrics, interpretation guidelines, and
// cost analysis, see: benchmark/METRICS.md

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	tempoql "github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	tempoencoding "github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// benchmarkQuery represents a query used for benchmarking
type benchmarkQuery struct {
	name         string
	traceqlQuery string
	description  string
	isStructural bool // true for structural operator queries (>>, >, ~, <<, <, !~); see benchmarkQueries for coverage
}

// Helper functions for format comparison benchmarks and tests.
// These are shared across benchmark and validation tests.

// traceTemplate represents a trace pattern similar to xk6-tracing templates
type traceTemplate struct {
	name  string
	spans []spanTemplate
}

// spanTemplate defines a span pattern with service, operation, and attributes
type spanTemplate struct {
	service        string
	operation      string
	semanticType   string // "http", "db", "rpc"
	dbSystem       string
	namespace      string
	parentIdx      int // -1 means root span
	durationMin    time.Duration
	durationMax    time.Duration
	httpStatusCode int
	isError        bool
}

// xk6TraceTemplates mirrors the templates from xk6-client-tracing
var xk6TraceTemplates = []traceTemplate{
	{
		name: "list-articles",
		spans: []spanTemplate{
			{
				service:        "shop-backend",
				operation:      "list-articles",
				parentIdx:      -1,
				durationMin:    200 * time.Millisecond,
				durationMax:    900 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "shop-backend",
				operation:      "authenticate",
				parentIdx:      0,
				durationMin:    50 * time.Millisecond,
				durationMax:    100 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "auth-service",
				operation:      "authenticate",
				parentIdx:      1,
				durationMin:    30 * time.Millisecond,
				durationMax:    80 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "auth",
			},
			{
				service:        "shop-backend",
				operation:      "fetch-articles",
				parentIdx:      0,
				durationMin:    100 * time.Millisecond,
				durationMax:    400 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "article-service",
				operation:      "list-articles",
				parentIdx:      3,
				durationMin:    80 * time.Millisecond,
				durationMax:    300 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "article-service",
				operation:      "select-articles",
				parentIdx:      4,
				durationMin:    50 * time.Millisecond,
				durationMax:    200 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "db",
				namespace:      "shop",
			},
			{
				service:        "postgres",
				operation:      "query-articles",
				parentIdx:      5,
				durationMin:    20 * time.Millisecond,
				durationMax:    150 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "db",
				dbSystem:       "postgresql",
				namespace:      "db",
			},
		},
	},
	{
		name: "article-to-cart",
		spans: []spanTemplate{
			{
				service:        "shop-backend",
				operation:      "article-to-cart",
				parentIdx:      -1,
				durationMin:    400 * time.Millisecond,
				durationMax:    1200 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "shop-backend",
				operation:      "authenticate",
				parentIdx:      0,
				durationMin:    70 * time.Millisecond,
				durationMax:    200 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "auth-service",
				operation:      "authenticate",
				parentIdx:      1,
				durationMin:    40 * time.Millisecond,
				durationMax:    150 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "auth",
			},
			{
				service:        "shop-backend",
				operation:      "get-article",
				parentIdx:      0,
				durationMin:    80 * time.Millisecond,
				durationMax:    300 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "article-service",
				operation:      "get-article",
				parentIdx:      3,
				durationMin:    60 * time.Millisecond,
				durationMax:    250 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "article-service",
				operation:      "select-articles",
				parentIdx:      4,
				durationMin:    40 * time.Millisecond,
				durationMax:    180 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "db",
				namespace:      "shop",
			},
			{
				service:        "postgres",
				operation:      "query-articles",
				parentIdx:      5,
				durationMin:    20 * time.Millisecond,
				durationMax:    120 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "db",
				dbSystem:       "postgresql",
				namespace:      "db",
			},
			{
				service:        "shop-backend",
				operation:      "place-articles",
				parentIdx:      0,
				durationMin:    100 * time.Millisecond,
				durationMax:    400 * time.Millisecond,
				httpStatusCode: 201,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "cart-service",
				operation:      "place-articles",
				parentIdx:      7,
				durationMin:    80 * time.Millisecond,
				durationMax:    350 * time.Millisecond,
				httpStatusCode: 201,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "cart-service",
				operation:      "persist-cart",
				parentIdx:      8,
				durationMin:    40 * time.Millisecond,
				durationMax:    200 * time.Millisecond,
				httpStatusCode: 201,
				semanticType:   "db",
				namespace:      "shop",
			},
		},
	},
	{
		name: "auth-failure",
		spans: []spanTemplate{
			{
				service:        "shop-backend",
				operation:      "checkout",
				parentIdx:      -1,
				durationMin:    50 * time.Millisecond,
				durationMax:    150 * time.Millisecond,
				httpStatusCode: 403,
				semanticType:   "http",
				isError:        true,
				namespace:      "shop",
			},
			{
				service:        "shop-backend",
				operation:      "authenticate",
				parentIdx:      0,
				durationMin:    40 * time.Millisecond,
				durationMax:    120 * time.Millisecond,
				httpStatusCode: 403,
				semanticType:   "http",
				isError:        true,
				namespace:      "shop",
			},
			{
				service:        "auth-service",
				operation:      "authenticate",
				parentIdx:      1,
				durationMin:    30 * time.Millisecond,
				durationMax:    100 * time.Millisecond,
				httpStatusCode: 403,
				semanticType:   "http",
				isError:        true,
				namespace:      "auth",
			},
		},
	},
	{
		name: "checkout-flow",
		spans: []spanTemplate{
			{
				service:        "shop-backend",
				operation:      "checkout",
				parentIdx:      -1,
				durationMin:    500 * time.Millisecond,
				durationMax:    2000 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "shop-backend",
				operation:      "authenticate",
				parentIdx:      0,
				durationMin:    50 * time.Millisecond,
				durationMax:    150 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "auth-service",
				operation:      "authenticate",
				parentIdx:      1,
				durationMin:    30 * time.Millisecond,
				durationMax:    100 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "auth",
			},
			{
				service:        "cart-service",
				operation:      "checkout",
				parentIdx:      0,
				durationMin:    200 * time.Millisecond,
				durationMax:    800 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "shop",
			},
			{
				service:        "billing-service",
				operation:      "payment",
				parentIdx:      3,
				durationMin:    150 * time.Millisecond,
				durationMax:    600 * time.Millisecond,
				httpStatusCode: 200,
				semanticType:   "http",
				namespace:      "billing",
			},
		},
	},
}

// generateBenchmarkTraces creates realistic test data using xk6-tracing-style templates
func generateBenchmarkTraces(
	tb testing.TB,
	numTraces, spansPerTrace int,
) ([]*tracev1.TracesData, []*tempopb.Trace, []common.ID) {
	tb.Helper()

	// Place all traces in the past - start 8 hours ago
	// With 25,000 traces at 1 second apart, they span ~7 hours
	baseTime := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-8 * time.Hour)
	otelTraces := make([]*tracev1.TracesData, 0)
	tempoTraces := make([]*tempopb.Trace, 0, numTraces)
	traceIDs := make([]common.ID, 0, numTraces)

	for traceIdx := 0; traceIdx < numTraces; traceIdx++ {
		traceID := generateRandomTraceID(traceIdx)
		traceIDs = append(traceIDs, traceID)
		startTime := baseTime.Add(time.Duration(traceIdx) * time.Second)

		// Select template based on trace index for deterministic distribution
		template := xk6TraceTemplates[traceIdx%len(xk6TraceTemplates)]

		// Generate spans from template
		otelSpans, tempoSpans := generateSpansFromTemplate(traceID, startTime, template, traceIdx)

		// Group spans by service for resource spans
		otelResourceSpans := groupSpansByService(otelSpans)
		tempoResourceSpans := groupTempoSpansByService(tempoSpans, traceIdx)

		// Add to OTel format
		otelTraces = append(otelTraces, &tracev1.TracesData{
			ResourceSpans: otelResourceSpans,
		})

		// Add to Tempo format
		tempoTraces = append(tempoTraces, &tempopb.Trace{
			ResourceSpans: tempoResourceSpans,
		})
	}

	return otelTraces, tempoTraces, traceIDs
}

// generateOTelTracesOnly creates OTel trace data without allocating Tempo traces.
// Use this instead of generateBenchmarkTraces when only the blockpack format is needed,
// to avoid the 2x peak memory spike from building both formats simultaneously.
func generateOTelTracesOnly(tb testing.TB, numTraces int) []*tracev1.TracesData {
	tb.Helper()
	baseTime := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-8 * time.Hour)
	otelTraces := make([]*tracev1.TracesData, 0, numTraces)
	for traceIdx := 0; traceIdx < numTraces; traceIdx++ {
		traceID := generateRandomTraceID(traceIdx)
		startTime := baseTime.Add(time.Duration(traceIdx) * time.Second)
		template := xk6TraceTemplates[traceIdx%len(xk6TraceTemplates)]
		otelSpans, _ := generateSpansFromTemplate(traceID, startTime, template, traceIdx)
		otelTraces = append(otelTraces, &tracev1.TracesData{
			ResourceSpans: groupSpansByService(otelSpans),
		})
	}
	return otelTraces
}

// generateTempoTracesOnly creates Tempo trace data without allocating OTel traces.
// Use this instead of generateBenchmarkTraces when only the Parquet format is needed,
// to avoid the 2x peak memory spike from building both formats simultaneously.
func generateTempoTracesOnly(tb testing.TB, numTraces int) ([]*tempopb.Trace, []common.ID) {
	tb.Helper()
	baseTime := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Add(-8 * time.Hour)
	tempoTraces := make([]*tempopb.Trace, 0, numTraces)
	traceIDs := make([]common.ID, 0, numTraces)
	for traceIdx := 0; traceIdx < numTraces; traceIdx++ {
		traceID := generateRandomTraceID(traceIdx)
		traceIDs = append(traceIDs, traceID)
		startTime := baseTime.Add(time.Duration(traceIdx) * time.Second)
		template := xk6TraceTemplates[traceIdx%len(xk6TraceTemplates)]
		_, tempoSpans := generateSpansFromTemplate(traceID, startTime, template, traceIdx)
		tempoTraces = append(tempoTraces, &tempopb.Trace{
			ResourceSpans: groupTempoSpansByService(tempoSpans, traceIdx),
		})
	}
	return tempoTraces, traceIDs
}

// generateSpansFromTemplate creates spans based on a trace template
func generateSpansFromTemplate(
	traceID []byte,
	startTime time.Time,
	template traceTemplate,
	traceIdx int,
) ([]*tracev1.Span, []*tracev1.Span) {
	otelSpans := make([]*tracev1.Span, 0, len(template.spans))
	tempoSpans := make([]*tracev1.Span, 0, len(template.spans))
	spanIDs := make([][]byte, len(template.spans))

	// Generate span IDs first
	for i := range template.spans {
		spanIDs[i] = generateRandomSpanID(traceIdx*1000 + i)
	}

	currentTime := startTime
	for i, spanTmpl := range template.spans {
		// Calculate duration using deterministic randomness
		durationRange := spanTmpl.durationMax - spanTmpl.durationMin
		duration := spanTmpl.durationMin + time.Duration(
			pseudoRandom(traceIdx*100+i, int(durationRange.Milliseconds())),
		)*time.Millisecond

		// Determine parent span ID
		var parentSpanID []byte
		if spanTmpl.parentIdx >= 0 && spanTmpl.parentIdx < len(spanIDs) {
			parentSpanID = spanIDs[spanTmpl.parentIdx]
		}

		// Determine status code
		statusCode := tracev1.Status_STATUS_CODE_OK
		if spanTmpl.isError || spanTmpl.httpStatusCode >= 400 {
			statusCode = tracev1.Status_STATUS_CODE_ERROR
		}

		// Determine HTTP method based on semantic type
		httpMethod := "GET"
		if spanTmpl.semanticType == "http" {
			if spanTmpl.httpStatusCode == 201 {
				httpMethod = "POST"
			} else if pseudoRandom(traceIdx+i, 4) == 0 {
				httpMethod = "POST"
			}
		}

		// Create spans
		otelSpans = append(otelSpans, createTemplatedOTelSpan(
			traceID, spanIDs[i], parentSpanID,
			spanTmpl, currentTime, duration, statusCode, httpMethod, traceIdx,
		))

		tempoSpans = append(tempoSpans, createTemplatedTempoSpan(
			traceID,
			spanIDs[i],
			parentSpanID,
			spanTmpl, currentTime, duration, statusCode, httpMethod, traceIdx,
		))

		// Advance time for next span
		currentTime = currentTime.Add(time.Duration(pseudoRandom(traceIdx*50+i, 10)+1) * time.Millisecond)
	}

	return otelSpans, tempoSpans
}

// groupSpansByService groups OTel spans by service name into ResourceSpans
func groupSpansByService(spans []*tracev1.Span) []*tracev1.ResourceSpans {
	serviceMap := make(map[string][]*tracev1.Span)

	for _, span := range spans {
		// Extract service name from span attributes
		serviceName := "unknown"
		for _, attr := range span.Attributes {
			if attr.Key == "service.name" {
				if strVal := attr.Value.GetStringValue(); strVal != "" {
					serviceName = strVal
					break
				}
			}
		}
		serviceMap[serviceName] = append(serviceMap[serviceName], span)
	}

	// Sort service names for deterministic ordering (maps have non-deterministic iteration)
	serviceNames := make([]string, 0, len(serviceMap))
	for serviceName := range serviceMap {
		serviceNames = append(serviceNames, serviceName)
	}
	sort.Strings(serviceNames)

	resourceSpans := make([]*tracev1.ResourceSpans, 0, len(serviceMap))
	for _, serviceName := range serviceNames {
		spans := serviceMap[serviceName]
		// Sort spans within each service by span ID for fully deterministic ordering
		sort.Slice(spans, func(i, j int) bool {
			return string(spans[i].SpanId) < string(spans[j].SpanId)
		})

		namespace := "default"
		// Extract namespace from first span
		if len(spans) > 0 {
			for _, attr := range spans[0].Attributes {
				if attr.Key == "namespace" {
					if strVal := attr.Value.GetStringValue(); strVal != "" {
						namespace = strVal
						break
					}
				}
			}
		}

		// Remove service.name and namespace from span attributes since they're now on the Resource
		// This is critical - OTLP resource attributes should NOT be duplicated on span attributes
		for _, span := range spans {
			filteredAttrs := make([]*commonv1.KeyValue, 0, len(span.Attributes))
			for _, attr := range span.Attributes {
				// Skip service.name and namespace - they belong on the Resource
				if attr.Key == "service.name" || attr.Key == "namespace" {
					continue
				}
				filteredAttrs = append(filteredAttrs, attr)
			}
			span.Attributes = filteredAttrs
		}

		resourceSpans = append(resourceSpans, &tracev1.ResourceSpans{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{
					{
						Key: "service.name",
						Value: &commonv1.AnyValue{
							Value: &commonv1.AnyValue_StringValue{StringValue: serviceName},
						},
					},
					{
						Key: "namespace",
						Value: &commonv1.AnyValue{
							Value: &commonv1.AnyValue_StringValue{StringValue: namespace},
						},
					},
				},
			},
			ScopeSpans: []*tracev1.ScopeSpans{
				{
					Spans: spans,
				},
			},
		})
	}

	return resourceSpans
}

// groupTempoSpansByService groups Tempo spans by service name into ResourceSpans
func groupTempoSpansByService(spans []*tracev1.Span, traceIdx int) []*tempotrace.ResourceSpans {
	serviceMap := make(map[string][]*tracev1.Span)

	for _, span := range spans {
		serviceName := "unknown"
		for _, attr := range span.Attributes {
			if attr.Key == "service.name" {
				if strVal := attr.Value.GetStringValue(); strVal != "" {
					serviceName = strVal
					break
				}
			}
		}
		serviceMap[serviceName] = append(serviceMap[serviceName], span)
	}

	// Sort service names for deterministic ordering (maps have non-deterministic iteration)
	serviceNames := make([]string, 0, len(serviceMap))
	for serviceName := range serviceMap {
		serviceNames = append(serviceNames, serviceName)
	}
	sort.Strings(serviceNames)

	resourceSpans := make([]*tempotrace.ResourceSpans, 0, len(serviceMap))
	for _, serviceName := range serviceNames {
		spans := serviceMap[serviceName]
		// Sort spans within each service by span ID for fully deterministic ordering
		sort.Slice(spans, func(i, j int) bool {
			return string(spans[i].SpanId) < string(spans[j].SpanId)
		})

		namespace := "default"
		if len(spans) > 0 {
			for _, attr := range spans[0].Attributes {
				if attr.Key == "namespace" {
					if strVal := attr.Value.GetStringValue(); strVal != "" {
						namespace = strVal
						break
					}
				}
			}
		}

		// Remove service.name and namespace from span attributes since they're now on the Resource
		// This is critical - OTLP resource attributes should NOT be duplicated on span attributes
		for _, span := range spans {
			filteredAttrs := make([]*commonv1.KeyValue, 0, len(span.Attributes))
			for _, attr := range span.Attributes {
				// Skip service.name and namespace - they belong on the Resource
				if attr.Key == "service.name" || attr.Key == "namespace" {
					continue
				}
				filteredAttrs = append(filteredAttrs, attr)
			}
			span.Attributes = filteredAttrs
		}

		resourceSpans = append(resourceSpans, &tempotrace.ResourceSpans{
			Resource: &temporesource.Resource{
				Attributes: []*tempocommon.KeyValue{
					{
						Key: "service.name",
						Value: &tempocommon.AnyValue{
							Value: &tempocommon.AnyValue_StringValue{StringValue: serviceName},
						},
					},
					{
						Key: "namespace",
						Value: &tempocommon.AnyValue{
							Value: &tempocommon.AnyValue_StringValue{StringValue: namespace},
						},
					},
				},
			},
			ScopeSpans: []*tempotrace.ScopeSpans{
				{
					Spans: convertOTLPToTempoSpans(spans),
				},
			},
		})
	}

	return resourceSpans
}

// createTemplatedOTelSpan creates an OTel span from a template
func createTemplatedOTelSpan(
	traceID, spanID, parentSpanID []byte,
	template spanTemplate,
	startTime time.Time,
	duration time.Duration,
	statusCode tracev1.Status_StatusCode,
	httpMethod string,
	traceIdx int,
) *tracev1.Span {
	// Start with empty attributes - service.name and namespace will be added to Resource by groupSpansByService
	// Note: NOT adding "service.name" or "namespace" here since they're resource-level attributes
	attrs := []*commonv1.KeyValue{}

	// Add a temporary marker attribute so groupSpansByService can extract the service name
	// This will be removed later and added to the Resource instead
	attrs = append(attrs, &commonv1.KeyValue{
		Key: "service.name",
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: template.service},
		},
	})

	// Temporarily add namespace for extraction during grouping
	if template.namespace != "" {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: "namespace",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: template.namespace},
			},
		})
	}

	// Add HTTP-specific attributes
	if template.semanticType == "http" {
		attrs = append(attrs,
			&commonv1.KeyValue{
				Key: "http.method",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: httpMethod},
				},
			},
			&commonv1.KeyValue{
				Key: "http.status_code",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_IntValue{IntValue: int64(template.httpStatusCode)},
				},
			},
		)
	}

	// Add DB-specific attributes
	if template.semanticType == "db" {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: "db.system",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: template.dbSystem},
			},
		})
		if template.dbSystem != "" {
			attrs = append(attrs, &commonv1.KeyValue{
				Key: "db.operation",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: template.operation},
				},
			})
		}
	}

	// Add selective attributes for query testing
	var customerTier string
	if traceIdx >= 100 && traceIdx < 150 {
		customerTier = "platinum"
	} else if traceIdx >= 1000 && traceIdx < 3000 {
		customerTier = "gold"
	} else {
		customerTier = "standard"
	}
	attrs = append(attrs, &commonv1.KeyValue{
		Key: "customer.tier",
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: customerTier},
		},
	})

	var region string
	switch traceIdx % 3 {
	case 0:
		region = "us-west"
	case 1:
		region = "us-east"
	default:
		region = "eu-west"
	}
	attrs = append(attrs, &commonv1.KeyValue{
		Key: "deployment.region",
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: region},
		},
	})

	if traceIdx >= 5000 && traceIdx < 5075 {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: "experiment.id",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: "exp-2024-q1"},
			},
		})
	}

	otelKind := tracev1.Span_SPAN_KIND_SERVER
	if template.semanticType == "db" {
		otelKind = tracev1.Span_SPAN_KIND_CLIENT
	}

	return &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		Name:              template.operation,
		Kind:              otelKind,
		StartTimeUnixNano: uint64(startTime.UnixNano()),
		EndTimeUnixNano:   uint64(startTime.Add(duration).UnixNano()),
		Status: &tracev1.Status{
			Code: statusCode,
		},
		Attributes: attrs,
	}
}

// createTemplatedTempoSpan creates a Tempo span from a template (using OTLP format)
func createTemplatedTempoSpan(
	traceID []byte,
	spanID []byte,
	parentSpanID []byte,
	template spanTemplate,
	startTime time.Time,
	duration time.Duration,
	statusCode tracev1.Status_StatusCode,
	httpMethod string,
	traceIdx int,
) *tracev1.Span {
	// Start with empty attributes - service.name and namespace will be added to Resource by groupTempoSpansByService
	attrs := []*commonv1.KeyValue{}

	// Add temporary marker attributes for grouping
	attrs = append(attrs, &commonv1.KeyValue{
		Key: "service.name",
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: template.service},
		},
	})

	if template.namespace != "" {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: "namespace",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: template.namespace},
			},
		})
	}

	if template.semanticType == "http" {
		attrs = append(attrs,
			&commonv1.KeyValue{
				Key: "http.method",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: httpMethod},
				},
			},
			&commonv1.KeyValue{
				Key: "http.status_code",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_IntValue{IntValue: int64(template.httpStatusCode)},
				},
			},
		)
	}

	if template.semanticType == "db" {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: "db.system",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: template.dbSystem},
			},
		})
		if template.dbSystem != "" {
			attrs = append(attrs, &commonv1.KeyValue{
				Key: "db.operation",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: template.operation},
				},
			})
		}
	}

	var customerTier string
	if traceIdx >= 100 && traceIdx < 150 {
		customerTier = "platinum"
	} else if traceIdx >= 1000 && traceIdx < 3000 {
		customerTier = "gold"
	} else {
		customerTier = "standard"
	}
	attrs = append(attrs, &commonv1.KeyValue{
		Key: "customer.tier",
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: customerTier},
		},
	})

	var region string
	switch traceIdx % 3 {
	case 0:
		region = "us-west"
	case 1:
		region = "us-east"
	default:
		region = "eu-west"
	}
	attrs = append(attrs, &commonv1.KeyValue{
		Key: "deployment.region",
		Value: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: region},
		},
	})

	if traceIdx >= 5000 && traceIdx < 5075 {
		attrs = append(attrs, &commonv1.KeyValue{
			Key: "experiment.id",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: "exp-2024-q1"},
			},
		})
	}

	tempoKind := tracev1.Span_SPAN_KIND_SERVER
	if template.semanticType == "db" {
		tempoKind = tracev1.Span_SPAN_KIND_CLIENT
	}

	return &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentSpanID,
		Name:              template.operation,
		StartTimeUnixNano: uint64(startTime.UnixNano()),
		EndTimeUnixNano:   uint64(startTime.Add(duration).UnixNano()),
		Kind:              tempoKind,
		Status: &tracev1.Status{
			Code: statusCode,
		},
		Attributes: attrs,
	}
}

// convertOTLPToTempoSpans converts OTLP spans to Tempo's span format
func convertOTLPToTempoSpans(otlpSpans []*tracev1.Span) []*tempotrace.Span {
	tempoSpans := make([]*tempotrace.Span, len(otlpSpans))
	for i, otlpSpan := range otlpSpans {
		// Convert attributes
		attrs := make([]*tempocommon.KeyValue, len(otlpSpan.Attributes))
		for j, attr := range otlpSpan.Attributes {
			attrs[j] = &tempocommon.KeyValue{
				Key:   attr.Key,
				Value: convertOTLPAnyValue(attr.Value),
			}
		}

		tempoSpans[i] = &tempotrace.Span{
			TraceId:           otlpSpan.TraceId,
			SpanId:            otlpSpan.SpanId,
			ParentSpanId:      otlpSpan.ParentSpanId,
			Name:              otlpSpan.Name,
			Kind:              tempotrace.Span_SpanKind(otlpSpan.Kind),
			StartTimeUnixNano: otlpSpan.StartTimeUnixNano,
			EndTimeUnixNano:   otlpSpan.EndTimeUnixNano,
			Attributes:        attrs,
			Status:            &tempotrace.Status{Code: tempotrace.Status_StatusCode(otlpSpan.Status.Code)},
		}
	}
	return tempoSpans
}

// convertOTLPAnyValue converts OTLP AnyValue to Tempo AnyValue
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
	}
	return tempoValue
}

// Helper functions for realistic data generation

func generateRandomTraceID(seed int) []byte {
	traceID := make([]byte, 16)
	for i := range traceID {
		traceID[i] = byte((seed >> (i * 8)) & 0xFF)
	}
	// Add some randomness
	traceID[15] = byte(seed % 256)
	return traceID
}

func generateRandomSpanID(seed int) []byte {
	spanID := make([]byte, 8)
	for i := range spanID {
		spanID[i] = byte((seed >> (i * 8)) & 0xFF)
	}
	return spanID
}

// pseudoRandom generates a deterministic pseudo-random number for benchmarks
func pseudoRandom(seed, max int) int {
	// Simple LCG (Linear Congruential Generator)
	return (seed*1103515245 + 12345) % max
}

// writeBlockpackFormat writes traces to the blockpack format and returns the file path.
// writeBlockpackFormat writes traces to the blockpack format and returns the file path.
// name is used as the filename stem for benchmarks (e.g. "traces-format" → "traces-format.blockpack");
// pass "" to use the default "traces" stem. Tests always write to a temp dir regardless of name.
func writeBlockpackFormat(tb testing.TB, traces []*tracev1.TracesData, name string) string {
	tb.Helper()
	// Benchmarks write to a fixed path so the report generator can read file sizes.
	// Tests use a temp dir to avoid polluting the working directory.
	var path string
	if _, isBench := tb.(*testing.B); isBench {
		stem := name
		if stem == "" {
			stem = "traces"
		}
		path = stem + ".blockpack"
	} else {
		path = filepath.Join(tb.TempDir(), "traces.blockpack")
	}
	f, err := os.Create(path)
	require.NoError(tb, err)
	defer f.Close()

	writer, err := blockpack.NewWriter(f, 0)
	require.NoError(tb, err)
	for _, trace := range traces {
		require.NoError(tb, writer.AddTracesData(trace))
	}

	_, err = writer.Flush()
	require.NoError(tb, err)
	return path
}

// writeTempoBlock writes traces to a Tempo vparquet5 block.
// name is used as the directory stem for benchmarks (e.g. "traces-format" → "traces-format.parquet/");
// pass "" to use the default "traces" stem. Tests always write to a temp dir regardless of name.
// Returns metadata and reader (NOT the opened block) to allow fair benchmarking.
func writeTempoBlock(
	tb testing.TB,
	traces []*tempopb.Trace,
	traceIDs []common.ID,
	name string,
) (*backend.BlockMeta, backend.Reader, int64, string, func()) {
	tb.Helper()

	// Benchmarks write to a fixed path so the report generator can read file sizes.
	// Tests use a temp dir to avoid polluting the working directory.
	var tmpDir string
	var cleanup func()
	if _, isBench := tb.(*testing.B); isBench {
		stem := name
		if stem == "" {
			stem = "traces"
		}
		tmpDir = stem + ".parquet"
		// Pre-clean outside the benchmark loop so this work is not measured.
		require.NoError(tb, os.RemoveAll(tmpDir))
		require.NoError(tb, os.MkdirAll(tmpDir, 0o750))
		cleanup = func() {}
	} else {
		var err error
		tmpDir, err = os.MkdirTemp("", "tempo-bench-*")
		require.NoError(tb, err)
		cleanup = func() { _ = os.RemoveAll(tmpDir) }
	}

	ctx := context.Background()
	tenant := "test-tenant"
	blockID := uuid.New()

	// Set up backend
	rawReader, rawWriter, _, err := local.New(&local.Config{Path: tmpDir})
	require.NoError(tb, err)

	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Create block metadata
	meta := backend.NewBlockMetaWithDedicatedColumns(
		tenant,
		blockID,
		vparquet5.VersionString,
		backend.DefaultDedicatedColumns(),
	)
	meta.TotalObjects = int64(len(traces))

	// Write the block using vparquet5
	cfg := &common.BlockConfig{
		BloomFP:             common.DefaultBloomFP,
		BloomShardSizeBytes: common.DefaultBloomShardSizeBytes,
		RowGroupSizeBytes:   100_000_000,
		DedicatedColumns:    backend.DefaultDedicatedColumns(),
	}

	// Create iterator for traces
	iter := &traceIterator{traces: traces, ids: traceIDs, index: 0}

	outMeta, err := vparquet5.CreateBlock(ctx, cfg, meta, iter, reader, writer)
	require.NoError(tb, err)

	require.NoError(tb, writer.WriteBlockMeta(ctx, outMeta))
	require.NoError(tb, writer.WriteTenantIndex(ctx, tenant, []*backend.BlockMeta{outMeta}, nil))

	// Load block metadata
	loadedMeta, err := reader.BlockMeta(ctx, blockID, tenant)
	require.NoError(tb, err)

	// Calculate block size from all files in the block directory
	blockPath := fmt.Sprintf("%s/%s/%s", tmpDir, tenant, blockID.String())
	var totalSize int64
	err = filepath.Walk(blockPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	require.NoError(tb, err)

	// Return metadata and reader (NOT the opened block)
	// This allows benchmarks to open the block inside the loop for fair comparison
	return loadedMeta, reader, totalSize, blockPath, cleanup
}

// traceIterator implements common.Iterator for a slice of traces
type traceIterator struct {
	traces []*tempopb.Trace
	ids    []common.ID
	index  int
}

func (it *traceIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
	if it.index >= len(it.traces) {
		return nil, nil, io.EOF
	}
	id := it.ids[it.index]
	tr := it.traces[it.index]
	it.index++
	return id, tr, nil
}

func (it *traceIterator) Close() {
	// Nothing to clean up
}

// simpleFileProvider wraps an os.File to implement blockpack.ReaderProvider without tracking.
type simpleFileProvider struct {
	file *os.File
}

func (p *simpleFileProvider) Size() (int64, error) {
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (p *simpleFileProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.file.ReadAt(buf, off)
}

// trackedFileProvider wraps an os.File to implement blockpack.ReaderProvider with I/O tracking
// and optional per-read latency simulation.
type trackedFileProvider struct {
	file      *os.File
	bytesRead int64
	ioOps     int64
	latency   time.Duration
}

func (p *trackedFileProvider) Size() (int64, error) {
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (p *trackedFileProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if p.latency > 0 {
		time.Sleep(p.latency)
	}
	n, err := p.file.ReadAt(buf, off)
	if err == nil || err == io.EOF {
		p.bytesRead += int64(n) //nolint:gosec
		p.ioOps++
	}
	return n, err
}

func (p *trackedFileProvider) reset() {
	p.bytesRead = 0
	p.ioOps = 0
}

// queryParquetKeys parses a TraceQL filter query and returns the exact set of
// Parquet-format attribute keys and whether the span name is included.
//
// These mirror vparquet5's FetchSpansRequest.Conditions: only attributes that
// appear as predicates in the query are fetched and returned in Span.Attributes
// (engine.go excludes IntrinsicName/Duration/TraceID/SpanID/TraceDuration/
// TraceRootService/TraceRootSpan from Span.Attributes; all others are included).
// IntrinsicName maps to Span.Name (not Attributes), so it is returned separately.
func queryParquetKeys(traceqlQuery string) (attrKeys map[string]bool, hasName bool, err error) {
	attrKeys = make(map[string]bool)
	_, _, _, _, req, compileErr := tempoql.Compile(traceqlQuery)
	if compileErr != nil {
		return nil, false, fmt.Errorf("compile %q: %w", traceqlQuery, compileErr)
	}
	for _, cond := range req.Conditions {
		a := cond.Attribute
		switch a.Intrinsic {
		case tempoql.IntrinsicNone:
			// User-defined attribute: bare name is the Parquet key (scope is separate).
			attrKeys[a.Name] = true
		case tempoql.IntrinsicName:
			hasName = true // goes to Span.Name, not Attributes
		case tempoql.IntrinsicDuration,
			tempoql.IntrinsicTraceDuration,
			tempoql.IntrinsicTraceRootService,
			tempoql.IntrinsicTraceRootSpan,
			tempoql.IntrinsicTraceID,
			tempoql.IntrinsicSpanID:
			// Excluded by engine.go — not emitted in Span.Attributes.
		default:
			// All other intrinsics (kind, status, statusMessage, parentID, …)
			// appear in Span.Attributes keyed by their string name.
			attrKeys[a.Intrinsic.String()] = true
		}
	}
	return attrKeys, hasName, nil
}

// executeBlockpackSearch runs a TraceQL filter query against a blockpack file and
// returns a SearchResponse with full span data (name, start, duration, attributes)
// in Parquet-compatible format. Parquet is the ground truth; this function produces
// output in the same schema so results can be compared directly.
func executeBlockpackSearch(path, traceqlQuery string) (*tempopb.SearchResponse, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	provider := &simpleFileProvider{file: f}
	r, err := blockpack.NewReaderFromProvider(provider)
	if err != nil {
		return nil, err
	}

	type traceInfo struct {
		spans     map[string]*tempopb.Span // keyed by normalised spanID
		spanOrder []string
	}
	traceMap := make(map[string]*traceInfo)
	traceOrder := make([]string, 0)

	// Derive the exact attribute keyset Parquet would return for this query so that
	// the blockpack response matches Parquet's query-driven output contract.
	wantParquetKeys, wantName, err := queryParquetKeys(traceqlQuery)
	if err != nil {
		return nil, err
	}

	bpMatches, err := blockpack.QueryTraceQL(r, traceqlQuery, blockpack.QueryOptions{})
	if err != nil {
		return nil, err
	}
	for i := range bpMatches {
		match := &bpMatches[i]
		if _, exists := traceMap[match.TraceID]; !exists {
			traceOrder = append(traceOrder, match.TraceID)
			traceMap[match.TraceID] = &traceInfo{spans: make(map[string]*tempopb.Span)}
		}
		info := traceMap[match.TraceID]
		norm := normalizeSpanID(match.SpanID)
		if _, seen := info.spans[norm]; !seen {
			info.spanOrder = append(info.spanOrder, norm)
			info.spans[norm] = blockpackMatchToProtoSpan(match, wantParquetKeys, wantName)
		}
	}

	traces := make([]*tempopb.TraceSearchMetadata, 0, len(traceOrder))
	for _, traceID := range traceOrder {
		info := traceMap[traceID]
		pbSpans := make([]*tempopb.Span, 0, len(info.spanOrder))
		for _, sid := range info.spanOrder {
			pbSpans = append(pbSpans, info.spans[sid])
		}
		ss := &tempopb.SpanSet{
			Matched: uint32(len(pbSpans)), //nolint:gosec
			Spans:   pbSpans,
		}
		traces = append(traces, &tempopb.TraceSearchMetadata{
			TraceID:  traceID,
			SpanSets: []*tempopb.SpanSet{ss},
		})
	}

	return &tempopb.SearchResponse{Traces: traces}, nil
}

// blockpackMatchToProtoSpan converts a blockpack SpanMatch into a tempopb.Span
// using Parquet's attribute key format and query-driven attribute selection.
//
// wantParquetKeys is the set of attribute keys (in Parquet format) that the query
// referenced — derived from queryParquetKeys. Only attributes whose Parquet-format
// key is in wantParquetKeys are emitted, mirroring vparquet5's fetch behaviour.
// wantName controls whether Span.Name is populated (only when the query references
// the name intrinsic). StartTimeUnixNano and DurationNanos are always populated.
//
// Key mapping (blockpack column → Parquet Span field / Attributes key):
//   - span:name       → Span.Name (only when wantName)
//   - span:start      → Span.StartTimeUnixNano (always)
//   - span:duration   → Span.DurationNanos (always)
//   - span:kind       → Attributes["kind"]   (OTLP int → Tempo kind string)
//   - span:status     → Attributes["status"] (OTLP int → Tempo status string)
//   - span:status_message → Attributes["statusMessage"]
//   - span:parent_id  → Attributes["span:parentID"] (bytes → hex string)
//   - span.X          → Attributes["X"]  (strip "span." scope prefix)
//   - resource.X      → Attributes["X"]  (strip "resource." scope prefix)
//   - span:id, trace:id, span:end, scope.*, scope:*, trace:state, *:schema_url → skipped
func blockpackMatchToProtoSpan(match *blockpack.SpanMatch, wantParquetKeys map[string]bool, wantName bool) *tempopb.Span {
	s := &tempopb.Span{SpanID: match.SpanID}
	if match.Fields == nil {
		return s
	}

	var attrs []*tempocommon.KeyValue
	match.Fields.IterateFields(func(name string, val any) bool {
		switch name {
		case "span:name":
			if wantName {
				if v, ok := val.(string); ok {
					s.Name = v
				}
			}
		case "span:start":
			if v, ok := val.(uint64); ok {
				s.StartTimeUnixNano = v
			}
		case "span:duration":
			if v, ok := val.(uint64); ok {
				s.DurationNanos = v
			}
		case "span:id", "trace:id", "span:end":
			// structural IDs and derived fields — not in Parquet Span.Attributes
		case "span:kind":
			if wantParquetKeys["kind"] {
				if v, ok := val.(int64); ok {
					attrs = append(attrs, &tempocommon.KeyValue{
						Key: "kind",
						Value: &tempocommon.AnyValue{
							Value: &tempocommon.AnyValue_StringValue{StringValue: otlpKindToString(v)},
						},
					})
				}
			}
		case "span:status":
			if wantParquetKeys["status"] {
				if v, ok := val.(int64); ok {
					attrs = append(attrs, &tempocommon.KeyValue{
						Key: "status",
						Value: &tempocommon.AnyValue{
							Value: &tempocommon.AnyValue_StringValue{StringValue: otlpStatusToString(v)},
						},
					})
				}
			}
		case "span:status_message":
			if wantParquetKeys["statusMessage"] {
				attrs = append(attrs, &tempocommon.KeyValue{Key: "statusMessage", Value: blockpackAnyValue(val)})
			}
		case "span:parent_id":
			if wantParquetKeys["span:parentID"] {
				if b, ok := val.([]byte); ok && len(b) > 0 {
					attrs = append(attrs, &tempocommon.KeyValue{
						Key: "span:parentID",
						Value: &tempocommon.AnyValue{
							Value: &tempocommon.AnyValue_StringValue{StringValue: hex.EncodeToString(b)},
						},
					})
				}
			}
		default:
			if strings.HasPrefix(name, "scope.") || strings.HasPrefix(name, "scope:") ||
				name == "trace:state" || strings.HasSuffix(name, ":schema_url") {
				return true
			}
			var bareKey string
			switch {
			case strings.HasPrefix(name, "span."):
				bareKey = name[len("span."):]
			case strings.HasPrefix(name, "resource."):
				bareKey = name[len("resource."):]
			default:
				return true // unknown prefix — skip
			}
			if wantParquetKeys[bareKey] {
				attrs = append(attrs, &tempocommon.KeyValue{Key: bareKey, Value: blockpackAnyValue(val)})
			}
		}
		return true
	})

	s.Attributes = attrs
	return s
}

// otlpKindToString converts an OTLP SpanKind integer to Tempo's TraceQL kind string.
// Follows vparquet5.otlpKindToTraceqlKind + traceql.Kind.String().
func otlpKindToString(v int64) string {
	switch v {
	case 0:
		return "unspecified"
	case 1:
		return "internal"
	case 2:
		return "server" // OTLP SPAN_KIND_SERVER(2) → traceql.KindServer → "server"
	case 3:
		return "client" // OTLP SPAN_KIND_CLIENT(3) → traceql.KindClient → "client"
	case 4:
		return "producer"
	case 5:
		return "consumer"
	default:
		return fmt.Sprintf("kind(%d)", v)
	}
}

// otlpStatusToString converts an OTLP StatusCode integer to Tempo's TraceQL status string.
// Follows vparquet5.otlpStatusToTraceqlStatus + traceql.Status.String().
func otlpStatusToString(v int64) string {
	switch v {
	case 0:
		return "unset" // OTLP STATUS_CODE_UNSET → traceql.StatusUnset → "unset"
	case 1:
		return "ok" // OTLP STATUS_CODE_OK → traceql.StatusOk → "ok"
	case 2:
		return "error" // OTLP STATUS_CODE_ERROR → traceql.StatusError → "error"
	default:
		return fmt.Sprintf("status(%d)", v)
	}
}

// blockpackAnyValue converts a blockpack field value (any) to a *tempocommon.AnyValue
// using the same type mapping as Parquet's attribute encoding.
func blockpackAnyValue(val any) *tempocommon.AnyValue {
	switch v := val.(type) {
	case string:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: v}}
	case int64:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_IntValue{IntValue: v}}
	case uint64:
		// OTLP attribute uint64 values are non-negative counters bounded well below MaxInt64;
		// the cast is safe in practice and matches proto's int64 AnyValue encoding. //nolint:gosec
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_IntValue{IntValue: int64(v)}}
	case float64:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_DoubleValue{DoubleValue: v}}
	case bool:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BoolValue{BoolValue: v}}
	case []byte:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_BytesValue{BytesValue: v}}
	default:
		return &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: fmt.Sprintf("%v", v)}}
	}
}

// benchmarkBlockpackQuery benchmarks TraceQL filter execution on the blockpack format.
func benchmarkBlockpackQuery(b *testing.B, blockpackPath string, query benchmarkQuery) {
	b.Helper()

	f, err := os.Open(blockpackPath)
	require.NoError(b, err)
	defer f.Close()

	tracked := &trackedFileProvider{file: f, latency: 20 * time.Millisecond}

	b.ResetTimer()
	b.ReportAllocs()

	traceSpans := make(map[string]int)
	traceOrder := make([]string, 0)
	var totalTime int64
	var totalCPUMs float64
	for i := 0; i < b.N; i++ {
		tracked.reset()
		traceSpans = make(map[string]int)
		traceOrder = traceOrder[:0]

		cpuBefore := readCPUSecs()
		start := time.Now()
		bpReader, err := blockpack.NewReaderFromProvider(tracked)
		require.NoError(b, err)
		var bpMatches []blockpack.SpanMatch
		bpMatches, err = blockpack.QueryTraceQL(bpReader, query.traceqlQuery, blockpack.QueryOptions{})
		totalTime += time.Since(start).Nanoseconds()
		cpuAfter := readCPUSecs()
		totalCPUMs += ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
		require.NoError(b, err)
		for _, match := range bpMatches {
			if _, exists := traceSpans[match.TraceID]; !exists {
				traceOrder = append(traceOrder, match.TraceID)
			}
			traceSpans[match.TraceID]++
		}
		if len(traceOrder) == 0 && i == 0 {
			b.Log("Warning: No matches found (blockpack)")
		}
	}

	avgTimeNs := totalTime / int64(b.N)
	bytesRead := tracked.bytesRead
	ioOps := tracked.ioOps

	traceCount := len(traceOrder)
	var spanCount int
	for _, traceID := range traceOrder {
		spanCount += traceSpans[traceID]
	}

	b.ReportMetric(float64(bytesRead), "bytes_read")
	b.ReportMetric(float64(ioOps), "io_ops")
	b.ReportMetric(float64(traceCount), "traces")
	b.ReportMetric(float64(spanCount), "spans")
	b.ReportMetric(totalCPUMs/float64(b.N), "cpuMs")
	if ioOps > 0 {
		b.ReportMetric(float64(bytesRead)/float64(ioOps), "bytes/io")
	}
	ReportCosts(b, ioOps, bytesRead, avgTimeNs)
	b.SetBytes(bytesRead)
}

// instrumentedReader wraps a backend.Reader to track bytes read and I/O operations.
// It also simulates object storage latency to provide realistic benchmark results.
//
// Default latency of 20ms is based on AWS S3 performance characteristics:
// "Typical first-byte latency for S3 is 10-20ms within the same region"
// Reference: https://aws.amazon.com/blogs/machine-learning/applying-data-loading-best-practices-for-ml-training-with-amazon-s3-clients/
type instrumentedReader struct {
	backend.Reader
	bytesRead    *int64
	ioOperations *int64
	latency      time.Duration // Artificial latency per I/O operation (default: 20ms)
}

func (ir *instrumentedReader) Read(
	ctx context.Context,
	name string,
	blockID uuid.UUID,
	tenantID string,
	cacheInfo *backend.CacheInfo,
) ([]byte, error) {
	// Simulate S3 latency
	if ir.latency > 0 {
		time.Sleep(ir.latency)
	}

	data, err := ir.Reader.Read(ctx, name, blockID, tenantID, cacheInfo)
	if err == nil {
		*ir.bytesRead += int64(len(data))
		*ir.ioOperations++
	}
	return data, err
}

func (ir *instrumentedReader) ReadRange(
	ctx context.Context,
	name string,
	blockID uuid.UUID,
	tenantID string,
	offset uint64,
	buffer []byte,
	cacheInfo *backend.CacheInfo,
) error {
	// Simulate S3 latency
	if ir.latency > 0 {
		time.Sleep(ir.latency)
	}

	err := ir.Reader.ReadRange(ctx, name, blockID, tenantID, offset, buffer, cacheInfo)
	if err == nil {
		*ir.bytesRead += int64(len(buffer))
		*ir.ioOperations++
	}
	return err
}

// benchmarkParquetQuery benchmarks TraceQL query execution on Tempo Parquet format
// Opens the block inside the loop for fair comparison with blockpack format
func benchmarkParquetQuery(b *testing.B, meta *backend.BlockMeta, reader backend.Reader, traceqlQuery string) {
	benchmarkParquetQueryWithLimits(b, meta, reader, traceqlQuery, 250000, 10000)
}

// benchmarkParquetQueryWithLimits runs a parquet query benchmark with custom limits
func benchmarkParquetQueryWithLimits(
	b *testing.B,
	meta *backend.BlockMeta,
	reader backend.Reader,
	traceqlQuery string,
	maxTraces, maxSpansPerTrace int,
) {
	ctx := context.Background()
	opts := common.DefaultSearchOptions()

	// Wrap reader to track bytes read and I/O operations with S3 latency simulation
	var bytesRead int64
	var ioOperations int64
	instrumentedRdr := &instrumentedReader{
		Reader:       reader,
		bytesRead:    &bytesRead,
		ioOperations: &ioOperations,
		latency:      20 * time.Millisecond, // Simulate S3 first-byte latency
	}

	b.ResetTimer()
	b.ReportAllocs()

	var resp *tempopb.SearchResponse
	var totalTime int64
	var totalCPUMs float64
	for i := 0; i < b.N; i++ {
		// Reset counters for this iteration
		bytesRead = 0
		ioOperations = 0

		cpuBefore := readCPUSecs()
		start := time.Now()
		// Open block inside loop (fair comparison with blockpack)
		block, err := tempoencoding.OpenBlock(meta, instrumentedRdr)
		if err != nil {
			b.Fatal(err)
		}

		// Create fetcher for the block
		fetcher := tempoql.NewSpansetFetcherWrapper(
			func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
				return block.Fetch(ctx, req, opts)
			},
		)

		engine := tempoql.NewEngine()
		resp, err = engine.ExecuteSearch(ctx, &tempopb.SearchRequest{
			Query:           traceqlQuery,
			Limit:           uint32(maxTraces),
			SpansPerSpanSet: uint32(maxSpansPerTrace),
		}, fetcher, false)
		totalTime += time.Since(start).Nanoseconds()
		cpuAfter := readCPUSecs()
		totalCPUMs += ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
		if err != nil {
			b.Fatal(err)
		}

		if len(resp.Traces) == 0 && i == 0 {
			b.Log("Warning: No matches found")
		}
	}

	if resp == nil {
		b.Fatal("No benchmark iterations ran")
	}

	avgTimeNs := totalTime / int64(b.N)

	// Count total spans across all traces
	totalSpans := 0
	for _, trace := range resp.Traces {
		for _, spanset := range trace.SpanSets {
			totalSpans += len(spanset.Spans)
		}
	}

	// Report metrics
	b.ReportMetric(float64(bytesRead), "bytes_read")
	b.ReportMetric(float64(ioOperations), "io_ops")
	b.ReportMetric(float64(len(resp.Traces)), "traces")
	b.ReportMetric(float64(totalSpans), "spans")
	b.ReportMetric(totalCPUMs/float64(b.N), "cpuMs")

	// Report I/O efficiency metric
	if ioOperations > 0 {
		bytesPerIO := float64(bytesRead) / float64(ioOperations)
		b.ReportMetric(bytesPerIO, "bytes/io")
	}

	// Report AWS cost metrics
	ReportCosts(b, ioOperations, bytesRead, avgTimeNs)

	b.SetBytes(bytesRead)
}

func executeTempoSearchResponse(
	meta *backend.BlockMeta,
	reader backend.Reader,
	traceqlQuery string,
) (*tempopb.SearchResponse, error) {
	ctx := context.Background()

	// Open block from metadata
	block, err := tempoencoding.OpenBlock(meta, reader)
	if err != nil {
		return nil, err
	}

	opts := common.DefaultSearchOptions()
	fetcher := tempoql.NewSpansetFetcherWrapper(
		func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
			return block.Fetch(ctx, req, opts)
		},
	)
	engine := tempoql.NewEngine()
	// Use complete=true to populate full span details including names.
	return engine.ExecuteSearch(ctx, &tempopb.SearchRequest{
		Query:           traceqlQuery,
		Limit:           250000, // Large enough to capture all results
		SpansPerSpanSet: 10000,  // Return all matching spans per trace (not just 3)
	}, fetcher, true)
}

func normalizeSpanID(raw string) string {
	rawID := strings.TrimSpace(raw)
	if rawID == "" {
		rawID = "0"
	}
	rawID = strings.TrimPrefix(rawID, "0x")
	if len(rawID)%2 != 0 {
		rawID = "0" + rawID
	}
	spanBytes, err := hex.DecodeString(rawID)
	if err != nil {
		return rawID
	}
	if len(spanBytes) > 8 {
		return hex.EncodeToString(spanBytes)
	}
	var normalized [8]byte
	copy(normalized[8-len(spanBytes):], spanBytes)
	return hex.EncodeToString(normalized[:])
}

func normalizeTraceID(raw string) (string, bool) {
	rawID := strings.TrimSpace(raw)
	if rawID == "" {
		rawID = "0"
	}
	rawID = strings.TrimPrefix(rawID, "0x")
	if len(rawID)%2 != 0 {
		rawID = "0" + rawID
	}
	traceBytes, err := hex.DecodeString(rawID)
	if err != nil {
		return "", false
	}
	if len(traceBytes) > 16 {
		return "", false
	}
	var normalized [16]byte
	copy(normalized[16-len(traceBytes):], traceBytes)
	return hex.EncodeToString(normalized[:]), true
}

// validateQueryResults checks that blockpack and parquet return identical span sets
// (per trace: same trace IDs, same span IDs within each trace).
func validateQueryResults(
	tb testing.TB,
	blockpackPath string,
	meta *backend.BlockMeta,
	reader backend.Reader,
	query benchmarkQuery,
) {
	tb.Helper()

	blockpackResp, err := executeBlockpackSearch(blockpackPath, query.traceqlQuery)
	require.NoError(tb, err)

	parquetResp, err := executeTempoSearchResponse(meta, reader, query.traceqlQuery)
	require.NoError(tb, err)

	blockpackResp.Metrics = nil
	parquetResp.Metrics = nil

	if err := compareSpanResults(blockpackResp, parquetResp); err != nil {
		tb.Fatalf("Query %q: span-level mismatch: %v", query.name, err)
	}

	// Structural queries are only meaningful when at least one engine returns non-empty results.
	// If both return nothing the comparison is vacuously true and provides no signal.
	if query.isStructural && len(blockpackResp.Traces) == 0 {
		tb.Errorf("Query %q: structural query returned no results; test data may be insufficient", query.name)
	}

	tb.Logf("✓ Validation passed: %d traces match", len(blockpackResp.Traces))
}

// compareTraceMetadata compares trace-level metadata (trace IDs and total span counts).
// Used when engines may differ in which spans they select per trace (e.g. modules vs blockpack),
// so only the set of matching traces and their aggregate span counts are compared.
func compareTraceMetadata(blockpackResp, parquetResp *tempopb.SearchResponse) error {
	if blockpackResp == nil && parquetResp == nil {
		return nil
	}
	if blockpackResp == nil {
		return fmt.Errorf("blockpack response is nil, parquet has %d traces", len(parquetResp.Traces))
	}
	if parquetResp == nil {
		return fmt.Errorf("parquet response is nil, blockpack has %d traces", len(blockpackResp.Traces))
	}

	// Build maps of trace IDs
	blockpackTraces := make(map[string]bool)
	for _, trace := range blockpackResp.Traces {
		if trace != nil {
			normalized, ok := normalizeTraceID(trace.TraceID)
			if ok {
				blockpackTraces[normalized] = true
			}
		}
	}

	parquetTraces := make(map[string]bool)
	for _, trace := range parquetResp.Traces {
		if trace != nil {
			normalized, ok := normalizeTraceID(trace.TraceID)
			if ok {
				parquetTraces[normalized] = true
			}
		}
	}

	// Check trace count
	if len(blockpackTraces) != len(parquetTraces) {
		return fmt.Errorf("trace count mismatch: blockpack=%d parquet=%d", len(blockpackTraces), len(parquetTraces))
	}

	// Check that all traces are present in both
	for traceID := range blockpackTraces {
		if !parquetTraces[traceID] {
			return fmt.Errorf("trace %s found in blockpack but not in parquet", traceID)
		}
	}
	for traceID := range parquetTraces {
		if !blockpackTraces[traceID] {
			return fmt.Errorf("trace %s found in parquet but not in blockpack", traceID)
		}
	}

	// Check total span counts match
	var blockpackSpanCount int
	for _, trace := range blockpackResp.Traces {
		for _, ss := range trace.GetSpanSets() {
			blockpackSpanCount += int(ss.Matched)
		}
	}

	var parquetSpanCount int
	for _, trace := range parquetResp.Traces {
		for _, ss := range trace.GetSpanSets() {
			parquetSpanCount += int(ss.Matched)
		}
	}

	if blockpackSpanCount != parquetSpanCount {
		return fmt.Errorf("span count mismatch: blockpack=%d parquet=%d", blockpackSpanCount, parquetSpanCount)
	}

	return nil
}

// compareSpanResults compares two search responses at the span level.
// For each trace it asserts the exact set of span IDs matches between blockpack and parquet.
func compareSpanResults(blockpackResp, parquetResp *tempopb.SearchResponse) error {
	if blockpackResp == nil && parquetResp == nil {
		return nil
	}
	if blockpackResp == nil {
		return fmt.Errorf("blockpack response is nil, parquet has %d traces", len(parquetResp.Traces))
	}
	if parquetResp == nil {
		return fmt.Errorf("parquet response is nil, blockpack has %d traces", len(blockpackResp.Traces))
	}

	bpMap := buildSpanMap(blockpackResp)
	pqMap := buildSpanMap(parquetResp)

	if len(bpMap) != len(pqMap) {
		return fmt.Errorf("trace count mismatch: blockpack=%d parquet=%d", len(bpMap), len(pqMap))
	}

	for traceID, bpSpans := range bpMap {
		pqSpans, ok := pqMap[traceID]
		if !ok {
			return fmt.Errorf("trace %s found in blockpack but not in parquet", traceID)
		}
		if len(bpSpans) != len(pqSpans) {
			return fmt.Errorf("trace %s: span count mismatch: blockpack=%d parquet=%d",
				traceID, len(bpSpans), len(pqSpans))
		}
		for spanID := range bpSpans {
			if !pqSpans[spanID] {
				return fmt.Errorf("trace %s: span %s found in blockpack but not in parquet", traceID, spanID)
			}
		}
	}
	for traceID := range pqMap {
		if _, ok := bpMap[traceID]; !ok {
			return fmt.Errorf("trace %s found in parquet but not in blockpack", traceID)
		}
	}
	return nil
}

// buildSpanMap builds a map[normalizedTraceID]map[normalizedSpanID]bool from a search response.
func buildSpanMap(resp *tempopb.SearchResponse) map[string]map[string]bool {
	if resp == nil {
		return nil
	}
	result := make(map[string]map[string]bool, len(resp.Traces))
	for _, trace := range resp.Traces {
		if trace == nil {
			continue
		}
		traceID, ok := normalizeTraceID(trace.TraceID)
		if !ok {
			continue
		}
		spanSet := make(map[string]bool)
		for _, ss := range trace.SpanSets {
			for _, span := range ss.Spans {
				if span != nil && span.SpanID != "" {
					spanSet[normalizeSpanID(span.SpanID)] = true
				}
			}
		}
		result[traceID] = spanSet
	}
	return result
}

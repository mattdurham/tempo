// Package main .
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/grafana/blockpack/internal/otlpconvert"
	"github.com/grafana/blockpack/internal/parquetconv"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func main() {
	numTraces := flag.Int("traces", 191119, "Number of traces to generate")
	blockpackOut := flag.String("blockpack", "traces.blockpack", "Output blockpack file")
	parquetOut := flag.String("parquet", "traces.parquet", "Output parquet directory")
	seed := flag.Int64("seed", 42, "Random seed for deterministic generation")
	flag.Parse()

	if err := generateTestData(*numTraces, *blockpackOut, *parquetOut, *seed); err != nil {
		log.Fatalf("Failed to generate test data: %v", err)
	}
}

func generateTestData(numTraces int, blockpackPath, parquetPath string, seed int64) error {
	log.Printf("Generating %d test traces...", numTraces)
	log.Printf("  Random seed: %d", seed)
	startTime := time.Now()

	// Generate OTLP traces
	traces := make([]*tracev1.TracesData, 0, numTraces)
	baseTime := time.Now().Add(-12 * time.Hour).Truncate(time.Second)

	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // Reviewed and acceptable

	for i := 0; i < numTraces; i++ {
		if i > 0 && i%50000 == 0 {
			log.Printf("Generated %d/%d traces...", i, numTraces)
		}

		trace := generateTrace(rng, i, baseTime.Add(time.Duration(i)*time.Second))
		traces = append(traces, trace)
	}

	totalSpans := 0
	for _, trace := range traces {
		for _, rs := range trace.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				totalSpans += len(ss.Spans)
			}
		}
	}

	log.Printf("Generated %d traces with %d total spans", numTraces, totalSpans)
	log.Printf("Average spans per trace: %.2f", float64(totalSpans)/float64(numTraces))

	// Write to blockpack
	log.Printf("Writing blockpack to %s...", blockpackPath)
	blockpackData, err := otlpconvert.WriteBlockpack(traces, 2000)
	if err != nil {
		return fmt.Errorf("failed to write blockpack: %w", err)
	}

	if err := os.WriteFile(blockpackPath, blockpackData, 0o644); err != nil { //nolint:gosec,govet // Reviewed and acceptable
		return fmt.Errorf("failed to write blockpack file: %w", err)
	}

	blockpackSizeMB := float64(len(blockpackData)) / 1024 / 1024
	log.Printf("✓ Blockpack written: %.2f MB", blockpackSizeMB)

	// Write to parquet
	log.Printf("Writing parquet to %s...", parquetPath)
	parquetSize, err := parquetconv.WriteParquet(traces, parquetPath)
	if err != nil {
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	parquetSizeMB := float64(parquetSize) / 1024 / 1024
	log.Printf("✓ Parquet written: %.2f MB", parquetSizeMB)

	elapsed := time.Since(startTime)
	log.Printf("\n=== Generation Summary ===")
	log.Printf("Traces: %d", numTraces)
	log.Printf("Spans: %d", totalSpans)
	log.Printf("Blockpack: %.2f MB (%s)", blockpackSizeMB, blockpackPath)
	log.Printf("Parquet: %.2f MB (%s)", parquetSizeMB, parquetPath)
	log.Printf("Time: %v", elapsed.Round(10*time.Millisecond))
	log.Printf("Compression ratio: %.2f%% (parquet vs blockpack)", (1.0-blockpackSizeMB/parquetSizeMB)*100)

	return nil
}

// Service definitions for realistic traces
var services = []string{
	"frontend", "frontend-proxy", "frontend-web",
	"cart", "checkout", "payment",
	"productcatalogservice", "product-catalog", "currency",
	"recommendation", "ad-service",
	"shipping", "email",
	"image-provider", "flagd",
	"redis", "valkey", // Dedicated cache services
}

var operations = map[string][]string{
	"frontend":       {"HTTP GET /", "HTTP GET /product", "HTTP GET /cart", "HTTP POST /checkout"},
	"frontend-proxy": {"proxy.request", "proxy.forward"},
	"cart":           {"cart.add", "cart.get", "cart.clear"},
	"checkout":       {"checkout.place", "checkout.validate"},
	"payment":        {"payment.charge", "payment.validate"},
	"productcatalogservice": {
		"ProductCatalogService.GetProduct",
		"ProductCatalogService.ListProducts",
		"ProductCatalogService.SearchProducts",
	},
	"currency":       {"GetSupportedCurrencies", "Convert"},
	"recommendation": {"GetRecommendations", "ListRecommendations"},
	"shipping":       {"GetQuote", "ShipOrder"},
	"image-provider": {"GetImage", "OptimizeImage"},
	"redis":          {"GET", "SET", "DEL", "HGET", "HSET"},
	"valkey":         {"GET", "SET", "DEL", "HGET", "HSET"},
}

func generateTrace(rng *rand.Rand, traceIdx int, startTime time.Time) *tracev1.TracesData {
	traceID := generateRandomBytes(16, traceIdx)

	// Determine trace pattern (1-6 spans per trace for variety)
	numSpans := 1 + (traceIdx % 6)

	spans := make([]*tracev1.Span, numSpans)
	currentTime := startTime
	var parentSpanID []byte

	// Select service for this trace
	service := services[traceIdx%len(services)]
	ops := operations[service]
	if ops == nil {
		ops = []string{"operation"}
	}

	for i := 0; i < numSpans; i++ {
		spanID := generateRandomBytes(8, traceIdx*1000+i)

		// Duration distribution (deterministic)
		// 80% normal (10-500ms), 10% fast (<10ms), 10% slow (>500ms, up to 2s)
		durationPattern := (traceIdx*numSpans + i) % 10
		var duration time.Duration
		switch durationPattern {
		case 0:
			// 10% fast operations: 1-9ms
			duration = time.Duration(1+rng.Intn(9)) * time.Millisecond
		case 1:
			// 10% slow operations: 500ms-2s
			duration = time.Duration(500+rng.Intn(1500)) * time.Millisecond
		default:
			// 80% normal operations: 10-500ms
			duration = time.Duration(10+rng.Intn(490)) * time.Millisecond
		}

		operation := ops[i%len(ops)]

		// Determine span type based on span index (deterministic distribution)
		spanType := (traceIdx*numSpans + i) % 10

		var attributes []*commonv1.KeyValue
		var kind tracev1.Span_SpanKind
		var statusCode tracev1.Status_StatusCode

		switch spanType {
		case 0, 1: // 20% HTTP GET requests (success)
			kind = tracev1.Span_SPAN_KIND_SERVER
			statusCode = tracev1.Status_STATUS_CODE_OK
			attributes = []*commonv1.KeyValue{
				{
					Key:   "http.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
				},
				{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}}},
				{
					Key:   "http.url",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "/api/v1/" + service}},
				},
			}

		case 2: // 10% HTTP POST requests
			kind = tracev1.Span_SPAN_KIND_SERVER
			statusCode = tracev1.Status_STATUS_CODE_OK
			attributes = []*commonv1.KeyValue{
				{
					Key:   "http.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "POST"}},
				},
				{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 201}}},
				{
					Key:   "http.url",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "/api/v1/" + service}},
				},
			}

		case 3: // 10% HTTP client spans
			kind = tracev1.Span_SPAN_KIND_CLIENT
			statusCode = tracev1.Status_STATUS_CODE_OK
			attributes = []*commonv1.KeyValue{
				{
					Key:   "http.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
				},
				{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 200}}},
				{
					Key: "http.url",
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{StringValue: "http://internal-service/" + operation},
					},
				},
			}

		case 4: // 10% HTTP errors (500)
			kind = tracev1.Span_SPAN_KIND_SERVER
			statusCode = tracev1.Status_STATUS_CODE_ERROR
			attributes = []*commonv1.KeyValue{
				{
					Key:   "http.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
				},
				{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 500}}},
				{
					Key:   "http.url",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "/api/v1/" + service}},
				},
			}

		case 5: // 10% HTTP errors (404)
			kind = tracev1.Span_SPAN_KIND_SERVER
			statusCode = tracev1.Status_STATUS_CODE_ERROR
			attributes = []*commonv1.KeyValue{
				{
					Key:   "http.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "GET"}},
				},
				{Key: "http.status_code", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 404}}},
				{
					Key:   "http.url",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "/api/v1/notfound"}},
				},
			}

		case 6: // 10% gRPC server spans
			kind = tracev1.Span_SPAN_KIND_SERVER
			statusCode = tracev1.Status_STATUS_CODE_OK
			attributes = []*commonv1.KeyValue{
				{
					Key:   "rpc.system",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "grpc"}},
				},
				{
					Key:   "rpc.service",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: service}},
				},
				{
					Key:   "rpc.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: operation}},
				},
				{
					Key:   "rpc.grpc.status_code",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 0}},
				},
			}
			// Make some gRPC spans slow for slow-grpc-calls query
			if (traceIdx+i)%4 == 0 && duration < 600*time.Millisecond {
				duration = time.Duration(600+rng.Intn(1400)) * time.Millisecond
			}

		case 7: // 10% gRPC client spans
			kind = tracev1.Span_SPAN_KIND_CLIENT
			statusCode = tracev1.Status_STATUS_CODE_OK
			attributes = []*commonv1.KeyValue{
				{
					Key:   "rpc.system",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "grpc"}},
				},
				{
					Key:   "rpc.service",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: service}},
				},
				{
					Key:   "rpc.method",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: operation}},
				},
				{
					Key:   "rpc.grpc.status_code",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 0}},
				},
			}
			// Make some gRPC spans slow for slow-grpc-calls query
			if (traceIdx+i)%4 == 0 && duration < 600*time.Millisecond {
				duration = time.Duration(600+rng.Intn(1400)) * time.Millisecond
			}

		case 8: // 10% Redis operations
			kind = tracev1.Span_SPAN_KIND_CLIENT
			statusCode = tracev1.Status_STATUS_CODE_OK
			redisOp := []string{"GET", "SET", "DEL", "HGET", "HSET"}[i%5] //nolint:gosec
			attributes = []*commonv1.KeyValue{
				{
					Key:   "db.system",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "redis"}},
				},
				{
					Key:   "db.operation",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: redisOp}},
				},
				{
					Key:   "db.redis.database_index",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 0}},
				},
			}

		case 9: // 10% Database operations
			kind = tracev1.Span_SPAN_KIND_CLIENT
			statusCode = tracev1.Status_STATUS_CODE_OK
			dbOp := []string{"SELECT", "INSERT", "UPDATE", "DELETE"}[i%4] //nolint:gosec
			attributes = []*commonv1.KeyValue{
				{
					Key:   "db.system",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "postgresql"}},
				},
				{
					Key:   "db.operation",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: dbOp}},
				},
				{Key: "db.name", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "shop"}}},
				{
					Key: "db.statement",
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{StringValue: dbOp + " FROM products"},
					},
				},
			}
		}

		span := &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			ParentSpanId:      parentSpanID,
			Name:              operation,
			Kind:              kind,
			StartTimeUnixNano: uint64(currentTime.UnixNano()),               //nolint:gosec
			EndTimeUnixNano:   uint64(currentTime.Add(duration).UnixNano()), //nolint:gosec
			Attributes:        attributes,
			Status: &tracev1.Status{
				Code: statusCode,
			},
		}

		spans[i] = span
		parentSpanID = spanID // Next span will be child of this one
		currentTime = currentTime.Add(duration)
	}

	// Create resource spans
	resourceSpans := &tracev1.ResourceSpans{
		Resource: &resourcev1.Resource{
			Attributes: []*commonv1.KeyValue{
				{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: service}},
				},
				{
					Key:   "service.namespace",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "shop"}},
				},
			},
		},
		ScopeSpans: []*tracev1.ScopeSpans{
			{
				Scope: &commonv1.InstrumentationScope{
					Name:    "go.opentelemetry.io/otel/sdk/tracer",
					Version: "1.0.0",
				},
				Spans: spans,
			},
		},
	}

	return &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{resourceSpans},
	}
}

func generateRandomBytes(length, seed int) []byte {
	r := rand.New(rand.NewSource(int64(seed))) //nolint:gosec
	b := make([]byte, length)
	r.Read(b)
	return b
}

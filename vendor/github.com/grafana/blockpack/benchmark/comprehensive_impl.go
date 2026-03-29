package benchmark

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/blockpack/internal/parquetconv"
	"github.com/grafana/tempo/pkg/tempopb"
	tempoql "github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	tempoencoding "github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// ComprehensiveQuery represents a query to benchmark
type ComprehensiveQuery struct {
	name    string
	traceql string
}

// TestDataset holds generated test data for benchmarking
type TestDataset struct {
	Cleanup       func()
	ParquetPath   string
	Traces        []*tracev1.TracesData
	BlockpackPath string
	TraceIDs      []string // Extracted trace IDs for trace-by-id queries
	SpanCount     int
}

// E-commerce microservices
var ecommerceServices = []string{
	"frontend", "frontend-proxy", "checkout-service", "payment-service",
	"currency-service", "shipping-service", "cart-service", "product-catalog",
	"ad-service", "redis-cache", "email-service", "recommendation-engine",
}

// Operation names per service
var ecommerceOperations = map[string][]string{
	"frontend":       {"GET /", "GET /product/:id", "GET /cart", "POST /checkout", "GET /search"},
	"frontend-proxy": {"ProxyRequest", "LoadBalancer/RouteRequest", "RateLimiter/Check"},
	"checkout-service": {
		"CheckoutService/PlaceOrder",
		"CheckoutService/ValidateCart",
		"CheckoutService/CalculateTotal",
	},
	"payment-service":  {"PaymentService/Charge", "PaymentService/Validate", "PaymentService/Refund"},
	"currency-service": {"CurrencyService/Convert", "CurrencyService/GetRates"},
	"shipping-service": {
		"ShippingService/GetQuote",
		"ShippingService/CreateShipment",
		"ShippingService/TrackPackage",
	},
	"cart-service": {
		"CartService/GetCart",
		"CartService/AddItem",
		"CartService/RemoveItem",
		"CartService/EmptyCart",
	},
	"product-catalog": {
		"ProductCatalogService/GetProduct",
		"ProductCatalogService/SearchProducts",
		"ProductCatalogService/ListProducts",
	},
	"ad-service":            {"AdService/GetAds", "AdService/TrackClick"},
	"redis-cache":           {"Redis/Get", "Redis/Set", "Redis/Delete"},
	"email-service":         {"EmailService/SendConfirmation", "EmailService/SendShipmentNotification"},
	"recommendation-engine": {"RecommendationService/GetRecommendations", "RecommendationService/TrackView"},
}

// HTTP methods and status codes
var (
	httpMethods     = []string{"GET", "POST", "PUT", "DELETE"}
	httpStatusCodes = []int{200, 200, 200, 200, 200, 200, 200, 200, 201, 204, 400, 404, 500, 503}
)

// RPC and DB systems
var (
	rpcSystems = []string{"grpc", "http"}
	dbSystems  = []string{"redis", "postgresql", "mongodb"}
)

// GenerateComprehensiveTestData creates a realistic e-commerce dataset
//
//nolint:deadcode // Public utility for external benchmark scripts
func GenerateComprehensiveTestData(b *testing.B, traceCount int) *TestDataset {
	b.Helper()

	// Generate realistic e-commerce traces with 12 microservices
	traces := generateEcommerceLikeTraces(traceCount)

	// Count total spans
	spanCount := 0
	traceIDs := make([]string, 0, traceCount)
	for _, trace := range traces {
		for _, rs := range trace.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				spanCount += len(ss.Spans)
				// Extract trace IDs
				if len(ss.Spans) > 0 && len(ss.Spans[0].TraceId) > 0 {
					traceID := hex.EncodeToString(ss.Spans[0].TraceId)
					traceIDs = append(traceIDs, traceID)
				}
			}
		}
	}

	b.Logf("Generated %d traces with %d spans", traceCount, spanCount)

	// Write to blockpack file
	tmpDir := b.TempDir()
	blockpackPath := filepath.Join(tmpDir, "test.blockpack")
	f, err := os.Create(blockpackPath)
	if err != nil {
		b.Fatal(err)
	}
	writer, err := blockpack.NewWriter(f, 10000)
	if err != nil {
		b.Fatal(err)
	}
	for _, trace := range traces {
		if err := writer.AddTracesData(trace); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := writer.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("failed to close blockpack file: %v", err)
	}

	// Write to parquet (optional, for future parquet benchmarks)
	parquetPath := filepath.Join(tmpDir, "test.parquet")
	_, err = parquetconv.WriteParquet(traces, parquetPath)
	if err != nil {
		b.Logf("Warning: Failed to write parquet: %v", err)
		parquetPath = ""
	}

	return &TestDataset{
		Traces:        traces,
		BlockpackPath: blockpackPath,
		ParquetPath:   parquetPath,
		TraceIDs:      traceIDs,
		SpanCount:     spanCount,
		Cleanup:       func() {},
	}
}

// QueryResult holds results from executing a query
type QueryResult struct {
	RowCount      int
	Elapsed       time.Duration
	IOOps         int
	BytesRead     int64
	BlocksScanned int
	TraceCount    int
}

// ExecuteBlockpackQuery runs a TraceQL query against blockpack data
// Compiles TraceQL natively without manual SQL conversion
//
//nolint:deadcode // Public utility for external benchmark scripts
func ExecuteBlockpackQuery(b *testing.B, blockpackPath string, traceql string) *QueryResult {
	b.Helper()
	start := time.Now()

	f, err := os.Open(blockpackPath)
	if err != nil {
		b.Fatalf("Failed to open blockpack file: %v (path: %s)", err, blockpackPath)
	}
	defer f.Close()

	tracked := &trackedFileProvider{file: f, latency: 20 * time.Millisecond}
	r, err := blockpack.NewReaderFromProvider(tracked)
	if err != nil {
		b.Fatalf("Failed to create reader: %v (path: %s)", err, blockpackPath)
	}

	traceSpans := make(map[string]int)
	traceOrder := make([]string, 0)
	matches, err := blockpack.QueryTraceQL(r, traceql, blockpack.QueryOptions{})
	if err != nil {
		b.Fatalf("Query failed: %v (query: %s)", err, traceql)
	}
	for _, match := range matches {
		if _, exists := traceSpans[match.TraceID]; !exists {
			traceOrder = append(traceOrder, match.TraceID)
		}
		traceSpans[match.TraceID]++
	}

	elapsed := time.Since(start)

	rowCount := 0
	for _, traceID := range traceOrder {
		rowCount += traceSpans[traceID]
	}

	return &QueryResult{
		RowCount:   rowCount,
		Elapsed:    elapsed,
		BytesRead:  tracked.bytesRead,
		IOOps:      int(tracked.ioOps), //nolint:gosec
		TraceCount: len(traceOrder),
	}
}

// ExecuteParquetQuery runs a TraceQL query against parquet data
// NOTE: Requires Tempo's parquet backend to execute TraceQL queries
//
//nolint:deadcode // Public utility for external benchmark scripts
func ExecuteParquetQuery(b *testing.B, parquetPath string, traceql string) *QueryResult {
	b.Helper()

	ctx := context.Background()
	opts := common.DefaultSearchOptions()

	// Get block ID from the parquet directory structure
	// parquetPath is like /tmp/xyz/default/{blockID}/...
	// We need to extract the blockID
	tenant := "default" // otlpconvert.WriteParquet uses "default"

	// Find the block ID by listing directories in the tenant folder
	// parquetPath is the base directory where WriteParquet wrote to
	tenantPath := filepath.Join(parquetPath, tenant)
	entries, err := os.ReadDir(tenantPath)
	if err != nil {
		b.Fatal(err)
	}
	if len(entries) == 0 {
		b.Fatal("No blocks found in tenant directory")
	}

	blockIDStr := entries[0].Name()
	blockID, err := uuid.Parse(blockIDStr)
	if err != nil {
		b.Fatal(err)
	}

	// Open parquet reader (parquetPath is the base directory)
	rawReader, _, _, err := local.New(&local.Config{
		Path: parquetPath,
	})
	if err != nil {
		b.Fatal(err)
	}
	reader := backend.NewReader(rawReader)

	// Load block metadata from the backend
	meta, err := reader.BlockMeta(ctx, blockID, tenant)
	if err != nil {
		b.Fatal(err)
	}

	// Track I/O metrics
	var bytesRead int64
	var ioOperations int64
	instrumentedRdr := &instrumentedReader{
		Reader:       reader,
		bytesRead:    &bytesRead,
		ioOperations: &ioOperations,
		latency:      20 * time.Millisecond, // S3 latency simulation
	}

	// Open the parquet block
	block, err := tempoencoding.OpenBlock(meta, instrumentedRdr)
	if err != nil {
		b.Fatal(err)
	}

	// Create fetcher for TraceQL
	fetcher := tempoql.NewSpansetFetcherWrapper(
		func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
			return block.Fetch(ctx, req, opts)
		},
	)

	// Execute TraceQL query
	start := time.Now()
	engine := tempoql.NewEngine()
	resp, err := engine.ExecuteSearch(ctx, &tempopb.SearchRequest{
		Query:           traceql,
		Limit:           250000,
		SpansPerSpanSet: 10000,
	}, fetcher, false)
	elapsed := time.Since(start)

	if err != nil {
		b.Fatal(err)
	}

	// Count spans
	totalSpans := 0
	for _, trace := range resp.Traces {
		for _, ss := range trace.SpanSets {
			totalSpans += len(ss.Spans)
		}
	}

	return &QueryResult{
		RowCount:   totalSpans,
		Elapsed:    elapsed,
		BytesRead:  bytesRead,
		IOOps:      int(ioOperations),
		TraceCount: len(resp.Traces),
	}
}

// BenchmarkQueryPair runs a single query against blockpack and parquet formats
// and validates that both return the same results
//
//nolint:deadcode // Public utility for external benchmark scripts
func BenchmarkQueryPair(b *testing.B, dataset *TestDataset, query ComprehensiveQuery) {
	var blockpackResult, parquetResult *QueryResult

	b.Run(query.name, func(b *testing.B) {
		// Blockpack
		b.Run("blockpack", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var result *QueryResult
			for i := 0; i < b.N; i++ {
				result = ExecuteBlockpackQuery(b, dataset.BlockpackPath, query.traceql)
			}
			blockpackResult = result

			// Report metrics after all iterations in format expected by unified report
			if result != nil {
				b.ReportMetric(float64(result.RowCount), "spans") // Report as "spans" for unified report
				b.ReportMetric(float64(result.BlocksScanned), "blocks")
				b.ReportMetric(float64(result.BytesRead), "bytes_read")
				b.ReportMetric(float64(result.IOOps), "io_ops")
			}
		})

		// Parquet
		b.Run("parquet", func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			var result *QueryResult
			for i := 0; i < b.N; i++ {
				result = ExecuteParquetQuery(b, dataset.ParquetPath, query.traceql)
			}
			parquetResult = result

			// Report metrics after all iterations
			if result != nil {
				b.ReportMetric(float64(result.RowCount), "spans")
				b.ReportMetric(float64(result.BytesRead), "bytes_read")
				b.ReportMetric(float64(result.IOOps), "io_ops")
				b.ReportMetric(float64(result.TraceCount), "traces")
			}
		})

		// Validate results match between formats
		if blockpackResult != nil && parquetResult != nil {
			if blockpackResult.RowCount != parquetResult.RowCount {
				b.Errorf("Query %q: span count mismatch: blockpack=%d parquet=%d",
					query.name, blockpackResult.RowCount, parquetResult.RowCount)
			}
		}
	})
}

// generateEcommerceLikeTraces creates realistic e-commerce traces
// Generates traces with 12 microservices and realistic operation patterns
//
//nolint:deadcode // Helper for GenerateComprehensiveTestData
func generateEcommerceLikeTraces(count int) []*tracev1.TracesData {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	baseTime := time.Now().Truncate(time.Hour)

	traces := make([]*tracev1.TracesData, 0, count)
	for i := 0; i < count; i++ {
		trace := generateEcommerceTrace(rng, i, baseTime)
		traces = append(traces, trace)
	}

	return traces
}

// generateEcommerceTrace generates a single realistic e-commerce trace
//
//nolint:deadcode // Helper for generateEcommerceLikeTraces
func generateEcommerceTrace(rng *rand.Rand, traceID int, baseTime time.Time) *tracev1.TracesData {
	serviceIdx := rng.Intn(len(ecommerceServices))
	serviceName := ecommerceServices[serviceIdx]
	spanCount := 1 + rng.Intn(10)

	traces := &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key:   "service.name",
							Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: serviceName}},
						},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{Spans: make([]*tracev1.Span, 0, spanCount)},
				},
			},
		},
	}

	// Generate proper 16-byte trace ID using binary encoding
	traceIDBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(traceIDBytes[0:8], uint64(traceID))
	binary.BigEndian.PutUint64(traceIDBytes[8:16], uint64(rng.Int63()))

	currentTime := baseTime.Add(time.Duration(traceID) * time.Second)
	for i := 0; i < spanCount; i++ {
		span := generateEcommerceSpan(rng, serviceName, traceIDBytes, i, currentTime)
		traces.ResourceSpans[0].ScopeSpans[0].Spans = append(traces.ResourceSpans[0].ScopeSpans[0].Spans, span)
		currentTime = currentTime.Add(time.Duration(span.EndTimeUnixNano - span.StartTimeUnixNano))
	}

	return traces
}

// generateEcommerceSpan generates a single span with realistic attributes
//
//nolint:deadcode // Helper for generateEcommerceTrace
func generateEcommerceSpan(
	rng *rand.Rand,
	serviceName string,
	traceID []byte,
	spanIdx int,
	startTime time.Time,
) *tracev1.Span {
	operations := ecommerceOperations[serviceName]
	if len(operations) == 0 {
		operations = []string{"UnknownOperation"}
	}
	operationName := operations[rng.Intn(len(operations))]

	// Duration distribution: 70% fast, 25% medium, 5% slow
	var durationNs uint64
	r := rng.Float64()
	if r < 0.7 {
		durationNs = uint64(100000 + rng.Int63n(100000000)) // 100us - 100ms
	} else if r < 0.95 {
		durationNs = uint64(100000000 + rng.Int63n(900000000)) // 100ms - 1s
	} else {
		durationNs = uint64(1000000000 + rng.Int63n(9000000000)) // 1s - 10s
	}

	startTimeNs := uint64(startTime.UnixNano())
	endTimeNs := startTimeNs + durationNs

	// Span kind distribution
	var kind tracev1.Span_SpanKind
	kindRoll := rng.Float64()
	if kindRoll < 0.4 {
		kind = tracev1.Span_SPAN_KIND_SERVER
	} else if kindRoll < 0.7 {
		kind = tracev1.Span_SPAN_KIND_CLIENT
	} else if kindRoll < 0.85 {
		kind = tracev1.Span_SPAN_KIND_INTERNAL
	} else if kindRoll < 0.95 {
		kind = tracev1.Span_SPAN_KIND_PRODUCER
	} else {
		kind = tracev1.Span_SPAN_KIND_CONSUMER
	}

	// Status: 90% OK, 10% ERROR
	var statusCode tracev1.Status_StatusCode
	if rng.Float64() < 0.9 {
		statusCode = tracev1.Status_STATUS_CODE_OK
	} else {
		statusCode = tracev1.Status_STATUS_CODE_ERROR
	}

	// Generate proper 8-byte span ID using binary encoding
	spanIDBytes := make([]byte, 8)
	binary.BigEndian.PutUint32(spanIDBytes[0:4], uint32(spanIdx))
	binary.BigEndian.PutUint32(spanIDBytes[4:8], rng.Uint32())

	span := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            spanIDBytes,
		Name:              operationName,
		Kind:              kind,
		StartTimeUnixNano: startTimeNs,
		EndTimeUnixNano:   endTimeNs,
		Status:            &tracev1.Status{Code: statusCode},
		Attributes:        []*commonv1.KeyValue{},
	}

	// Add HTTP attributes for HTTP operations
	if strings.Contains(operationName, "GET") || strings.Contains(operationName, "POST") ||
		strings.Contains(operationName, "PUT") || strings.Contains(operationName, "DELETE") {
		method := httpMethods[rng.Intn(len(httpMethods))]
		span.Attributes = append(span.Attributes, &commonv1.KeyValue{
			Key:   "http.method",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: method}},
		})

		statusCodeValue := httpStatusCodes[rng.Intn(len(httpStatusCodes))]
		span.Attributes = append(span.Attributes, &commonv1.KeyValue{
			Key:   "http.status_code",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(statusCodeValue)}},
		})
	}

	// Add RPC attributes for gRPC services
	if strings.Contains(serviceName, "service") || rng.Float64() < 0.3 {
		rpcSystem := rpcSystems[rng.Intn(len(rpcSystems))]
		span.Attributes = append(span.Attributes, &commonv1.KeyValue{
			Key:   "rpc.system",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: rpcSystem}},
		})
	}

	// Add DB attributes for DB operations
	if strings.Contains(serviceName, "redis") || strings.Contains(serviceName, "cache") ||
		strings.Contains(serviceName, "cart") || strings.Contains(serviceName, "product") {
		dbSystem := dbSystems[rng.Intn(len(dbSystems))]
		span.Attributes = append(span.Attributes, &commonv1.KeyValue{
			Key:   "db.system",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: dbSystem}},
		})
	}

	// Add custom request ID for full scan tests (1% of spans)
	if rng.Float64() < 0.01 {
		span.Attributes = append(span.Attributes, &commonv1.KeyValue{
			Key: "custom.request_id",
			Value: &commonv1.AnyValue{
				Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("req-%d", rng.Int63())},
			},
		})
	}

	return span
}

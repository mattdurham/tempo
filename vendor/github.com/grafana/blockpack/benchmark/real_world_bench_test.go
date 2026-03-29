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
	"os"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempoql "github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	tempoencoding "github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// realWorldBenchmarkQueries mirrors the type of queries seen in production e-commerce systems.
// All queries use the xk6-generated data schema (shop-backend, auth-service, article-service, etc.).
var realWorldBenchmarkQueries = []struct {
	name     string
	traceql  string
	category string
}{
	// Equality queries on dedicated columns (fast with dedicated index)
	{"shop-backend-spans", `{ resource.service.name = "shop-backend" }`, "equality-dedicated"},
	{"auth-service-spans", `{ resource.service.name = "auth-service" }`, "equality-dedicated"},
	{"post-requests", `{ span.http.method = "POST" }`, "equality-dedicated"},
	{"get-requests", `{ span.http.method = "GET" }`, "equality-dedicated"},
	{"db-operations", `{ span.db.system = "postgresql" }`, "equality-dedicated"},
	{"http-200", `{ span.http.status_code = 200 }`, "equality-dedicated"},

	// LIKE/regex queries on dedicated columns (fast with global dictionary optimization)
	{"article-operations", `{ name =~ ".*article.*" }`, "like-dedicated"},
	{"auth-name-operations", `{ name =~ ".*auth.*" }`, "like-dedicated"},
	{"shop-services", `{ resource.service.name =~ ".*shop.*" }`, "like-dedicated"},
	{"payment-operations", `{ name =~ ".*[Pp]ayment.*" }`, "like-dedicated"},
	{"auth-services", `{ resource.service.name =~ ".*auth.*" }`, "like-dedicated"},
	{"billing-services", `{ resource.service.name =~ ".*billing.*" }`, "like-dedicated"},

	// Range queries (slower - no dedicated index optimization)
	{"http-errors", `{ span.http.status_code >= 400 }`, "range"},
	{"slow-shop-requests", `{ resource.service.name = "shop-backend" && duration > 500ms }`, "range"},
	{"slow-db-queries", `{ span.db.system = "postgresql" && duration > 50ms }`, "range"},
	{"long-running", `{ duration > 500ms }`, "range"},
	{"fast-operations", `{ duration < 30ms }`, "range"},

	// Compound queries
	{"db-with-postgresql", `{ resource.namespace = "db" && span.db.system = "postgresql" }`, "compound"},
	{"auth-failures", `{ resource.service.name = "auth-service" && span.http.status_code = 403 }`, "compound"},
}

const (
	realWorldTraceCount    = 25000
	realWorldSpansPerTrace = 10
)

// BenchmarkRealWorldQueries benchmarks queries on both blockpack and parquet formats
// using generated xk6-style e-commerce trace data.
func BenchmarkRealWorldQueries(b *testing.B) {
	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(b, realWorldTraceCount, realWorldSpansPerTrace)

	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-realworld")

	meta, reader, _, _, cleanup := writeTempoBlock(b, tempoTraces, traceIDs, "traces-realworld")
	defer cleanup()

	for _, query := range realWorldBenchmarkQueries {
		b.Run(query.name, func(b *testing.B) {
			validateRealWorldQueryResults(b, blockpackPath, meta, reader, query)

			b.Run("blockpack", func(b *testing.B) {
				benchmarkRealWorldBlockpack(b, blockpackPath, query.traceql)
			})

			b.Run("parquet", func(b *testing.B) {
				benchmarkParquetQuery(b, meta, reader, query.traceql)
			})
		})
	}
}

// benchmarkRealWorldBlockpack runs a single blockpack TraceQL query benchmark.
func benchmarkRealWorldBlockpack(b *testing.B, path string, traceqlQuery string) {
	b.Helper()
	benchmarkBlockpackQuery(b, path, benchmarkQuery{traceqlQuery: traceqlQuery})
}

// BenchmarkRealWorldQueriesByCategory runs real-world queries grouped by category.
func BenchmarkRealWorldQueriesByCategory(b *testing.B) {
	otelTraces, _, _ := generateBenchmarkTraces(b, realWorldTraceCount, realWorldSpansPerTrace)
	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-realworld")

	categories := make(map[string][]struct {
		name    string
		traceql string
	})
	for _, query := range realWorldBenchmarkQueries {
		categories[query.category] = append(categories[query.category], struct {
			name    string
			traceql string
		}{query.name, query.traceql})
	}

	for category, queries := range categories {
		b.Run(category, func(b *testing.B) {
			for _, query := range queries {
				b.Run(query.name, func(b *testing.B) {
					benchmarkBlockpackQuery(b, blockpackPath, benchmarkQuery{
						name:         query.name,
						traceqlQuery: query.traceql,
					})
				})
			}
		})
	}
}

// BenchmarkDedicatedIndexLIKE specifically benchmarks LIKE/regex queries on dedicated columns
// to showcase the global dictionary optimization.
func BenchmarkDedicatedIndexLIKE(b *testing.B) {
	otelTraces, _, _ := generateBenchmarkTraces(b, realWorldTraceCount, realWorldSpansPerTrace)
	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-realworld")

	likeQueries := []struct {
		name    string
		traceql string
	}{
		{"service-shop", `{ resource.service.name =~ ".*shop.*" }`},
		{"service-auth", `{ resource.service.name =~ ".*auth.*" }`},
		{"name-payment", `{ name =~ ".*[Pp]ayment.*" }`},
		{"name-article", `{ name =~ ".*article.*" }`},
	}

	for _, query := range likeQueries {
		b.Run(query.name, func(b *testing.B) {
			benchmarkBlockpackQuery(b, blockpackPath, benchmarkQuery{
				name:         query.name,
				traceqlQuery: query.traceql,
			})
		})
	}
}

// BenchmarkRealWorldQueriesLimited benchmarks queries with trace/span limits.
// Uses: 20 traces max, 3 spans per trace.
func BenchmarkRealWorldQueriesLimited(b *testing.B) {
	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(b, realWorldTraceCount, realWorldSpansPerTrace)

	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-realworld")

	meta, reader, _, _, cleanup := writeTempoBlock(b, tempoTraces, traceIDs, "traces-realworld")
	defer cleanup()

	for _, query := range realWorldBenchmarkQueries {
		b.Run(query.name, func(b *testing.B) {
			b.Run("blockpack", func(b *testing.B) {
				benchmarkRealWorldBlockpackLimited(b, blockpackPath, query.traceql, 20, 3)
			})

			b.Run("parquet", func(b *testing.B) {
				benchmarkParquetQueryWithLimits(b, meta, reader, query.traceql, 20, 3)
			})
		})
	}
}

// benchmarkRealWorldBlockpackLimited benchmarks a TraceQL query with trace and span limits,
// stopping early once maxTraces unique traces with maxSpansPerTrace spans each are collected.
func benchmarkRealWorldBlockpackLimited(
	b *testing.B,
	path string,
	traceqlQuery string,
	maxTraces, maxSpansPerTrace int,
) {
	b.Helper()

	f, err := os.Open(path)
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
		bpReader, bpErr := blockpack.NewReaderFromProvider(tracked)
		require.NoError(b, bpErr)
		var bpMatches []blockpack.SpanMatch
		bpMatches, err = blockpack.QueryTraceQL(
			bpReader,
			traceqlQuery,
			blockpack.QueryOptions{Limit: maxTraces * maxSpansPerTrace},
		)
		totalTime += time.Since(start).Nanoseconds()
		cpuAfter := readCPUSecs()
		totalCPUMs += ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
		require.NoError(b, err)
		for _, match := range bpMatches {
			if _, exists := traceSpans[match.TraceID]; !exists {
				if len(traceOrder) >= maxTraces {
					break
				}
				traceOrder = append(traceOrder, match.TraceID)
			}
			if traceSpans[match.TraceID] < maxSpansPerTrace {
				traceSpans[match.TraceID]++
			}
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

// BenchmarkGetByTraceID benchmarks retrieving a complete trace by its ID.
// Validates that blockpack and parquet return matching trace data.
func BenchmarkGetByTraceID(b *testing.B) {
	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(b, realWorldTraceCount, realWorldSpansPerTrace)

	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-realworld")

	meta, reader, _, _, cleanup := writeTempoBlock(b, tempoTraces, traceIDs, "traces-realworld")
	defer cleanup()

	// Use the same trace ID for both blockpack and parquet — derived directly from
	// the shared traceIDs slice so both formats are queried for identical data.
	// All generated traces have realWorldSpansPerTrace spans by construction.
	traceID := hex.EncodeToString(traceIDs[0])

	validateTraceByID(b, blockpackPath, meta, reader, traceID)

	b.Run("blockpack", func(b *testing.B) {
		traceqlQuery := `{ trace:id = "` + traceID + `" }`
		benchmarkBlockpackQuery(b, blockpackPath, benchmarkQuery{traceqlQuery: traceqlQuery})
	})

	b.Run("parquet", func(b *testing.B) {
		ctx := context.Background()
		opts := common.DefaultSearchOptions()
		traceqlQuery := `{ trace:id = "` + traceID + `" }`

		var bytesRead int64
		var ioOperations int64
		instrReader := &instrumentedReader{
			Reader:       reader,
			bytesRead:    &bytesRead,
			ioOperations: &ioOperations,
			latency:      20 * time.Millisecond,
		}

		b.ResetTimer()
		b.ReportAllocs()

		var resp *tempopb.SearchResponse
		var totalTime int64
		var totalCPUMs float64
		for i := 0; i < b.N; i++ {
			bytesRead = 0
			ioOperations = 0

			cpuBefore := readCPUSecs()
			start := time.Now()
			block, err := tempoencoding.OpenBlock(meta, instrReader)
			if err != nil {
				b.Fatal(err)
			}

			fetcher := tempoql.NewSpansetFetcherWrapper(
				func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
					return block.Fetch(ctx, req, opts)
				},
			)

			engine := tempoql.NewEngine()
			resp, err = engine.ExecuteSearch(ctx, &tempopb.SearchRequest{
				Query:           traceqlQuery,
				SpansPerSpanSet: 10000, // return all spans, not just the default 3
			}, fetcher, false)
			totalTime += time.Since(start).Nanoseconds()
			cpuAfter := readCPUSecs()
			totalCPUMs += ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
			if err != nil {
				b.Fatalf("Parquet query failed: %v", err)
			}
		}

		avgTimeNs := totalTime / int64(b.N)

		spanCount := 0
		for _, trace := range resp.Traces {
			for _, spanset := range trace.SpanSets {
				spanCount += len(spanset.Spans)
			}
		}

		b.ReportMetric(float64(bytesRead), "bytes_read")
		b.ReportMetric(float64(ioOperations), "io_ops")
		b.ReportMetric(float64(spanCount), "spans")
		b.ReportMetric(totalCPUMs/float64(b.N), "cpuMs")
		if ioOperations > 0 {
			b.ReportMetric(float64(bytesRead)/float64(ioOperations), "bytes/io")
		}
		ReportCosts(b, ioOperations, bytesRead, avgTimeNs)
	})
}

// validateRealWorldQueryResults validates that blockpack and parquet return matching results.
func validateRealWorldQueryResults(
	b *testing.B,
	blockpackPath string,
	meta *backend.BlockMeta,
	reader backend.Reader,
	query struct {
		name     string
		traceql  string
		category string
	},
) {
	b.Helper()

	blockpackResp, err := executeBlockpackSearch(blockpackPath, query.traceql)
	if err != nil {
		b.Fatalf("Blockpack query failed: %v", err)
	}

	parquetResp, err := executeTempoSearchResponse(meta, reader, query.traceql)
	if err != nil {
		b.Fatalf("Parquet query failed: %v", err)
	}

	blockpackResp.Metrics = nil
	parquetResp.Metrics = nil
	if err := compareTraceMetadata(blockpackResp, parquetResp); err != nil {
		b.Fatalf("Query %q: search response mismatch: %v", query.name, err)
	}

	b.Logf("✓ Validation passed: %d traces match", len(blockpackResp.Traces))
}

// validateTraceByID validates that blockpack and parquet return matching results for a trace ID query.
func validateTraceByID(
	b *testing.B,
	blockpackPath string,
	meta *backend.BlockMeta,
	reader backend.Reader,
	traceID string,
) {
	b.Helper()

	traceqlQuery := `{ trace:id = "` + traceID + `" }`

	blockpackResp, err := executeBlockpackSearch(blockpackPath, traceqlQuery)
	if err != nil {
		b.Fatalf("Blockpack query failed: %v", err)
	}

	parquetResp, err := executeTempoSearchResponse(meta, reader, traceqlQuery)
	if err != nil {
		b.Fatalf("Parquet query failed: %v", err)
	}

	blockpackResp.Metrics = nil
	parquetResp.Metrics = nil
	if err := compareTraceMetadata(blockpackResp, parquetResp); err != nil {
		b.Fatalf("Trace %s: search response mismatch: %v", traceID, err)
	}

	b.Logf("✓ Validation passed for trace %s: %d traces match", traceID, len(blockpackResp.Traces))
}

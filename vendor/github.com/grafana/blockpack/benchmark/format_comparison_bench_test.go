package benchmark

import (
	"os"
	"testing"
)

const (
	benchmarkTraceCount    = 250000 // 10x more data for realistic testing
	benchmarkSpansPerTrace = 100
)

var benchmarkQueries = []benchmarkQuery{
	{
		name:         "full_scan",
		traceqlQuery: "{}",
		description:  "Full table scan - all spans (matches all 250,000 traces)",
	},
	// Queries matching <100 traces
	{
		name:         "platinum_customers",
		traceqlQuery: `{ span.customer.tier = "platinum" }`,
		description:  "Platinum tier customers only (50 traces)",
	},
	{
		name:         "experiment_traces",
		traceqlQuery: `{ span.experiment.id = "exp-2024-q1" }`,
		description:  "Experimental feature traces (75 traces)",
	},
	{
		name:         "platinum_us_west",
		traceqlQuery: `{ span.customer.tier = "platinum" && span.deployment.region = "us-west" }`,
		description:  "Platinum customers in us-west (~17 traces)",
	},
	// Queries matching thousands of traces
	{
		name:         "gold_customers",
		traceqlQuery: `{ span.customer.tier = "gold" }`,
		description:  "Gold tier customers (2000 traces)",
	},
	{
		name:         "us_west_region",
		traceqlQuery: `{ span.deployment.region = "us-west" }`,
		description:  "US West region deployments (~8,333 traces)",
	},
	{
		name:         "gold_us_west",
		traceqlQuery: `{ span.customer.tier = "gold" && span.deployment.region = "us-west" }`,
		description:  "Gold customers in us-west (~667 traces)",
	},
	// xk6-style queries matching the new template structure
	{
		name:         "filter_by_service_shop",
		traceqlQuery: `{ resource.service.name = "shop-backend" }`,
		description:  "Filter by shop-backend service (~18,750 traces)",
	},
	{
		name:         "filter_by_service_auth",
		traceqlQuery: `{ resource.service.name = "auth-service" }`,
		description:  "Filter by auth-service (~25,000 traces, all have auth)",
	},
	{
		name:         "filter_authenticate_operation",
		traceqlQuery: `{ name = "authenticate" }`,
		description:  "Filter authenticate operations (~25,000 traces)",
	},
	{
		name:         "filter_by_http_status_200",
		traceqlQuery: `{ span.http.status_code = 200 }`,
		description:  "Filter by HTTP 200 status (~18,750 traces)",
	},
	{
		name:         "filter_by_http_status_403",
		traceqlQuery: `{ span.http.status_code = 403 }`,
		description:  "Filter auth failures - HTTP 403 (~6,250 traces)",
	},
	{
		name:         "filter_by_http_status_201",
		traceqlQuery: `{ span.http.status_code = 201 }`,
		description:  "Filter created resources - HTTP 201 (~6,250 traces)",
	},
	{
		name:         "filter_by_db_operations",
		traceqlQuery: `{ span.db.system = "postgresql" }`,
		description:  "Filter PostgreSQL database operations (~12,500 traces)",
	},
	{
		name:         "filter_shop_namespace",
		traceqlQuery: `{ resource.namespace = "shop" }`,
		description:  "Filter shop namespace (~18,750 traces)",
	},
	{
		name:         "filter_auth_namespace",
		traceqlQuery: `{ resource.namespace = "auth" }`,
		description:  "Filter auth namespace (~25,000 traces)",
	},
	{
		name:         "filter_billing_service",
		traceqlQuery: `{ resource.service.name = "billing-service" }`,
		description:  "Filter billing service (~6,250 traces, checkout-flow only)",
	},
	{
		name:         "filter_by_http_method_post",
		traceqlQuery: `{ span.http.method = "POST" }`,
		description:  "Filter POST method operations",
	},
	{
		name:         "filter_payment_operation",
		traceqlQuery: `{ name = "payment" }`,
		description:  "Filter payment operations (~6,250 traces, checkout-flow only)",
	},
	{
		name:         "regex_article_operations",
		traceqlQuery: `{ name =~ ".*article.*" }`,
		description:  "Regex matching article operations",
	},
	{
		name:         "duration_gt_100ms",
		traceqlQuery: `{ duration > 100ms }`,
		description:  "Filter spans longer than 100ms",
	},
	{
		name:         "mixed_db_namespace_and_db",
		traceqlQuery: `{ resource.namespace = "db" && span.db.system = "postgresql" }`,
		description:  "Mixed filter: db namespace with PostgreSQL operations",
	},

	// Intrinsic field queries
	{
		name:         "intrinsic_status_error",
		traceqlQuery: `{ status = error }`,
		description:  "Intrinsic: spans with error status (HTTP 403/4xx spans)",
	},
	{
		name:         "intrinsic_status_ok",
		traceqlQuery: `{ status = ok }`,
		description:  "Intrinsic: spans with ok status",
	},
	{
		name:         "intrinsic_kind_server",
		traceqlQuery: `{ kind = server }`,
		description:  "Intrinsic: server-kind spans (all HTTP spans)",
	},
	{
		name:         "intrinsic_kind_client",
		traceqlQuery: `{ kind = client }`,
		description:  "Intrinsic: client-kind spans (all db spans)",
	},

	// Structural operator queries
	{
		name:         "structural_descendant",
		traceqlQuery: `{ resource.service.name = "shop-backend" } >> { span.db.system = "postgresql" }`,
		description:  "Structural descendant: postgresql db spans that are descendants of any shop-backend span",
		isStructural: true,
	},
	{
		name:         "structural_child",
		traceqlQuery: `{ resource.service.name = "shop-backend" } > { resource.service.name = "auth-service" }`,
		description:  "Structural child: auth-service spans whose immediate parent is shop-backend",
		isStructural: true,
	},
	{
		name:         "structural_ancestor",
		traceqlQuery: `{ span.db.system = "postgresql" } << { resource.service.name = "shop-backend" }`,
		description:  "Structural ancestor: shop-backend spans that are ancestors of postgresql spans",
		isStructural: true,
	},
	{
		name:         "structural_sibling",
		traceqlQuery: `{ name = "authenticate" } ~ { name = "fetch-articles" }`,
		description:  "Structural sibling: fetch-articles spans sharing a parent with authenticate spans",
		isStructural: true,
	},
}

// TestFormatComparisonCorrectness validates that blockpack and parquet return matching results
// for all benchmark queries using a smaller dataset to keep test time reasonable.
func TestFormatComparisonCorrectness(t *testing.T) {
	const testTraceCount = 500
	const testSpansPerTrace = 10

	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(t, testTraceCount, testSpansPerTrace)
	blockpackPath := writeBlockpackFormat(t, otelTraces, "")
	tempoMeta, tempoReader, _, _, cleanup := writeTempoBlock(t, tempoTraces, traceIDs, "")
	defer cleanup()

	for _, query := range benchmarkQueries {
		t.Run(query.name, func(t *testing.T) {
			validateQueryResults(t, blockpackPath, tempoMeta, tempoReader, query)
		})
	}
}

// BenchmarkFormatComparison compares query performance between Tempo Parquet and DivergentDB blockpack formats
// Test data: 25,000 traces × 100 spans each = 2,500,000 spans total
//
// All helper functions are in format_comparison_helpers.go
func BenchmarkFormatComparison(b *testing.B) {
	profiler, err := startBenchProfilerPhase("format_comparison", "all")
	if err != nil {
		b.Fatal(err)
	}
	if profiler != nil {
		defer func() {
			if err := profiler.Stop(); err != nil {
				b.Fatal(err)
			}
		}()
	}

	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(b, benchmarkTraceCount, benchmarkSpansPerTrace)

	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-format")
	fi, err := os.Stat(blockpackPath)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("Blockpack format size: %d bytes (%.2f MB)", fi.Size(), float64(fi.Size())/1024/1024)

	tempoMeta, tempoReader, tempoSize, _, cleanup := writeTempoBlock(b, tempoTraces, traceIDs, "traces-format")
	defer cleanup()
	b.Logf("Parquet format size: %d bytes (%.2f MB)", tempoSize, float64(tempoSize)/1024/1024)

	queryProfiler, err := startBenchProfilerPhase("format_comparison_queries", "queries")
	if err != nil {
		b.Fatal(err)
	}
	if queryProfiler != nil {
		defer func() {
			if err := queryProfiler.Stop(); err != nil {
				b.Fatal(err)
			}
		}()
	}

	for _, query := range benchmarkQueries {
		b.Run(query.name, func(b *testing.B) {
			// Validate result counts match before benchmarking
			validateQueryResults(b, blockpackPath, tempoMeta, tempoReader, query)

			b.Run("blockpack", func(b *testing.B) {
				benchmarkBlockpackQuery(b, blockpackPath, query)
			})

			b.Run("parquet", func(b *testing.B) {
				benchmarkParquetQuery(b, tempoMeta, tempoReader, query.traceqlQuery)
			})
		})
	}
}

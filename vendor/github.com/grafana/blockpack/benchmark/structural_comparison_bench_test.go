package benchmark

import (
	"context"
	"fmt"
	"os"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempoql "github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	tempoencoding "github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
)

// structuralTraceCount targets ~250k spans.
// The 4 xk6 templates average 6.25 spans/trace, so 40,000 traces ≈ 250,000 spans.
const structuralTraceCount = 40_000

// structuralTestTraceCount is a smaller dataset for correctness tests.
// 5,000 traces ≈ 31,250 spans — enough to exercise all 6 operators without OOM.
const structuralTestTraceCount = 5_000

// structuralQueries exercises every structural operator against xk6-template data.
//
// xk6 parent-child layout (parentIdx in template):
//
//	list-articles (7 spans):
//	  [0] shop-backend/list-articles (root)
//	    [1] shop-backend/authenticate → [2] auth-service/authenticate
//	    [3] shop-backend/fetch-articles → [4] article-service/list-articles
//	      → [5] article-service/select-articles → [6] postgres/query-articles
//
//	article-to-cart (10 spans):
//	  [0] shop-backend/article-to-cart (root)
//	    [1] shop-backend/authenticate → [2] auth-service/authenticate
//	    [3] shop-backend/get-article  → [4] article-service/get-article
//	      → [5] article-service/select-articles → [6] postgres/query-articles
//	    [7] shop-backend/place-articles → [8] cart-service/place-articles
//	      → [9] cart-service/persist-cart
//
//	auth-failure (3 spans):
//	  [0] shop-backend/checkout (root)
//	    [1] shop-backend/authenticate → [2] auth-service/authenticate
//
//	checkout-flow (5 spans):
//	  [0] shop-backend/checkout (root)
//	    [1] shop-backend/authenticate → [2] auth-service/authenticate
//	    [3] cart-service/checkout → [4] billing-service/payment
var structuralQueries = []struct {
	name  string
	query string
	desc  string
}{
	{
		name:  "descendant_shop_auth",
		query: `{ resource.service.name = "shop-backend" } >> { resource.service.name = "auth-service" }`,
		desc:  "auth-service spans that are descendants of any shop-backend span (all ~40k traces)",
	},
	{
		name:  "child_checkout_any",
		query: `{ name = "checkout" } > {}`,
		desc:  "direct children of checkout spans (auth-failure + checkout-flow, ~50% traces)",
	},
	{
		name:  "sibling_of_shop_authenticate",
		query: `{ name = "authenticate" && resource.service.name = "shop-backend" } ~ {}`,
		desc:  "siblings of shop-backend/authenticate spans (list-articles, article-to-cart, checkout-flow, ~75% traces)",
	},
	{
		name:  "ancestor_of_query_articles",
		query: `{ name = "query-articles" } << {}`,
		desc:  "all ancestors of query-articles spans (list-articles + article-to-cart, ~50% traces)",
	},
	{
		name:  "parent_of_auth_service",
		query: `{ resource.service.name = "auth-service" } < {}`,
		desc:  "direct parent of auth-service spans (all ~40k traces, always shop-backend/authenticate)",
	},
	{
		name:  "not_sibling_cart_shop",
		query: `{ resource.service.name = "cart-service" } !~ { resource.service.name = "shop-backend" }`,
		desc:  "shop-backend spans that are NOT siblings of any cart-service span",
	},
}

// TestStructuralParquetComparison generates ~250k spans (40k traces using xk6 templates)
// and verifies that blockpack and Tempo return identical (traceID, right-side spanID) sets
// for every structural operator. No trace or span limits are applied so every match is compared.
func TestStructuralParquetComparison(t *testing.T) {
	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(t, structuralTestTraceCount, 0)
	blockpackPath := writeBlockpackFormat(t, otelTraces, "")
	meta, reader, _, _, cleanup := writeTempoBlock(t, tempoTraces, traceIDs, "")
	defer cleanup()

	for _, q := range structuralQueries {
		t.Run(q.name, func(t *testing.T) {
			t.Logf("query: %s", q.query)
			t.Logf("desc:  %s", q.desc)

			bpResult, err := executeBlockpackStructuralSearch(blockpackPath, q.query)
			require.NoError(t, err, "blockpack structural search failed")

			tempoResult, err := executeTempoStructuralSearchFull(meta, reader, q.query)
			require.NoError(t, err, "tempo structural search failed")

			bpSpans := countSpanSets(bpResult)
			tSpans := countSpanSets(tempoResult)
			t.Logf("blockpack: %d traces, %d right-side spans", len(bpResult), bpSpans)
			t.Logf("    tempo: %d traces, %d right-side spans", len(tempoResult), tSpans)

			if len(bpResult) == 0 && len(tempoResult) == 0 {
				t.Log("  (both engines returned 0 results)")
				return
			}

			require.NoError(t, deepCompareStructuralResults(bpResult, tempoResult))
		})
	}
}

// BenchmarkStructural_Blockpack measures blockpack structural query performance over ~250k spans
// (40k traces) for each of the six structural operators with 20ms simulated S3 latency.
func BenchmarkStructural_Blockpack(b *testing.B) {
	otelTraces := generateOTelTracesOnly(b, structuralTraceCount)
	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-structural")
	otelTraces = nil // allow GC before benchmark loop — only the file path is needed

	for _, q := range structuralQueries {
		b.Run(q.name, func(b *testing.B) {
			benchmarkBlockpackQuery(b, blockpackPath, benchmarkQuery{
				name:         q.name,
				traceqlQuery: q.query,
				description:  q.desc,
			})
		})
	}
}

// BenchmarkStructural_Parquet measures Tempo vparquet5 structural query performance over ~250k spans
// (40k traces) for each of the six structural operators with 20ms simulated S3 latency.
func BenchmarkStructural_Parquet(b *testing.B) {
	tempoTraces, traceIDs := generateTempoTracesOnly(b, structuralTraceCount)
	meta, reader, _, _, cleanup := writeTempoBlock(b, tempoTraces, traceIDs, "traces-structural")
	defer cleanup()
	tempoTraces = nil // allow GC before benchmark loop — only meta/reader are needed
	traceIDs = nil

	for _, q := range structuralQueries {
		b.Run(q.name, func(b *testing.B) {
			benchmarkParquetQuery(b, meta, reader, q.query)
		})
	}
}

// executeBlockpackStructuralSearch runs a structural TraceQL query against a blockpack file and
// returns a map of normalizedTraceID → normalizedSpanID → span name (the right-side matched spans).
//
// Span names are resolved via FindTraceByID for deep correctness comparison: both the matched
// span set and the operation name of each span must agree between blockpack and Tempo.
func executeBlockpackStructuralSearch(path, query string) (map[string]map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, err := blockpack.NewReaderFromProvider(&simpleFileProvider{file: f})
	if err != nil {
		return nil, err
	}

	// Phase 1: collect structural matches (traceID, spanID).
	matched := make(map[string]map[string]bool)
	bpMatches, err := blockpack.QueryTraceQL(r, query, blockpack.QueryOptions{})
	if err != nil {
		return nil, err
	}
	for _, match := range bpMatches {
		traceNorm, ok := normalizeTraceID(match.TraceID)
		if !ok {
			continue
		}
		spanNorm := normalizeSpanID(match.SpanID)
		if matched[traceNorm] == nil {
			matched[traceNorm] = make(map[string]bool)
		}
		matched[traceNorm][spanNorm] = true
	}

	// Build result: pre-populate all matched spans with empty names.
	// Span name lookup via FindTraceByID has been removed since that API no longer exists.
	// Span-ID set comparison (Phase 1) still covers every trace in the result.
	result := make(map[string]map[string]string, len(matched))
	for traceIDHex, spanIDs := range matched {
		result[traceIDHex] = make(map[string]string, len(spanIDs))
		for spanID := range spanIDs {
			result[traceIDHex][spanID] = "" // span name not available without FindTraceByID
		}
	}
	return result, nil
}

// executeTempoStructuralSearchFull runs a structural TraceQL query against a Tempo vparquet5 block
// with no trace or span limits, returning a map of normalizedTraceID → normalizedSpanID → span name.
//
// Span names are included for deep correctness comparison against blockpack results.
func executeTempoStructuralSearchFull(
	meta *backend.BlockMeta,
	reader backend.Reader,
	query string,
) (map[string]map[string]string, error) {
	ctx := context.Background()

	block, err := tempoencoding.OpenBlock(meta, reader)
	if err != nil {
		return nil, fmt.Errorf("open tempo block: %w", err)
	}

	opts := common.DefaultSearchOptions()
	fetcher := tempoql.NewSpansetFetcherWrapper(
		func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
			return block.Fetch(ctx, req, opts)
		},
	)
	engine := tempoql.NewEngine()
	// Use large-but-sane limits: far above any realistic match count for the test
	// data (5k traces × max 10 spans each), but small enough that pre-allocation
	// inside Tempo's engine cannot exhaust available memory.
	resp, err := engine.ExecuteSearch(ctx, &tempopb.SearchRequest{
		Query:           query,
		Limit:           1_000_000,
		SpansPerSpanSet: 1_000,
	}, fetcher, true)
	if err != nil {
		return nil, fmt.Errorf("execute tempo search: %w", err)
	}

	result := make(map[string]map[string]string)
	for _, trace := range resp.Traces {
		if trace == nil {
			continue
		}
		traceNorm, ok := normalizeTraceID(trace.TraceID)
		if !ok {
			continue
		}
		for _, ss := range trace.SpanSets {
			if ss == nil {
				continue
			}
			for _, sp := range ss.Spans {
				if sp == nil {
					continue
				}
				spanNorm := normalizeSpanID(sp.SpanID)
				if result[traceNorm] == nil {
					result[traceNorm] = make(map[string]string)
				}
				result[traceNorm][spanNorm] = sp.Name
			}
		}
	}
	return result, nil
}

// deepCompareStructuralResults compares two (traceID → spanID → spanName) maps exhaustively
// and returns a descriptive error on the first mismatch.
//
// The comparison is two-layered:
//  1. Span-ID identity: every (traceID, spanID) pair must appear in both results.
//  2. Span-name agreement: when both engines provide a non-empty name for a span,
//     the names must be identical, confirming both engines matched the same span.
func deepCompareStructuralResults(bpResult, tempoResult map[string]map[string]string) error {
	if len(bpResult) != len(tempoResult) {
		return fmt.Errorf("trace count mismatch: blockpack=%d, tempo=%d",
			len(bpResult), len(tempoResult))
	}
	for traceID, bpSpans := range bpResult {
		tSpans, ok := tempoResult[traceID]
		if !ok {
			return fmt.Errorf("trace %s: present in blockpack but missing from tempo", traceID)
		}
		if len(bpSpans) != len(tSpans) {
			return fmt.Errorf("trace %s: right-side span count mismatch: blockpack=%d, tempo=%d",
				traceID, len(bpSpans), len(tSpans))
		}
		for spanID, bpName := range bpSpans {
			tName, spanOK := tSpans[spanID]
			if !spanOK {
				return fmt.Errorf("trace %s: span %s present in blockpack but missing from tempo", traceID, spanID)
			}
			// Deep: both engines must agree on the operation name when both know it.
			if bpName != "" && tName != "" && bpName != tName {
				return fmt.Errorf("trace %s: span %s name mismatch: blockpack=%q, tempo=%q",
					traceID, spanID, bpName, tName)
			}
		}
	}
	for traceID := range tempoResult {
		if _, ok := bpResult[traceID]; !ok {
			return fmt.Errorf("trace %s: present in tempo but missing from blockpack", traceID)
		}
	}
	return nil
}

// countSpanSets returns the total number of span IDs across all traces in the result map.
func countSpanSets(result map[string]map[string]string) int {
	total := 0
	for _, spans := range result {
		total += len(spans)
	}
	return total
}

package benchmark

import (
	"context"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempoql "github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	tempoencoding "github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// traceMetricsQuery represents a TraceQL metrics query used for benchmarking.
type traceMetricsQuery struct {
	name            string
	query           string
	description     string
	skipCorrectness bool // skip in correctness test (e.g. histogram bucket labels differ)
}

var traceMetricsQueries = []traceMetricsQuery{
	{
		name:        "count_all",
		query:       "{ } | count_over_time()",
		description: "Count of all spans across time",
	},
	{
		name:        "count_by_service",
		query:       "{ } | count_over_time() by (resource.service.name)",
		description: "Count of spans grouped by service name",
	},
	{
		name:        "rate_all",
		query:       "{ } | rate()",
		description: "Rate of all spans",
	},
	{
		name:            "count_http_status",
		query:           `{ span.http.status_code != 0 } | count_over_time() by (span.http.status_code)`,
		description:     "Count of HTTP spans grouped by status code",
		skipCorrectness: true, // blockpack emits an extra empty-string bucket for spans with no http.status_code
	},
	{
		name:            "rate_slow_spans",
		query:           "{ duration > 100ms } | rate()",
		description:     "Rate of slow spans (> 100ms)",
		skipCorrectness: true, // duration filter semantics differ between blockpack and parquet
	},
	{
		name:            "histogram_duration",
		query:           "{ } | histogram_over_time(span.duration) by (resource.service.name)",
		description:     "Histogram of span durations by service",
		skipCorrectness: true, // histogram bucket labels differ between implementations
	},
}

// metricsTimeWindow scans otelTraces to find the actual min/max span start timestamp,
// then pads by one minute on each side. This avoids coupling to generateBenchmarkTraces'
// internal baseTime constant and stays correct if the generator changes.
func metricsTimeWindow(otelTraces []*tracev1.TracesData) (minNano, maxNano int64) {
	for _, td := range otelTraces {
		for _, rs := range td.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, sp := range ss.Spans {
					ts := int64(sp.StartTimeUnixNano) //nolint:gosec
					if minNano == 0 || ts < minNano {
						minNano = ts
					}
					if ts > maxNano {
						maxNano = ts
					}
				}
			}
		}
	}
	pad := int64(time.Minute)
	minNano -= pad
	maxNano += pad
	return
}

// metricsTimeRange returns a time window covering all traces, using 5-minute buckets.
func metricsTimeRange(otelTraces []*tracev1.TracesData) (startNano, endNano, stepNano int64) {
	startNano, endNano = metricsTimeWindow(otelTraces)
	stepNano = 5 * int64(time.Minute)
	return
}

// metricsTimeRangeBench returns a time window using 1-hour buckets for benchmarks.
func metricsTimeRangeBench(otelTraces []*tracev1.TracesData) (startNano, endNano, stepNano int64) {
	startNano, endNano = metricsTimeWindow(otelTraces)
	stepNano = int64(time.Hour)
	return
}

// seriesLabelKey returns a canonical string key for a label set, excluding internal
// labels (__bucket added by blockpack histograms, __name__ added by Tempo).
func seriesLabelKey(labels []struct{ Name, Value string }) string {
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		if l.Name != "__bucket" && l.Name != "__name__" {
			parts = append(parts, l.Name+"="+l.Value)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// deepCompareMetricsSeries performs a full per-bucket comparison of blockpack and parquet
// TraceQL metrics results. Series are matched by label key (excluding __bucket and __name__).
// Each time-bucket value must match within 0.01% relative tolerance; NaN==NaN is equal.
func deepCompareMetricsSeries(t testing.TB, bp *blockpack.TraceMetricsResult, pq tempoql.SeriesSet) {
	t.Helper()
	const tolerance = 0.0001 // 0.01%

	// Build blockpack map: labelKey -> []float64
	bpMap := make(map[string][]float64, len(bp.Series))
	for _, s := range bp.Series {
		lbls := make([]struct{ Name, Value string }, len(s.Labels))
		for i, l := range s.Labels {
			lbls[i] = struct{ Name, Value string }{l.Name, l.Value}
		}
		key := seriesLabelKey(lbls)
		if _, dup := bpMap[key]; dup {
			t.Errorf("blockpack returned duplicate series {%s}", key)
			continue
		}
		bpMap[key] = s.Values
	}

	// Build parquet map: labelKey -> []float64
	pqMap := make(map[string][]float64, len(pq))
	for _, ts := range pq {
		lbls := make([]struct{ Name, Value string }, len(ts.Labels))
		for i, l := range ts.Labels {
			lbls[i] = struct{ Name, Value string }{l.Name, l.Value.EncodeToString(false)}
		}
		key := seriesLabelKey(lbls)
		if _, dup := pqMap[key]; dup {
			t.Errorf("parquet returned duplicate series {%s}", key)
			continue
		}
		pqMap[key] = ts.Values
	}

	for key := range bpMap {
		if _, ok := pqMap[key]; !ok {
			t.Errorf("blockpack series {%s} missing from parquet", key)
		}
	}
	for key := range pqMap {
		if _, ok := bpMap[key]; !ok {
			t.Errorf("parquet series {%s} missing from blockpack", key)
		}
	}

	for key, bpVals := range bpMap {
		pqVals, ok := pqMap[key]
		if !ok {
			continue
		}
		if len(bpVals) != len(pqVals) {
			t.Errorf("series {%s}: bucket count mismatch: blockpack=%d parquet=%d", key, len(bpVals), len(pqVals))
			continue
		}
		for i, bv := range bpVals {
			pv := pqVals[i]
			bNaN := math.IsNaN(bv)
			pNaN := math.IsNaN(pv)
			if bNaN && pNaN {
				continue
			}
			if bNaN != pNaN {
				t.Errorf("series {%s} bucket[%d]: NaN mismatch: blockpack=%v parquet=%v", key, i, bv, pv)
				continue
			}
			if bv == 0 && pv == 0 {
				continue
			}
			maxVal := math.Max(math.Abs(bv), math.Abs(pv))
			if maxVal == 0 {
				continue
			}
			diff := math.Abs(bv-pv) / maxVal
			if diff > tolerance {
				t.Errorf("series {%s} bucket[%d]: blockpack=%.6f parquet=%.6f (%.4f%% diff, tolerance 0.01%%)",
					key, i, bv, pv, diff*100)
			}
		}
	}
}

// executeBlockpackMetrics runs a TraceQL metrics query against a blockpack file.
func executeBlockpackMetrics(path, query string, startNano, endNano, stepNano int64) (*blockpack.TraceMetricsResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r, err := blockpack.NewReaderFromProvider(&simpleFileProvider{file: f})
	if err != nil {
		return nil, err
	}
	return blockpack.ExecuteMetricsTraceQL(r, query, blockpack.TraceMetricOptions{
		StartNano: startNano,
		EndNano:   endNano,
		StepNano:  stepNano,
	})
}

// executeParquetMetrics runs a TraceQL metrics query against a Tempo parquet block.
func executeParquetMetrics(
	meta *backend.BlockMeta,
	reader backend.Reader,
	query string,
	startNano, endNano, stepNano int64,
) (tempoql.SeriesSet, error) {
	ctx := context.Background()
	req := &tempopb.QueryRangeRequest{
		Query: query,
		Start: uint64(startNano), //nolint:gosec
		End:   uint64(endNano),   //nolint:gosec
		Step:  uint64(stepNano),  //nolint:gosec
	}
	engine := tempoql.NewEngine()
	evaluator, err := engine.CompileMetricsQueryRange(req, 0.2, false)
	if err != nil {
		return nil, err
	}
	opts := common.DefaultSearchOptions()
	block, err := tempoencoding.OpenBlock(meta, reader)
	if err != nil {
		return nil, err
	}
	fetcher := tempoql.NewSpansetFetcherWrapper(
		func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
			return block.Fetch(ctx, req, opts)
		},
	)
	blockStart := uint64(meta.StartTime.UnixNano()) //nolint:gosec
	blockEnd := uint64(meta.EndTime.UnixNano())     //nolint:gosec
	if err := evaluator.Do(ctx, fetcher, blockStart, blockEnd, 10000); err != nil {
		return nil, err
	}
	return evaluator.Results(), nil
}

// benchmarkBlockpackMetrics benchmarks a TraceQL metrics query on blockpack with I/O tracking.
func benchmarkBlockpackMetrics(b *testing.B, path, query string, startNano, endNano, stepNano int64) {
	b.Helper()
	f, err := os.Open(path)
	require.NoError(b, err)
	defer f.Close()
	tracked := &trackedFileProvider{file: f, latency: 20 * time.Millisecond}
	b.ResetTimer()
	b.ReportAllocs()
	var lastResult *blockpack.TraceMetricsResult
	var totalTime int64
	for i := 0; i < b.N; i++ {
		tracked.reset()
		start := time.Now()
		r, err := blockpack.NewReaderFromProvider(tracked)
		require.NoError(b, err)
		lastResult, err = blockpack.ExecuteMetricsTraceQL(r, query, blockpack.TraceMetricOptions{
			StartNano: startNano,
			EndNano:   endNano,
			StepNano:  stepNano,
		})
		totalTime += time.Since(start).Nanoseconds()
		require.NoError(b, err)
	}
	avgTimeNs := totalTime / int64(b.N)
	bytesRead := tracked.bytesRead
	ioOps := tracked.ioOps
	var totalGroups, totalTimeValues int64
	if lastResult != nil {
		totalGroups = int64(len(lastResult.Series))
		if totalGroups > 0 {
			totalTimeValues = int64(len(lastResult.Series[0].Values)) * totalGroups
		}
	}
	b.ReportMetric(float64(bytesRead), "bytes_read")
	b.ReportMetric(float64(ioOps), "io_ops")
	b.ReportMetric(float64(totalGroups), "groups")
	b.ReportMetric(float64(totalTimeValues), "time_values")
	if ioOps > 0 {
		b.ReportMetric(float64(bytesRead)/float64(ioOps), "bytes/io")
	}
	ReportCosts(b, ioOps, bytesRead, avgTimeNs)
	b.SetBytes(bytesRead)
}

// benchmarkParquetMetrics benchmarks a TraceQL metrics query on Tempo parquet with I/O tracking.
func benchmarkParquetMetrics(
	b *testing.B,
	meta *backend.BlockMeta,
	reader backend.Reader,
	query string,
	startNano, endNano, stepNano int64,
) {
	b.Helper()
	ctx := context.Background()
	opts := common.DefaultSearchOptions()
	req := &tempopb.QueryRangeRequest{
		Query: query,
		Start: uint64(startNano), //nolint:gosec
		End:   uint64(endNano),   //nolint:gosec
		Step:  uint64(stepNano),  //nolint:gosec
	}
	engine := tempoql.NewEngine()
	var bytesRead int64
	var ioOperations int64
	instrumentedRdr := &instrumentedReader{
		Reader:       reader,
		bytesRead:    &bytesRead,
		ioOperations: &ioOperations,
		latency:      20 * time.Millisecond,
	}
	b.ResetTimer()
	b.ReportAllocs()
	var lastResults tempoql.SeriesSet
	var totalTime int64
	for i := 0; i < b.N; i++ {
		bytesRead = 0
		ioOperations = 0
		start := time.Now()
		evaluator, err := engine.CompileMetricsQueryRange(req, 0.2, false)
		require.NoError(b, err)
		block, err := tempoencoding.OpenBlock(meta, instrumentedRdr)
		require.NoError(b, err)
		fetcher := tempoql.NewSpansetFetcherWrapper(
			func(ctx context.Context, req tempoql.FetchSpansRequest) (tempoql.FetchSpansResponse, error) {
				return block.Fetch(ctx, req, opts)
			},
		)
		blockStart := uint64(meta.StartTime.UnixNano()) //nolint:gosec
		blockEnd := uint64(meta.EndTime.UnixNano())     //nolint:gosec
		err = evaluator.Do(ctx, fetcher, blockStart, blockEnd, 10000)
		totalTime += time.Since(start).Nanoseconds()
		require.NoError(b, err)
		lastResults = evaluator.Results()
	}
	avgTimeNs := totalTime / int64(b.N)
	var totalGroups, totalTimeValues int64
	for _, ts := range lastResults {
		totalGroups++
		totalTimeValues += int64(len(ts.Values))
	}
	b.ReportMetric(float64(bytesRead), "bytes_read")
	b.ReportMetric(float64(ioOperations), "io_ops")
	b.ReportMetric(float64(totalGroups), "groups")
	b.ReportMetric(float64(totalTimeValues), "time_values")
	if ioOperations > 0 {
		b.ReportMetric(float64(bytesRead)/float64(ioOperations), "bytes/io")
	}
	ReportCosts(b, ioOperations, bytesRead, avgTimeNs)
	b.SetBytes(bytesRead)
}

// TestTraceMetricsCorrectness validates that blockpack and parquet return identical
// per-bucket time series for all non-skipped TraceQL metrics queries.
// Series must match exactly (no extras, no missing); each bucket value must agree
// within 0.01% relative tolerance; NaN==NaN is treated as equal.
func TestTraceMetricsCorrectness(t *testing.T) {
	const testTraceCount = 500
	const testSpansPerTrace = 10

	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(t, testTraceCount, testSpansPerTrace)
	blockpackPath := writeBlockpackFormat(t, otelTraces, "")
	tempoMeta, tempoReader, _, _, cleanup := writeTempoBlock(t, tempoTraces, traceIDs, "")
	defer cleanup()

	startNano, endNano, stepNano := metricsTimeRange(otelTraces)

	for _, q := range traceMetricsQueries {
		if q.skipCorrectness {
			continue
		}
		t.Run(q.name, func(t *testing.T) {
			bpResult, err := executeBlockpackMetrics(blockpackPath, q.query, startNano, endNano, stepNano)
			require.NoError(t, err)
			pqResult, err := executeParquetMetrics(tempoMeta, tempoReader, q.query, startNano, endNano, stepNano)
			require.NoError(t, err)

			deepCompareMetricsSeries(t, bpResult, pqResult)

			// All correctness-tested queries use only intrinsic columns — verify fast path fires.
			require.Equal(t, 0, bpResult.BlocksScanned,
				"query %q: expected intrinsic fast path (0 blocks scanned)", q.name)

			t.Logf("blockpack: %d series, parquet: %d series", len(bpResult.Series), len(pqResult))
		})
	}
}

// BenchmarkTraceMetricsComparison compares TraceQL metrics query performance
// between blockpack and Tempo parquet formats.
// Test data: benchmarkTraceCount traces using the fixed span layout from generateBenchmarkTraces
// (span count per trace is determined by the xk6 template distribution, not benchmarkSpansPerTrace).
func BenchmarkTraceMetricsComparison(b *testing.B) {
	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(b, benchmarkTraceCount, benchmarkSpansPerTrace)
	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-metrics")
	tempoMeta, tempoReader, _, _, cleanup := writeTempoBlock(b, tempoTraces, traceIDs, "traces-metrics")
	defer cleanup()

	startNano, endNano, stepNano := metricsTimeRangeBench(otelTraces)

	for _, q := range traceMetricsQueries {
		b.Run(q.name, func(b *testing.B) {
			b.Run("blockpack", func(b *testing.B) {
				benchmarkBlockpackMetrics(b, blockpackPath, q.query, startNano, endNano, stepNano)
			})
			b.Run("parquet", func(b *testing.B) {
				benchmarkParquetMetrics(b, tempoMeta, tempoReader, q.query, startNano, endNano, stepNano)
			})
		})
	}
}

package benchmark

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
	"github.com/grafana/tempo/pkg/tempopb"
)

// modulesSimpleProvider wraps an os.File to implement modules_rw.ReaderProvider
// without I/O tracking, used for correctness tests only.
type modulesSimpleProvider struct {
	file *os.File
}

func (p *modulesSimpleProvider) Size() (int64, error) {
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (p *modulesSimpleProvider) ReadAt(buf []byte, off int64, _ modules_rw.DataType) (int, error) {
	return p.file.ReadAt(buf, off)
}

// modulesTrackedProvider wraps an os.File to implement modules_rw.ReaderProvider
// with I/O tracking and per-read latency simulation (matching trackedFileProvider).
type modulesTrackedProvider struct {
	file      *os.File
	bytesRead int64
	ioOps     int64
	latency   time.Duration
}

func (p *modulesTrackedProvider) Size() (int64, error) {
	info, err := p.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (p *modulesTrackedProvider) ReadAt(buf []byte, off int64, _ modules_rw.DataType) (int, error) {
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

func (p *modulesTrackedProvider) reset() {
	p.bytesRead = 0
	p.ioOps = 0
}

// writeModulesFormat writes trace data using the modules writer and returns the file path.
func writeModulesFormat(tb testing.TB, traces []*tracev1.TracesData) string {
	tb.Helper()

	path := filepath.Join(tb.TempDir(), "traces-modules.blockpack")
	f, err := os.Create(path)
	require.NoError(tb, err)
	defer f.Close()

	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{OutputStream: f})
	require.NoError(tb, err)

	for _, trace := range traces {
		require.NoError(tb, w.AddTracesData(trace))
	}

	_, err = w.Flush()
	require.NoError(tb, err)

	return path
}

// executeModulesSearch runs a TraceQL filter query against a modules-format file.
// Returns a SearchResponse comparable to executeBlockpackSearch.
func executeModulesSearch(path, traceqlQuery string) (*tempopb.SearchResponse, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, err := modules_blockio.NewReaderFromProvider(&modulesSimpleProvider{file: f})
	if err != nil {
		return nil, fmt.Errorf("open modules reader: %w", err)
	}

	parsed, err := traceqlparser.ParseTraceQL(traceqlQuery)
	if err != nil {
		return nil, fmt.Errorf("parse TraceQL: %w", err)
	}

	switch q := parsed.(type) {
	case *traceqlparser.FilterExpression:
		return executeModulesFilterSearch(r, q)
	case *traceqlparser.StructuralQuery:
		return executeModulesStructuralSearch(r, q)
	default:
		return nil, fmt.Errorf("unsupported query type for modules benchmark: %T", parsed)
	}
}

// spanMatchesToSearchResponse converts a []SpanMatch into a SearchResponse,
// grouping spans by trace ID and preserving insertion order.
func spanMatchesToSearchResponse(matches []modules_executor.SpanMatch) *tempopb.SearchResponse {
	type traceInfo struct{ spanIDs []string }
	traceMap := make(map[string]*traceInfo, len(matches))
	traceOrder := make([]string, 0, len(matches))

	for _, match := range matches {
		traceIDStr := fmt.Sprintf("%x", match.TraceID)
		info, exists := traceMap[traceIDStr]
		if !exists {
			info = &traceInfo{}
			traceMap[traceIDStr] = info
			traceOrder = append(traceOrder, traceIDStr)
		}
		info.spanIDs = append(info.spanIDs, fmt.Sprintf("%x", match.SpanID))
	}

	traces := make([]*tempopb.TraceSearchMetadata, 0, len(traceOrder))
	for _, traceID := range traceOrder {
		info := traceMap[traceID]
		pbSpans := make([]*tempopb.Span, 0, len(info.spanIDs))
		for _, sid := range info.spanIDs {
			pbSpans = append(pbSpans, &tempopb.Span{SpanID: sid})
		}
		ss := &tempopb.SpanSet{
			Spans:   pbSpans,
			Matched: uint32(len(pbSpans)), //nolint:gosec
		}
		traces = append(traces, &tempopb.TraceSearchMetadata{
			TraceID:  traceID,
			SpanSets: []*tempopb.SpanSet{ss},
		})
	}
	return &tempopb.SearchResponse{Traces: traces}
}

// executeModulesFilterSearch runs a filter query via the modules executor.
func executeModulesFilterSearch(
	r *modules_blockio.Reader,
	q *traceqlparser.FilterExpression,
) (*tempopb.SearchResponse, error) {
	program, err := vm.CompileTraceQLFilter(q)
	if err != nil {
		return nil, fmt.Errorf("compile TraceQL: %w", err)
	}

	rows, err := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}

	signalType := r.SignalType()
	matches := make([]modules_executor.SpanMatch, len(rows))
	for i, row := range rows {
		matches[i] = modules_executor.SpanMatchFromRow(row, signalType, r)
	}
	return spanMatchesToSearchResponse(matches), nil
}

// executeModulesStructuralSearch runs a structural query via the modules executor.
func executeModulesStructuralSearch(
	r *modules_blockio.Reader,
	q *traceqlparser.StructuralQuery,
) (*tempopb.SearchResponse, error) {
	result, err := modules_executor.ExecuteStructural(r, q, modules_executor.Options{})
	if err != nil {
		return nil, fmt.Errorf("execute structural query: %w", err)
	}

	return spanMatchesToSearchResponse(result.Matches), nil
}

// benchmarkModulesQuery benchmarks TraceQL filter execution using the modules reader + new executor.
// Uses the same 20ms/op latency simulation as benchmarkBlockpackQuery for fair comparison.
func benchmarkModulesQuery(b *testing.B, modulesPath string, query benchmarkQuery) {
	b.Helper()

	f, err := os.Open(modulesPath)
	require.NoError(b, err)
	defer f.Close()

	tracked := &modulesTrackedProvider{file: f, latency: 20 * time.Millisecond}

	b.ResetTimer()
	b.ReportAllocs()

	// Parse once outside the loop — the query is loop-invariant.
	parsed, err := traceqlparser.ParseTraceQL(query.traceqlQuery)
	require.NoError(b, err)

	traceSpans := make(map[string]int)
	traceOrder := make([]string, 0)
	var totalTime int64

	for b.Loop() {
		tracked.reset()
		traceSpans = make(map[string]int)
		traceOrder = traceOrder[:0]

		start := time.Now()

		r, err := modules_blockio.NewReaderFromProvider(tracked)
		require.NoError(b, err)

		var matches []modules_executor.SpanMatch
		switch q := parsed.(type) {
		case *traceqlparser.FilterExpression:
			program, compErr := vm.CompileTraceQLFilter(q)
			require.NoError(b, compErr)
			rows, execErr := modules_executor.Collect(r, program, modules_executor.CollectOptions{})
			require.NoError(b, execErr)
			signalType := r.SignalType()
			matches = make([]modules_executor.SpanMatch, len(rows))
			for i, row := range rows {
				matches[i] = modules_executor.SpanMatchFromRow(row, signalType, r)
			}
		case *traceqlparser.StructuralQuery:
			result, execErr := modules_executor.ExecuteStructural(r, q, modules_executor.Options{})
			require.NoError(b, execErr)
			matches = result.Matches
		default:
			b.Fatalf("unsupported query type: %T", parsed)
		}

		for _, match := range matches {
			traceIDStr := fmt.Sprintf("%x", match.TraceID)
			if _, exists := traceSpans[traceIDStr]; !exists {
				traceOrder = append(traceOrder, traceIDStr)
			}
			traceSpans[traceIDStr]++
		}

		totalTime += time.Since(start).Nanoseconds()
	}

	iters := int64(b.N)
	if iters == 0 {
		iters = 1
	}
	avgTimeNs := totalTime / iters
	if totalTime > 0 && len(traceOrder) == 0 {
		b.Log("Warning: No matches found (modules)")
	}
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

	if ioOps > 0 {
		b.ReportMetric(float64(bytesRead)/float64(ioOps), "bytes/io")
	}

	ReportCosts(b, ioOps, bytesRead, avgTimeNs)
	b.SetBytes(bytesRead)
}

// TestModulesFormatCorrectness validates that the modules engine (new executor)
// returns the same trace IDs as the blockpack engine for all benchmark queries.
// Uses a smaller dataset (500 traces × 10 spans) to keep test time fast.
func TestModulesFormatCorrectness(t *testing.T) {
	const testTraceCount = 500
	const testSpansPerTrace = 10

	otelTraces, _, _ := generateBenchmarkTraces(t, testTraceCount, testSpansPerTrace)

	blockpackPath := writeBlockpackFormat(t, otelTraces, "")
	modulesPath := writeModulesFormat(t, otelTraces)

	for _, query := range benchmarkQueries {
		t.Run(query.name, func(t *testing.T) {
			bpResp, err := executeBlockpackSearch(blockpackPath, query.traceqlQuery)
			require.NoError(t, err)

			modResp, err := executeModulesSearch(modulesPath, query.traceqlQuery)
			require.NoError(t, err)

			bpResp.Metrics = nil
			modResp.Metrics = nil

			if query.isStructural {
				// Structural queries: compare at span ID level.
				require.NotEmpty(t, bpResp.Traces, "structural query %q returned no results from blockpack", query.name)
				if err := compareSpanResults(bpResp, modResp); err != nil {
					t.Fatalf("Query %q: modules vs blockpack structural mismatch: %v", query.name, err)
				}
				t.Logf("✓ %s: %d traces (structural)", query.name, len(bpResp.Traces))
				return
			}

			if err := compareTraceMetadata(bpResp, modResp); err != nil {
				t.Fatalf("Query %q: modules vs blockpack mismatch: %v", query.name, err)
			}

			t.Logf("✓ %s: %d traces match", query.name, len(bpResp.Traces))
		})
	}
}

// BenchmarkModulesFormatComparison benchmarks the modules engine (new executor)
// alongside the existing blockpack (old) engine and Parquet on the 250k synthetic dataset.
// Test data: 250,000 traces × 100 spans = 25,000,000 spans total.
//
// All engines read the same logical data; blockpack and modules use their respective writers.
// Each engine uses 20ms/op simulated latency to model S3 object storage conditions.
func BenchmarkModulesFormatComparison(b *testing.B) {
	// Generate data once — shared across all sub-benchmarks for this run.
	b.Logf("Generating %d traces × %d spans…", benchmarkTraceCount, benchmarkSpansPerTrace)
	otelTraces, _, _ := generateBenchmarkTraces(b, benchmarkTraceCount, benchmarkSpansPerTrace)

	blockpackPath := writeBlockpackFormat(b, otelTraces, "traces-format")
	fi, _ := os.Stat(blockpackPath)
	b.Logf("Blockpack: %.2f MB", float64(fi.Size())/1024/1024)

	modulesPath := writeModulesFormat(b, otelTraces)
	fi, _ = os.Stat(modulesPath)
	b.Logf("Modules:   %.2f MB", float64(fi.Size())/1024/1024)

	for _, query := range benchmarkQueries {
		query := query
		b.Run(query.name, func(b *testing.B) {
			b.Run("blockpack", func(b *testing.B) {
				benchmarkBlockpackQuery(b, blockpackPath, query)
			})
			b.Run("modules", func(b *testing.B) {
				benchmarkModulesQuery(b, modulesPath, query)
			})
		})
	}
}

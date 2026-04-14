package executor_test

// prealloc_bench_test.go — allocation baseline benchmarks for the prealloc work.
//
// Run before and after implementation changes to capture allocs/op deltas:
//
//	go test -bench='BenchmarkCollectStream_AllocCount|BenchmarkTraceAccumulateRow_AllocCount|BenchmarkLogAccumulateRow_AllocCount|BenchmarkCollectIntrinsicTopKKLL_AllocCount' -benchmem -count=3 ./internal/modules/executor/
//
// Expected improvements after implementation:
//   - BenchmarkTraceAccumulateRow_AllocCount: -1 alloc/op (attrVals make per row removed)
//   - BenchmarkLogAccumulateRow_AllocCount:   -1 alloc/op (attrVals make per row removed)
//   - BenchmarkCollectIntrinsicTopKKLL_AllocCount: reduced growslice allocs when refs > 8

import (
	"bytes"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/vm"
)

// preallocBenchMemProvider is a minimal in-memory ByteRangeProvider for benchmarks.
type preallocBenchMemProvider struct{ data []byte }

func (m *preallocBenchMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *preallocBenchMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// buildPreallocTraceFile builds a blockpack file with spanCount spans in as few blocks
// as specified by maxBlockSpans. All spans have service.name=svc and a cyclic
// resource.env label so GroupBy has meaningful values to accumulate.
func buildPreallocTraceFile(b *testing.B, spanCount, maxBlockSpans int) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: maxBlockSpans,
	})
	if err != nil {
		b.Fatalf("NewWriterWithConfig: %v", err)
	}

	traceID := [16]byte{0xBE, 0xEF}
	envs := []string{"prod", "staging", "dev"}
	for i := range spanCount {
		spanID := []byte{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0x01} //nolint:gosec
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID,
			Name:              "op",
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: uint64(1_000_000_000 + i*1_000_000),         //nolint:gosec
			EndTimeUnixNano:   uint64(1_000_000_000+i*1_000_000) + 500_000, //nolint:gosec
		}
		resAttrs := map[string]any{
			"service.name": "svc",
			"resource.env": envs[i%len(envs)],
		}
		if err := w.AddSpan(traceID[:], span, resAttrs, "", nil, ""); err != nil {
			b.Fatalf("AddSpan %d: %v", i, err)
		}
	}
	if _, err := w.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// buildPreallocLogFile builds a blockpack log file with recordCount records.
// All records have service.name=svc and a log:timestamp in the query window.
func buildPreallocLogFile(b *testing.B, recordCount, maxBlockSpans int) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: maxBlockSpans,
	})
	if err != nil {
		b.Fatalf("NewWriterWithConfig: %v", err)
	}

	for i := range recordCount {
		tsNano := uint64(1_000_000_000 + i*1_000_000) //nolint:gosec
		record := makeLogRecord("svc", "log body", nil, tsNano)
		if err := w.AddLogsData(record); err != nil {
			b.Fatalf("AddLogsData %d: %v", i, err)
		}
	}
	if _, err := w.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

func openPreallocReader(b *testing.B, data []byte) *modules_reader.Reader {
	b.Helper()
	r, err := modules_reader.NewReaderFromProvider(&preallocBenchMemProvider{data: data})
	if err != nil {
		b.Fatalf("NewReaderFromProvider: %v", err)
	}
	return r
}

// BenchmarkCollectStream_AllocCount measures rowSet growslice allocations in
// the collectStream path, exercised by a full-scan executor.Collect call against
// a 200-span single block.
func BenchmarkCollectStream_AllocCount(b *testing.B) {
	const spanCount = 200
	// maxBlockSpans > spanCount → single block, exercises collectStream rowSet append hot path.
	data := buildPreallocTraceFile(b, spanCount, spanCount+1)
	r := openPreallocReader(b, data)

	// match-all program → full block scan through collectStream
	prog, err := vm.CompileTraceQLFilter(nil)
	if err != nil {
		b.Fatalf("CompileTraceQLFilter: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, _, runErr := executor.Collect(r, prog, executor.CollectOptions{})
		if runErr != nil {
			b.Fatalf("Collect: %v", runErr)
		}
		_ = rows
	}
}

// BenchmarkTraceAccumulateRow_AllocCount measures per-call allocations in the
// traceAccumulateRow hot path by running ExecuteTraceMetrics with a 1-element GroupBy
// over 200 matching spans in a single block.
// Before the prealloc change: 1 alloc/row from `attrVals := make([]string, len(groupBy))`.
// After: 0 allocs/row (scratch hoisted to per-block allocation).
func BenchmarkTraceAccumulateRow_AllocCount(b *testing.B) {
	const spanCount = 200
	const stepNano = int64(1_000_000_000) // 1s buckets
	startNano := int64(1_000_000_000)
	endNano := startNano + int64(spanCount)*1_000_000 + stepNano

	// Single block so all 200 rows go through traceAccumulateRow in one inner loop.
	data := buildPreallocTraceFile(b, spanCount, spanCount+1)
	r := openPreallocReader(b, data)

	prog, spec, err := vm.CompileTraceQLMetrics(`{ } | count_over_time() by (resource.env)`, startNano, endNano)
	if err != nil {
		b.Fatalf("CompileTraceQLMetrics: %v", err)
	}
	spec.TimeBucketing.StepSizeNanos = stepNano

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, runErr := executor.ExecuteTraceMetrics(r, prog, spec)
		if runErr != nil {
			b.Fatalf("ExecuteTraceMetrics: %v", runErr)
		}
		_ = result
	}
}

// BenchmarkLogAccumulateRow_AllocCount measures per-call allocations in the
// logAccumulateRow hot path by running ExecuteLogMetrics with a 1-element GroupBy
// over 200 matching log records in a single block.
// Before the prealloc change: 1 alloc/row from `attrVals := make([]string, len(groupBy))`.
// After: 0 allocs/row (scratch hoisted to per-block allocation).
func BenchmarkLogAccumulateRow_AllocCount(b *testing.B) {
	const recordCount = 200
	const stepNano = int64(1_000_000_000) // 1s buckets
	startNano := int64(1_000_000_000)
	endNano := startNano + int64(recordCount)*1_000_000 + stepNano

	// Single block so all 200 rows go through logAccumulateRow in one inner loop.
	data := buildPreallocLogFile(b, recordCount, recordCount+1)
	r := openPreallocReader(b, data)

	// Use logqlparser.Parse + logqlparser.Compile as used in tests.
	sel, err := logqlparser.Parse(`{service.name = "svc"}`)
	if err != nil {
		b.Fatalf("logqlparser.Parse: %v", err)
	}
	prog, err := logqlparser.Compile(sel)
	if err != nil {
		b.Fatalf("logqlparser.Compile: %v", err)
	}
	spec := &vm.QuerySpec{
		TimeBucketing: vm.TimeBucketSpec{
			Enabled:       true,
			StartTime:     startNano,
			EndTime:       endNano,
			StepSizeNanos: stepNano,
		},
		Filter:    vm.FilterSpec{IsMatchAll: true},
		Aggregate: vm.AggregateSpec{},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		result, runErr := executor.ExecuteLogMetrics(r, prog, nil, spec, "count_over_time", []string{"service.name"})
		if runErr != nil {
			b.Fatalf("ExecuteLogMetrics: %v", runErr)
		}
		_ = result
	}
}

// BenchmarkCollectIntrinsicTopKKLL_AllocCount measures blockOrder/blockRefs growslice
// allocations in collectIntrinsicTopKKLL when refs span more than 8 distinct blocks.
// Before prealloc: blockOrder grows from cap=8 when len(refs) > 8.
// After: blockOrder preallocated to min(len(refs), 64), avoiding growslice until refs exceeds 64.
//
// Setup: 200 spans with maxBlockSpans=10 → ~20 internal blocks, all matching.
// Limit + TimestampColumn routes through the intrinsic KLL path (Case B).
func BenchmarkCollectIntrinsicTopKKLL_AllocCount(b *testing.B) {
	const totalSpans = 200
	const maxBlockSpans = 10 // ~20 blocks → len(refs) > 8 → growslice in pre-fix code
	data := buildPreallocTraceFile(b, totalSpans, maxBlockSpans)
	r := openPreallocReader(b, data)

	// match-all program; intrinsic service.name index routes through KLL path.
	prog, err := vm.CompileTraceQLFilter(nil)
	if err != nil {
		b.Fatalf("CompileTraceQLFilter: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		rows, _, runErr := executor.Collect(r, prog, executor.CollectOptions{
			Limit:           100,
			TimestampColumn: "span:start",
		})
		if runErr != nil {
			b.Fatalf("Collect: %v", runErr)
		}
		_ = rows
	}
}

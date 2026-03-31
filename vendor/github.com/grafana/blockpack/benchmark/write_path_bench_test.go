package benchmark

import (
	"os"
	"testing"

	"github.com/grafana/tempo/pkg/tempopb"
	common "github.com/grafana/tempo/tempodb/encoding/common"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

const (
	writePathTraceCount = 10000
	// writePathSpansPerTrace is passed to generateBenchmarkTraces to control the target spans per
	// trace. The actual span count varies by xk6 template (7, 10, 3, or 5 spans per template in
	// rotation). The real total is counted from the generated data via countSpans.
	writePathSpansPerTrace = 100
)

// BenchmarkWritePathComparison measures bulk write performance for blockpack and parquet.
//
// Benchmark name hierarchy:
//
//	WritePathComparison/small/blockpack
//	WritePathComparison/small/parquet
//
// Data scale: 10,000 traces x ~62.5 actual spans (varies by xk6 template) = ~625,000 spans total.
// This is intentionally smaller than BenchmarkFormatComparison (25M spans) because vparquet5.CreateBlock
// builds bloom filters and multiple index files, making large-scale writes prohibitively slow.
//
// Profiling: activated by BENCH_PROFILE_CPU=true BENCH_PROFILE_MEM=true BENCH_PROFILE_DIR=<dir>
// (see make bench-profile-writes).
func BenchmarkWritePathComparison(b *testing.B) {
	profiler, err := startBenchProfiler("write_path")
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

	otelTraces, tempoTraces, traceIDs := generateBenchmarkTraces(b, writePathTraceCount, writePathSpansPerTrace)

	// Count actual spans from the generated data. xk6 templates have varying span counts
	// (7, 10, 3, 5) so the actual total differs from writePathTraceCount * writePathSpansPerTrace.
	totalSpans := countSpans(otelTraces)

	b.Run("small", func(b *testing.B) {
		b.Run("blockpack", func(b *testing.B) {
			benchmarkBlockpackWrite(b, otelTraces, totalSpans)
		})

		b.Run("parquet", func(b *testing.B) {
			benchmarkParquetWrite(b, tempoTraces, traceIDs, totalSpans)
		})
	})
}

// countSpans returns the total number of spans across all TracesData entries.
func countSpans(traces []*tracev1.TracesData) int64 {
	var total int64

	for _, td := range traces {
		for _, rs := range td.GetResourceSpans() {
			for _, ss := range rs.GetScopeSpans() {
				total += int64(len(ss.GetSpans()))
			}
		}
	}

	return total
}

// benchmarkBlockpackWrite exercises the full blockpack write path and reports three custom
// metrics in addition to the standard Go benchmark metrics (sec/op, B/op, allocs/op):
//
//   - bytes_written: total bytes in the serialized blockpack buffer
//   - spans_written: number of spans actually written (counted from input traces)
//   - ns/span: cumulative elapsed nanoseconds divided by (N * spans_written)
func benchmarkBlockpackWrite(b *testing.B, traces []*tracev1.TracesData, totalSpans int64) {
	b.Helper()

	var outputSize int64
	var totalCPUMs float64

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cpuBefore := readCPUSecs()
		data := writeBlockpackFormat(b, traces, "traces-writepath")
		cpuAfter := readCPUSecs()
		totalCPUMs += ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
		b.StopTimer()
		fi, err := os.Stat(data)
		if err != nil {
			b.Fatalf("Failed to stat blockpack file: %v", err)
		}
		outputSize = fi.Size()
		b.StartTimer()
	}

	b.StopTimer()

	elapsed := b.Elapsed()

	b.SetBytes(outputSize)
	b.ReportMetric(float64(outputSize), "bytes_written")
	b.ReportMetric(float64(totalSpans), "spans_written")
	b.ReportMetric(totalCPUMs/float64(b.N), "cpuMs")

	if totalSpans > 0 {
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(int64(b.N)*totalSpans), "ns/span")
	}
}

// benchmarkParquetWrite measures the parquet (vparquet5) write path.
// Note: blockpack writes to an in-memory buffer; parquet writes to a temp filesystem directory
// with bloom filter construction, dedicated columns, and block metadata. This asymmetry is
// intentional — it reflects how each format is actually used in production.
//
// Reports the same custom metrics as benchmarkBlockpackWrite. The cleanup of the temp directory
// is performed inside the timer-stopped region so filesystem removal time does not inflate write time.
func benchmarkParquetWrite(b *testing.B, traces []*tempopb.Trace, traceIDs []common.ID, totalSpans int64) {
	b.Helper()

	var outputSize int64
	var totalCPUMs float64

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cpuBefore := readCPUSecs()
		_, _, totalSize, _, cleanup := writeTempoBlock(b, traces, traceIDs, "traces-writepath")
		cpuAfter := readCPUSecs()
		totalCPUMs += ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
		b.StopTimer()
		cleanup()
		outputSize = totalSize
		b.StartTimer()
	}

	b.StopTimer()
	elapsed := b.Elapsed()

	b.SetBytes(outputSize)
	b.ReportMetric(float64(outputSize), "bytes_written")
	b.ReportMetric(float64(totalSpans), "spans_written")
	b.ReportMetric(totalCPUMs/float64(b.N), "cpuMs")

	if totalSpans > 0 {
		b.ReportMetric(float64(elapsed.Nanoseconds())/float64(int64(b.N)*totalSpans), "ns/span")
	}
}

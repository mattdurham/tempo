package queryplanner_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// writeNBlockFile writes a blockpack file with n blocks, each having blockSize spans
// with a unique service name ("svc-0", "svc-1", ..., "svc-{n-1}").
func writeNBlockFile(b *testing.B, nBlocks, blockSize int) []byte {
	b.Helper()
	var buf bytes.Buffer
	wc, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: blockSize,
	})
	require.NoError(b, err)

	spanIdx := 0
	for blk := range nBlocks {
		svc := fmt.Sprintf("svc-%d", blk)
		for i := range blockSize {
			sp := &tracev1.Span{
				TraceId: []byte{
					byte(spanIdx),
					byte(spanIdx >> 8),
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
				}, //nolint:gosec
				SpanId: []byte{
					byte(spanIdx + 1),
					0,
					0,
					0,
					0,
					0,
					0,
					0,
				}, //nolint:gosec
				Name:              fmt.Sprintf("op-%d-%d", blk, i),
				StartTimeUnixNano: uint64(spanIdx) * 1_000_000_000,             //nolint:gosec
				EndTimeUnixNano:   uint64(spanIdx)*1_000_000_000 + 500_000_000, //nolint:gosec
			}
			require.NoError(b, wc.AddSpan(sp.TraceId, sp, map[string]any{"service.name": svc}, "", nil, ""))
			spanIdx++
		}
	}
	_, err = wc.Flush()
	require.NoError(b, err)
	return buf.Bytes()
}

func benchOpenReader(b *testing.B, data []byte) *reader.Reader {
	b.Helper()
	r, err := reader.NewReaderFromProvider(&memProvider{data: data})
	require.NoError(b, err)
	return r
}

// BenchmarkPlannerPruning measures Plan() latency and pruning effectiveness
// across different block counts. Queries a single service that exists in
// exactly 1 block out of N — sketch pruning should eliminate N-1 blocks.
func BenchmarkPlannerPruning(b *testing.B) {
	for _, nBlocks := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("blocks=%d", nBlocks), func(b *testing.B) {
			data := writeNBlockFile(b, nBlocks, 50)
			r := benchOpenReader(b, data)
			require.Equal(b, nBlocks, r.BlockCount())

			p := queryplanner.NewPlanner(r)
			pred := queryplanner.Predicate{
				Columns: []string{"resource.service.name"},
				Values:  []string{"svc-0"}, // exists only in block 0
			}

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})
				if len(plan.SelectedBlocks) == 0 {
					b.Fatal("expected at least 1 selected block")
				}
			}

			// Report pruning stats from last iteration.
			plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})
			b.ReportMetric(float64(plan.TotalBlocks), "total_blocks")
			b.ReportMetric(float64(len(plan.SelectedBlocks)), "selected_blocks")
			b.ReportMetric(float64(plan.PrunedByIndex), "pruned_by_index")
			b.ReportMetric(float64(plan.PrunedByFuse), "pruned_by_fuse")
		})
	}
}

// BenchmarkPlannerSketchOnly measures Plan() for a column that has sketch data
// but NO range index. This is where fuse+TopK pruning provides value that the
// range index cannot.
//
// Setup: N blocks, each with distinct span-level attribute "http.method" values.
// Block 0 has "GET", block 1 has "POST", etc. The span attribute column has
// sketch data but no range index (only resource attributes get range indexes).
func BenchmarkPlannerSketchOnly(b *testing.B) {
	for _, nBlocks := range []int{4, 8, 16, 32} {
		b.Run(fmt.Sprintf("blocks=%d", nBlocks), func(b *testing.B) {
			methods := []string{
				"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE",
				"CONNECT", "CUSTOM0", "CUSTOM1", "CUSTOM2", "CUSTOM3", "CUSTOM4", "CUSTOM5", "CUSTOM6",
				"CUSTOM7", "CUSTOM8", "CUSTOM9", "CUSTOMA", "CUSTOMB", "CUSTOMC", "CUSTOMD", "CUSTOME",
				"CUSTOMF", "CUSTOMG", "CUSTOMH", "CUSTOMI", "CUSTOMJ", "CUSTOMK", "CUSTOML", "CUSTOMM",
			}

			var buf bytes.Buffer
			wc, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
				OutputStream:  &buf,
				MaxBlockSpans: 50,
			})
			require.NoError(b, err)

			spanIdx := 0
			for blk := range nBlocks {
				method := methods[blk%len(methods)]
				for range 50 {
					sp := &tracev1.Span{
						TraceId: []byte{
							byte(spanIdx),
							byte(spanIdx >> 8),
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
							0,
						}, //nolint:gosec
						SpanId: []byte{
							byte(spanIdx + 1),
							0,
							0,
							0,
							0,
							0,
							0,
							0,
						}, //nolint:gosec
						Name:              "op",
						StartTimeUnixNano: uint64(spanIdx) * 1_000_000_000,             //nolint:gosec
						EndTimeUnixNano:   uint64(spanIdx)*1_000_000_000 + 500_000_000, //nolint:gosec
						Attributes: []*commonv1.KeyValue{
							{
								Key:   "http.method",
								Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: method}},
							},
						},
					}
					require.NoError(b, wc.AddSpan(sp.TraceId, sp, map[string]any{"service.name": "svc"}, "", nil, ""))
					spanIdx++
				}
			}
			_, err = wc.Flush()
			require.NoError(b, err)
			data := buf.Bytes()

			r := benchOpenReader(b, data)
			require.Equal(b, nBlocks, r.BlockCount())

			// Verify: span.http.method should NOT have a range index.
			_, hasRange := r.RangeColumnType("span.http.method")
			// Verify: span.http.method should have sketch data.
			cs := r.ColumnSketch("span.http.method")

			p := queryplanner.NewPlanner(r)
			pred := queryplanner.Predicate{
				Columns: []string{"span.http.method"},
				Values:  []string{"GET"}, // exists only in block 0
			}

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})
				_ = plan
			}

			// Report pruning stats.
			plan := p.Plan([]queryplanner.Predicate{pred}, queryplanner.TimeRange{})
			b.ReportMetric(float64(plan.TotalBlocks), "total_blocks")
			b.ReportMetric(float64(len(plan.SelectedBlocks)), "selected_blocks")
			b.ReportMetric(float64(plan.PrunedByIndex), "pruned_by_index")
			b.ReportMetric(float64(plan.PrunedByFuse), "pruned_by_fuse")

			if !hasRange {
				b.Log("span.http.method: no range index (sketch-only pruning)")
			}
			if cs != nil {
				b.Log("span.http.method: sketch data present")
			} else {
				b.Log("span.http.method: NO sketch data")
			}
		})
	}
}

// BenchmarkPlannerNoPredicate measures Plan() with no predicates (baseline).
func BenchmarkPlannerNoPredicate(b *testing.B) {
	for _, nBlocks := range []int{4, 16, 32} {
		b.Run(fmt.Sprintf("blocks=%d", nBlocks), func(b *testing.B) {
			data := writeNBlockFile(b, nBlocks, 50)
			r := benchOpenReader(b, data)
			require.Equal(b, nBlocks, r.BlockCount())

			p := queryplanner.NewPlanner(r)

			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				plan := p.Plan(nil, queryplanner.TimeRange{})
				if len(plan.SelectedBlocks) != nBlocks {
					b.Fatalf("expected %d blocks, got %d", nBlocks, len(plan.SelectedBlocks))
				}
			}
		})
	}
}

package executor_test

// BenchmarkCollect_IntrinsicMaterialization measures the end-to-end cost of queries
// that find matching spans and materialize their intrinsic fields.
//
// This benchmark catches the regression introduced by PR #172 where intrinsic columns
// were removed from block payloads. After that change, every field access during result
// materialization required an O(N) scan through the intrinsic column section (sorted by
// value, not by BlockRef), causing O(8.6B) operations per query at production scale.
//
// With dual storage (block columns + intrinsic section), field access is O(1) per row
// because intrinsic values are read directly from the parsed block column by row index.
//
// Run with:
//
//	go test -bench=BenchmarkCollect_IntrinsicMaterialization -benchmem ./internal/modules/executor/

import (
	"bytes"
	"testing"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	"github.com/grafana/blockpack/internal/traceqlparser"
	"github.com/grafana/blockpack/internal/vm"
)

const (
	intrinsicBenchMaxBlockSpans = 100  // MaxBlockSpans → ~50 blocks for 5000 spans
	intrinsicBenchTotalSpans    = 5000 // ~50 blocks × 100 spans
)

// intrinsicBenchMemProvider is an in-memory ByteRangeProvider for benchmarks.
type intrinsicBenchMemProvider struct{ data []byte }

func (m *intrinsicBenchMemProvider) Size() (int64, error) { return int64(len(m.data)), nil }

func (m *intrinsicBenchMemProvider) ReadAt(p []byte, off int64, _ modules_rw.DataType) (int, error) {
	if off < 0 || off > int64(len(m.data)) {
		return 0, bytes.ErrTooLarge
	}
	n := copy(p, m.data[off:])
	return n, nil
}

// buildIntrinsicBenchFile writes a file with intrinsicBenchTotalSpans spans across
// multiple blocks. Uses 5 distinct service names and 3 span kinds so predicate
// selectivity is ~20% per service name (~1000 matching spans per query).
func buildIntrinsicBenchFile(b *testing.B) []byte {
	b.Helper()
	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: intrinsicBenchMaxBlockSpans,
	})
	if err != nil {
		b.Fatalf("NewWriterWithConfig: %v", err)
	}

	serviceNames := []string{"auth-service", "query-service", "ingester", "distributor", "compactor"}
	spanNames := []string{"HTTP GET", "HTTP POST", "gRPC Query", "Write", "Flush"}
	kinds := []tracev1.Span_SpanKind{
		tracev1.Span_SPAN_KIND_SERVER,
		tracev1.Span_SPAN_KIND_CLIENT,
		tracev1.Span_SPAN_KIND_INTERNAL,
	}

	traceID := [16]byte{0xBE, 0xEF}
	for i := range intrinsicBenchTotalSpans {
		spanID := []byte{byte(i), byte(i >> 8), byte(i >> 16), 0, 0, 0, 0, 0x01} //nolint:gosec
		span := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            spanID,
			Name:              spanNames[i%len(spanNames)],
			Kind:              kinds[i%len(kinds)],
			StartTimeUnixNano: uint64(i) * 1_000_000,                     //nolint:gosec
			EndTimeUnixNano:   uint64(i)*1_000_000 + uint64(i%500)*1_000, //nolint:gosec
		}
		resourceAttrs := map[string]any{
			"service.name": serviceNames[i%len(serviceNames)],
		}
		if err := w.AddSpan(traceID[:], span, resourceAttrs, "", nil, ""); err != nil {
			b.Fatalf("AddSpan %d: %v", i, err)
		}
	}
	if _, err := w.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	return buf.Bytes()
}

// openIntrinsicBenchReader returns a Reader over the given data.
func openIntrinsicBenchReader(b *testing.B, data []byte) *modules_reader.Reader {
	b.Helper()
	r, err := modules_reader.NewReaderFromProvider(&intrinsicBenchMemProvider{data: data})
	if err != nil {
		b.Fatalf("NewReaderFromProvider: %v", err)
	}
	return r
}

// compileBenchQuery parses and compiles a TraceQL filter expression for benchmarks.
// Named differently from compileQuery (which accepts *testing.T) to avoid collision.
func compileBenchQuery(b *testing.B, query string) *vm.Program {
	b.Helper()
	parsed, err := traceqlparser.ParseTraceQL(query)
	if err != nil {
		b.Fatalf("ParseTraceQL %q: %v", query, err)
	}
	filterExpr, ok := parsed.(*traceqlparser.FilterExpression)
	if !ok {
		b.Fatalf("expected FilterExpression for %q", query)
	}
	program, err := vm.CompileTraceQLFilter(filterExpr)
	if err != nil {
		b.Fatalf("CompileTraceQLFilter %q: %v", query, err)
	}
	return program
}

// BenchmarkCollect_IntrinsicMaterialization measures the end-to-end query cost for
// queries that exercise the intrinsic pre-filter and result materialization paths.
//
// Sub-benchmarks:
//   - PureIntrinsic: service.name predicate only; exercises intrinsic pre-filter +
//     block column O(1) field access during materialization (~1000 matching spans).
//   - MixedQuery: service.name AND span:kind predicate; exercises the mixed execution
//     path where intrinsic pre-filter narrows candidates and block scan evaluates the
//     non-intrinsic predicate.
//   - ResultFields: same as PureIntrinsic but with AllColumns=true, forcing
//     IterateFields over every block column for each matching span; exercises the
//     full materialization cost including intrinsic columns in the block payload.
func BenchmarkCollect_IntrinsicMaterialization(b *testing.B) {
	data := buildIntrinsicBenchFile(b)

	b.Run("PureIntrinsic", func(b *testing.B) {
		// Query matches ~1000 of 5000 spans via service.name (intrinsic column).
		// With dual storage, the executor uses the intrinsic pre-filter (Case A/B)
		// and materializes field values from block columns in O(1) per span.
		// Regression: without dual storage (PR #172), each field access scanned O(N)
		// entries in the intrinsic section per span via loadIntrinsicCache.
		r := openIntrinsicBenchReader(b, data)
		prog := compileBenchQuery(b, `{ resource.service.name = "auth-service" }`)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows, _, err := executor.Collect(r, prog, executor.CollectOptions{})
			if err != nil {
				b.Fatalf("Collect: %v", err)
			}
			_ = rows
		}
	})

	b.Run("MixedQuery", func(b *testing.B) {
		// Query combines an intrinsic predicate (service.name) with a non-intrinsic
		// kind predicate. Exercises the mixed execution path (Case C/D): intrinsic
		// pre-filter narrows the candidate block set, then block scan evaluates the
		// span:kind predicate within those blocks. Tests intrinsic block column access
		// interleaved with non-intrinsic column access.
		r := openIntrinsicBenchReader(b, data)
		prog := compileBenchQuery(b, `{ resource.service.name = "auth-service" && span:kind = server }`)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows, _, err := executor.Collect(r, prog, executor.CollectOptions{})
			if err != nil {
				b.Fatalf("Collect: %v", err)
			}
			_ = rows
		}
	})

	b.Run("ResultFields", func(b *testing.B) {
		// Same query as PureIntrinsic but with AllColumns=true, forcing IterateFields
		// over ALL block columns (including intrinsic: trace:id, span:id, span:name,
		// span:kind, span:start, span:end, span:duration, resource.service.name) for
		// each matching span. Exercises the full result-materialization hot path that
		// showed 62.71% flat CPU in pprof profiles after PR #172.
		// With dual storage, IterateFields reads from block columns (O(1) per row).
		// Without dual storage (PR #172), GetField triggered loadIntrinsicCache — an
		// O(N) linear scan through all intrinsic column entries for each matched span.
		r := openIntrinsicBenchReader(b, data)
		prog := compileBenchQuery(b, `{ resource.service.name = "auth-service" }`)
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			rows, _, err := executor.Collect(r, prog, executor.CollectOptions{
				AllColumns: true,
			})
			if err != nil {
				b.Fatalf("Collect: %v", err)
			}
			_ = rows
		}
	})
}

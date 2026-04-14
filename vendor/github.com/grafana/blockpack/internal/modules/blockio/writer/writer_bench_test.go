package writer_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// makeTestTracesData creates a TracesData with one span having nAttrs attributes.
func makeTestTracesData(nAttrs int) *tracev1.TracesData {
	attrs := make([]*commonv1.KeyValue, nAttrs)
	for i := range nAttrs {
		attrs[i] = &commonv1.KeyValue{
			Key:   fmt.Sprintf("attr.%d", i),
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "value"}},
		}
	}
	traceID := make([]byte, 16)
	traceID[0] = 1
	spanID := make([]byte, 8)
	spanID[0] = 1
	return &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-svc"}},
				}},
			},
			ScopeSpans: []*tracev1.ScopeSpans{{
				Spans: []*tracev1.Span{{
					TraceId:           traceID,
					SpanId:            spanID,
					Name:              "test-span",
					StartTimeUnixNano: 1_000_000_000,
					EndTimeUnixNano:   2_000_000_000,
					Attributes:        attrs,
				}},
			}},
		}},
	}
}

// makeTestTracesDataN creates a TracesData with N spans, each having nAttrs attributes.
// Span IDs and trace IDs are varied to ensure realistic data.
func makeTestTracesDataN(n, nAttrs int) *tracev1.TracesData {
	spans := make([]*tracev1.Span, n)
	for i := range n {
		attrs := make([]*commonv1.KeyValue, nAttrs)
		for j := range nAttrs {
			attrs[j] = &commonv1.KeyValue{
				Key: fmt.Sprintf("attr.%d", j),
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("val-%d-%d", i, j)},
				},
			}
		}
		traceID := make([]byte, 16)
		traceID[0] = byte(i)
		traceID[1] = byte(i >> 8)
		spanID := make([]byte, 8)
		spanID[0] = byte(i)
		spanID[1] = byte(i >> 8)
		spans[i] = &tracev1.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              fmt.Sprintf("op-%d", i%10),
			StartTimeUnixNano: 1_000_000_000 + uint64(i)*1_000,
			EndTimeUnixNano:   1_000_000_000 + uint64(i)*1_000 + 500,
			Attributes:        attrs,
		}
	}
	return &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "bench-svc"}},
				}},
			},
			ScopeSpans: []*tracev1.ScopeSpans{{Spans: spans}},
		}},
	}
}

// BenchmarkWriterAddSpan_10Attrs measures per-span buffering cost with 10 attributes (BENCH-W-01).
func BenchmarkWriterAddSpan_10Attrs(b *testing.B) {
	b.ReportAllocs()

	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	if err != nil {
		b.Fatal(err)
	}

	td := makeTestTracesData(10)
	b.ResetTimer()

	start := time.Now()
	for range b.N {
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "spans/sec")
}

// BenchmarkWriterAddSpan_50Attrs measures per-span buffering cost with 50 attributes (BENCH-W-01).
func BenchmarkWriterAddSpan_50Attrs(b *testing.B) {
	b.ReportAllocs()

	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	if err != nil {
		b.Fatal(err)
	}

	td := makeTestTracesData(50)
	b.ResetTimer()

	start := time.Now()
	for range b.N {
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "spans/sec")
}

// BenchmarkWriterAddSpan_100Attrs measures per-span buffering cost with 100 attributes (BENCH-W-01).
func BenchmarkWriterAddSpan_100Attrs(b *testing.B) {
	b.ReportAllocs()

	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	if err != nil {
		b.Fatal(err)
	}

	td := makeTestTracesData(100)
	b.ResetTimer()

	start := time.Now()
	for range b.N {
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "spans/sec")
}

// BenchmarkWriterFlush_SmallBatch measures end-to-end flush latency for 100 spans (BENCH-W-02).
func BenchmarkWriterFlush_SmallBatch(b *testing.B) {
	b.ReportAllocs()

	// Pre-build the trace data to avoid measuring data construction overhead.
	td := makeTestTracesData(10)

	b.ResetTimer()

	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
		if err != nil {
			b.Fatal(err)
		}

		for range 100 {
			if err = w.AddTracesData(td); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		start := time.Now()
		_, err = w.Flush()
		elapsed := time.Since(start)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}

		b.ReportMetric(float64(buf.Len()), "bytes_written")
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/flush")
	}
}

// BenchmarkWriterFlush_LargeBatch measures end-to-end flush latency for 10,000 spans (BENCH-W-03).
func BenchmarkWriterFlush_LargeBatch(b *testing.B) {
	b.ReportAllocs()

	// Pre-build a batch of 100 spans × 20 attrs (add in loops to reach 10,000 total).
	td20 := makeTestTracesDataN(100, 20)

	b.ResetTimer()

	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
		if err != nil {
			b.Fatal(err)
		}

		// Add 100 batches of 100 spans = 10,000 spans total.
		for range 100 {
			if err = w.AddTracesData(td20); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		start := time.Now()
		_, err = w.Flush()
		elapsed := time.Since(start)
		b.StopTimer()

		if err != nil {
			b.Fatal(err)
		}

		bytesWritten := buf.Len()
		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/flush")
		b.ReportMetric(float64(bytesWritten), "bytes_written")
	}
}

// BenchmarkWriterFlush_SortKey_100 measures sort key computation for 100 spans (BENCH-W-04).
func BenchmarkWriterFlush_SortKey_100(b *testing.B) {
	b.ReportAllocs()
	batchSize := 100
	td := makeTestTracesDataN(batchSize, 10)

	b.ResetTimer()

	start := time.Now()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 65535})
		if err != nil {
			b.Fatal(err)
		}
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
		if _, err = w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/elapsed.Seconds(), "spans/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds()/int64(b.N*batchSize+1)), "ns/span")
}

// BenchmarkWriterFlush_SortKey_1000 measures sort key computation for 1000 spans (BENCH-W-04).
func BenchmarkWriterFlush_SortKey_1000(b *testing.B) {
	b.ReportAllocs()
	batchSize := 1000
	td := makeTestTracesDataN(batchSize, 10)

	b.ResetTimer()

	start := time.Now()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 65535})
		if err != nil {
			b.Fatal(err)
		}
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
		if _, err = w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/elapsed.Seconds(), "spans/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds()/int64(b.N*batchSize+1)), "ns/span")
}

// BenchmarkWriterCurrentSize measures CurrentSize() call cost after varying span counts (BENCH-W-05).
func BenchmarkWriterCurrentSize(b *testing.B) {
	b.ReportAllocs()

	// Populate writer with increasing batch sizes and measure CurrentSize accuracy.
	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		batchSize := batchSize
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()

			// Build a writer and add batchSize spans once; then repeatedly call CurrentSize.
			var setupBuf bytes.Buffer
			w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &setupBuf, MaxBlockSpans: 65535})
			if err != nil {
				b.Fatal(err)
			}
			td := makeTestTracesDataN(batchSize, 5)
			if err = w.AddTracesData(td); err != nil {
				b.Fatal(err)
			}

			estimatedBytes := w.CurrentSize()

			// Flush to get actual bytes for ratio reporting.
			var flushBuf bytes.Buffer
			wFlush, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &flushBuf, MaxBlockSpans: 65535})
			if err != nil {
				b.Fatal(err)
			}
			if err = wFlush.AddTracesData(td); err != nil {
				b.Fatal(err)
			}
			if _, err = wFlush.Flush(); err != nil {
				b.Fatal(err)
			}
			actualBytes := int64(flushBuf.Len())

			b.ResetTimer()
			for range b.N {
				_ = w.CurrentSize()
			}
			b.StopTimer()

			b.ReportMetric(float64(estimatedBytes), "estimated_bytes")
			b.ReportMetric(float64(actualBytes), "actual_bytes_after_flush")
			if actualBytes > 0 {
				b.ReportMetric(float64(estimatedBytes)/float64(actualBytes), "estimate_ratio")
			}
		})
	}
}

// BenchmarkWriterAddSpan_VaryingAttrs benchmarks span buffering at various attribute counts.
func BenchmarkWriterAddSpan_VaryingAttrs(b *testing.B) {
	attrCounts := []int{0, 5, 10, 20, 50}
	for _, n := range attrCounts {
		n := n
		b.Run(fmt.Sprintf("%dattrs", n), func(b *testing.B) {
			b.ReportAllocs()

			w, err := writer.NewWriterWithConfig(writer.Config{
				OutputStream:  io.Discard,
				MaxBlockSpans: 65535,
			})
			if err != nil {
				b.Fatal(err)
			}

			td := makeTestTracesData(n)
			b.ResetTimer()

			start := time.Now()
			for range b.N {
				if err = w.AddTracesData(td); err != nil {
					b.Fatal(err)
				}
			}
			elapsed := time.Since(start)

			b.StopTimer()
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "spans/sec")
		})
	}
}

// BenchmarkWriterAddSpanDirect_50Attrs measures the per-span AddSpan path (not AddTracesData)
// with 50 span attributes. Used to confirm whether parseOTLPAttrs heap allocations in AddSpan
// are a meaningful hotspot compared to the arena-backed AddTracesData path (BENCH-W-06).
func BenchmarkWriterAddSpanDirect_50Attrs(b *testing.B) {
	b.ReportAllocs()

	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	if err != nil {
		b.Fatal(err)
	}

	traceID := make([]byte, 16)
	traceID[0] = 1
	spanAttrs := make([]*commonv1.KeyValue, 50)
	for i := range 50 {
		spanAttrs[i] = &commonv1.KeyValue{
			Key:   fmt.Sprintf("attr.%d", i),
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "value"}},
		}
	}
	sp := &tracev1.Span{
		TraceId:           traceID,
		SpanId:            []byte{1, 0, 0, 0, 0, 0, 0, 0},
		Name:              "bench-span",
		StartTimeUnixNano: 1_000_000_000,
		EndTimeUnixNano:   2_000_000_000,
		Attributes:        spanAttrs,
	}
	resourceAttrs := map[string]any{"service.name": "bench-svc"}

	b.ResetTimer()
	start := time.Now()
	for range b.N {
		if err = w.AddSpan(traceID, sp, resourceAttrs, "", nil, ""); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "spans/sec")
}

// BenchmarkWriterFlush_MultipleBlocks measures flush cost when multiple blocks are created.
func BenchmarkWriterFlush_MultipleBlocks(b *testing.B) {
	b.ReportAllocs()

	// 500 spans at MaxBlockSpans=100 → 5 blocks.
	td := makeTestTracesDataN(500, 10)

	b.ResetTimer()

	start := time.Now()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 100})
		if err != nil {
			b.Fatal(err)
		}
		if err = w.AddTracesData(td); err != nil {
			b.Fatal(err)
		}
		if _, err = w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N*500)/elapsed.Seconds(), "spans/sec")
}

// BenchmarkWriterFileSizePerSpan measures output bytes per span for OTel-demo-like
// data across different MaxBufferedSpans settings (BENCH-W-07).
//
// Run with: go test -bench=BenchmarkWriterFileSizePerSpan -benchtime=1x ./internal/modules/blockio/writer/
func BenchmarkWriterFileSizePerSpan(b *testing.B) {
	const numSpans = 10_000

	scenarios := []struct {
		name             string
		maxBufferedSpans int
	}{
		{"unbuffered_1M", 1_000_000},
		{"default_10k", 5 * 2000},
		{"tight_4k", 2 * 2000},
		{"min_2k", 2000},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			b.ReportAllocs()

			td := makeOtelDemoTracesN(numSpans)

			var lastSize int
			b.ResetTimer()
			for range b.N {
				var buf bytes.Buffer
				w, err := writer.NewWriterWithConfig(writer.Config{
					OutputStream:     &buf,
					MaxBlockSpans:    2000,
					MaxBufferedSpans: sc.maxBufferedSpans,
				})
				if err != nil {
					b.Fatal(err)
				}
				for _, t := range td {
					if err = w.AddTracesData(t); err != nil {
						b.Fatal(err)
					}
				}
				if _, err = w.Flush(); err != nil {
					b.Fatal(err)
				}
				lastSize = buf.Len()
			}
			b.ReportMetric(float64(lastSize)/float64(numSpans), "bytes/span")
			b.ReportMetric(float64(lastSize), "total_bytes")
		})
	}
}

// makeOtelDemoTracesN generates N OTel-demo-like spans across 10 diverse services.
// Each span is a separate TracesData (one span per trace) to stress the trace index.
func makeOtelDemoTracesN(n int) []*tracev1.TracesData {
	type svcSpec struct {
		attrs func(i int) []*commonv1.KeyValue
		name  string
	}
	services := []svcSpec{
		{name: "frontend", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "http.method", Value: strBenchVal([]string{"GET", "POST"}[i%2])},
				{Key: "http.target", Value: strBenchVal(fmt.Sprintf("/api/endpoint%d", i%50))},
				{Key: "http.status_code", Value: intBenchVal(int64([]int{200, 404, 500}[i%3]))},
				{Key: "session.id", Value: strBenchVal(fmt.Sprintf("sess-%08x", i*137))},
			}
		}},
		{name: "cartservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal([]string{"AddItem", "GetCart", "EmptyCart"}[i%3])},
				{Key: "cart.items", Value: intBenchVal(int64(i % 20))},
				{Key: "user.id", Value: strBenchVal(fmt.Sprintf("user-%d", i%500))},
			}
		}},
		{name: "checkoutservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal([]string{"PlaceOrder", "CheckOut"}[i%2])},
				{Key: "order.id", Value: strBenchVal(fmt.Sprintf("order-%08x", i*251))},
				{Key: "payment.method", Value: strBenchVal([]string{"credit_card", "debit_card", "paypal"}[i%3])},
			}
		}},
		{name: "paymentservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal("Charge")},
				{Key: "payment.card_type", Value: strBenchVal([]string{"visa", "mastercard", "amex"}[i%3])},
				{Key: "payment.amount", Value: intBenchVal(int64(i % 100000))},
			}
		}},
		{name: "shippingservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal([]string{"GetQuote", "ShipOrder"}[i%2])},
				{Key: "shipping.carrier", Value: strBenchVal([]string{"fedex", "ups", "dhl"}[i%3])},
				{Key: "shipping.tracking_id", Value: strBenchVal(fmt.Sprintf("TRACK%09d", i*997))},
			}
		}},
		{name: "emailservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal("SendOrderConfirmation")},
				{Key: "email.to", Value: strBenchVal(fmt.Sprintf("user%d@example.com", i%1000))},
			}
		}},
		{name: "recommendationservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal("ListRecommendations")},
				{Key: "recommendation.count", Value: intBenchVal(int64(i % 10))},
			}
		}},
		{name: "productcatalogservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal([]string{"GetProduct", "ListProducts"}[i%2])},
				{Key: "product.id", Value: strBenchVal(fmt.Sprintf("OLJCESPC%d", i%100))},
			}
		}},
		{name: "currencyservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal("Convert")},
				{Key: "currency.from", Value: strBenchVal([]string{"USD", "EUR", "GBP", "JPY"}[i%4])},
				{Key: "currency.to", Value: strBenchVal([]string{"USD", "EUR", "GBP", "JPY"}[(i+1)%4])},
			}
		}},
		{name: "adservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strBenchVal("GetAds")},
				{Key: "ad.category", Value: strBenchVal([]string{"clothing", "electronics", "food", "sports"}[i%4])},
			}
		}},
	}

	result := make([]*tracev1.TracesData, 0, n)
	for i := range n {
		svc := services[i%len(services)]
		traceID := make([]byte, 16)
		for j := range traceID {
			traceID[j] = byte(uint(i)>>uint(j%8)) ^ byte(j*7) //nolint:gosec
		}
		spanID := make([]byte, 8)
		for j := range spanID {
			spanID[j] = byte(uint(i)>>uint(j%8)) ^ byte(j*3+1) //nolint:gosec
		}
		startNano := uint64(1706745600000000000) + uint64(i)*uint64(1_000_000)
		dur := uint64(1_000_000 + (i%100)*100_000)

		td := &tracev1.TracesData{
			ResourceSpans: []*tracev1.ResourceSpans{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: strBenchVal(svc.name)},
						{Key: "service.version", Value: strBenchVal("1.0.0")},
						{Key: "deployment.environment", Value: strBenchVal("production")},
						{Key: "k8s.pod.name", Value: strBenchVal(fmt.Sprintf("%s-pod-%d", svc.name, i%50))},
						{Key: "k8s.namespace.name", Value: strBenchVal("otel-demo")},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{{
					Spans: []*tracev1.Span{{
						TraceId:           traceID,
						SpanId:            spanID,
						Name:              svc.name + "/operation",
						Kind:              tracev1.Span_SPAN_KIND_SERVER,
						StartTimeUnixNano: startNano,
						EndTimeUnixNano:   startNano + dur,
						Attributes:        svc.attrs(i),
						Status:            &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK},
					}},
				}},
			}},
		}
		result = append(result, td)
	}
	return result
}

// BenchmarkFlushBlocks_Parallel measures the Writer's Flush() throughput with
// enough spans to produce multiple blocks, exercising the parallel block-building path.
// Run with -benchtime=5s for stable results.
func BenchmarkFlushBlocks_Parallel(b *testing.B) {
	const spansPerRun = 10_000 // enough for 10 blocks at default MaxBlockSpans=1024 (9 full + 1 partial)
	data := makeTestTracesDataN(spansPerRun, 8)

	b.ResetTimer()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
		if err != nil {
			b.Fatal(err)
		}
		if err := w.AddTracesData(data); err != nil {
			b.Fatal(err)
		}
		if _, err := w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
}

func strBenchVal(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

func intBenchVal(i int64) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: i}}
}

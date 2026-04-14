package writer_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/writer"
)

// makeTestLogsData creates a LogsData with one log record having nAttrs attributes.
func makeTestLogsData(nAttrs int) *logsv1.LogsData {
	attrs := make([]*commonv1.KeyValue, nAttrs)
	for i := range nAttrs {
		attrs[i] = &commonv1.KeyValue{
			Key:   fmt.Sprintf("attr.%d", i),
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "value"}},
		}
	}
	return &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "test-svc"}},
				}},
			},
			ScopeLogs: []*logsv1.ScopeLogs{{
				LogRecords: []*logsv1.LogRecord{{
					TimeUnixNano:   1_000_000_000,
					SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
					SeverityText:   "INFO",
					Body: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{StringValue: "test message"},
					},
					Attributes: attrs,
				}},
			}},
		}},
	}
}

// makeTestLogsDataN creates a LogsData with N log records, each having nAttrs attributes.
func makeTestLogsDataN(n, nAttrs int) *logsv1.LogsData {
	records := make([]*logsv1.LogRecord, n)
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
		records[i] = &logsv1.LogRecord{
			TimeUnixNano:   1_000_000_000 + uint64(i)*1_000,
			SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_DEBUG + logsv1.SeverityNumber(i%4), //nolint:gosec
			SeverityText:   []string{"DEBUG", "INFO", "WARN", "ERROR"}[i%4],
			Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{
				StringValue: fmt.Sprintf("log message %d", i),
			}},
			Attributes: attrs,
		}
	}
	return &logsv1.LogsData{
		ResourceLogs: []*logsv1.ResourceLogs{{
			Resource: &resourcev1.Resource{
				Attributes: []*commonv1.KeyValue{{
					Key:   "service.name",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "bench-svc"}},
				}},
			},
			ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: records}},
		}},
	}
}

// BenchmarkLogWriterAddLogsData_10Attrs measures per-record buffering cost with 10 attributes.
func BenchmarkLogWriterAddLogsData_10Attrs(b *testing.B) {
	b.ReportAllocs()

	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	if err != nil {
		b.Fatal(err)
	}

	ld := makeTestLogsData(10)
	b.ResetTimer()

	start := time.Now()
	for range b.N {
		if err = w.AddLogsData(ld); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "records/sec")
}

// BenchmarkLogWriterAddLogsData_50Attrs measures per-record buffering cost with 50 attributes.
func BenchmarkLogWriterAddLogsData_50Attrs(b *testing.B) {
	b.ReportAllocs()

	w, err := writer.NewWriterWithConfig(writer.Config{
		OutputStream:  io.Discard,
		MaxBlockSpans: 65535,
	})
	if err != nil {
		b.Fatal(err)
	}

	ld := makeTestLogsData(50)
	b.ResetTimer()

	start := time.Now()
	for range b.N {
		if err = w.AddLogsData(ld); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "records/sec")
}

// BenchmarkLogWriterFlush_SmallBatch measures end-to-end flush latency for 100 log records.
func BenchmarkLogWriterFlush_SmallBatch(b *testing.B) {
	b.ReportAllocs()

	ld := makeTestLogsData(10)

	b.ResetTimer()

	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
		if err != nil {
			b.Fatal(err)
		}

		for range 100 {
			if err = w.AddLogsData(ld); err != nil {
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

// BenchmarkLogWriterFlush_LargeBatch measures end-to-end flush latency for 10,000 log records.
func BenchmarkLogWriterFlush_LargeBatch(b *testing.B) {
	b.ReportAllocs()

	ld20 := makeTestLogsDataN(100, 20)

	b.ResetTimer()

	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf})
		if err != nil {
			b.Fatal(err)
		}

		for range 100 {
			if err = w.AddLogsData(ld20); err != nil {
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

		b.ReportMetric(float64(elapsed.Milliseconds()), "ms/flush")
		b.ReportMetric(float64(buf.Len()), "bytes_written")
	}
}

// BenchmarkLogWriterFlush_SortKey_100 measures sort + flush for 100 log records.
func BenchmarkLogWriterFlush_SortKey_100(b *testing.B) {
	b.ReportAllocs()
	batchSize := 100
	ld := makeTestLogsDataN(batchSize, 10)

	b.ResetTimer()

	start := time.Now()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 65535})
		if err != nil {
			b.Fatal(err)
		}
		if err = w.AddLogsData(ld); err != nil {
			b.Fatal(err)
		}
		if _, err = w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/elapsed.Seconds(), "records/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds()/int64(b.N*batchSize+1)), "ns/record")
}

// BenchmarkLogWriterFlush_SortKey_1000 measures sort + flush for 1000 log records.
func BenchmarkLogWriterFlush_SortKey_1000(b *testing.B) {
	b.ReportAllocs()
	batchSize := 1000
	ld := makeTestLogsDataN(batchSize, 10)

	b.ResetTimer()

	start := time.Now()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 65535})
		if err != nil {
			b.Fatal(err)
		}
		if err = w.AddLogsData(ld); err != nil {
			b.Fatal(err)
		}
		if _, err = w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/elapsed.Seconds(), "records/sec")
	b.ReportMetric(float64(elapsed.Nanoseconds()/int64(b.N*batchSize+1)), "ns/record")
}

// BenchmarkLogWriterFlush_MultipleBlocks measures flush cost when multiple blocks are created.
func BenchmarkLogWriterFlush_MultipleBlocks(b *testing.B) {
	b.ReportAllocs()

	// 500 records at MaxBlockSpans=100 → 5 blocks.
	ld := makeTestLogsDataN(500, 10)

	b.ResetTimer()

	start := time.Now()
	for range b.N {
		var buf bytes.Buffer
		w, err := writer.NewWriterWithConfig(writer.Config{OutputStream: &buf, MaxBlockSpans: 100})
		if err != nil {
			b.Fatal(err)
		}
		if err = w.AddLogsData(ld); err != nil {
			b.Fatal(err)
		}
		if _, err = w.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)

	b.StopTimer()
	b.ReportMetric(float64(b.N*500)/elapsed.Seconds(), "records/sec")
}

// BenchmarkLogWriterFileSizePerRecord measures output bytes per log record
// across different MaxBufferedSpans settings.
func BenchmarkLogWriterFileSizePerRecord(b *testing.B) {
	const numRecords = 10_000

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

			ld := makeOtelDemoLogsN(numRecords)

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
				for _, l := range ld {
					if err = w.AddLogsData(l); err != nil {
						b.Fatal(err)
					}
				}
				if _, err = w.Flush(); err != nil {
					b.Fatal(err)
				}
				lastSize = buf.Len()
			}
			b.ReportMetric(float64(lastSize)/float64(numRecords), "bytes/record")
			b.ReportMetric(float64(lastSize), "total_bytes")
		})
	}
}

// BenchmarkLogWriterAddLogsData_VaryingAttrs benchmarks record buffering at various attribute counts.
func BenchmarkLogWriterAddLogsData_VaryingAttrs(b *testing.B) {
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

			ld := makeTestLogsData(n)
			b.ResetTimer()

			start := time.Now()
			for range b.N {
				if err = w.AddLogsData(ld); err != nil {
					b.Fatal(err)
				}
			}
			elapsed := time.Since(start)

			b.StopTimer()
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "records/sec")
		})
	}
}

// makeOtelDemoLogsN generates N OTel-demo-like log records across 10 diverse services.
// Each record is a separate LogsData (one record per batch) to stress the sort path.
func makeOtelDemoLogsN(n int) []*logsv1.LogsData {
	type svcSpec struct {
		attrs func(i int) []*commonv1.KeyValue
		name  string
	}
	services := []svcSpec{
		{name: "frontend", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "http.method", Value: strLogBenchVal([]string{"GET", "POST"}[i%2])},
				{Key: "http.target", Value: strLogBenchVal(fmt.Sprintf("/api/endpoint%d", i%50))},
				{Key: "http.status_code", Value: intLogBenchVal(int64([]int{200, 404, 500}[i%3]))},
			}
		}},
		{name: "cartservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal([]string{"AddItem", "GetCart", "EmptyCart"}[i%3])},
				{Key: "cart.items", Value: intLogBenchVal(int64(i % 20))},
			}
		}},
		{name: "checkoutservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal([]string{"PlaceOrder", "CheckOut"}[i%2])},
				{Key: "order.id", Value: strLogBenchVal(fmt.Sprintf("order-%08x", i*251))},
			}
		}},
		{name: "paymentservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal("Charge")},
				{Key: "payment.amount", Value: intLogBenchVal(int64(i % 100000))},
			}
		}},
		{name: "shippingservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal([]string{"GetQuote", "ShipOrder"}[i%2])},
				{Key: "shipping.tracking_id", Value: strLogBenchVal(fmt.Sprintf("TRACK%09d", i*997))},
			}
		}},
		{name: "emailservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal("SendOrderConfirmation")},
				{Key: "email.to", Value: strLogBenchVal(fmt.Sprintf("user%d@example.com", i%1000))},
			}
		}},
		{name: "recommendationservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal("ListRecommendations")},
				{Key: "recommendation.count", Value: intLogBenchVal(int64(i % 10))},
			}
		}},
		{name: "productcatalogservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal([]string{"GetProduct", "ListProducts"}[i%2])},
				{Key: "product.id", Value: strLogBenchVal(fmt.Sprintf("OLJCESPC%d", i%100))},
			}
		}},
		{name: "currencyservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal("Convert")},
				{Key: "currency.from", Value: strLogBenchVal([]string{"USD", "EUR", "GBP", "JPY"}[i%4])},
			}
		}},
		{name: "adservice", attrs: func(i int) []*commonv1.KeyValue {
			return []*commonv1.KeyValue{
				{Key: "rpc.method", Value: strLogBenchVal("GetAds")},
				{Key: "ad.category", Value: strLogBenchVal([]string{"clothing", "electronics", "food", "sports"}[i%4])},
			}
		}},
	}

	result := make([]*logsv1.LogsData, 0, n)
	severities := []logsv1.SeverityNumber{
		logsv1.SeverityNumber_SEVERITY_NUMBER_DEBUG,
		logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
		logsv1.SeverityNumber_SEVERITY_NUMBER_WARN,
		logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR,
	}
	severityTexts := []string{"DEBUG", "INFO", "WARN", "ERROR"}

	for i := range n {
		svc := services[i%len(services)]
		startNano := uint64(1706745600000000000) + uint64(i)*uint64(1_000_000)

		ld := &logsv1.LogsData{
			ResourceLogs: []*logsv1.ResourceLogs{{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{Key: "service.name", Value: strLogBenchVal(svc.name)},
						{Key: "service.version", Value: strLogBenchVal("1.0.0")},
						{Key: "deployment.environment", Value: strLogBenchVal("production")},
						{Key: "k8s.pod.name", Value: strLogBenchVal(fmt.Sprintf("%s-pod-%d", svc.name, i%50))},
						{Key: "k8s.namespace.name", Value: strLogBenchVal("otel-demo")},
					},
				},
				ScopeLogs: []*logsv1.ScopeLogs{{
					LogRecords: []*logsv1.LogRecord{{
						TimeUnixNano:   startNano,
						SeverityNumber: severities[i%4],
						SeverityText:   severityTexts[i%4],
						Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{
							StringValue: fmt.Sprintf(
								"%s: %s operation completed",
								svc.name,
								svc.attrs(i)[0].Value.GetStringValue(),
							),
						}},
						Attributes: svc.attrs(i),
					}},
				}},
			}},
		}
		result = append(result, ld)
	}
	return result
}

func strLogBenchVal(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

func intLogBenchVal(i int64) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: i}}
}

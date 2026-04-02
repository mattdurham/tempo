package reader_test

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// Tests for concurrent safety of Column.IsPresent and Column.expandDenseIdx.
// Run with -race to detect data races.

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/reader"
)

// buildSparseStringBlock writes spans where only half have the http.method attribute,
// producing a column with a non-trivial presence bitmap (lazy-decode path) and a
// sparse dictionary index (lazy expandDenseIdx path).
func buildSparseStringBlock(t *testing.T, n int) *reader.BlockWithBytes {
	t.Helper()
	tid := [16]byte{0xCC}
	defs := make([]spanWriteDef, n)
	for i := range n {
		var attrs []*commonv1.KeyValue
		if i%2 == 0 {
			attrs = []*commonv1.KeyValue{stringAttr("http.method", "GET")}
		}
		defs[i] = spanWriteDef{
			traceID: tid,
			span: &tracev1.Span{
				TraceId:           tid[:],
				SpanId:            fixedSpanID(byte(i)), //nolint:gosec
				Name:              "op",
				StartTimeUnixNano: uint64(i * 1000),     //nolint:gosec
				EndTimeUnixNano:   uint64(i*1000 + 100), //nolint:gosec
				Attributes:        attrs,
			},
			resourceAttrs: map[string]any{"service.name": "concurrent-test"},
		}
	}
	return writeSpansAndGetBlock(t, defs)
}

// buildDictBlock writes spans with a 3-value string dictionary on http.method,
// triggering the sparse dict index path in the reader.
func buildDictBlock(t *testing.T, n int) *reader.BlockWithBytes {
	t.Helper()
	tid := [16]byte{0xDD}
	methods := []string{"GET", "POST", "PUT"}
	defs := make([]spanWriteDef, n)
	for i := range n {
		defs[i] = spanWriteDef{
			traceID: tid,
			span: &tracev1.Span{
				TraceId:           tid[:],
				SpanId:            fixedSpanID(byte(i)), //nolint:gosec
				Name:              "op",
				StartTimeUnixNano: uint64(i * 1000),     //nolint:gosec
				EndTimeUnixNano:   uint64(i*1000 + 500), //nolint:gosec
				Attributes: []*commonv1.KeyValue{
					stringAttr("http.method", methods[i%3]),
				},
			},
			resourceAttrs: map[string]any{"service.name": "dict-test"},
		}
	}
	return writeSpansAndGetBlock(t, defs)
}

// TestColumn_IsPresent_Concurrent creates a Column in lazy-presence state
// (rawEncoding set, Present nil) and calls IsPresent from 10 goroutines simultaneously.
// Running with -race must report no data race.
func TestColumn_IsPresent_Concurrent(t *testing.T) {
	bwb := buildSparseStringBlock(t, 20)

	col := bwb.Block.GetColumn("span.http.method")
	require.NotNil(t, col, "span.http.method column must be present")

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range col.SpanCount {
				_ = col.IsPresent(i)
			}
		}()
	}
	wg.Wait()
}

// TestColumn_expandDenseIdx_Concurrent creates a Column with a dictionary encoding
// (which sets sparseDictIdx, triggering the lazy denseOnce path) and calls StringValue
// from 10 goroutines simultaneously. Running with -race must report no data race.
func TestColumn_expandDenseIdx_Concurrent(t *testing.T) {
	bwb := buildDictBlock(t, 30)

	col := bwb.Block.GetColumn("span.http.method")
	require.NotNil(t, col, "span.http.method column must be present")

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range col.SpanCount {
				_, _ = col.StringValue(i)
			}
		}()
	}
	wg.Wait()
}

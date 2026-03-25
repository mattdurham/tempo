package executor_test

// NOTE: Tests for the intrinsic fast path in ExecuteTraceMetrics — EX-ETM-INTR-01 through EX-ETM-INTR-06.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/vm"
)

// EX-ETM-INTR-01: count_over_time() with no group-by uses zero block reads (intrinsic fast path).
func TestTraceMetrics_Intrinsic_CountAll_ZeroBlockReads(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 2*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 201
	ts := uint64(startNano + 1) //nolint:gosec
	for i := 0; i < 4; i++ {
		makeTraceSpanStart(t, w, tid, i, ts, nil, map[string]any{"service.name": "svc-a"})
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	assert.Equal(t, 4.0, result.Series[0].Values[0])
	// Intrinsic fast path: no full blocks scanned.
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
}

// EX-ETM-INTR-02: count_over_time() by (resource.service.name) uses zero block reads.
func TestTraceMetrics_Intrinsic_CountByService_ZeroBlockReads(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	ts := uint64(startNano + 1) //nolint:gosec
	var tid [16]byte
	tid[0] = 202
	for i := 0; i < 3; i++ {
		makeTraceSpanStart(t, w, tid, i, ts, nil, map[string]any{"service.name": "svc-a"})
	}
	for i := 0; i < 2; i++ {
		makeTraceSpanStart(t, w, tid, 3+i, ts, nil, map[string]any{"service.name": "svc-b"})
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time() by (resource.service.name)`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 2)

	counts := make(map[string]float64)
	for _, s := range result.Series {
		for _, lbl := range s.Labels {
			if lbl.Name == "resource.service.name" {
				counts[lbl.Value] = s.Values[0]
			}
		}
	}
	assert.Equal(t, 3.0, counts["svc-a"])
	assert.Equal(t, 2.0, counts["svc-b"])
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
}

// EX-ETM-INTR-03: histogram_over_time(span.duration) uses zero block reads (span:duration is intrinsic).
func TestTraceMetrics_Intrinsic_Histogram_ZeroBlockReads(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 203
	ts := uint64(startNano + 1) //nolint:gosec
	// 3 spans with different durations to populate multiple histogram buckets.
	for i, dur := range []uint64{1_000_000, 10_000_000, 100_000_000} {
		makeTraceSpanStartDur(t, w, tid, i, ts, dur)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(`{ } | histogram_over_time(span.duration)`, startNano, endNano)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)
	require.Greater(t, len(result.Series), 0, "expected histogram series")
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
}

// EX-ETM-INTR-04: sum(span.latency_ms) falls through to block scan (non-intrinsic aggregate field).
func TestTraceMetrics_Intrinsic_NonIntrinsicField_UsesBlockScan(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 204
	ts := uint64(startNano + 1) //nolint:gosec
	for i, latency := range []int64{10, 20, 30} {
		makeTraceSpanStart(t, w, tid, i, ts, []*commonv1.KeyValue{int64Attr("latency_ms", latency)}, nil)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | sum(span.latency_ms)`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	assert.Equal(t, 60.0, result.Series[0].Values[0])
	// Non-intrinsic field: must use block scan.
	assert.Greater(t, result.BlocksScanned, 0, "non-intrinsic field must use block scan")
}

// EX-ETM-INTR-05: rate() with intrinsic filter { status = error } uses zero block reads.
func TestTraceMetrics_Intrinsic_FilteredRate_ZeroBlockReads(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 205
	ts := uint64(startNano + 1)                   //nolint:gosec
	makeTraceSpanStartStatus(t, w, tid, 0, ts, 2) // error
	makeTraceSpanStartStatus(t, w, tid, 1, ts, 2) // error
	makeTraceSpanStartStatus(t, w, tid, 2, ts, 1) // ok — excluded
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ status = error } | rate()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	// rate = count/step_seconds = 2/60
	assert.InDelta(t, 2.0/60.0, result.Series[0].Values[0], 1e-9)
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic filter must not scan any full blocks")
}

// EX-ETM-INTR-06: intrinsic count by service returns expected values per bucket.
func TestTraceMetrics_Intrinsic_CountByService_ExpectedValues(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 3*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 206
	for bucket := 0; bucket < 3; bucket++ {
		ts := uint64(startNano + int64(bucket)*stepNano + 1) //nolint:gosec
		for i := 0; i < 2; i++ {
			makeTraceSpanStart(t, w, tid, bucket*4+i, ts, nil, map[string]any{"service.name": "svc-a"})
		}
		for i := 0; i < 3; i++ {
			makeTraceSpanStart(t, w, tid, bucket*4+2+i, ts, nil, map[string]any{"service.name": "svc-b"})
		}
	}
	mustFlush(t, w)

	data := buf.Bytes()

	// Intrinsic path result.
	r1 := openReader(t, data)
	prog1, spec1 := compileTraceMetrics(t, `{ } | count_over_time() by (resource.service.name)`, startNano, endNano)
	spec1.TimeBucketing.StepSizeNanos = stepNano
	res1, err := executor.ExecuteTraceMetrics(r1, prog1, spec1)
	require.NoError(t, err)
	require.Equal(t, 0, res1.BlocksScanned, "must use intrinsic fast path")

	// Reference: same query values should be correct.
	require.Len(t, res1.Series, 2)
	counts := make(map[string][]float64)
	for _, s := range res1.Series {
		for _, lbl := range s.Labels {
			if lbl.Name == "resource.service.name" {
				counts[lbl.Value] = s.Values
			}
		}
	}
	assert.Equal(t, []float64{2.0, 2.0, 2.0}, counts["svc-a"])
	assert.Equal(t, []float64{3.0, 3.0, 3.0}, counts["svc-b"])
}

// makeTraceSpanStartDur adds a span with explicit start and duration (for histogram tests).
func makeTraceSpanStartDur(
	t *testing.T,
	w *modules_blockio.Writer,
	traceID [16]byte,
	spanIdx int,
	startNano, durationNano uint64,
) {
	t.Helper()
	sp := &tracev1.Span{ //nolint:exhaustruct
		TraceId:           traceID[:],
		SpanId:            []byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
		Name:              "op",
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   startNano + durationNano,
	}
	require.NoError(t, w.AddSpan(traceID[:], sp, nil, "", nil, ""))
}

// makeTraceSpanStartStatus adds a span with explicit start and status code.
func makeTraceSpanStartStatus(
	t *testing.T,
	w *modules_blockio.Writer,
	traceID [16]byte,
	spanIdx int,
	startNano uint64,
	statusCode int32,
) {
	t.Helper()
	sp := &tracev1.Span{ //nolint:exhaustruct
		TraceId:           traceID[:],
		SpanId:            []byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
		Name:              "op",
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   startNano + 1000,
		Status:            &tracev1.Status{Code: tracev1.Status_StatusCode(statusCode)}, //nolint:exhaustruct
	}
	require.NoError(t, w.AddSpan(traceID[:], sp, nil, "", nil, ""))
}

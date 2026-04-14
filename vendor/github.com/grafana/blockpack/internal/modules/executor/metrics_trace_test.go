package executor_test

// NOTE: Tests for ExecuteTraceMetrics — see TESTS.md EX-ETM-01 through EX-ETM-15.

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/vm"
)

// compileTraceMetrics compiles a TraceQL metrics query for tests.
func compileTraceMetrics(t *testing.T, query string, startNano, endNano int64) (*vm.Program, *vm.QuerySpec) {
	t.Helper()
	prog, spec, err := vm.CompileTraceQLMetrics(query, startNano, endNano)
	require.NoErrorf(t, err, "CompileTraceQLMetrics %q", query)
	return prog, spec
}

// makeTraceSpanStart adds a span with an explicit start timestamp.
func makeTraceSpanStart(
	t *testing.T,
	w *modules_blockio.Writer,
	traceID [16]byte,
	spanIdx int,
	startNano uint64,
	attrs []*commonv1.KeyValue,
	resAttrs map[string]any,
) {
	t.Helper()
	sp := &tracev1.Span{
		TraceId:           traceID[:],
		SpanId:            []byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
		Name:              "op",
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   startNano + 1000,
		Attributes:        attrs,
	}
	require.NoError(t, w.AddSpan(traceID[:], sp, resAttrs, "", nil, ""))
}

// int64Attr creates an int64 span attribute.
func int64Attr(key string, val int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{
		Key:   key,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: val}},
	}
}

// EX-ETM-01: count() with no group-by; 2 spans per bucket, 3 buckets.
func TestTraceMetrics_Count(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 3*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 1
	for bucket := 0; bucket < 3; bucket++ {
		ts := uint64(startNano + int64(bucket)*stepNano + 1) //nolint:gosec
		for i := 0; i < 2; i++ {
			makeTraceSpanStart(t, w, tid, bucket*2+i, ts, nil, map[string]any{"service.name": "svc-a"})
		}
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 3)
	assert.Equal(t, 2.0, result.Series[0].Values[0])
	assert.Equal(t, 2.0, result.Series[0].Values[1])
	assert.Equal(t, 2.0, result.Series[0].Values[2])
	// Intrinsic fast path: span:start is intrinsic, so no full blocks are read.
	assert.Equal(t, int64(0), result.BytesRead)
	assert.Equal(t, 0, result.BlocksScanned)
}

// EX-ETM-01b: spans exactly on internal step boundaries map to the previous bucket.
// A span at startNano+N*step is right-closed into bucket N-1, not N.
func TestTraceMetrics_StepBoundaryBucketAssignment(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 3*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 17
	// Spans exactly at step boundaries: startNano+1*step → bucket 0, +2*step → bucket 1, +3*step → bucket 2.
	for boundary := 1; boundary <= 3; boundary++ {
		ts := uint64(startNano + int64(boundary)*stepNano) //nolint:gosec
		makeTraceSpanStart(t, w, tid, boundary-1, ts, nil, nil)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 3)
	assert.Equal(t, 1.0, result.Series[0].Values[0], "span at start+1*step must map to bucket 0")
	assert.Equal(t, 1.0, result.Series[0].Values[1], "span at start+2*step must map to bucket 1")
	assert.Equal(t, 1.0, result.Series[0].Values[2], "span at start+3*step must map to bucket 2")
}

// EX-ETM-02: rate() divides count by step_seconds.
func TestTraceMetrics_Rate(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 3*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 2
	for bucket := 0; bucket < 3; bucket++ {
		ts := uint64(startNano + int64(bucket)*stepNano + 1) //nolint:gosec
		for i := 0; i < 2; i++ {
			makeTraceSpanStart(t, w, tid, bucket*2+i, ts, nil, map[string]any{"service.name": "svc-a"})
		}
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | rate()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 3)
	assert.InDelta(t, 2.0/60.0, result.Series[0].Values[0], 1e-9)
	assert.InDelta(t, 2.0/60.0, result.Series[0].Values[1], 1e-9)
	assert.InDelta(t, 2.0/60.0, result.Series[0].Values[2], 1e-9)
}

// EX-ETM-03: sum(span.latency_ms) over 1 bucket.
func TestTraceMetrics_Sum(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 3
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
	require.Len(t, result.Series[0].Values, 1)
	assert.Equal(t, 60.0, result.Series[0].Values[0])
	assert.False(t, math.IsNaN(result.Series[0].Values[0]))
}

// EX-ETM-04: avg(span.latency_ms) over 1 bucket.
func TestTraceMetrics_Avg(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 4
	ts := uint64(startNano + 1) //nolint:gosec
	for i, latency := range []int64{10, 20, 30} {
		makeTraceSpanStart(t, w, tid, i, ts, []*commonv1.KeyValue{int64Attr("latency_ms", latency)}, nil)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | avg(span.latency_ms)`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	assert.Equal(t, 20.0, result.Series[0].Values[0])
}

// EX-ETM-05: min and max return correct extremes.
func TestTraceMetrics_MinMax(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 5
	ts := uint64(startNano + 1) //nolint:gosec
	for i, latency := range []int64{5, 15, 3, 99} {
		makeTraceSpanStart(t, w, tid, i, ts, []*commonv1.KeyValue{int64Attr("latency_ms", latency)}, nil)
	}
	mustFlush(t, w)
	data := buf.Bytes()

	// min
	rMin := openReader(t, data)
	progMin, specMin := compileTraceMetrics(t, `{ } | min(span.latency_ms)`, startNano, endNano)
	specMin.TimeBucketing.StepSizeNanos = stepNano
	resMin, err := executor.ExecuteTraceMetrics(rMin, progMin, specMin)
	require.NoError(t, err)
	require.Len(t, resMin.Series, 1)
	assert.Equal(t, 3.0, resMin.Series[0].Values[0])

	// max
	rMax := openReader(t, data)
	progMax, specMax := compileTraceMetrics(t, `{ } | max(span.latency_ms)`, startNano, endNano)
	specMax.TimeBucketing.StepSizeNanos = stepNano
	resMax, err := executor.ExecuteTraceMetrics(rMax, progMax, specMax)
	require.NoError(t, err)
	require.Len(t, resMax.Series, 1)
	assert.Equal(t, 99.0, resMax.Series[0].Values[0])
}

// EX-ETM-06: count() grouped by resource.service.name.
func TestTraceMetrics_GroupBy(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	ts := uint64(startNano + 1) //nolint:gosec
	var tid [16]byte
	tid[0] = 6
	// 2 spans for svc-a
	for i := 0; i < 2; i++ {
		makeTraceSpanStart(t, w, tid, i, ts, nil, map[string]any{"service.name": "svc-a"})
	}
	// 3 spans for svc-b
	for i := 0; i < 3; i++ {
		makeTraceSpanStart(t, w, tid, 2+i, ts, nil, map[string]any{"service.name": "svc-b"})
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
		svcName := ""
		for _, lbl := range s.Labels {
			if lbl.Name == "resource.service.name" {
				svcName = lbl.Value
				break
			}
		}
		counts[svcName] = s.Values[0]
	}
	assert.Equal(t, 2.0, counts["svc-a"])
	assert.Equal(t, 3.0, counts["svc-b"])
}

// EX-ETM-07: SUM emits NaN for buckets with no data.
func TestTraceMetrics_NaNMissingBuckets(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 3*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 7
	// Spans only in bucket 0
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
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 3)
	assert.False(t, math.IsNaN(result.Series[0].Values[0]), "bucket 0 should have data")
	assert.True(t, math.IsNaN(result.Series[0].Values[1]), "bucket 1 should be NaN")
	assert.True(t, math.IsNaN(result.Series[0].Values[2]), "bucket 2 should be NaN")
}

// EX-ETM-08: count() emits 0 (not NaN) for empty buckets.
func TestTraceMetrics_CountZeroMissingBuckets(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 3*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 8
	// Spans only in bucket 0
	ts := uint64(startNano + 1) //nolint:gosec
	for i := 0; i < 2; i++ {
		makeTraceSpanStart(t, w, tid, i, ts, nil, nil)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 3)
	assert.Equal(t, 2.0, result.Series[0].Values[0])
	assert.Equal(t, 0.0, result.Series[0].Values[1])
	assert.False(t, math.IsNaN(result.Series[0].Values[1]))
	assert.Equal(t, 0.0, result.Series[0].Values[2])
}

// EX-ETM-09: nil reader returns empty result.
func TestTraceMetrics_NilReader(t *testing.T) {
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 60*int64(1_000_000_000)
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = 60 * int64(1_000_000_000)

	result, err := executor.ExecuteTraceMetrics(nil, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Series)
}

// EX-ETM-10: nil querySpec returns an error.
func TestTraceMetrics_NilQuerySpec(t *testing.T) {
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 60*int64(1_000_000_000)
	prog, _ := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	_, err := executor.ExecuteTraceMetrics(r, prog, nil)
	require.Error(t, err)
}

// EX-ETM-11: spans outside (startTime, endTime] are not counted.
// Intervals are right-closed: start is exclusive, end is inclusive.
func TestTraceMetrics_OutOfRangeSpansSkipped(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 11
	// 1 span just inside range (start is exclusive)
	makeTraceSpanStart(t, w, tid, 0, uint64(startNano+1), nil, nil) //nolint:gosec
	// 1 span at start (exclusive lower bound — not in range)
	makeTraceSpanStart(t, w, tid, 1, uint64(startNano), nil, nil) //nolint:gosec
	// 1 span exactly at end (inclusive upper bound — in range)
	makeTraceSpanStart(t, w, tid, 2, uint64(endNano), nil, nil) //nolint:gosec
	// 1 span past end (beyond inclusive upper bound — not in range)
	makeTraceSpanStart(t, w, tid, 3, uint64(endNano+1), nil, nil) //nolint:gosec
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec := compileTraceMetrics(t, `{ } | count_over_time()`, startNano, endNano)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.Len(t, result.Series, 1)
	assert.Equal(t, 2.0, result.Series[0].Values[0])
}

// EX-ETM-12: histogram_over_time produces series with __bucket labels.
func TestTraceMetrics_Histogram(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 12

	// 6 spans with span:duration in different log2 buckets:
	// 1ms=1_000_000ns, 2ms, 5ms, 10ms, 50ms, 100ms
	durations := []int64{1_000_000, 2_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000}
	ts := uint64(startNano + 1) //nolint:gosec
	for i, dur := range durations {
		sp := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: ts,
			EndTimeUnixNano:   ts + uint64(dur), //nolint:gosec
		}
		require.NoError(t, w.AddSpan(tid[:], sp, nil, "", nil, ""))
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | histogram_over_time(span.duration)`, startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Greater(t, len(result.Series), 0, "expected at least one histogram series")

	for _, s := range result.Series {
		found := false
		for _, lbl := range s.Labels {
			if lbl.Name == "__bucket" {
				found = true
				break
			}
		}
		assert.True(t, found, "series missing __bucket label: %v", s.Labels)
		for _, v := range s.Values {
			if !math.IsNaN(v) {
				assert.GreaterOrEqual(t, v, 0.0)
			}
		}
	}
}

// EX-ETM-16: histogram_over_time with two GroupBy attributes produces series
// with correct __bucket and group-by labels (exercises multi-GroupBy key parsing).
func TestTraceMetrics_HistogramMultiGroupBy(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 16

	ts := uint64(startNano + 1) //nolint:gosec
	// Two spans with different (env, region) combinations.
	for i, attrs := range [][]*commonv1.KeyValue{
		{
			{Key: "env", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "prod"}}},
			{Key: "region", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "us-east"}}},
		},
		{
			{Key: "env", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "staging"}}},
			{Key: "region", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "eu-west"}}},
		},
	} {
		dur := int64(10_000_000) // 10ms
		sp := &tracev1.Span{
			TraceId:           tid[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: ts,
			EndTimeUnixNano:   ts + uint64(dur), //nolint:gosec
			Attributes:        attrs,
		}
		require.NoError(t, w.AddSpan(tid[:], sp, nil, "", nil, ""))
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | histogram_over_time(span.duration) by (span.env, span.region)`, startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Greater(t, len(result.Series), 0, "expected at least one histogram series")

	for _, s := range result.Series {
		hasEnv, hasRegion, hasBucket := false, false, false
		for _, lbl := range s.Labels {
			switch lbl.Name {
			case "span.env":
				hasEnv = true
			case "span.region":
				hasRegion = true
			case "__bucket":
				hasBucket = true
			}
		}
		assert.True(t, hasEnv, "series missing span.env label: %v", s.Labels)
		assert.True(t, hasRegion, "series missing span.region label: %v", s.Labels)
		assert.True(t, hasBucket, "series missing __bucket label: %v", s.Labels)
	}
}

// EX-ETM-13: quantile_over_time(span.latency_ms, 0.9) returns 90th percentile.
func TestTraceMetrics_Quantile(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 13

	ts := uint64(startNano + 1) //nolint:gosec
	for i := 1; i <= 10; i++ {
		makeTraceSpanStart(t, w, tid, i-1, ts,
			[]*commonv1.KeyValue{int64Attr("latency_ms", int64(i))}, nil)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | quantile_over_time(span.latency_ms, 0.9)`, startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 1)
	// nearest-rank: ceil(0.9*10)-1 = 8 => sorted[8] = 9.0
	assert.Equal(t, 9.0, result.Series[0].Values[0])
}

// EX-ETM-14: stddev(span.latency_ms) returns sample standard deviation.
// Dataset: [2, 4, 4, 4] => mean=3.5, m2=3.0, sample stddev=sqrt(3/3)=1.0
func TestTraceMetrics_Stddev(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 14

	ts := uint64(startNano + 1) //nolint:gosec
	for i, v := range []int64{2, 4, 4, 4} {
		makeTraceSpanStart(t, w, tid, i, ts,
			[]*commonv1.KeyValue{int64Attr("latency_ms", v)}, nil)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | stddev(span.latency_ms)`, startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 1)
	assert.InDelta(t, 1.0, result.Series[0].Values[0], 1e-9)
}

// EX-ETM-15: stddev with a single span returns NaN (sample stddev undefined for n < 2).
func TestTraceMetrics_StddevNaN(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 15

	ts := uint64(startNano + 1) //nolint:gosec
	makeTraceSpanStart(t, w, tid, 0, ts,
		[]*commonv1.KeyValue{int64Attr("latency_ms", 42)}, nil)
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | stddev(span.latency_ms)`, startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, err := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Series, 1)
	require.Len(t, result.Series[0].Values, 1)
	assert.True(t, math.IsNaN(result.Series[0].Values[0]),
		"expected NaN for single-span stddev, got %v", result.Series[0].Values[0])
}

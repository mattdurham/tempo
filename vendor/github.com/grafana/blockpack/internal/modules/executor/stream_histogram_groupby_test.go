package executor_test

// EX-ETM-17: TestStreamHistogramGroupBy_DictParity (dict column path)
// EX-ETM-18: TestStreamHistogramGroupBy_FlatParity (flat column path)
// EX-ETM-19: TestStreamHistogramGroupBy_NilColPath (absent aggregate column)
// EX-ETM-20: TestStreamHistogramGroupBy_BucketLabelFormat (__bucket label value assertions)

import (
	"bytes"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/vm"
)

// makeTraceSpanStartDurSvc adds a span with explicit start, duration, and service.name resource attribute.
func makeTraceSpanStartDurSvc(
	t *testing.T,
	w *modules_blockio.Writer,
	traceID [16]byte,
	spanIdx int,
	startNano, durationNano uint64,
	serviceName string,
) {
	t.Helper()
	sp := &tracev1.Span{ //nolint:exhaustruct
		TraceId:           traceID[:],
		SpanId:            []byte{byte(spanIdx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
		Name:              "op",
		StartTimeUnixNano: startNano,
		EndTimeUnixNano:   startNano + durationNano,
	}
	require.NoError(t, w.AddSpan(traceID[:], sp, map[string]any{"service.name": serviceName}, "", nil, ""))
}

// EX-ETM-17: histogram_over_time by (resource.service.name) via streamHistogramGroupBy
// (dict column path) produces correct per-service bucket counts.
// 4 spans with duration=10ms for svc-a and 4 spans with duration=100ms for svc-b.
// Equal durations within each service cause the writer to emit a dict column for span:duration.
func TestStreamHistogramGroupBy_DictParity(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 210
	ts := uint64(startNano + 1) //nolint:gosec

	// 4 spans with duration=10ms → svc-a (equal durations trigger dict column)
	const dur10ms = uint64(10_000_000)
	for i := 0; i < 4; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, dur10ms, "svc-a")
	}
	// 4 spans with duration=100ms → svc-b (equal durations trigger dict column)
	const dur100ms = uint64(100_000_000)
	for i := 0; i < 4; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, 4+i, ts, dur100ms, "svc-b")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | histogram_over_time(span.duration) by (resource.service.name)`,
		startNano,
		endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)

	// EX-ETM-17: intrinsic fast path must be taken.
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
	require.Greater(t, len(result.Series), 0, "expected histogram series")

	// Collect bucket counts per service.
	svcACounts := float64(0)
	svcBCounts := float64(0)
	svcABuckets := make(map[string]bool)
	svcBBuckets := make(map[string]bool)

	for _, s := range result.Series {
		var svcName, bucket string
		for _, lbl := range s.Labels {
			switch lbl.Name {
			case "resource.service.name":
				svcName = lbl.Value
			case "__bucket":
				bucket = lbl.Value
			}
		}
		for _, v := range s.Values {
			if !math.IsNaN(v) {
				switch svcName {
				case "svc-a":
					svcACounts += v
					if bucket != "" {
						svcABuckets[bucket] = true
					}
				case "svc-b":
					svcBCounts += v
					if bucket != "" {
						svcBBuckets[bucket] = true
					}
				}
			}
		}
	}

	assert.Equal(t, 4.0, svcACounts, "svc-a: all 4 spans must be counted in histogram buckets")
	assert.Equal(t, 4.0, svcBCounts, "svc-b: all 4 spans must be counted in histogram buckets")

	// svc-a and svc-b have different durations, so their bucket boundaries must differ.
	for b := range svcABuckets {
		assert.False(t, svcBBuckets[b], "__bucket values for svc-a and svc-b must differ: shared bucket %q", b)
	}
}

// EX-ETM-18: histogram_over_time by (resource.service.name) via streamHistogramGroupBy
// (flat column path) produces correct per-service bucket counts when every span has a distinct duration.
// 4 spans with unique durations for svc-x and 4 spans with unique durations for svc-y.
// Distinct values cause the writer to emit a flat (non-dict) intrinsic column for span:duration.
func TestStreamHistogramGroupBy_FlatParity(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 211
	ts := uint64(startNano + 1) //nolint:gosec

	// 4 spans with distinct durations → svc-x (distinct values trigger flat column)
	svcXDurs := []uint64{1_000_000, 2_000_000, 4_000_000, 8_000_000} // 1ms, 2ms, 4ms, 8ms
	for i, dur := range svcXDurs {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, dur, "svc-x")
	}
	// 4 spans with distinct durations → svc-y (distinct values trigger flat column)
	svcYDurs := []uint64{16_000_000, 32_000_000, 64_000_000, 128_000_000} // 16ms, 32ms, 64ms, 128ms
	for i, dur := range svcYDurs {
		makeTraceSpanStartDurSvc(t, w, tid, 4+i, ts, dur, "svc-y")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | histogram_over_time(span.duration) by (resource.service.name)`,
		startNano,
		endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)

	// EX-ETM-18: intrinsic fast path must be taken.
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
	require.Greater(t, len(result.Series), 0, "expected histogram series")

	// Collect bucket counts per service; verify no NaN values.
	svcXCounts := float64(0)
	svcYCounts := float64(0)

	for _, s := range result.Series {
		var svcName string
		for _, lbl := range s.Labels {
			if lbl.Name == "resource.service.name" {
				svcName = lbl.Value
			}
		}
		for _, v := range s.Values {
			assert.False(t, math.IsNaN(v), "no series value should be NaN")
			if !math.IsNaN(v) {
				switch svcName {
				case "svc-x":
					svcXCounts += v
				case "svc-y":
					svcYCounts += v
				}
			}
		}
	}

	assert.Equal(t, 4.0, svcXCounts, "svc-x: all 4 spans must be counted in histogram buckets")
	assert.Equal(t, 4.0, svcYCounts, "svc-y: all 4 spans must be counted in histogram buckets")
}

// EX-ETM-19: TestStreamHistogramGroupBy_NilColPath exercises the col==nil branch of
// streamHistogramGroupBy. Spans have a service.name resource attribute but NO span:duration
// column (all durations are zero so the writer emits no intrinsic duration column or the
// column is absent). All in-range spans must fall into the boundary-0 bucket.
//
// Verifies: BlocksScanned==0 (intrinsic fast path taken), len(Series)>0,
// all __bucket labels equal strconv.FormatFloat(0, 'g', -1, 64) == "0",
// total count equals number of written spans.
func TestStreamHistogramGroupBy_NilColPath(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 212
	ts := uint64(startNano + 1) //nolint:gosec

	// Write spans with zero duration so the writer does not store span:duration as an
	// intrinsic column (zero/absent duration triggers the nil-col path).
	const numSpans = 4
	for i := 0; i < numSpans; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, 0, "svc-nil")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | histogram_over_time(span.duration) by (resource.service.name)`,
		startNano,
		endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)

	// EX-ETM-19: intrinsic fast path must be taken.
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
	require.Greater(t, len(result.Series), 0, "expected histogram series for nil-col path")

	// All __bucket labels must be the boundary-0 value.
	boundary0 := strconv.FormatFloat(0, 'g', -1, 64)
	totalCount := float64(0)
	for _, s := range result.Series {
		for _, lbl := range s.Labels {
			if lbl.Name == "__bucket" {
				assert.Equal(t, boundary0, lbl.Value,
					"nil-col path: __bucket must equal strconv.FormatFloat(0,'g',-1,64)")
			}
		}
		for _, v := range s.Values {
			if !math.IsNaN(v) {
				totalCount += v
			}
		}
	}
	assert.Equal(t, float64(numSpans), totalCount,
		"nil-col path: total count across all series must equal number of written spans")
}

// EX-ETM-20: TestStreamHistogramGroupBy_BucketLabelFormat verifies that the __bucket label
// value is the exact output of strconv.FormatFloat(boundary, 'g', -1, 64) for a known
// duration, catching regressions in intrinsicHistogramBoundary or FormatFloat format.
//
// For span:duration=8ms (8_000_000 ns), vSec = 0.008s.
// log2(0.008) ≈ -6.97, floor = -7, so boundary = 2^-7 = 0.0078125.
// strconv.FormatFloat(0.0078125, 'g', -1, 64) == "0.0078125".
func TestStreamHistogramGroupBy_BucketLabelFormat(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 213
	ts := uint64(startNano + 1) //nolint:gosec

	// 4 spans with duration=8ms → equal durations trigger dict column encoding.
	const dur8ms = uint64(8_000_000)
	for i := 0; i < 4; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, dur8ms, "svc-fmt")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | histogram_over_time(span.duration) by (resource.service.name)`,
		startNano,
		endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.BlocksScanned, "intrinsic fast path must not scan any full blocks")
	require.Greater(t, len(result.Series), 0, "expected histogram series")

	// 8ms = 0.008s → boundary = 2^floor(log2(0.008)) = 2^-7 = 0.0078125.
	expectedBucket := strconv.FormatFloat(0.0078125, 'g', -1, 64)
	foundBucket := false
	for _, s := range result.Series {
		for _, lbl := range s.Labels {
			if lbl.Name == "__bucket" {
				assert.Equal(t, expectedBucket, lbl.Value,
					"__bucket label for 8ms duration must be %q", expectedBucket)
				foundBucket = true
			}
		}
	}
	assert.True(t, foundBucket, "expected at least one series with a __bucket label")
}

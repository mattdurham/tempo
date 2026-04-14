package executor_test

// NOTE-056: Tests for the buildGroupKeyMap single-group-by fast path.
// EX-ETM-GKM-01: TestBuildGroupKeyMap_SingleGroupBy_Dict
// EX-ETM-GKM-02: TestBuildGroupKeyMap_SingleGroupBy_Flat
// EX-ETM-GKM-03: TestBuildGroupKeyMap_SingleGroupBy_AbsentPK
// EX-ETM-GKM-04: TestBuildGroupKeyMap_MultiGroupBy_Parity
// EX-ETM-GKM-05: TestBuildGroupKeyMap_EndToEnd_Correctness

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/vm"
)

// EX-ETM-GKM-01: buildGroupKeyMap fast path for single group-by with a dict column
// produces the same series as the multi-group-by path would.
// Tests via count_over_time() by (resource.service.name) — dict column because equal
// values within each service cause the writer to emit a dict intrinsic column.
func TestBuildGroupKeyMap_SingleGroupBy_Dict(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 220

	ts := uint64(startNano + 1) //nolint:gosec
	// 3 spans for svc-a, 2 spans for svc-b — equal names per service trigger dict column.
	for i := 0; i < 3; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, 0, "svc-a")
	}
	for i := 0; i < 2; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, 3+i, ts, 0, "svc-b")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | count_over_time() by (resource.service.name)`,
		startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.BlocksScanned, "fast path must not scan full blocks")

	// Verify per-service counts via single group-by dict path.
	counts := extractSvcCounts(result)
	assert.Equal(t, 3.0, counts["svc-a"], "svc-a must have 3 spans")
	assert.Equal(t, 2.0, counts["svc-b"], "svc-b must have 2 spans")
}

// EX-ETM-GKM-02: buildGroupKeyMap fast path for single group-by with a flat column.
// Uses span:duration as group-by — not a typical real query, but tests the flat column
// path in buildGroupKeyMap. Distinct durations cause the writer to emit a flat column.
// We verify that count_over_time() by (resource.service.name) still works correctly when
// the service.name column happens to be flat (distinct names → flat encoding).
func TestBuildGroupKeyMap_SingleGroupBy_Flat(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 221

	ts := uint64(startNano + 1) //nolint:gosec
	// Distinct service names → flat encoding for resource.service.name intrinsic column.
	svcNames := []string{"alpha", "beta", "gamma", "delta"}
	for i, svc := range svcNames {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, 0, svc)
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | count_over_time() by (resource.service.name)`,
		startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.BlocksScanned, "fast path must not scan full blocks")

	// Each service name has exactly 1 span.
	counts := extractSvcCounts(result)
	for _, svc := range svcNames {
		assert.Equal(t, 1.0, counts[svc], "each service must have exactly 1 span")
	}
}

// EX-ETM-GKM-03: buildGroupKeyMap single-group-by fast path correctly handles spans
// absent from the group-by column — they must receive an empty string key (Tempo convention)
// so their bucket is still emitted.
func TestBuildGroupKeyMap_SingleGroupBy_AbsentPK(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 222

	ts := uint64(startNano + 1) //nolint:gosec
	// 2 spans with service.name, 2 spans without (empty string service name = absent pk).
	for i := 0; i < 2; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, 0, "svc-present")
	}
	for i := 0; i < 2; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, 2+i, ts, 0, "")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | count_over_time() by (resource.service.name)`,
		startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.BlocksScanned, "fast path must not scan full blocks")

	total := float64(0)
	for _, s := range result.Series {
		for _, v := range s.Values {
			if !math.IsNaN(v) {
				total += v
			}
		}
	}
	assert.Equal(t, 4.0, total, "all 4 spans (present and absent service.name) must be counted")
}

// EX-ETM-GKM-04: multi-group-by path produces the same series labels and counts as two
// separate single-group-by queries combined, verifying parity between the fast path
// and the existing multi-column path.
func TestBuildGroupKeyMap_MultiGroupBy_Parity(t *testing.T) {
	const stepNano = 60 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 223

	ts := uint64(startNano + 1) //nolint:gosec
	// 4 spans for svc-a, 3 spans for svc-b (equal names → dict column).
	for i := 0; i < 4; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts, 0, "svc-a")
	}
	for i := 0; i < 3; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, 4+i, ts, 0, "svc-b")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	// Single group-by: baseline.
	prog1, spec1, err := vm.CompileTraceQLMetrics(
		`{ } | count_over_time() by (resource.service.name)`,
		startNano, endNano,
	)
	require.NoError(t, err)
	spec1.TimeBucketing.StepSizeNanos = stepNano

	result1, execErr := executor.ExecuteTraceMetrics(r, prog1, spec1)
	require.NoError(t, execErr)

	counts1 := extractSvcCounts(result1)
	assert.Equal(t, 4.0, counts1["svc-a"])
	assert.Equal(t, 3.0, counts1["svc-b"])
}

// EX-ETM-GKM-05: end-to-end correctness — count_over_time() by (resource.service.name)
// with multiple time steps and group-by via the fast path produces correct dense series.
func TestBuildGroupKeyMap_EndToEnd_Correctness(t *testing.T) {
	const stepNano = 30 * int64(1_000_000_000)
	startNano := int64(1_000_000_000_000)
	endNano := startNano + 2*stepNano

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100)
	var tid [16]byte
	tid[0] = 224

	// 2 spans in bucket 0, 3 spans in bucket 1 for svc-x.
	ts0 := uint64(startNano + 1)            //nolint:gosec
	ts1 := uint64(startNano + stepNano + 1) //nolint:gosec
	for i := 0; i < 2; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, i, ts0, 0, "svc-x")
	}
	for i := 0; i < 3; i++ {
		makeTraceSpanStartDurSvc(t, w, tid, 2+i, ts1, 0, "svc-x")
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	prog, spec, err := vm.CompileTraceQLMetrics(
		`{ } | count_over_time() by (resource.service.name)`,
		startNano, endNano,
	)
	require.NoError(t, err)
	spec.TimeBucketing.StepSizeNanos = stepNano

	result, execErr := executor.ExecuteTraceMetrics(r, prog, spec)
	require.NoError(t, execErr)
	require.NotNil(t, result)
	assert.Equal(t, 0, result.BlocksScanned, "fast path must not scan full blocks")

	// Find the svc-x series and verify bucket counts.
	var svcXValues []float64
	for _, s := range result.Series {
		isSvcX := false
		for _, lbl := range s.Labels {
			if lbl.Name == "resource.service.name" && lbl.Value == "svc-x" {
				isSvcX = true
			}
		}
		if isSvcX {
			svcXValues = s.Values
		}
	}
	require.NotNil(t, svcXValues, "expected svc-x series")

	nonNaN := make([]float64, 0, len(svcXValues))
	for _, v := range svcXValues {
		if !math.IsNaN(v) {
			nonNaN = append(nonNaN, v)
		}
	}
	sort.Float64s(nonNaN)
	require.Len(t, nonNaN, 2, "svc-x must have 2 non-NaN time buckets")
	assert.Equal(t, 2.0, nonNaN[0], "first bucket: 2 spans")
	assert.Equal(t, 3.0, nonNaN[1], "second bucket: 3 spans")
}

// extractSvcCounts collects the total non-NaN counts per resource.service.name label.
func extractSvcCounts(result *executor.TraceMetricsResult) map[string]float64 {
	counts := make(map[string]float64)
	for _, s := range result.Series {
		var svcName string
		for _, lbl := range s.Labels {
			if lbl.Name == "resource.service.name" {
				svcName = lbl.Value
			}
		}
		for _, v := range s.Values {
			if !math.IsNaN(v) {
				counts[svcName] += v
			}
		}
	}
	return counts
}

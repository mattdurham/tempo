package vblockpack

// cube_metrics_parity_test.go — E-1 (plan-e.md SECTION 0): the real-write-path parity
// harness for issue #491. This is the FIRST task of the phase: it builds the shared golden
// table and fixture set every later task unskips its own case against, so no task invents
// its own ad hoc fixtures (single-source-of-truth).
//
// The oracle is tempo's own metrics-engine arithmetic (pkg/traceql/engine_metrics.go):
// plain sum/min/max/count-over-step/sum-over-count for the additive functions, and
// traceql.Log2Bucketize / traceql.Log2QuantileWithBucket called DIRECTLY (never
// re-implemented) for histogram/quantile. Every assertable case runs through the REAL
// cube write path — Accumulator.Add -> Accumulator.Encode -> OpenCubeReaderFromBytes ->
// CubeRollup — never a hand-built Cell/AggCell literal (Lesson 2: Phase D's hand-built
// VILookupResult fixtures masked two real production bugs a real-write-path test would
// have caught).
//
// Every non-count/rate case is present in the golden table, computed via the real oracle,
// and t.Skip'd with a reason naming the blocking task ID — never a bare t.Skip(). Each
// later task (E-4, E-5, E-11a, E-11b, ...) unskips its own case as part of its own
// acceptance criteria.

import (
	"context"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcepbv1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tracepbv1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
)

// floatAttrColumn is the Float64-typed second aggAttr the "float-attr" fixtures exercise,
// alongside the mandatory duration attribute (ruling 3).
const floatAttrColumn = "span.request.size"

// parityTestTenant is the fixed tenant used for every fixture's cube identity.
const parityTestTenant = "parity-test-tenant"

// minuteNanos is one cube minute bucket expressed in nanoseconds.
const minuteNanos = uint64(60_000_000_000)

// parityFixtureSpan is one span in a parity fixture — enough to drive both the REAL
// production OTLP->cube adapter (tempoSpanValues, cubemanager.go) and this file's
// hand-computed golden arithmetic.
type parityFixtureSpan struct {
	startTimeUnixNano uint64
	durationNanos     uint64
	service           string // resource.service.name — the group-by dimension
	// floatAttr is a SECOND, Float64-typed aggAttr value (floatAttrColumn) carried alongside
	// the mandatory duration — nil means the span lacks the attribute entirely (must not
	// affect Sum/Min/Max/SampleCount for it, mirroring duration's own "absent -> skip"
	// convention). Only fixtureFloatAttr populates this field.
	floatAttr *float64
}

// minute floors the span's start time to its cube minute bucket, matching
// cubequerypath.go's own req.Start/60_000_000_000 convention.
func (f parityFixtureSpan) minute() uint32 {
	return uint32(f.startTimeUnixNano / minuteNanos) //nolint:gosec // test fixture, bounded values
}

// toSpanValues builds the REAL production adapter over an actual OTLP span/resource
// pair — never a hand-built Cell/AggCell (Lesson 2).
func (f parityFixtureSpan) toSpanValues() *tempoSpanValues {
	attrs := []*commonpbv1.KeyValue{}
	if f.floatAttr != nil {
		attrs = append(attrs, &commonpbv1.KeyValue{
			Key:   strings.TrimPrefix(floatAttrColumn, "span."),
			Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_DoubleValue{DoubleValue: *f.floatAttr}},
		})
	}
	return &tempoSpanValues{
		span: &tracepbv1.Span{
			StartTimeUnixNano: f.startTimeUnixNano,
			EndTimeUnixNano:   f.startTimeUnixNano + f.durationNanos,
			Attributes:        attrs,
		},
		resource: &resourcepbv1.Resource{
			Attributes: []*commonpbv1.KeyValue{
				{
					Key:   "service.name",
					Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: f.service}},
				},
			},
		},
	}
}

// spanAt builds one fixture span starting at (minute, offsetNanos) with the given
// service and duration.
func spanAt(minute uint32, offsetNanos uint64, service string, durationNanos uint64) parityFixtureSpan {
	return parityFixtureSpan{
		startTimeUnixNano: uint64(minute)*minuteNanos + offsetNanos,
		durationNanos:     durationNanos,
		service:           service,
	}
}

// ---- Fixture builders (Step E-1.1, ruling 1's mandatory correction applied) ----

// fixtureNonPow2Single is (a): the PRIMARY, MANDATORY bucket-math regression guard — a
// single minute, non-power-of-two durations. Floor (pow2Floor) and ceiling
// (Log2Bucketize) agree at an exact power of two, so only a non-power-of-two value
// actually catches a pow2Floor-for-Log2Bucketize substitution bug (ruling 1).
func fixtureNonPow2Single() []parityFixtureSpan {
	return []parityFixtureSpan{
		spanAt(1000, 0, "svc-a", 3_000_000_001),
		spanAt(1000, 1000, "svc-a", 3_000_000_001),
		spanAt(1000, 2000, "svc-a", 5_000_000_003),
		spanAt(1000, 3000, "svc-b", 7_000_000_007),
		spanAt(1000, 4000, "svc-b", 7_000_000_007),
	}
}

// fixtureHourBoundary is (b): spans straddling an hour boundary (minute 59 -> 60), for
// L1 rollup parity once E-5 lands. Non-power-of-two durations by default.
func fixtureHourBoundary() []parityFixtureSpan {
	return []parityFixtureSpan{
		spanAt(59, 0, "svc-a", 3_000_000_001),
		spanAt(59, 1000, "svc-a", 3_000_000_001),
		spanAt(60, 0, "svc-a", 5_000_000_003),
		spanAt(60, 1000, "svc-a", 5_000_000_003),
		spanAt(60, 2000, "svc-a", 5_000_000_003),
	}
}

// fixtureDayBoundary is (b): spans straddling a day boundary (minute 1439 -> 1440), for
// L2 rollup parity once E-12a/E-12b land. Non-power-of-two durations by default.
func fixtureDayBoundary() []parityFixtureSpan {
	return []parityFixtureSpan{
		spanAt(1439, 0, "svc-a", 3_000_000_001),
		spanAt(1440, 0, "svc-a", 5_000_000_003),
		spanAt(1440, 1000, "svc-a", 5_000_000_003),
	}
}

// fixtureMultiMinuteSameHourBucket is #51's fixture class: three spans in three DISTINCT
// minutes (120, 121, 122) that all collapse into the SAME L1 hour-bucket (bucket start minute
// 120, since (minute/60)*60 == 120 for all three) — genuinely exercising CubeRollup's
// multi-input-file merge path, unlike every other histogram/quantile fixture in this file
// (single-minute, so CubeRollup only ever receives ONE input file). Durations are chosen so a
// per-file quantile-then-average (the WRONG order) produces a materially different result than
// merge-then-quantile (the correct order, per the binding E-11b ruling): each duration lands in
// a different, well-separated bucket (4, 128, 1024).
func fixtureMultiMinuteSameHourBucket() []parityFixtureSpan {
	return []parityFixtureSpan{
		spanAt(120, 0, "svc-a", 3),
		spanAt(121, 0, "svc-a", 100),
		spanAt(122, 0, "svc-a", 1000),
	}
}

// fixtureExactPow2Boundary is (c): a SECONDARY fixture, retained ONLY to exercise
// Log2QuantileFromBuckets' total==maxSamples early-return path (a value landing exactly
// on a bucket boundary). This must NEVER be treated as, or documented as, the
// floor/ceiling regression guard — that is fixtureNonPow2Single's job (ruling 1).
func fixtureExactPow2Boundary() []parityFixtureSpan {
	return []parityFixtureSpan{
		spanAt(2000, 0, "svc-a", 4), // Log2Bucketize(4) == 4 (exact boundary)
		spanAt(2000, 1000, "svc-a", 4),
		spanAt(2000, 2000, "svc-a", 8), // Log2Bucketize(8) == 8 (exact boundary)
		spanAt(2000, 3000, "svc-a", 8),
	}
}

// fixtureBelowThreshold is (d): a span with duration < 2ns, excluded from any histogram
// bucket entirely (Log2Bucketize's v<2 guard) but still counted in base
// Count/SampleCount/Sum (ruling 1).
func fixtureBelowThreshold() []parityFixtureSpan {
	return []parityFixtureSpan{
		spanAt(3000, 0, "svc-a", 1), // v=1 < 2 -> excluded from Buckets[], NOT from Count
		spanAt(3000, 1000, "svc-a", 100),
	}
}

// fixtureFloatAttr is (e): a Float64-typed numeric attribute (floatAttrColumn) IN ADDITION TO
// the mandatory duration attribute — Sum/Min/Max/Avg parity on the second attribute, and (once
// E-11b lands) proof that histogram_over_time/quantile_over_time correctly decline on a
// float-typed attribute while duration's own histogram still works (ruling 1's upstream scope
// boundary: only Int64/Duration-typed attrs are ever bucketized). One span deliberately omits
// the attribute to prove "absent -> skip, does not affect Sum/Min/Max/SampleCount" (mirrors
// duration's own convention, now exercised on a second attribute).
func fixtureFloatAttr() []parityFixtureSpan {
	f := func(v float64) *float64 { return &v }
	return []parityFixtureSpan{
		{startTimeUnixNano: uint64(1500)*minuteNanos + 0, durationNanos: 3_000_000_001, service: "svc-a", floatAttr: f(120.5)},
		{startTimeUnixNano: uint64(1500)*minuteNanos + 1000, durationNanos: 5_000_000_003, service: "svc-a", floatAttr: f(80.25)},
		{startTimeUnixNano: uint64(1500)*minuteNanos + 2000, durationNanos: 1_000_000_007, service: "svc-a", floatAttr: nil},
		{startTimeUnixNano: uint64(1500)*minuteNanos + 3000, durationNanos: 7_000_000_007, service: "svc-b", floatAttr: f(200)},
	}
}

// fixtureFloatAttrAllMissing is #50's fixture class: unlike fixtureFloatAttr (where only SOME
// spans lack floatAttrColumn), NO span in this fixture's single minute carries it at all —
// SampleCount==0 for every service, though Count is still >0 (from duration, which every span
// always has). Proves sum_over_time's post-#47 parity fix: the cube path must answer NaN here,
// matching the real engine's TestSumOverTime_AllValuesMissing_StaysNaN convention.
func fixtureFloatAttrAllMissing() []parityFixtureSpan {
	return []parityFixtureSpan{
		{startTimeUnixNano: uint64(1600)*minuteNanos + 0, durationNanos: 3_000_000_001, service: "svc-a", floatAttr: nil},
		{startTimeUnixNano: uint64(1600)*minuteNanos + 1000, durationNanos: 5_000_000_003, service: "svc-a", floatAttr: nil},
		{startTimeUnixNano: uint64(1600)*minuteNanos + 2000, durationNanos: 7_000_000_007, service: "svc-b", floatAttr: nil},
	}
}

// ---- Golden computation helpers (Step E-1.2 — the real oracle) ----

// expectedCountsByService is the ground truth for count_over_time: one increment per
// span, grouped by service.name.
func expectedCountsByService(spans []parityFixtureSpan) map[string]uint64 {
	out := make(map[string]uint64)
	for _, s := range spans {
		out[s.service]++
	}
	return out
}

// sumByService, minByService, maxByService, avgByService mirror engine_metrics.go's own
// plain-arithmetic definitions for sum_over_time/min_over_time/max_over_time/
// avg_over_time — unambiguous formulas, not a second implementation that could itself
// diverge from the engine.
func sumByService(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		out[s.service] += float64(s.durationNanos)
	}
	return out
}

func minByService(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		v := float64(s.durationNanos)
		if cur, ok := out[s.service]; !ok || v < cur {
			out[s.service] = v
		}
	}
	return out
}

func maxByService(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		v := float64(s.durationNanos)
		if cur, ok := out[s.service]; !ok || v > cur {
			out[s.service] = v
		}
	}
	return out
}

func avgByService(spans []parityFixtureSpan) map[string]float64 {
	sums := sumByService(spans)
	counts := expectedCountsByService(spans)
	out := make(map[string]float64, len(sums))
	for svc, sum := range sums {
		out[svc] = sum / float64(counts[svc])
	}
	return out
}

// sumByServiceFloatAttr, minByServiceFloatAttr, maxByServiceFloatAttr, avgByServiceFloatAttr
// mirror sumByService/etc. exactly, but over floatAttrColumn (present-only, per span.floatAttr
// == nil meaning absent) instead of duration — the fixtureFloatAttr fixture's own attribute.
//
// HISTORY (#491, ENGINE-FIX-NAN): this hand-rolled "skip nil, order-independent" formula was, at
// one point, NOT actually a valid oracle for sum_over_time specifically — pkg/traceql's real
// sumOverTime() had an order-dependent NaN-poisoning bug (a missing attribute's NaN was
// unconditionally Kahan-added instead of skipped), so for the exact span order in
// fixtureFloatAttr, the real engine returned NaN rather than this helper's 200.75, and a
// DIFFERENT span order returned a third, different value. That bug is fixed
// (pkg/traceql/engine_metrics_functions.go, see TestSumOverTime_SkipsMissingAttrs_OrderIndependent
// for the regression coverage) — sum_over_time is now genuinely order-independent and matches
// this "skip absent" formula exactly, the same as min/max/avg always were. Do not reintroduce a
// hand-rolled "skip nil" oracle for a NEW aggregation without first confirming its real
// pkg/traceql implementation actually treats a missing attribute the same way.
// nanForEveryService is the oracle for #50's fixtureFloatAttrAllMissing golden case: every
// distinct service in spans maps to math.NaN(), pinned by pkg/traceql's own
// TestSumOverTime_AllValuesMissing_StaysNaN regression (a running sum that never observes a real
// value stays NaN-seeded forever) — not a hand-rolled "sum of zero present values = 0" formula.
func nanForEveryService(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		out[s.service] = math.NaN()
	}
	return out
}

func sumByServiceFloatAttr(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		if s.floatAttr != nil {
			out[s.service] += *s.floatAttr
		}
	}
	return out
}

func minByServiceFloatAttr(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		if s.floatAttr == nil {
			continue
		}
		if cur, ok := out[s.service]; !ok || *s.floatAttr < cur {
			out[s.service] = *s.floatAttr
		}
	}
	return out
}

func maxByServiceFloatAttr(spans []parityFixtureSpan) map[string]float64 {
	out := make(map[string]float64)
	for _, s := range spans {
		if s.floatAttr == nil {
			continue
		}
		if cur, ok := out[s.service]; !ok || *s.floatAttr > cur {
			out[s.service] = *s.floatAttr
		}
	}
	return out
}

func avgByServiceFloatAttr(spans []parityFixtureSpan) map[string]float64 {
	sums := sumByServiceFloatAttr(spans)
	counts := make(map[string]int)
	for _, s := range spans {
		if s.floatAttr != nil {
			counts[s.service]++
		}
	}
	out := make(map[string]float64, len(sums))
	for svc, sum := range sums {
		out[svc] = sum / float64(counts[svc])
	}
	return out
}

// histogramFromDurations groups durations into tempo's own bucket boundaries
// (traceql.Log2Bucketize, called directly — the real oracle, ruling 1) and returns a
// bucket slice sorted ascending by Max, matching the shape engine_metrics.go itself
// sorts into before quantile computation (engine_metrics.go:2067).
func histogramFromDurations(durations []uint64) []traceql.HistogramBucket {
	counts := make(map[float64]int)
	for _, d := range durations {
		b := traceql.Log2Bucketize(d)
		if b < 0 {
			continue // v < 2 -> excluded from any bucket entirely (ruling 1)
		}
		counts[b]++
	}
	boundaries := make([]float64, 0, len(counts))
	for b := range counts {
		boundaries = append(boundaries, b)
	}
	sort.Float64s(boundaries)
	buckets := make([]traceql.HistogramBucket, 0, len(boundaries))
	for _, b := range boundaries {
		buckets = append(buckets, traceql.HistogramBucket{Max: b, Count: counts[b]})
	}
	return buckets
}

func histogramByService(spans []parityFixtureSpan) map[string][]traceql.HistogramBucket {
	byService := make(map[string][]uint64)
	for _, s := range spans {
		byService[s.service] = append(byService[s.service], s.durationNanos)
	}
	out := make(map[string][]traceql.HistogramBucket, len(byService))
	for svc, durations := range byService {
		out[svc] = histogramFromDurations(durations)
	}
	return out
}

// quantileByService calls traceql.Log2QuantileWithBucket DIRECTLY (the real oracle,
// ruling 1) against each service's histogram.
func quantileByService(spans []parityFixtureSpan, p float64) map[string]float64 {
	out := make(map[string]float64)
	for svc, buckets := range histogramByService(spans) {
		v, _ := traceql.Log2QuantileWithBucket(p, buckets)
		out[svc] = v
	}
	return out
}

// ---- Golden table (Step E-1.1/E-1.2) ----

// cubeMetricsParityCase pins one (fixture, function) pair's expected series, computed
// from tempo's own metrics-engine arithmetic (the real oracle). wantSkipReason is
// non-empty (and the case is t.Skip'd, never silently omitted) until the corresponding
// implementation task lands and unskips it as part of its own acceptance criteria.
type cubeMetricsParityCase struct {
	name         string
	fixtureSpans func() []parityFixtureSpan
	query        string // documents the TraceQL shape this case represents
	function     string

	stepSeconds float64 // rate only: count/step
	quantileP   float64 // quantile_over_time only

	wantScalar    map[string]float64                   // sum/min/max/avg/rate: per-service expected value
	wantHistogram map[string][]traceql.HistogramBucket // histogram_over_time only

	wantSkipReason string
}

var cubeMetricsParityGolden = []cubeMetricsParityCase{
	// count_over_time — implemented today (this is the case this task unskips).
	{
		name:         "count_over_time/non-pow2-single-minute",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | count_over_time() by (service.name)",
		function:     "count_over_time",
	},
	{
		name:         "count_over_time/hour-boundary",
		fixtureSpans: fixtureHourBoundary,
		query:        "{} | count_over_time() by (service.name)",
		function:     "count_over_time",
	},
	// rate — derived from the same L0 counts (count/step); implemented today.
	{
		name:         "rate/non-pow2-single-minute",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | rate() by (service.name)",
		function:     "rate",
		stepSeconds:  60,
	},
	{
		name:         "rate/day-boundary",
		fixtureSpans: fixtureDayBoundary,
		query:        "{} | rate() by (service.name)",
		function:     "rate",
		stepSeconds:  60,
	},
	// sum_over_time — E-4 landed; single-minute (non-rollup) fixtures unskipped per its
	// acceptance criteria.
	{
		name:         "sum_over_time/non-pow2-single-minute",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | sum_over_time(span:duration) by (service.name)",
		function:     "sum_over_time",
		wantScalar:   sumByService(fixtureNonPow2Single()),
	},
	{
		name:         "sum_over_time/float-attr",
		fixtureSpans: fixtureFloatAttr,
		query:        "{} | sum_over_time(span.request.size) by (service.name)",
		function:     "sum_over_time",
		wantScalar:   sumByServiceFloatAttr(fixtureFloatAttr()),
	},
	{
		// #50: every span in this fixture's single minute lacks floatAttrColumn entirely —
		// SampleCount==0 for every service. wantScalar is math.NaN() per service, pinned by
		// pkg/traceql's own TestSumOverTime_AllValuesMissing_StaysNaN regression (the real
		// engine's running sum never escapes its NaN seed when it never observes a real value) —
		// NOT the additive-identity 0 a naive "skip nil, sum what's left" formula would suggest.
		name:         "sum_over_time/float-attr-all-missing",
		fixtureSpans: fixtureFloatAttrAllMissing,
		query:        "{} | sum_over_time(span.request.size) by (service.name)",
		function:     "sum_over_time",
		wantScalar:   nanForEveryService(fixtureFloatAttrAllMissing()),
	},
	// min_over_time — E-4 landed.
	{
		name:         "min_over_time/non-pow2-single-minute",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | min_over_time(span:duration) by (service.name)",
		function:     "min_over_time",
		wantScalar:   minByService(fixtureNonPow2Single()),
	},
	{
		name:         "min_over_time/float-attr",
		fixtureSpans: fixtureFloatAttr,
		query:        "{} | min_over_time(span.request.size) by (service.name)",
		function:     "min_over_time",
		wantScalar:   minByServiceFloatAttr(fixtureFloatAttr()),
	},
	// max_over_time — E-4 landed.
	{
		name:         "max_over_time/non-pow2-single-minute",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | max_over_time(span:duration) by (service.name)",
		function:     "max_over_time",
		wantScalar:   maxByService(fixtureNonPow2Single()),
	},
	{
		name:         "max_over_time/float-attr",
		fixtureSpans: fixtureFloatAttr,
		query:        "{} | max_over_time(span.request.size) by (service.name)",
		function:     "max_over_time",
		wantScalar:   maxByServiceFloatAttr(fixtureFloatAttr()),
	},
	// avg_over_time — E-4 landed (derivable from Sum/SampleCount).
	{
		name:         "avg_over_time/non-pow2-single-minute",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | avg_over_time(span:duration) by (service.name)",
		function:     "avg_over_time",
		wantScalar:   avgByService(fixtureNonPow2Single()),
	},
	{
		name:         "avg_over_time/float-attr",
		fixtureSpans: fixtureFloatAttr,
		query:        "{} | avg_over_time(span.request.size) by (service.name)",
		function:     "avg_over_time",
		wantScalar:   avgByServiceFloatAttr(fixtureFloatAttr()),
	},
	// histogram_over_time — implemented (E-2 bucket math + E-4 accumulation + E-11b response
	// mapping).
	{
		name:          "histogram_over_time/non-pow2-single-minute",
		fixtureSpans:  fixtureNonPow2Single,
		query:         "{} | histogram_over_time(span:duration) by (service.name)",
		function:      "histogram_over_time",
		wantHistogram: histogramByService(fixtureNonPow2Single()),
	},
	{
		name:          "histogram_over_time/exact-pow2-boundary",
		fixtureSpans:  fixtureExactPow2Boundary,
		query:         "{} | histogram_over_time(span:duration) by (service.name)",
		function:      "histogram_over_time",
		wantHistogram: histogramByService(fixtureExactPow2Boundary()),
	},
	{
		name:          "histogram_over_time/below-threshold-excluded",
		fixtureSpans:  fixtureBelowThreshold,
		query:         "{} | histogram_over_time(span:duration) by (service.name)",
		function:      "histogram_over_time",
		wantHistogram: histogramByService(fixtureBelowThreshold()),
	},
	{
		// wantHistogram is deliberately left nil: a Float64-typed aggAttr's Buckets[] is never
		// populated (ruling 1) — assertHistogramParity treats a nil wantHistogram as "every
		// service's histogram must come back empty," the natural decline case.
		name:         "histogram_over_time/float-attr-declines",
		fixtureSpans: fixtureFloatAttr,
		query:        "{} | histogram_over_time(span.request.size) by (service.name)",
		function:     "histogram_over_time",
	},
	// quantile_over_time — implemented (E-2 bucket math + E-4 accumulation + E-11b response
	// mapping).
	{
		name:         "quantile_over_time/non-pow2-single-minute-p50",
		fixtureSpans: fixtureNonPow2Single,
		query:        "{} | quantile_over_time(span:duration, .5) by (service.name)",
		function:     "quantile_over_time",
		quantileP:    0.5,
		wantScalar:   quantileByService(fixtureNonPow2Single(), 0.5),
	},
	{
		name:         "quantile_over_time/exact-pow2-boundary-p50",
		fixtureSpans: fixtureExactPow2Boundary,
		query:        "{} | quantile_over_time(span:duration, .5) by (service.name)",
		function:     "quantile_over_time",
		quantileP:    0.5,
		wantScalar:   quantileByService(fixtureExactPow2Boundary(), 0.5),
	},
	{
		name:         "quantile_over_time/below-threshold-excluded-p50",
		fixtureSpans: fixtureBelowThreshold,
		query:        "{} | quantile_over_time(span:duration, .5) by (service.name)",
		function:     "quantile_over_time",
		quantileP:    0.5,
		wantScalar:   quantileByService(fixtureBelowThreshold(), 0.5),
	},
	{
		// wantScalar is deliberately left nil: a Float64-typed aggAttr's Buckets[] is never
		// populated (ruling 1), so Log2QuantileFromBuckets finds every bucket empty and declines
		// to NaN — assertQuantileParity treats a nil wantScalar as "every service must decline,"
		// the sibling case to histogram_over_time/float-attr-declines above (#491, E-11b polish
		// item 3 — closes the asymmetry where only histogram's decline was golden-pinned through
		// the real write path; quantile's was previously only unit-tested against a
		// hand-constructed cell in TestCellValueForFunction).
		name:         "quantile_over_time/float-attr-declines",
		fixtureSpans: fixtureFloatAttr,
		query:        "{} | quantile_over_time(span.request.size, .5) by (service.name)",
		function:     "quantile_over_time",
		quantileP:    0.5,
	},
}

// ---- Real write path (Step E-1.3 — mandatory, no hand-built Cell/AggCell literals) ----

// rollupThroughRealWritePath feeds spans through the REAL Accumulator.Add -> Encode
// sequence, one L0 file per minute (matching production's per-minute rotation in
// cubemanager.go's rotateLocked), then opens and rolls up every file through the REAL
// reader/rollup path. No Cell/AggCell is ever constructed by hand — this is the concrete
// instantiation of the "no hand-built cube fixtures anywhere in this phase" mandate.
func rollupThroughRealWritePath(t *testing.T, spans []parityFixtureSpan) []blockpack.CubeMergedCell {
	t.Helper()

	byMinute := make(map[uint32][]parityFixtureSpan)
	for _, s := range spans {
		byMinute[s.minute()] = append(byMinute[s.minute()], s)
	}

	// Every v2 Definition must materialize duration (ruling 3 + the third-round clamp,
	// enforced by NewCubeAccumulator/validateDefinition) — these single-dimension count/rate
	// fixtures don't need duration for their own assertions, but the accumulator still requires
	// it in AggAttrs to construct at all.
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}
	aggAttrNames := []string{blockpack.CubeDurationColumn}
	cubeIDHex := blockpack.CubeComputeID(parityTestTenant, []string{"service.name"}, nil, aggAttrNames)
	cubeID, err := blockpack.CubeIDFromHex(cubeIDHex)
	if err != nil {
		t.Fatalf("CubeIDFromHex: %v", err)
	}
	def := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__", // single-dimension cube sentinel (CubeRegistryEntryToDefinition's own convention)
		AggAttrs:   aggAttrs,
		ID:         cubeID,
		Resolution: 1,
	}

	minMinute, maxMinute := ^uint32(0), uint32(0)
	inputs := make([]blockpack.CubeRollupInput, 0, len(byMinute))
	for minute, minuteSpans := range byMinute {
		acc, accErr := blockpack.NewCubeAccumulator(def, minute)
		if accErr != nil {
			t.Fatalf("NewCubeAccumulator: %v", accErr)
		}
		for _, s := range minuteSpans {
			counted, addErr := acc.Add(s.toSpanValues())
			if addErr != nil {
				t.Fatalf("Accumulator.Add: %v", addErr)
			}
			if !counted {
				t.Fatalf("span for service %q at minute %d was not counted", s.service, minute)
			}
		}
		data, encErr := acc.Encode()
		if encErr != nil {
			t.Fatalf("Accumulator.Encode: %v", encErr)
		}
		reader, openErr := blockpack.OpenCubeReaderFromBytes(data)
		if openErr != nil {
			t.Fatalf("OpenCubeReaderFromBytes: %v", openErr)
		}
		inputs = append(inputs, blockpack.CubeNewRollupInput(reader))
		if minute < minMinute {
			minMinute = minute
		}
		if minute > maxMinute {
			maxMinute = minute
		}
	}

	cells, rollupErr := blockpack.CubeRollup(inputs, 1, minMinute, maxMinute)
	if rollupErr != nil {
		t.Fatalf("CubeRollup: %v", rollupErr)
	}
	return cells
}

// sumCountsByService collapses rolled-up cells into per-service totals. dim2 is the
// "__all__" sentinel for these single-dimension fixtures, so every cell for a service
// merges into exactly one total.
func sumCountsByService(cells []blockpack.CubeMergedCell) map[string]uint64 {
	out := make(map[string]uint64)
	for _, c := range cells {
		out[c.Dim1Val] += uint64(c.Count)
	}
	return out
}

func assertCountOverTimeParity(t *testing.T, c cubeMetricsParityCase) {
	t.Helper()
	spans := c.fixtureSpans()
	got := sumCountsByService(rollupThroughRealWritePath(t, spans))
	want := expectedCountsByService(spans)
	if len(got) != len(want) {
		t.Fatalf("service count mismatch: got %d services (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for svc, wantCount := range want {
		if got[svc] != wantCount {
			t.Errorf("count_over_time[%s] = %d, want %d (oracle: one increment per span, grouped by service.name)",
				svc, got[svc], wantCount)
		}
	}
}

func assertRateParity(t *testing.T, c cubeMetricsParityCase) {
	t.Helper()
	if c.stepSeconds <= 0 {
		t.Fatalf("case %q: rate cases must set stepSeconds", c.name)
	}
	spans := c.fixtureSpans()
	got := sumCountsByService(rollupThroughRealWritePath(t, spans))
	want := expectedCountsByService(spans)
	for svc, wantCount := range want {
		wantRate := float64(wantCount) / c.stepSeconds
		gotRate := float64(got[svc]) / c.stepSeconds
		if gotRate != wantRate {
			t.Errorf("rate[%s] = %v, want %v (oracle: count/step, engine_metrics.go's rate formula)",
				svc, gotRate, wantRate)
		}
	}
}

// parityAggResult mirrors the fields of one AggAttrValues record this test needs (Sum/Min/Max/
// SampleCount) — a locally-defined shape, never cube.AggAttrValues by name (that internal-
// package type isn't importable from tempo; its values are used here purely via inferred typing
// from AggCell.Aggs[i], per Go's internal-package rule: usable, just not nameable).
type parityAggResult struct {
	sum, minV, maxV float64
	sampleCount     uint32
	buckets         [blockpack.CubeBucketCount]uint64
}

// targetAggAttrForCase returns the AggAttrs a sum/min/max/avg case's Definition needs, and the
// index within it the case's query target attribute occupies. "float-attr" cases add
// floatAttrColumn as a SECOND, Float64-typed attribute alongside the mandatory duration
// (ruling 3); every other case's target IS duration itself (index 0).
func targetAggAttrForCase(c cubeMetricsParityCase) (aggAttrs []blockpack.CubeAggAttrDef, targetIdx int) {
	duration := blockpack.CubeAggAttrDef{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}
	if strings.Contains(c.name, "float-attr") {
		return []blockpack.CubeAggAttrDef{
			duration,
			{Column: floatAttrColumn, Type: blockpack.CubeAggAttrTypeFloat64},
		}, 1
	}
	return []blockpack.CubeAggAttrDef{duration}, 0
}

// aggAttrThroughRealWritePath is rollupThroughRealWritePath's sibling for Sum/Min/Max/
// SampleCount parity: feeds a SINGLE-MINUTE fixture through the REAL Accumulator.Add ->
// Encode -> OpenCubeReaderFromBytes -> GetAggCell sequence (E-3's new AggCell-shaped reader
// method) and returns the targetIdx-th aggAttr's values per service. Single-minute only (no
// rollup) — E-5's cross-minute merge is a separate concern the hour/day-boundary fixtures
// exercise once E-5 lands.
func aggAttrThroughRealWritePath(
	t *testing.T, spans []parityFixtureSpan, aggAttrs []blockpack.CubeAggAttrDef, targetIdx int,
) map[string]parityAggResult {
	t.Helper()

	byMinute := make(map[uint32][]parityFixtureSpan)
	for _, s := range spans {
		byMinute[s.minute()] = append(byMinute[s.minute()], s)
	}
	if len(byMinute) != 1 {
		t.Fatalf("aggAttrThroughRealWritePath requires a single-minute fixture, got %d minutes", len(byMinute))
	}

	aggAttrNames := make([]string, len(aggAttrs))
	for i, a := range aggAttrs {
		aggAttrNames[i] = a.Column
	}
	cubeIDHex := blockpack.CubeComputeID(parityTestTenant, []string{"service.name"}, nil, aggAttrNames)
	cubeID, err := blockpack.CubeIDFromHex(cubeIDHex)
	if err != nil {
		t.Fatalf("CubeIDFromHex: %v", err)
	}
	def := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__",
		AggAttrs:   aggAttrs,
		ID:         cubeID,
		Resolution: 1,
	}

	var minute uint32
	var minuteSpans []parityFixtureSpan
	for m, s := range byMinute {
		minute, minuteSpans = m, s
	}

	acc, accErr := blockpack.NewCubeAccumulator(def, minute)
	if accErr != nil {
		t.Fatalf("NewCubeAccumulator: %v", accErr)
	}
	services := make(map[string]bool)
	for _, s := range minuteSpans {
		services[s.service] = true
		counted, addErr := acc.Add(s.toSpanValues())
		if addErr != nil {
			t.Fatalf("Accumulator.Add: %v", addErr)
		}
		if !counted {
			t.Fatalf("span for service %q was not counted", s.service)
		}
	}
	data, encErr := acc.Encode()
	if encErr != nil {
		t.Fatalf("Accumulator.Encode: %v", encErr)
	}
	reader, openErr := blockpack.OpenCubeReaderFromBytes(data)
	if openErr != nil {
		t.Fatalf("OpenCubeReaderFromBytes: %v", openErr)
	}

	out := make(map[string]parityAggResult, len(services))
	for svc := range services {
		agg, found := reader.GetAggCell(minute, svc, "__all__")
		if !found {
			t.Fatalf("service %q missing from GetAggCell result", svc)
		}
		if targetIdx >= len(agg.Aggs) {
			t.Fatalf("targetIdx %d out of range (only %d aggAttrs)", targetIdx, len(agg.Aggs))
		}
		target := agg.Aggs[targetIdx]
		out[svc] = parityAggResult{
			sum: target.Sum, minV: target.Min, maxV: target.Max,
			sampleCount: target.SampleCount, buckets: target.Buckets,
		}
	}
	return out
}

// assertScalarAggParity runs sum/min/max/avg_over_time's shared real-write-path shape,
// extracting the specific scalar each function compares via extract. A NaN in wantScalar (#50's
// all-missing-attribute fixture class) is compared via math.IsNaN, never plain equality — NaN
// never equals itself, so an equality check would always report a spurious mismatch for a
// legitimately-expected NaN.
func assertScalarAggParity(t *testing.T, c cubeMetricsParityCase, extract func(parityAggResult) float64) {
	t.Helper()
	spans := c.fixtureSpans()
	aggAttrs, targetIdx := targetAggAttrForCase(c)
	got := aggAttrThroughRealWritePath(t, spans, aggAttrs, targetIdx)
	for svc, want := range c.wantScalar {
		g, ok := got[svc]
		if !ok {
			t.Fatalf("service %q missing from real-write-path result", svc)
		}
		gotVal := extract(g)
		if math.IsNaN(want) {
			if !math.IsNaN(gotVal) {
				t.Errorf("%s[%s] = %v, want NaN", c.function, svc, gotVal)
			}
			continue
		}
		if gotVal != want {
			t.Errorf("%s[%s] = %v, want %v", c.function, svc, gotVal, want)
		}
	}
}

// histogramFromBuckets converts a merged AggAttrValues.Buckets[64]uint64 array into a sorted
// []traceql.HistogramBucket (Max/Count pairs), skipping empty slots — the same shape
// histogramFromDurations produces from raw durations, so both sides of the parity comparison
// use an identical type. blockpack.CubeBucketMax is the SAME single-source-of-truth function
// buildHistogramResponse itself calls (ruling 5; #491, E-11b polish item 1).
func histogramFromBuckets(buckets [blockpack.CubeBucketCount]uint64) []traceql.HistogramBucket {
	var out []traceql.HistogramBucket
	for i, count := range buckets {
		if count == 0 {
			continue
		}
		out = append(out, traceql.HistogramBucket{Max: blockpack.CubeBucketMax(i), Count: int(count)})
	}
	return out
}

// assertHistogramParity runs histogram_over_time's real-write-path shape, comparing the merged
// Buckets[64]uint64 array (converted to the oracle's own []traceql.HistogramBucket shape) against
// c.wantHistogram. A nil c.wantHistogram (the float-attr-declines case) asserts every service's
// Buckets[] is entirely empty — ruling 1's own accumulator-level decline for Float64-typed
// aggAttrs, which E-11b's response layer inherits with no separate type check of its own.
func assertHistogramParity(t *testing.T, c cubeMetricsParityCase) {
	t.Helper()
	spans := c.fixtureSpans()
	aggAttrs, targetIdx := targetAggAttrForCase(c)
	got := aggAttrThroughRealWritePath(t, spans, aggAttrs, targetIdx)

	if c.wantHistogram == nil {
		for svc, g := range got {
			if buckets := histogramFromBuckets(g.buckets); len(buckets) != 0 {
				t.Errorf("%s[%s]: expected a declined (empty) histogram for a Float64-typed aggAttr, got %+v",
					c.function, svc, buckets)
			}
		}
		return
	}

	for svc, want := range c.wantHistogram {
		g, ok := got[svc]
		if !ok {
			t.Fatalf("service %q missing from real-write-path result", svc)
		}
		gotBuckets := histogramFromBuckets(g.buckets)
		if !reflect.DeepEqual(gotBuckets, want) {
			t.Errorf("%s[%s] = %+v, want %+v", c.function, svc, gotBuckets, want)
		}
	}
}

// assertQuantileParity runs quantile_over_time's real-write-path shape, dispatching through the
// SAME cellValueForFunction production code buildCubeQueryResponse itself calls. The merged-cell
// literal built here wraps ALREADY-REAL Buckets[]/SampleCount values obtained via the real
// Accumulator->Encode->GetAggCell path — Lesson 2's "no hand-built AggCell" bar is about
// fabricating the underlying DATA, not about which merge-shaped wrapper the real bucket counts
// are subsequently passed through to reach the dispatch function under test. A nil c.wantScalar
// (the float-attr-declines case, #491 E-11b polish item 3) asserts every service declines to NaN
// — mirroring assertHistogramParity's own nil-wantHistogram convention; without this explicit
// branch, `for range nil map` silently executes zero iterations and the case would pass without
// asserting anything at all.
func assertQuantileParity(t *testing.T, c cubeMetricsParityCase) {
	t.Helper()
	spans := c.fixtureSpans()
	aggAttrs, targetIdx := targetAggAttrForCase(c)
	got := aggAttrThroughRealWritePath(t, spans, aggAttrs, targetIdx)

	if c.wantScalar == nil {
		for svc, g := range got {
			cell := blockpack.CubeMergedCell{
				AggAttrs: []blockpack.CubeAggAttrValues{{SampleCount: g.sampleCount, Buckets: g.buckets}},
			}
			gotVal := cellValueForFunction(cell, "quantile_over_time", 0, c.quantileP)
			if !math.IsNaN(gotVal) {
				t.Errorf("%s[%s]: expected a declined (NaN) quantile for a Float64-typed aggAttr, got %v",
					c.function, svc, gotVal)
			}
		}
		return
	}

	for svc, want := range c.wantScalar {
		g, ok := got[svc]
		if !ok {
			t.Fatalf("service %q missing from real-write-path result", svc)
		}
		cell := blockpack.CubeMergedCell{
			AggAttrs: []blockpack.CubeAggAttrValues{{SampleCount: g.sampleCount, Buckets: g.buckets}},
		}
		gotVal := cellValueForFunction(cell, "quantile_over_time", 0, c.quantileP)
		if gotVal != want {
			t.Errorf("%s[%s] = %v, want %v", c.function, svc, gotVal, want)
		}
	}
}

// TestCubeMetricsParity_DurationSpellingContract_SumAndCountNonZero pins the DurationColumn
// spelling contract (cube.DurationColumn = blockio/shared.SpanDurationColumnName =
// "span:duration", matching tempoSpanValues' own hardcoded "span:duration" case) as its OWN,
// isolated assertion — per E-4's (#22) own acceptance criterion: a spelling regression must
// make ONLY this test red, not scatter across the four differently-symptomed sum/min/max/avg
// golden subtests in TestCubeMetricsParity_MatchesFrozenGolden (which would otherwise be the
// only signal, each failing for a different-looking reason). Feeds ONE span with a known
// non-zero duration through the real write path and asserts the duration aggAttr's Sum and
// SampleCount are both non-zero (and exactly correct) — a spelling mismatch makes
// Float64(DurationColumn) return (0, false) for every span, silently zeroing both.
func TestCubeMetricsParity_DurationSpellingContract_SumAndCountNonZero(t *testing.T) {
	spans := []parityFixtureSpan{spanAt(5000, 0, "svc-a", 3_000_000_001)}
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}

	got := aggAttrThroughRealWritePath(t, spans, aggAttrs, 0)
	g, ok := got["svc-a"]
	if !ok {
		t.Fatal("svc-a missing from real-write-path result")
	}
	if g.sampleCount == 0 {
		t.Fatal("duration aggAttr SampleCount == 0 — DurationColumn's spelling does not match " +
			"the production adapter's key (spelling regression)")
	}
	if g.sum == 0 {
		t.Fatal("duration aggAttr Sum == 0 — DurationColumn's spelling does not match the " +
			"production adapter's key (spelling regression)")
	}
	const wantSum = float64(3_000_000_001)
	if g.sum != wantSum {
		t.Fatalf("duration aggAttr Sum = %v, want %v", g.sum, wantSum)
	}
	if g.sampleCount != 1 {
		t.Fatalf("duration aggAttr SampleCount = %d, want 1", g.sampleCount)
	}
}

// TestCubeMetricsParity_MatchesFrozenGolden is E-1's shared golden table (SECTION 0):
// each case either runs an end-to-end assertion through the REAL cube write path when its
// function is already implemented cube-side, or is t.Skip'd naming the blocking task ID.
func TestCubeMetricsParity_MatchesFrozenGolden(t *testing.T) {
	for _, c := range cubeMetricsParityGolden {
		t.Run(c.name, func(t *testing.T) {
			if c.wantSkipReason != "" {
				t.Skip(c.wantSkipReason)
			}
			switch c.function {
			case "count_over_time":
				assertCountOverTimeParity(t, c)
			case "rate":
				assertRateParity(t, c)
			case "sum_over_time":
				assertScalarAggParity(t, c, func(g parityAggResult) float64 {
					// #50: matches cellValueForFunction's own post-#47 convention — sum stays NaN
					// (not the additive identity 0) when every span in the window lacked the
					// attribute, the same short-circuit min/max/avg already apply.
					if g.sampleCount == 0 {
						return math.NaN()
					}
					return g.sum
				})
			case "min_over_time":
				assertScalarAggParity(t, c, func(g parityAggResult) float64 { return g.minV })
			case "max_over_time":
				assertScalarAggParity(t, c, func(g parityAggResult) float64 { return g.maxV })
			case "avg_over_time":
				assertScalarAggParity(t, c, func(g parityAggResult) float64 {
					if g.sampleCount == 0 {
						return 0
					}
					return g.sum / float64(g.sampleCount)
				})
			case "histogram_over_time":
				assertHistogramParity(t, c)
			case "quantile_over_time":
				assertQuantileParity(t, c)
			default:
				t.Fatalf("no real-write-path assertion wired for function %q (case %q) — "+
					"every non-skipped case must have one", c.function, c.name)
			}
		})
	}
}

// buildCubeQueryResponseThroughRealPath feeds spans through the REAL Accumulator -> Encode ->
// OpenCubeReaderFromBytes -> CubeRollup -> buildCubeQueryResponse path (E-11a) — mirroring
// production's own tryQueryFromCube, minus the S3 I/O. This proves E-11a's response-mapping
// function ITSELF produces correct values; E-4's aggAttrThroughRealWritePath already proved the
// accumulator does (by reading AggCell directly, bypassing buildCubeQueryResponse entirely) —
// this closes the gap between "the accumulator is right" and "the API response is right."
//
// One input file is built per DISTINCT MINUTE in spans (mirroring production's own per-minute L0
// rotation in cubemanager.go), then CubeRollup merges them to targetResolution — a
// targetResolution > 1 with spans landing in the SAME target bucket is the ONLY way to exercise a
// genuine multi-file merge (#51); every targetResolution==1 caller before #51 only ever produced
// one output cell per input file, a trivial "merge of one" that cannot distinguish correct
// (merge-then-aggregate) from incorrect (aggregate-then-average-across-files) behavior.
func buildCubeQueryResponseThroughRealPath(
	t *testing.T, spans []parityFixtureSpan, aggAttrs []blockpack.CubeAggAttrDef, query string, targetResolution uint32,
) *tempopb.QueryRangeResponse {
	t.Helper()

	byMinute := make(map[uint32][]parityFixtureSpan)
	for _, s := range spans {
		byMinute[s.minute()] = append(byMinute[s.minute()], s)
	}

	aggAttrNames := make([]string, len(aggAttrs))
	for i, a := range aggAttrs {
		aggAttrNames[i] = a.Column
	}
	cubeIDHex := blockpack.CubeComputeID(parityTestTenant, []string{"service.name"}, nil, aggAttrNames)
	cubeID, err := blockpack.CubeIDFromHex(cubeIDHex)
	if err != nil {
		t.Fatalf("CubeIDFromHex: %v", err)
	}
	def := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__",
		AggAttrs:   aggAttrs,
		ID:         cubeID,
		Resolution: 1,
	}

	minMinute, maxMinute := ^uint32(0), uint32(0)
	inputs := make([]blockpack.CubeRollupInput, 0, len(byMinute))
	for minute, minuteSpans := range byMinute {
		acc, accErr := blockpack.NewCubeAccumulator(def, minute)
		if accErr != nil {
			t.Fatalf("NewCubeAccumulator: %v", accErr)
		}
		for _, s := range minuteSpans {
			if _, addErr := acc.Add(s.toSpanValues()); addErr != nil {
				t.Fatalf("Add: %v", addErr)
			}
		}
		data, encErr := acc.Encode()
		if encErr != nil {
			t.Fatalf("Encode: %v", encErr)
		}
		reader, openErr := blockpack.OpenCubeReaderFromBytes(data)
		if openErr != nil {
			t.Fatalf("OpenCubeReaderFromBytes: %v", openErr)
		}
		inputs = append(inputs, blockpack.CubeNewRollupInput(reader))
		if minute < minMinute {
			minMinute = minute
		}
		if minute > maxMinute {
			maxMinute = minute
		}
	}

	cells, rollupErr := blockpack.CubeRollup(inputs, targetResolution, minMinute, maxMinute)
	if rollupErr != nil {
		t.Fatalf("CubeRollup: %v", rollupErr)
	}

	req := &tempopb.QueryRangeRequest{Query: query}
	return buildCubeQueryResponse(cells, []string{"service.name"}, aggAttrNames, req)
}

// sampleValuesByService extracts each series' single sample value, keyed by its first label's
// (service.name's) value — sufficient for these single-minute fixtures, which produce exactly
// one sample per series.
func sampleValuesByService(resp *tempopb.QueryRangeResponse) map[string]float64 {
	out := make(map[string]float64)
	for _, series := range resp.Series {
		if len(series.Labels) == 0 || len(series.Samples) == 0 {
			continue
		}
		out[series.Labels[0].Value.GetStringValue()] = series.Samples[0].Value
	}
	return out
}

// histogramSeriesToByService reconstructs a map[string][]traceql.HistogramBucket from
// buildHistogramResponse's own series shape — one series per (dims, traceql.LabelBucket) tuple,
// dims first then the bucket label (buildHistogramResponse's own label ordering) — the inverse
// of what that function builds, letting the test compare the ACTUAL production response against
// the oracle's histogramByService shape.
func histogramSeriesToByService(resp *tempopb.QueryRangeResponse) map[string][]traceql.HistogramBucket {
	out := make(map[string][]traceql.HistogramBucket)
	for _, series := range resp.Series {
		if len(series.Labels) < 2 || len(series.Samples) == 0 {
			continue
		}
		svc := series.Labels[0].Value.GetStringValue()
		bucketMax := series.Labels[1].Value.GetDoubleValue()
		var count int
		for _, s := range series.Samples {
			count += int(s.Value)
		}
		out[svc] = append(out[svc], traceql.HistogramBucket{Max: bucketMax, Count: count})
	}
	for svc := range out {
		sort.Slice(out[svc], func(i, j int) bool { return out[svc][i].Max < out[svc][j].Max })
	}
	return out
}

// TestBuildCubeQueryResponse_RealWritePath_HistogramQuantile (#491, E-11b) proves
// buildCubeQueryResponse itself — the actual production response-mapping function
// tryQueryFromCube calls — correctly maps real, rolled-up cells to histogram_over_time's
// multi-series bucket shape and quantile_over_time's scalar shape (closing the same
// accumulator-vs-response-mapping gap E-11a closed for sum/min/max/avg).
func TestBuildCubeQueryResponse_RealWritePath_HistogramQuantile(t *testing.T) {
	spans := fixtureNonPow2Single()
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}

	t.Run("histogram_over_time", func(t *testing.T) {
		resp := buildCubeQueryResponseThroughRealPath(t, spans, aggAttrs,
			"{} | histogram_over_time(span:duration) by (service.name)", 1)
		got := histogramSeriesToByService(resp)
		want := histogramByService(spans)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("histogram_over_time response = %+v, want %+v", got, want)
		}
	})

	t.Run("quantile_over_time", func(t *testing.T) {
		resp := buildCubeQueryResponseThroughRealPath(t, spans, aggAttrs,
			"{} | quantile_over_time(span:duration, .5) by (service.name)", 1)
		got := sampleValuesByService(resp)
		want := quantileByService(spans, 0.5)
		for svc, wantVal := range want {
			if got[svc] != wantVal {
				t.Errorf("quantile_over_time[%s] = %v, want %v", svc, got[svc], wantVal)
			}
		}
	})
}

// TestBuildCubeQueryResponse_RealWritePath_MultiFileMerge_HistogramQuantile (#51) proves the
// binding E-11b ruling — "quantile_over_time via Log2QuantileFromBuckets AFTER full-window merge
// to target resolution, never partial-resolution mixing" — in the ONE scenario that can actually
// distinguish correct (merge Buckets[] element-wise across ALL input files, THEN compute one
// quantile/histogram) from incorrect (compute a value per input file, then average/concatenate
// the results). fixtureMultiMinuteSameHourBucket's three spans each land in their OWN per-minute
// L0 file (mirroring production's real per-minute file rotation, buildCubeQueryResponseThroughRealPath's
// own byMinute grouping) but all three collapse into the SAME L1 (60-minute) bucket cell.
// TestBuildCubeQueryResponse_RealWritePath_HistogramQuantile above only uses a single-minute
// fixture at L0 (targetResolution 1), where CubeRollup receives exactly one input file — a
// trivial "merge of one" that cannot distinguish correct from incorrect merge ordering.
func TestBuildCubeQueryResponse_RealWritePath_MultiFileMerge_HistogramQuantile(t *testing.T) {
	spans := fixtureMultiMinuteSameHourBucket()
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}
	const l1Resolution = 60

	t.Run("histogram_over_time", func(t *testing.T) {
		resp := buildCubeQueryResponseThroughRealPath(t, spans, aggAttrs,
			"{} | histogram_over_time(span:duration) by (service.name)", l1Resolution)
		got := histogramSeriesToByService(resp)
		want := histogramByService(spans) // oracle over the FULL span set, not per-minute-file
		if !reflect.DeepEqual(got, want) {
			t.Errorf("histogram_over_time response = %+v, want %+v (merge-then-bucket across all "+
				"input files, not per-file)", got, want)
		}
	})

	t.Run("quantile_over_time", func(t *testing.T) {
		resp := buildCubeQueryResponseThroughRealPath(t, spans, aggAttrs,
			"{} | quantile_over_time(span:duration, .5) by (service.name)", l1Resolution)
		got := sampleValuesByService(resp)
		want := quantileByService(spans, 0.5) // oracle over the FULL span set, not per-minute-file
		for svc, wantVal := range want {
			if got[svc] != wantVal {
				t.Errorf("quantile_over_time[%s] = %v, want %v (merge-then-quantile across all "+
					"input files, not per-file-then-average)", svc, got[svc], wantVal)
			}
		}
	})
}

// TestBuildCubeQueryResponse_RealWritePath_SumMinMaxAvg (#491, E-11a) proves buildCubeQueryResponse
// itself — the actual production response-mapping function tryQueryFromCube calls — maps
// real, rolled-up cells to the correct sum/min/max/avg_over_time sample values.
func TestBuildCubeQueryResponse_RealWritePath_SumMinMaxAvg(t *testing.T) {
	spans := fixtureNonPow2Single()
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}

	cases := []struct {
		function string
		query    string
		want     map[string]float64
	}{
		{"sum_over_time", "{} | sum_over_time(span:duration) by (service.name)", sumByService(spans)},
		{"min_over_time", "{} | min_over_time(span:duration) by (service.name)", minByService(spans)},
		{"max_over_time", "{} | max_over_time(span:duration) by (service.name)", maxByService(spans)},
		{"avg_over_time", "{} | avg_over_time(span:duration) by (service.name)", avgByService(spans)},
	}
	for _, tc := range cases {
		t.Run(tc.function, func(t *testing.T) {
			resp := buildCubeQueryResponseThroughRealPath(t, spans, aggAttrs, tc.query, 1)
			got := sampleValuesByService(resp)
			for svc, want := range tc.want {
				if got[svc] != want {
					t.Errorf("%s[%s] = %v, want %v", tc.function, svc, got[svc], want)
				}
			}
		})
	}
}

// TestBuildCubeQueryResponse_RealWritePath_SumOverTime_AllMissingStaysNaN (#50) proves
// buildCubeQueryResponse itself — not just the accumulator (already covered by the golden table's
// sum_over_time/float-attr-all-missing case) — answers NaN, not the additive-identity 0, for a
// sum_over_time query whose target aggAttr had zero valid samples in the window. Closes the same
// accumulator-vs-response-mapping gap E-11a/E-11b closed for the other functions.
func TestBuildCubeQueryResponse_RealWritePath_SumOverTime_AllMissingStaysNaN(t *testing.T) {
	spans := fixtureFloatAttrAllMissing()
	aggAttrs := []blockpack.CubeAggAttrDef{
		{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64},
		{Column: floatAttrColumn, Type: blockpack.CubeAggAttrTypeFloat64},
	}
	resp := buildCubeQueryResponseThroughRealPath(t, spans, aggAttrs,
		"{} | sum_over_time(span.request.size) by (service.name)", 1)
	got := sampleValuesByService(resp)
	for _, s := range spans {
		v, ok := got[s.service]
		if !ok {
			t.Fatalf("service %q missing from response", s.service)
		}
		if !math.IsNaN(v) {
			t.Errorf("sum_over_time[%s] = %v, want NaN", s.service, v)
		}
	}
}

// TestCellValueForFunction (#491, E-11a/E-11b/#50): a focused unit test on the pure dispatch
// function itself — sum/avg/min/max all short-circuit to NaN when SampleCount==0 (the
// sentinel-init Min/Max must never leak into a response; sum's NaN short-circuit, task #50,
// matches pkg/traceql's own post-#47 sumOverTime() convention — a running sum that never
// observes a real value stays NaN, it does not report the additive identity 0); count_over_time/
// rate are unaffected by targetIdx. quantile_over_time short-circuits to NaN the same way when
// every bucket is empty (the natural decline case for a Float64-typed aggAttr, ruling 1).
func TestCellValueForFunction(t *testing.T) {
	populated := blockpack.CubeMergedCell{
		Dim1Val: "svc-a",
		Count:   5,
		AggAttrs: []blockpack.CubeAggAttrValues{
			{SampleCount: 3, Sum: 30, Min: 5, Max: 20},
		},
	}
	empty := blockpack.CubeMergedCell{
		Dim1Val: "svc-b",
		Count:   2,
		AggAttrs: []blockpack.CubeAggAttrValues{
			{SampleCount: 0, Sum: 0, Min: math.MaxFloat64, Max: -math.MaxFloat64},
		},
	}

	// withBuckets exercises quantile_over_time's Buckets[]-based dispatch. wantQ50 is computed
	// via the SAME oracle (CubeLog2QuantileFromBuckets) cellValueForFunction itself calls — this
	// test verifies the WIRING (that cellValueForFunction passes the right buckets/p through),
	// not Log2QuantileFromBuckets's own interpolation correctness (already covered by E-2).
	var q50Buckets [blockpack.CubeBucketCount]uint64
	q50Buckets[1] = 4
	withBuckets := blockpack.CubeMergedCell{
		Dim1Val: "svc-c",
		Count:   4,
		AggAttrs: []blockpack.CubeAggAttrValues{
			{SampleCount: 4, Buckets: q50Buckets},
		},
	}
	wantQ50, wantQ50BucketIdx := blockpack.CubeLog2QuantileFromBuckets(0.5, q50Buckets)
	if wantQ50BucketIdx == -1 {
		t.Fatal("test setup error: q50Buckets must produce a valid (non-declined) quantile")
	}

	cases := []struct {
		name      string
		cell      blockpack.CubeMergedCell
		function  string
		targetIdx int
		quantileP float64
		want      float64
		wantNaN   bool
	}{
		{"sum", populated, "sum_over_time", 0, 0, 30, false},
		{"min", populated, "min_over_time", 0, 0, 5, false},
		{"max", populated, "max_over_time", 0, 0, 20, false},
		{"avg", populated, "avg_over_time", 0, 0, 10, false},
		{"min sample-count 0 -> NaN", empty, "min_over_time", 0, 0, 0, true},
		{"max sample-count 0 -> NaN", empty, "max_over_time", 0, 0, 0, true},
		{"avg sample-count 0 -> NaN", empty, "avg_over_time", 0, 0, 0, true},
		{"sum sample-count 0 -> NaN (post-#47 parity, not the additive identity 0)", empty, "sum_over_time", 0, 0, 0, true},
		{"count_over_time reads Count directly (targetIdx unused)", populated, "count_over_time", -1, 0, 5, false},
		{"rate reads Count directly (targetIdx unused)", populated, "rate", -1, 0, 5, false},
		{"quantile p50 dispatches through Log2QuantileFromBuckets", withBuckets, "quantile_over_time", 0, 0.5, wantQ50, false},
		{"quantile all-buckets-empty -> NaN (Float64-typed aggAttr decline)", empty, "quantile_over_time", 0, 0.5, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cellValueForFunction(tc.cell, tc.function, tc.targetIdx, tc.quantileP)
			if tc.wantNaN {
				if !math.IsNaN(got) {
					t.Fatalf("cellValueForFunction() = %v, want NaN", got)
				}
				return
			}
			if got != tc.want {
				t.Fatalf("cellValueForFunction() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCubeMetricsParity_RouterSupersetTieBreak is fixture (f): two registered cubes share
// (dims, filters) but differ in attribute set; a query needing the wider set must route to
// the superset cube. E-6/E-6b/E-12a have all landed (this test was a stale t.Skip stub left
// over from before they did, #491 Phase E fix pass, review.md Issue 4 / go-presubmit.md #1) —
// this proves the TEMPO-side integration (real Registry round trip, real QueryRouter.Route, real
// cube files via the real Accumulator write path, real CubeRollup + buildCubeQueryResponse), which
// internal/modules/cube/router_test.go's own TestRoute_SupersetTieBreak_* unit tests do not cover.
func TestCubeMetricsParity_RouterSupersetTieBreak(t *testing.T) {
	ctx := context.Background()
	tenant := "parity-superset-tenant"
	objStore := &fakeSchedObjectStore{}
	reg := blockpack.NewCubeRegistry(objStore, tenant)

	// Cube A tracks only duration — does NOT cover the query's needed attribute below.
	idAHex := blockpack.CubeComputeID(tenant, []string{"service.name"}, nil, []string{blockpack.CubeDurationColumn})
	idA, err := blockpack.CubeIDFromHex(idAHex)
	if err != nil {
		t.Fatalf("CubeIDFromHex(A): %v", err)
	}
	entryA := blockpack.CubeRegistryEntry{
		CubeID:     idAHex,
		Tenant:     tenant,
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{1: {MinMinute: 0, MaxMinute: 100}},
		Resolution: 1,
		CreatedAt:  1000,
	}
	if err := reg.Add(ctx, entryA); err != nil {
		t.Fatalf("reg.Add(A): %v", err)
	}

	// Cube B shares (tenant, dims, filters) with A but ALSO tracks http.status_code — the only
	// candidate that can answer a query needing that attribute.
	idBHex := blockpack.CubeComputeID(tenant, []string{"service.name"}, nil, []string{blockpack.CubeDurationColumn, "http.status_code"})
	idB, err := blockpack.CubeIDFromHex(idBHex)
	if err != nil {
		t.Fatalf("CubeIDFromHex(B): %v", err)
	}
	entryB := blockpack.CubeRegistryEntry{
		CubeID:     idBHex,
		Tenant:     tenant,
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn, "http.status_code"},
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{1: {MinMinute: 0, MaxMinute: 100}},
		Resolution: 1,
		CreatedAt:  2000,
	}
	if err := reg.Add(ctx, entryB); err != nil {
		t.Fatalf("reg.Add(B): %v", err)
	}

	// Build real L0 files for BOTH cubes via the real Accumulator write path, with
	// distinguishable data (A: 1 span, B: 3 spans) so the test can tell which one a query
	// actually read from.
	defA := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         idA,
		Resolution: 1,
	}
	accA, err := blockpack.NewCubeAccumulator(defA, 10)
	if err != nil {
		t.Fatalf("NewCubeAccumulator(A): %v", err)
	}
	if _, addErr := accA.Add(spanAt(10, 0, "svc-a", 1_000_000).toSpanValues()); addErr != nil {
		t.Fatalf("Add(A): %v", addErr)
	}
	dataA, err := accA.Encode()
	if err != nil {
		t.Fatalf("Encode(A): %v", err)
	}
	if _, err := blockpack.OpenCubeReaderFromBytes(dataA); err != nil {
		t.Fatalf("OpenCubeReaderFromBytes(A): %v", err)
	}

	defB := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__",
		AggAttrs: []blockpack.CubeAggAttrDef{
			{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64},
			{Column: "http.status_code", Type: blockpack.CubeAggAttrTypeFloat64},
		},
		ID:         idB,
		Resolution: 1,
	}
	accB, err := blockpack.NewCubeAccumulator(defB, 10)
	if err != nil {
		t.Fatalf("NewCubeAccumulator(B): %v", err)
	}
	for i := range 3 {
		if _, addErr := accB.Add(spanAt(10, uint64(i)*1000, "svc-a", 2_000_000).toSpanValues()); addErr != nil {
			t.Fatalf("Add(B) span %d: %v", i, addErr)
		}
	}
	dataB, err := accB.Encode()
	if err != nil {
		t.Fatalf("Encode(B): %v", err)
	}
	readerB, err := blockpack.OpenCubeReaderFromBytes(dataB)
	if err != nil {
		t.Fatalf("OpenCubeReaderFromBytes(B): %v", err)
	}

	// Real Registry round trip: load entries back exactly as tryQueryFromCube would.
	entries, _, err := reg.Load(ctx)
	if err != nil {
		t.Fatalf("reg.Load: %v", err)
	}
	router := blockpack.NewCubeQueryRouter(entries)
	res, err := router.Route(tenant, []string{"service.name"}, nil, "http.status_code", 1, 0, 100)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if !res.Found {
		t.Fatal("Route must find a matching cube (B covers the needed attribute http.status_code)")
	}
	if res.Entry.CubeID != idBHex {
		t.Fatalf("Route must select cube B (the smallest superset covering neededAttr), got %q, want %q", res.Entry.CubeID, idBHex)
	}

	// Complete the query through the REAL rollup + response-mapping path using B's real file, and
	// assert the response reflects B's data (3 spans) — proving the routing decision above
	// actually determines which cube's data answers the query, not just that Route's return value
	// looks right in isolation.
	cells, err := blockpack.CubeRollup([]blockpack.CubeRollupInput{blockpack.CubeNewRollupInput(readerB)}, res.Resolution, 10, 10)
	if err != nil {
		t.Fatalf("CubeRollup: %v", err)
	}
	req := &tempopb.QueryRangeRequest{Query: "{} | count_over_time() by (service.name)"}
	resp := buildCubeQueryResponse(cells, res.Entry.Dimensions, res.Entry.AggAttrs, req)
	got := sampleValuesByService(resp)
	if got["svc-a"] != 3 {
		t.Fatalf("response count for svc-a = %v, want 3 (cube B's real data, not cube A's 1-span file)", got["svc-a"])
	}
}

// TestCubeMetricsParity_ResolutionCompletenessDecline is fixture (g): a query window not fully
// covered by the chosen resolution's watermark must decline the WHOLE query, never a partial or
// mixed-resolution answer (ruling 4(b)). E-6b/E-12a have both landed (this test was a stale
// t.Skip stub left over from before they did, #491 Phase E fix pass, review.md Issue 4 /
// go-presubmit.md #1) — this proves the tempo-side integration via a real Registry round trip and
// the real Registry.UpdateWatermarks production method (E-12a), not a hand-built RegistryEntry
// literal with Watermarks poked in directly.
func TestCubeMetricsParity_ResolutionCompletenessDecline(t *testing.T) {
	ctx := context.Background()
	tenant := "parity-completeness-tenant"
	objStore := &fakeSchedObjectStore{}
	reg := blockpack.NewCubeRegistry(objStore, tenant)

	idHex := blockpack.CubeComputeID(tenant, []string{"service.name"}, nil, []string{blockpack.CubeDurationColumn})
	entry := blockpack.CubeRegistryEntry{
		CubeID:     idHex,
		Tenant:     tenant,
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		// PARTIAL coverage: the query below asks for [0,100], but the watermark only covers
		// [0,50].
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{1: {MinMinute: 0, MaxMinute: 50}},
		Resolution: 1,
		CreatedAt:  1000,
	}
	if err := reg.Add(ctx, entry); err != nil {
		t.Fatalf("reg.Add: %v", err)
	}

	entries, _, err := reg.Load(ctx)
	if err != nil {
		t.Fatalf("reg.Load: %v", err)
	}
	router := blockpack.NewCubeQueryRouter(entries)

	// A query window [0,100] is NOT fully covered by the watermark [0,50] — must decline the
	// WHOLE query, never serve a partial/mixed-resolution answer.
	res, err := router.Route(tenant, []string{"service.name"}, nil, "", 1, 0, 100)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}
	if res.Found {
		t.Fatal("Route must decline when the watermark does not fully cover the requested window")
	}

	// Positive control: extend the watermark via the REAL Registry.UpdateWatermarks production
	// method (E-12a — the same method Compactor.Execute calls on every successful rollup write)
	// to fully cover [0,100], then re-route. This must now succeed, proving the decline above was
	// genuinely caused by incomplete coverage and not some unrelated bug.
	if err := reg.UpdateWatermarks(ctx, idHex, 1, 51, 100); err != nil {
		t.Fatalf("UpdateWatermarks: %v", err)
	}
	entries, _, err = reg.Load(ctx)
	if err != nil {
		t.Fatalf("reg.Load (after UpdateWatermarks): %v", err)
	}
	router = blockpack.NewCubeQueryRouter(entries)
	res, err = router.Route(tenant, []string{"service.name"}, nil, "", 1, 0, 100)
	if err != nil {
		t.Fatalf("Route (after watermark extension): %v", err)
	}
	if !res.Found {
		t.Fatal("Route must succeed once the watermark fully covers the requested window")
	}
	if res.Entry.CubeID != idHex {
		t.Fatalf("Route must select the registered cube, got %q, want %q", res.Entry.CubeID, idHex)
	}
}

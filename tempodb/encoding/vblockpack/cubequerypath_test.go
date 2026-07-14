package vblockpack

import (
	"context"
	"errors"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcepbv1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tracepbv1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/stretchr/testify/require"
)

// TestExtractFilters_EmptyPredicate: an empty {} query yields no filters but is a valid
// (distinct) cube identity — issue #480 decision (2): unfiltered cubes are their own kind.
func TestExtractFilters_EmptyPredicate(t *testing.T) {
	filters, ok := extractFilters(`{} | rate() by (span.http.request.method)`)
	if !ok {
		t.Fatal("empty predicate must be cube-representable")
	}
	if len(filters) != 0 {
		t.Fatalf("empty predicate must yield zero filters, got %d: %v", len(filters), filters)
	}
}

// TestExtractFilters_SimpleEquality: a single equality predicate maps to one EQ filter.
func TestExtractFilters_SimpleEquality(t *testing.T) {
	filters, ok := extractFilters(`{span.kind = server} | rate() by (span.http.request.method)`)
	if !ok {
		t.Fatal("simple equality must be cube-representable")
	}
	if len(filters) != 1 {
		t.Fatalf("want 1 filter, got %d: %v", len(filters), filters)
	}
	f := filters[0]
	if f.Column != "span.kind" {
		t.Fatalf("column = %q, want span.kind", f.Column)
	}
	if f.Op != blockpack.CubeDefFilterOpEQ {
		t.Fatalf("op = %q, want EQ", f.Op)
	}
	if v, _ := f.Value.(string); v != "server" {
		t.Fatalf("value = %v, want server", f.Value)
	}
}

// TestExtractFilters_NumericComparison: duration > 300ms maps to a GT filter.
func TestExtractFilters_NumericComparison(t *testing.T) {
	filters, ok := extractFilters(`{duration > 300ms} | rate() by (resource.service.name)`)
	if !ok {
		t.Fatal("numeric comparison must be cube-representable")
	}
	if len(filters) != 1 {
		t.Fatalf("want 1 filter, got %d: %v", len(filters), filters)
	}
	if filters[0].Op != blockpack.CubeDefFilterOpGT {
		t.Fatalf("op = %q, want GT", filters[0].Op)
	}
	if filters[0].Column != "duration" {
		t.Fatalf("column = %q, want duration", filters[0].Column)
	}
}

// TestExtractFilters_Conjunction: multiple AND predicates are all captured and sorted
// deterministically (column-then-op-then-value) regardless of query ordering.
func TestExtractFilters_Conjunction(t *testing.T) {
	a, ok := extractFilters(`{span.kind = server && duration > 300ms} | rate() by (name)`)
	if !ok {
		t.Fatal("conjunction must be cube-representable")
	}
	b, ok := extractFilters(`{duration > 300ms && span.kind = server} | rate() by (name)`)
	if !ok {
		t.Fatal("reordered conjunction must be cube-representable")
	}
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("want 2 filters each, got %d / %d", len(a), len(b))
	}
	// Canonical ordering makes the two orderings produce identical routing keys.
	if filterDedupKey(a) != filterDedupKey(b) {
		t.Fatalf("reordered conjunction must canonicalize identically: %q vs %q",
			filterDedupKey(a), filterDedupKey(b))
	}
}

// TestExtractFilters_UnsupportedOps: predicates a cube cannot faithfully bake in must
// force a fall-back (ok=false) rather than route to a filter-mismatched cube — the core
// of the #480 fix.
func TestExtractFilters_UnsupportedOps(t *testing.T) {
	cases := []string{
		`{name =~ "foo.*"} | rate() by (name)`,                   // regex
		`{span.kind != server} | rate() by (name)`,               // not-equal
		`{span.kind = server || status = error} | rate() by (n)`, // OR (not a conjunction)
	}
	for _, q := range cases {
		if _, ok := extractFilters(q); ok {
			t.Fatalf("query %q must NOT be cube-representable", q)
		}
	}
}

// TestExtractFilters_DistinctRoutingKeys: the whole point of #480 — same group-by dims,
// different filters must produce DIFFERENT cube IDs (via the canonical filter set), and
// the unfiltered variant must differ from every filtered variant.
func TestExtractFilters_DistinctRoutingKeys(t *testing.T) {
	tenant := "t"
	dims := []string{"span.http.request.method"}

	serverFilters, ok := extractFilters(`{span.kind = server} | rate() by (span.http.request.method)`)
	if !ok {
		t.Fatal("server filter must be representable")
	}
	errorFilters, ok := extractFilters(`{status = error} | rate() by (span.http.request.method)`)
	if !ok {
		t.Fatal("error filter must be representable")
	}
	noFilters, ok := extractFilters(`{} | rate() by (span.http.request.method)`)
	if !ok {
		t.Fatal("empty filter must be representable")
	}

	serverID := blockpack.CubeComputeID(tenant, dims, serverFilters, nil)
	errorID := blockpack.CubeComputeID(tenant, dims, errorFilters, nil)
	noID := blockpack.CubeComputeID(tenant, dims, noFilters, nil)

	if serverID == errorID {
		t.Fatal("server-filtered and error-filtered cubes must have distinct IDs")
	}
	if serverID == noID || errorID == noID {
		t.Fatal("filtered cubes must differ from the unfiltered cube")
	}

	// End-to-end via the router: a registry holding only the server cube must NOT answer
	// the error-filtered or unfiltered variant.
	serverEntry := blockpack.CubeRegistryEntry{
		CubeID:     serverID,
		Tenant:     tenant,
		Dimensions: dims,
		Filters:    serverFilters,
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{
			1: {MinMinute: 0, MaxMinute: 1000},
		},
		Resolution: 1,
	}
	router := blockpack.NewCubeQueryRouter([]blockpack.CubeRegistryEntry{serverEntry})

	if res, _ := router.Route(tenant, dims, errorFilters, "", 1, 0, 1000); res.Found {
		t.Fatal("error-filtered query must not route to the server-filtered cube")
	}
	if res, _ := router.Route(tenant, dims, noFilters, "", 1, 0, 1000); res.Found {
		t.Fatal("unfiltered query must not route to the server-filtered cube")
	}
	if res, _ := router.Route(tenant, dims, serverFilters, "", 1, 0, 1000); !res.Found {
		t.Fatal("server-filtered query must route to its own cube")
	}
}

// TestExtractAggAttr_SumOverTime_ExtractsFieldAndType (#491, E-10): the target attribute and its
// query-text-only type heuristic (duration -> Int64, anything else -> Float64, per ruling 1's
// safe-default rationale) are both extracted correctly.
func TestExtractAggAttr_SumOverTime_ExtractsFieldAndType(t *testing.T) {
	col, aggType, ok := extractAggAttr("{} | sum_over_time(span:duration) by (service.name)")
	if !ok {
		t.Fatal("sum_over_time must extract an attribute")
	}
	if col != blockpack.CubeDurationColumn {
		t.Fatalf("col = %q, want %q", col, blockpack.CubeDurationColumn)
	}
	if aggType != blockpack.CubeAggAttrTypeInt64 {
		t.Fatalf("aggType = %v, want CubeAggAttrTypeInt64 for duration", aggType)
	}

	col, aggType, ok = extractAggAttr("{} | sum_over_time(span.request.size) by (service.name)")
	if !ok {
		t.Fatal("sum_over_time must extract an attribute")
	}
	if col != "span.request.size" {
		t.Fatalf("col = %q, want %q", col, "span.request.size")
	}
	if aggType != blockpack.CubeAggAttrTypeFloat64 {
		t.Fatalf("aggType = %v, want CubeAggAttrTypeFloat64 for a non-duration attribute", aggType)
	}

	// quantile_over_time's second argument (the percentile) must NOT be captured.
	col, _, ok = extractAggAttr("{} | quantile_over_time(span:duration, .5) by (service.name)")
	if !ok || col != blockpack.CubeDurationColumn {
		t.Fatalf("quantile_over_time: col = %q ok=%v, want %q true", col, ok, blockpack.CubeDurationColumn)
	}
}

// TestExtractAggAttr_CountOverTime_ReturnsOkFalseNoAggAttrNeeded (#491, E-10): count_over_time
// and rate need no specific attribute — Route's own neededAttr=="" convention matches any
// candidate cube regardless of its AggAttrs set.
func TestExtractAggAttr_CountOverTime_ReturnsOkFalseNoAggAttrNeeded(t *testing.T) {
	if _, _, ok := extractAggAttr("{} | count_over_time() by (service.name)"); ok {
		t.Fatal("count_over_time must not extract an attribute")
	}
	if _, _, ok := extractAggAttr("{} | rate() by (service.name)"); ok {
		t.Fatal("rate must not extract an attribute")
	}
}

// TestExtractAggAttr_MinMaxAvgHistogramOverTime_AllExtractCorrectly closes a coverage gap: only
// sum_over_time and quantile_over_time were previously exercised by name against aggFuncArgRe's
// shared alternation, even though min_over_time/max_over_time/avg_over_time/histogram_over_time
// share the exact same regex with no per-function branching. No live bug (verified functionally
// correct via a standalone regex probe during review) — this pins that correctness explicitly,
// per this phase's "every branch verified, not spot-checked" convention.
func TestExtractAggAttr_MinMaxAvgHistogramOverTime_AllExtractCorrectly(t *testing.T) {
	cases := []struct {
		function string
		query    string
	}{
		{"min_over_time", "{} | min_over_time(span:duration) by (service.name)"},
		{"max_over_time", "{} | max_over_time(span:duration) by (service.name)"},
		{"avg_over_time", "{} | avg_over_time(span:duration) by (service.name)"},
		{"histogram_over_time", "{} | histogram_over_time(span:duration) by (service.name)"},
	}
	for _, tc := range cases {
		t.Run(tc.function, func(t *testing.T) {
			col, aggType, ok := extractAggAttr(tc.query)
			if !ok {
				t.Fatalf("%s must extract an attribute", tc.function)
			}
			if col != blockpack.CubeDurationColumn {
				t.Fatalf("%s: col = %q, want %q", tc.function, col, blockpack.CubeDurationColumn)
			}
			if aggType != blockpack.CubeAggAttrTypeInt64 {
				t.Fatalf("%s: aggType = %v, want CubeAggAttrTypeInt64 for duration", tc.function, aggType)
			}
		})
	}
}

// TestMaybeCreateCube_AlwaysIncludesDurationInAggAttrs (#491, E-10, mutation-verification
// required): buildAggAttrs must include duration UNCONDITIONALLY (ruling 3), regardless of
// whether the triggering query needed a specific attribute or not.
func TestMaybeCreateCube_AlwaysIncludesDurationInAggAttrs(t *testing.T) {
	cases := []struct {
		name           string
		neededAttr     string
		neededAttrType blockpack.CubeAggAttrType
		neededAttrOK   bool
	}{
		{"count_over_time (no needed attr)", "", blockpack.CubeAggAttrTypeFloat64, false},
		{"sum_over_time(duration) — must not duplicate", blockpack.CubeDurationColumn, blockpack.CubeAggAttrTypeInt64, true},
		{"sum_over_time(other attr)", "span.request.size", blockpack.CubeAggAttrTypeFloat64, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			aggAttrs := buildAggAttrs(tc.neededAttr, tc.neededAttrType, tc.neededAttrOK)
			found := false
			for _, a := range aggAttrs {
				if a.Column == blockpack.CubeDurationColumn {
					found = true
				}
			}
			if !found {
				t.Fatalf("buildAggAttrs(%q, _, %v) = %v, must always include duration", tc.neededAttr, tc.neededAttrOK, aggAttrs)
			}
			// The duration-needed case must not produce two entries for the same column.
			if tc.neededAttrOK && tc.neededAttr == blockpack.CubeDurationColumn && len(aggAttrs) != 1 {
				t.Fatalf("buildAggAttrs must not duplicate duration, got %v", aggAttrs)
			}
		})
	}
}

// TestTryQueryFromCube_RoutesWithRequestedResolution_NotHardcodedOne (#491, E-10): the router is
// given the query's OWN requested granularity (converted from Step), not a hardcoded 1 (always
// L0) — the bug this task fixes.
func TestTryQueryFromCube_RoutesWithRequestedResolution_NotHardcodedOne(t *testing.T) {
	cases := []struct {
		step time.Duration
		want uint32
	}{
		{30 * time.Second, 0},  // sub-minute step -> Route itself defaults 0 to 1 (L0)
		{time.Minute, 1},       // exactly 1 minute
		{time.Hour, 60},        // 1 hour -> L1 candidate
		{24 * time.Hour, 1440}, // 1 day -> L2 candidate
		{25 * time.Hour, 1500}, // non-round value: must not silently clamp to a rollup level here (that's ResolutionLevel's job, not this conversion's)
	}
	for _, tc := range cases {
		got := requestedResolutionMinutes(uint64(tc.step.Nanoseconds())) //nolint:gosec // test durations are all positive
		if got != tc.want {
			t.Errorf("requestedResolutionMinutes(%v) = %d, want %d", tc.step, got, tc.want)
		}
	}
}

// realCubeReaderWithNAggAttrs builds a REAL, minimal cube file via the production write path
// (Accumulator.Add -> Encode -> OpenCubeReaderFromBytes, through the REAL tempoSpanValues OTLP
// adapter) with exactly n aggAttrs — never a hand-built Cell/AggCell literal (Lesson 2). n must
// be >= 1 (duration is always present).
func realCubeReaderWithNAggAttrs(t *testing.T, n int) *blockpack.CubeReader {
	t.Helper()
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}
	for i := 1; i < n; i++ {
		aggAttrs = append(aggAttrs, blockpack.CubeAggAttrDef{
			Column: "extra" + string(rune('0'+i)),
			Type:   blockpack.CubeAggAttrTypeFloat64,
		})
	}
	def := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__",
		AggAttrs:   aggAttrs,
		ID:         [16]byte{0x01},
		Resolution: 1,
	}
	acc, err := blockpack.NewCubeAccumulator(def, 100)
	if err != nil {
		t.Fatalf("NewCubeAccumulator: %v", err)
	}
	sv := &tempoSpanValues{
		span: &tracepbv1.Span{StartTimeUnixNano: 0, EndTimeUnixNano: 1_000_000_000},
		resource: &resourcepbv1.Resource{
			Attributes: []*commonpbv1.KeyValue{
				{Key: "service.name", Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: "svc-a"}}},
			},
		},
	}
	if _, addErr := acc.Add(sv); addErr != nil {
		t.Fatalf("Add: %v", addErr)
	}
	data, err := acc.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	reader, err := blockpack.OpenCubeReaderFromBytes(data)
	if err != nil {
		t.Fatalf("OpenCubeReaderFromBytes: %v", err)
	}
	return reader
}

// realCubeReaderAtMinute builds a REAL, minimal cube file (production write path, Lesson 2) with
// exactly one duration aggAttr and one cell stamped at minute — used to prove which resolution
// CubeRollup was actually invoked with, since MergedCell.Minute is rebucketed to the TARGET
// resolution's bucket start (rollup.go's own documented contract), not left at the input's minute.
func realCubeReaderAtMinute(t *testing.T, minute uint32) *blockpack.CubeReader {
	t.Helper()
	def := blockpack.CubeDefinition{
		Dim1Column: "service.name",
		Dim2Column: "__all__",
		AggAttrs:   []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}},
		ID:         [16]byte{0x02},
		Resolution: 1,
	}
	acc, err := blockpack.NewCubeAccumulator(def, minute)
	if err != nil {
		t.Fatalf("NewCubeAccumulator: %v", err)
	}
	sv := &tempoSpanValues{
		span: &tracepbv1.Span{StartTimeUnixNano: 0, EndTimeUnixNano: 1_000_000_000},
		resource: &resourcepbv1.Resource{
			Attributes: []*commonpbv1.KeyValue{
				{Key: "service.name", Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: "svc-a"}}},
			},
		},
	}
	if _, addErr := acc.Add(sv); addErr != nil {
		t.Fatalf("Add: %v", addErr)
	}
	data, err := acc.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	reader, err := blockpack.OpenCubeReaderFromBytes(data)
	if err != nil {
		t.Fatalf("OpenCubeReaderFromBytes: %v", err)
	}
	return reader
}

// TestRollupCubeInputs_UsesRoutedResolution_NotHardcodedOne (#491, E-10 Gap B, mutation-
// verification required): rollupCubeInputs must call CubeRollup with result.Resolution — the
// resolution Route actually decided — never a hardcoded 1 (always L0). Distinguishing signal: a
// cell written at minute 65 rebuckets to minute 60 (the hour start) ONLY if CubeRollup is invoked
// with resolution=60; a hardcoded resolution=1 would leave it at its own minute, 65. A regression
// reverting to a hardcoded 1 would pass every OTHER existing test (none of them observe this call
// boundary) while silently serving every query at L0 regardless of what Route decided.
func TestRollupCubeInputs_UsesRoutedResolution_NotHardcodedOne(t *testing.T) {
	reader := realCubeReaderAtMinute(t, 65)
	inputs := []blockpack.CubeRollupInput{blockpack.CubeNewRollupInput(reader)}
	result := blockpack.CubeRoutingResult{Resolution: 60}

	cells, err := rollupCubeInputs(inputs, result, 60, 119)
	if err != nil {
		t.Fatalf("rollupCubeInputs: %v", err)
	}
	if len(cells) != 1 {
		t.Fatalf("expected 1 merged cell, got %d", len(cells))
	}
	if cells[0].Minute != 60 {
		t.Fatalf("cell.Minute = %d, want 60 (hour start) — CubeRollup was not invoked with the "+
			"routed resolution (60); a hardcoded resolution=1 would have left it at 65", cells[0].Minute)
	}
}

// TestTryQueryFromCube_ExcludesFileOnAggAttrsMismatch (#491, E-10/APPENDIX 3, mutation-
// verification required): a real cube file whose declared NumAggAttrs disagrees with its
// RegistryEntry's AggAttrs count must classify as NOT ok — the caller excludes it from
// CubeRollup's inputs.
func TestTryQueryFromCube_ExcludesFileOnAggAttrsMismatch(t *testing.T) {
	reader := realCubeReaderWithNAggAttrs(t, 1) // file declares 1 aggAttr (duration only)
	mismatchedEntry := blockpack.CubeRegistryEntry{
		CubeID:   "abc",
		AggAttrs: []string{blockpack.CubeDurationColumn, "extra1"}, // registry expects 2
	}
	ok, err := classifyCubeFile(reader, mismatchedEntry)
	if ok {
		t.Fatal("classifyCubeFile must return ok=false for a mismatched aggAttrs count")
	}
	if err == nil {
		t.Fatal("classifyCubeFile must return a non-nil error for a mismatch")
	}
}

// TestCubeCoveredWindow_RealRouterResult (#217/SPEC-CUBE-028, Phase 3.3) drives
// cubeCoveredWindow/cubePartialCoverageMessage against a REAL router.Route result over a real
// CubeRegistry round trip (mirroring TestCubeMetricsParity_ResolutionCompletenessDecline's own
// real-Registry style in cube_metrics_parity_test.go) — extracted the same way
// rollupCubeInputs was, because tryQueryFromCube's S3/minio file-listing wiring is otherwise
// untestable in isolation (no minio test double exists in this codebase for listObjects/
// getObject, only for the registry store seam).
func TestCubeCoveredWindow_RealRouterResult(t *testing.T) {
	ctx := context.Background()
	tenant := "cube-covered-window-tenant"
	objStore := &fakeSchedObjectStore{}
	reg := blockpack.NewCubeRegistry(objStore, tenant)

	idHex := blockpack.CubeComputeID(tenant, []string{"service.name"}, nil, []string{blockpack.CubeDurationColumn})
	entry := blockpack.CubeRegistryEntry{
		CubeID:     idHex,
		Tenant:     tenant,
		Dimensions: []string{"service.name"},
		AggAttrs:   []string{blockpack.CubeDurationColumn},
		Watermarks: map[uint32]blockpack.CubeResolutionWatermark{1: {MinMinute: 10, MaxMinute: 50}},
		Resolution: 1,
	}
	require.NoError(t, reg.Add(ctx, entry))
	entries, _, err := reg.Load(ctx)
	require.NoError(t, err)
	router := blockpack.NewCubeQueryRouter(entries)

	t.Run("right-edge partial", func(t *testing.T) {
		result, routeErr := router.Route(tenant, []string{"service.name"}, nil, "", 1, 0, 100)
		require.NoError(t, routeErr)
		require.True(t, result.Found)

		minM, maxM, partial := cubeCoveredWindow(result, 0, 100)
		require.True(t, partial)
		require.Equal(t, uint32(10), minM)
		require.Equal(t, uint32(50), maxM)

		msg := cubePartialCoverageMessage(minM, maxM, 0, 100)
		require.Contains(t, msg, "[10,50]")
		require.Contains(t, msg, "[0,100]")
	})

	t.Run("full coverage", func(t *testing.T) {
		result, routeErr := router.Route(tenant, []string{"service.name"}, nil, "", 1, 10, 50)
		require.NoError(t, routeErr)
		require.True(t, result.Found)

		minM, maxM, partial := cubeCoveredWindow(result, 10, 50)
		require.False(t, partial, "requesting exactly the covered window must not be partial")
		require.Equal(t, uint32(10), minM)
		require.Equal(t, uint32(50), maxM)
	})
}

// TestTryQueryFromCube_AggAttrsMismatchIsDistinguishableFromMissingFile (#491, E-10/APPENDIX 3):
// a genuine mismatch is a *CubeAggAttrsMismatchError specifically — never conflated with (or
// indistinguishable from) a routine missing/undecodable file, which is a categorically earlier
// code path that never even calls classifyCubeFile.
func TestTryQueryFromCube_AggAttrsMismatchIsDistinguishableFromMissingFile(t *testing.T) {
	reader := realCubeReaderWithNAggAttrs(t, 1)

	matchingEntry := blockpack.CubeRegistryEntry{CubeID: "abc", AggAttrs: []string{blockpack.CubeDurationColumn}}
	if ok, err := classifyCubeFile(reader, matchingEntry); !ok || err != nil {
		t.Fatalf("matching file must classify ok with no error, got ok=%v err=%v", ok, err)
	}

	mismatchedEntry := blockpack.CubeRegistryEntry{CubeID: "abc", AggAttrs: []string{blockpack.CubeDurationColumn, "extra1"}}
	ok, err := classifyCubeFile(reader, mismatchedEntry)
	if ok {
		t.Fatal("mismatched file must not classify ok")
	}
	var mismatchErr *blockpack.CubeAggAttrsMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Fatalf("mismatch must be a *CubeAggAttrsMismatchError specifically, got %T: %v", err, err)
	}
}

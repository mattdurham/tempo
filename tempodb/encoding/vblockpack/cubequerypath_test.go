package vblockpack

// cubequerypath_test.go — #508 Phase 14: the buildAggAttrs/requestedResolutionMinutes/
// rollupCubeInputs/classifyCubeFile/cubeCoveredWindow unit tests that used to live below this
// file's extractFilters/extractAggAttr tests moved into blockpack's own
// cube_query_path_internal_test.go (those functions moved there in Phases 4/5) --
// requestedResolutionMinutes itself no longer exists anywhere as a named function (inlined as a
// one-line conversion in tryQueryFromCube, since it has exactly one caller and CubeQueryPathRequest
// now carries the pre-computed value). extractFilters/extractAggAttr below are UNCHANGED
// tempo-side TraceQL-string-parsing functions, still fully covered here.

import (
	"testing"

	blockpack "github.com/grafana/blockpack"
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

package vblockpack

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

	serverID := blockpack.CubeComputeID(tenant, dims, serverFilters)
	errorID := blockpack.CubeComputeID(tenant, dims, errorFilters)
	noID := blockpack.CubeComputeID(tenant, dims, noFilters)

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
		Resolution: 1,
	}
	router := blockpack.NewCubeQueryRouter([]blockpack.CubeRegistryEntry{serverEntry})

	if res, _ := router.Route(tenant, dims, errorFilters, 1, 0, 1000); res.Found {
		t.Fatal("error-filtered query must not route to the server-filtered cube")
	}
	if res, _ := router.Route(tenant, dims, noFilters, 1, 0, 1000); res.Found {
		t.Fatal("unfiltered query must not route to the server-filtered cube")
	}
	if res, _ := router.Route(tenant, dims, serverFilters, 1, 0, 1000); !res.Found {
		t.Fatal("server-filtered query must route to its own cube")
	}
}

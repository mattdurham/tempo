package cube

// NOTE: SPEC-CUBE-017 — QueryRouter selects the optimal cube + rollup level for a metrics
// query. Amended by SPEC-CUBE-023/E-6b (ruling 4(b)): Route no longer reports a partial-coverage
// window for the caller to stitch with a value-index fallback — it verifies the chosen
// resolution's watermark COMPLETELY covers the requested window and returns Found=false (decline
// the whole query) otherwise. There is no partial/mixed-resolution answer.

import (
	"fmt"
)

// ResolutionLevel selects a rollup level based on a requested resolution in minutes.
// Returns the coarsest level whose granularity is ≤ the requested resolution.
// Follows the routing table from the ticket:
//
//	resolution ≥ 1440 → L2 (if data exists); else L1 (if ≥ 60); else L0.
func ResolutionLevel(requestedMinutes uint32) uint32 {
	switch {
	case requestedMinutes >= uint32(RollupL2):
		return uint32(RollupL2)
	case requestedMinutes >= uint32(RollupL1):
		return uint32(RollupL1)
	default:
		return uint32(RollupL0)
	}
}

// RoutingResult is the outcome of a QueryRouter.Route call.
type RoutingResult struct {
	// Entry is the matching cube definition, or zero if no cube was found.
	Entry RegistryEntry
	// Found reports whether a matching cube exists in the registry.
	Found bool
	// Resolution is the minutes-per-bucket the router selected (1, 60, or 1440).
	Resolution uint32
}

// QueryRouter resolves a (tenant, dims, filters, resolution) request to the best
// available cube entry and routing metadata. It does not perform any I/O — the
// caller provides the loaded registry snapshot.
type QueryRouter struct {
	cubes []RegistryEntry
}

// NewQueryRouter creates a router from a pre-loaded registry snapshot.
func NewQueryRouter(cubes []RegistryEntry) *QueryRouter {
	return &QueryRouter{cubes: cubes}
}

// Route finds the best matching cube for (tenant, dims, filters, neededAttr,
// requestedResolution). A cube matches when it has the same tenant, the same (sorted)
// dimensions, and the same filters (computeDimsFiltersKey — the SAME 3-segment hash
// ComputeCubeID uses, ruling 5's single-source-of-truth pairing) AND, if neededAttr is
// non-empty, has that attribute in its AggAttrs set. neededAttr == "" matches any candidate
// regardless of its attribute set (count_over_time/rate need no specific attribute).
//
// Superset tie-break (ruling 3's amendment): when multiple registered cubes share
// (dims, filters) but differ in their AggAttrs set (e.g. one tracks {duration}, a later one
// tracks {duration, http.status_code}), Route deterministically prefers the SMALLEST set that
// still covers neededAttr, breaking ties by newest CreatedAt.
//
// Resolution-completeness-or-decline (ruling 4(b)): once a target resolution is chosen (the
// coarsest level whose granularity is ≤ requestedResolution, via ResolutionLevel), Route
// verifies the chosen cube's Watermarks[level] COMPLETELY covers [watermarkMinute,
// queryMaxMinute] before returning Found=true. Incomplete coverage declines the WHOLE query —
// never a partial/mixed-resolution answer — so the caller falls back to the value index.
func (r *QueryRouter) Route(
	tenant string,
	dims []string,
	filters []ColumnFilter,
	neededAttr string,
	requestedResolution uint32,
	watermarkMinute uint32,
	queryMaxMinute uint32,
) (RoutingResult, error) {
	if requestedResolution == 0 {
		requestedResolution = 1
	}
	key := computeDimsFiltersKey(tenant, dims, filters)

	var candidates []RegistryEntry
	for _, c := range r.cubes {
		if computeDimsFiltersKey(c.Tenant, c.Dimensions, c.Filters) != key {
			continue
		}
		if neededAttr != "" && !containsString(c.AggAttrs, neededAttr) {
			continue
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return RoutingResult{Found: false}, nil
	}
	best := smallestSupersetNewest(candidates)

	level := ResolutionLevel(requestedResolution)
	wm, ok := best.Watermarks[level]
	if !ok || wm.MinMinute > watermarkMinute || wm.MaxMinute < queryMaxMinute {
		// Incomplete (or entirely absent) coverage at the chosen resolution — decline the
		// whole query rather than serve a partial/mixed-resolution answer.
		return RoutingResult{Found: false}, nil
	}

	return RoutingResult{
		Entry:      best,
		Found:      true,
		Resolution: level,
	}, nil
}

// smallestSupersetNewest picks the candidate with the FEWEST AggAttrs (the smallest attribute
// set among candidates that already matched dims+filters+neededAttr), breaking ties by newest
// CreatedAt. Candidates must be non-empty.
func smallestSupersetNewest(candidates []RegistryEntry) RegistryEntry {
	best := candidates[0]
	for _, c := range candidates[1:] {
		if len(c.AggAttrs) < len(best.AggAttrs) ||
			(len(c.AggAttrs) == len(best.AggAttrs) && c.CreatedAt > best.CreatedAt) {
			best = c
		}
	}
	return best
}

// containsString reports whether s is present in slice.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// ErrNoMatchingCube is returned when no cube matches the query pattern.
var ErrNoMatchingCube = fmt.Errorf("cube router: no matching cube for query pattern")

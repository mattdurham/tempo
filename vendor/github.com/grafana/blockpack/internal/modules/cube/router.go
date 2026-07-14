package cube

// NOTE: SPEC-CUBE-017 — QueryRouter selects the optimal cube + rollup level for a metrics
// query. Amended by SPEC-CUBE-028/#217 (ruling 4(b) revisit): Route now reports the actual
// covered sub-range (CoveredMinMinute/CoveredMaxMinute) instead of declining the whole query on
// any incomplete watermark coverage. Found=false is now reserved for "no usable overlap at all"
// (no watermark entry for the chosen level, or the watermark's covered range doesn't overlap the
// query window). Edge-truncated partial coverage (the common backfill-in-progress shape) is
// served for its covered sub-range; the caller is responsible for falling back to the value
// index for the uncovered edge(s) (see cubequerypath.go). A genuine INTERIOR gap in the cube's
// own coverage (covered, then a hole, then covered again) is NOT representable by the single
// [MinMinute,MaxMinute] ResolutionWatermark pair and remains out of scope — see NOTE-CUBE-026.

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
	// Found reports whether a matching cube exists in the registry AND its chosen resolution
	// level has at least some overlap with the requested window.
	Found bool
	// Resolution is the minutes-per-bucket the router selected (1, 60, or 1440).
	Resolution uint32
	// CoveredMinMinute/CoveredMaxMinute (#217/SPEC-CUBE-028) report the actual sub-range of the
	// requested [watermarkMinute, queryMaxMinute] window this entry's chosen resolution level
	// covers. Only meaningful when Found is true. Equal to the full requested window when
	// coverage is complete, so callers have one code path regardless of whether coverage is
	// partial or complete.
	CoveredMinMinute uint32
	CoveredMaxMinute uint32
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
// Resolution-completeness-or-partial (ruling 4(b) revisit, #217/SPEC-CUBE-028): once a target
// resolution is chosen (the coarsest level whose granularity is ≤ requestedResolution, via
// ResolutionLevel), Route computes the overlap of the chosen cube's Watermarks[level] with
// [watermarkMinute, queryMaxMinute]. A non-empty overlap returns Found=true with
// CoveredMinMinute/CoveredMaxMinute set to that overlap (the full window when coverage is
// complete); the caller is responsible for covering any uncovered edge via the value-index
// fallback. Only a chosen level with NO watermark entry at all, or one whose watermark doesn't
// overlap the query window at all, returns Found=false.
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
	if !ok {
		// No watermark recorded at all for this level — nothing to serve.
		return RoutingResult{Found: false}, nil
	}

	coveredMin := wm.MinMinute
	if watermarkMinute > coveredMin {
		coveredMin = watermarkMinute
	}
	coveredMax := wm.MaxMinute
	if queryMaxMinute < coveredMax {
		coveredMax = queryMaxMinute
	}
	if coveredMin > coveredMax {
		// The watermark's covered range doesn't overlap the query window at all.
		return RoutingResult{Found: false}, nil
	}

	return RoutingResult{
		Entry:            best,
		Found:            true,
		Resolution:       level,
		CoveredMinMinute: coveredMin,
		CoveredMaxMinute: coveredMax,
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

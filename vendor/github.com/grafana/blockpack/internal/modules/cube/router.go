package cube

// NOTE: SPEC-CUBE-017 — QueryRouter selects the optimal cube + rollup level for a metrics
// query and reports the query window for which the cube covers data (≥ watermark). Callers
// stitch cube results with value-index fallback for minutes below the watermark.

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
	// CubeMinMinute is the earliest minute covered by the cube (≥ watermark).
	// Callers use the value-index fallback for minutes < CubeMinMinute.
	CubeMinMinute uint32
	// CubeMaxMinute is the latest minute covered (typically currentMinute-1).
	CubeMaxMinute uint32
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

// Route finds the best matching cube for (tenant, dims, filters, requestedResolution).
// A cube matches when it has the same tenant, the same (sorted) dimensions, and the
// same filters. If multiple cubes match, the first one is returned (cubes are
// non-overlapping by construction — same dims+filters → same CubeID).
//
// Returns a RoutingResult with Found=false when no cube matches; the caller falls
// back to the value index and may log the miss to trigger cube creation (#446).
func (r *QueryRouter) Route(
	tenant string,
	dims []string,
	filters []ColumnFilter,
	requestedResolution uint32,
	watermarkMinute uint32,
	queryMaxMinute uint32,
) (RoutingResult, error) {
	if requestedResolution == 0 {
		requestedResolution = 1
	}
	cubeID := ComputeCubeID(tenant, dims, filters)

	for _, c := range r.cubes {
		if c.CubeID != cubeID {
			continue
		}
		// Snap resolution to the coarsest available level.
		level := ResolutionLevel(requestedResolution)
		res := RoutingResult{
			Entry:         c,
			Found:         true,
			Resolution:    level,
			CubeMinMinute: watermarkMinute,
			CubeMaxMinute: queryMaxMinute,
		}
		return res, nil
	}

	return RoutingResult{Found: false}, nil
}

// ErrNoMatchingCube is returned when no cube matches the query pattern.
var ErrNoMatchingCube = fmt.Errorf("cube router: no matching cube for query pattern")

package executor

// NOTE-036: planBlocks unifies the block-selection pipeline across all query paths.
// See NOTES.md §NOTE-036.

import (
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// planBlocks runs the full block-selection pipeline for a query:
//  1. BuildPredicates — converts vm.Program predicates into planner predicates
//  2. PlanWithOptions — applies time-range filtering
//  3. fileLevelVectorPrune — VECTOR() centroid reject (when present)
//  4. BlocksFromIntrinsicTOC intersection — intrinsic-column fast reject (when available)
//
// NOTE-036: All query paths (Collect, ExecuteTraceMetrics) must use planBlocks to
// ensure intrinsic TOC pruning is active everywhere, not just in Collect.
func planBlocks(
	r *modules_reader.Reader,
	program *vm.Program,
	tr queryplanner.TimeRange,
	opts queryplanner.PlanOptions,
) *queryplanner.Plan {
	predicates := BuildPredicates(r, program)
	planner := queryplanner.NewPlanner(r)
	plan := planner.PlanWithOptions(predicates, tr, opts)

	// NOTE(#439): File-level range reject removed — the range index (bucketMin/bucketMax
	// per column) no longer exists in the data file. The value index is now the
	// authoritative source for value-based file/block pruning.
	//
	// NOTE: File-level bloom reject removed (2026-06-29, in-file block pruning removal).
	// Value index is now the authoritative source for pruning.

	// File-level and block-level vector centroid reject (VECTOR() predicates only).
	// If the file centroid is too distant from the query vector, skip the entire file.
	// If only some blocks are distant, prune those blocks.
	if program != nil && program.HasVector {
		plan.SelectedBlocks = fileLevelVectorPrune(r, program, plan.SelectedBlocks)
		if len(plan.SelectedBlocks) == 0 {
			plan.Explain = "file-level reject: vector centroid too distant"
			return plan
		}
	}

	// Intersect with intrinsic-column TOC when available.
	// Returns nil when no pruning is possible (no intrinsic section, no intrinsic
	// predicates, or all blocks survive), so we skip the intersection step in that case.
	if intrinsicBlocks := BlocksFromIntrinsicTOC(r, program); intrinsicBlocks != nil {
		beforeIntrinsic := len(plan.SelectedBlocks)
		keepSet := make(map[int]struct{}, len(intrinsicBlocks))
		for _, bi := range intrinsicBlocks {
			keepSet[bi] = struct{}{}
		}
		filtered := plan.SelectedBlocks[:0]
		for _, bi := range plan.SelectedBlocks {
			if _, ok := keepSet[bi]; ok {
				filtered = append(filtered, bi)
			}
		}
		plan.SelectedBlocks = filtered
		plan.PrunedByIntrinsicTOC = beforeIntrinsic - len(plan.SelectedBlocks) // NOTE-449
	}

	// NOTE: ColStats block pruning removed (2026-06-29, in-file block pruning removal).
	// Value index is now the authoritative source for pruning.

	return plan
}

// NOTE: pruneByColStats removed (2026-06-29, in-file block pruning removal).
// Value index is now the authoritative source for pruning.

// NOTE: colStatsRejects, colStatsRejectsNumeric, helper functions removed (2026-06-29).
// Value index is now the authoritative source for pruning.

// colStatsRejectsInt64 applies signed int64 range comparison against ColStats numeric bounds.
// stat.MinNum and stat.MaxNum store int64 bit patterns as uint64 (writer emits raw int64 LE).
// Casting back to int64 reconstructs the signed value correctly, including negative numbers.
//
// NOTE: colStatsRejectsInt64 removed (2026-06-29, in-file block pruning removal).

// NOTE: colStatsRejectsFloat64 removed (2026-06-29, in-file block pruning removal).

// fileLevelVectorPrune prunes blocks using VECTOR() centroid distances.
// If the file centroid is too distant (similarity < threshold), all blocks are pruned.
// Otherwise, blocks whose centroid is too distant are pruned individually.
// Returns the surviving block indices.
func fileLevelVectorPrune(r *modules_reader.Reader, program *vm.Program, selectedBlocks []int) []int {
	if !program.HasVector || len(program.QueryVector) == 0 {
		return selectedBlocks
	}

	vi, err := r.VectorIndex()
	if err != nil || vi == nil {
		// No vector index — cannot prune; keep all blocks.
		return selectedBlocks
	}

	// Determine threshold: use the minimum VectorThreshold from VECTOR() nodes.
	threshold := findVectorThreshold(program.Predicates)

	// File-level check: if the file centroid is too distant, skip all blocks.
	fileSim := float32(1.0) - vi.FileCentroidDistance(program.QueryVector)
	if fileSim < threshold {
		return nil
	}

	// Block-level check: prune blocks whose centroid is too distant.
	if len(vi.BlockCentroids) == 0 {
		return selectedBlocks
	}
	filtered := make([]int, 0, len(selectedBlocks))
	for _, bi := range selectedBlocks {
		if bi >= len(vi.BlockCentroids) {
			// No centroid for this block index — keep it.
			filtered = append(filtered, bi)
			continue
		}
		blockSim := float32(1.0) - vi.BlockCentroidDistance(bi, program.QueryVector)
		if blockSim >= threshold {
			filtered = append(filtered, bi)
		}
	}
	return filtered
}

// findVectorThreshold returns the minimum VectorThreshold from VECTOR() RangeNodes.
// Falls back to DefaultVectorThreshold if no node is found.
func findVectorThreshold(preds *vm.QueryPredicates) float32 {
	if preds == nil {
		return vm.DefaultVectorThreshold
	}
	threshold := findVectorThresholdNodes(preds.Nodes)
	if threshold == 0 {
		return vm.DefaultVectorThreshold
	}
	return threshold
}

func findVectorThresholdNodes(nodes []vm.RangeNode) float32 {
	for _, n := range nodes {
		if len(n.Children) > 0 {
			if t := findVectorThresholdNodes(n.Children); t > 0 {
				return t
			}
			continue
		}
		if len(n.QueryVector) > 0 && n.VectorThreshold > 0 {
			return n.VectorThreshold
		}
	}
	return 0
}

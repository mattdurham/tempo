package executor

// NOTE-036: planBlocks unifies the block-selection pipeline across all query paths.
// See NOTES.md §NOTE-036.

import (
	"math"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// planBlocks runs the full block-selection pipeline for a query:
//  1. BuildPredicates — converts vm.Program predicates into planner predicates
//  2. PlanWithOptions — applies bloom/range-index pruning and time range filtering
//  3. fileLevelReject — fast-reject the entire file when bucketMin/bucketMax guarantees no match
//  4. BlocksFromIntrinsicTOC intersection — intrinsic-column fast reject (when available)
//
// NOTE-036: All query paths (Collect, ExecuteTraceMetrics, ExecuteLogMetrics,
// StreamLogs, CollectLogs) must use planBlocks to ensure intrinsic TOC pruning
// is active everywhere, not just in Collect.
func planBlocks(
	r *modules_reader.Reader,
	program *vm.Program,
	tr queryplanner.TimeRange,
	opts queryplanner.PlanOptions,
) *queryplanner.Plan {
	predicates := BuildPredicates(r, program)
	planner := queryplanner.NewPlanner(r)
	plan := planner.PlanWithOptions(predicates, tr, opts)

	// File-level fast reject: if the query value is guaranteed outside the
	// file's [bucketMin, bucketMax] range, skip all remaining pruning.
	if program != nil && program.Predicates != nil {
		if fileLevelReject(r, program.Predicates.Nodes) {
			plan.SelectedBlocks = nil
			plan.Explain = "file-level reject: query value outside column [bucketMin, bucketMax]"
			plan.PrunedByIndex = 0
			plan.PrunedByTime = 0
			plan.PrunedByFuse = 0
			plan.PrunedByCMS = 0
			return plan
		}
	}

	// Intersect with intrinsic-column TOC when available.
	// Returns nil when no pruning is possible (no intrinsic section, no intrinsic
	// predicates, or all blocks survive), so we skip the intersection step in that case.
	if intrinsicBlocks := BlocksFromIntrinsicTOC(r, program); intrinsicBlocks != nil {
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
	}

	return plan
}

// fileLevelReject returns true if the AND-combined predicates in nodes guarantee
// that no span in the file can match — i.e. the file should be entirely skipped.
// NOTE-45: Uses RangeColumnBoundaries (bucketMin/bucketMax) for O(1) file rejection.
// It is conservative: it only rejects when a leaf node's range predicate is entirely
// outside the column's [bucketMin, bucketMax] range.
//
// AND semantics: reject if ANY leaf rejects.
// OR semantics: reject only if ALL children reject.
func fileLevelReject(r *modules_reader.Reader, nodes []vm.RangeNode) bool {
	for i := range nodes {
		if rejectByBoundary(r, &nodes[i]) {
			return true
		}
	}
	return false
}

// rejectByBoundary returns true if the node guarantees no match based on
// file-level bucket boundaries.
func rejectByBoundary(r *modules_reader.Reader, node *vm.RangeNode) bool {
	if len(node.Children) > 0 {
		// Composite node.
		if node.IsOR {
			// OR: reject only if ALL children reject.
			for i := range node.Children {
				if !rejectByBoundary(r, &node.Children[i]) {
					return false
				}
			}
			return true
		}
		// AND: reject if ANY child rejects.
		for i := range node.Children {
			if rejectByBoundary(r, &node.Children[i]) {
				return true
			}
		}
		return false
	}

	// Leaf node — only handle range predicates (Min/Max) for numeric columns.
	if node.Min == nil && node.Max == nil {
		return false // equality or regex — defer to block-level pruning
	}
	if node.Column == "" {
		return false
	}

	bounds := r.RangeColumnBoundaries(node.Column)
	if bounds == nil {
		return false
	}

	return rangeRejectsFile(bounds, node)
}

// rangeRejectsFile returns true when the predicate's interval is entirely outside
// the file's [bucketMin, bucketMax] range.
func rangeRejectsFile(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	switch bounds.ColType {
	case modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		return rejectInt64Range(bounds.BucketMin, bounds.BucketMax, node)
	case modules_shared.ColumnTypeRangeUint64:
		return rejectUint64Range(uint64(bounds.BucketMin), uint64(bounds.BucketMax), node) //nolint:gosec
	case modules_shared.ColumnTypeRangeFloat64:
		return rejectFloat64Range(bounds, node)
	}
	return false
}

// rejectInt64Range checks if a range predicate can be rejected for an int64 column.
func rejectInt64Range(fileMin, fileMax int64, node *vm.RangeNode) bool {
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToInt64(node.Min); ok {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToInt64(node.Max); ok {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToInt64(node.Min)
		queryMax, okMax := ptrValueToInt64(node.Max)
		if okMin && okMax {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// rejectUint64Range checks if a range predicate can be rejected for a uint64 column.
func rejectUint64Range(fileMin, fileMax uint64, node *vm.RangeNode) bool {
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToUint64(node.Min); ok {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToUint64(node.Max); ok {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToUint64(node.Min)
		queryMax, okMax := ptrValueToUint64(node.Max)
		if okMin && okMax {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// rejectFloat64Range checks if a range predicate can be rejected for a float64 column.
func rejectFloat64Range(bounds *modules_reader.RangeBoundaries, node *vm.RangeNode) bool {
	fileMin := math.Float64frombits(uint64(bounds.BucketMin)) //nolint:gosec
	fileMax := math.Float64frombits(uint64(bounds.BucketMax)) //nolint:gosec
	if math.IsNaN(fileMin) || math.IsNaN(fileMax) {
		return false
	}
	if node.Min != nil && node.Max == nil {
		if queryMin, ok := ptrValueToFloat64(node.Min); ok && !math.IsNaN(queryMin) {
			return queryMin > fileMax
		}
	}
	if node.Max != nil && node.Min == nil {
		if queryMax, ok := ptrValueToFloat64(node.Max); ok && !math.IsNaN(queryMax) {
			return queryMax < fileMin
		}
	}
	if node.Min != nil && node.Max != nil {
		queryMin, okMin := ptrValueToFloat64(node.Min)
		queryMax, okMax := ptrValueToFloat64(node.Max)
		if okMin && okMax && !math.IsNaN(queryMin) && !math.IsNaN(queryMax) {
			return queryMin > fileMax || queryMax < fileMin
		}
	}
	return false
}

// ptrValueToInt64 converts a *vm.Value to int64 for file-level boundary comparison.
// Returns false if the pointer is nil or the type cannot be converted.
func ptrValueToInt64(v *vm.Value) (int64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.Type {
	case vm.TypeInt:
		if i, ok := v.Data.(int64); ok {
			return i, true
		}
	case vm.TypeDuration:
		if i, ok := v.Data.(int64); ok {
			return i, true
		}
	}
	return 0, false
}

// ptrValueToUint64 converts a *vm.Value to uint64 for file-level boundary comparison.
// Returns false if the pointer is nil or the type cannot be converted.
func ptrValueToUint64(v *vm.Value) (uint64, bool) {
	if v == nil {
		return 0, false
	}
	switch v.Type {
	case vm.TypeInt:
		if i, ok := v.Data.(int64); ok {
			if i >= 0 {
				return uint64(i), true //nolint:gosec // safe: i >= 0
			}
		}
	case vm.TypeDuration:
		if i, ok := v.Data.(int64); ok {
			if i >= 0 {
				return uint64(i), true //nolint:gosec // safe: i >= 0
			}
		}
	}
	return 0, false
}

// ptrValueToFloat64 converts a *vm.Value to float64 for file-level boundary comparison.
// Returns false if the pointer is nil or the type cannot be converted.
func ptrValueToFloat64(v *vm.Value) (float64, bool) {
	if v == nil {
		return 0, false
	}
	if f, ok := v.Data.(float64); ok {
		return f, true
	}
	return 0, false
}

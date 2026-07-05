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
//
// NOTE-436: column-level block pruning is handled by the value-index pipeline
// upstream; there is no in-file intrinsic-TOC pruning step.
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

	// NOTE-436: intrinsic-TOC block pruning removed — there is no intrinsic section.
	// All column-level block pruning is now done by the value-index pipeline upstream.

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

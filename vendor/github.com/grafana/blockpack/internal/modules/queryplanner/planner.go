// Package queryplanner decides which blocks to fetch for a query.
//
// Responsibility boundary:
//   - queryplanner owns: which blocks to read (bloom filter pruning, range index pruning)
//   - blockio/reader owns: how to read them (coalescing, wire parsing)
//
// The planner depends on [BlockIndexer], not on the concrete reader type, so it
// can be used with any storage backend that satisfies the interface.
//
// # Usage
//
//	planner := queryplanner.NewPlanner(r)       // r implements BlockIndexer
//	plan    := planner.Plan(predicates)
//	rawBlocks, err := planner.FetchBlocks(plan) // map[blockIdx]rawBytes
//	// caller parses rawBlocks and evaluates spans
package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"slices"
)

// BlockIndexer is the interface queryplanner requires from its storage backend.
// reader.Reader satisfies this interface; any alternative backend may also implement it.

// BlockCount returns the total number of blocks in the file.

// BlockMeta returns the metadata for the block at blockIdx.
// Called for every candidate block during bloom-filter pruning.

// ReadBlocks reads raw bytes for the given block indices using aggressive
// coalescing. Returns a map from block index to raw byte slice.

// BlocksInTimeRange returns block indices whose timestamp window overlaps
// [minNano, maxNano] using the per-file TS index (O(log n) binary search).
// Returns nil when the TS index is absent (old files); callers must fall back
// to a full BlockMeta scan in that case.
// The returned slice is sorted in ascending blockID order.

// LogicalOp specifies the boolean operator used to combine a Predicate's children.
type LogicalOp uint8

const (
	// LogicalAND (default) combines children via intersection:
	// a block must satisfy ALL children to be kept.
	LogicalAND LogicalOp = 0

	// LogicalOR combines children via union:
	// a block satisfying ANY child is kept.
	LogicalOR LogicalOp = 1
)

// Predicate is a tree node describing the columns referenced by a query.
//
// Block pruning by predicate value was removed with the range index (#439); the
// value index is now the authoritative source for value-based pruning. A Predicate
// therefore only records the column-tree structure used by explain output.
//
// A Predicate is either a leaf or a composite node:
//
//   - Leaf (len(Children) == 0): Columns names the column(s) referenced.
//
//   - Composite (len(Children) > 0): Op specifies how Children are combined.
//     LogicalAND: block must satisfy ALL children (intersection).
//     LogicalOR:  block must satisfy AT LEAST ONE child (union).
//
// Examples:
//
//	// AND query { A && B }: two leaf predicates at the top level.
//	[]Predicate{{Columns: ["A"]}, {Columns: ["B"]}}
//
//	// OR query { A || B } different columns: composite OR node.
//	[]Predicate{{Op: LogicalOR, Children: []Predicate{
//	    {Columns: ["A"]}, {Columns: ["B"]},
//	}}}
//
//	// Mixed { A && (B || C) }: leaf A AND composite OR of B, C.
//	[]Predicate{
//	    {Columns: ["A"]},
//	    {Op: LogicalOR, Children: []Predicate{
//	        {Columns: ["B"]}, {Columns: ["C"]},
//	    }},
//	}

// Columns holds one or more column names referenced by a leaf node.
// Used only in leaf nodes (len(Children) == 0). An empty slice is a no-op.

// Children makes this a composite node. When non-empty, Columns is ignored and
// Op controls how the children combine.

// Op specifies how Children are combined. Ignored when len(Children) == 0.
// LogicalAND (default): block must satisfy all children.
// LogicalOR: block must satisfy at least one child.

// TimeRange is an optional time window for block-level pruning.
// A zero-value TimeRange (both fields 0) disables time-range pruning.

// inclusive lower bound (Unix nanoseconds); 0 means no lower bound
// inclusive upper bound (Unix nanoseconds); 0 means no upper bound

// Direction controls the order of blocks in Plan.SelectedBlocks.
type Direction uint8

const (
	// Forward returns blocks in ascending blockID order (oldest-first for time-sorted files).
	Forward Direction = 0
	// Backward returns blocks in descending blockID order (newest-first for time-sorted files).
	Backward Direction = 1
)

// PlanOptions are additional planning parameters that do not affect block selection
// but do affect block ordering and the Limit hint stored in the plan.

// Limit is an informational hint for the executor: stop after collecting this many
// results. 0 means no limit. Stored in Plan.Limit; the executor uses it for early
// termination when iterating SelectedBlocks in the plan's direction.

// Direction controls block ordering in SelectedBlocks. Default (zero) is Forward.

// EnableExplain controls whether Plan.Explain is populated.
// When false (the default), explainPlan is skipped entirely — no strings.Builder,
// fmt.Sprintf, or slice allocations are incurred.
// Set to true in debug/observability contexts where the explain output is consumed.
//
// NOTE-020: explain is opt-in to eliminate ~63% of queryplanner allocs in production.

// Plan is the output of a planning step: the block indices to read.

// Explain is an ASCII trace of how the predicate tree resolved to block sets.
// Empty by default. Populated only when PlanOptions.EnableExplain is true.
// File-level reject strings (set in plan_blocks.go) are always populated
// regardless of EnableExplain. See NOTE-020.
//
// BEHAVIOR CHANGE (NOTE-020): prior to this opt-in flag, Explain was always
// populated. Callers using Plan() or PlanWithOptions({}) without EnableExplain:true
// will now receive "". Production callers in stream.go and stream_log_topk.go store
// plan.Explain in QueryStats metadata — receiving "" is acceptable there (the field
// is informational). Pass EnableExplain:true in debug or observability contexts
// where the explain string is actually consumed.

// SelectedBlocks is a sorted slice of block indices to fetch.

// TotalBlocks is the total number of blocks in the file.

// PrunedByIndex is the number of blocks eliminated by range index lookups.

// PrunedByTime is the number of blocks eliminated by time-range comparison.

// PrunedByFuse is the number of blocks eliminated by BinaryFuse8 membership checks.

// Limit is the early-termination hint passed via PlanOptions.
// 0 means no limit (all selected blocks should be scanned).

// Direction is the ordering applied to SelectedBlocks (Forward or Backward).
// Set by PlanWithOptions; always Forward when Plan() is called directly.

// Planner selects candidate blocks for a query.
// It never performs I/O itself; I/O is delegated to the BlockIndexer via FetchBlocks.
type Planner struct {
	r BlockIndexer
}

// NewPlanner creates a Planner backed by the given BlockIndexer.
func NewPlanner(r BlockIndexer) *Planner {
	return &Planner{r: r}
}

// Plan returns the set of block indices to read for the given predicates and time range.
//
// Stage 0 (time-range): blocks whose time window does not overlap [timeRange.MinNano,
// timeRange.MaxNano] are eliminated. A zero TimeRange skips this stage.
//
// Stage 1 (column-presence bloom, issue #531): each predicate applies an OR bloom check
// across its Columns — a block is removed if ALL of a leaf's named columns are
// definitively absent per BlockIndexer.MayContainColumn. Multiple top-level predicates
// are ANDed: a block must survive every predicate (composite Children nest the same
// AND/OR semantics recursively). This is column-PRESENCE pruning only — value-based
// pruning (matching a specific value, not just column existence) was removed with the
// range index (#439) and is NOT reinstated here; the value index remains the sole
// source of value-level pruning. With no predicates, all blocks are selected.
//
// Plan always produces Plan.Explain == "". Use PlanWithOptions with EnableExplain: true
// to populate the explain string. NOTE-020.
func (p *Planner) Plan(predicates []Predicate, timeRange TimeRange) *Plan {
	return p.planInternal(predicates, timeRange, false)
}

// PlanWithOptions is like Plan but accepts PlanOptions to control block ordering,
// Limit hint, and whether to populate Plan.Explain. NOTE-020.
//
// When opts.Direction == Backward, SelectedBlocks is reversed in-place (descending order).
// The executor iterates SelectedBlocks sequentially; reversing here gives newest-first
// block traversal at zero additional cost.
func (p *Planner) PlanWithOptions(predicates []Predicate, timeRange TimeRange, opts PlanOptions) *Plan {
	plan := p.planInternal(predicates, timeRange, opts.EnableExplain)
	plan.Direction = opts.Direction
	plan.Limit = opts.Limit
	if opts.Direction == Backward {
		for i, j := 0, len(plan.SelectedBlocks)-1; i < j; i, j = i+1, j-1 {
			plan.SelectedBlocks[i], plan.SelectedBlocks[j] = plan.SelectedBlocks[j], plan.SelectedBlocks[i]
		}
	}
	return plan
}

// planInternal is the core planning implementation. Both Plan and PlanWithOptions delegate here.
// enableExplain gates all explain allocations (strings.Builder, fmt.Sprintf, slices).
// Direction, Limit, and other PlanOptions fields are applied by PlanWithOptions after this returns.
func (p *Planner) planInternal(predicates []Predicate, timeRange TimeRange, enableExplain bool) *Plan {
	total := p.r.BlockCount()
	plan := &Plan{TotalBlocks: total}

	if total == 0 {
		// plan.SelectedBlocks is nil by zero-value, matching SPECS §5.2.
		return plan
	}

	candidates := allBlocks(total)

	// Stage 0: Time-range pruning (metadata-only, zero I/O).
	// Fast path: use the per-file TS index (O(log n)) when available.
	// Slow path: fall back to O(n) BlockMeta scan for files without a TS index.
	if timeRange.MinNano > 0 || timeRange.MaxNano > 0 {
		if timeBlocks := p.r.BlocksInTimeRange(timeRange.MinNano, timeRange.MaxNano); timeBlocks != nil {
			// TS index present: intersect candidates with the time-range result.
			// Build a keep set with the same word-count as candidates (same n).
			keep := make(blockSet, len(candidates))
			for _, bi := range timeBlocks {
				keep.set(bi)
			}
			candidates.iter(func(blockIdx int) {
				if !keep.test(blockIdx) {
					candidates.clear(blockIdx)
					plan.PrunedByTime++
				}
			})
		} else {
			// Old file without TS index: scan BlockMeta for each candidate.
			// Blocks with MinStart==0 && MaxStart==0 have unknown timestamps
			// (e.g. older trace files). Never prune these — they must match all ranges.
			candidates.iter(func(blockIdx int) {
				meta := p.r.BlockMeta(blockIdx)
				if meta.MinStart == 0 && meta.MaxStart == 0 {
					return
				}
				tooOld := timeRange.MaxNano > 0 && meta.MinStart > timeRange.MaxNano
				tooNew := timeRange.MinNano > 0 && meta.MaxStart < timeRange.MinNano
				if tooOld || tooNew {
					candidates.clear(blockIdx)
					plan.PrunedByTime++
				}
			})
		}
	}

	// Snapshot time-surviving blocks for explain output (only when explain is enabled).
	// NOTE-020: skip this allocation when enableExplain is false.
	var timeBlocks []int
	if enableExplain && plan.PrunedByTime > 0 {
		timeBlocks = make([]int, 0, candidates.count())
		candidates.iter(func(b int) {
			timeBlocks = append(timeBlocks, b)
		})
		slices.Sort(timeBlocks)
	}

	if len(predicates) == 0 {
		plan.SelectedBlocks = setToSortedByTime(candidates, p.r)
		if enableExplain {
			explainPlan(predicates, plan, timeBlocks)
		}
		return plan
	}

	// NOTE(#439): Range-index VALUE pruning removed — the range index no longer exists
	// in the data file (the value index is now the authoritative source for
	// value-based pruning). Predicates' Columns are used below only for column-PRESENCE
	// pruning (issue #531), never a value comparison.
	//
	// NOTE(#435,#437): BinaryFuse8 sketch pruning and block scoring also removed — the
	// KLL sketch index was removed in #435 and file-level bloom in #437. Blocks are
	// sorted by MinStart only; column-presence pruning below affects SET membership,
	// not ordering.
	//
	// Stage 1 (issue #531): column-presence bloom pruning. A block is removed only when
	// at least one top-level predicate is PROVABLY unsatisfiable for it (every leaf
	// column definitively absent per MayContainColumn's no-false-negative contract) —
	// this can never incorrectly drop a block that could actually match; it only fails
	// to prune when the bloom lacks the information (old files) or has a false-positive
	// hit, exactly the same conservative-degradation contract MayContainColumn documents.
	candidates.iter(func(blockIdx int) {
		if !blockMayMatchPredicates(p.r, blockIdx, predicates) {
			candidates.clear(blockIdx)
			plan.PrunedByColumnBloom++
		}
	})

	plan.SelectedBlocks = setToSortedByTime(candidates, p.r)
	if enableExplain {
		explainPlan(predicates, plan, timeBlocks)
	}
	return plan
}

// blockMayMatchPredicates reports whether blockIdx might satisfy every predicate in
// predicates (issue #531). The top-level list is implicitly ANDed, mirroring this
// package's own documented example ("AND query { A && B }: two leaf predicates at the
// top level"). Returns false only when at least one predicate is PROVEN unsatisfiable
// for this block via column-presence bloom checks; returns true whenever the bloom
// lacks the information to prove that (conservative — never a false negative).
func blockMayMatchPredicates(idx BlockIndexer, blockIdx int, predicates []Predicate) bool {
	for _, p := range predicates {
		if !predicateMayMatchBlock(idx, blockIdx, p) {
			return false
		}
	}
	return true
}

// predicateMayMatchBlock recursively evaluates one predicate node against blockIdx using
// only column-PRESENCE bloom checks (never a value comparison — value pruning stays
// value-index-only per NOTE(#439)):
//
//   - Leaf (no Children): Columns are OR'd together — the leaf may match if ANY named
//     column may be present (this package's doc comment: "applies an OR bloom check
//     across its Columns"). An empty Columns leaf is a no-op (always may-match).
//   - Composite LogicalAND: may match only if ALL children may match.
//   - Composite LogicalOR: may match if ANY child may match.
func predicateMayMatchBlock(idx BlockIndexer, blockIdx int, p Predicate) bool {
	if len(p.Children) == 0 {
		if len(p.Columns) == 0 {
			return true
		}
		for _, col := range p.Columns {
			if idx.MayContainColumn(blockIdx, col) {
				return true
			}
		}
		return false
	}
	if p.Op == LogicalOR {
		for _, child := range p.Children {
			if predicateMayMatchBlock(idx, blockIdx, child) {
				return true
			}
		}
		return false
	}
	for _, child := range p.Children {
		if !predicateMayMatchBlock(idx, blockIdx, child) {
			return false
		}
	}
	return true
}

// FetchBlocks reads raw bytes for all blocks in plan using aggressive coalescing.
// Returns a map from block index to raw byte slice ready for parsing.
func (p *Planner) FetchBlocks(plan *Plan) (map[int][]byte, error) {
	return p.r.ReadBlocks(plan.SelectedBlocks)
}

// setToSortedByTime converts the candidate set to a slice sorted by block MinStart
// timestamp (ascending), with block index as a final tiebreaker for stable ordering.
// Sketch-derived selectivity scoring was removed in #435; ordering is timestamp-only.
func setToSortedByTime(s blockSet, r BlockIndexer) []int {
	out := make([]int, 0, s.count())
	s.iter(func(k int) {
		out = append(out, k)
	})
	slices.SortFunc(out, func(a, b int) int {
		ma := r.BlockMeta(a)
		mb := r.BlockMeta(b)
		if n := cmp.Compare(ma.MinStart, mb.MinStart); n != 0 {
			return n
		}
		return cmp.Compare(a, b)
	})
	return out
}

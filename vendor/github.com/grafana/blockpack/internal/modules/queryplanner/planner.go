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

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// BlockIndexer is the interface queryplanner requires from its storage backend.
// reader.Reader satisfies this interface; any alternative backend may also implement it.
type BlockIndexer interface {
	// BlockCount returns the total number of blocks in the file.
	BlockCount() int

	// BlockMeta returns the metadata for the block at blockIdx.
	// Called for every candidate block during bloom-filter pruning.
	BlockMeta(blockIdx int) shared.BlockMeta

	// ReadBlocks reads raw bytes for the given block indices using aggressive
	// coalescing. Returns a map from block index to raw byte slice.
	ReadBlocks(blockIndices []int) (map[int][]byte, error)

	// RangeColumnType returns the ColumnType for a range-indexed column, if indexed.
	// Returns (0, false) when the column has no range index.
	RangeColumnType(col string) (shared.ColumnType, bool)

	// BlocksForRange returns the sorted block indices that may contain the
	// given query value for the named column. queryValue must be wire-encoded
	// (8-byte LE for numeric types, raw string for string/bytes).
	// Returns nil (no error) when the value is below all stored lower boundaries.
	BlocksForRange(col string, queryValue shared.RangeValueKey) ([]int, error)

	// BlocksForRangeInterval returns block indices from all buckets whose lower
	// boundary falls within [minKey, maxKey]. This is useful for case-insensitive
	// prefix lookups where the query spans a range of lexicographic values
	// (e.g., "DEBUG" to "debug"). Returns nil when no buckets overlap the interval.
	BlocksForRangeInterval(col string, minKey, maxKey shared.RangeValueKey) ([]int, error)

	// BlocksInTimeRange returns block indices whose timestamp window overlaps
	// [minNano, maxNano] using the per-file TS index (O(log n) binary search).
	// Returns nil when the TS index is absent (old files); callers must fall back
	// to a full BlockMeta scan in that case.
	// The returned slice is sorted in ascending blockID order.
	BlocksInTimeRange(minNano, maxNano uint64) []int

	// ColumnSketch returns bulk per-block sketch data for the named column.
	// Returns nil when no sketch data is available (old files or column not sketched).
	// The returned ColumnSketch has methods returning slices indexed by block number.
	ColumnSketch(col string) ColumnSketch
}

// SketchEvictor is an optional interface that a BlockIndexer may implement.
// When Plan() determines that all blocks in a file have been pruned, it calls
// EvictSketch() to release the sketch section from the process-level cache,
// preventing unbounded memory growth during wide time-range queries.
type SketchEvictor interface {
	EvictSketch()
}

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

// Predicate is a tree node for bloom-filter and range-index block pruning.
//
// A Predicate is either a leaf or a composite node:
//
//   - Leaf (len(Children) == 0): Columns and Values describe a single bloom + range
//     index condition. Columns are OR-combined for bloom (block survives if any column
//     is possibly present). Values are OR-combined for range index lookup.
//
//   - Composite (len(Children) > 0): Op specifies how Children are combined.
//     LogicalAND: block must satisfy ALL children (intersection).
//     LogicalOR:  block must satisfy AT LEAST ONE child (union).
//
// The top-level []Predicate passed to Plan is AND-combined: a block must satisfy every
// top-level predicate to survive.
//
// Examples:
//
//	// AND query { A && B }: two leaf predicates at the top level.
//	[]Predicate{{Columns: ["A"]}, {Columns: ["B"]}}
//
//	// OR query { A || B } same column: one leaf with both values.
//	[]Predicate{{Columns: ["col"], Values: ["A", "B"]}}
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
//
//	// Fully nested { (A || B) && (C || D) }: two composite OR nodes.
//	[]Predicate{
//	    {Op: LogicalOR, Children: []Predicate{{Columns: ["A"]}, {Columns: ["B"]}}},
//	    {Op: LogicalOR, Children: []Predicate{{Columns: ["C"]}, {Columns: ["D"]}}},
//	}
type Predicate struct {
	// Columns holds one or more column names combined with OR for bloom pruning.
	// Used only in leaf nodes (len(Children) == 0). An empty slice is a no-op.
	Columns []string

	// Values holds wire-encoded query values for range index lookup.
	// Used only in leaf nodes. When non-empty and Columns has exactly one entry,
	// the planner unions BlocksForRange results for each value.
	Values []string

	// Children makes this a composite node. When non-empty, Columns/Values/ColType/
	// IntervalMatch are ignored and Op controls how the children's block sets combine.
	Children []Predicate

	// ColType is informational: callers use it when wire-encoding Values before passing
	// them in. The planner does not inspect ColType internally — it calls RangeColumnType
	// to determine the indexed type. Used only in leaf nodes when Values is non-empty.
	ColType shared.ColumnType

	// IntervalMatch changes how Values is interpreted for range-index pruning.
	// When false (default), Values are individual point lookups unioned together.
	// When true, Values must have exactly 2 elements: Values[0] is the min key and
	// Values[1] is the max key. All buckets whose lower boundary falls within
	// [min, max] are included.
	IntervalMatch bool

	// Op specifies how Children are combined. Ignored when len(Children) == 0.
	// LogicalAND (default): block must satisfy all children.
	// LogicalOR: block must satisfy at least one child.
	Op LogicalOp
}

// TimeRange is an optional time window for block-level pruning.
// A zero-value TimeRange (both fields 0) disables time-range pruning.
type TimeRange struct {
	MinNano uint64 // inclusive lower bound (Unix nanoseconds); 0 means no lower bound
	MaxNano uint64 // inclusive upper bound (Unix nanoseconds); 0 means no upper bound
}

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
type PlanOptions struct {
	// Direction controls block ordering in SelectedBlocks. Default (zero) is Forward.
	Direction Direction
	// Limit is an informational hint for the executor: stop after collecting this many
	// results. 0 means no limit. Stored in Plan.Limit; the executor uses it for early
	// termination when iterating SelectedBlocks in the plan's direction.
	Limit int
}

// Plan is the output of a planning step: the block indices to read.
type Plan struct {
	// BlockScores holds the per-block selectivity score (freq/max(cardinality,1)).
	// Higher scores mean more selective (fewer distinct values = more useful for pruning).
	// Only populated when sketch data is available.
	BlockScores map[int]float64

	// Explain is an ASCII trace of how the predicate tree resolved to block sets.
	// Always populated when predicates are present. Example:
	//   (resource.service.name=[0,1,2] || span.service.name=nil) => [0,1,2]
	//   AND resource.env=[1,2,3]
	//   => [1,2]
	Explain string

	// SelectedBlocks is a sorted slice of block indices to fetch.
	SelectedBlocks []int

	// TotalBlocks is the total number of blocks in the file.
	TotalBlocks int

	// PrunedByIndex is the number of blocks eliminated by range index lookups.
	PrunedByIndex int

	// PrunedByTime is the number of blocks eliminated by time-range comparison.
	PrunedByTime int

	// PrunedByFuse is the number of blocks eliminated by BinaryFuse8 membership checks.
	PrunedByFuse int

	// Limit is the early-termination hint passed via PlanOptions.
	// 0 means no limit (all selected blocks should be scanned).
	Limit int

	// Direction is the ordering applied to SelectedBlocks (Forward or Backward).
	// Set by PlanWithOptions; always Forward when Plan() is called directly.
	Direction Direction
}

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
// Each predicate applies an OR bloom check across its Columns: a block is removed
// if all of its columns are definitely absent. When Values is non-empty, a range
// index lookup further narrows the candidates. Multiple predicates are ANDed:
// a block must survive every predicate. With no predicates, all blocks are selected.
func (p *Planner) Plan(predicates []Predicate, timeRange TimeRange) *Plan {
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

	// Snapshot time-surviving blocks for explain output.
	var timeBlocks []int
	if plan.PrunedByTime > 0 {
		timeBlocks = make([]int, 0, candidates.count())
		candidates.iter(func(b int) {
			timeBlocks = append(timeBlocks, b)
		})
		slices.Sort(timeBlocks)
	}

	if len(predicates) == 0 {
		plan.SelectedBlocks = setToSortedByScore(candidates, p.r, nil)
		explainPlan(p.r, predicates, plan, timeBlocks)
		return plan
	}

	// Stage 1: Range index pruning — recursive tree evaluation, top-level predicates AND-combined.
	pruned, err := pruneByIndexAll(p.r, candidates, predicates)
	if err == nil {
		plan.PrunedByIndex += pruned
	}

	// Stage 2: BinaryFuse8 membership pruning — hard exclusion at ~0.39% FPR.
	plan.PrunedByFuse += pruneByFuseAll(p.r, candidates, predicates)

	// If fuse pruning eliminated every block, evict the sketch section from the
	// process-level cache. There is no point keeping tens of MB of sketch data for
	// a file we have determined contains no matching spans.
	if candidates.count() == 0 {
		if ev, ok := p.r.(SketchEvictor); ok {
			ev.EvictSketch()
		}
		plan.SelectedBlocks = nil
		explainPlan(p.r, predicates, plan, timeBlocks)
		return plan
	}

	// Stage 3: Score remaining blocks for selectivity (freq/max(cardinality,1)).
	// NOTE-014: Higher score = fewer distinct values relative to query frequency.
	plan.BlockScores = scoreBlocks(p.r, candidates, predicates)

	plan.SelectedBlocks = setToSortedByScore(candidates, p.r, plan.BlockScores)
	explainPlan(p.r, predicates, plan, timeBlocks)
	return plan
}

// PlanWithOptions is like Plan but accepts PlanOptions to control block ordering and
// stores a Limit hint in the returned Plan.
//
// When opts.Direction == Backward, SelectedBlocks is reversed in-place (descending order).
// The executor iterates SelectedBlocks sequentially; reversing here gives newest-first
// block traversal at zero additional cost.
func (p *Planner) PlanWithOptions(predicates []Predicate, timeRange TimeRange, opts PlanOptions) *Plan {
	plan := p.Plan(predicates, timeRange)
	plan.Direction = opts.Direction
	plan.Limit = opts.Limit
	if opts.Direction == Backward {
		for i, j := 0, len(plan.SelectedBlocks)-1; i < j; i, j = i+1, j-1 {
			plan.SelectedBlocks[i], plan.SelectedBlocks[j] = plan.SelectedBlocks[j], plan.SelectedBlocks[i]
		}
	}
	return plan
}

// FetchBlocks reads raw bytes for all blocks in plan using aggressive coalescing.
// Returns a map from block index to raw byte slice ready for parsing.
func (p *Planner) FetchBlocks(plan *Plan) (map[int][]byte, error) {
	return p.r.ReadBlocks(plan.SelectedBlocks)
}

// setToSortedByScore converts the candidate set to a slice sorted by block MinStart
// timestamp (ascending), with sketch score as a secondary sort (descending — higher
// score first). When scores are available, blocks with the same timestamp window are
// ordered so the most promising blocks (high frequency, low cardinality) come first.
// This improves early termination for limited queries: the executor is more likely to
// find matches in the first few blocks.
// Block index is used as a final tiebreaker for stable ordering.
func setToSortedByScore(s blockSet, r BlockIndexer, scores map[int]float64) []int {
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
		// Secondary: higher score first (descending).
		if len(scores) > 0 {
			sa, sb := scores[a], scores[b]
			if n := cmp.Compare(sb, sa); n != 0 { // note: sb before sa for descending
				return n
			}
		}
		return cmp.Compare(a, b)
	})
	return out
}

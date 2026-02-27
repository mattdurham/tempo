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
}

// Predicate is a bloom-filter condition used for block-level pruning.
//
// Semantics:
//   - Within one Predicate, Columns are combined with OR: a block survives if its
//     bloom filter reports any of the columns as possibly present. A block is pruned
//     only when the bloom filter reports all columns as definitely absent.
//   - Multiple Predicates passed to Plan are combined with AND: a block must survive
//     every predicate to appear in the plan.
//
// This lets callers express both AND and OR query structures:
//
//	// AND query:  { A && B }  → two predicates, each with one column
//	[]Predicate{{Columns: ["A"]}, {Columns: ["B"]}}
//
//	// OR query:   { A || B }  → one predicate with both columns
//	[]Predicate{{Columns: ["A", "B"]}}
//
//	// Mixed:  { A && (B || C) }  → two predicates
//	[]Predicate{{Columns: ["A"]}, {Columns: ["B", "C"]}}
type Predicate struct {
	// Columns holds one or more column names combined with OR for bloom pruning.
	// An empty slice is a no-op for the bloom stage.
	Columns []string

	// Values holds wire-encoded query values for range index lookup.
	// When non-empty and ColType is set, the planner intersects the bloom-surviving
	// candidates with the union of BlocksForRange results for each value.
	// An empty slice skips the range index stage.
	Values []string

	// ColType is the ColumnType needed to interpret Values in the range index.
	// 0 (ColumnTypeString) is valid but only meaningful when Values is non-empty.
	// When Values is empty, ColType is ignored.
	ColType shared.ColumnType
}

// Plan is the output of a planning step: the block indices to read.
type Plan struct {
	// SelectedBlocks is a sorted slice of block indices to fetch.
	SelectedBlocks []int

	// TotalBlocks is the total number of blocks in the file.
	TotalBlocks int

	// PrunedByBloom is the number of blocks eliminated by column name bloom filters.
	PrunedByBloom int

	// PrunedByIndex is the number of blocks eliminated by range index lookups.
	PrunedByIndex int
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

// Plan returns the set of block indices to read for the given predicates.
//
// Each predicate applies an OR bloom check across its Columns: a block is removed
// if all of its columns are definitely absent. When Values is non-empty, a range
// index lookup further narrows the candidates. Multiple predicates are ANDed:
// a block must survive every predicate. With no predicates, all blocks are selected.
func (p *Planner) Plan(predicates []Predicate) *Plan {
	total := p.r.BlockCount()
	plan := &Plan{TotalBlocks: total}

	if total == 0 || len(predicates) == 0 {
		plan.SelectedBlocks = allBlocks(total)
		return plan
	}

	candidates := allBlockSet(total)

	for _, pred := range predicates {
		plan.PrunedByBloom += pruneByBloom(p.r, candidates, pred)
		pruned, err := pruneByIndex(p.r, candidates, pred)
		if err == nil {
			plan.PrunedByIndex += pruned
		}
	}

	plan.SelectedBlocks = setToSortedSlice(candidates)
	return plan
}

// FetchBlocks reads raw bytes for all blocks in plan using aggressive coalescing.
// Returns a map from block index to raw byte slice ready for parsing.
func (p *Planner) FetchBlocks(plan *Plan) (map[int][]byte, error) {
	return p.r.ReadBlocks(plan.SelectedBlocks)
}

// allBlocks returns a sorted slice [0, n).
func allBlocks(n int) []int {
	out := make([]int, n)
	for i := range n {
		out[i] = i
	}
	return out
}

// allBlockSet returns a set containing all block indices [0, n).
func allBlockSet(n int) map[int]struct{} {
	s := make(map[int]struct{}, n)
	for i := range n {
		s[i] = struct{}{}
	}
	return s
}

// setToSortedSlice converts the candidate set to a sorted slice.
func setToSortedSlice(s map[int]struct{}) []int {
	out := make([]int, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

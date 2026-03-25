package queryplanner

// Tests for blockSetForPred and pruneByIndexAll using a mock BlockIndexer.
// No blockpack files — purely functional tests driven by configurable range column indexes.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// --- mock BlockIndexer ---

// mockBlock is a placeholder for per-block test state.
type mockBlock struct{}

// mockRangeIndex maps column → value → block indices.
type mockRangeIndex struct {
	pointMap  map[string][]int    // value → blocks
	intervals map[[2]string][]int // [min,max] → blocks (for interval match)
	colType   shared.ColumnType
}

// mockIndexer implements BlockIndexer for unit tests.
type mockIndexer struct {
	blocks     []mockBlock
	ranges     map[string]*mockRangeIndex // column → range index
	timeBlocks []int                      // if non-nil, BlocksInTimeRange returns this
}

func (m *mockIndexer) BlockCount() int { return len(m.blocks) }

func (m *mockIndexer) BlockMeta(blockIdx int) shared.BlockMeta {
	return shared.BlockMeta{}
}

func (m *mockIndexer) ReadBlocks(indices []int) (map[int][]byte, error) {
	return nil, nil // unused in selection tests
}

func (m *mockIndexer) RangeColumnType(col string) (shared.ColumnType, bool) {
	if ri, ok := m.ranges[col]; ok {
		return ri.colType, true
	}
	return 0, false
}

func (m *mockIndexer) BlocksForRange(col string, key shared.RangeValueKey) ([]int, error) {
	ri, ok := m.ranges[col]
	if !ok {
		return nil, nil
	}
	return ri.pointMap[key], nil
}

func (m *mockIndexer) BlocksInTimeRange(_, _ uint64) []int { return m.timeBlocks }
func (m *mockIndexer) ColumnSketch(_ string) ColumnSketch  { return nil }

func (m *mockIndexer) BlocksForRangeInterval(col, minKey, maxKey shared.RangeValueKey) ([]int, error) {
	ri, ok := m.ranges[col]
	if !ok {
		return nil, nil
	}
	key := [2]string{minKey, maxKey}
	if blocks, ok := ri.intervals[key]; ok {
		return blocks, nil
	}
	// Fallback: union all point lookups whose keys fall in [minKey, maxKey].
	var result []int
	for k, blocks := range ri.pointMap {
		if k >= minKey && k <= maxKey {
			result = append(result, blocks...)
		}
	}
	return result, nil
}

// --- blockSetForPred tests (range index) ---

func TestBlockSetForPred_LeafIndexed(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 2}, "web": {1, 3}},
			},
		},
	}

	set, err := blockSetForPred(m, Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"auth"},
	})
	require.NoError(t, err)
	require.NotNil(t, set)
	assert.Len(t, set, 2)
	assert.Contains(t, set, 0)
	assert.Contains(t, set, 2)
}

func TestBlockSetForPred_LeafNotIndexed(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{}, // no range indexes
	}

	set, err := blockSetForPred(m, Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"auth"},
	})
	require.NoError(t, err)
	assert.Nil(t, set, "no range index → nil (unconstrained)")
}

func TestBlockSetForPred_LeafNoValues(t *testing.T) {
	m := &mockIndexer{blocks: make([]mockBlock, 3)}

	set, err := blockSetForPred(m, Predicate{
		Columns: []string{"resource.service.name"},
	})
	require.NoError(t, err)
	assert.Nil(t, set, "no values → nil (unconstrained)")
}

func TestBlockSetForPred_LeafNoMatch(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 3),
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0}},
			},
		},
	}

	set, err := blockSetForPred(m, Predicate{
		Columns: []string{"resource.service.name"},
		Values:  []string{"nonexistent"},
	})
	require.NoError(t, err)
	require.NotNil(t, set, "indexed but no match → non-nil empty set")
	assert.Empty(t, set)
}

// TestBlockSetForPred_ORSkipNil verifies NOTE-012 nil-skip semantics: when an OR child
// has no range index (nil), it is skipped. The OR returns the union of constrained
// children. This is safe because the writer guarantees: a column with no range index
// is absent from the file entirely — no block can satisfy that scope.
func TestBlockSetForPred_ORSkipNil(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			// Only resource.service.name has a range index.
			// span.service.name and log.service.name do NOT → skipped by nil-skip.
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 2}},
			},
		},
	}

	// Unscoped .service.name = "auth" expands to OR(resource, span, log).
	// span and log have no range index → skipped. Result = resource's blocks.
	pred := Predicate{
		Op: LogicalOR,
		Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"span.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"log.service.name"}, Values: []string{"auth"}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set, "nil-skip: constrained child provides blocks")
	assert.Len(t, set, 2)
	assert.Contains(t, set, 0)
	assert.Contains(t, set, 2)
}

// TestBlockSetForPred_ORAllUnconstrained verifies that when ALL OR children
// are unconstrained (no range index), the OR returns nil.
func TestBlockSetForPred_ORAllUnconstrained(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 3),
		ranges: map[string]*mockRangeIndex{}, // no range indexes at all
	}

	pred := Predicate{
		Op: LogicalOR,
		Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"span.service.name"}, Values: []string{"auth"}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	assert.Nil(t, set, "all children unconstrained → OR must be nil")
}

// TestBlockSetForPred_ORUnion verifies that when ALL OR children are indexed,
// the result is a union of their block sets.
func TestBlockSetForPred_ORUnion(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0}},
			},
			"span.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {3}},
			},
			"log.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {4}},
			},
		},
	}

	pred := Predicate{
		Op: LogicalOR,
		Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"span.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"log.service.name"}, Values: []string{"auth"}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set)
	assert.Len(t, set, 3, "union of blocks 0, 3, and 4")
	assert.Contains(t, set, 0)
	assert.Contains(t, set, 3)
	assert.Contains(t, set, 4)
}

// TestBlockSetForPred_ANDIntersect verifies AND composites intersect children.
func TestBlockSetForPred_ANDIntersect(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 1, 2}},
			},
			"resource.env": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"prod": {1, 2, 3}},
			},
		},
	}

	pred := Predicate{
		Op: LogicalAND,
		Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"resource.env"}, Values: []string{"prod"}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set)
	assert.Len(t, set, 2, "intersection of {0,1,2} and {1,2,3} = {1,2}")
	assert.Contains(t, set, 1)
	assert.Contains(t, set, 2)
}

// TestBlockSetForPred_ANDSkipsUnconstrained verifies AND skips unconstrained
// children (no range index) conservatively.
func TestBlockSetForPred_ANDSkipsUnconstrained(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 2}},
			},
			// resource.env has NO range index.
		},
	}

	pred := Predicate{
		Op: LogicalAND,
		Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"resource.env"}, Values: []string{"prod"}}, // not indexed → skipped
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set)
	assert.Len(t, set, 2, "only indexed child constrains: {0, 2}")
	assert.Contains(t, set, 0)
	assert.Contains(t, set, 2)
}

// TestBlockSetForPred_NestedORInsideAND verifies A && (B || C) where C is unindexed.
// OR(B, C): nil-skip skips C, returns B's blocks {1,3}. AND intersects with A.
func TestBlockSetForPred_NestedORInsideAND(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 1, 2}},
			},
			"resource.env": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"prod": {1, 3}},
			},
			// span.env NOT indexed → skipped in OR (nil-skip).
		},
	}

	// service.name="auth" AND (resource.env="prod" OR span.env="prod")
	pred := Predicate{
		Op: LogicalAND,
		Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Op: LogicalOR, Children: []Predicate{
				{Columns: []string{"resource.env"}, Values: []string{"prod"}},
				{Columns: []string{"span.env"}, Values: []string{"prod"}}, // not indexed → skipped
			}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set)
	// service.name="auth" → {0,1,2}
	// OR(resource.env="prod" → {1,3}, span.env → nil → skipped) → {1,3}
	// AND({0,1,2}, {1,3}) → {1}
	assert.Len(t, set, 1)
	assert.Contains(t, set, 1)
}

// TestBlockSetForPred_NestedANDInsideOR verifies (A && B) || (C && D).
func TestBlockSetForPred_NestedANDInsideOR(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.a": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"x": {0, 1}},
			},
			"resource.b": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"y": {1, 2}},
			},
			"resource.c": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"z": {3, 4}},
			},
			"resource.d": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"w": {3}},
			},
		},
	}

	// (a="x" && b="y") || (c="z" && d="w")
	pred := Predicate{
		Op: LogicalOR,
		Children: []Predicate{
			{Op: LogicalAND, Children: []Predicate{
				{Columns: []string{"resource.a"}, Values: []string{"x"}},
				{Columns: []string{"resource.b"}, Values: []string{"y"}},
			}},
			{Op: LogicalAND, Children: []Predicate{
				{Columns: []string{"resource.c"}, Values: []string{"z"}},
				{Columns: []string{"resource.d"}, Values: []string{"w"}},
			}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set)
	// AND(a→{0,1}, b→{1,2}) → {1}
	// AND(c→{3,4}, d→{3}) → {3}
	// OR union → {1, 3}
	assert.Len(t, set, 2)
	assert.Contains(t, set, 1)
	assert.Contains(t, set, 3)
}

// TestBlockSetForPred_ThreeLevel verifies A && ((B||C) && (D||E)) with mixed indexing.
// C is unindexed so OR(B,C) → nil; AND skips that nil → result is A && OR(D,E).
func TestBlockSetForPred_ThreeLevel(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 6),
		ranges: map[string]*mockRangeIndex{
			"resource.a": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"v": {0, 1, 2, 3}},
			},
			"resource.b": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"v": {0, 1}},
			},
			// resource.c NOT indexed → OR(B,C) returns nil (unconstrained).
			"resource.d": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"v": {1, 2, 3}},
			},
			"resource.e": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"v": {0, 4}},
			},
		},
	}

	// A && ((B||C) && (D||E))
	pred := Predicate{
		Op: LogicalAND,
		Children: []Predicate{
			{Columns: []string{"resource.a"}, Values: []string{"v"}},
			{Op: LogicalAND, Children: []Predicate{
				{Op: LogicalOR, Children: []Predicate{
					{Columns: []string{"resource.b"}, Values: []string{"v"}},
					{Columns: []string{"resource.c"}, Values: []string{"v"}}, // not indexed
				}},
				{Op: LogicalOR, Children: []Predicate{
					{Columns: []string{"resource.d"}, Values: []string{"v"}},
					{Columns: []string{"resource.e"}, Values: []string{"v"}},
				}},
			}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	require.NotNil(t, set)
	// A → {0,1,2,3}
	// OR(B→{0,1}, C→nil→skipped) → {0,1} (nil-skip: only B contributes)
	// OR(D→{1,2,3}, E→{0,4}) → {0,1,2,3,4} (all indexed, union)
	// AND({0,1}, {0,1,2,3,4}) → {0,1}
	// AND({0,1,2,3}, {0,1}) → {0,1}
	assert.Len(t, set, 2)
	assert.Contains(t, set, 0)
	assert.Contains(t, set, 1)
}

// TestBlockSetForPred_IntervalMatch verifies interval range lookups.
func TestBlockSetForPred_IntervalMatch(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"span:duration": {
				colType:  shared.ColumnTypeRangeUint64,
				pointMap: map[string][]int{},
				intervals: map[[2]string][]int{
					{"min", "max"}: {1, 3, 4},
				},
			},
		},
	}

	set, err := blockSetForPred(m, Predicate{
		Columns:       []string{"span:duration"},
		Values:        []string{"min", "max"},
		IntervalMatch: true,
	})
	require.NoError(t, err)
	require.NotNil(t, set)
	assert.Len(t, set, 3)
	assert.Contains(t, set, 1)
	assert.Contains(t, set, 3)
	assert.Contains(t, set, 4)
}

// TestBlockSetForPred_IntervalWrongValueCount verifies that an interval match
// with != 2 values returns nil (unconstrained) rather than panicking.
func TestBlockSetForPred_IntervalWrongValueCount(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 3),
		ranges: map[string]*mockRangeIndex{
			"span:duration": {
				colType:  shared.ColumnTypeRangeUint64,
				pointMap: map[string][]int{},
			},
		},
	}

	// IntervalMatch=true but only 1 value (needs exactly 2).
	set, err := blockSetForPred(m, Predicate{
		Columns:       []string{"span:duration"},
		Values:        []string{"onlyone"},
		IntervalMatch: true,
	})
	require.NoError(t, err)
	assert.Nil(t, set, "interval with != 2 values → nil (unconstrained)")

	// IntervalMatch=true with 3 values.
	set2, err := blockSetForPred(m, Predicate{
		Columns:       []string{"span:duration"},
		Values:        []string{"a", "b", "c"},
		IntervalMatch: true,
	})
	require.NoError(t, err)
	assert.Nil(t, set2, "interval with 3 values → nil (unconstrained)")
}

// TestBlockSetForPred_MultipleColumns verifies that a leaf with >1 column
// returns nil (not indexable — range lookup requires exactly one column).
func TestBlockSetForPred_MultipleColumns(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 3),
		ranges: map[string]*mockRangeIndex{
			"resource.a": {colType: shared.ColumnTypeRangeString, pointMap: map[string][]int{"v": {0}}},
		},
	}

	set, err := blockSetForPred(m, Predicate{
		Columns: []string{"resource.a", "resource.b"},
		Values:  []string{"v"},
	})
	require.NoError(t, err)
	assert.Nil(t, set, "multiple columns → nil (not indexable)")
}

// TestBlockSetForPred_LeafMultiValue verifies union of multiple values in a leaf.
func TestBlockSetForPred_LeafMultiValue(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 5),
		ranges: map[string]*mockRangeIndex{
			"resource.env": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"prod": {0, 1}, "staging": {2, 3}},
			},
		},
	}

	set, err := blockSetForPred(m, Predicate{
		Columns: []string{"resource.env"},
		Values:  []string{"prod", "staging"},
	})
	require.NoError(t, err)
	require.NotNil(t, set)
	assert.Len(t, set, 4, "union of prod{0,1} and staging{2,3}")
}

// TestBlockSetForPred_ANDAllUnconstrained verifies AND with all children
// unconstrained returns nil.
func TestBlockSetForPred_ANDAllUnconstrained(t *testing.T) {
	m := &mockIndexer{
		blocks: make([]mockBlock, 3),
		ranges: map[string]*mockRangeIndex{},
	}

	pred := Predicate{
		Op: LogicalAND,
		Children: []Predicate{
			{Columns: []string{"resource.a"}, Values: []string{"v"}},
			{Columns: []string{"resource.b"}, Values: []string{"v"}},
		},
	}

	set, err := blockSetForPred(m, pred)
	require.NoError(t, err)
	assert.Nil(t, set, "AND with all unconstrained children → nil")
}

// --- Full Plan() tests with mock ---

// --- Explain tests ---

func TestPlan_Explain_Simple(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 2}},
			},
		},
	}

	p := NewPlanner(m)
	plan := p.Plan([]Predicate{
		{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
	}, TimeRange{})

	assert.Contains(t, plan.Explain, "resource.service.name=[0,2] → [0,2]")
	assert.Contains(t, plan.Explain, "range-index:")
}

func TestPlan_Explain_ORSkipNil(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 1}},
			},
		},
	}

	p := NewPlanner(m)
	plan := p.Plan([]Predicate{
		{Op: LogicalOR, Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"span.service.name"}, Values: []string{"auth"}},
		}},
	}, TimeRange{})

	// span.service.name has no index → nil-skip → OR uses only resource → {0,1}
	assert.Contains(t, plan.Explain, "(resource.service.name=[0,1] || span.service.name=[]) → [0,1]")
}

func TestPlan_Explain_ANDWithOR(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 1, 2}},
			},
			"resource.env": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"prod": {1, 2, 3}},
			},
		},
	}

	p := NewPlanner(m)
	plan := p.Plan([]Predicate{
		{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
		{Columns: []string{"resource.env"}, Values: []string{"prod"}},
	}, TimeRange{})

	assert.Contains(t, plan.Explain, "(resource.service.name=[0...2] && resource.env=[1...3]) → [1,2]")
}

func TestPlan_Explain_NoPredicates(t *testing.T) {
	m := &mockIndexer{blocks: make([]mockBlock, 5)}
	p := NewPlanner(m)
	plan := p.Plan(nil, TimeRange{})
	assert.Equal(t, "no predicates → all 5 blocks", plan.Explain)
}

func TestPlan_Explain_TimeRangePruning(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.env": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"prod": {0, 1, 2, 3}},
			},
		},
		timeBlocks: []int{1, 2}, // only blocks 1,2 in time range
	}

	p := NewPlanner(m)
	plan := p.Plan([]Predicate{
		{Columns: []string{"resource.env"}, Values: []string{"prod"}},
	}, TimeRange{MinNano: 100, MaxNano: 200})

	assert.Equal(t, 2, plan.PrunedByTime)
	assert.Contains(t, plan.Explain, "ts:[1,2]")
}

// TestPlan_ORSkipNil_EndToEnd verifies the full Plan flow: when any OR child is unindexed,
// the entire OR returns nil (unconstrained) — no blocks are pruned by index.
func TestPlan_ORSkipNil_EndToEnd(t *testing.T) {
	// 4 blocks. Range index: resource.service.name has "auth" in blocks 0,1.
	// span.service.name and log.service.name have NO range index.
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 1}},
			},
		},
	}

	p := NewPlanner(m)
	// Unscoped .service.name = "auth" → OR(resource, span, log).
	plan := p.Plan([]Predicate{
		{Op: LogicalOR, Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"span.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"log.service.name"}, Values: []string{"auth"}},
		}},
	}, TimeRange{})

	assert.Equal(t, 4, plan.TotalBlocks)
	// OR(resource→{0,1}, span→nil→skip, log→nil→skip) → {0,1} (nil-skip).
	// 2 blocks pruned by index.
	assert.Equal(t, 2, plan.PrunedByIndex)
	assert.Equal(t, 2, len(plan.SelectedBlocks))
}

// TestPlan_ANDWithOR_EndToEnd verifies service.name="auth" AND .env="prod"
// where env is unscoped → OR(resource.env, span.env, log.env).
// span.env and log.env are unindexed → OR returns nil → AND skips it → only service.name prunes.
func TestPlan_ANDWithOR_EndToEnd(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0, 1, 2}},
			},
			"resource.env": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"prod": {1, 2, 3}},
			},
		},
	}

	p := NewPlanner(m)
	// resource.service.name="auth" AND OR(resource.env, span.env, log.env)="prod"
	plan := p.Plan([]Predicate{
		{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
		{Op: LogicalOR, Children: []Predicate{
			{Columns: []string{"resource.env"}, Values: []string{"prod"}},
			{Columns: []string{"span.env"}, Values: []string{"prod"}},
			{Columns: []string{"log.env"}, Values: []string{"prod"}},
		}},
	}, TimeRange{})

	assert.Equal(t, 4, plan.TotalBlocks)
	// service.name="auth" → {0,1,2}
	// OR(resource.env→{1,2,3}, span.env→nil→skip, log.env→nil→skip) → {1,2,3} (nil-skip)
	// AND({0,1,2}, {1,2,3}) → {1,2}
	// blocks 0 and 3 pruned by index.
	assert.Equal(t, 2, plan.PrunedByIndex)
	assert.Equal(t, 2, len(plan.SelectedBlocks))
	assert.Contains(t, plan.SelectedBlocks, 1)
	assert.Contains(t, plan.SelectedBlocks, 2)
}

// TestPlan_RangePrunesAll verifies that range index prunes all non-matching blocks.
func TestPlan_RangePrunesAll(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{}, // block 0: "auth" maps here
			{}, // block 1: not in range for "auth"
			{}, // block 2: not in range for "auth"
			{}, // block 3: not in range for "auth"
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {0}},
			},
		},
	}

	p := NewPlanner(m)
	plan := p.Plan([]Predicate{
		{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
	}, TimeRange{})

	assert.Equal(t, 4, plan.TotalBlocks)
	assert.Equal(t, 3, plan.PrunedByIndex, "blocks 1,2,3 pruned by range")
	assert.Equal(t, 1, len(plan.SelectedBlocks))
	assert.Contains(t, plan.SelectedBlocks, 0)
}

// TestPlan_OREmptyResult verifies that when ALL OR children are indexed but find
// no matching blocks, the result is an empty set (prunes all blocks).
func TestPlan_OREmptyResult(t *testing.T) {
	m := &mockIndexer{
		blocks: []mockBlock{
			{},
			{},
		},
		ranges: map[string]*mockRangeIndex{
			"resource.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {}}, // indexed but empty
			},
			"span.service.name": {
				colType:  shared.ColumnTypeRangeString,
				pointMap: map[string][]int{"auth": {}}, // indexed but empty
			},
		},
	}

	p := NewPlanner(m)
	plan := p.Plan([]Predicate{
		{Op: LogicalOR, Children: []Predicate{
			{Columns: []string{"resource.service.name"}, Values: []string{"auth"}},
			{Columns: []string{"span.service.name"}, Values: []string{"auth"}},
		}},
	}, TimeRange{})

	assert.Equal(t, 2, plan.TotalBlocks)
	// OR: resource→empty set (indexed, no match), span→empty set (indexed, no match).
	// Union of two empty sets → empty → prune all.
	assert.Equal(t, 2, plan.PrunedByIndex)
	assert.Empty(t, plan.SelectedBlocks)
}

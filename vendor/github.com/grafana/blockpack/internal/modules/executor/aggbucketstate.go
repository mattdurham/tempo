package executor

// makeGroupBuckets allocates a numGroups×numSteps matrix of *aggBucketState as a [][]
// whose rows are reslices of ONE flat backing array. NOTE-272: the per-group-by intrinsic
// accumulation paths (count/rate-by, general-agg-by, direct-scan-by) previously allocated
// the matrix as numGroups+1 separate slices (one outer + one make([]*aggBucketState,
// numSteps) per group). On high-cardinality group-bys (hundreds–thousands of groups, e.g.
// rate/histogram by a high-card attribute) that is numGroups individual heap allocations
// per query whose only purpose is to hold the same numGroups*numSteps pointer slots. A
// single flat backing array sliced into rows collapses that to two allocations — the row
// reslices share the backing storage and carry no extra per-row allocation. Indexing
// (groupBuckets[gIdx][bk-1]) and the nil-cell emit walk are byte-for-byte unchanged: each
// row is a [0:numSteps] window into the flat array, so reads/writes land in the same logical
// cell as before. Pointer slots are zeroed (the backing is a []*aggBucketState), preserving
// the "nil until first write" cell contract the accumulation and emit loops rely on.
func makeGroupBuckets(numGroups int, numSteps int64) [][]*aggBucketState {
	ns := int(numSteps)
	flat := make([]*aggBucketState, numGroups*ns)
	rows := make([][]*aggBucketState, numGroups)
	for i := range rows {
		off := i * ns
		rows[i] = flat[off : off+ns : off+ns]
	}
	return rows
}

// bucketArena hands out *aggBucketState pointers backed by a growing flat slab instead of
// one independent heap allocation per occupied cell.
//
// NOTE-276: the per-group-by accumulation loops (count/rate-by, general-agg-by) materialize
// one &aggBucketState{} on the first write to each occupied (group, timestep) cell. On a
// high-cardinality group-by spanning many timesteps (e.g. histogram/agg by a high-card
// attribute over a wide window) that is thousands of individual ~80-byte heap allocations
// per query — each a separate object the GC must track, the dominant residual alloc/GC
// source on these paths once NOTE-272 collapsed the matrix scaffold itself. The arena
// appends each newly-occupied bucket into a single flat []aggBucketState backing slice and
// returns &slab[len-1]; pointers stay stable because the arena never reslices a slab it has
// already vended from — when the current slab fills it allocates a fresh slab and continues
// there, so previously returned pointers are never invalidated. This collapses N tiny
// allocations into ceil(N/slabCap) slab allocations (amortized ~one alloc per arenaSlabCap
// occupied cells), cutting GC object count on the high-card group-by path. The emit walk and
// the downstream buckets map hold these interior pointers; the arena's slabs remain
// reachable through them for the lifetime of the per-query result, so there is no dangling
// reference. Behavior is otherwise identical to per-cell &aggBucketState{}: each vended
// bucket is zero-valued (a fresh slab element) and the caller sets min/max as before.
type bucketArena struct {
	slab []aggBucketState
}

// arenaSlabCap is the number of aggBucketState structs per arena slab. Sized so a slab is a
// single modestly-sized allocation; once it fills, the arena allocates the next slab and
// keeps vending stable interior pointers from it (prior pointers stay valid).
const arenaSlabCap = 1024

// alloc returns a pointer to a fresh, zero-valued aggBucketState backed by the arena. The
// returned pointer is stable: the arena never grows the slab it vended from in place (it
// starts a new slab when the current one is full), so append never reallocates live storage.
func (a *bucketArena) alloc() *aggBucketState {
	if len(a.slab) == cap(a.slab) {
		// Current slab is full (or unallocated) — start a fresh one. This leaves any
		// previously vended pointers untouched: they point into the prior slab, which
		// stays alive as long as those pointers do.
		a.slab = make([]aggBucketState, 0, arenaSlabCap)
	}
	a.slab = a.slab[:len(a.slab)+1]
	return &a.slab[len(a.slab)-1]
}

type aggBucketState struct {
	values []float64
	sum    float64
	count  int64
	min    float64
	max    float64
	mean   float64
	m2     float64
}

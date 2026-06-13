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

type aggBucketState struct {
	values []float64
	sum    float64
	count  int64
	min    float64
	max    float64
	mean   float64
	m2     float64
}

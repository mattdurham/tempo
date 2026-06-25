package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"slices"
)

// sortPending sorts the pending span buffer by (service.name ASC, span.name ASC, TraceID ASC).
//
// NOTE-457: span.name as secondary sort key guarantees same-name spans cluster into the same
// blocks, enabling the exact-value range index fast path (min==max per block → zero false
// positives for {span.name = "X"} queries).
//
// NOTE-473: MinHash was previously the tertiary sort key to sub-cluster spans within a
// (service.name, span.name) group by attribute-set similarity. It has been removed because:
//  1. Within a (service.name, span.name) group, spans share nearly identical column presence
//     patterns — the primary and secondary keys already provide ~all compression benefit.
//  2. Computing MinHash from block columns during compaction required O(N_spans × N_columns)
//     atomic loads (~40 B operations for a typical L1→L2 job), dominating compaction wall-time.
//  3. Computing MinHash from proto attributes during ingestion adds complexity with no
//     measurable compression improvement over (service.name, span.name, traceID) ordering.
//
// traceID as final tiebreaker keeps a trace's same-group spans physically adjacent.
//
// Sorts a []int index slice to avoid copying pendingSpan values during the O(n log n) comparison
// phase, then applies the final permutation in one O(n) copy pass.
func sortPending(pending []pendingSpan) {
	n := len(pending)
	if n <= 1 {
		return
	}

	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	slices.SortFunc(indices, func(ai, bi int) int {
		return spanSortKeyCmp(&pending[ai], &pending[bi])
	})

	// Apply the permutation with a single O(n) copy pass.
	sorted := make([]pendingSpan, n)
	for i, idx := range indices {
		sorted[i] = pending[idx]
	}
	copy(pending, sorted)
}

// spanSortKeyCmp is the active span ordering comparator. It is a var (not a
// direct call) solely so benchmarks can swap in candidate orderings.
// Production always uses compareSpanSortKey.
var spanSortKeyCmp = compareSpanSortKey

// compareSpanSortKey is the production span ordering comparator (NOTE-457, NOTE-473):
//
//	(service.name ASC, span.name ASC, TraceID ASC)
func compareSpanSortKey(a, b *pendingSpan) int {
	if a.svcName != b.svcName {
		if a.svcName < b.svcName {
			return -1
		}
		return 1
	}
	if a.spanName != b.spanName {
		if a.spanName < b.spanName {
			return -1
		}
		return 1
	}
	return bytes.Compare(a.traceID[:], b.traceID[:])
}

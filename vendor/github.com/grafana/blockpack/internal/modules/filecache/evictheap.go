package filecache

// evictHeap is a min-heap of *entry ordered by entry.order (the monotonic
// insertion sequence). The smallest order is the oldest entry — the next FIFO
// eviction victim. Maintaining the heap incrementally on Put makes eviction
// O(k·log N) for k victims instead of the previous O(N²) full-index re-sort on
// every Put (NOTE-439: filecache.evictLocked was the dominant CPU frame ~13% in
// the M9 filtered rate-by-group profile, where the filter columns push the
// per-query working set past MaxBytes and trigger eviction on nearly every Put).
//
// Stale entries (removed from the index by a Get ErrNotExist cleanup, or
// superseded) are tolerated via lazy deletion: evictLocked validates each popped
// entry against the live index by pointer identity and simply drops mismatches.
type evictHeap []*entry

func (h evictHeap) Len() int           { return len(h) }
func (h evictHeap) Less(i, j int) bool { return h[i].order < h[j].order }
func (h evictHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *evictHeap) Push(x any)        { *h = append(*h, x.(*entry)) }
func (h *evictHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil // avoid retaining the *entry for GC
	*h = old[:n-1]
	return e
}

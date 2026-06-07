package executor

type topKHeap struct {
	entries  []topKEntry
	backward bool
}

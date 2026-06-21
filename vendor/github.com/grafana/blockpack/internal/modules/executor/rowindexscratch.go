package executor

import "sync"

// rowIndexScratch is a reusable []int scratch buffer pool shared by the
// row-filtering paths (e.g. stream.go time-range row filtering). Buffers larger
// than rowIndexScratchMaxCap are dropped on release to avoid retaining oversized
// allocations.
const (
	rowIndexScratchDefaultCap = 256
	rowIndexScratchMaxCap     = 65536
)

//nolint:gochecknoglobals
var rowIndexScratchPool = &sync.Pool{
	New: func() any {
		s := make([]int, 0, rowIndexScratchDefaultCap)
		return &s
	},
}

func acquireRowIndexScratch() *[]int {
	//nolint:forcetypeassert
	return rowIndexScratchPool.Get().(*[]int)
}

func releaseRowIndexScratch(p *[]int) {
	if cap(*p) > rowIndexScratchMaxCap {
		*p = make([]int, 0, rowIndexScratchDefaultCap)
	} else {
		*p = (*p)[:0]
	}
	rowIndexScratchPool.Put(p)
}

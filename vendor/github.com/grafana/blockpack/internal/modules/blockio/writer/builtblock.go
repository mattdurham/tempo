package writer

import "github.com/grafana/blockpack/internal/modules/blockio/shared"

type builtBlock struct {
	traceRows    map[[16]byte]struct{}
	colMinMax    map[string]*blockColMinMax
	colStats     []shared.ColStat
	colSketches  blockSketchSet
	localAccum   *intrinsicAccumulator
	blockVectors [][]float32
	payload      []byte
	spanCount    int
	minStart     uint64
	maxStart     uint64
	minTraceID   [16]byte
	maxTraceID   [16]byte
}

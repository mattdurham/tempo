package writer

type builtBlock struct {
	colMinMax map[string]*blockColMinMax
	// NOTE: colStats field removed (2026-06-29, in-file block pruning removal).
	blockVectors [][]float32
	payload      []byte
	spanCount    int
	minStart     uint64
	maxStart     uint64
	minTraceID   [16]byte
	maxTraceID   [16]byte
}

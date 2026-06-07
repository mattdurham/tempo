package executor

type structuralSpanRec struct {
	spanID    [8]byte
	parentID  [8]byte
	parentIdx int
	blockIdx  int
	rowIdx    int
	nodeMatch uint8
	present   uint8
}

package executor

type groupResult struct {
	err        error
	data       map[int][]byte
	groupIdx   int
	blockCount int
	byteCount  int64
}

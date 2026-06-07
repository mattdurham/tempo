package writer

type tsIndexEntry struct {
	minTS   uint64
	maxTS   uint64
	blockID uint32
}

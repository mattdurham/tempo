package writer

// KLLBytes is a blockpack data type.
type KLLBytes struct {
	levels [][][]byte
	k      int
	n      int
	rng    uint64
}

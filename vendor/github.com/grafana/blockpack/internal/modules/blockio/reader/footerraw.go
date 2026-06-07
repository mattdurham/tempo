package reader

type footerRaw struct {
	headerOffset  uint64
	compactOffset uint64
	compactLen    uint32
}

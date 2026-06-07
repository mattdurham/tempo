package reader

type columnSketchData struct {
	presence    []uint64
	topkFP      [][]uint64
	topkCount   [][]uint16
	presentMap  []int
	distinctRaw []byte
	bloom       [][]byte
	numBlocks   int
}

package reader

type blockHeader struct {
	magic       uint32
	version     uint8
	spanCount   uint32
	columnCount uint32
}

package reader

type sketchIndex struct {
	columns   map[string]*columnSketchData
	numBlocks int
}

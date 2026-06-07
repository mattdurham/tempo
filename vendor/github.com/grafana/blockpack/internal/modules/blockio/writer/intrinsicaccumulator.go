package writer

type intrinsicAccumulator struct {
	flatCols map[string]*flatAccum
	dictCols map[string]*dictAccum
}

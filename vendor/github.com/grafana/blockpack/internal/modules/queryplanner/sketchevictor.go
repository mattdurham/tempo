package queryplanner

// SketchEvictor is a blockpack data type.
type SketchEvictor interface {
	EvictSketch()
}

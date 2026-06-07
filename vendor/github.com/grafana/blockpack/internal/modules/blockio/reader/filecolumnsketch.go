package reader

// FileColumnSketch is a blockpack data type.
type FileColumnSketch struct {
	TopK          []FileTopKEntry
	TotalDistinct uint32
}

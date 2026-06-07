package sketch

// CountMinSketch is a blockpack data type.
type CountMinSketch struct {
	rows [cmsD][cmsW]uint16
}

package queryplanner

// ColumnSketch is a blockpack data type.
type ColumnSketch interface {
	Presence() []uint64
	Distinct() []uint32
	DistinctAt(blockIdx int) uint32
	TopKMatch(valFP uint64) []uint16
	TopKMatchAt(valFP uint64, blockIdx int) uint16
	FuseContains(valHash uint64) []bool
	FuseContainsAt(valHash uint64, blockIdx int) bool
}

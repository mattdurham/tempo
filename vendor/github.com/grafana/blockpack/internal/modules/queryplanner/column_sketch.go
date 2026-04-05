package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// ColumnSketch provides bulk per-block sketch queries for one column.
// Methods return slices indexed by block number (length = total blocks).
// Nil return from BlockIndexer.ColumnSketch means no sketch data for the column.
type ColumnSketch interface {
	// Presence returns a bitset with 1 bit per block (1 = column present in block).
	Presence() []uint64
	// Distinct returns pre-computed HLL cardinality per block (0 for absent blocks).
	Distinct() []uint32
	// TopKMatch returns the TopK count for valFP per block (0 if not in top-K or absent).
	TopKMatch(valFP uint64) []uint16
	// FuseContains returns true per block if the fuse filter indicates valHash may be present.
	// Returns true (conservative) for blocks without a fuse filter.
	FuseContains(valHash uint64) []bool
}

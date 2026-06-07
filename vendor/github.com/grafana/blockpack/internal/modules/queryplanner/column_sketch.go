package queryplanner

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

// ColumnSketch provides bulk per-block sketch queries for one column.
// Methods return slices indexed by block number (length = total blocks).
// Nil return from BlockIndexer.ColumnSketch means no sketch data for the column.
//
// NOTE-022: Per-block scalar accessors (DistinctAt, TopKMatchAt, FuseContainsAt) eliminate
// per-call slice allocations from the bulk methods. Callers iterating candidates use the
// scalar accessors; callers needing all blocks at once use the bulk methods.

// Presence returns a bitset with 1 bit per block (1 = column present in block).

// Distinct returns pre-computed HLL cardinality per block (0 for absent blocks).

// DistinctAt returns the HLL cardinality for blockIdx (0 for absent or out-of-range).
// Zero-allocation alternative to Distinct()[blockIdx] for per-block iteration.

// TopKMatch returns the TopK count for valFP per block (0 if not in top-K or absent).

// TopKMatchAt returns the TopK count for valFP at blockIdx (0 if not in top-K, absent,
// or out-of-range). O(presentCount + K) per call where presentCount is the number of
// blocks where the column is present and K is the number of top-K entries (≤20).
// Zero-allocation alternative to TopKMatch(fp)[blockIdx].

// FuseContains returns true per block if the fuse filter indicates valHash may be present.
// Returns true (conservative) for blocks without a fuse filter.

// FuseContainsAt returns whether the fuse filter for blockIdx indicates valHash may be
// present. Returns true (conservative) for blocks without a fuse filter.
// Returns false for blocks where the column is absent (value cannot be present).
// Zero-allocation alternative to FuseContains(h)[blockIdx].

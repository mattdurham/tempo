// Package cube implements the binary on-disk format for pre-aggregated metrics cubes.
//
// NOTE: Key invariant — cells are sparse (12 bytes: minute[4] + dim1_id[2] + dim2_id[2] +
// count[4]), stored in snappy-compressed chunks (2048 cells nominal), sorted by (minute ASC,
// dim1_id ASC, dim2_id ASC). Random access via binary search on chunk directory and within-chunk.
// All byte encoding is LittleEndian. Magic number 0x43554245 ("CUBE" ASCII).
//
// See SPECS.md for wire formats, NOTES.md for design decisions, TESTS.md for test plan.
package cube

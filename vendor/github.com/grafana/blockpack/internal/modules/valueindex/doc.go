// Package valueindex implements an S3-native column value inverted index for blockpack.
//
// # Core Invariant
//
// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
//
// Value index files are self-contained binary files produced by the block builder and
// compacted by value. Each file covers exactly one column and contains all observed
// (value, trace_id, source_ref, time_sec) tuples for that column in one block builder flush.
//
// Files are named L<level>-<id>.blockpack; the compaction level is also stored in the VIMT
// section (authoritative on conflict with the filename).
//
// # File Layout
//
//	[VIMT section — column identity + compaction level + wall min/max ts]
//	[VHIX section — sorted (value_hash[16] → chunk_idx[4]) hash index]
//	[VINX section — snappy-chunked posting list: (value, trace_id, source_ref, time_sec)]
//	[VKLL section — KLL sketch over vi:value with k=10000]
//	[Footer  32 bytes — magic + offsets to each section]
//
// # Supported Predicates
//
// Equality, inequality, regex, range (>, >=, <, <=), and between.
// Range types and VectorF32 columns are never indexed.
package valueindex

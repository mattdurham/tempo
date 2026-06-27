// Package valueindexcompactor implements the value-index compactor service.
//
// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// The compactor is the third stage of the value-index pipeline (publisher #397,
// consumer #398, compactor #399). It periodically merges the many small L0
// value index files written by the consumer into fewer, larger L1/L2 files per
// (tenant, column), and drops entries whose originating blockpack file has been
// deleted by retention.
//
// # Pipeline
//
//	for each tenant:
//	  for each column_hash directory under <index_prefix>/<tenant>/:
//	    list value index files at the lowest compaction level present
//	    if count >= compact_threshold_files:
//	      open the readers, merge + dedup via valueindex.CompactFiles
//	      drop entries whose SourceRef no longer exists (RefChecker)
//	      write the merged L(level+1) output file(s)
//	      delete the compacted input files
//
// # Key properties
//
//   - Per-(tenant, column) compaction: each column directory is compacted
//     independently, so a hot column does not block a sparse one.
//   - Level-respecting merge: only same-level inputs are merged together
//     (VI-012); output level is input level + 1, mirroring blockpack's L0→L1→L2
//     model.
//   - Retention via source-existence: each entry's SourceRef is checked against
//     object storage; entries from a deleted blockpack are dropped rather than
//     propagated. HEAD checks are batched and cached per source path within a
//     compaction job, so each unique source is probed once.
//   - Stateless: write-then-delete with fresh output IDs means multiple
//     instances are safe — a crash before delete leaves the inputs in place and
//     they are recompacted on the next run; duplicate entries are removed by the
//     merge.
//
// # Decoupling
//
// Object storage is abstracted behind the IndexStore interface (List/Get/Put/
// Delete) and source-existence behind valueindex.RefChecker, so the
// orchestration is unit-testable without a real object store. The production
// IndexStore is supplied by tempo through the public valueindexcompactor
// subpackage.
package valueindexcompactor

// Package valuecountscompactor implements the value-counts (VCNT) compactor service.
//
// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.
//
// The compactor periodically merges the many small L0 VCNT files written per
// (tenant, column) into fewer, larger L1+ files, summing counts across files
// and dropping any group whose net count is <= 0 (a value's positive
// introduction canceled by a later negative retention delta).
//
// # Pipeline
//
//	for each tenant:
//	  for each column_hash directory under <index_prefix>/<tenant>/unique_values/:
//	    list VCNT files at the lowest compaction level present
//	    if count >= compact_threshold_files:
//	      decode inputs (self-describing or legacy single-chunk format)
//	      merge + sum + net-drop via valuecounts.Compact
//	      write the merged L(level+1) output file
//	      delete the compacted input files
//
// # Key properties
//
//   - Per-(tenant, column) compaction: each column directory is compacted
//     independently, so a hot column does not block a sparse one.
//   - Level-respecting merge: only same-level inputs are merged together;
//     output level is input level + 1.
//   - Retention via net-sum accounting: unlike valueindexcompactor's
//     source-existence probe, VCNT's own group-sum-drop-nonpositive rule
//     (valuecounts.Compact) is the sole retention signal — there is no
//     SourceExister equivalent.
//   - One-level directory walk: VCNT's key layout has no <type> segment
//     (unlike valueindexcompactor's three-level walk), so buildWorkList walks
//     unique_values/<colHash>/ directly.
//   - Decoded-record-count admission gate: MaxRecordsPerMerge bounds peak
//     decoded memory directly, independent of and in addition to
//     CompactBatchBytes' compressed-byte cap, since weak compression at VCNT's
//     file sizes can let a byte-capped batch admit far more records than the
//     byte number suggests.
//   - Write-then-delete, with a caveat unlike valueindexcompactor: a crash
//     before any Delete, or a fully successful Delete batch, degrades
//     gracefully — every input in a batch ends up either fully untouched
//     (recompacted next pass) or fully consumed. This is NOT "identical in
//     spirit" to valueindexcompactor's crash-safety guarantee for the
//     *partial*-failure case, though: valueindexcompactor is safe there
//     because valueindex.CompactFiles dedups by identity — duplicate entries
//     are removed by the merge, so reprocessing a surviving un-deleted input
//     is a no-op. valuecounts.Compact has no identity field on Record and
//     instead SUMS Count per merge key, so if store.Delete fails for even one
//     input in a batch while the rest succeed (and the merged output's Put
//     already succeeded), that surviving input can be summed a second time by
//     a later merge, permanently double-counting its value. mergeLevel
//     mitigates (does not eliminate) this by retrying each failed Delete a
//     few times before giving up, and surfacing an exhausted-retry failure via
//     both the returned error and a dedicated
//     blockpack_value_count_compactor_merge_delete_failed_after_retry_total
//     metric operators should alert on. See NOTE-VC-009 for the full
//     analysis and why this is a documented residual risk, not a design
//     guarantee.
//
// # Decoupling
//
// Object storage is abstracted behind the Store interface (List/ListDirs/Get/
// Put/Delete), so the orchestration is unit-testable without a real object
// store. The production Store is supplied by tempo through the public
// valuecountscompactor subpackage.
package valuecountscompactor

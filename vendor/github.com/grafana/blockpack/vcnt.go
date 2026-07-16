package blockpack

// vcnt.go — public surface for the VCNT value-counts pipeline (issue #400,
// NOTE-VC-001–004). Re-exports the minimal API tempo needs to accumulate
// per-column span counts during block writes and write L0 .vcnt files to S3,
// without importing internal packages directly.
//
// Key layout for .vcnt files (v1, still supported, unchanged):
//   <tenant>/value_counts/<colHash>/<type>/L0-<id>.vcnt
//
// As of issue #494, a second, additive v2 key layout also exists, embedding the file's
// wall-clock time range for O(1) input-side clustering during compaction:
//   <tenant>/value_counts/<colHash>/L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt
// See VCNTObjectKeyV2/VCNTFormatFilenameV2/VCNTParseFilenameV2 below.
//
// value_counts is a direct child of tenant -- a sibling of the VI index prefix tree
// and cube's own top-level prefix, not nested under either. This is a deliberate
// structural change from the earlier <tenant>/<indexPrefix>/unique_values/ layout: it
// eliminates the VI/VCNT directory-collision class of bug fixed by NOTE-VI-096, since
// the two subsystems no longer share any directory tree at all.

import (
	"path"

	"github.com/grafana/blockpack/internal/modules/valuecounts"
	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// VCNTRecord is one value-count observation: how many spans in a time window
// carried a specific (column, value) pair. Positive Count = observed; negative
// Count = delta-accounting subtraction (used by the compactor).
type VCNTRecord = valuecounts.Record

// VCNTChunkDirEntry is the chunk-directory entry for a .vcnt section, used for
// time-bounded query acceleration.
type VCNTChunkDirEntry = valuecounts.ChunkDirEntry

// SortVCNTRecords sorts records into canonical VCNT order:
// (ColumnName ASC, TimeStart ASC, Value ASC, Count ASC).
// Must be called before EncodeVCNTFile.
func SortVCNTRecords(records []VCNTRecord) {
	valuecounts.Sort(records)
}

// EncodeVCNTFile encodes a pre-sorted slice of VCNTRecords into a self-describing
// .vcnt file: a snappy-chunked body plus an embedded chunk directory and trailer,
// so any consumer can decode the file directly from object storage without a
// side-channel directory. perChunk ≤ 0 uses the package default (4096).
func EncodeVCNTFile(records []VCNTRecord, perChunk int) []byte {
	return valuecounts.EncodeVCNTFile(records, perChunk)
}

// CompactVCNTRecords merges records via delta accounting: records are grouped
// by (ColumnName, TimeStart, TimeEnd, Value), Count is summed per group, and
// groups whose summed Count is <= 0 are dropped. The returned slice is sorted
// in canonical VCNTRecord order (see SortVCNTRecords). Exposed so callers
// outside this module (e.g. tempo's block-builder tests) can exercise the
// real merge semantics end-to-end instead of reimplementing them.
func CompactVCNTRecords(records []VCNTRecord) []VCNTRecord {
	return valuecounts.Compact(records)
}

// VCNTSelectivityEstimate is an approximate span count for a leaf predicate
// `column = value`, produced by VCNTSelectivityInRange. Covered distinguishes
// "the index affirmatively knows this value matches zero live spans" (Covered,
// Count 0 — maximally selective) from "no VCNT coverage — unknown selectivity"
// (not Covered). See NOTE-VC-013.
type VCNTSelectivityEstimate = valuecounts.SelectivityEstimate

// VCNTSelectivityInRange approximates how many spans the leaf predicate
// `column = value` matches over [minTS, maxTS] against a single VCNT section,
// summing the net live Count for exactly that one canonical-encoded value. It is
// the cost oracle behind cost-based AND-leaf resolution ordering (issue #484,
// Phase 1); it opens no blockpack data files. See NOTE-VC-013.
func VCNTSelectivityInRange(
	data []byte,
	dir []VCNTChunkDirEntry,
	column string,
	value []byte,
	minTS, maxTS uint64,
) (VCNTSelectivityEstimate, error) {
	return valuecounts.SelectivityInRange(data, dir, column, value, minTS, maxTS)
}

// VCNTColHash returns the per-column directory hash used in the .vcnt object key.
func VCNTColHash(colName string) string {
	return valuecounts.ColHash(colName)
}

// VCNTFormatFilename returns a .vcnt filename for the given compaction level and ID.
func VCNTFormatFilename(level int, id string) string {
	return valuecounts.FormatFilename(level, id)
}

// VCNTNewID returns a new unique ID for .vcnt filenames.
func VCNTNewID() string {
	return valuecounts.NewID()
}

// VCNTObjectKey returns the full S3 object key for an L0 .vcnt file:
//
//	<tenant>/value_counts/<colHash>/L0-<id>.vcnt
func VCNTObjectKey(tenant, colName, id string) string {
	colHash := valuecounts.ColHash(colName)
	filename := valuecounts.FormatFilename(0, id)
	return path.Join(tenant, "value_counts", colHash, filename)
}

// VCNTFormatFilenameV2 returns a .vcnt filename that embeds the file's wall-clock time range
// for O(1) input-side clustering during compaction (issue #494).
func VCNTFormatFilenameV2(level int, wallMinSec, wallMaxSec uint64, id string) string {
	return valuecounts.FormatFilenameV2(level, wallMinSec, wallMaxSec, id)
}

// VCNTFileMeta holds the parsed metadata from a v2 .vcnt filename.
type VCNTFileMeta = valuecounts.FileMeta

// VCNTParseFilenameV2 parses a v2 .vcnt filename into its components.
func VCNTParseFilenameV2(name string) (VCNTFileMeta, error) {
	return valuecounts.ParseFilenameV2(name)
}

// VCNTRecordTimeRange scans records and returns (minSec, maxSec): the minimum TimeStart and
// maximum TimeEnd across every record, computed via a full O(n) scan (issue #494, R3/R5).
func VCNTRecordTimeRange(records []VCNTRecord) (minSec, maxSec uint64) {
	return valuecounts.TimeRange(records)
}

// VCNTDurationHistogramColumnName returns the synthetic VCNT column name a duration
// histogram's records are stored under for the given real column name (e.g.
// "span:duration" -> "span:duration#hist"). Tempo's writer keys its accumulator map
// under this name (#205).
func VCNTDurationHistogramColumnName(column string) string {
	return valuecounts.HistogramColumnName(column)
}

// VCNTDurationBucketBoundaryMillis returns the lower-boundary, in milliseconds, of the one
// fixed bucket durationMillis falls into (#205's floor rule). Tempo's writer must use this
// SAME rule the read side (valuecounts.DurationHistogramInRange) uses, so a span's duration is
// never bucketed differently on write than it is interpreted on read.
func VCNTDurationBucketBoundaryMillis(durationMillis uint64) uint64 {
	return valuecounts.DurationBucketBoundsMillis[valuecounts.BucketIndex(durationMillis)]
}

// VCNTDurationHistogramValue returns the canonical Record.Value encoding for a duration
// histogram bucket boundary (#205) — the exact inverse of what
// valuecounts.DurationHistogramInRange decodes.
func VCNTDurationHistogramValue(boundaryMillis uint64) []byte {
	return valuecounts.EncodeHistogramValue(boundaryMillis)
}

// VCNTObjectKeyV2 returns the full S3 object key for a .vcnt file using the v2 filename
// format, which embeds the file's wall-clock time range (issue #494):
//
//	<tenant>/value_counts/<colHash>/L<level>-<wallMinSec>-<wallMaxSec>-<id>.vcnt
func VCNTObjectKeyV2(tenant, colName, id string, wallMinSec, wallMaxSec uint64) string {
	colHash := valuecounts.ColHash(colName)
	filename := valuecounts.FormatFilenameV2(0, wallMinSec, wallMaxSec, id)
	return path.Join(tenant, "value_counts", colHash, filename)
}

// VCNTBuildSectionFromObjects decodes each raw .vcnt object (self-describing —
// via DecodeVCNTObject), merges all records via delta-accounting Compact,
// and re-encodes them into a single consolidated VCNT section (data + dir). The
// result is the exact (data, dir) shape CubeCreationTrigger.TryCreate /
// CheckCardinality consume, so a caller that has fetched the .vcnt objects covering
// a set of dimensions can produce one section spanning all of them in a single call
// — instead of feeding the cardinality gate nil data (issue #483).
//
// Objects that fail to decode are skipped rather than failing the whole build: a
// single corrupt or truncated .vcnt file must not defeat the cardinality gate for
// every other dimension. An empty or all-unreadable input yields an empty-but-valid
// section (dir may be empty), which CheckCardinality treats as "no VCNT coverage" —
// the gate then passes by default for the affected dimension, matching prior
// best-effort behavior. See NOTE-VC-012.
//
// The skip-don't-fail design is deliberate and unchanged (holistic-review/go-presubmit
// Fix 4), but the count of skipped objects (empty or undecodable) is now returned so a
// wave of unreadable objects is operator-visible instead of silently under-reporting
// cardinality — matching the visibility discipline the sibling
// valuecountscompactor/service.go:mergeLevel already applies to the identical
// DecodeVCNTObject failure via its filesQuarantined counter.
func VCNTBuildSectionFromObjects(objects [][]byte) (data []byte, dir []VCNTChunkDirEntry, skipped int) {
	var all []VCNTRecord
	for _, obj := range objects {
		if len(obj) == 0 {
			skipped++
			continue
		}
		recs, err := valuecounts.DecodeVCNTObject(obj)
		if err != nil {
			skipped++
			continue // skip undecodable object; other dims still gate correctly
		}
		all = append(all, recs...)
	}
	merged := valuecounts.Compact(all)
	data, dir = valuecounts.EncodeRecords(merged, 0)
	return data, dir, skipped
}

// VCNTFileOverlapsRange reports whether the .vcnt file named name should be fetched to answer a
// query over [minSec, maxSec] (argument order matches VCNTFileMeta.IsInTimeRange's own
// (queryMinSec, queryMaxSec) order — do not swap). Ported from tempo's tempodb/encoding/
// vblockpack/vcnt_prune.go (#508 Decision 3) so the moved cube_backfill_runner.go's
// buildVCNTSection can call it without a tempo-side dependency; tempo's own vcnt_prune.go now
// aliases this function so modules/frontend/vcnt_fetch.go's call site needs no change.
//
// A v1-shaped or otherwise unparseable name (VCNTParseFilenameV2 returns an error) ALWAYS
// returns true: unknown range means always fetch, never drop. This is an unconditional,
// hard-coded safety rule, not a tunable — it deliberately avoids repeating blockpack's own
// valueindex/discovery.go mistake (NOTE-VI-030) of treating "I don't know this file's range" as
// "skip it," which silently and permanently drops pre-v2-format files from ever being
// considered.
func VCNTFileOverlapsRange(name string, minSec, maxSec uint64) bool {
	meta, err := VCNTParseFilenameV2(name)
	if err != nil {
		return true
	}
	return meta.IsInTimeRange(minSec, maxSec)
}

// VIFileMeta holds the parsed metadata from a v2 value-index filename.
type VIFileMeta = valueindex.FileMeta

// VIParseFilenameV2 parses a v2 value-index filename into its time-range components.
func VIParseFilenameV2(name string) (VIFileMeta, error) {
	return valueindex.ParseFilenameV2(name)
}

// VIQueryResult is one matching span from a value-index file query.
// SourceRef is the originating block's object key; Value is the canonical column value.
type VIQueryResult = valueindex.QueryResult

// VIOpenReader opens a value-index file from in-memory bytes.
func VIOpenReader(data []byte) (*valueindex.Reader, error) {
	return valueindex.OpenReader(data)
}

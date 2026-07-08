package blockpack

// vcnt.go — public surface for the VCNT value-counts pipeline (issue #400,
// NOTE-VC-001–004). Re-exports the minimal API tempo needs to accumulate
// per-column span counts during block writes and write L0 .vcnt files to S3,
// without importing internal packages directly.
//
// Key layout for .vcnt files:
//   <tenant>/indexes/unique_values/<colHash>/<type>/L0-<id>.vcnt

import (
	"path"

	"github.com/grafana/blockpack/internal/modules/queryplan"
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
//	<tenant>/indexes/unique_values/<colHash>/L0-<id>.vcnt
func VCNTObjectKey(tenant, indexPrefix, colName, id string) string {
	colHash := valuecounts.ColHash(colName)
	filename := valuecounts.FormatFilename(0, id)
	return path.Join(tenant, indexPrefix, "unique_values", colHash, filename)
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

// Selectivity is the value-index selectivity verdict for a compiled query
// program: whether the query's lead predicate matches a small fraction of its
// column's population (Selective — index pruning helps), most of it
// (LowSelectivity — pruning skips almost nothing, e.g. `kind=server`), or
// carries no VCNT signal at all (UnknownSelectivity). Re-exported from the
// internal queryplan package so tempo can classify a query's selectivity on the
// live index query path without importing internals (issue #481 part 2, driving
// the eventual pointed-recent-block-first strategy choice — NOTE-QP-002/003).
type Selectivity = queryplan.Selectivity

const (
	// SelectivityUnknown means no VCNT cost signal covered the plan's lead leaf, so
	// selectivity cannot be determined. The caller keeps its default strategy.
	SelectivityUnknown = queryplan.UnknownSelectivity
	// SelectivitySelective means the lead leaf matches a small enough fraction of
	// its column's population that index pruning is expected to skip meaningful
	// work — build the index source over the window as usual.
	SelectivitySelective = queryplan.Selective
	// SelectivityLow means the lead leaf matches most of its column's population, so
	// index pruning skips almost nothing (the kind=server case in #481). A
	// limit-bounded caller should prefer pointed, recent-block-first execution.
	SelectivityLow = queryplan.LowSelectivity
)

// ClassifyProgramVCNT classifies the selectivity of a compiled query program from
// an already-decoded VCNT section (data + dir) over the query window [minTS, maxTS]
// (unix seconds). It composes the plan, VCNT cost oracle, and column-total oracle
// with the SAME window threaded through both, so the selectivity fraction's
// numerator (a leaf's live count) and denominator (its column's population) are
// always measured over the same time slice (NOTE-QP-003). It performs no
// object-storage I/O: data/dir are the caller's already-fetched VCNT section bytes.
//
// Returns SelectivityUnknown when the program has no plannable predicate, no leaf
// carried a Known VCNT cost, or the lead leaf's column has no VCNT population
// coverage — i.e. "no signal", on which the caller keeps its default strategy.
func ClassifyProgramVCNT(
	prog *Program,
	data []byte,
	dir []VCNTChunkDirEntry,
	minTS, maxTS uint64,
) Selectivity {
	return queryplan.ClassifyProgramVCNT(prog, data, dir, minTS, maxTS)
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

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
// Must be called before EncodeVCNTRecords.
func SortVCNTRecords(records []VCNTRecord) {
	valuecounts.Sort(records)
}

// EncodeVCNTRecords encodes a pre-sorted slice of VCNTRecords into snappy-chunked
// wire bytes plus a chunk directory. perChunk ≤ 0 uses the package default (4096).
func EncodeVCNTRecords(records []VCNTRecord, perChunk int) ([]byte, []VCNTChunkDirEntry) {
	return valuecounts.EncodeRecords(records, perChunk)
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

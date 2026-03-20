package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// NOTE-SL-016: blockLabelSet is the hot-path LabelSet implementation for pre-parsed blocks.
// Reads are zero-allocation — col.StringValue(rowIdx) is called directly.
// Writes go into an overlay map allocated on first write.
// A sync.Pool of *blockLabelSet eliminates per-row struct allocation on the scan loop.

import (
	"strconv"
	"strings"
	"sync"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Ensure blockLabelSet satisfies LabelSet at compile time.
var _ logqlparser.LabelSet = (*blockLabelSet)(nil)

// blockLabelSetPool is a sync.Pool of *blockLabelSet for the hot scan path.
// NOTE-SL-016: pool eliminates per-row struct allocation.
var blockLabelSetPool = &sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return &blockLabelSet{}
	},
}

// blockLabelSet is a LabelSet backed by block columns.
// NOTE-SL-016: hot-path implementation; reused across rows via resetForRow.
type blockLabelSet struct {
	block    *modules_reader.Block
	colMap   map[string]int           // label name (e.g. "service.name") → index into colNames
	overlay  map[string]string        // nil until first Set; allocated lazily
	deleted  map[string]bool          // nil until first Delete; allocated lazily
	colNames []string                 // original column names (e.g. "resource.service.name"); indexed by colMap values
	colCols  []*modules_reader.Column // parallel to colNames; resolved once per block
	rowIdx   int
}

// resetForRow resets the labelset for a new row in the same block.
// Clears overlay and deleted maps in-place (preserves allocations for reuse).
func (b *blockLabelSet) resetForRow(rowIdx int) {
	b.rowIdx = rowIdx
	for k := range b.overlay {
		delete(b.overlay, k)
	}
	for k := range b.deleted {
		delete(b.deleted, k)
	}
}

// Get returns the label value for key.
// Lookup order: overlay mutations > deleted set > block column.
// Non-string column types (int64, float64, bool, bytes) are converted to string
// via metricsColumnString to match the behavior of the previous logReadLabels path.
func (b *blockLabelSet) Get(key string) string {
	// 1. Overlay wins: pipeline stages (label_format, drop, keep) write here via Set/Delete.
	if b.overlay != nil {
		if v, ok := b.overlay[key]; ok {
			return v
		}
	}
	// 2. Check deleted set.
	if b.deleted != nil && b.deleted[key] {
		return ""
	}
	// 3. Look up column by label name.
	if b.block == nil {
		return ""
	}
	idx, ok := b.colMap[key]
	if !ok {
		return ""
	}
	col := b.colCols[idx]
	if col == nil || !col.IsPresent(b.rowIdx) {
		return ""
	}
	return metricsColumnString(col, b.rowIdx)
}

// Has reports whether key is present in the block at this row (even if the value is "").
// NOTE-SL-016: Has reflects block-level column presence, not decode state. A lazily-
// registered column (not in wantColumns) returns true here if the row has a value —
// the subsequent Get() call will trigger full decode (decodeNow) on first access.
// Materialize() intentionally excludes undecoded columns to avoid expensive zstd
// decompression for non-predicate columns; Get()-then-Materialize() is consistent
// because Get() decodes on first access, making the column visible to Materialize().
func (b *blockLabelSet) Has(key string) bool {
	// Deleted keys are not present.
	if b.deleted != nil && b.deleted[key] {
		return false
	}
	// Overlay takes precedence.
	if b.overlay != nil {
		if _, ok := b.overlay[key]; ok {
			return true
		}
	}
	// Check column presence.
	if b.block == nil {
		return false
	}
	idx, ok := b.colMap[key]
	if !ok {
		return false
	}
	col := b.colCols[idx]
	return col != nil && col.IsPresent(b.rowIdx)
}

// HasLive reports whether key has a live (immediately accessible) value.
// Returns true if the key is in the overlay (not deleted) OR if the backing block
// column is decoded (IsDecoded()==true) and present at this row.
// Undecoded block columns (lazily registered, rawEncoding != nil) return false —
// the column exists in storage but its value is not yet accessible without a
// full zstd decode. See NOTE-SL-016 and SPEC-001a.
func (b *blockLabelSet) HasLive(key string) bool {
	// Deleted keys are not live.
	if b.deleted != nil && b.deleted[key] {
		return false
	}
	// Overlay values are always live.
	if b.overlay != nil {
		if _, ok := b.overlay[key]; ok {
			return true
		}
	}
	// Check block column: live only if decoded.
	if b.block == nil {
		return false
	}
	idx, ok := b.colMap[key]
	if !ok {
		return false
	}
	col := b.colCols[idx]
	return col != nil && col.IsDecoded() && col.IsPresent(b.rowIdx)
}

// Set adds or overwrites key in the overlay.
func (b *blockLabelSet) Set(key, val string) {
	if b.overlay == nil {
		b.overlay = make(map[string]string, 4)
	}
	b.overlay[key] = val
	// Un-delete if previously deleted.
	if b.deleted != nil {
		delete(b.deleted, key)
	}
}

// Delete marks key as deleted. Get returns "" for deleted keys.
func (b *blockLabelSet) Delete(key string) {
	if b.deleted == nil {
		b.deleted = make(map[string]bool, 4)
	}
	b.deleted[key] = true
	if b.overlay != nil {
		delete(b.overlay, key)
	}
}

// HideBodyParsedColumns implements logqlparser.LabelSet. It marks all log.*
// ingest-time column keys as deleted so that Get() returns "" for them unless
// a pipeline stage explicitly sets the value via Set(). Called by LogfmtStage
// and JSONStage before parsing the body so that ingest-time last-wins column
// values cannot shadow body-re-parsed first-wins values. Set() un-deletes a
// key when the stage writes to it, so the parsed result takes precedence.
func (b *blockLabelSet) HideBodyParsedColumns() {
	for _, name := range b.colNames {
		labelName, ok := strings.CutPrefix(name, "log.")
		if !ok {
			continue
		}
		if b.deleted == nil {
			b.deleted = make(map[string]bool, len(b.colNames))
		}
		b.deleted[labelName] = true
	}
}

// Keys returns all present (non-deleted) label names in unspecified order.
func (b *blockLabelSet) Keys() []string {
	out := make([]string, 0, len(b.colMap)+len(b.overlay))
	// Add column-backed label names that are present at this row.
	for labelName, idx := range b.colMap {
		if b.deleted != nil && b.deleted[labelName] {
			continue
		}
		if b.overlay != nil {
			if _, overridden := b.overlay[labelName]; overridden {
				// Will be added from overlay below.
				continue
			}
		}
		if b.block != nil {
			col := b.colCols[idx]
			if col == nil || !col.IsPresent(b.rowIdx) {
				continue
			}
		}
		out = append(out, labelName)
	}
	// Add overlay-only keys (keys not in colMap).
	for k := range b.overlay {
		if b.deleted != nil && b.deleted[k] {
			continue
		}
		out = append(out, k)
	}
	return out
}

// Materialize copies all present labels into a new map[string]string.
func (b *blockLabelSet) Materialize() map[string]string {
	out := make(map[string]string, len(b.colMap)+len(b.overlay))
	// Column-backed values first.
	// Use metricsColumnString to handle all column types (int64, float64, bool, bytes, string).
	// Include keys for any IsPresent row even when the string representation is "" so that
	// Has() and Materialize() agree on what keys are present.
	if b.block != nil {
		for labelName, idx := range b.colMap {
			if b.deleted != nil && b.deleted[labelName] {
				continue
			}
			if b.overlay != nil {
				if _, overridden := b.overlay[labelName]; overridden {
					continue // overlay wins; added below
				}
			}
			col := b.colCols[idx]
			// NOTE-SL-016: skip columns that were not in wantColumns (rawEncoding != nil).
			// Calling any value accessor on an un-decoded column triggers decodeNow — expensive
			// zstd decompression. Only columns decoded during ColumnPredicate evaluation are
			// included; body-parsed non-predicate columns are intentionally excluded.
			if col == nil || !col.IsDecoded() || !col.IsPresent(b.rowIdx) {
				continue
			}
			out[labelName] = metricsColumnString(col, b.rowIdx)
		}
	}
	// Overlay additions and overrides.
	for k, v := range b.overlay {
		if b.deleted != nil && b.deleted[k] {
			continue
		}
		out[k] = v
	}
	return out
}

// metricsColumnString converts any column type to its string representation.
func metricsColumnString(col *modules_reader.Column, rowIdx int) string {
	if col == nil || !col.IsPresent(rowIdx) {
		return ""
	}
	switch col.Type {
	case modules_shared.ColumnTypeString, modules_shared.ColumnTypeRangeString:
		v, ok := col.StringValue(rowIdx)
		if ok {
			return v
		}
	case modules_shared.ColumnTypeInt64, modules_shared.ColumnTypeRangeInt64, modules_shared.ColumnTypeRangeDuration:
		v, ok := col.Int64Value(rowIdx)
		if ok {
			return strconv.FormatInt(v, 10)
		}
	case modules_shared.ColumnTypeUint64, modules_shared.ColumnTypeRangeUint64:
		v, ok := col.Uint64Value(rowIdx)
		if ok {
			return strconv.FormatUint(v, 10)
		}
	case modules_shared.ColumnTypeFloat64, modules_shared.ColumnTypeRangeFloat64:
		v, ok := col.Float64Value(rowIdx)
		if ok {
			return strconv.FormatFloat(v, 'f', -1, 64)
		}
	case modules_shared.ColumnTypeBool:
		v, ok := col.BoolValue(rowIdx)
		if ok {
			return strconv.FormatBool(v)
		}
	case modules_shared.ColumnTypeBytes, modules_shared.ColumnTypeRangeBytes:
		v, ok := col.BytesValue(rowIdx)
		if ok {
			return string(v)
		}
	}
	return ""
}

// acquireBlockLabelSet retrieves a *blockLabelSet from the pool and initializes it for use.
func acquireBlockLabelSet(
	block *modules_reader.Block,
	rowIdx int,
	colNames []string,
	colMap map[string]int,
	colCols []*modules_reader.Column,
) *blockLabelSet {
	bls := blockLabelSetPool.Get().(*blockLabelSet) //nolint:forcetypeassert
	bls.block = block
	bls.rowIdx = rowIdx
	bls.colNames = colNames
	bls.colMap = colMap
	bls.colCols = colCols
	// overlay and deleted were cleared by releaseBlockLabelSet before pool.Put.
	return bls
}

// releaseBlockLabelSet clears state and returns bls to the pool.
// Callers must not use bls after this call.
func releaseBlockLabelSet(bls *blockLabelSet) {
	bls.block = nil
	bls.colNames = nil
	bls.colMap = nil
	bls.colCols = nil
	for k := range bls.overlay {
		delete(bls.overlay, k)
	}
	for k := range bls.deleted {
		delete(bls.deleted, k)
	}
	blockLabelSetPool.Put(bls)
}

// buildBlockColMapsWithLogCache combines column-map construction and log string column caching
// into a single block.Columns() pass, reducing per-block iteration overhead.
// logStrNames and logStrCols are the same outputs as buildLogStringColCache.
func buildBlockColMapsWithLogCache(
	block *modules_reader.Block,
) (colNames []string, colMap map[string]int, colCols []*modules_reader.Column, logStrNames []string, logStrCols []*modules_reader.Column) {
	cols := block.Columns()
	colNames = make([]string, 0, len(cols))
	colMap = make(map[string]int, len(cols))
	colCols = make([]*modules_reader.Column, 0, len(cols))

	type candidate struct {
		col     *modules_reader.Column
		colName string
		colType modules_shared.ColumnType
	}
	logCandidates := make(map[string]candidate, len(cols))

	for key, c := range cols {
		name := key.Name
		isString := key.Type == modules_shared.ColumnTypeString
		if after, ok := strings.CutPrefix(name, "resource."); ok {
			if _, dup := colMap[after]; dup {
				continue
			}
			colMap[after] = len(colNames)
			colNames = append(colNames, name)
			colCols = append(colCols, c)
		} else if after, ok := strings.CutPrefix(name, "log."); ok {
			// Collect for log string cache (ColumnTypeString and ColumnTypeUUID only;
			// ColumnTypeRangeString body-auto-parsed columns are intentionally excluded).
			if key.Type == modules_shared.ColumnTypeString || key.Type == modules_shared.ColumnTypeUUID {
				logStrNames = append(logStrNames, name)
				logStrCols = append(logStrCols, c)
			}
			// Collect for label map (precedence: ColumnTypeString wins over ColumnTypeRangeString).
			if prev, exists := logCandidates[after]; exists {
				prevIsString := prev.colType == modules_shared.ColumnTypeString
				if prevIsString || !isString {
					continue
				}
			}
			logCandidates[after] = candidate{colName: name, col: c, colType: key.Type}
		}
	}

	for labelName, cand := range logCandidates {
		if _, covered := colMap[labelName]; covered {
			continue
		}
		colMap[labelName] = len(colNames)
		colNames = append(colNames, cand.colName)
		colCols = append(colCols, cand.col)
	}

	return colNames, colMap, colCols, logStrNames, logStrCols
}

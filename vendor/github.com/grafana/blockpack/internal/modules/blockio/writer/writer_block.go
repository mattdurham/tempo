package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// pendingSpan is a lightweight span record buffered before sorting and flushing.
// Stores only the sort keys and proto pointers; full OTLP→column decoding is deferred
// to addRowFromProto, eliminating per-span AttrKV materialization and attrSlab growth.
//
// Size: ~88 bytes (4 pointer fields) vs shared.BufferedSpan ~344 bytes (13 pointer fields).
// The proto fields are kept alive by w.protoRoots for the duration of the flush cycle.
type pendingSpan struct {
	rs         *tracev1.ResourceSpans // proto pointer; kept alive by w.protoRoots
	ss         *tracev1.ScopeSpans    // proto pointer; kept alive by w.protoRoots
	span       *tracev1.Span          // proto pointer; kept alive by w.protoRoots
	svcName    string                 // sort key (primary); zero-copy reference into proto
	minHashSig [4]uint64              // sort key (secondary)
	traceID    [16]byte               // sort key (tertiary)
}

// blockBuilder manages construction of a single block.
type blockBuilder struct {
	// Cached intrinsic column builders for direct access without map lookup or
	// interface dispatch. Pre-created in newBlockBuilder with pre-allocated
	// value/present slices to eliminate growslice during the per-span append loop.
	colTraceID   *bytesColumnBuilder
	colSpanID    *bytesColumnBuilder
	colParentID  *bytesColumnBuilder
	colSpanName  *stringColumnBuilder
	colSpanKind  *int64ColumnBuilder
	colSpanStart *uint64ColumnBuilder
	colSpanEnd   *uint64ColumnBuilder
	colSpanDur   *uint64ColumnBuilder

	columns   map[string]columnBuilder // column name → builder (all columns, for finalize)
	traceRows map[[16]byte][]uint16    // trace_id → span row indices within this block

	// Column name caches: attribute key → full column name (e.g. "http.method" → "span.http.method").
	// Populated lazily on first encounter within a block; eliminates per-span string concat allocs
	// in addRowFromProto. Separate caches per prefix avoid key collisions between namespaces.
	spanColNames     map[string]string
	resourceColNames map[string]string
	scopeColNames    map[string]string

	rangeVals     []blockRangeValue // range index entries for this block
	sparseColumns []columnBuilder   // non-intrinsic columns that may need null-filling

	spanCount int
	// spanHint is the expected total span count for this block.
	// Used as the initial capacity for dynamically-created attribute column builder
	// value/present slices, eliminating growslice calls in the per-span append loop.
	spanHint   int
	minStart   uint64
	maxStart   uint64
	bloom      [32]byte
	minTraceID [16]byte
	maxTraceID [16]byte
}

type blockRangeValue struct {
	colName string
	key     string // encoded value key
	colType shared.ColumnType
}

// newBlockBuilder creates an empty block builder.
// spanHint is the expected number of spans in this block; used to pre-allocate
// value/present slices in the intrinsic column builders, eliminating growslice calls
// during the per-span append loop.
// rangeVals is preallocated to 4096 to reduce growslice calls during the per-span append loop.
func newBlockBuilder(spanHint int) *blockBuilder {
	// Estimate range index entries per span: ~7 fixed intrinsic columns +
	// typical attribute count (20–30 span+resource+scope attrs) = ~30 entries/span.
	// Over-estimating by a small factor eliminates all growslice events for b.rangeVals.
	rangeValsCap := max(spanHint*32, 4096)
	b := &blockBuilder{
		columns:          make(map[string]columnBuilder, 32),
		traceRows:        make(map[[16]byte][]uint16, 16),
		rangeVals:        make([]blockRangeValue, 0, rangeValsCap),
		sparseColumns:    make([]columnBuilder, 0, 32),
		spanHint:         spanHint,
		spanColNames:     make(map[string]string, 32),
		resourceColNames: make(map[string]string, 16),
		scopeColNames:    make(map[string]string, 8),
	}

	// Pre-create intrinsic column builders with pre-allocated value/present slices.
	// Registering them in b.columns makes them visible to finalize().
	// They are NOT added to b.sparseColumns because they are always written in addRow,
	// so fillNullsForRow never needs to null-fill them.

	b.colTraceID = &bytesColumnBuilder{
		colName: traceIDColumnName,
		values:  make([][]byte, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colSpanID = &bytesColumnBuilder{
		colName: "span:id",
		values:  make([][]byte, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colParentID = &bytesColumnBuilder{
		colName: "span:parent_id",
		values:  make([][]byte, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colSpanName = &stringColumnBuilder{
		colName: "span:name",
		values:  make([]string, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colSpanKind = &int64ColumnBuilder{
		values:  make([]int64, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colSpanStart = &uint64ColumnBuilder{
		colName: "span:start",
		values:  make([]uint64, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colSpanEnd = &uint64ColumnBuilder{
		colName: "span:end",
		values:  make([]uint64, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}
	b.colSpanDur = &uint64ColumnBuilder{
		colName: "span:duration",
		values:  make([]uint64, 0, spanHint),
		present: make([]bool, 0, spanHint),
	}

	// Register in map so finalize() can collect them.
	b.columns[traceIDColumnName] = b.colTraceID
	b.columns["span:id"] = b.colSpanID
	b.columns["span:parent_id"] = b.colParentID
	b.columns["span:name"] = b.colSpanName
	b.columns["span:kind"] = b.colSpanKind
	b.columns["span:start"] = b.colSpanStart
	b.columns["span:end"] = b.colSpanEnd
	b.columns["span:duration"] = b.colSpanDur

	// Seed bloom filter for the pre-created intrinsic columns.
	for _, name := range []string{
		traceIDColumnName, "span:id", "span:parent_id", "span:name",
		"span:kind", "span:start", "span:end", "span:duration",
	} {
		shared.AddToBloom(b.bloom[:], name)
	}

	return b
}

// addColumn ensures a column builder exists for the given name/type.
// Returns the builder. Dynamic (non-intrinsic) columns are also tracked in
// b.sparseColumns so that fillNullsForRow can iterate only columns that may
// need null-filling, skipping the always-present intrinsic columns.
func (b *blockBuilder) addColumn(name string, typ shared.ColumnType) columnBuilder {
	if cb, ok := b.columns[name]; ok {
		return cb
	}
	cb := newColumnBuilder(typ, name, max(b.spanHint-b.spanCount, 0))
	// Backfill null rows for all spans written before this column first appeared.
	// Without this, the column's row_count would be less than the block's spanCount,
	// causing a "row_count != spanCount" parse error in the reader.
	for range b.spanCount {
		switch typ {
		case shared.ColumnTypeString, shared.ColumnTypeRangeString:
			cb.addString("", false)
		case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
			cb.addInt64(0, false)
		case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
			cb.addUint64(0, false)
		case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
			cb.addFloat64(0, false)
		case shared.ColumnTypeBool:
			cb.addBool(false, false)
		default:
			cb.addBytes(nil, false)
		}
	}
	b.columns[name] = cb
	// Track in sparseColumns for efficient null-filling (intrinsics are excluded
	// because they are always written in addRow).
	b.sparseColumns = append(b.sparseColumns, cb)
	// Update bloom filter.
	shared.AddToBloom(b.bloom[:], name)
	return cb
}

// fillNullsForRow adds a null row to all sparse (non-intrinsic) columns whose
// rowCount is behind rowIdx. Called after all present values for a row have been
// written; any column still at rowIdx hasn't been written this row and needs a
// null entry.
//
// Iterates b.sparseColumns (non-intrinsic columns) rather than the full b.columns
// map, avoiding overhead for the always-present intrinsic column builders.
func (b *blockBuilder) fillNullsForRow(rowIdx int) {
	for _, cb := range b.sparseColumns {
		if cb.rowCount() <= rowIdx {
			switch cb.colType() {
			case shared.ColumnTypeString, shared.ColumnTypeRangeString:
				cb.addString("", false)
			case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
				cb.addInt64(0, false)
			case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
				cb.addUint64(0, false)
			case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
				cb.addFloat64(0, false)
			case shared.ColumnTypeBool:
				cb.addBool(false, false)
			default:
				cb.addBytes(nil, false)
			}
		}
	}
}

// addRowFromProto adds all column values for one span row from a pendingSpan.
// Full OTLP→column decoding happens here (deferred from AddTracesData) to eliminate
// per-span AttrKV materialization and attrSlab growth.
// Column names for dynamic attributes are interned via the block-level name caches.
func (b *blockBuilder) addRowFromProto(ps *pendingSpan, rowIdx int) {
	span := ps.span

	// --- Intrinsic columns (direct concrete-type calls, no map lookup or interface dispatch) ---

	// trace:id — always present; excluded from the range index.
	b.colTraceID.addBytes(span.TraceId, true)

	// span:id — may be absent; always written (null or present) to keep rowCount consistent.
	spanIDPresent := len(span.SpanId) > 0
	b.colSpanID.addBytes(span.SpanId, spanIDPresent)
	if spanIDPresent {
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:id",
			key:     string(span.SpanId),
			colType: shared.ColumnTypeBytes,
		})
	}

	// span:parent_id — may be absent; always written.
	parentIDPresent := len(span.ParentSpanId) > 0
	b.colParentID.addBytes(span.ParentSpanId, parentIDPresent)
	if parentIDPresent {
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:parent_id",
			key:     string(span.ParentSpanId),
			colType: shared.ColumnTypeBytes,
		})
	}

	// span:name — always present.
	b.colSpanName.addString(span.Name, true)
	if span.Name != "" {
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:name",
			key:     span.Name,
			colType: shared.ColumnTypeString,
		})
	}

	// span:kind — always present.
	spanKind := int64(span.Kind)
	b.colSpanKind.addInt64(spanKind, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(spanKind)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:kind",
			key:     string(tmp[:]),
			colType: shared.ColumnTypeInt64,
		})
	}

	// span:start — always present.
	b.colSpanStart.addUint64(span.StartTimeUnixNano, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.StartTimeUnixNano)
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:start",
			key:     string(tmp[:]),
			colType: shared.ColumnTypeUint64,
		})
	}

	// span:end — always present.
	b.colSpanEnd.addUint64(span.EndTimeUnixNano, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.EndTimeUnixNano)
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:end",
			key:     string(tmp[:]),
			colType: shared.ColumnTypeUint64,
		})
	}

	// span:duration — always present.
	var dur uint64
	if span.EndTimeUnixNano >= span.StartTimeUnixNano {
		dur = span.EndTimeUnixNano - span.StartTimeUnixNano
	}
	b.colSpanDur.addUint64(dur, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], dur)
		b.rangeVals = append(b.rangeVals, blockRangeValue{
			colName: "span:duration",
			key:     string(tmp[:]),
			colType: shared.ColumnTypeUint64,
		})
	}

	// --- Conditional intrinsic columns ---

	if span.Status != nil {
		if span.Status.Code != 0 {
			b.addPresent("span:status", shared.ColumnTypeInt64, shared.AttrValue{
				Type: shared.ColumnTypeInt64,
				Int:  int64(span.Status.Code),
			})
		}
		if span.Status.Message != "" {
			b.addPresent("span:status_message", shared.ColumnTypeString, shared.AttrValue{
				Type: shared.ColumnTypeString,
				Str:  span.Status.Message,
			})
		}
	}

	if span.TraceState != "" {
		b.addPresent("trace:state", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  span.TraceState,
		})
	}

	if ps.rs != nil && ps.rs.SchemaUrl != "" {
		b.addPresent("resource:schema_url", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  ps.rs.SchemaUrl,
		})
	}

	if ps.ss != nil && ps.ss.SchemaUrl != "" {
		b.addPresent("scope:schema_url", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  ps.ss.SchemaUrl,
		})
	}

	// --- Span attributes (interned column names, zero-copy values) ---
	for _, kv := range span.Attributes {
		if kv == nil {
			continue
		}
		name := b.internColName(kv.Key, b.spanColNames, "span.")
		val := protoToAttrValue(kv.Value)
		b.addPresent(name, val.Type, val)
	}

	// --- Resource attributes ---
	if ps.rs != nil && ps.rs.Resource != nil {
		for _, kv := range ps.rs.Resource.Attributes {
			if kv == nil {
				continue
			}
			name := b.internColName(kv.Key, b.resourceColNames, "resource.")
			val := protoToAttrValue(kv.Value)
			b.addPresent(name, val.Type, val)
		}
	}

	// --- Scope attributes ---
	if ps.ss != nil && ps.ss.Scope != nil {
		for _, kv := range ps.ss.Scope.Attributes {
			if kv == nil {
				continue
			}
			name := b.internColName(kv.Key, b.scopeColNames, "scope.")
			val := protoToAttrValue(kv.Value)
			b.addPresent(name, val.Type, val)
		}
	}

	// Fill null values for columns that existed before this row but weren't written.
	b.fillNullsForRow(rowIdx)

	// Update min/max start time.
	if b.spanCount == 0 {
		b.minStart = span.StartTimeUnixNano
		b.maxStart = span.StartTimeUnixNano
		b.minTraceID = ps.traceID
		b.maxTraceID = ps.traceID
	} else {
		if span.StartTimeUnixNano < b.minStart {
			b.minStart = span.StartTimeUnixNano
		}
		if span.StartTimeUnixNano > b.maxStart {
			b.maxStart = span.StartTimeUnixNano
		}
		if bytes.Compare(ps.traceID[:], b.minTraceID[:]) < 0 {
			b.minTraceID = ps.traceID
		}
		if bytes.Compare(ps.traceID[:], b.maxTraceID[:]) > 0 {
			b.maxTraceID = ps.traceID
		}
	}

	// Track trace → row indices.
	b.traceRows[ps.traceID] = append(b.traceRows[ps.traceID], uint16(rowIdx)) //nolint:gosec // safe: rowIdx bounded by MaxBlockSpans (65535)

	b.spanCount++
}

// internColName returns the full column name for the given attribute key and prefix,
// using cache to avoid per-span string concatenation allocations.
// On first encounter of key in cache: allocates "prefix+key" once and stores it.
// On subsequent encounters: returns the cached string with zero allocation.
func (b *blockBuilder) internColName(key string, cache map[string]string, prefix string) string {
	if cached, ok := cache[key]; ok {
		return cached
	}
	name := prefix + key
	cache[key] = name
	return name
}

// addPresent writes a present (non-null) attribute value to the named column
// and feeds the range index. Promoted from a closure inside addRow to eliminate
// the per-span closure heap allocation (~24 bytes per span at scale).
func (b *blockBuilder) addPresent(name string, typ shared.ColumnType, val shared.AttrValue) {
	cb := b.addColumn(name, typ)
	switch typ {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		cb.addString(val.Str, true)
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		cb.addInt64(val.Int, true)
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		cb.addUint64(val.Uint, true)
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		cb.addFloat64(val.Float, true)
	case shared.ColumnTypeBool:
		cb.addBool(val.Bool, true)
	default: // bytes / rangebytes
		cb.addBytes(val.Bytes, true)
	}

	// Feed range column index.
	// Excluded: trace:id (unique per trace, not useful for block pruning)
	//           Bool (no Range* equivalent; cardinality is always ≤2)
	if name != traceIDColumnName && typ != shared.ColumnTypeBool {
		if key := encodeRangeKey(typ, val); key != "" {
			b.rangeVals = append(b.rangeVals, blockRangeValue{
				colName: name,
				key:     key,
				colType: typ,
			})
		}
	}
}

// finalize encodes all columns, writes the block payload, and returns:
// - the serialized block bytes (header + column metadata + stats + data + trace table)
// - per-column byte offsets/lengths for the column index
func (b *blockBuilder) finalize(enc *zstdEncoder) ([]byte, columnIndexBlock, error) {
	// Collect and sort column names lexicographically.
	colNames := make([]string, 0, len(b.columns))
	for name := range b.columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	colCount := len(colNames)

	// Build column data and stats blobs.
	type colBlob struct {
		name      string
		dataBlob  []byte
		statsBlob []byte
		typ       shared.ColumnType
	}
	blobs := make([]colBlob, 0, colCount)

	for _, name := range colNames {
		cb := b.columns[name]
		data, err := cb.buildData(enc)
		if err != nil {
			return nil, columnIndexBlock{}, fmt.Errorf("finalize column %q: %w", name, err)
		}
		stats := cb.buildStats()
		blobs = append(blobs, colBlob{
			name:      name,
			typ:       cb.colType(),
			dataBlob:  data,
			statsBlob: stats,
		})
	}

	// Build trace table bytes.
	traceTableBytes := buildBlockTraceTable(b.traceRows)

	// --- Compute layout ---
	// Block header: 24 bytes
	// Column metadata array: sum of (2 + len(name) + 1 + 8 + 8 + 8 + 8) per column
	// Column stats section: immediately after metadata
	// Column data section: immediately after stats
	// Trace table: at the end

	headerSize := 24

	// Compute column metadata array size.
	colMetaSize := 0
	for _, bl := range blobs {
		// name_len[2] + name + col_type[1] + data_offset[8] + data_len[8] + stats_offset[8] + stats_len[8]
		colMetaSize += 2 + len(bl.name) + 1 + 8 + 8 + 8 + 8
	}

	// Stats section start.
	statsStart := headerSize + colMetaSize

	// Compute per-column stats offsets and total stats size.
	statsSizes := make([]int, colCount)
	totalStatsSize := 0
	for i, bl := range blobs {
		statsSizes[i] = len(bl.statsBlob)
		totalStatsSize += len(bl.statsBlob)
	}

	// Data section start.
	dataStart := statsStart + totalStatsSize

	// Compute per-column data offsets and total data size.
	dataSizes := make([]int, colCount)
	totalDataSize := 0
	for i, bl := range blobs {
		dataSizes[i] = len(bl.dataBlob)
		totalDataSize += len(bl.dataBlob)
	}

	// Trace table starts after data.
	traceTableStart := dataStart + totalDataSize

	// Total block size.
	totalSize := traceTableStart + len(traceTableBytes)

	// Allocate the output buffer.
	payload := make([]byte, 0, totalSize)

	// --- Write block header (24 bytes) ---
	// magic[4]
	payload = appendUint32LE(payload, shared.MagicNumber)
	// version[1]
	payload = append(payload, shared.VersionV11)
	// reserved[3]
	payload = append(payload, 0, 0, 0)
	// span_count[4]
	payload = appendUint32LE(payload, uint32(b.spanCount)) //nolint:gosec // safe: spanCount bounded by MaxBlockSpans (65535)
	// column_count[4]
	payload = appendUint32LE(payload, uint32(colCount)) //nolint:gosec // safe: column count bounded by MaxColumns
	// trace_count[4]
	payload = appendUint32LE(payload, uint32(len(b.traceRows))) //nolint:gosec // safe: trace count bounded by MaxTraceCount
	// trace_table_len[4]
	payload = appendUint32LE(payload, uint32(len(traceTableBytes))) //nolint:gosec // safe: trace table size bounded by block size

	// --- Write column metadata array ---
	// Track running offsets for stats and data.
	curStatsOff := uint64(statsStart)
	curDataOff := uint64(dataStart)

	colIdxEntries := make([]columnIndexEntry, 0, colCount)

	for i, bl := range blobs {
		statsOff := curStatsOff
		statsLen := uint64(statsSizes[i])
		dataOff := curDataOff
		dataLen := uint64(dataSizes[i])

		// name_len[2 LE]
		payload = append(payload, byte(len(bl.name)), byte(len(bl.name)>>8)) //nolint:gosec // safe: col name len bounded by MaxNameLen (1024)
		// name
		payload = append(payload, bl.name...)
		// col_type[1]
		payload = append(payload, byte(bl.typ))
		// data_offset[8 LE]
		payload = appendUint64LE(payload, dataOff)
		// data_len[8 LE]
		payload = appendUint64LE(payload, dataLen)
		// stats_offset[8 LE]
		payload = appendUint64LE(payload, statsOff)
		// stats_len[8 LE]
		payload = appendUint64LE(payload, statsLen)

		// Record column index entry (offsets relative to block start).
		colIdxEntries = append(colIdxEntries, columnIndexEntry{
			name:   bl.name,
			offset: uint32(dataOff), //nolint:gosec // safe: block offsets are bounded by max block size (fits in uint32)
			length: uint32(dataLen), //nolint:gosec // safe: column data length bounded by max block size (fits in uint32)
		})

		curStatsOff += statsLen
		curDataOff += dataLen
	}

	// --- Write column stats section ---
	for _, bl := range blobs {
		payload = append(payload, bl.statsBlob...)
	}

	// --- Write column data section ---
	for _, bl := range blobs {
		payload = append(payload, bl.dataBlob...)
	}

	// --- Write trace table ---
	payload = append(payload, traceTableBytes...)

	return payload, columnIndexBlock{entries: colIdxEntries}, nil
}

// buildBlockTraceTable serializes the block-level trace table.
// Format: trace_count × {trace_id[16] + span_count[2 LE] + span_indices[span_count × uint16 LE]}
func buildBlockTraceTable(traceRows map[[16]byte][]uint16) []byte {
	if len(traceRows) == 0 {
		return nil
	}

	// Sort trace IDs for deterministic output.
	traceIDs := make([][16]byte, 0, len(traceRows))
	for tid := range traceRows {
		traceIDs = append(traceIDs, tid)
	}
	sort.Slice(traceIDs, func(i, j int) bool {
		return bytes.Compare(traceIDs[i][:], traceIDs[j][:]) < 0
	})

	var buf []byte
	for _, tid := range traceIDs {
		rows := traceRows[tid]
		buf = append(buf, tid[:]...)
		buf = append(buf, byte(len(rows)), byte(len(rows)>>8)) //nolint:gosec // safe: row count bounded by MaxBlockSpans (65535)
		for _, r := range rows {
			buf = append(buf, byte(r), byte(r>>8))
		}
	}
	return buf
}

// appendUint64LE appends a uint64 in little-endian byte order to buf.
func appendUint64LE(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

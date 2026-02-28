package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
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
// Rows are added one at a time via addRowFromProto, then finalized in one shot.
// The mutable builder pattern (vs. a single BuildBlock([]rows) function) exists
// because column builders must track running state (null-fill, bloom filter,
// range values) that is simpler to maintain incrementally than in a batch pass.
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
	traceRows map[[16]byte][]uint16    // trace_id → span row indices; used by writer.go to build file-level trace index

	// Column name caches: attribute key → full column name (e.g. "http.method" → "span.http.method").
	// Populated lazily on first encounter within a block; eliminates per-span string concat allocs
	// in addRowFromProto. Separate caches per prefix avoid key collisions between namespaces.
	spanColNames     map[string]string
	resourceColNames map[string]string
	scopeColNames    map[string]string

	// colMinMax tracks the per-column minimum and maximum encoded key observed
	// within this block. At block write time the writer records exactly two values
	// per column into the file-level range index (vs. O(spans × attrs) previously).
	// The map key is the column name; the value holds min/max encoded keys.
	colMinMax     map[string]*blockColMinMax // column name → min/max for this block
	sparseColumns []columnBuilder            // non-intrinsic columns that may need null-filling

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

// blockColMinMax records the minimum and maximum encoded key seen for one column
// within a single block. The key encoding matches encodeRangeKey (8-byte LE for
// numeric types, raw string/bytes for string/bytes types).
type blockColMinMax struct {
	colName string
	minKey  string // encoded minimum value key for this block
	maxKey  string // encoded maximum value key for this block
	colType shared.ColumnType
}

// builtBlock holds the outputs of buildBlock: the serialized payload and all
// per-block statistics extracted from blockBuilder after finalization.
// It is an intermediate value type used only inside buildAndWriteBlock.
type builtBlock struct {
	traceRows  map[[16]byte][]uint16
	colMinMax  map[string]*blockColMinMax // per-column min/max for this block
	payload    []byte
	spanCount  int
	minStart   uint64
	maxStart   uint64
	bloom      [32]byte
	minTraceID [16]byte
	maxTraceID [16]byte
}

// buildBlock constructs a single block from the given pending spans and returns
// the serialized payload together with all per-block statistics.
// It is a thin wrapper around the newBlockBuilder → addRowFromProto loop → finalize
// pattern, keeping blockBuilder as an unexported implementation detail.
func buildBlock(pending []pendingSpan, enc *zstdEncoder) (builtBlock, error) {
	bb := newBlockBuilder(len(pending))
	for rowIdx := range pending {
		bb.addRowFromProto(&pending[rowIdx], rowIdx)
	}
	payload, err := bb.finalize(enc)
	if err != nil {
		return builtBlock{}, err
	}
	return builtBlock{
		payload:    payload,
		spanCount:  bb.spanCount,
		minStart:   bb.minStart,
		maxStart:   bb.maxStart,
		minTraceID: bb.minTraceID,
		maxTraceID: bb.maxTraceID,
		bloom:      bb.bloom,
		traceRows:  bb.traceRows,
		colMinMax:  bb.colMinMax,
	}, nil
}

// newBlockBuilder creates an empty block builder.
// spanHint is the expected number of spans in this block; used to pre-allocate
// value/present slices in the intrinsic column builders, eliminating growslice calls
// during the per-span append loop.
func newBlockBuilder(spanHint int) *blockBuilder {
	b := &blockBuilder{
		columns:          make(map[string]columnBuilder, 32),
		traceRows:        make(map[[16]byte][]uint16, 16),
		colMinMax:        make(map[string]*blockColMinMax, 64),
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
		b.updateMinMax("span:id", shared.ColumnTypeBytes, string(span.SpanId))
	}

	// span:parent_id — may be absent; always written.
	parentIDPresent := len(span.ParentSpanId) > 0
	b.colParentID.addBytes(span.ParentSpanId, parentIDPresent)
	if parentIDPresent {
		b.updateMinMax("span:parent_id", shared.ColumnTypeBytes, string(span.ParentSpanId))
	}

	// span:name — always present.
	b.colSpanName.addString(span.Name, true)
	if span.Name != "" {
		b.updateMinMax("span:name", shared.ColumnTypeString, span.Name)
	}

	// span:kind — always present.
	spanKind := int64(span.Kind)
	b.colSpanKind.addInt64(spanKind, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(spanKind)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
		b.updateMinMax("span:kind", shared.ColumnTypeInt64, string(tmp[:]))
	}

	// span:start — always present.
	b.colSpanStart.addUint64(span.StartTimeUnixNano, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.StartTimeUnixNano)
		b.updateMinMax("span:start", shared.ColumnTypeUint64, string(tmp[:]))
	}

	// span:end — always present.
	b.colSpanEnd.addUint64(span.EndTimeUnixNano, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.EndTimeUnixNano)
		b.updateMinMax("span:end", shared.ColumnTypeUint64, string(tmp[:]))
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
		b.updateMinMax("span:duration", shared.ColumnTypeUint64, string(tmp[:]))
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

// updateMinMax updates the per-block min/max for the named column.
// key is the encoded range key (from encodeRangeKey). Called once per present value.
// On first call for a column, min and max are both set to key.
// On subsequent calls, min and max are updated using type-aware comparison.
//
// 8-byte LE encoding for int64/uint64/float64 does NOT preserve lexicographic ordering
// (e.g. enc(256)="\x00\x01..." < enc(255)="\xff..." in string compare, but 256 > 255
// numerically). Numeric types require decoded comparison to produce correct min/max;
// the recorded encoded keys are then fed to the KLL sketch in addBlockRangeToColumn.
// String/bytes columns use raw lexicographic comparison which is correct by definition.
func (b *blockBuilder) updateMinMax(name string, typ shared.ColumnType, key string) {
	if mm, ok := b.colMinMax[name]; ok {
		if rangeKeyLess(typ, key, mm.minKey) {
			mm.minKey = key
		}
		if rangeKeyLess(typ, mm.maxKey, key) {
			mm.maxKey = key
		}
	} else {
		b.colMinMax[name] = &blockColMinMax{
			colName: name,
			minKey:  key,
			maxKey:  key,
			colType: typ,
		}
	}
}

// rangeKeyLess returns true when encoded key a is strictly less than encoded key b,
// using type-aware comparison. For numeric types (int64/uint64/float64), the 8-byte
// LE encoding is decoded to its native type before comparison. For string/bytes the
// comparison is raw lexicographic.
func rangeKeyLess(typ shared.ColumnType, a, b string) bool {
	switch typ {
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if len(a) < 8 || len(b) < 8 {
			return a < b
		}
		av := int64(binary.LittleEndian.Uint64([]byte(a))) //nolint:gosec // safe: reinterpreting uint64 bits as int64
		bv := int64(binary.LittleEndian.Uint64([]byte(b))) //nolint:gosec // safe: reinterpreting uint64 bits as int64
		return av < bv
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		if len(a) < 8 || len(b) < 8 {
			return a < b
		}
		return binary.LittleEndian.Uint64([]byte(a)) < binary.LittleEndian.Uint64([]byte(b))
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		if len(a) < 8 || len(b) < 8 {
			return a < b
		}
		av := math.Float64frombits(binary.LittleEndian.Uint64([]byte(a)))
		bv := math.Float64frombits(binary.LittleEndian.Uint64([]byte(b)))
		// NaN sorts after all real values: NaN < x is false, x < NaN is true.
		if math.IsNaN(av) {
			return false
		}
		if math.IsNaN(bv) {
			return true
		}
		return av < bv
	default: // String, Bytes, Bool: lexicographic is correct
		return a < b
	}
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
			b.updateMinMax(name, typ, key)
		}
	}
}

// finalize encodes all columns and returns the serialized block bytes
// (header + column metadata + data).
func (b *blockBuilder) finalize(enc *zstdEncoder) ([]byte, error) {
	// Collect and sort column names lexicographically.
	colNames := make([]string, 0, len(b.columns))
	for name := range b.columns {
		colNames = append(colNames, name)
	}
	sort.Strings(colNames)

	colCount := len(colNames)

	// Build column data blobs.
	type colBlob struct {
		name     string
		dataBlob []byte
		typ      shared.ColumnType
	}
	blobs := make([]colBlob, 0, colCount)

	for _, name := range colNames {
		cb := b.columns[name]
		data, err := cb.buildData(enc)
		if err != nil {
			return nil, fmt.Errorf("finalize column %q: %w", name, err)
		}
		blobs = append(blobs, colBlob{
			name:     name,
			typ:      cb.colType(),
			dataBlob: data,
		})
	}

	// --- Compute layout ---
	// Block header: 24 bytes
	// Column metadata array: sum of (2 + len(name) + 1 + 8 + 8 + 8 + 8) per column
	//   (stats_offset and stats_len are always 0 — column stats section removed)
	// Column data section: immediately after metadata

	headerSize := 24

	// Compute column metadata array size.
	colMetaSize := 0
	for _, bl := range blobs {
		// name_len[2] + name + col_type[1] + data_offset[8] + data_len[8] + stats_offset[8] + stats_len[8]
		colMetaSize += 2 + len(bl.name) + 1 + 8 + 8 + 8 + 8
	}

	// Data section starts immediately after column metadata (no stats section).
	dataStart := headerSize + colMetaSize

	// Compute per-column data offsets and total data size.
	dataSizes := make([]int, colCount)
	totalDataSize := 0
	for i, bl := range blobs {
		dataSizes[i] = len(bl.dataBlob)
		totalDataSize += len(bl.dataBlob)
	}

	// Total block size.
	totalSize := dataStart + totalDataSize

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
	// reserved2[8] (formerly trace_count[4] + trace_table_len[4], now always zero)
	payload = append(payload, 0, 0, 0, 0, 0, 0, 0, 0)

	// --- Write column metadata array ---
	// Track running data offset.
	curDataOff := uint64(dataStart)

	for i, bl := range blobs {
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
		// stats_offset[8 LE] — always 0 (column stats section removed)
		payload = appendUint64LE(payload, 0)
		// stats_len[8 LE] — always 0 (column stats section removed)
		payload = appendUint64LE(payload, 0)

		curDataOff += dataLen
	}

	// --- Write column data section ---
	for _, bl := range blobs {
		payload = append(payload, bl.dataBlob...)
	}

	return payload, nil
}

// appendUint32LE appends a uint32 in little-endian byte order to buf.
func appendUint32LE(buf []byte, v uint32) []byte {
	return append(buf,
		byte(v),     //nolint:gosec // safe: truncating uint32 bytes for LE encoding
		byte(v>>8),  //nolint:gosec // safe: truncating uint32 bytes for LE encoding
		byte(v>>16), //nolint:gosec // safe: truncating uint32 bytes for LE encoding
		byte(v>>24), //nolint:gosec // safe: truncating uint32 bytes for LE encoding
	)
}

// appendUint64LE appends a uint64 in little-endian byte order to buf.
func appendUint64LE(buf []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(buf, tmp[:]...)
}

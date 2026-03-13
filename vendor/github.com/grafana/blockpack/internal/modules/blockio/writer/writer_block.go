package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"

	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// pendingSpan is a lightweight span record buffered before sorting and flushing.
// Stores only the sort keys and proto pointers; full OTLP→column decoding is deferred
// to addRowFromProto, eliminating per-span AttrKV materialization and attrSlab growth.
//
// Size: ~88 bytes (4 pointer fields) vs shared.BufferedSpan ~344 bytes (13 pointer fields).
// The proto fields are kept alive by w.protoRoots for the duration of the flush cycle.
//
// For the columnar compaction path: srcBlock and srcRowIdx are set instead of proto fields.
// When srcBlock is non-nil, addRowFromBlock is used instead of addRowFromProto, avoiding
// all OTLP proto allocations.
type pendingSpan struct {
	rs         *tracev1.ResourceSpans // proto pointer; kept alive by w.protoRoots
	ss         *tracev1.ScopeSpans    // proto pointer; kept alive by w.protoRoots
	span       *tracev1.Span          // proto pointer; kept alive by w.protoRoots
	srcBlock   *modules_reader.Block  // non-nil for columnar path (compaction); nil for proto path
	svcName    string                 // sort key (primary); zero-copy reference into proto
	minHashSig [4]uint64              // sort key (secondary)
	srcRowIdx  int                    // source row index within srcBlock
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

	columns   map[shared.ColumnKey]columnBuilder // column (name, type) → builder (all columns, for finalize)
	traceRows map[[16]byte]struct{}              // trace_id set; used by writer.go to build file-level trace index

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
	colMinMax map[string]*blockColMinMax // column name → min/max for this block

	// colSketches accumulates HLL, CMS, and fuse keys per column for this block.
	// Populated alongside colMinMax; flushed at block write time.
	colSketches blockSketchSet

	// intrinsicAccum is the file-level accumulator, set by buildAndWriteBlock before
	// per-row calls. Nil for test helpers that don't need accumulation.
	intrinsicAccum  *intrinsicAccumulator
	intrinsicBlockID uint16

	// builderCache holds reset column builders from previous blocks, keyed by (name, type).
	// On addColumn, a matching builder is popped from the cache and reused, avoiding
	// fresh slice allocations. Most blocks share the same attribute columns, so the
	// cache hit rate is high.
	builderCache map[shared.ColumnKey]columnBuilder

	spanCount int
	// spanHint is the expected total span count for this block.
	// Used as the initial capacity for dynamically-created attribute column builder
	// value/present slices, eliminating growslice calls in the per-span append loop.
	spanHint   int
	minStart   uint64
	maxStart   uint64
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
	traceRows   map[[16]byte]struct{}
	colMinMax   map[string]*blockColMinMax // per-column min/max for this block
	colSketches blockSketchSet             // per-column HLL/CMS/fuse sketches for this block
	payload     []byte
	spanCount   int
	minStart    uint64
	maxStart    uint64
	minTraceID  [16]byte
	maxTraceID  [16]byte
}

// reset clears the blockBuilder for reuse with the next block.
// Intrinsic column builders are reset in place (preserving slice capacity).
// Sparse (attribute) column builders are moved to builderCache for reuse by addColumn.
// Column name caches (spanColNames, etc.) are preserved across blocks.
func (b *blockBuilder) reset(spanHint int) {
	b.spanHint = spanHint
	b.spanCount = 0
	b.minStart = 0
	b.maxStart = 0
	b.minTraceID = [16]byte{}
	b.maxTraceID = [16]byte{}

	// Reset intrinsic column builders in place — preserves backing slice capacity.
	// Then prepare to full block size (all slots null; indexed writes flip present).
	b.colTraceID.resetForReuse(traceIDColumnName)
	b.colTraceID.prepare(spanHint)
	b.colSpanID.resetForReuse("span:id")
	b.colSpanID.prepare(spanHint)
	b.colParentID.resetForReuse("span:parent_id")
	b.colParentID.prepare(spanHint)
	b.colSpanName.resetForReuse("span:name")
	b.colSpanName.prepare(spanHint)
	b.colSpanKind.resetForReuse("")
	b.colSpanKind.prepare(spanHint)
	b.colSpanStart.resetForReuse("span:start")
	b.colSpanStart.prepare(spanHint)
	b.colSpanEnd.resetForReuse("span:end")
	b.colSpanEnd.prepare(spanHint)
	b.colSpanDur.resetForReuse("span:duration")
	b.colSpanDur.prepare(spanHint)

	// Move non-intrinsic builders to cache for reuse.
	for key, cb := range b.columns {
		switch cb.(type) {
		case *bytesColumnBuilder:
			if cb == columnBuilder(b.colTraceID) || cb == columnBuilder(b.colSpanID) || cb == columnBuilder(b.colParentID) {
				continue
			}
		case *stringColumnBuilder:
			if cb == columnBuilder(b.colSpanName) {
				continue
			}
		case *int64ColumnBuilder:
			if cb == columnBuilder(b.colSpanKind) {
				continue
			}
		case *uint64ColumnBuilder:
			if cb == columnBuilder(b.colSpanStart) || cb == columnBuilder(b.colSpanEnd) || cb == columnBuilder(b.colSpanDur) {
				continue
			}
		}
		cb.resetForReuse(key.Name)
		b.builderCache[key] = cb
		delete(b.columns, key)
	}

	// Clear traceRows map (keep allocated buckets).
	for k := range b.traceRows {
		delete(b.traceRows, k)
	}

	// Clear colMinMax map (keep allocated buckets).
	for k := range b.colMinMax {
		delete(b.colMinMax, k)
	}

	// Reset colSketches for reuse.
	b.colSketches = newBlockSketchSet()
}

// buildBlock constructs a single block from the given pending spans and returns
// the serialized payload together with all per-block statistics.
// If bb is non-nil it is reset and reused; otherwise a new blockBuilder is created.
func buildBlock(
	pending []pendingSpan, enc *zstdEncoder, bb *blockBuilder,
) (builtBlock, *blockBuilder, error) {
	if bb != nil {
		bb.reset(len(pending))
	} else {
		bb = newBlockBuilder(len(pending))
	}
	for rowIdx := range pending {
		ps := &pending[rowIdx]
		if ps.srcBlock != nil {
			bb.addRowFromBlock(ps.srcBlock, ps.srcRowIdx, rowIdx)
		} else {
			bb.addRowFromProto(ps, rowIdx)
		}
	}
	payload, err := bb.finalize(enc)
	if err != nil {
		return builtBlock{}, bb, err
	}
	return builtBlock{
		payload:     payload,
		spanCount:   bb.spanCount,
		minStart:    bb.minStart,
		maxStart:    bb.maxStart,
		minTraceID:  bb.minTraceID,
		maxTraceID:  bb.maxTraceID,
		traceRows:   bb.traceRows,
		colMinMax:   bb.colMinMax,
		colSketches: bb.colSketches,
	}, bb, nil
}

// newBlockBuilder creates an empty block builder.
// spanHint is the expected number of spans in this block; used to pre-allocate
// value/present slices in the intrinsic column builders, eliminating growslice calls
// during the per-span append loop.
func newBlockBuilder(spanHint int) *blockBuilder {
	b := &blockBuilder{
		columns:          make(map[shared.ColumnKey]columnBuilder, 32),
		traceRows:        make(map[[16]byte]struct{}, 16),
		colMinMax:        make(map[string]*blockColMinMax, 64),
		colSketches:      newBlockSketchSet(),
		builderCache:     make(map[shared.ColumnKey]columnBuilder, 32),
		spanHint:         spanHint,
		spanColNames:     make(map[string]string, 32),
		resourceColNames: make(map[string]string, 16),
		scopeColNames:    make(map[string]string, 8),
	}

	// Pre-create intrinsic column builders and prepare them to full block size.
	// All slots start as null (zero value + present=false); addRowFromProto sets
	// present values by index, eliminating append-based null-filling.

	b.colTraceID = &bytesColumnBuilder{colName: traceIDColumnName}
	b.colTraceID.prepare(spanHint)
	b.colSpanID = &bytesColumnBuilder{colName: "span:id"}
	b.colSpanID.prepare(spanHint)
	b.colParentID = &bytesColumnBuilder{colName: "span:parent_id"}
	b.colParentID.prepare(spanHint)
	b.colSpanName = &stringColumnBuilder{colName: "span:name"}
	b.colSpanName.prepare(spanHint)
	b.colSpanKind = &int64ColumnBuilder{}
	b.colSpanKind.prepare(spanHint)
	b.colSpanStart = &uint64ColumnBuilder{colName: "span:start"}
	b.colSpanStart.prepare(spanHint)
	b.colSpanEnd = &uint64ColumnBuilder{colName: "span:end"}
	b.colSpanEnd.prepare(spanHint)
	b.colSpanDur = &uint64ColumnBuilder{colName: "span:duration"}
	b.colSpanDur.prepare(spanHint)

	// Register in map so finalize() can collect them.
	b.columns[shared.ColumnKey{Name: traceIDColumnName, Type: shared.ColumnTypeBytes}] = b.colTraceID
	b.columns[shared.ColumnKey{Name: "span:id", Type: shared.ColumnTypeBytes}] = b.colSpanID
	b.columns[shared.ColumnKey{Name: "span:parent_id", Type: shared.ColumnTypeBytes}] = b.colParentID
	b.columns[shared.ColumnKey{Name: "span:name", Type: shared.ColumnTypeString}] = b.colSpanName
	b.columns[shared.ColumnKey{Name: "span:kind", Type: shared.ColumnTypeInt64}] = b.colSpanKind
	b.columns[shared.ColumnKey{Name: "span:start", Type: shared.ColumnTypeUint64}] = b.colSpanStart
	b.columns[shared.ColumnKey{Name: "span:end", Type: shared.ColumnTypeUint64}] = b.colSpanEnd
	b.columns[shared.ColumnKey{Name: "span:duration", Type: shared.ColumnTypeUint64}] = b.colSpanDur

	return b
}

// addColumn ensures a column builder exists for the given name/type.
// Returns the builder. If a matching builder exists in builderCache from a
// previous block, it is reused; otherwise a new builder is allocated.
// Because columns is keyed by (name, type), two columns with the same name
// but different types are stored independently — no data loss on type conflicts.
func (b *blockBuilder) addColumn(name string, typ shared.ColumnType) columnBuilder {
	key := shared.ColumnKey{Name: name, Type: typ}
	if cb, ok := b.columns[key]; ok {
		return cb
	}

	// Try to reuse a cached builder from a previous block.
	var cb columnBuilder
	if cached, ok := b.builderCache[key]; ok {
		delete(b.builderCache, key)
		cb = cached
	}
	if cb == nil {
		cb = newColumnBuilder(typ, name, 0)
	}

	// Pre-allocate to full block size — all slots start null.
	// Present values are set by indexed writes in addPresent.
	cb.prepare(b.spanHint)
	b.columns[key] = cb
	return cb
}

// copyBytes returns a copy of src. Returns nil for empty/nil input.
func copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp
}

// addRowFromProto adds all column values for one span row from a pendingSpan.
// Full OTLP→column decoding happens here (deferred from AddTracesData) to eliminate
// per-span AttrKV materialization and attrSlab growth.
// Column names for dynamic attributes are interned via the block-level name caches.
func (b *blockBuilder) addRowFromProto(ps *pendingSpan, rowIdx int) {
	span := ps.span

	// --- Intrinsic columns (indexed writes into pre-allocated slices) ---

	// trace:id — always present; excluded from the range index.
	b.colTraceID.values[rowIdx] = copyBytes(span.TraceId)
	b.colTraceID.present[rowIdx] = true

	// span:id — may be absent.
	if len(span.SpanId) > 0 {
		b.colSpanID.values[rowIdx] = copyBytes(span.SpanId)
		b.colSpanID.present[rowIdx] = true
		b.updateMinMax("span:id", shared.ColumnTypeBytes, string(span.SpanId))
	}

	// span:parent_id — may be absent.
	if len(span.ParentSpanId) > 0 {
		b.colParentID.values[rowIdx] = copyBytes(span.ParentSpanId)
		b.colParentID.present[rowIdx] = true
		b.updateMinMax("span:parent_id", shared.ColumnTypeBytes, string(span.ParentSpanId))
	}

	// span:name — always present; truncate to MaxStringLen.
	spanName := span.Name
	if len(spanName) > shared.MaxStringLen {
		spanName = spanName[:shared.MaxStringLen]
	}
	b.colSpanName.values[rowIdx] = spanName
	b.colSpanName.present[rowIdx] = true
	if spanName != "" {
		b.updateMinMax("span:name", shared.ColumnTypeString, spanName)
		if a := b.intrinsicAccum; a != nil {
			a.feedString("span:name", shared.ColumnTypeString, spanName, b.intrinsicBlockID, rowIdx)
		}
	}

	// span:kind — always present.
	spanKind := int64(span.Kind)
	b.colSpanKind.values[rowIdx] = spanKind
	b.colSpanKind.present[rowIdx] = true
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(spanKind)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
		b.updateMinMax("span:kind", shared.ColumnTypeInt64, string(tmp[:]))
	}
	if a := b.intrinsicAccum; a != nil {
		a.feedInt64("span:kind", shared.ColumnTypeInt64, spanKind, b.intrinsicBlockID, rowIdx)
	}

	// span:start — always present.
	b.colSpanStart.values[rowIdx] = span.StartTimeUnixNano
	b.colSpanStart.present[rowIdx] = true
	b.colSpanStart.trackMinMax(span.StartTimeUnixNano)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.StartTimeUnixNano)
		b.updateMinMax("span:start", shared.ColumnTypeUint64, string(tmp[:]))
	}
	if a := b.intrinsicAccum; a != nil {
		a.feedUint64("span:start", shared.ColumnTypeUint64, span.StartTimeUnixNano, b.intrinsicBlockID, rowIdx)
	}
	// Task T-TS-2: implied timestamp sketch — 1-second bucket granularity.
	if span.StartTimeUnixNano > 0 {
		b.colSketches.add(sketchTimestampColName, encodeSecondBucket(span.StartTimeUnixNano))
	}

	// span:end — always present.
	b.colSpanEnd.values[rowIdx] = span.EndTimeUnixNano
	b.colSpanEnd.present[rowIdx] = true
	b.colSpanEnd.trackMinMax(span.EndTimeUnixNano)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.EndTimeUnixNano)
		b.updateMinMax("span:end", shared.ColumnTypeUint64, string(tmp[:]))
	}
	if a := b.intrinsicAccum; a != nil {
		a.feedUint64("span:end", shared.ColumnTypeUint64, span.EndTimeUnixNano, b.intrinsicBlockID, rowIdx)
	}

	// span:duration — always present.
	var dur uint64
	if span.EndTimeUnixNano >= span.StartTimeUnixNano {
		dur = span.EndTimeUnixNano - span.StartTimeUnixNano
	}
	b.colSpanDur.values[rowIdx] = dur
	b.colSpanDur.present[rowIdx] = true
	b.colSpanDur.trackMinMax(dur)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], dur)
		b.updateMinMax("span:duration", shared.ColumnTypeUint64, string(tmp[:]))
	}
	if a := b.intrinsicAccum; a != nil {
		a.feedUint64("span:duration", shared.ColumnTypeUint64, dur, b.intrinsicBlockID, rowIdx)
	}

	// --- Conditional intrinsic columns ---

	if span.Status != nil {
		// Always write span:status when the Status object is present, even for code 0
		// (STATUS_CODE_UNSET). This ensures the range index tracks unset status so that
		// { status = unset } queries can prune blocks correctly. Spans where span.Status
		// is nil entirely (the majority) remain null in the column — bloom filter handles
		// block pruning for those cases.
		b.addPresent(rowIdx, "span:status", shared.ColumnTypeInt64, shared.AttrValue{
			Type: shared.ColumnTypeInt64,
			Int:  int64(span.Status.Code),
		})
		if a := b.intrinsicAccum; a != nil {
			a.feedInt64("span:status", shared.ColumnTypeInt64, int64(span.Status.Code), b.intrinsicBlockID, rowIdx)
		}
		if span.Status.Message != "" {
			b.addPresent(rowIdx, "span:status_message", shared.ColumnTypeString, shared.AttrValue{
				Type: shared.ColumnTypeString,
				Str:  span.Status.Message,
			})
			if a := b.intrinsicAccum; a != nil {
				a.feedString("span:status_message", shared.ColumnTypeString, span.Status.Message, b.intrinsicBlockID, rowIdx)
			}
		}
	}

	if span.TraceState != "" {
		b.addPresent(rowIdx, "trace:state", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  span.TraceState,
		})
	}

	if ps.rs != nil && ps.rs.SchemaUrl != "" {
		b.addPresent(rowIdx, "resource:schema_url", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  ps.rs.SchemaUrl,
		})
	}

	if ps.ss != nil && ps.ss.SchemaUrl != "" {
		b.addPresent(rowIdx, "scope:schema_url", shared.ColumnTypeString, shared.AttrValue{
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
		b.addPresent(rowIdx, name, val.Type, val)
	}

	// --- Resource attributes ---
	if ps.rs != nil && ps.rs.Resource != nil {
		for _, kv := range ps.rs.Resource.Attributes {
			if kv == nil {
				continue
			}
			name := b.internColName(kv.Key, b.resourceColNames, "resource.")
			val := protoToAttrValue(kv.Value)
			b.addPresent(rowIdx, name, val.Type, val)
			if kv.Key == "service.name" {
				if a := b.intrinsicAccum; a != nil && val.Type == shared.ColumnTypeString && val.Str != "" {
					a.feedString("resource.service.name", shared.ColumnTypeString, val.Str, b.intrinsicBlockID, rowIdx)
				}
			}
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
			b.addPresent(rowIdx, name, val.Type, val)
		}
	}

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

	// Track which traces appear in this block.
	b.traceRows[ps.traceID] = struct{}{}

	b.spanCount++
}

// baseColumnType normalizes Range column types to their base logical type.
// Range types are an encoding detail (they enable range-index optimized storage) but
// are logically identical to their base types for value reads and addPresent dispatch.
func baseColumnType(t shared.ColumnType) shared.ColumnType {
	switch t {
	case shared.ColumnTypeRangeString:
		return shared.ColumnTypeString
	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		return shared.ColumnTypeInt64
	case shared.ColumnTypeRangeUint64:
		return shared.ColumnTypeUint64
	case shared.ColumnTypeRangeFloat64:
		return shared.ColumnTypeFloat64
	case shared.ColumnTypeRangeBytes:
		return shared.ColumnTypeBytes
	case shared.ColumnTypeUUID:
		// UUID is logically a string; compaction roundtrips via string builder which
		// re-detects UUID and re-encodes as ColumnTypeUUID on the next write.
		return shared.ColumnTypeString
	default:
		return t
	}
}

// readDynAttrValue reads a typed value from col at rowIdx for dynamic attribute columns.
// Returns the AttrValue and true on success; false if the column value is absent or unreadable.
func readDynAttrValue(col *modules_reader.Column, rowIdx int, baseType shared.ColumnType) (shared.AttrValue, bool) {
	var val shared.AttrValue
	val.Type = baseType
	switch baseType {
	case shared.ColumnTypeString:
		v, ok := col.StringValue(rowIdx)
		if !ok {
			return val, false
		}
		val.Str = v
	case shared.ColumnTypeInt64:
		v, ok := col.Int64Value(rowIdx)
		if !ok {
			return val, false
		}
		val.Int = v
	case shared.ColumnTypeUint64:
		v, ok := col.Uint64Value(rowIdx)
		if !ok {
			return val, false
		}
		val.Uint = v
	case shared.ColumnTypeFloat64:
		v, ok := col.Float64Value(rowIdx)
		if !ok {
			return val, false
		}
		val.Float = v
	case shared.ColumnTypeBool:
		v, ok := col.BoolValue(rowIdx)
		if !ok {
			return val, false
		}
		val.Bool = v
	default: // bytes
		v, ok := col.BytesValue(rowIdx)
		if !ok {
			return val, false
		}
		val.Bytes = slices.Clone(v)
	}
	return val, true
}

// addRowFromBlock adds all column values for one row from a source Block.
// This is the native columnar path used by the compaction writer — it bypasses
// all OTLP proto objects, reading typed values directly from decoded columns and
// writing them into the destination block via addPresent.
func (b *blockBuilder) addRowFromBlock(srcBlock *modules_reader.Block, srcRowIdx int, dstRowIdx int) {
	var traceID [16]byte
	var spanStart, spanEnd uint64
	traceIDFound := false
	spanStartFound := false
	spanEndFound := false
	durationFound := false

	for colKey, col := range srcBlock.Columns() {
		if !col.IsPresent(srcRowIdx) {
			continue
		}

		baseType := baseColumnType(col.Type)

		switch colKey.Name {
		case traceIDColumnName:
			if v, ok := col.BytesValue(srcRowIdx); ok && len(v) == 16 {
				b.colTraceID.values[dstRowIdx] = slices.Clone(v)
				b.colTraceID.present[dstRowIdx] = true
				copy(traceID[:], v)
				traceIDFound = true
			}
			continue

		case "span:id":
			if v, ok := col.BytesValue(srcRowIdx); ok && len(v) > 0 {
				b.colSpanID.values[dstRowIdx] = slices.Clone(v)
				b.colSpanID.present[dstRowIdx] = true
				b.updateMinMax("span:id", shared.ColumnTypeBytes, string(v))
			}
			continue

		case "span:parent_id":
			if v, ok := col.BytesValue(srcRowIdx); ok && len(v) > 0 {
				b.colParentID.values[dstRowIdx] = slices.Clone(v)
				b.colParentID.present[dstRowIdx] = true
				b.updateMinMax("span:parent_id", shared.ColumnTypeBytes, string(v))
			}
			continue

		case "span:name":
			if v, ok := col.StringValue(srcRowIdx); ok {
				b.colSpanName.values[dstRowIdx] = v
				b.colSpanName.present[dstRowIdx] = true
				if v != "" {
					b.updateMinMax("span:name", shared.ColumnTypeString, v)
					if a := b.intrinsicAccum; a != nil {
						a.feedString("span:name", shared.ColumnTypeString, v, b.intrinsicBlockID, dstRowIdx)
					}
				}
			}
			continue

		case "span:kind":
			if v, ok := col.Int64Value(srcRowIdx); ok {
				b.colSpanKind.values[dstRowIdx] = v
				b.colSpanKind.present[dstRowIdx] = true
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
				b.updateMinMax("span:kind", shared.ColumnTypeInt64, string(tmp[:]))
				if a := b.intrinsicAccum; a != nil {
					a.feedInt64("span:kind", shared.ColumnTypeInt64, v, b.intrinsicBlockID, dstRowIdx)
				}
			}
			continue

		case "span:start":
			if v, ok := col.Uint64Value(srcRowIdx); ok {
				b.colSpanStart.values[dstRowIdx] = v
				b.colSpanStart.present[dstRowIdx] = true
				b.colSpanStart.trackMinMax(v)
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], v)
				b.updateMinMax("span:start", shared.ColumnTypeUint64, string(tmp[:]))
				spanStart = v
				spanStartFound = true
				if a := b.intrinsicAccum; a != nil {
					a.feedUint64("span:start", shared.ColumnTypeUint64, v, b.intrinsicBlockID, dstRowIdx)
				}
			}
			continue

		case "span:end":
			if v, ok := col.Uint64Value(srcRowIdx); ok {
				b.colSpanEnd.values[dstRowIdx] = v
				b.colSpanEnd.present[dstRowIdx] = true
				b.colSpanEnd.trackMinMax(v)
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], v)
				b.updateMinMax("span:end", shared.ColumnTypeUint64, string(tmp[:]))
				spanEnd = v
				spanEndFound = true
				if a := b.intrinsicAccum; a != nil {
					a.feedUint64("span:end", shared.ColumnTypeUint64, v, b.intrinsicBlockID, dstRowIdx)
				}
			}
			continue

		case "span:duration":
			if v, ok := col.Uint64Value(srcRowIdx); ok {
				b.colSpanDur.values[dstRowIdx] = v
				b.colSpanDur.present[dstRowIdx] = true
				b.colSpanDur.trackMinMax(v)
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], v)
				b.updateMinMax("span:duration", shared.ColumnTypeUint64, string(tmp[:]))
				durationFound = true
				if a := b.intrinsicAccum; a != nil {
					a.feedUint64("span:duration", shared.ColumnTypeUint64, v, b.intrinsicBlockID, dstRowIdx)
				}
			}
			continue

		case "span:status":
			if v, ok := col.Int64Value(srcRowIdx); ok {
				var tmp [8]byte
				binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
				b.updateMinMax("span:status", shared.ColumnTypeInt64, string(tmp[:]))
				if a := b.intrinsicAccum; a != nil {
					a.feedInt64("span:status", shared.ColumnTypeInt64, v, b.intrinsicBlockID, dstRowIdx)
				}
			}
			// Fall through to addPresent below (don't continue).

		case "span:status_message":
			if v, ok := col.StringValue(srcRowIdx); ok && v != "" {
				if a := b.intrinsicAccum; a != nil {
					a.feedString("span:status_message", shared.ColumnTypeString, v, b.intrinsicBlockID, dstRowIdx)
				}
			}
			// Fall through to addPresent below (don't continue).

		case "resource.service.name":
			val, ok := readDynAttrValue(col, srcRowIdx, baseColumnType(col.Type))
			if ok && val.Type == shared.ColumnTypeString && val.Str != "" {
				if a := b.intrinsicAccum; a != nil {
					a.feedString("resource.service.name", shared.ColumnTypeString, val.Str, b.intrinsicBlockID, dstRowIdx)
				}
			}
			// Fall through to addPresent below (don't continue).
		}

		// Dynamic attribute columns: read typed value and call addPresent.
		val, ok := readDynAttrValue(col, srcRowIdx, baseType)
		if !ok {
			continue
		}
		b.addPresent(dstRowIdx, colKey.Name, baseType, val)
	}

	// If the source block lacked span:duration (e.g. filtered decode or older format),
	// derive it from start and end — matching the proto path which always computes duration.
	// Guard against underflow: if end < start (clock skew / corrupt data), use 0.
	if !durationFound && spanStartFound && spanEndFound {
		var dur uint64
		if spanEnd >= spanStart {
			dur = spanEnd - spanStart
		}
		b.colSpanDur.values[dstRowIdx] = dur
		b.colSpanDur.present[dstRowIdx] = true
		b.colSpanDur.trackMinMax(dur)
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], dur)
		b.updateMinMax("span:duration", shared.ColumnTypeUint64, string(tmp[:]))
	}

	// Update min/max start time and trace ID bookkeeping — same as tail of addRowFromProto.
	if b.spanCount == 0 {
		if spanStartFound {
			b.minStart = spanStart
			b.maxStart = spanStart
		}
		if traceIDFound {
			b.minTraceID = traceID
			b.maxTraceID = traceID
		}
	} else {
		if spanStartFound {
			if spanStart < b.minStart {
				b.minStart = spanStart
			}
			if spanStart > b.maxStart {
				b.maxStart = spanStart
			}
		}
		if traceIDFound {
			if bytes.Compare(traceID[:], b.minTraceID[:]) < 0 {
				b.minTraceID = traceID
			}
			if bytes.Compare(traceID[:], b.maxTraceID[:]) > 0 {
				b.maxTraceID = traceID
			}
		}
	}

	if traceIDFound {
		b.traceRows[traceID] = struct{}{}
	}

	// Task T-TS-2: implied timestamp sketch for compaction path (same as addRowFromProto).
	if spanStartFound && spanStart > 0 {
		b.colSketches.add(sketchTimestampColName, encodeSecondBucket(spanStart))
	}

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

// updateMinMax updates the per-block min/max for the named column and records the
// value in the sketch accumulators (HLL, CMS, BinaryFuse8 keys).
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
	// Update sketch accumulators for every observed value (not just min/max).
	// SPEC-SK-16: same key encoding as at query time.
	b.colSketches.add(name, key)
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
// at the given row index and feeds the range index. Uses direct indexed writes
// into pre-allocated slices, avoiding append and null-filling entirely.
func (b *blockBuilder) addPresent(rowIdx int, name string, typ shared.ColumnType, val shared.AttrValue) {
	cb := b.addColumn(name, typ)
	if cb == nil {
		return // type conflict with an existing same-named column; skip this value
	}
	switch typ {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		scb := cb.(*stringColumnBuilder)
		s := val.Str
		if len(s) > shared.MaxStringLen {
			s = s[:shared.MaxStringLen]
		}
		scb.values[rowIdx] = s
		scb.present[rowIdx] = true
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		icb := cb.(*int64ColumnBuilder)
		icb.values[rowIdx] = val.Int
		icb.present[rowIdx] = true
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		ucb := cb.(*uint64ColumnBuilder)
		ucb.values[rowIdx] = val.Uint
		ucb.present[rowIdx] = true
		ucb.trackMinMax(val.Uint)
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		fcb := cb.(*float64ColumnBuilder)
		fcb.values[rowIdx] = val.Float
		fcb.present[rowIdx] = true
	case shared.ColumnTypeBool:
		bcb := cb.(*boolColumnBuilder)
		bcb.values[rowIdx] = val.Bool
		bcb.present[rowIdx] = true
	default: // bytes / rangebytes
		bcb := cb.(*bytesColumnBuilder)
		bv := val.Bytes
		if len(bv) > shared.MaxBytesLen {
			bv = bv[:shared.MaxBytesLen]
		}
		bcb.values[rowIdx] = bv
		bcb.present[rowIdx] = true
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
	// Collect and sort columns by (name, type) for deterministic output.
	// cb is placed before key to minimize GC scan region (betteralign).
	type colEntry struct {
		cb  columnBuilder
		key shared.ColumnKey
	}
	entries := make([]colEntry, 0, len(b.columns))
	for k, cb := range b.columns {
		entries = append(entries, colEntry{cb, k})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].key.Name != entries[j].key.Name {
			return entries[i].key.Name < entries[j].key.Name
		}
		return entries[i].key.Type < entries[j].key.Type
	})

	colCount := len(entries)

	// Build column data blobs.
	type colBlob struct {
		name     string
		dataBlob []byte
		typ      shared.ColumnType
	}
	blobs := make([]colBlob, 0, colCount)

	for _, e := range entries {
		data, err := e.cb.buildData(enc)
		if err != nil {
			return nil, fmt.Errorf("finalize column %q (type %v): %w", e.key.Name, e.key.Type, err)
		}
		blobs = append(blobs, colBlob{
			name:     e.key.Name,
			typ:      e.cb.colType(),
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
	spanCountU32 := uint32(b.spanCount) //nolint:gosec // safe: bounded by MaxBlockSpans
	payload = appendUint32LE(payload, spanCountU32)
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
		nameLen := len(bl.name)
		payload = append(payload, byte(nameLen), byte(nameLen>>8)) //nolint:gosec // safe: bounded by MaxNameLen
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

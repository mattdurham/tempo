package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// pendingSpan is a lightweight span record buffered before sorting and flushing.
// Stores only the sort keys and proto pointers; full OTLP→column decoding is deferred
// to addRowFromProto, eliminating per-span AttrKV materialization and attrSlab growth.
//
// Size: ~112 bytes (7 pointer fields) vs shared.BufferedSpan ~344 bytes (13 pointer fields).
// OTLP proto fields (rs/ss/span) are kept alive by w.protoRoots; Tempo proto fields
// (tempoRS/tempoSS/tempoSpan) by w.tempoProtoRoots. The two sets are mutually exclusive.
//
// For the columnar compaction path: srcBlock and srcRowIdx are set instead of proto fields.
// When srcBlock is non-nil, addRowFromBlock is used instead of addRowFromProto, avoiding
// all OTLP proto allocations.
type pendingSpan struct {
	rs   *tracev1.ResourceSpans // proto pointer; kept alive by w.protoRoots
	ss   *tracev1.ScopeSpans    // proto pointer; kept alive by w.protoRoots
	span *tracev1.Span          // proto pointer; kept alive by w.protoRoots
	// Tempo-native proto pointers (mutually exclusive with rs/ss/span above).
	// Kept alive by w.tempoProtoRoots until flushBlocks() processes them.
	tempoRS     *tempotrace.ResourceSpans
	tempoSS     *tempotrace.ScopeSpans
	tempoSpan   *tempotrace.Span
	srcBlock    *modules_reader.Block  // non-nil for columnar path (compaction); nil for proto path
	srcReader   *modules_reader.Reader // non-nil when srcBlock is set; used for intrinsic lookup
	svcName     string                 // sort key (primary); zero-copy reference into proto
	minHashSig  [4]uint64              // sort key (secondary)
	srcRowIdx   int                    // source row index within srcBlock
	srcBlockIdx int                    // block index within srcReader (for intrinsic lookup)
	traceID     [16]byte               // sort key (tertiary)
}

// blockBuilder manages construction of a single block.
// Rows are added one at a time via addRowFromProto, then finalized in one shot.
// The mutable builder pattern (vs. a single BuildBlock([]rows) function) exists
// because column builders must track running state (null-fill, bloom filter,
// range values) that is simpler to maintain incrementally than in a batch pass.
type blockBuilder struct {
	columns   map[shared.ColumnKey]columnBuilder // column (name, type) → builder (all columns, for finalize)
	traceRows map[[16]byte]struct{}              // trace_id set; used by writer.go to build file-level trace index

	// intrinsicAccum is the file-level accumulator, set by buildAndWriteBlock before
	// per-row calls. Nil for test helpers that don't need accumulation.
	intrinsicAccum *intrinsicAccumulator

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

	// colSketches accumulates HLL, TopK, and fuse keys per column for this block.
	// Populated alongside colMinMax; flushed at block write time.
	colSketches blockSketchSet

	// builderCache holds reset column builders from previous blocks, keyed by (name, type).
	// On addColumn, a matching builder is popped from the cache and reused, avoiding
	// fresh slice allocations. Most blocks share the same attribute columns, so the
	// cache hit rate is high.
	builderCache map[shared.ColumnKey]columnBuilder

	spanCount int
	// spanHint is the expected total span count for this block.
	// Used as the initial capacity for dynamically-created attribute column builder
	// value/present slices, eliminating growslice calls in the per-span append loop.
	spanHint         int
	minStart         uint64
	maxStart         uint64
	intrinsicBlockID uint16

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
	colSketches blockSketchSet             // per-column HLL/TopK/fuse sketches for this block
	payload     []byte
	spanCount   int
	minStart    uint64
	maxStart    uint64
	minTraceID  [16]byte
	maxTraceID  [16]byte
}

// reset clears the blockBuilder for reuse with the next block.
// Attribute column builders are moved to builderCache for reuse by addColumn.
// Column name caches (spanColNames, etc.) are preserved across blocks.
func (b *blockBuilder) reset(spanHint int) {
	b.spanHint = spanHint
	b.spanCount = 0
	b.minStart = 0
	b.maxStart = 0
	b.minTraceID = [16]byte{}
	b.maxTraceID = [16]byte{}

	// Move all builders to cache for reuse.
	for key, cb := range b.columns {
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
// intrinsicAccum is the file-level intrinsic accumulator; may be nil (no accumulation).
// blockID is the 0-based block index used as BlockIdx in intrinsic refs.
func buildBlock(
	pending []pendingSpan, enc *zstdEncoder, bb *blockBuilder, blockVersion uint8,
	intrinsicAccum *intrinsicAccumulator, blockID int,
) (builtBlock, *blockBuilder, error) {
	if bb != nil {
		bb.reset(len(pending))
	} else {
		bb = newBlockBuilder(len(pending))
	}
	bb.intrinsicAccum = intrinsicAccum
	bb.intrinsicBlockID = uint16(blockID) //nolint:gosec // safe: blockID bounded by 65534 (checked by caller)

	// Pre-build per-(reader, srcBlockIdx) intrinsic index to avoid O(N) linear scans
	// inside feedIntrinsicsFromIndex. Index is built once per unique (reader, blockIdx)
	// pair; each row then does an O(1) map lookup.
	type readerBlockKey struct {
		r        *modules_reader.Reader
		blockIdx int
	}
	var intrinsicIndexCache map[readerBlockKey]intrinsicRowFields
	for i := range pending {
		ps := &pending[i]
		if ps.srcReader == nil {
			continue
		}
		k := readerBlockKey{ps.srcReader, ps.srcBlockIdx}
		if intrinsicIndexCache == nil {
			intrinsicIndexCache = make(map[readerBlockKey]intrinsicRowFields)
		}
		if _, already := intrinsicIndexCache[k]; !already {
			intrinsicIndexCache[k] = buildIntrinsicBlockIndex(ps.srcReader, ps.srcBlockIdx)
		}
	}

	for rowIdx := range pending {
		ps := &pending[rowIdx]
		switch {
		case ps.srcBlock != nil:
			bb.addRowFromBlock(ps.srcBlock, ps.srcRowIdx, rowIdx)
			if ps.srcReader != nil {
				k := readerBlockKey{ps.srcReader, ps.srcBlockIdx}
				// feedIntrinsicsFromIndex is only needed for v4+ blocks that store
				// identity columns (trace:id, span:id, etc.) exclusively in the
				// intrinsic section. For v3 blocks that use dual storage (same fields
				// in both block columns and the intrinsic section), addRowFromBlock
				// has already written these fields via applyTraceID and friends.
				// Calling feedIntrinsicsFromIndex for a dual-storage block would
				// append a second entry to the intrinsic accumulator for the same
				// (blockIdx, rowIdx), causing GetTraceByID to return each span twice.
				if ps.srcBlock.GetColumn(traceIDColumnName) == nil {
					bb.feedIntrinsicsFromIndex(intrinsicIndexCache[k], ps.srcRowIdx, rowIdx)
				}
			}
		case ps.tempoSpan != nil:
			bb.addRowFromTempoProto(ps, rowIdx)
		default:
			bb.addRowFromProto(ps, rowIdx)
		}
	}
	payload, err := bb.finalize(enc, blockVersion)
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

// feedIntrinsicUint64 feeds a uint64 value to the intrinsic accumulator if present.
func (b *blockBuilder) feedIntrinsicUint64(name string, colType shared.ColumnType, val uint64, rowIdx int) {
	if a := b.intrinsicAccum; a != nil {
		a.feedUint64(name, colType, val, b.intrinsicBlockID, rowIdx)
	}
}

// feedIntrinsicString feeds a string value to the intrinsic accumulator if present.
func (b *blockBuilder) feedIntrinsicString(name string, colType shared.ColumnType, val string, rowIdx int) {
	if a := b.intrinsicAccum; a != nil && val != "" {
		a.feedString(name, colType, val, b.intrinsicBlockID, rowIdx)
	}
}

// feedIntrinsicInt64 feeds an int64 value to the intrinsic accumulator if present.
func (b *blockBuilder) feedIntrinsicInt64(name string, colType shared.ColumnType, val int64, rowIdx int) {
	if a := b.intrinsicAccum; a != nil {
		a.feedInt64(name, colType, val, b.intrinsicBlockID, rowIdx)
	}
}

// feedIntrinsicBytes feeds a bytes value to the intrinsic accumulator if present.
func (b *blockBuilder) feedIntrinsicBytes(name string, colType shared.ColumnType, val []byte, rowIdx int) {
	if a := b.intrinsicAccum; a != nil && len(val) > 0 {
		a.feedBytes(name, colType, val, b.intrinsicBlockID, rowIdx)
	}
}

// addRowFromProto adds all column values for one span row from a pendingSpan.
// Full OTLP→column decoding happens here (deferred from AddTracesData) to eliminate
// per-span AttrKV materialization and attrSlab growth.
// Column names for dynamic attributes are interned via the block-level name caches.
//
//nolint:dupl // intentional mirror of addRowFromTempoProto for OTLP types; different field types prevent sharing
func (b *blockBuilder) addRowFromProto(ps *pendingSpan, rowIdx int) {
	span := ps.span

	// --- Intrinsic columns — feed accumulator AND write block columns (dual storage) ---

	// trace:id — always present; excluded from the range index.
	b.feedIntrinsicBytes(traceIDColumnName, shared.ColumnTypeBytes, span.TraceId, rowIdx)
	b.addPresent(
		rowIdx,
		traceIDColumnName,
		shared.ColumnTypeBytes,
		shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: span.TraceId},
	)

	// span:id — may be absent.
	if len(span.SpanId) > 0 {
		b.updateMinMax(spanIDColumnName, shared.ColumnTypeBytes, string(span.SpanId))
		b.feedIntrinsicBytes(spanIDColumnName, shared.ColumnTypeBytes, span.SpanId, rowIdx)
		b.addPresent(
			rowIdx,
			spanIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: span.SpanId},
		)
	}

	// span:parent_id — may be absent.
	if len(span.ParentSpanId) > 0 {
		b.updateMinMax(spanParentIDColumnName, shared.ColumnTypeBytes, string(span.ParentSpanId))
		b.feedIntrinsicBytes(spanParentIDColumnName, shared.ColumnTypeBytes, span.ParentSpanId, rowIdx)
		b.addPresent(
			rowIdx,
			spanParentIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: span.ParentSpanId},
		)
	}

	// span:name — always present; truncate to MaxStringLen.
	spanName := span.Name
	if len(spanName) > shared.MaxStringLen {
		spanName = spanName[:shared.MaxStringLen]
	}
	if spanName != "" {
		b.updateMinMax(spanNameColumnName, shared.ColumnTypeString, spanName)
		b.feedIntrinsicString(spanNameColumnName, shared.ColumnTypeString, spanName, rowIdx)
		b.addPresent(
			rowIdx,
			spanNameColumnName,
			shared.ColumnTypeString,
			shared.AttrValue{Type: shared.ColumnTypeString, Str: spanName},
		)
	}

	// span:kind — always present.
	spanKind := int64(span.Kind)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(
			tmp[:],
			uint64(spanKind), //nolint:gosec // safe: reinterpreting int64 bits as uint64
		)
		b.updateMinMax(spanKindColumnName, shared.ColumnTypeInt64, string(tmp[:]))
	}
	b.feedIntrinsicInt64(spanKindColumnName, shared.ColumnTypeInt64, spanKind, rowIdx)
	b.addPresent(
		rowIdx,
		spanKindColumnName,
		shared.ColumnTypeInt64,
		shared.AttrValue{Type: shared.ColumnTypeInt64, Int: spanKind},
	)

	// span:start — always present.
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.StartTimeUnixNano)
		b.updateMinMax(spanStartColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	b.feedIntrinsicUint64(spanStartColumnName, shared.ColumnTypeUint64, span.StartTimeUnixNano, rowIdx)
	b.addPresent(
		rowIdx,
		spanStartColumnName,
		shared.ColumnTypeUint64,
		shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: span.StartTimeUnixNano},
	)
	// Task T-TS-2: implied timestamp sketch — 1-second bucket granularity.
	if span.StartTimeUnixNano > 0 {
		b.colSketches.add(sketchTimestampColName, encodeSecondBucket(span.StartTimeUnixNano))
	}

	// span:end — written to block columns; synthesized from start+duration in intrinsic path.
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.EndTimeUnixNano)
		b.updateMinMax(spanEndColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	b.addPresent(
		rowIdx,
		spanEndColumnName,
		shared.ColumnTypeUint64,
		shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: span.EndTimeUnixNano},
	)

	// span:duration — always present.
	var dur uint64
	if span.EndTimeUnixNano >= span.StartTimeUnixNano {
		dur = span.EndTimeUnixNano - span.StartTimeUnixNano
	}
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], dur)
		b.updateMinMax(spanDurationColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	b.feedIntrinsicUint64(spanDurationColumnName, shared.ColumnTypeUint64, dur, rowIdx)
	b.addPresent(
		rowIdx,
		spanDurationColumnName,
		shared.ColumnTypeUint64,
		shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: dur},
	)

	// --- Conditional intrinsic columns ---

	if span.Status != nil {
		// Always feed span:status when the Status object is present, even for code 0
		// (STATUS_CODE_UNSET). This ensures the range index tracks unset status so that
		// { status = unset } queries can prune blocks correctly. Spans where span.Status
		// is nil entirely (the majority) remain null in the column — bloom filter handles
		// block pruning for those cases.
		b.feedIntrinsicInt64(spanStatusColumnName, shared.ColumnTypeInt64, int64(span.Status.Code), rowIdx)
		b.addPresent(
			rowIdx,
			spanStatusColumnName,
			shared.ColumnTypeInt64,
			shared.AttrValue{Type: shared.ColumnTypeInt64, Int: int64(span.Status.Code)},
		)
		if span.Status.Message != "" {
			b.feedIntrinsicString(spanStatusMsgColumnName, shared.ColumnTypeString, span.Status.Message, rowIdx)
			b.addPresent(
				rowIdx,
				spanStatusMsgColumnName,
				shared.ColumnTypeString,
				shared.AttrValue{Type: shared.ColumnTypeString, Str: span.Status.Message},
			)
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
			if kv.Key == svcNameAttrKey && val.Type == shared.ColumnTypeString {
				// resource.service.name is written to BOTH intrinsic section and block column (dual storage).
				if val.Str != "" {
					b.updateMinMax(svcNameColumnName, shared.ColumnTypeRangeString, val.Str)
				}
				b.feedIntrinsicString(svcNameColumnName, shared.ColumnTypeString, val.Str, rowIdx)
				b.addPresent(rowIdx, name, val.Type, val)
			} else {
				b.addPresent(rowIdx, name, val.Type, val)
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
		b.minStart = min(b.minStart, span.StartTimeUnixNano)
		b.maxStart = max(b.maxStart, span.StartTimeUnixNano)
		if traceIDBefore(ps.traceID, b.minTraceID) {
			b.minTraceID = ps.traceID
		}
		if traceIDBefore(b.maxTraceID, ps.traceID) {
			b.maxTraceID = ps.traceID
		}
	}

	// Track which traces appear in this block.
	b.traceRows[ps.traceID] = struct{}{}

	b.spanCount++
}

// addRowFromTempoProto adds all column values for one span row from a pendingSpan sourced from
// Tempo-native proto types. Mirrors addRowFromProto for tempocommon/tempotrace types.
//
//nolint:dupl // intentional mirror of addRowFromProto for Tempo types; different field types prevent sharing
func (b *blockBuilder) addRowFromTempoProto(
	ps *pendingSpan,
	rowIdx int,
) { //nolint:cyclop // mirrors addRowFromProto complexity
	span := ps.tempoSpan

	// --- Intrinsic columns — feed accumulator AND write block columns (dual storage) ---

	b.feedIntrinsicBytes(traceIDColumnName, shared.ColumnTypeBytes, span.TraceId, rowIdx)
	b.addPresent(
		rowIdx,
		traceIDColumnName,
		shared.ColumnTypeBytes,
		shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: span.TraceId},
	)

	if len(span.SpanId) > 0 {
		b.updateMinMax(spanIDColumnName, shared.ColumnTypeBytes, string(span.SpanId))
		b.feedIntrinsicBytes(spanIDColumnName, shared.ColumnTypeBytes, span.SpanId, rowIdx)
		b.addPresent(
			rowIdx,
			spanIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: span.SpanId},
		)
	}

	if len(span.ParentSpanId) > 0 {
		b.updateMinMax(spanParentIDColumnName, shared.ColumnTypeBytes, string(span.ParentSpanId))
		b.feedIntrinsicBytes(spanParentIDColumnName, shared.ColumnTypeBytes, span.ParentSpanId, rowIdx)
		b.addPresent(
			rowIdx,
			spanParentIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: span.ParentSpanId},
		)
	}

	spanName := span.Name
	if len(spanName) > shared.MaxStringLen {
		spanName = spanName[:shared.MaxStringLen]
	}
	if spanName != "" {
		b.updateMinMax(spanNameColumnName, shared.ColumnTypeString, spanName)
		b.feedIntrinsicString(spanNameColumnName, shared.ColumnTypeString, spanName, rowIdx)
		b.addPresent(
			rowIdx,
			spanNameColumnName,
			shared.ColumnTypeString,
			shared.AttrValue{Type: shared.ColumnTypeString, Str: spanName},
		)
	}

	spanKind := int64(span.Kind)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(
			tmp[:],
			uint64(spanKind), //nolint:gosec // G115: safe reinterpret int64 bits as uint64
		)
		b.updateMinMax(spanKindColumnName, shared.ColumnTypeInt64, string(tmp[:]))
	}
	b.feedIntrinsicInt64(spanKindColumnName, shared.ColumnTypeInt64, spanKind, rowIdx)
	b.addPresent(
		rowIdx,
		spanKindColumnName,
		shared.ColumnTypeInt64,
		shared.AttrValue{Type: shared.ColumnTypeInt64, Int: spanKind},
	)

	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.StartTimeUnixNano)
		b.updateMinMax(spanStartColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	b.feedIntrinsicUint64(spanStartColumnName, shared.ColumnTypeUint64, span.StartTimeUnixNano, rowIdx)
	b.addPresent(
		rowIdx,
		spanStartColumnName,
		shared.ColumnTypeUint64,
		shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: span.StartTimeUnixNano},
	)
	if span.StartTimeUnixNano > 0 {
		b.colSketches.add(sketchTimestampColName, encodeSecondBucket(span.StartTimeUnixNano))
	}

	// span:end — written to block columns; synthesized from start+duration in intrinsic path.
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], span.EndTimeUnixNano)
		b.updateMinMax(spanEndColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	b.addPresent(
		rowIdx,
		spanEndColumnName,
		shared.ColumnTypeUint64,
		shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: span.EndTimeUnixNano},
	)

	var dur uint64
	if span.EndTimeUnixNano >= span.StartTimeUnixNano {
		dur = span.EndTimeUnixNano - span.StartTimeUnixNano
	}
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], dur)
		b.updateMinMax(spanDurationColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	b.feedIntrinsicUint64(spanDurationColumnName, shared.ColumnTypeUint64, dur, rowIdx)
	b.addPresent(
		rowIdx,
		spanDurationColumnName,
		shared.ColumnTypeUint64,
		shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: dur},
	)

	if span.Status != nil {
		b.feedIntrinsicInt64(spanStatusColumnName, shared.ColumnTypeInt64, int64(span.Status.Code), rowIdx)
		b.addPresent(
			rowIdx,
			spanStatusColumnName,
			shared.ColumnTypeInt64,
			shared.AttrValue{Type: shared.ColumnTypeInt64, Int: int64(span.Status.Code)},
		)
		if span.Status.Message != "" {
			b.feedIntrinsicString(spanStatusMsgColumnName, shared.ColumnTypeString, span.Status.Message, rowIdx)
			b.addPresent(
				rowIdx,
				spanStatusMsgColumnName,
				shared.ColumnTypeString,
				shared.AttrValue{Type: shared.ColumnTypeString, Str: span.Status.Message},
			)
		}
	}

	if span.TraceState != "" {
		b.addPresent(rowIdx, "trace:state", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  span.TraceState,
		})
	}

	if ps.tempoRS != nil && ps.tempoRS.SchemaUrl != "" {
		b.addPresent(rowIdx, "resource:schema_url", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  ps.tempoRS.SchemaUrl,
		})
	}

	if ps.tempoSS != nil && ps.tempoSS.SchemaUrl != "" {
		b.addPresent(rowIdx, "scope:schema_url", shared.ColumnTypeString, shared.AttrValue{
			Type: shared.ColumnTypeString,
			Str:  ps.tempoSS.SchemaUrl,
		})
	}

	for _, kv := range span.Attributes {
		if kv == nil {
			continue
		}
		name := b.internColName(kv.Key, b.spanColNames, "span.")
		val := tempoToAttrValue(kv.Value)
		b.addPresent(rowIdx, name, val.Type, val)
	}

	if ps.tempoRS != nil && ps.tempoRS.Resource != nil {
		for _, kv := range ps.tempoRS.Resource.Attributes {
			if kv == nil {
				continue
			}
			name := b.internColName(kv.Key, b.resourceColNames, "resource.")
			val := tempoToAttrValue(kv.Value)
			if kv.Key == svcNameAttrKey && val.Type == shared.ColumnTypeString {
				// resource.service.name is written to BOTH intrinsic section and block column (dual storage).
				if val.Str != "" {
					b.updateMinMax(svcNameColumnName, shared.ColumnTypeRangeString, val.Str)
				}
				b.feedIntrinsicString(svcNameColumnName, shared.ColumnTypeString, val.Str, rowIdx)
				b.addPresent(rowIdx, name, val.Type, val)
			} else {
				b.addPresent(rowIdx, name, val.Type, val)
			}
		}
	}

	if ps.tempoSS != nil && ps.tempoSS.Scope != nil {
		for _, kv := range ps.tempoSS.Scope.Attributes {
			if kv == nil {
				continue
			}
			name := b.internColName(kv.Key, b.scopeColNames, "scope.")
			val := tempoToAttrValue(kv.Value)
			b.addPresent(rowIdx, name, val.Type, val)
		}
	}

	if b.spanCount == 0 {
		b.minStart = span.StartTimeUnixNano
		b.maxStart = span.StartTimeUnixNano
		b.minTraceID = ps.traceID
		b.maxTraceID = ps.traceID
	} else {
		b.minStart = min(b.minStart, span.StartTimeUnixNano)
		b.maxStart = max(b.maxStart, span.StartTimeUnixNano)
		if traceIDBefore(ps.traceID, b.minTraceID) {
			b.minTraceID = ps.traceID
		}
		if traceIDBefore(b.maxTraceID, ps.traceID) {
			b.maxTraceID = ps.traceID
		}
	}

	b.traceRows[ps.traceID] = struct{}{}
	b.spanCount++
}

// traceIDBefore reports whether trace ID a comes before b in lexicographic order.
func traceIDBefore(a, b [16]byte) bool {
	return bytes.Compare(a[:], b[:]) < 0
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

// applyTraceID feeds trace:id into the intrinsic accumulator and writes to block column (dual storage).
// Returns the extracted traceID and whether it was valid (used for bookkeeping).
func (b *blockBuilder) applyTraceID(
	col *modules_reader.Column,
	srcRowIdx, dstRowIdx int,
) (traceID [16]byte, found bool) {
	if v, ok := col.BytesValue(srcRowIdx); ok && len(v) == 16 {
		copy(traceID[:], v)
		b.feedIntrinsicBytes("trace:id", shared.ColumnTypeBytes, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			traceIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: v},
		)
		return traceID, true
	}
	return traceID, false
}

// applySpanID feeds span:id into the intrinsic accumulator and writes to block column (dual storage).
func (b *blockBuilder) applySpanID(col *modules_reader.Column, srcRowIdx, dstRowIdx int) {
	if v, ok := col.BytesValue(srcRowIdx); ok && len(v) > 0 {
		b.updateMinMax(spanIDColumnName, shared.ColumnTypeBytes, string(v))
		b.feedIntrinsicBytes(spanIDColumnName, shared.ColumnTypeBytes, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: v},
		)
	}
}

// applySpanParentID feeds span:parent_id into the intrinsic accumulator and writes to block column (dual storage).
func (b *blockBuilder) applySpanParentID(col *modules_reader.Column, srcRowIdx, dstRowIdx int) {
	if v, ok := col.BytesValue(srcRowIdx); ok && len(v) > 0 {
		b.updateMinMax(spanParentIDColumnName, shared.ColumnTypeBytes, string(v))
		b.feedIntrinsicBytes(spanParentIDColumnName, shared.ColumnTypeBytes, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanParentIDColumnName,
			shared.ColumnTypeBytes,
			shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: v},
		)
	}
}

// applySpanName feeds span:name into the intrinsic accumulator and writes to block column (dual storage).
func (b *blockBuilder) applySpanName(col *modules_reader.Column, srcRowIdx, dstRowIdx int) {
	if v, ok := col.StringValue(srcRowIdx); ok && v != "" {
		b.updateMinMax(spanNameColumnName, shared.ColumnTypeString, v)
		b.feedIntrinsicString(spanNameColumnName, shared.ColumnTypeString, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanNameColumnName,
			shared.ColumnTypeString,
			shared.AttrValue{Type: shared.ColumnTypeString, Str: v},
		)
	}
}

// applySpanKind feeds span:kind into the intrinsic accumulator and writes to block column (dual storage).
func (b *blockBuilder) applySpanKind(col *modules_reader.Column, srcRowIdx, dstRowIdx int) {
	if v, ok := col.Int64Value(srcRowIdx); ok {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
		b.updateMinMax(spanKindColumnName, shared.ColumnTypeInt64, string(tmp[:]))
		b.feedIntrinsicInt64(spanKindColumnName, shared.ColumnTypeInt64, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanKindColumnName,
			shared.ColumnTypeInt64,
			shared.AttrValue{Type: shared.ColumnTypeInt64, Int: v},
		)
	}
}

// applySpanStart feeds span:start into the intrinsic accumulator and writes to block column (dual storage).
// Returns the value and whether it was present (used by finalizeRowBookkeeping).
func (b *blockBuilder) applySpanStart(col *modules_reader.Column, srcRowIdx, dstRowIdx int) (uint64, bool) {
	if v, ok := col.Uint64Value(srcRowIdx); ok {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], v)
		b.updateMinMax(spanStartColumnName, shared.ColumnTypeUint64, string(tmp[:]))
		b.feedIntrinsicUint64(spanStartColumnName, shared.ColumnTypeUint64, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanStartColumnName,
			shared.ColumnTypeUint64,
			shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: v},
		)
		return v, true
	}
	return 0, false
}

// applySpanEnd updates span:end min/max and writes to block column (dual storage).
// span:end is NOT written to the intrinsic section — it is synthesized on read from start+duration.
// Written to block column payloads (dual storage); NOT written to intrinsic section.
// Returns the value and whether it was present (used by finalizeRowBookkeeping).
func (b *blockBuilder) applySpanEnd(col *modules_reader.Column, srcRowIdx, dstRowIdx int) (uint64, bool) {
	if v, ok := col.Uint64Value(srcRowIdx); ok {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], v)
		b.updateMinMax(spanEndColumnName, shared.ColumnTypeUint64, string(tmp[:]))
		b.addPresent(
			dstRowIdx,
			spanEndColumnName,
			shared.ColumnTypeUint64,
			shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: v},
		)
		return v, true
	}
	return 0, false
}

// applySpanDuration feeds span:duration into the intrinsic accumulator and writes to block column (dual storage).
func (b *blockBuilder) applySpanDuration(col *modules_reader.Column, srcRowIdx, dstRowIdx int) bool {
	if v, ok := col.Uint64Value(srcRowIdx); ok {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], v)
		b.updateMinMax(spanDurationColumnName, shared.ColumnTypeUint64, string(tmp[:]))
		b.feedIntrinsicUint64(spanDurationColumnName, shared.ColumnTypeUint64, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanDurationColumnName,
			shared.ColumnTypeUint64,
			shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: v},
		)
		return true
	}
	return false
}

// applySpanStatus feeds the span:status column value into the intrinsic accumulator and writes to block column (dual storage).
func (b *blockBuilder) applySpanStatus(col *modules_reader.Column, srcRowIdx, dstRowIdx int) {
	if v, ok := col.Int64Value(srcRowIdx); ok {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
		b.updateMinMax(spanStatusColumnName, shared.ColumnTypeInt64, string(tmp[:]))
		b.feedIntrinsicInt64(spanStatusColumnName, shared.ColumnTypeInt64, v, dstRowIdx)
		b.addPresent(
			dstRowIdx,
			spanStatusColumnName,
			shared.ColumnTypeInt64,
			shared.AttrValue{Type: shared.ColumnTypeInt64, Int: v},
		)
	}
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
			traceID, traceIDFound = b.applyTraceID(col, srcRowIdx, dstRowIdx)
			continue

		case spanIDColumnName:
			b.applySpanID(col, srcRowIdx, dstRowIdx)
			continue

		case spanParentIDColumnName:
			b.applySpanParentID(col, srcRowIdx, dstRowIdx)
			continue

		case spanNameColumnName:
			b.applySpanName(col, srcRowIdx, dstRowIdx)
			continue

		case spanKindColumnName:
			b.applySpanKind(col, srcRowIdx, dstRowIdx)
			continue

		case spanStartColumnName:
			spanStart, spanStartFound = b.applySpanStart(col, srcRowIdx, dstRowIdx)
			continue

		case spanEndColumnName:
			spanEnd, spanEndFound = b.applySpanEnd(col, srcRowIdx, dstRowIdx)
			continue

		case spanDurationColumnName:
			durationFound = b.applySpanDuration(col, srcRowIdx, dstRowIdx)
			continue

		case spanStatusColumnName:
			b.applySpanStatus(col, srcRowIdx, dstRowIdx)
			continue

		case spanStatusMsgColumnName:
			if v, ok := col.StringValue(srcRowIdx); ok && v != "" {
				b.feedIntrinsicString(spanStatusMsgColumnName, shared.ColumnTypeString, v, dstRowIdx)
				b.addPresent(
					dstRowIdx,
					spanStatusMsgColumnName,
					shared.ColumnTypeString,
					shared.AttrValue{Type: shared.ColumnTypeString, Str: v},
				)
			}
			continue

		case svcNameColumnName:
			if v, ok := col.StringValue(srcRowIdx); ok && v != "" {
				b.updateMinMax(svcNameColumnName, shared.ColumnTypeRangeString, v)
				b.feedIntrinsicString(svcNameColumnName, shared.ColumnTypeString, v, dstRowIdx)
				b.addPresent(
					dstRowIdx,
					svcNameColumnName,
					shared.ColumnTypeString,
					shared.AttrValue{Type: shared.ColumnTypeString, Str: v},
				)
			}
			continue
		}

		// Dynamic attribute columns: read typed value and call addPresent to write to block column.
		val, ok := readDynAttrValue(col, srcRowIdx, baseType)
		if !ok {
			continue
		}
		b.addPresent(dstRowIdx, colKey.Name, baseType, val)
	}

	b.finalizeRowBookkeeping(
		dstRowIdx, traceID, traceIDFound,
		spanStart, spanStartFound, spanEnd, spanEndFound, durationFound,
	)
}

// finalizeRowBookkeeping handles post-column-copy bookkeeping for addRowFromBlock:
// derives missing duration, updates min/max start/traceID, and increments spanCount.
func (b *blockBuilder) finalizeRowBookkeeping(
	dstRowIdx int, traceID [16]byte, traceIDFound bool,
	spanStart uint64, spanStartFound bool, spanEnd uint64, spanEndFound bool, durationFound bool,
) {
	// Derive duration from start+end if source block lacked span:duration.
	if !durationFound && spanStartFound && spanEndFound {
		var dur uint64
		if spanEnd >= spanStart {
			dur = spanEnd - spanStart
		}
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], dur)
		b.updateMinMax(spanDurationColumnName, shared.ColumnTypeUint64, string(tmp[:]))
		b.feedIntrinsicUint64(spanDurationColumnName, shared.ColumnTypeUint64, dur, dstRowIdx)
	}

	// Update min/max start time and trace ID bookkeeping.
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
			b.minStart = min(b.minStart, spanStart)
			b.maxStart = max(b.maxStart, spanStart)
		}
		if traceIDFound {
			if traceIDBefore(traceID, b.minTraceID) {
				b.minTraceID = traceID
			}
			if traceIDBefore(b.maxTraceID, traceID) {
				b.maxTraceID = traceID
			}
		}
	}

	if traceIDFound {
		b.traceRows[traceID] = struct{}{}
	}

	// Task T-TS-2: implied timestamp sketch for compaction path.
	if spanStartFound && spanStart > 0 {
		b.colSketches.add(sketchTimestampColName, encodeSecondBucket(spanStart))
	}

	b.spanCount++
}

// intrinsicRowFields is a per-row value cache built once per source block during compaction.
// Key: rowIdx (uint16). Value: map of intrinsic field name → typed value.
// Built by buildIntrinsicBlockIndex; consumed by feedIntrinsicsFromIndex.
type intrinsicRowFields = map[uint16]map[string]any

// buildIntrinsicBlockIndex builds a per-row intrinsic field cache for the given
// (reader, srcBlockIdx) pair. Each intrinsic column is read once (O(N) over the
// column's BlockRefs), avoiding the O(N) per-row linear scan done by IntrinsicBytesAt
// and friends. Returns nil when r has no intrinsic section.
//
// The result maps typed values using the same Go types that feedIntrinsicsFromIndex
// switches on: []byte for bytes columns, uint64 for uint64 columns, string for string
// columns, and int64 for int64 columns.
func buildIntrinsicBlockIndex(r *modules_reader.Reader, srcBlockIdx int) intrinsicRowFields {
	if r == nil {
		return nil
	}
	names := r.IntrinsicColumnNames()
	if len(names) == 0 {
		return nil
	}
	out := make(intrinsicRowFields)
	for _, colName := range names {
		col, err := r.GetIntrinsicColumn(colName)
		if err != nil || col == nil {
			continue
		}
		switch col.Format {
		case shared.IntrinsicFormatFlat:
			for i, ref := range col.BlockRefs {
				if int(ref.BlockIdx) != srcBlockIdx {
					continue
				}
				if out[ref.RowIdx] == nil {
					out[ref.RowIdx] = make(map[string]any, 10)
				}
				if len(col.Uint64Values) > i {
					out[ref.RowIdx][colName] = col.Uint64Values[i]
				} else if len(col.BytesValues) > i {
					out[ref.RowIdx][colName] = col.BytesValues[i]
				}
			}
		case shared.IntrinsicFormatDict:
			for _, entry := range col.DictEntries {
				for _, ref := range entry.BlockRefs {
					if int(ref.BlockIdx) != srcBlockIdx {
						continue
					}
					if out[ref.RowIdx] == nil {
						out[ref.RowIdx] = make(map[string]any, 10)
					}
					if col.Type == shared.ColumnTypeInt64 || col.Type == shared.ColumnTypeRangeInt64 {
						out[ref.RowIdx][colName] = entry.Int64Val
					} else {
						out[ref.RowIdx][colName] = entry.Value
					}
				}
			}
		}
	}
	return out
}

// feedIntrinsicsFromIndex copies intrinsic column values from a pre-built per-block
// index (see buildIntrinsicBlockIndex) into this block's intrinsic accumulator at
// dstRowIdx. O(1) per call — the index is built once per source block.
// Used by the compaction path when source blocks no longer carry intrinsic columns
// in their block-column storage.
func (b *blockBuilder) feedIntrinsicsFromIndex(index intrinsicRowFields, srcRowIdx, dstRowIdx int) {
	if index == nil || b.intrinsicAccum == nil {
		return
	}
	fields, ok := index[uint16(srcRowIdx)] //nolint:gosec // bounded by SpanCount (<= 65535)
	if !ok {
		return
	}
	if v, ok := fields["trace:id"]; ok {
		if bv, ok := v.([]byte); ok {
			b.feedIntrinsicBytes("trace:id", shared.ColumnTypeBytes, bv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				traceIDColumnName,
				shared.ColumnTypeBytes,
				shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: bv},
			)
		}
	}
	if v, ok := fields[spanIDColumnName]; ok {
		if bv, ok := v.([]byte); ok {
			b.updateMinMax(spanIDColumnName, shared.ColumnTypeBytes, string(bv))
			b.feedIntrinsicBytes(spanIDColumnName, shared.ColumnTypeBytes, bv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanIDColumnName,
				shared.ColumnTypeBytes,
				shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: bv},
			)
		}
	}
	if v, ok := fields[spanParentIDColumnName]; ok {
		if bv, ok := v.([]byte); ok {
			b.updateMinMax(spanParentIDColumnName, shared.ColumnTypeBytes, string(bv))
			b.feedIntrinsicBytes(spanParentIDColumnName, shared.ColumnTypeBytes, bv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanParentIDColumnName,
				shared.ColumnTypeBytes,
				shared.AttrValue{Type: shared.ColumnTypeBytes, Bytes: bv},
			)
		}
	}
	if v, ok := fields[spanNameColumnName]; ok {
		if sv, ok := v.(string); ok && sv != "" {
			b.updateMinMax(spanNameColumnName, shared.ColumnTypeString, sv)
			b.feedIntrinsicString(spanNameColumnName, shared.ColumnTypeString, sv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanNameColumnName,
				shared.ColumnTypeString,
				shared.AttrValue{Type: shared.ColumnTypeString, Str: sv},
			)
		}
	}
	if v, ok := fields[spanKindColumnName]; ok {
		if iv, ok := v.(int64); ok {
			var tmp [8]byte
			binary.LittleEndian.PutUint64(
				tmp[:],
				uint64(iv), //nolint:gosec // G115: safe reinterpret int64 bits as uint64
			)
			b.updateMinMax(spanKindColumnName, shared.ColumnTypeInt64, string(tmp[:]))
			b.feedIntrinsicInt64(spanKindColumnName, shared.ColumnTypeInt64, iv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanKindColumnName,
				shared.ColumnTypeInt64,
				shared.AttrValue{Type: shared.ColumnTypeInt64, Int: iv},
			)
		}
	}
	if v, ok := fields[spanStartColumnName]; ok {
		if uv, ok := v.(uint64); ok {
			var tmp [8]byte
			binary.LittleEndian.PutUint64(tmp[:], uv)
			b.updateMinMax(spanStartColumnName, shared.ColumnTypeUint64, string(tmp[:]))
			b.feedIntrinsicUint64(spanStartColumnName, shared.ColumnTypeUint64, uv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanStartColumnName,
				shared.ColumnTypeUint64,
				shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: uv},
			)
			if uv > 0 {
				b.colSketches.add(sketchTimestampColName, encodeSecondBucket(uv))
			}
		}
	}
	if v, ok := fields[spanDurationColumnName]; ok {
		if uv, ok := v.(uint64); ok {
			var tmp [8]byte
			binary.LittleEndian.PutUint64(tmp[:], uv)
			b.updateMinMax(spanDurationColumnName, shared.ColumnTypeUint64, string(tmp[:]))
			b.feedIntrinsicUint64(spanDurationColumnName, shared.ColumnTypeUint64, uv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanDurationColumnName,
				shared.ColumnTypeUint64,
				shared.AttrValue{Type: shared.ColumnTypeUint64, Uint: uv},
			)
		}
	}
	if v, ok := fields[spanStatusColumnName]; ok {
		if iv, ok := v.(int64); ok {
			var tmp [8]byte
			binary.LittleEndian.PutUint64(
				tmp[:],
				uint64(iv), //nolint:gosec // G115: safe reinterpret int64 bits as uint64
			)
			b.updateMinMax(spanStatusColumnName, shared.ColumnTypeInt64, string(tmp[:]))
			b.feedIntrinsicInt64(spanStatusColumnName, shared.ColumnTypeInt64, iv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanStatusColumnName,
				shared.ColumnTypeInt64,
				shared.AttrValue{Type: shared.ColumnTypeInt64, Int: iv},
			)
		}
	}
	if v, ok := fields[spanStatusMsgColumnName]; ok {
		if sv, ok := v.(string); ok && sv != "" {
			b.feedIntrinsicString(spanStatusMsgColumnName, shared.ColumnTypeString, sv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				spanStatusMsgColumnName,
				shared.ColumnTypeString,
				shared.AttrValue{Type: shared.ColumnTypeString, Str: sv},
			)
		}
	}
	if v, ok := fields[svcNameColumnName]; ok {
		if sv, ok := v.(string); ok && sv != "" {
			b.updateMinMax(svcNameColumnName, shared.ColumnTypeRangeString, sv)
			b.feedIntrinsicString(svcNameColumnName, shared.ColumnTypeString, sv, dstRowIdx)
			b.addPresent(
				dstRowIdx,
				svcNameColumnName,
				shared.ColumnTypeString,
				shared.AttrValue{Type: shared.ColumnTypeString, Str: sv},
			)
		}
	}
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
// value in the sketch accumulators (HLL, TopK, BinaryFuse8 keys).
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
// blockVersion controls the on-disk layout:
//   - shared.VersionBlockV12+: omits the 16-byte stats_offset/stats_len stubs per column
//   - earlier: includes the stats_offset/stats_len stubs (always 0, dead data)
func (b *blockBuilder) finalize(enc *zstdEncoder, blockVersion uint8) ([]byte, error) {
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
	slices.SortFunc(entries, func(a, b colEntry) int {
		if a.key.Name != b.key.Name {
			return cmp.Compare(a.key.Name, b.key.Name)
		}
		return cmp.Compare(a.key.Type, b.key.Type)
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
	// VersionBlockV12+: name_len[2] + name + col_type[1] + data_offset[8] + data_len[8]
	// Earlier:          name_len[2] + name + col_type[1] + data_offset[8] + data_len[8] + stats_offset[8] + stats_len[8]
	statsFieldSize := 0
	if blockVersion < shared.VersionBlockV12 {
		statsFieldSize = 16 // stats_offset[8] + stats_len[8]
	}
	colMetaSize := 0
	for _, bl := range blobs {
		colMetaSize += 2 + len(bl.name) + 1 + 8 + 8 + statsFieldSize
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
	payload = append(payload, blockVersion)
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
		// stats_offset[8 LE] + stats_len[8 LE] — omitted in VersionBlockV12+ (always were 0)
		if blockVersion < shared.VersionBlockV12 {
			payload = appendUint64LE(payload, 0)
			payload = appendUint64LE(payload, 0)
		}

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

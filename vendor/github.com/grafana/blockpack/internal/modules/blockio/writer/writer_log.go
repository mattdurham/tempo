package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math"
	"slices"
	"strconv"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// pendingLogRecord is a lightweight log record buffered before sorting and flushing.
// Stores only the sort keys and proto pointers; full OTLP→column decoding is deferred
// to addLogRecordFromProto, eliminating per-record materialization during buffering.
//
// Size: ~88 bytes (3 pointer fields + sort keys).
// The proto fields are kept alive by w.logProtoRoots for the duration of the flush cycle.
type pendingLogRecord struct {
	rl         *logsv1.ResourceLogs // proto pointer; kept alive by w.logProtoRoots
	sl         *logsv1.ScopeLogs    // proto pointer; kept alive by w.logProtoRoots
	record     *logsv1.LogRecord    // proto pointer; kept alive by w.logProtoRoots
	svcName    string               // sort key (primary); zero-copy reference into proto
	minHashSig [4]uint64            // sort key (secondary)
	timestamp  uint64               // sort key (tertiary): TimeUnixNano
}

// logBlockBuilder manages construction of a single log block.
// Mirrors blockBuilder but with log-specific intrinsic columns and no traceRows map.
type logBlockBuilder struct {
	// Intrinsic column builders (log-specific, pre-created for O(1) write).
	colTimestamp         *uint64ColumnBuilder
	colObservedTimestamp *uint64ColumnBuilder
	colBody              *stringColumnBuilder
	colSeverityNumber    *int64ColumnBuilder
	colSeverityText      *stringColumnBuilder
	colTraceID           *bytesColumnBuilder
	colSpanID            *bytesColumnBuilder
	colFlags             *uint64ColumnBuilder

	columns          map[shared.ColumnKey]columnBuilder
	logColNames      map[string]string // log attribute key → "log.{key}" (cache)
	resourceColNames map[string]string // resource attribute key → "resource.{key}" (cache)
	scopeColNames    map[string]string // scope attribute key → "scope.{key}" (cache)

	colMinMax        map[string]*blockColMinMax
	colNumericMinMax map[string]*blockColMinMax // int64-encoded min/max for all-numeric string cols
	colFloatMinMax   map[string]*blockColMinMax // float64-encoded min/max for all-float string cols
	colNonNumeric    map[string]bool            // true if any non-empty value failed int64 parse
	colNonFloat      map[string]bool            // true if any non-empty value failed float64 parse
	// colSketches accumulates HLL, TopK, and fuse keys per column for this log block.
	colSketches blockSketchSet

	sparseColumns []columnBuilder

	recordCount int
	recordHint  int
	minStart    uint64
	maxStart    uint64
}

// newLogBlockBuilder creates an empty log block builder.
// recordHint is the expected number of log records; used to pre-allocate slices.
func newLogBlockBuilder(recordHint int) *logBlockBuilder {
	b := &logBlockBuilder{
		columns:          make(map[shared.ColumnKey]columnBuilder, 32),
		colMinMax:        make(map[string]*blockColMinMax, 64),
		colNumericMinMax: make(map[string]*blockColMinMax, 32),
		colFloatMinMax:   make(map[string]*blockColMinMax, 32),
		colNonNumeric:    make(map[string]bool, 32),
		colNonFloat:      make(map[string]bool, 32),
		sparseColumns:    make([]columnBuilder, 0, 32),
		colSketches:      newBlockSketchSet(),
		recordHint:       recordHint,
		logColNames:      make(map[string]string, 32),
		resourceColNames: make(map[string]string, 16),
		scopeColNames:    make(map[string]string, 8),
	}

	// Pre-create always-present intrinsic column builders.
	b.colTimestamp = &uint64ColumnBuilder{
		colName: logTimestampColumnName,
		values:  make([]uint64, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colObservedTimestamp = &uint64ColumnBuilder{
		colName: logObservedTimestampColumnName,
		values:  make([]uint64, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colBody = &stringColumnBuilder{
		colName: logBodyColumnName,
		values:  make([]string, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colSeverityNumber = &int64ColumnBuilder{
		values:  make([]int64, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colSeverityText = &stringColumnBuilder{
		colName: logSeverityTextColumnName,
		values:  make([]string, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colTraceID = &bytesColumnBuilder{
		colName: logTraceIDColumnName,
		values:  make([][]byte, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colSpanID = &bytesColumnBuilder{
		colName: logSpanIDColumnName,
		values:  make([][]byte, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}
	b.colFlags = &uint64ColumnBuilder{
		colName: logFlagsColumnName,
		values:  make([]uint64, 0, recordHint),
		present: make([]bool, 0, recordHint),
	}

	// Register in map so finalize() can collect them.
	b.columns[shared.ColumnKey{Name: logTimestampColumnName, Type: shared.ColumnTypeUint64}] = b.colTimestamp
	b.columns[shared.ColumnKey{Name: logObservedTimestampColumnName, Type: shared.ColumnTypeUint64}] = b.colObservedTimestamp
	b.columns[shared.ColumnKey{Name: logBodyColumnName, Type: shared.ColumnTypeString}] = b.colBody
	b.columns[shared.ColumnKey{Name: logSeverityNumberColumnName, Type: shared.ColumnTypeInt64}] = b.colSeverityNumber
	b.columns[shared.ColumnKey{Name: logSeverityTextColumnName, Type: shared.ColumnTypeString}] = b.colSeverityText
	b.columns[shared.ColumnKey{Name: logTraceIDColumnName, Type: shared.ColumnTypeBytes}] = b.colTraceID
	b.columns[shared.ColumnKey{Name: logSpanIDColumnName, Type: shared.ColumnTypeBytes}] = b.colSpanID
	b.columns[shared.ColumnKey{Name: logFlagsColumnName, Type: shared.ColumnTypeUint64}] = b.colFlags

	return b
}

// internLogColName returns the full column name for an attribute key and prefix,
// using cache to avoid per-record string concatenation allocations.
func (b *logBlockBuilder) internLogColName(key string, cache map[string]string, prefix string) string {
	if cached, ok := cache[key]; ok {
		return cached
	}
	name := prefix + key
	cache[key] = name
	return name
}

// addLogColumn ensures a column builder exists for the given name/type.
// Dynamic (non-intrinsic) columns are also tracked in b.sparseColumns for null-filling.
// Because columns is keyed by (name, type), two columns with the same name but
// different types are stored independently — no data loss on type conflicts.
func (b *logBlockBuilder) addLogColumn(name string, typ shared.ColumnType) columnBuilder {
	key := shared.ColumnKey{Name: name, Type: typ}
	if cb, ok := b.columns[key]; ok {
		return cb
	}
	cb := newColumnBuilder(typ, name, max(b.recordHint-b.recordCount, 0))
	for range b.recordCount {
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
	b.columns[key] = cb
	b.sparseColumns = append(b.sparseColumns, cb)
	return cb
}

// fillNullsForLogRow adds a null row to all sparse (non-intrinsic) columns whose
// rowCount is behind rowIdx.
func (b *logBlockBuilder) fillNullsForLogRow(rowIdx int) {
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

// updateLogMinMax updates the per-block min/max for the named column and records
// the value in the sketch accumulators (HLL, TopK, BinaryFuse8 keys).
// Delegates to the same logic as blockBuilder.updateMinMax (same types, same semantics).
func (b *logBlockBuilder) updateLogMinMax(name string, typ shared.ColumnType, key string) {
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

// updateNumericMinMax tracks int64 min/max for a string column being evaluated
// for numeric range index promotion. Keys are 8-byte LE-encoded (same wire
// format as native int64 columns) so addBlockRangeToColumn can decode them.
// NOTE-040: numeric string range index promotion.
func (b *logBlockBuilder) updateNumericMinMax(name string, n int64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(n)) //nolint:gosec // safe: reinterpreting int64 bits
	key := string(tmp[:])
	if mm, ok := b.colNumericMinMax[name]; ok {
		existingMin := int64(binary.LittleEndian.Uint64([]byte(mm.minKey))) //nolint:gosec
		existingMax := int64(binary.LittleEndian.Uint64([]byte(mm.maxKey))) //nolint:gosec
		if n < existingMin {
			mm.minKey = key
		}
		if n > existingMax {
			mm.maxKey = key
		}
	} else {
		b.colNumericMinMax[name] = &blockColMinMax{
			colName: name,
			minKey:  key,
			maxKey:  key,
			colType: shared.ColumnTypeRangeInt64,
		}
	}
}

// updateFloatMinMax tracks float64 min/max for string columns where all values
// parse as float64 but not int64. Keys are 8-byte LE IEEE-754 encoded.
// NOTE-040: float fallback for numeric string range index promotion.
func (b *logBlockBuilder) updateFloatMinMax(name string, f float64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(f))
	key := string(tmp[:])
	if mm, ok := b.colFloatMinMax[name]; ok {
		existingMin := math.Float64frombits(binary.LittleEndian.Uint64([]byte(mm.minKey)))
		existingMax := math.Float64frombits(binary.LittleEndian.Uint64([]byte(mm.maxKey)))
		if f < existingMin {
			mm.minKey = key
		}
		if f > existingMax {
			mm.maxKey = key
		}
	} else {
		b.colFloatMinMax[name] = &blockColMinMax{
			colName: name,
			minKey:  key,
			maxKey:  key,
			colType: shared.ColumnTypeRangeFloat64,
		}
	}
}

// addLogPresent writes a present (non-null) value to the named column and
// feeds the range index. Mirrors blockBuilder.addPresent.
func (b *logBlockBuilder) addLogPresent(name string, typ shared.ColumnType, val shared.AttrValue) {
	cb := b.addLogColumn(name, typ)
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
	default:
		cb.addBytes(val.Bytes, true)
	}
	// Feed range column index. Excluded: Bool, logTraceIDColumnName, logSpanIDColumnName
	// (IDs are unique per record; not useful for block pruning).
	if name != logTraceIDColumnName && name != logSpanIDColumnName && typ != shared.ColumnTypeBool {
		if key := encodeRangeKey(typ, val); key != "" {
			b.updateLogMinMax(name, typ, key)
		}
	}
	// NOTE-040: attempt numeric promotion for string columns.
	// Only string types carry string-encoded values; skip empty strings (null fills).
	// Int64 and float64 tracking are independent: a value that fails int64 parse (e.g.
	// "1.5") still enters float64 tracking. Both paths run to completion for every value
	// so colFloatMinMax always reflects the full block range, not a partial suffix.
	if (typ == shared.ColumnTypeString || typ == shared.ColumnTypeRangeString) && val.Str != "" {
		if !b.colNonNumeric[name] {
			if n, err := strconv.ParseInt(val.Str, 10, 64); err == nil {
				b.updateNumericMinMax(name, n)
			} else {
				b.colNonNumeric[name] = true
			}
		}
		if !b.colNonFloat[name] {
			if f, err := strconv.ParseFloat(val.Str, 64); err == nil && f >= 0 {
				b.updateFloatMinMax(name, f)
			} else {
				b.colNonFloat[name] = true
			}
		}
	}
}

// addLogRecordFromProto adds all column values for one log record row.
// Full OTLP→column decoding happens here (deferred from AddLogsData).
func (b *logBlockBuilder) addLogRecordFromProto(plr *pendingLogRecord, rowIdx int) {
	record := plr.record

	// --- Always-present intrinsic columns ---

	// log:timestamp — always present.
	b.colTimestamp.addUint64(record.TimeUnixNano, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], record.TimeUnixNano)
		b.updateLogMinMax(logTimestampColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}
	// Task T-TS-3: implied timestamp sketch — 1-second bucket granularity.
	if record.TimeUnixNano > 0 {
		b.colSketches.add(sketchTimestampColName, encodeSecondBucket(record.TimeUnixNano))
	}

	// log:observed_timestamp — always present (may be zero).
	b.colObservedTimestamp.addUint64(record.ObservedTimeUnixNano, true)
	{
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], record.ObservedTimeUnixNano)
		b.updateLogMinMax(logObservedTimestampColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}

	// log:body — present when Body is non-nil; absent (present=false) when nil.
	if record.Body != nil {
		bodyAttr := protoToAttrValue(record.Body)
		b.colBody.addString(bodyAttr.Str, true)
		if bodyAttr.Str != "" {
			b.updateLogMinMax(logBodyColumnName, shared.ColumnTypeString, bodyAttr.Str)
		}
		// SPEC-11.5: auto-parse body into log.{key} sparse range columns.
		// NOTE-37: silent failure — nil parseLogBody result leaves body as-is.
		if fields := parseLogBody(bodyAttr.Str); fields != nil {
			for k, v := range fields {
				colName := b.internLogColName(k, b.logColNames, "log.")
				b.addLogPresent(colName, shared.ColumnTypeRangeString, shared.AttrValue{
					Type: shared.ColumnTypeRangeString,
					Str:  v,
				})
			}
		}
	} else {
		b.colBody.addString("", false)
	}

	// log:severity_number — always written (null when zero).
	sevNum := int64(record.SeverityNumber)
	sevNumPresent := sevNum != 0
	b.colSeverityNumber.addInt64(sevNum, sevNumPresent)
	if sevNumPresent {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(sevNum)) //nolint:gosec // safe: reinterpreting int64 bits
		b.updateLogMinMax(logSeverityNumberColumnName, shared.ColumnTypeInt64, string(tmp[:]))
	}

	// log:severity_text — always written (null when empty).
	sevText := record.SeverityText
	sevTextPresent := sevText != ""
	b.colSeverityText.addString(sevText, sevTextPresent)
	if sevTextPresent {
		b.updateLogMinMax(logSeverityTextColumnName, shared.ColumnTypeString, sevText)
	}

	// log:trace_id — always written (null when absent).
	traceIDPresent := len(record.TraceId) > 0
	b.colTraceID.addBytes(record.TraceId, traceIDPresent)

	// log:span_id — always written (null when absent).
	spanIDPresent := len(record.SpanId) > 0
	b.colSpanID.addBytes(record.SpanId, spanIDPresent)

	// log:flags — always written (null when zero).
	flags := uint64(record.Flags) //nolint:gosec // safe: uint32→uint64 widening
	flagsPresent := flags != 0
	b.colFlags.addUint64(flags, flagsPresent)
	if flagsPresent {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], flags)
		b.updateLogMinMax(logFlagsColumnName, shared.ColumnTypeUint64, string(tmp[:]))
	}

	// --- Resource attributes ---
	if plr.rl != nil && plr.rl.Resource != nil {
		for _, kv := range plr.rl.Resource.Attributes {
			if kv == nil {
				continue
			}
			name := b.internLogColName(kv.Key, b.resourceColNames, "resource.")
			val := protoToAttrValue(kv.Value)
			b.addLogPresent(name, val.Type, val)
		}
	}

	// --- Scope attributes ---
	if plr.sl != nil && plr.sl.Scope != nil {
		for _, kv := range plr.sl.Scope.Attributes {
			if kv == nil {
				continue
			}
			name := b.internLogColName(kv.Key, b.scopeColNames, "scope.")
			val := protoToAttrValue(kv.Value)
			b.addLogPresent(name, val.Type, val)
		}
	}

	// --- Log record attributes ---
	for _, kv := range record.Attributes {
		if kv == nil {
			continue
		}
		name := b.internLogColName(kv.Key, b.logColNames, "log.")
		val := protoToAttrValue(kv.Value)
		b.addLogPresent(name, val.Type, val)
	}

	// Fill null values for columns that existed before this row but weren't written.
	b.fillNullsForLogRow(rowIdx)

	// Update min/max timestamp (used for BlockMeta.MinStart/MaxStart).
	if b.recordCount == 0 {
		b.minStart = record.TimeUnixNano
		b.maxStart = record.TimeUnixNano
	} else {
		if record.TimeUnixNano < b.minStart {
			b.minStart = record.TimeUnixNano
		}
		if record.TimeUnixNano > b.maxStart {
			b.maxStart = record.TimeUnixNano
		}
	}

	b.recordCount++
}

// sortPendingLogs sorts the pending log record buffer by
// (minHashSig ASC, timestamp ASC).
//
// NOTE-37: svcName was removed as primary key to enable label-value locality.
// MinHash over key=value pairs clusters similar streams together, producing
// blocks with tight per-column value ranges for effective range index pruning.
// This only affects log files — the trace sort in sortPending is unchanged.
// Uses the same index-sort pattern as sortPending to avoid copying large structs.
func sortPendingLogs(pending []pendingLogRecord) {
	n := len(pending)
	if n <= 1 {
		return
	}

	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	slices.SortFunc(indices, func(ai, bi int) int {
		a, b := &pending[ai], &pending[bi]
		for i := range 4 {
			if a.minHashSig[i] != b.minHashSig[i] {
				if a.minHashSig[i] < b.minHashSig[i] {
					return -1
				}
				return 1
			}
		}
		if a.timestamp < b.timestamp {
			return -1
		}
		if a.timestamp > b.timestamp {
			return 1
		}
		return 0
	})

	sorted := make([]pendingLogRecord, n)
	for i, idx := range indices {
		sorted[i] = pending[idx]
	}
	copy(pending, sorted)
}

// computeMinHashSigFromLog computes a compact MinHash signature for a pendingLogRecord's
// attribute set. Uses FNV-1a hashing of "key=value" pairs (not just key names) so that
// streams sharing identical attribute keys but different values produce distinct signatures.
// This improves block-level clustering and pruning for Loki-style log data where every
// stream has the same schema but different label values.
func computeMinHashSigFromLog(plr *pendingLogRecord) {
	plr.minHashSig = [4]uint64{
		^uint64(0), ^uint64(0), ^uint64(0), ^uint64(0),
	}

	// hashKV hashes "key=stringValue" for string attributes via AddKVHashToMinHeap;
	// falls back to AddHashToMinHeap (key-only) for non-string attributes (int, double,
	// bool, bytes) to avoid allocating a string representation of the value.
	hashKV := func(kv *commonv1.KeyValue) {
		if sv, ok := kv.Value.GetValue().(*commonv1.AnyValue_StringValue); ok {
			shared.AddKVHashToMinHeap(kv.Key, sv.StringValue, &plr.minHashSig)
		} else {
			shared.AddHashToMinHeap(kv.Key, &plr.minHashSig)
		}
	}

	if plr.record != nil {
		for _, kv := range plr.record.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
	if plr.rl != nil && plr.rl.Resource != nil {
		for _, kv := range plr.rl.Resource.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
	if plr.sl != nil && plr.sl.Scope != nil {
		for _, kv := range plr.sl.Scope.Attributes {
			if kv != nil {
				hashKV(kv)
			}
		}
	}
}

// buildLogBlock constructs a single block from the given pending log records and returns
// the serialized payload together with all per-block statistics.
// Mirrors buildBlock for the log path.
func buildLogBlock(pending []pendingLogRecord) (builtBlock, error) {
	bb := newLogBlockBuilder(len(pending))
	for rowIdx := range pending {
		bb.addLogRecordFromProto(&pending[rowIdx], rowIdx)
	}
	// NOTE-040: substitute numeric range index entries for all-parseable string columns.
	// Prefer int64 over float64. Only substitutes string-typed colMinMax entries;
	// native int64 columns (colType != ColumnTypeString/RangeString) are left unchanged.
	for name, mm := range bb.colMinMax {
		if mm.colType != shared.ColumnTypeString && mm.colType != shared.ColumnTypeRangeString {
			continue
		}
		if nmm, ok := bb.colNumericMinMax[name]; ok && !bb.colNonNumeric[name] {
			bb.colMinMax[name] = nmm
		} else if fmm, ok := bb.colFloatMinMax[name]; ok && !bb.colNonFloat[name] {
			bb.colMinMax[name] = fmm
		}
	}
	payload, err := bb.finalize(shared.VersionBlockV14)
	if err != nil {
		return builtBlock{}, err
	}
	// No traceRows for log blocks; MinTraceID/MaxTraceID are zero ([16]byte zero value).
	return builtBlock{
		payload:     payload,
		spanCount:   bb.recordCount,
		minStart:    bb.minStart,
		maxStart:    bb.maxStart,
		colMinMax:   bb.colMinMax,
		colSketches: bb.colSketches,
		// traceRows is nil — log blocks have no trace ID index.
		// minTraceID/maxTraceID are the zero [16]byte value.
	}, nil
}

// finalize encodes all log columns and returns the serialized block bytes.
// Delegates to the shared finalize logic on blockBuilder by using its column map.
// Since logBlockBuilder mirrors blockBuilder's column infrastructure exactly,
// we build a minimal blockBuilder shell to reuse finalize().
func (b *logBlockBuilder) finalize(blockVersion uint8) ([]byte, error) {
	// Build a minimal blockBuilder to reuse the finalize() method.
	// We only need the columns map and spanCount fields for finalization.
	shell := &blockBuilder{
		columns:   b.columns,
		spanCount: b.recordCount,
	}
	return shell.finalize(blockVersion)
}

package writer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// writer_blocks.go contains functions for building and serializing block payloads.

// Bloom helpers live in bloom.go.

func buildTraceTable(traces []*traceEntry, encoder *zstd.Encoder) ([]byte, map[string]columnStatsBuilder, error) {
	if len(traces) == 0 {
		return nil, nil, nil
	}

	// Collect all unique trace column names across all traces
	allColumns := make(map[string]ColumnType)
	for _, trace := range traces {
		for name, cb := range trace.resourceAttrs {
			allColumns[name] = cb.typ
		}
	}

	// Sort column names for deterministic output
	names := make([]string, 0, len(allColumns))
	for name := range allColumns {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build column data for each trace column
	// Each column has len(traces) rows, with NULL for traces that don't have that attribute
	columnData := make(map[string][]byte, len(names))
	columnStats := make(map[string]columnStatsBuilder, len(names))

	for _, name := range names {
		typ := allColumns[name]
		// Create a temporary column builder with len(traces) rows
		cb := newColumnBuilder(name, typ, len(traces))

		// Populate column data: for each trace, set the value if it exists
		for traceIdx, trace := range traces {
			if traceCB, ok := trace.resourceAttrs[name]; ok {
				// Trace has this attribute - copy the value (trace columns have 1 row at index 0)
				if err := copyColumnValue(cb, traceIdx, traceCB, 0); err != nil {
					return nil, nil, fmt.Errorf("failed to copy trace column %s: %w", name, err)
				}
			}
			// If trace doesn't have this attribute, cb already has NULL at traceIdx
		}

		// Build column data with per-Writer encoder
		data, stats, err := cb.buildData(len(traces), encoder)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build trace column %s: %w", name, err)
		}
		columnData[name] = data
		columnStats[name] = stats
	}

	// Serialize trace table:
	// Format: traceCount(4) + columnCount(4) + [name(2+N) + type(1) + dataLen(4) + data]...
	var buf bytes.Buffer

	// Trace count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(traces))); err != nil { //nolint:gosec
		return nil, nil, err
	}

	// Column count
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(names))); err != nil { //nolint:gosec
		return nil, nil, err
	}

	// For each column: name + type + data
	for _, name := range names {
		typ := allColumns[name]
		data := columnData[name]

		// Name length + name
		if err := binary.Write(&buf, binary.LittleEndian, uint16(len(name))); err != nil { //nolint:gosec
			return nil, nil, err
		}
		if _, err := buf.Write([]byte(name)); err != nil {
			return nil, nil, err
		}

		// Type
		if err := buf.WriteByte(byte(typ)); err != nil {
			return nil, nil, err
		}

		// Data length + data
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(data))); err != nil { //nolint:gosec
			return nil, nil, err
		}
		if _, err := buf.Write(data); err != nil {
			return nil, nil, err
		}
	}

	return buf.Bytes(), columnStats, nil
}

// copyColumnValue copies a value from one column to another
func copyColumnValue(dst *columnBuilder, dstIdx int, src *columnBuilder, srcIdx int) error {
	if dst.typ != src.typ {
		return fmt.Errorf("type mismatch: dst=%d src=%d", dst.typ, src.typ)
	}

	// Check if src has a value at srcIdx
	if !isBitSet(src.present, srcIdx) {
		// NULL value - dst already has NULL at dstIdx
		return nil
	}

	switch dst.typ {
	case ColumnTypeString:
		if srcIdx < len(src.stringIndexes) {
			dictIdx := src.stringIndexes[srcIdx]
			if int(dictIdx) < len(src.stringDictVals) {
				return dst.setString(dstIdx, src.stringDictVals[dictIdx])
			}
		}
	case ColumnTypeInt64:
		if srcIdx < len(src.intIndexes) {
			dictIdx := src.intIndexes[srcIdx]
			if int(dictIdx) < len(src.intDictVals) {
				return dst.setInt64(dstIdx, src.intDictVals[dictIdx])
			}
		}
	case ColumnTypeUint64:
		if srcIdx < len(src.uintValues) {
			return dst.setUint64(dstIdx, src.uintValues[srcIdx])
		}
	case ColumnTypeBool:
		if srcIdx < len(src.boolIndexes) {
			dictIdx := src.boolIndexes[srcIdx]
			if int(dictIdx) < len(src.boolDictVals) {
				return dst.setBool(dstIdx, src.boolDictVals[dictIdx] != 0)
			}
		}
	case ColumnTypeFloat64:
		if srcIdx < len(src.floatIndexes) {
			dictIdx := src.floatIndexes[srcIdx]
			if int(dictIdx) < len(src.floatDictVals) {
				return dst.setFloat64(dstIdx, src.floatDictVals[dictIdx])
			}
		}
	case ColumnTypeBytes:
		if srcIdx < len(src.bytesIndexes) {
			dictIdx := src.bytesIndexes[srcIdx]
			if int(dictIdx) < len(src.bytesDictVals) {
				return dst.setBytes(dstIdx, src.bytesDictVals[dictIdx])
			}
		}
	default:
		return fmt.Errorf("unsupported column type: %d", dst.typ)
	}
	return nil
}

// writeColumnIndex serializes the per-column offset data for all blocks
func writeColumnIndex(buf *bytes.Buffer, blockIndex []blockIndexEntry) error {
	for _, entry := range blockIndex {
		// Write column count for this block
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(entry.ColumnIndex))); err != nil { //nolint:gosec
			return err
		}

		// Write each column entry
		for _, col := range entry.ColumnIndex {
			// Write column name length
			if err := binary.Write(buf, binary.LittleEndian, uint16(len(col.Name))); err != nil { //nolint:gosec
				return err
			}

			// Write column name
			if _, err := buf.Write([]byte(col.Name)); err != nil {
				return err
			}

			// Write offset (relative to block start)
			if err := binary.Write(buf, binary.LittleEndian, col.Offset); err != nil {
				return err
			}

			// Write length
			if err := binary.Write(buf, binary.LittleEndian, col.Length); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeBlockHeader writes the block header, span column metadata, and trace column metadata.
// Returns the column index for selective I/O.
func writeBlockHeader(
	buf *bytes.Buffer,
	b *blockBuilder,
	totalColumnCount int,
	traceTableData []byte,
	names []string,
	traceColumnNames []string,
	columnData map[string][]byte,
	columnStatsBytes map[string][]byte,
	statsStart int,
	dataStart int,
	version uint8,
) ([]internalColumnIndexEntry, error) {
	// Block header (v9 block layout)
	if err := binary.Write(buf, binary.LittleEndian, magicNumber); err != nil {
		return nil, fmt.Errorf("write block magic: %w", err)
	}
	if err := buf.WriteByte(version); err != nil {
		return nil, fmt.Errorf("write block version: %w", err)
	}
	_, _ = buf.Write([]byte{0, 0, 0})                                                   // reserved
	if err := binary.Write(buf, binary.LittleEndian, uint32(b.spanCount)); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write span count: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(totalColumnCount)); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write column count: %w", err)
	}
	// Trace count and trace table length
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(b.traces))); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write trace count: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(traceTableData))); err != nil { //nolint:gosec
		return nil, fmt.Errorf("write trace table length: %w", err)
	}

	// Metadata: first write span columns, then trace columns
	statsOffset := statsStart
	dataOffset := dataStart

	// Track per-column offsets for selective I/O
	columnIndex := make([]internalColumnIndexEntry, 0, len(names))

	// Write span column metadata
	for _, name := range names {
		cb := b.columns[name]
		if cb == nil {
			return nil, fmt.Errorf("nil column builder for %q", name)
		}
		data := columnData[name]
		statsBytes := columnStatsBytes[name]

		if err := binary.Write(buf, binary.LittleEndian, uint16(len(name))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write column %q name length: %w", name, err)
		}
		if _, err := buf.Write([]byte(name)); err != nil {
			return nil, fmt.Errorf("write column %q name: %w", name, err)
		}
		if err := buf.WriteByte(byte(cb.typ)); err != nil {
			return nil, fmt.Errorf("write column %q type: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(dataOffset)); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write column %q data offset: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(data))); err != nil {
			return nil, fmt.Errorf("write column %q data length: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(statsOffset)); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write column %q stats offset: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(statsBytes))); err != nil {
			return nil, fmt.Errorf("write column %q stats length: %w", name, err)
		}

		// Record column offset for selective I/O
		columnIndex = append(columnIndex, internalColumnIndexEntry{
			Name:   name,
			Offset: uint32(dataOffset), //nolint:gosec
			Length: uint32(len(data)),  //nolint:gosec
		})

		dataOffset += len(data)
		statsOffset += len(statsBytes)
	}

	// Write trace column metadata (dataLen=0 signals trace-level, dataOffset unused)
	for _, name := range traceColumnNames {
		statsBytes := columnStatsBytes[name]

		// Get type from first trace that has this column
		var typ ColumnType
		for _, trace := range b.traces {
			if cb, ok := trace.resourceAttrs[name]; ok {
				typ = cb.typ
				break
			}
		}

		if err := binary.Write(buf, binary.LittleEndian, uint16(len(name))); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write trace column %q name length: %w", name, err)
		}
		if _, err := buf.Write([]byte(name)); err != nil {
			return nil, fmt.Errorf("write trace column %q name: %w", name, err)
		}
		if err := buf.WriteByte(byte(typ)); err != nil {
			return nil, fmt.Errorf("write trace column %q type: %w", name, err)
		}
		// dataOffset=0, dataLen=0 signals this is a trace-level column
		if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
			return nil, fmt.Errorf("write trace column %q data offset: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(0)); err != nil {
			return nil, fmt.Errorf("write trace column %q data length: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(statsOffset)); err != nil { //nolint:gosec
			return nil, fmt.Errorf("write trace column %q stats offset: %w", name, err)
		}
		if err := binary.Write(buf, binary.LittleEndian, uint64(len(statsBytes))); err != nil {
			return nil, fmt.Errorf("write trace column %q stats length: %w", name, err)
		}
		statsOffset += len(statsBytes)
	}

	return columnIndex, nil
}

// buildBlockPayload serializes a block to bytes. The encoder is passed through to column builders
// for dictionary compression, using a per-Writer encoder instead of sync.Pool to prevent memory accumulation.
func buildBlockPayload(b *blockBuilder, version uint8, encoder *zstd.Encoder) ([]byte, blockIndexEntry, error) {
	// Add trace.index column to map spans to traces - only if we have traces
	if len(b.traces) > 0 {
		traceIndexCol := newColumnBuilder("trace.index", ColumnTypeUint64, b.spanCount)
		for spanIdx, traceIdx := range b.spanToTrace {
			if err := traceIndexCol.setUint64(spanIdx, uint64(traceIdx)); err != nil { //nolint:gosec
				return nil, blockIndexEntry{}, fmt.Errorf("build trace index column: %w", err)
			}
		}
		b.columns["trace.index"] = traceIndexCol
	}

	// Build span-level column data
	names := make([]string, 0, len(b.columns))
	for name := range b.columns {
		names = append(names, name)
	}
	sort.Strings(names)

	columnData := make(map[string][]byte, len(names))
	columnStats := make(map[string]columnStatsBuilder, len(names))
	columnStatsBytes := make(map[string][]byte, len(names))
	totalStatsLen := 0
	for _, name := range names {
		col := b.columns[name]
		if col == nil {
			return nil, blockIndexEntry{}, fmt.Errorf("nil column builder for %q", name)
		}
		data, stats, err := col.buildData(b.spanCount, encoder)
		if err != nil {
			return nil, blockIndexEntry{}, fmt.Errorf("build column %q data: %w", name, err)
		}
		columnData[name] = data
		columnStats[name] = stats
		encodedStats := encodeColumnStats(col.typ, stats)
		totalStatsLen += len(encodedStats)
		columnStatsBytes[name] = encodedStats
	}

	// Build trace table and get trace-level column stats with per-Writer encoder
	traceTableData, traceTableStats, err := buildTraceTable(b.traces, encoder)
	if err != nil {
		return nil, blockIndexEntry{}, fmt.Errorf("build trace table: %w", err)
	}

	// Add trace-level column stats to metadata (stats only, no data - dataLen=0 signals trace-level)
	traceColumnNames := make([]string, 0, len(traceTableStats))
	for name := range traceTableStats {
		traceColumnNames = append(traceColumnNames, name)
	}
	sort.Strings(traceColumnNames)

	for _, name := range traceColumnNames {
		stats := traceTableStats[name]
		// Get type from first trace that has this column
		var typ ColumnType
		for _, trace := range b.traces {
			if cb, ok := trace.resourceAttrs[name]; ok {
				typ = cb.typ
				break
			}
		}
		encodedStats := encodeColumnStats(typ, stats)
		totalStatsLen += len(encodedStats)
		columnStatsBytes[name] = encodedStats
	}

	// Total column count = span columns + trace columns
	totalColumnCount := len(names) + len(traceColumnNames)

	// Metadata length: all columns (span + trace)
	metaLen := 0
	for _, name := range names {
		metaLen += 2 + len(name) + 1 + 8 + 8 + 8 + 8 // nameLen + name + type + data offset/len + stats offset/len
	}
	for _, name := range traceColumnNames {
		metaLen += 2 + len(name) + 1 + 8 + 8 + 8 + 8 // same for trace columns
	}

	// Block header is 24 bytes
	headerLen := 24
	statsStart := headerLen + metaLen
	dataStart := statsStart + totalStatsLen

	var buf bytes.Buffer

	columnIndex, err := writeBlockHeader(
		&buf,
		b,
		totalColumnCount,
		traceTableData,
		names,
		traceColumnNames,
		columnData,
		columnStatsBytes,
		statsStart,
		dataStart,
		version,
	)
	if err != nil {
		return nil, blockIndexEntry{}, fmt.Errorf("write block header: %w", err)
	}

	// Column stats: span columns first, then trace columns
	for _, name := range names {
		if _, err := buf.Write(columnStatsBytes[name]); err != nil {
			return nil, blockIndexEntry{}, fmt.Errorf("write column %q stats: %w", name, err)
		}
	}
	for _, name := range traceColumnNames {
		if _, err := buf.Write(columnStatsBytes[name]); err != nil {
			return nil, blockIndexEntry{}, fmt.Errorf("write trace column %q stats: %w", name, err)
		}
	}

	// Span column data
	for _, name := range names {
		data := columnData[name]
		if _, err := buf.Write(data); err != nil {
			return nil, blockIndexEntry{}, fmt.Errorf("write column %q data: %w", name, err)
		}
	}

	// Trace table data
	if len(traceTableData) > 0 {
		if _, err := buf.Write(traceTableData); err != nil {
			return nil, blockIndexEntry{}, fmt.Errorf("write trace table: %w", err)
		}
	}

	meta := blockIndexEntry{
		SpanCount:       uint32(b.spanCount), //nolint:gosec
		MinStart:        b.minStart,
		MaxStart:        b.maxStart,
		ColumnNameBloom: b.columnBloom,
		ColumnIndex:     columnIndex,  // Per-column offsets for selective I/O
		ValueStats:      b.valueStats, // v10: Per-attribute value statistics
		Kind:            blockEntryKindLeaf,
	}
	meta.MinTraceID = b.minTrace
	meta.MaxTraceID = b.maxTrace

	return buf.Bytes(), meta, nil
}

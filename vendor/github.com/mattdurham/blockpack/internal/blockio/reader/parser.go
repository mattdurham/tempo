package reader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/mattdurham/blockpack/internal/arena"
	"github.com/mattdurham/blockpack/internal/blockio/shared"
)

func parseBlockColumnsReuse(
	data []byte,
	want map[string]struct{},
	reusable *Block,
	queryArena *arena.Arena,
) (*Block, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("data too small for block header")
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magicNumber {
		return nil, fmt.Errorf("invalid magic in block")
	}

	version := data[4]
	if version != versionV10 && version != versionV11 {
		return nil, fmt.Errorf("unsupported block version %d (only v10/v11 supported)", version)
	}

	if len(data) < 24 {
		return nil, fmt.Errorf("data too small for block header")
	}

	spanCountRaw := binary.LittleEndian.Uint32(data[8:12])
	spanCount, err := shared.SafeUint32ToInt(spanCountRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid span count: %w", err)
	}
	if err := shared.ValidateAllocationSize(spanCount, shared.MaxBlockSpans, "span count"); err != nil { //nolint:govet
		return nil, err
	}

	columnCountRaw := binary.LittleEndian.Uint32(data[12:16])
	columnCount, err := shared.SafeUint32ToInt(columnCountRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid column count: %w", err)
	}
	if err := shared.ValidateAllocationSize(columnCount, shared.MaxAllocColumns, "column count"); err != nil { //nolint:govet
		return nil, err
	}

	traceCountRaw := binary.LittleEndian.Uint32(data[16:20])
	traceCount, err := shared.SafeUint32ToInt(traceCountRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid trace count: %w", err)
	}
	if err := shared.ValidateAllocationSize(traceCount, shared.MaxAllocTraces, "trace count"); err != nil { //nolint:govet
		return nil, err
	}

	traceTableLenRaw := binary.LittleEndian.Uint32(data[20:24])
	traceTableLen, err := shared.SafeUint32ToInt(traceTableLenRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid trace table length: %w", err)
	}

	offset := 24
	metas := make([]columnMeta, 0, columnCount)
	for i := 0; i < columnCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("truncated column name length at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen+1+32 > len(data) {
			return nil, fmt.Errorf("truncated metadata at column %d", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typ := ColumnType(data[offset])
		offset++
		// Read column metadata: data location (offset + length) and stats location
		// This allows columns to be stored non-contiguously in the file for better compression
		dataOffset := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		dataLen := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		statsOffset := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		statsLen := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		// Skip columns not in want set (if want is specified)
		if want != nil {
			if _, ok := want[name]; !ok {
				continue
			}
		}

		dataOffsetInt, err := shared.SafeUint64ToInt(dataOffset) //nolint:govet
		if err != nil {
			return nil, fmt.Errorf("column %s: invalid data offset: %w", name, err)
		}
		dataLenInt, err := shared.SafeUint64ToInt(dataLen)
		if err != nil {
			return nil, fmt.Errorf("column %s: invalid data length: %w", name, err)
		}
		statsOffsetInt, err := shared.SafeUint64ToInt(statsOffset)
		if err != nil {
			return nil, fmt.Errorf("column %s: invalid stats offset: %w", name, err)
		}
		statsLenInt, err := shared.SafeUint64ToInt(statsLen)
		if err != nil {
			return nil, fmt.Errorf("column %s: invalid stats length: %w", name, err)
		}

		metas = append(metas, columnMeta{
			name:        name,
			typ:         typ,
			dataOffset:  dataOffsetInt,
			dataLen:     dataLenInt,
			statsOffset: statsOffsetInt,
			statsLen:    statsLenInt,
		})
	}

	// Reuse Block and columns map if provided, otherwise allocate new
	var block *Block
	var columns map[string]*Column
	var existingColumns map[string]*Column

	if reusable != nil {
		block = reusable
		columns = block.columns
		// Initialize columns map if nil (handles zero-value Block)
		if columns == nil {
			columns = make(map[string]*Column, len(metas))
			block.columns = columns
		} else {
			// Save existing columns for reuse, then clear map
			existingColumns = make(map[string]*Column, len(columns))
			for k, v := range columns {
				existingColumns[k] = v
			}
			// Clear map entries (but keep Column objects in existingColumns for reuse)
			for k := range columns {
				delete(columns, k)
			}
		}
	} else {
		block = &Block{}
		columns = make(map[string]*Column, len(metas))
		block.columns = columns
	}

	block.spanCount = spanCount

	// Separate span-level and trace-level columns
	spanMetas := make([]columnMeta, 0, len(metas))
	traceMetas := make([]columnMeta, 0)
	for _, meta := range metas {
		// Columns with dataLen=0 are trace-level (stats only in metadata)
		if meta.dataLen == 0 {
			traceMetas = append(traceMetas, meta)
		} else {
			spanMetas = append(spanMetas, meta)
		}
	}

	// Parse span-level columns
	maxDataEnd, err := parseSpanColumns(spanMetas, data, spanCount, existingColumns, queryArena, columns)
	if err != nil {
		return nil, fmt.Errorf("parse span columns: %w", err)
	}
	// Parse trace table and expand trace-level columns
	if err := parseTraceColumns(traceMetas, data, maxDataEnd, traceTableLen, traceCount, spanCount, columns); err != nil {
		return nil, fmt.Errorf("parse trace columns: %w", err)
	}

	return block, nil
}

func parseBlockColumnStats(data []byte, want map[string]struct{}) (map[string]ColumnStatsWithType, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("data too small for block header")
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magicNumber {
		return nil, fmt.Errorf("invalid magic in block")
	}
	if data[4] != versionV10 && data[4] != versionV11 {
		return nil, fmt.Errorf("unsupported block version %d (only v10/v11 supported)", data[4])
	}

	columnCount := int(binary.LittleEndian.Uint32(data[12:16]))
	offset := 24
	out := make(map[string]ColumnStatsWithType, len(want))

	for i := 0; i < columnCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("truncated column name length at column %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2
		if offset+nameLen+1+32 > len(data) {
			return nil, fmt.Errorf("truncated metadata at column %d", i)
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen
		typ := ColumnType(data[offset])
		offset++
		offset += 8                                                             // dataOffset
		offset += 8                                                             // dataLen
		statsOffset := int(binary.LittleEndian.Uint64(data[offset : offset+8])) //nolint:gosec
		offset += 8
		statsLen := int(binary.LittleEndian.Uint64(data[offset : offset+8])) //nolint:gosec
		offset += 8

		if _, ok := want[name]; !ok {
			continue
		}
		statsEnd := statsOffset + statsLen
		if statsOffset < 0 || statsEnd > len(data) {
			return nil, fmt.Errorf("column %s stats out of bounds", name)
		}
		statsData := data[statsOffset:statsEnd]
		stats, err := parseColumnStats(typ, statsData)
		if err != nil {
			return nil, fmt.Errorf("column %s stats: %w", name, err)
		}
		out[name] = ColumnStatsWithType{Type: typ, Stats: stats}
	}

	return out, nil
}

type columnMeta struct {
	name        string
	typ         ColumnType
	dataOffset  int
	dataLen     int
	statsOffset int
	statsLen    int
}

// parseTraceTable deserializes the trace table from blocks
func parseTraceTable(data []byte, expectedTraces int) (map[string]*Column, error) {
	rd := &sliceReader{data: data}

	traceCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read trace count: %w", err)
	}
	if int(traceCount) != expectedTraces {
		return nil, fmt.Errorf("trace count mismatch: got %d, expected %d", traceCount, expectedTraces)
	}

	columnCount, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read column count: %w", err)
	}

	traceColumns := make(map[string]*Column, columnCount)

	for i := 0; i < int(columnCount); i++ {
		nameLen, err := rd.readUint16()
		if err != nil {
			return nil, fmt.Errorf("failed to read name length for column %d: %w", i, err)
		}

		// Validate name length to prevent large allocations
		if err := shared.ValidateNameLen(int(nameLen)); err != nil { //nolint:govet
			return nil, fmt.Errorf("invalid name length for column %d: %w", i, err)
		}

		nameBytes, err := rd.readBytes(int(nameLen))
		if err != nil {
			return nil, fmt.Errorf("failed to read name for column %d: %w", i, err)
		}
		name := string(nameBytes)

		typ, err := rd.readUint8()
		if err != nil {
			return nil, fmt.Errorf("failed to read type for column %s: %w", name, err)
		}

		// Validate column type
		if err := shared.ValidateColumnType(typ); err != nil { //nolint:govet
			return nil, fmt.Errorf("invalid column type for %s: %w", name, err)
		}

		dataLen, err := rd.readUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read data length for column %s: %w", name, err)
		}

		// Validate data length to prevent large allocations
		// Use block size limit as upper bound for column data
		if err := shared.ValidateBlockSize(uint64(dataLen)); err != nil { //nolint:govet
			return nil, fmt.Errorf("invalid data length for column %s: %w", name, err)
		}

		colData, err := rd.readBytes(int(dataLen))
		if err != nil {
			return nil, fmt.Errorf("failed to read data for column %s: %w", name, err)
		}

		// Parse column (trace table columns have traceCount rows, no stats)
		// Use nil arena since trace table is metadata that lives beyond query scope
		col, err := parseColumnReuse(name, ColumnType(typ), int(traceCount), colData, nil, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to parse column %s: %w", name, err)
		}

		traceColumns[name] = col
	}

	return traceColumns, nil
}

// expandTraceColumn expands a trace-level column to span-level using the trace.index column
// expandTraceColumnString expands string trace-level column to span-level.
func expandTraceColumnString(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.StringDict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.StringValue(int(traceIdx)); ok { //nolint:gosec
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx) //nolint:gosec //nolint:gosec
					shared.SetBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetStringData(dict, values, present)
}

// expandTraceColumnInt64 expands int64 trace-level column to span-level.
func expandTraceColumnInt64(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.Int64Dict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.Int64Value(int(traceIdx)); ok { //nolint:gosec
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx) //nolint:gosec //nolint:gosec
					shared.SetBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetInt64Data(dict, values, present)
}

// expandTraceColumnUint64 expands uint64 trace-level column to span-level.
func expandTraceColumnUint64(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.Uint64Dict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.Uint64Value(int(traceIdx)); ok { //nolint:gosec
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx) //nolint:gosec
					shared.SetBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetUint64Data(dict, values, present)
}

// expandTraceColumnBool expands bool trace-level column to span-level.
func expandTraceColumnBool(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.BoolDict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.BoolValue(int(traceIdx)); ok { //nolint:gosec
			for dictIdx, dictVal := range dict {
				if (dictVal == 1) == val {
					values[spanIdx] = uint32(dictIdx) //nolint:gosec
					shared.SetBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetBoolData(dict, values, present)
}

// expandTraceColumnFloat64 expands float64 trace-level column to span-level.
func expandTraceColumnFloat64(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	dict, _ := traceCol.Float64Dict()
	values := make([]uint32, spanCount)
	for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
		traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
		if !ok {
			continue
		}
		if val, ok := traceCol.Float64Value(int(traceIdx)); ok { //nolint:gosec
			for dictIdx, dictVal := range dict {
				if dictVal == val {
					values[spanIdx] = uint32(dictIdx) //nolint:gosec
					shared.SetBit(present, spanIdx)
					break
				}
			}
		}
	}
	spanCol.SetFloat64Data(dict, values, present)
}

// expandTraceColumnBytes expands bytes trace-level column to span-level.
func expandTraceColumnBytes(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int, present []byte) {
	if inline := traceCol.BytesInline(); inline != nil {
		values := make([][]byte, spanCount)
		for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
			traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
			if !ok {
				continue
			}
			if val, ok := traceCol.BytesValueView(int(traceIdx)); ok { //nolint:gosec
				values[spanIdx] = val
				shared.SetBit(present, spanIdx)
			}
		}
		spanCol.SetBytesInlineData(values, present)
	} else {
		dict, _ := traceCol.BytesDict()
		values := make([]uint32, spanCount)
		for spanIdx := 0; spanIdx < spanCount; spanIdx++ {
			traceIdx, ok := traceIndexCol.Uint64Value(spanIdx)
			if !ok {
				continue
			}
			if val, ok := traceCol.BytesValueView(int(traceIdx)); ok { //nolint:gosec
				for dictIdx, dictVal := range dict {
					if bytes.Equal(dictVal, val) {
						values[spanIdx] = uint32(dictIdx) //nolint:gosec
						shared.SetBit(present, spanIdx)
						break
					}
				}
			}
		}
		spanCol.SetBytesDictData(dict, values, present)
	}
}

func expandTraceColumn(spanCol *Column, traceCol *Column, traceIndexCol *Column, spanCount int) error {
	present := make([]byte, shared.BitsLen(spanCount))
	// NOTE: Don't copy stats from traceCol - caller has already set stats from metadata.

	switch spanCol.Type {
	case ColumnTypeString:
		expandTraceColumnString(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeInt64:
		expandTraceColumnInt64(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeUint64:
		expandTraceColumnUint64(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeBool:
		expandTraceColumnBool(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeFloat64:
		expandTraceColumnFloat64(spanCol, traceCol, traceIndexCol, spanCount, present)
	case ColumnTypeBytes:
		expandTraceColumnBytes(spanCol, traceCol, traceIndexCol, spanCount, present)
	default:
		return fmt.Errorf("unsupported column type: %d", spanCol.Type)
	}

	return nil
}

func parseColumnStats(typ ColumnType, data []byte) (ColumnStats, error) {
	if len(data) < 1 {
		return ColumnStats{}, fmt.Errorf("column stats truncated")
	}
	hasValues := data[0] == 1

	stats := ColumnStats{
		HasValues: hasValues,
	}
	if !hasValues {
		return stats, nil
	}

	switch typ {
	case ColumnTypeString:
		min, next, err := readSizedBytes(data, 1) //nolint:revive
		if err != nil {
			return ColumnStats{}, err
		}
		max, _, err := readSizedBytes(data, next) //nolint:revive
		if err != nil {
			return ColumnStats{}, err
		}
		stats.StringMin = string(min)
		stats.StringMax = string(max)
	case ColumnTypeBytes:
		min, next, err := readSizedBytes(data, 1) //nolint:revive
		if err != nil {
			return ColumnStats{}, err
		}
		max, _, err := readSizedBytes(data, next) //nolint:revive
		if err != nil {
			return ColumnStats{}, err
		}
		stats.BytesMin = min
		stats.BytesMax = max
	case ColumnTypeInt64:
		offset := 1
		if offset+16 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for int64")
		}
		stats.IntMin = int64(binary.LittleEndian.Uint64(data[offset:]))   //nolint:gosec
		stats.IntMax = int64(binary.LittleEndian.Uint64(data[offset+8:])) //nolint:gosec
	case ColumnTypeUint64:
		offset := 1
		if offset+16 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for uint64")
		}
		stats.UintMin = binary.LittleEndian.Uint64(data[offset:])
		stats.UintMax = binary.LittleEndian.Uint64(data[offset+8:])
	case ColumnTypeFloat64:
		offset := 1
		if offset+16 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for float64")
		}
		stats.FloatMin = math.Float64frombits(binary.LittleEndian.Uint64(data[offset:]))
		stats.FloatMax = math.Float64frombits(binary.LittleEndian.Uint64(data[offset+8:]))
	case ColumnTypeBool:
		offset := 1
		if offset+2 > len(data) {
			return ColumnStats{}, fmt.Errorf("column stats truncated for bool")
		}
		stats.BoolMin = data[offset] != 0
		stats.BoolMax = data[offset+1] != 0
	}

	return stats, nil
}

func readSizedBytes(data []byte, offset int) ([]byte, int, error) {
	if offset+4 > len(data) {
		return nil, offset, fmt.Errorf("truncated sized bytes length")
	}
	lRaw := binary.LittleEndian.Uint32(data[offset:])
	l, err := shared.SafeUint32ToInt(lRaw)
	if err != nil {
		return nil, offset, fmt.Errorf("invalid string length: %w", err)
	}
	if err := shared.ValidateAllocationSize(l, shared.MaxAllocStringLen, "string value"); err != nil { //nolint:govet
		return nil, offset, err
	}
	offset += 4
	endOffset, err := shared.SafeAddInt(offset, l)
	if err != nil {
		return nil, offset, fmt.Errorf("string offset overflow: %w", err)
	}
	if endOffset > len(data) {
		return nil, offset, fmt.Errorf("truncated sized bytes value at offset %d", offset)
	}
	val := make([]byte, l)
	copy(val, data[offset:offset+l])
	return val, offset + l, nil
}

// parseTraceColumns parses trace-level columns from the trace table and expands them to span-level.
func parseTraceColumns(
	traceMetas []columnMeta,
	data []byte,
	maxDataEnd, traceTableLen, traceCount, spanCount int,
	columns map[string]*Column,
) error {
	if len(traceMetas) == 0 {
		return nil
	}

	// Find trace table data (after all span column data)
	if maxDataEnd+traceTableLen > len(data) {
		return fmt.Errorf("trace table out of bounds")
	}
	traceTableData := data[maxDataEnd : maxDataEnd+traceTableLen]

	// Parse trace table
	traceTable, err := parseTraceTable(traceTableData, traceCount)
	if err != nil {
		return fmt.Errorf("failed to parse trace table: %w", err)
	}

	// Get trace.index column to map spans to traces
	traceIndexCol, ok := columns["trace.index"]
	if !ok {
		return fmt.Errorf("block missing trace.index column")
	}

	// Expand trace-level columns to span-level using trace.index
	for _, meta := range traceMetas {
		traceCol, ok := traceTable[meta.name]
		if !ok {
			continue
		}
		// Load stats for trace-level column
		statsEnd := meta.statsOffset + meta.statsLen
		if meta.statsOffset < 0 || statsEnd > len(data) {
			return fmt.Errorf("trace column %s stats out of bounds", meta.name)
		}
		statsData := data[meta.statsOffset:statsEnd]
		stats, err := parseColumnStats(meta.typ, statsData)
		if err != nil {
			return fmt.Errorf("trace column %s stats: %w", meta.name, err)
		}

		spanCol := &Column{
			Name:  meta.name,
			Type:  meta.typ,
			Stats: stats,
		}
		if err := expandTraceColumn(spanCol, traceCol, traceIndexCol, spanCount); err != nil {
			return fmt.Errorf("failed to expand trace column %s: %w", meta.name, err)
		}
		columns[meta.name] = spanCol
	}
	return nil
}

// parseSpanColumns parses span-level columns from metadata and populates the columns map.
// Returns the maximum data end offset needed for trace table parsing.
func parseSpanColumns(
	spanMetas []columnMeta,
	data []byte,
	spanCount int,
	existingColumns map[string]*Column,
	queryArena *arena.Arena,
	columns map[string]*Column,
) (int, error) {
	maxDataEnd := 0
	for _, meta := range spanMetas {
		end := meta.dataOffset + meta.dataLen
		if meta.dataOffset < 0 || end > len(data) {
			return 0, fmt.Errorf("column %s data out of bounds", meta.name)
		}
		if end > maxDataEnd {
			maxDataEnd = end
		}
		statsEnd := meta.statsOffset + meta.statsLen
		if meta.statsOffset < 0 || statsEnd > len(data) {
			return 0, fmt.Errorf("column %s stats out of bounds", meta.name)
		}
		colData := data[meta.dataOffset:end]
		statsData := data[meta.statsOffset:statsEnd]

		// Try to reuse existing column allocation
		var reusableCol *Column
		if existingColumns != nil {
			reusableCol = existingColumns[meta.name]
		}

		col, err := parseColumnReuse(meta.name, meta.typ, spanCount, colData, statsData, reusableCol, queryArena)
		if err != nil {
			return 0, err
		}
		columns[meta.name] = col
	}
	return maxDataEnd, nil
}

// BOT: Move this to own file
// BOT: Can slice reader implement more..friendly names wrappers?
type sliceReader struct {
	data   []byte
	offset int
}

func (sr *sliceReader) readUint8() (uint8, error) {
	if sr.offset+1 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint8")
	}
	val := sr.data[sr.offset]
	sr.offset++
	return val, nil
}

func (sr *sliceReader) readBytes(n int) ([]byte, error) {
	if n < 0 || sr.offset+n > len(sr.data) {
		return nil, fmt.Errorf("truncated buffer")
	}
	out := sr.data[sr.offset : sr.offset+n]
	sr.offset += n
	return out, nil
}

func (sr *sliceReader) readUint32() (uint32, error) {
	if sr.offset+4 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint32")
	}
	val := binary.LittleEndian.Uint32(sr.data[sr.offset : sr.offset+4])
	sr.offset += 4
	return val, nil
}

func (sr *sliceReader) readInt32() (int32, error) {
	if sr.offset+4 > len(sr.data) {
		return 0, fmt.Errorf("truncated int32")
	}
	val := int32(binary.LittleEndian.Uint32(sr.data[sr.offset : sr.offset+4])) //nolint:gosec
	sr.offset += 4
	return val, nil
}

func (sr *sliceReader) readInt64() (int64, error) {
	if sr.offset+8 > len(sr.data) {
		return 0, fmt.Errorf("truncated int64")
	}
	val := int64(binary.LittleEndian.Uint64(sr.data[sr.offset : sr.offset+8])) //nolint:gosec
	sr.offset += 8
	return val, nil
}

func (sr *sliceReader) readUint64() (uint64, error) {
	if sr.offset+8 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint64")
	}
	val := binary.LittleEndian.Uint64(sr.data[sr.offset : sr.offset+8])
	sr.offset += 8
	return val, nil
}

func (sr *sliceReader) readFloat64() (float64, error) {
	if sr.offset+8 > len(sr.data) {
		return 0, fmt.Errorf("truncated float64")
	}
	bits := binary.LittleEndian.Uint64(sr.data[sr.offset : sr.offset+8])
	sr.offset += 8
	return math.Float64frombits(bits), nil
}

func (sr *sliceReader) readFixedWidth(width uint8) (uint32, error) {
	switch width {
	case 1:
		v, err := sr.readUint8()
		return uint32(v), err
	case 2:
		v, err := sr.readUint16()
		return uint32(v), err
	case 4:
		v, err := sr.readUint32()
		return v, err
	default:
		return 0, fmt.Errorf("unsupported width %d", width)
	}
}

func (sr *sliceReader) readUint16() (uint16, error) {
	if sr.offset+2 > len(sr.data) {
		return 0, fmt.Errorf("truncated uint16")
	}
	val := binary.LittleEndian.Uint16(sr.data[sr.offset : sr.offset+2])
	sr.offset += 2
	return val, nil
}

func (sr *sliceReader) remaining() int {
	return len(sr.data) - sr.offset
}

func readPresence(rd *sliceReader, rows int) ([]byte, error) {
	length, err := rd.readUint32()
	if err != nil {
		return nil, fmt.Errorf("read presence length: %w", err)
	}
	data, err := rd.readBytes(int(length))
	if err != nil {
		return nil, fmt.Errorf("read presence data: %w", err)
	}
	decoded, err := shared.DecodePresenceRLE(data, rows)
	if err != nil {
		return nil, fmt.Errorf("decode presence RLE: %w", err)
	}
	return decoded, nil
}

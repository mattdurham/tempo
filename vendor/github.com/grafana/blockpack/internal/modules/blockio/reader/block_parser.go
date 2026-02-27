package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// blockHeader holds the parsed block header fields.
type blockHeader struct {
	magic         uint32
	version       uint8
	spanCount     uint32
	columnCount   uint32
	traceCount    uint32
	traceTableLen uint32
}

// colMetaEntry holds one parsed column metadata entry.
type colMetaEntry struct {
	name        string
	colType     shared.ColumnType
	dataOffset  uint64
	dataLen     uint64
	statsOffset uint64
	statsLen    uint64
}

// parseBlockHeader parses the 24-byte block header from data.
func parseBlockHeader(data []byte) (blockHeader, error) {
	if len(data) < 24 {
		return blockHeader{}, fmt.Errorf("block header: need 24 bytes, have %d", len(data))
	}

	hdr := blockHeader{
		magic:         binary.LittleEndian.Uint32(data[0:]),
		version:       data[4],
		spanCount:     binary.LittleEndian.Uint32(data[8:]),
		columnCount:   binary.LittleEndian.Uint32(data[12:]),
		traceCount:    binary.LittleEndian.Uint32(data[16:]),
		traceTableLen: binary.LittleEndian.Uint32(data[20:]),
	}

	if hdr.magic != shared.MagicNumber {
		return blockHeader{}, fmt.Errorf("block header: bad magic 0x%08X", hdr.magic)
	}

	if hdr.version != shared.VersionV10 && hdr.version != shared.VersionV11 {
		return blockHeader{}, fmt.Errorf("block header: unsupported version %d", hdr.version)
	}

	return hdr, nil
}

// parseColumnMetadataArray parses colCount column metadata entries starting at offset.
// Returns entries and the new offset after the last entry.
func parseColumnMetadataArray(data []byte, offset int, colCount int) ([]colMetaEntry, int, error) {
	entries := make([]colMetaEntry, 0, colCount)
	pos := offset

	for i := range colCount {
		if pos+2 > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for name_len", i)
		}

		nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2

		if nameLen > shared.MaxNameLen {
			return nil, pos, fmt.Errorf("col_meta[%d]: name_len %d exceeds MaxNameLen", i, nameLen)
		}

		if pos+nameLen > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for name", i)
		}

		name := string(data[pos : pos+nameLen])
		pos += nameLen

		// col_type[1] + data_offset[8] + data_len[8] + stats_offset[8] + stats_len[8]
		if pos+33 > len(data) {
			return nil, pos, fmt.Errorf("col_meta[%d]: short for type+offsets", i)
		}

		colType := shared.ColumnType(data[pos])
		pos++

		dataOffset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		dataLen := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		statsOffset := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		statsLen := binary.LittleEndian.Uint64(data[pos:])
		pos += 8

		entries = append(entries, colMetaEntry{
			name:        name,
			colType:     colType,
			dataOffset:  dataOffset,
			dataLen:     dataLen,
			statsOffset: statsOffset,
			statsLen:    statsLen,
		})
	}

	return entries, pos, nil
}

// parseColumnStats parses the stats blob for one column.
func parseColumnStats(data []byte, typ shared.ColumnType) (shared.ColumnStats, error) {
	if len(data) < 1 {
		return shared.ColumnStats{}, fmt.Errorf("column_stats: empty data")
	}

	hasValues := data[0]
	if hasValues == 0 {
		return shared.ColumnStats{}, nil
	}

	stats := shared.ColumnStats{HasValues: true}
	pos := 1

	switch typ {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if pos+4 > len(data) {
			return stats, fmt.Errorf("column_stats(string): short for min_len")
		}

		minLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+minLen > len(data) {
			return stats, fmt.Errorf("column_stats(string): short for min_bytes")
		}

		stats.StringMin = string(data[pos : pos+minLen])
		pos += minLen

		if pos+4 > len(data) {
			return stats, fmt.Errorf("column_stats(string): short for max_len")
		}

		maxLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+maxLen > len(data) {
			return stats, fmt.Errorf("column_stats(string): short for max_bytes")
		}

		stats.StringMax = string(data[pos : pos+maxLen])

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if pos+4 > len(data) {
			return stats, fmt.Errorf("column_stats(bytes): short for min_len")
		}

		minLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+minLen > len(data) {
			return stats, fmt.Errorf("column_stats(bytes): short for min_bytes")
		}

		stats.BytesMin = make([]byte, minLen)
		copy(stats.BytesMin, data[pos:pos+minLen])
		pos += minLen

		if pos+4 > len(data) {
			return stats, fmt.Errorf("column_stats(bytes): short for max_len")
		}

		maxLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+maxLen > len(data) {
			return stats, fmt.Errorf("column_stats(bytes): short for max_bytes")
		}

		stats.BytesMax = make([]byte, maxLen)
		copy(stats.BytesMax, data[pos:pos+maxLen])

	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if pos+16 > len(data) {
			return stats, fmt.Errorf("column_stats(int64): short for min+max")
		}

		stats.IntMin = int64(binary.LittleEndian.Uint64(data[pos:])) //nolint:gosec // safe: reinterpreting serialized int64 bits
		pos += 8
		stats.IntMax = int64(binary.LittleEndian.Uint64(data[pos:])) //nolint:gosec // safe: reinterpreting serialized int64 bits

	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		if pos+16 > len(data) {
			return stats, fmt.Errorf("column_stats(uint64): short for min+max")
		}

		stats.UintMin = binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		stats.UintMax = binary.LittleEndian.Uint64(data[pos:])

	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		if pos+16 > len(data) {
			return stats, fmt.Errorf("column_stats(float64): short for min+max")
		}

		stats.FloatMin = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		stats.FloatMax = math.Float64frombits(binary.LittleEndian.Uint64(data[pos:]))

	case shared.ColumnTypeBool:
		if pos+2 > len(data) {
			return stats, fmt.Errorf("column_stats(bool): short for min+max")
		}

		stats.BoolMin = data[pos] != 0
		stats.BoolMax = data[pos+1] != 0

	default:
		// Unknown type — skip stats.
	}

	return stats, nil
}

// parseColumnStatsSection parses stats for all columns.
// metas contains the parsed column metadata entries (with absolute offsets into rawBytes).
func parseColumnStatsSection(data []byte, metas []colMetaEntry, baseOffset int) ([]shared.ColumnStats, error) {
	_ = baseOffset
	statsList := make([]shared.ColumnStats, len(metas))

	for i, m := range metas {
		if m.statsLen == 0 {
			continue
		}

		start := int(m.statsOffset)    //nolint:gosec // safe: statsOffset bounded by block size < MaxBlockSize
		end := start + int(m.statsLen) //nolint:gosec // safe: statsLen bounded by block size < MaxBlockSize
		if start < 0 || end > len(data) {
			return nil, fmt.Errorf(
				"col_stats[%d]: offset %d len %d out of range (block size %d)",
				i, m.statsOffset, m.statsLen, len(data),
			)
		}

		s, err := parseColumnStats(data[start:end], m.colType)
		if err != nil {
			return nil, fmt.Errorf("col_stats[%d] %q: %w", i, m.name, err)
		}

		statsList[i] = s
	}

	return statsList, nil
}

// parseBlockColumnsReuse decodes rawBytes into a Block.
// wantColumns: if non-nil, only decode columns in this set.
// prevBlock: if non-nil and same column set, reuse Column allocations.
func parseBlockColumnsReuse(
	rawBytes []byte,
	version uint8,
	wantColumns map[string]struct{},
	prevBlock *Block,
	meta shared.BlockMeta,
) (*Block, error) {
	hdr, err := parseBlockHeader(rawBytes)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: %w", err)
	}

	_ = version

	spanCount := int(hdr.spanCount)
	colCount := int(hdr.columnCount)

	metas, _, err := parseColumnMetadataArray(rawBytes, 24, colCount)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: column metadata: %w", err)
	}

	stats, err := parseColumnStatsSection(rawBytes, metas, 0)
	if err != nil {
		return nil, fmt.Errorf("parseBlock: column stats: %w", err)
	}

	var columns map[string]*Column
	if prevBlock != nil && prevBlock.columns != nil {
		columns = prevBlock.columns
		// Clear values from existing columns; we will re-populate.
		for _, col := range columns {
			resetColumn(col)
		}
	} else {
		columns = make(map[string]*Column, colCount)
	}

	for i, m := range metas {
		if wantColumns != nil {
			if _, ok := wantColumns[m.name]; !ok {
				continue
			}
		}

		// Trace-level columns (dataLen == 0) are skipped here.
		if m.dataLen == 0 {
			continue
		}

		start := int(m.dataOffset)    //nolint:gosec // safe: dataOffset bounded by block size < MaxBlockSize
		end := start + int(m.dataLen) //nolint:gosec // safe: dataLen bounded by block size < MaxBlockSize
		if start < 0 || end > len(rawBytes) {
			return nil, fmt.Errorf(
				"parseBlock: col %q data offset %d len %d out of range (block %d bytes)",
				m.name, m.dataOffset, m.dataLen, len(rawBytes),
			)
		}

		colData := rawBytes[start:end]

		var col *Column
		if existing, ok := columns[m.name]; ok {
			col = existing
		} else {
			col = &Column{}
		}

		col.Name = m.name
		col.Type = m.colType
		col.Stats = stats[i]

		decoded, err := readColumnEncoding(colData, spanCount, m.colType)
		if err != nil {
			return nil, fmt.Errorf("parseBlock: col %q: %w", m.name, err)
		}

		// Copy decoded fields into col (preserving pointer if reusing).
		col.StringDict = decoded.StringDict
		col.StringIdx = decoded.StringIdx
		col.Int64Dict = decoded.Int64Dict
		col.Int64Idx = decoded.Int64Idx
		col.Uint64Dict = decoded.Uint64Dict
		col.Uint64Idx = decoded.Uint64Idx
		col.Float64Dict = decoded.Float64Dict
		col.Float64Idx = decoded.Float64Idx
		col.BoolDict = decoded.BoolDict
		col.BoolIdx = decoded.BoolIdx
		col.BytesDict = decoded.BytesDict
		col.BytesIdx = decoded.BytesIdx
		col.BytesInline = decoded.BytesInline
		col.Present = decoded.Present
		col.SpanCount = decoded.SpanCount

		columns[m.name] = col
	}

	blk := &Block{
		spanCount: spanCount,
		columns:   columns,
		meta:      meta,
	}

	return blk, nil
}

// resetColumn zeroes a Column's value fields while retaining the allocation.
func resetColumn(col *Column) {
	col.StringDict = col.StringDict[:0]
	col.StringIdx = col.StringIdx[:0]
	col.Int64Dict = col.Int64Dict[:0]
	col.Int64Idx = col.Int64Idx[:0]
	col.Uint64Dict = col.Uint64Dict[:0]
	col.Uint64Idx = col.Uint64Idx[:0]
	col.Float64Dict = col.Float64Dict[:0]
	col.Float64Idx = col.Float64Idx[:0]
	col.BoolDict = col.BoolDict[:0]
	col.BoolIdx = col.BoolIdx[:0]
	col.BytesDict = col.BytesDict[:0]
	col.BytesIdx = col.BytesIdx[:0]
	col.BytesInline = nil
	col.Present = nil
	col.Stats = shared.ColumnStats{}
}

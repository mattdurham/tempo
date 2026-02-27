package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// rangeEntry is one range entry in the range column index (SPECS §5.2.1).
// lower is the encoded lower boundary key (SPECS §5.2.1).
type rangeEntry struct {
	lower    string
	blockIDs []uint32
}

// parsedRangeIndex holds the parsed result for one column's range index.
// entries is sorted in ascending value order by lower boundary (SPECS §5.2.1).
type parsedRangeIndex struct {
	entries []rangeEntry
	colType shared.ColumnType
}

// ensureRangeColumnParsed parses the range index for colName if not already done.
// Caches result in r.rangeParsed.
func (r *Reader) ensureRangeColumnParsed(colName string) error {
	if _, ok := r.rangeParsed[colName]; ok {
		return nil
	}

	meta, ok := r.rangeOffsets[colName]
	if !ok {
		return fmt.Errorf("range index: column %q not found", colName)
	}

	end := meta.offset + meta.length
	if end > len(r.metadataBytes) {
		return fmt.Errorf(
			"range index: column %q slice [%d:%d] out of range (metadata %d bytes)",
			colName, meta.offset, end, len(r.metadataBytes),
		)
	}

	entryData := r.metadataBytes[meta.offset:end]
	_, idx, _, err := parseRangeColumnEntry(entryData, 0)
	if err != nil {
		return fmt.Errorf("range index: column %q: %w", colName, err)
	}

	if r.rangeParsed == nil {
		r.rangeParsed = make(map[string]parsedRangeIndex)
	}

	r.rangeParsed[colName] = idx
	return nil
}

// parseRangeColumnEntry parses one RangeColumnEntry from data[pos:].
// Wire format (SPECS §5.2):
//
//	name_len[2] + name + col_type[1] + bucket_metadata + value_count[4] + [value entries]
func parseRangeColumnEntry(
	data []byte, pos int,
) (colName string, idx parsedRangeIndex, newPos int, err error) {
	if pos+2 > len(data) {
		return "", idx, pos, fmt.Errorf("range entry: short for name_len")
	}

	nameLen := int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2
	if pos+nameLen > len(data) {
		return "", idx, pos, fmt.Errorf("range entry: short for name")
	}

	colName = string(data[pos : pos+nameLen])
	pos += nameLen

	if pos+1 > len(data) {
		return colName, idx, pos, fmt.Errorf("range entry %q: short for type", colName)
	}

	colType := shared.ColumnType(data[pos])
	pos++
	idx.colType = colType

	// Bucket metadata always present: bucket_min[8] + bucket_max[8] + boundary_count[4] + boundaries[N×8]
	if pos+20 > len(data) {
		return colName, idx, pos, fmt.Errorf("range entry %q: short for bucket header", colName)
	}

	pos += 16 // skip min + max
	boundaryCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	pos += boundaryCount * 8

	// typed_count[4] + typed boundaries
	if pos+4 > len(data) {
		return colName, idx, pos, fmt.Errorf("range entry %q: short for typed_count", colName)
	}

	typedCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	_, newPos2, err2 := skipTypedBoundaries(data, pos, colType, typedCount)
	if err2 != nil {
		return colName, idx, pos, fmt.Errorf("range entry %q: typed boundaries: %w", colName, err2)
	}

	pos = newPos2

	// value_count[4]
	if pos+4 > len(data) {
		return colName, idx, pos, fmt.Errorf("range entry %q: short for value_count", colName)
	}

	valueCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	idx.entries = make([]rangeEntry, 0, valueCount)

	for v := range valueCount {
		key, blockIDs, newPos2, err2 := parseRangeValueEntry(data, pos, colType)
		if err2 != nil {
			return colName, idx, pos, fmt.Errorf("range entry %q value[%d]: %w", colName, v, err2)
		}

		pos = newPos2
		idx.entries = append(idx.entries, rangeEntry{lower: key, blockIDs: blockIDs})
	}

	// The writer serializes entries in lexicographic key order. For numeric types
	// LE-encoded bytes do not sort lexicographically by value (e.g. enc(256) < enc(10)),
	// so re-sort by decoded numeric value to enable correct binary-search lookup.
	sortRangeEntries(colType, idx.entries)

	return colName, idx, pos, nil
}

// sortRangeEntries sorts range index entries by ascending lower-boundary value.
// String/bytes entries are already in lexicographic (correct) order after the writer's sort;
// numeric types require a numeric decode before comparison.
func sortRangeEntries(colType shared.ColumnType, entries []rangeEntry) {
	switch colType {
	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		sort.Slice(entries, func(i, j int) bool {
			return decodeInt64Key(entries[i].lower) < decodeInt64Key(entries[j].lower)
		})
	case shared.ColumnTypeRangeUint64:
		sort.Slice(entries, func(i, j int) bool {
			return decodeUint64Key(entries[i].lower) < decodeUint64Key(entries[j].lower)
		})
	case shared.ColumnTypeRangeFloat64:
		sort.Slice(entries, func(i, j int) bool {
			return decodeFloat64Key(entries[i].lower) < decodeFloat64Key(entries[j].lower)
		})
	}
}

func decodeInt64Key(key string) int64 {
	if len(key) < 8 {
		return 0
	}

	return int64(binary.LittleEndian.Uint64([]byte(key))) //nolint:gosec // safe: reinterpreting uint64 bits as int64
}

func decodeUint64Key(key string) uint64 {
	if len(key) < 8 {
		return 0
	}

	return binary.LittleEndian.Uint64([]byte(key))
}

func decodeFloat64Key(key string) float64 {
	if len(key) < 8 {
		return 0
	}

	return math.Float64frombits(binary.LittleEndian.Uint64([]byte(key)))
}

// compareRangeKey compares two encoded boundary keys using type-aware comparison.
// Returns negative if a < b, 0 if equal, positive if a > b.
func compareRangeKey(colType shared.ColumnType, a, b string) int {
	switch colType {
	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		va, vb := decodeInt64Key(a), decodeInt64Key(b)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}

		return 0

	case shared.ColumnTypeRangeUint64:
		va, vb := decodeUint64Key(a), decodeUint64Key(b)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}

		return 0

	case shared.ColumnTypeRangeFloat64:
		va, vb := decodeFloat64Key(a), decodeFloat64Key(b)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}

		return 0

	default: // RangeString, RangeBytes: lexicographic
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}

		return 0
	}
}

// parseRangeValueEntry parses one RangeValueEntry from data[pos:].
// Returns the encoded key string, block IDs, new position, and error.
func parseRangeValueEntry(
	data []byte, pos int, colType shared.ColumnType,
) (key string, blockIDs []uint32, newPos int, err error) {
	var keyBytes []byte

	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if pos+4 > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(string): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(string): short for data")
		}

		keyBytes = data[pos : pos+kLen]
		pos += kLen

	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if pos+4 > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(bytes): short for len")
		}

		kLen := int(binary.LittleEndian.Uint32(data[pos:]))
		pos += 4
		if pos+kLen > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(bytes): short for data")
		}

		keyBytes = data[pos : pos+kLen]
		pos += kLen

	case shared.ColumnTypeInt64, shared.ColumnTypeUint64, shared.ColumnTypeFloat64:
		if pos+8 > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(numeric): short")
		}

		keyBytes = data[pos : pos+8]
		pos += 8

	case shared.ColumnTypeBool:
		if pos+1 > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(bool): short")
		}

		keyBytes = data[pos : pos+1]
		pos++

	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeUint64,
		shared.ColumnTypeRangeDuration, shared.ColumnTypeRangeFloat64:
		if pos+1 > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(range numeric): short for length_prefix")
		}

		kLen := int(data[pos])
		pos++
		if pos+kLen > len(data) {
			return "", nil, pos, fmt.Errorf("value_key(range numeric): short for key_data (len=%d)", kLen)
		}

		keyBytes = data[pos : pos+kLen]
		pos += kLen

	default:
		return "", nil, pos, fmt.Errorf("value_entry: unknown column type %d", colType)
	}

	// block_id_count[4] + block_ids[N×4]
	if pos+4 > len(data) {
		return "", nil, pos, fmt.Errorf("value_entry: short for block_id_count")
	}

	bidCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	need := bidCount * 4
	if pos+need > len(data) {
		return "", nil, pos, fmt.Errorf("value_entry: short for block_ids (count=%d)", bidCount)
	}

	blockIDs = make([]uint32, bidCount)
	for i := range bidCount {
		blockIDs[i] = binary.LittleEndian.Uint32(data[pos+i*4:])
	}

	pos += need

	return string(keyBytes), blockIDs, pos, nil
}

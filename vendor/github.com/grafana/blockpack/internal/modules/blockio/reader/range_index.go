package reader

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"slices"

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
// bucketMin and bucketMax hold the global min/max across all blocks for the column
// (stored in the wire format bucket metadata). For RangeString/RangeBytes they are 0.
type parsedRangeIndex struct {
	entries          []rangeEntry
	float64BoundsRaw []byte   // NOTE-PERF-RANGE: zero-copy sub-slice of metadataBytes; decoded on demand in RangeColumnBoundaries
	stringBounds     []string // decoded for RangeString; nil otherwise
	bytesBounds      [][]byte // decoded for RangeBytes; nil otherwise
	bucketMin        int64    // global min as int64 bits; 0 for String/Bytes
	bucketMax        int64    // global max as int64 bits; 0 for String/Bytes
	colType          shared.ColumnType
}

// ensureRangeColumnParsed parses the range index for colName if not already done.
// Caches result in r.rangeParsed.
func (r *Reader) ensureRangeColumnParsed(colName string) error {
	if _, ok := r.rangeParsed[colName]; ok {
		return nil
	}

	// For V14 files, the range section bytes are loaded lazily on first access.
	if err := r.ensureV14RangeSection(); err != nil {
		return err
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

	idx.bucketMin = int64(binary.LittleEndian.Uint64(data[pos:])) //nolint:gosec
	pos += 8
	idx.bucketMax = int64(binary.LittleEndian.Uint64(data[pos:])) //nolint:gosec
	pos += 8
	boundaryCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	if pos+boundaryCount*8 > len(data) {
		return colName, idx, pos, fmt.Errorf("range entry %q: short for boundaries (%d)", colName, boundaryCount)
	}
	pos += boundaryCount * 8 // raw int64 boundaries (already captured via bucketMin/bucketMax)

	// typed_count[4] + typed boundaries
	if pos+4 > len(data) {
		return colName, idx, pos, fmt.Errorf("range entry %q: short for typed_count", colName)
	}

	typedCount := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	newPos2, parseErr2 := parseTypedBoundaries(data, pos, colType, typedCount, &idx)
	if parseErr2 != nil {
		return colName, idx, pos, fmt.Errorf("range entry %q: typed boundaries: %w", colName, parseErr2)
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
		slices.SortFunc(entries, func(a, b rangeEntry) int {
			return cmp.Compare(decodeInt64Key(a.lower), decodeInt64Key(b.lower))
		})
	case shared.ColumnTypeRangeUint64:
		slices.SortFunc(entries, func(a, b rangeEntry) int {
			return cmp.Compare(decodeUint64Key(a.lower), decodeUint64Key(b.lower))
		})
	case shared.ColumnTypeRangeFloat64:
		slices.SortFunc(entries, func(a, b rangeEntry) int {
			return cmp.Compare(decodeFloat64Key(a.lower), decodeFloat64Key(b.lower))
		})
	}
}

// parseTypedBoundaries reads typed boundary data and stores it in idx.
// This mirrors skipTypedBoundaries but captures the values instead of discarding them.
// For RangeInt64/RangeUint64/RangeDuration, typed_count must be 0 (no additional bytes).
// For RangeFloat64, reads count×float64.
// For RangeString, reads count×(uint32_len + string_bytes).
// For RangeBytes, reads count×(uint32_len + bytes).
func parseTypedBoundaries(
	data []byte, pos int, colType shared.ColumnType, count int, idx *parsedRangeIndex,
) (int, error) {
	switch colType {
	case shared.ColumnTypeRangeFloat64:
		need := count * 8
		if pos+need > len(data) {
			return pos, fmt.Errorf("typed boundaries(float64): need %d bytes at pos %d", need, pos)
		}
		// NOTE-PERF-RANGE: store a zero-copy sub-slice rather than decoding to []float64.
		// Decoded on demand in RangeColumnBoundaries only when a caller requests boundaries.
		idx.float64BoundsRaw = data[pos : pos+need]
		return pos + need, nil

	case shared.ColumnTypeRangeString:
		bounds := make([]string, 0, count)
		for i := range count {
			if pos+4 > len(data) {
				return pos, fmt.Errorf("typed boundaries(string)[%d]: short for len", i)
			}
			sLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+sLen > len(data) {
				return pos, fmt.Errorf("typed boundaries(string)[%d]: short for data", i)
			}
			bounds = append(bounds, string(data[pos:pos+sLen]))
			pos += sLen
		}
		idx.stringBounds = bounds
		return pos, nil

	case shared.ColumnTypeRangeBytes:
		bounds := make([][]byte, 0, count)
		for i := range count {
			if pos+4 > len(data) {
				return pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for len", i)
			}
			bLen := int(binary.LittleEndian.Uint32(data[pos:]))
			pos += 4
			if pos+bLen > len(data) {
				return pos, fmt.Errorf("typed boundaries(bytes)[%d]: short for data", i)
			}
			b := make([]byte, bLen)
			copy(b, data[pos:pos+bLen])
			bounds = append(bounds, b)
			pos += bLen
		}
		idx.bytesBounds = bounds
		return pos, nil

	default:
		// RangeInt64/RangeUint64/RangeDuration: typed_count must be 0.
		if count != 0 {
			return pos, fmt.Errorf("typed boundaries: non-zero count %d for type %d", count, colType)
		}
		return pos, nil
	}
}

// readLE8 reads an 8-byte little-endian uint64 from key.
// Returns (value, true) on success, (0, false) if key is too short.
// Callers use the bool to apply type-appropriate sentinels (BUG-08).
func readLE8(key string) (uint64, bool) {
	if len(key) < 8 {
		return 0, false
	}

	return binary.LittleEndian.Uint64([]byte(key)), true
}

func decodeInt64Key(key string) int64 {
	// BUG-08: return math.MinInt64 (not 0) for short keys — 0 is a valid value.
	// Sentinel sorts below all valid int64 values; callers must not prune on it.
	v, ok := readLE8(key)
	if !ok {
		return math.MinInt64
	}

	return int64(v) //nolint:gosec // safe: reinterpreting uint64 bits as int64
}

func decodeUint64Key(key string) uint64 {
	// For uint64, 0 is already the minimum sentinel — no change from original.
	v, _ := readLE8(key)
	return v
}

func decodeFloat64Key(key string) float64 {
	// BUG-08: return NaN (not 0.0) for short keys — 0.0 is a valid value.
	// BUG-07 fix ensures NaN is handled correctly by compareRangeKey via cmp.Compare.
	v, ok := readLE8(key)
	if !ok {
		return math.NaN()
	}

	return math.Float64frombits(v)
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
		// NOTE-BUG-07: Use cmp.Compare instead of manual < / > to handle NaN.
		// IEEE 754: NaN < x, NaN > x, and NaN == x are all false, so the
		// manual branches all failed for NaN inputs, returning 0 (equal) and
		// corrupting binary search. cmp.Compare provides a stable total order:
		// NaN is treated as less than any non-NaN value.
		va, vb := decodeFloat64Key(a), decodeFloat64Key(b)
		return cmp.Compare(va, vb)

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

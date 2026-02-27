package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// pickIndexWidth returns 1, 2, or 4 bytes per index based on dictionary size.
func pickIndexWidth(dictSize int) uint8 {
	switch {
	case dictSize <= 255:
		return 1
	case dictSize <= 65535:
		return 2
	default:
		return 4
	}
}

// encodeDictionaryKind handles kinds 1/2/6/7 for any column type.
//
// Wire format:
//
//	enc_version[1] + kind[1]
//	+ index_width[1]
//	+ dict_data_len[4 LE] + zstd(dict_payload)
//	+ row_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ indexes (kind 6/7: index_count[4]+rle_len[4]+rle_data; kind 1/2: index_count×index_width bytes)
func encodeDictionaryKind(
	kind uint8,
	colType shared.ColumnType,
	values any,
	present []bool,
	nRows int,
	enc *zstdEncoder,
) ([]byte, error) {
	// Build indexes and dict payload depending on column type.
	var indexes []uint32
	var dictPayload []byte

	switch colType {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		sv, _ := values.([]string)
		entries, idx := buildStringDict(sv, present)
		indexes = idx
		dictPayload = encodeStringDictPayload(entries)

	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		iv, _ := values.([]int64)
		entries, idx := buildInt64Dict(iv, present)
		indexes = idx
		dictPayload = encodeInt64DictPayload(entries)

	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		uv, _ := values.([]uint64)
		entries, idx := buildUint64Dict(uv, present)
		indexes = idx
		dictPayload = encodeUint64DictPayload(entries)

	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		fv, _ := values.([]float64)
		entries, idx := buildFloat64Dict(fv, present)
		indexes = idx
		dictPayload = encodeFloat64DictPayload(entries)

	case shared.ColumnTypeBool:
		bv, _ := values.([]bool)
		entries, idx := buildBoolDict(bv, present)
		indexes = idx
		dictPayload = encodeBoolDictPayload(entries)

	default: // ColumnTypeBytes, ColumnTypeRangeBytes
		bv, _ := values.([][]byte)
		entries, idx := buildBytesDict(bv, present)
		indexes = idx
		dictPayload = encodeBytesDictPayload(entries)
	}

	// Present rows are the indexes slice (one per present row for sparse, or all rows for dense).
	indexWidth := pickIndexWidth(len(indexes))

	// Build presence bitset.
	bitsetLen := (nRows + 7) / 8
	bitset := make([]byte, bitsetLen)
	presentCount := 0

	for i := range nRows {
		if i < len(present) && present[i] {
			bitset[i/8] |= 1 << uint(i%8)
			presentCount++
		}
	}

	rleData, err := shared.EncodePresenceRLE(bitset, nRows)
	if err != nil {
		return nil, err
	}

	compressedDict, err := enc.compress(dictPayload)
	if err != nil {
		return nil, err
	}

	// For sparse kinds (2, 7), indexes covers only present rows.
	// For dense kinds (1, 6), indexes covers all rows.
	// The indexes slice from buildXxxDict already contains only present-row indexes.
	// For dense, we need full-row indexes (null rows get index 0).
	var fullIndexes []uint32
	if kind == KindDictionary || kind == KindRLEIndexes {
		// Dense: build full row indexes — null rows point to index 0.
		fullIndexes = make([]uint32, nRows)
		pi := 0
		for i := range nRows {
			if i < len(present) && present[i] {
				if pi < len(indexes) {
					fullIndexes[i] = indexes[pi]
					pi++
				}
			}
			// null rows remain 0
		}
	} else {
		// Sparse: use only present-row indexes.
		fullIndexes = indexes
	}

	buf := make([]byte, 0, 3+4+len(compressedDict)+4+4+len(rleData)+nRows*int(indexWidth)+20)
	buf = append(buf, shared.ColumnEncodingVersion, kind, indexWidth)
	buf = appendUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
	buf = append(buf, compressedDict...)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)

	if kind == KindRLEIndexes || kind == KindSparseRLEIndexes {
		// RLE index encoding.
		rleIndexData, err := shared.EncodeIndexRLE(fullIndexes)
		if err != nil {
			return nil, err
		}
		buf = appendUint32LE(buf, uint32(len(fullIndexes)))  //nolint:gosec // safe: index count bounded by MaxBlockSpans
		buf = appendUint32LE(buf, uint32(len(rleIndexData))) //nolint:gosec // safe: rle data bounded by block size
		buf = append(buf, rleIndexData...)
	} else {
		// Raw index array.
		if kind == KindSparseDictionary {
			buf = appendUint32LE(buf, uint32(len(fullIndexes))) //nolint:gosec // safe: index count bounded by MaxBlockSpans
		}
		for _, idx := range fullIndexes {
			buf = appendUintLE(buf, uint64(idx), indexWidth)
		}
	}

	return buf, nil
}

// encodeDeltaDictionaryKind handles kinds 12/13 for [][]byte columns.
//
// Wire format:
//
//	enc_version[1] + kind[1]
//	+ index_width[1]
//	+ dict_data_len[4 LE] + zstd(bytes_dict_payload)
//	+ row_count[4 LE]
//	+ presence_rle_len[4 LE] + presence_rle_data
//	+ delta_len[4 LE] + zstd(delta_array)
func encodeDeltaDictionaryKind(
	kind uint8,
	values [][]byte,
	present []bool,
	nRows int,
	enc *zstdEncoder,
) ([]byte, error) {
	entries, indexes := buildBytesDict(values, present)
	indexWidth := pickIndexWidth(len(entries))

	// Dictionary uses the same wire format as a plain bytes dict (§9.2).
	// The "delta" refers only to the index array, not the dictionary entries.
	dictPayload := encodeBytesDictPayload(entries)

	// Build presence bitset.
	bitsetLen := (nRows + 7) / 8
	bitset := make([]byte, bitsetLen)
	presentCount := 0

	for i := range nRows {
		if i < len(present) && present[i] {
			bitset[i/8] |= 1 << uint(i%8)
			presentCount++
		}
	}

	rleData, err := shared.EncodePresenceRLE(bitset, nRows)
	if err != nil {
		return nil, err
	}

	compressedDict, err := enc.compress(dictPayload)
	if err != nil {
		return nil, err
	}

	// Build delta index array.
	// Dense (kind 12): one delta per row (including null rows).
	// Sparse (kind 13): one delta per present row only.
	var deltaRows []uint32
	if kind == KindDeltaDictionary {
		// Dense: full row delta stream.
		fullIndexes := make([]uint32, nRows)
		pi := 0
		for i := range nRows {
			if i < len(present) && present[i] {
				if pi < len(indexes) {
					fullIndexes[i] = indexes[pi]
					pi++
				}
			}
		}
		deltaRows = fullIndexes
	} else {
		// Sparse: only present rows.
		deltaRows = indexes
	}

	// Convert indexes to int32 deltas.
	deltaData := make([]byte, 0, len(deltaRows)*4)
	var prev int32
	for _, idx := range deltaRows {
		cur := int32(idx) //nolint:gosec // safe: dictionary index bounded by MaxDictionarySize (65535), fits in int32
		delta := cur - prev
		prev = cur
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], uint32(delta)) //nolint:gosec // safe: reinterpreting int32 delta as uint32 bits for serialization
		deltaData = append(deltaData, tmp[:]...)
	}

	compressedDelta, err := enc.compress(deltaData)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 3+4+len(compressedDict)+4+4+len(rleData)+4+len(compressedDelta))
	buf = append(buf, shared.ColumnEncodingVersion, kind, indexWidth)
	buf = appendUint32LE(buf, uint32(len(compressedDict))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
	buf = append(buf, compressedDict...)
	buf = appendUint32LE(buf, uint32(nRows))        //nolint:gosec // safe: nRows bounded by MaxBlockSpans (65535)
	buf = appendUint32LE(buf, uint32(len(rleData))) //nolint:gosec // safe: rle data bounded by block size
	buf = append(buf, rleData...)
	buf = appendUint32LE(buf, uint32(len(compressedDelta))) //nolint:gosec // safe: compressed data bounded by block size
	buf = append(buf, compressedDelta...)

	return buf, nil
}

// ---- Dictionary builders ----

// buildStringDict returns deduplicated sorted entries and per-present-row indexes.
func buildStringDict(values []string, present []bool) (entries []string, indexes []uint32) {
	seen := make(map[string]uint32)
	entries = make([]string, 0)

	for i, p := range present {
		if !p {
			continue
		}
		var v string
		if i < len(values) {
			v = values[i]
		}
		if _, ok := seen[v]; !ok {
			seen[v] = uint32(len(entries)) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
			entries = append(entries, v)
		}
	}

	slices.Sort(entries)
	// Rebuild index map after sort.
	for i, e := range entries {
		seen[e] = uint32(i) //nolint:gosec // safe: dict size bounded by MaxDictionarySize, fits in uint32
	}

	for i, p := range present {
		if !p {
			continue
		}
		var v string
		if i < len(values) {
			v = values[i]
		}
		indexes = append(indexes, seen[v])
	}

	return entries, indexes
}

// buildInt64Dict returns deduplicated sorted entries and per-present-row indexes.
func buildInt64Dict(values []int64, present []bool) (entries []int64, indexes []uint32) {
	seen := make(map[int64]uint32)
	entries = make([]int64, 0)

	for i, p := range present {
		if !p {
			continue
		}
		var v int64
		if i < len(values) {
			v = values[i]
		}
		if _, ok := seen[v]; !ok {
			seen[v] = uint32(len(entries)) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
			entries = append(entries, v)
		}
	}

	slices.Sort(entries)
	for i, e := range entries {
		seen[e] = uint32(i) //nolint:gosec // safe: dict size bounded by MaxDictionarySize, fits in uint32
	}

	for i, p := range present {
		if !p {
			continue
		}
		var v int64
		if i < len(values) {
			v = values[i]
		}
		indexes = append(indexes, seen[v])
	}

	return entries, indexes
}

// buildUint64Dict returns deduplicated sorted entries and per-present-row indexes.
func buildUint64Dict(values []uint64, present []bool) (entries []uint64, indexes []uint32) {
	seen := make(map[uint64]uint32)
	entries = make([]uint64, 0)

	for i, p := range present {
		if !p {
			continue
		}
		var v uint64
		if i < len(values) {
			v = values[i]
		}
		if _, ok := seen[v]; !ok {
			seen[v] = uint32(len(entries)) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
			entries = append(entries, v)
		}
	}

	slices.Sort(entries)
	for i, e := range entries {
		seen[e] = uint32(i) //nolint:gosec // safe: dict size bounded by MaxDictionarySize, fits in uint32
	}

	for i, p := range present {
		if !p {
			continue
		}
		var v uint64
		if i < len(values) {
			v = values[i]
		}
		indexes = append(indexes, seen[v])
	}

	return entries, indexes
}

// buildFloat64Dict returns deduplicated sorted entries and per-present-row indexes.
func buildFloat64Dict(values []float64, present []bool) (entries []float64, indexes []uint32) {
	seen := make(map[float64]uint32)
	entries = make([]float64, 0)

	for i, p := range present {
		if !p {
			continue
		}
		var v float64
		if i < len(values) {
			v = values[i]
		}
		if _, ok := seen[v]; !ok {
			seen[v] = uint32(len(entries)) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
			entries = append(entries, v)
		}
	}

	slices.Sort(entries)
	for i, e := range entries {
		seen[e] = uint32(i)
	}

	for i, p := range present {
		if !p {
			continue
		}
		var v float64
		if i < len(values) {
			v = values[i]
		}
		indexes = append(indexes, seen[v])
	}

	return entries, indexes
}

// buildBoolDict returns deduplicated entries (as uint8: 0/1) and per-present-row indexes.
func buildBoolDict(values []bool, present []bool) (entries []uint8, indexes []uint32) {
	var hasFalse, hasTrue bool

	for i, p := range present {
		if !p {
			continue
		}
		var v bool
		if i < len(values) {
			v = values[i]
		}
		if v {
			hasTrue = true
		} else {
			hasFalse = true
		}
	}

	indexMap := make(map[bool]uint32)
	if hasFalse {
		indexMap[false] = uint32(len(entries)) //nolint:gosec // safe: bool dict has at most 2 entries
		entries = append(entries, 0)
	}
	if hasTrue {
		indexMap[true] = uint32(len(entries)) //nolint:gosec // safe: bool dict has at most 2 entries
		entries = append(entries, 1)
	}

	for i, p := range present {
		if !p {
			continue
		}
		var v bool
		if i < len(values) {
			v = values[i]
		}
		indexes = append(indexes, indexMap[v])
	}

	return entries, indexes
}

// buildBytesDict returns deduplicated entries and per-present-row indexes.
// Entries are kept in insertion order (not sorted, since []byte map keys would need string conversion).
func buildBytesDict(values [][]byte, present []bool) (entries [][]byte, indexes []uint32) {
	seen := make(map[string]uint32)
	entries = make([][]byte, 0)

	for i, p := range present {
		if !p {
			continue
		}
		var v []byte
		if i < len(values) {
			v = values[i]
		}
		key := string(v)
		if _, ok := seen[key]; !ok {
			seen[key] = uint32(len(entries)) //nolint:gosec // safe: dict size bounded by MaxDictionarySize
			cp := make([]byte, len(v))
			copy(cp, v)
			entries = append(entries, cp)
		}
	}

	// Sort entries lexicographically and rebuild index map.
	slices.SortFunc(entries, func(a, b []byte) int {
		minLen := min(len(a), len(b))
		for i := range minLen {
			if a[i] < b[i] {
				return -1
			}
			if a[i] > b[i] {
				return 1
			}
		}
		return len(a) - len(b)
	})
	for i, e := range entries {
		seen[string(e)] = uint32(i)
	}

	for i, p := range present {
		if !p {
			continue
		}
		var v []byte
		if i < len(values) {
			v = values[i]
		}
		indexes = append(indexes, seen[string(v)])
	}

	return entries, indexes
}

// ---- Dict payload serializers ----

func encodeStringDictPayload(entries []string) []byte {
	sz := 4
	for _, e := range entries {
		sz += 4 + len(e)
	}

	buf := make([]byte, 0, sz)
	buf = appendUint32LE(buf, uint32(len(entries))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize

	for _, e := range entries {
		buf = appendUint32LE(buf, uint32(len(e))) //nolint:gosec // safe: entry length bounded by MaxStringLen
		buf = append(buf, e...)
	}

	return buf
}

func encodeInt64DictPayload(entries []int64) []byte {
	buf := make([]byte, 0, 4+len(entries)*8)
	buf = appendUint32LE(buf, uint32(len(entries))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize

	for _, e := range entries {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(e)) //nolint:gosec // safe: reinterpreting int64 bits as uint64 for serialization
		buf = append(buf, tmp[:]...)
	}

	return buf
}

func encodeUint64DictPayload(entries []uint64) []byte {
	buf := make([]byte, 0, 4+len(entries)*8)
	buf = appendUint32LE(buf, uint32(len(entries))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize

	for _, e := range entries {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], e)
		buf = append(buf, tmp[:]...)
	}

	return buf
}

func encodeFloat64DictPayload(entries []float64) []byte {
	buf := make([]byte, 0, 4+len(entries)*8)
	buf = appendUint32LE(buf, uint32(len(entries))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize

	for _, e := range entries {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(e))
		buf = append(buf, tmp[:]...)
	}

	return buf
}

func encodeBoolDictPayload(entries []uint8) []byte {
	buf := make([]byte, 0, 4+len(entries))
	buf = appendUint32LE(buf, uint32(len(entries))) //nolint:gosec // safe: bool dict has at most 2 entries

	buf = append(buf, entries...)

	return buf
}

func encodeBytesDictPayload(entries [][]byte) []byte {
	sz := 4
	for _, e := range entries {
		sz += 4 + len(e)
	}

	buf := make([]byte, 0, sz)
	buf = appendUint32LE(buf, uint32(len(entries))) //nolint:gosec // safe: dict size bounded by MaxDictionarySize

	for _, e := range entries {
		buf = appendUint32LE(buf, uint32(len(e))) //nolint:gosec // safe: entry length bounded by MaxBytesLen
		buf = append(buf, e...)
	}

	return buf
}

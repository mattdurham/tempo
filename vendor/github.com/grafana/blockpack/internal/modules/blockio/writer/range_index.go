package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// rangeColumnData holds the value map and range-bucket metadata for one range column.
// Field order: slices first (largest), then map, then int64s, then small types (betteralign).
type rangeColumnData struct {
	values        map[string][]uint32 // in-memory key → sorted block IDs
	boundaries    []int64             // KLL quantile boundaries as int64 bits (empty for String/Bytes)
	float64Bounds []float64           // type-specific boundaries for RangeFloat64 (nil otherwise)
	stringBounds  []string            // type-specific boundaries for RangeString (nil otherwise)
	bytesBounds   [][]byte            // type-specific boundaries for RangeBytes (nil otherwise)
	bucketMin     int64               // bucket_min as int64 bits
	bucketMax     int64               // bucket_max as int64 bits
	colType       shared.ColumnType
}

// rangeIndex is the final nested map after log deduplication.
// map[colName]*rangeColumnData
type rangeIndex map[string]*rangeColumnData

// globalKLLState holds per-column KLL sketches accumulated across all blocks.
type globalKLLState struct {
	int64   map[string]*KLL[int64]
	uint64  map[string]*KLL[uint64]
	float64 map[string]*KLL[float64]
	str     map[string]*KLLString
	bytes   map[string]*KLLBytes
}

func newGlobalKLLState() globalKLLState {
	return globalKLLState{
		int64:   make(map[string]*KLL[int64]),
		uint64:  make(map[string]*KLL[uint64]),
		float64: make(map[string]*KLL[float64]),
		str:     make(map[string]*KLLString),
		bytes:   make(map[string]*KLLBytes),
	}
}

// encodeRangeKey returns the in-memory key for a given column type and value.
// Returns "" if the type should not be indexed.
func encodeRangeKey(typ shared.ColumnType, val shared.AttrValue) string {
	var tmp [8]byte
	switch typ {
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		return val.Str
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeDuration, shared.ColumnTypeRangeInt64:
		binary.LittleEndian.PutUint64(tmp[:], uint64(val.Int)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
		return string(tmp[:])
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		binary.LittleEndian.PutUint64(tmp[:], val.Uint)
		return string(tmp[:])
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(val.Float))
		return string(tmp[:])
	case shared.ColumnTypeBool:
		if val.Bool {
			return "\x01"
		}
		return "\x00"
	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		return string(val.Bytes)
	default:
		return ""
	}
}

// encodeInt64BoundaryKey returns the 8-byte LE encoding of an int64 boundary value as a string key.
func encodeInt64BoundaryKey(v int64) string {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // safe: reinterpreting int64 bits as uint64
	return string(tmp[:])
}

// encodeUint64BoundaryKey returns the 8-byte LE encoding of a uint64 boundary value as a string key.
func encodeUint64BoundaryKey(v uint64) string {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return string(tmp[:])
}

// encodeFloat64BoundaryKey returns the 8-byte LE IEEE-754 bits encoding of a float64 boundary as a string key.
func encodeFloat64BoundaryKey(v float64) string {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v))
	return string(tmp[:])
}

// mergeBlockIDs appends non-duplicate IDs from incoming to existing, then sorts.
func mergeBlockIDs(existing, incoming []uint32) []uint32 {
	for _, id := range incoming {
		if !slices.Contains(existing, id) {
			existing = append(existing, id)
		}
	}
	slices.Sort(existing)
	return existing
}

// findBucketInt64 returns the bucket index for v given the sorted boundaries.
// Uses binary search: O(log n) comparisons instead of O(n).
func findBucketInt64(v int64, bounds []int64) uint16 {
	lo, hi := 1, len(bounds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bounds[mid] <= v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= len(bounds)-1 {
		return uint16(len(bounds) - 2) //nolint:gosec // safe: len(bounds)-2 is in range [0, nBuckets-1] which fits uint16
	}
	return uint16(idx) //nolint:gosec // safe: idx is in range [0, len(bounds)-2] which fits uint16
}

// findBucketUint64 returns the bucket index for v given the sorted boundaries.
// Uses binary search: O(log n) comparisons instead of O(n).
func findBucketUint64(v uint64, bounds []uint64) uint16 {
	lo, hi := 1, len(bounds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bounds[mid] <= v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= len(bounds)-1 {
		return uint16(len(bounds) - 2) //nolint:gosec // safe: len(bounds)-2 is in range [0, nBuckets-1] which fits uint16
	}
	return uint16(idx) //nolint:gosec // safe: idx is in range [0, len(bounds)-2] which fits uint16
}

// findBucketFloat64 returns the bucket index for v given the sorted boundaries.
// Uses binary search: O(log n) comparisons instead of O(n).
func findBucketFloat64(v float64, bounds []float64) uint16 {
	lo, hi := 1, len(bounds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bounds[mid] <= v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= len(bounds)-1 {
		return uint16(len(bounds) - 2) //nolint:gosec // safe: len(bounds)-2 is in range [0, nBuckets-1] which fits uint16
	}
	return uint16(idx) //nolint:gosec // safe: idx is in range [0, len(bounds)-2] which fits uint16
}

// findBucketString returns the bucket index for v given the sorted string boundaries.
// Uses binary search: O(log n) comparisons instead of O(n).
func findBucketString(v string, bounds []string) uint16 {
	lo, hi := 1, len(bounds)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if bounds[mid] <= v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	idx := lo - 1
	if idx >= len(bounds)-1 {
		return uint16(len(bounds) - 2) //nolint:gosec // safe: len(bounds)-2 is in range [0, nBuckets-1] which fits uint16
	}
	return uint16(idx) //nolint:gosec // safe: idx is in range [0, len(bounds)-2] which fits uint16
}

// remapInt64Keys replaces exact int64-encoded keys with lower-boundary keys, merging block IDs.
// The key for each bucket is the lower boundary value encoded as 8-byte LE int64 (see SPECS §5.2.1).
func remapInt64Keys(values map[string][]uint32, bounds []int64) map[string][]uint32 {
	out := make(map[string][]uint32, len(bounds))
	for key, blockIDs := range values {
		var raw [8]byte
		copy(raw[:], key)
		v := int64(binary.LittleEndian.Uint64(raw[:])) //nolint:gosec // safe: reinterpreting uint64 bits as int64
		bid := findBucketInt64(v, bounds)
		bk := encodeInt64BoundaryKey(bounds[bid])
		out[bk] = mergeBlockIDs(out[bk], blockIDs)
	}
	return out
}

// remapUint64Keys replaces exact uint64-encoded keys with lower-boundary keys, merging block IDs.
// The key for each bucket is the lower boundary value encoded as 8-byte LE uint64 (see SPECS §5.2.1).
func remapUint64Keys(values map[string][]uint32, bounds []uint64) map[string][]uint32 {
	out := make(map[string][]uint32, len(bounds))
	for key, blockIDs := range values {
		var raw [8]byte
		copy(raw[:], key)
		v := binary.LittleEndian.Uint64(raw[:])
		bid := findBucketUint64(v, bounds)
		bk := encodeUint64BoundaryKey(bounds[bid])
		out[bk] = mergeBlockIDs(out[bk], blockIDs)
	}
	return out
}

// remapFloat64Keys replaces exact float64-encoded keys with lower-boundary keys, merging block IDs.
// The key for each bucket is the lower boundary value encoded as 8-byte LE IEEE-754 bits (see SPECS §5.2.1).
func remapFloat64Keys(values map[string][]uint32, bounds []float64) map[string][]uint32 {
	out := make(map[string][]uint32, len(bounds))
	for key, blockIDs := range values {
		var raw [8]byte
		copy(raw[:], key)
		bits := binary.LittleEndian.Uint64(raw[:])
		v := math.Float64frombits(bits)
		bid := findBucketFloat64(v, bounds)
		bk := encodeFloat64BoundaryKey(bounds[bid])
		out[bk] = mergeBlockIDs(out[bk], blockIDs)
	}
	return out
}

// remapStringKeys replaces string value keys with boundary-prefix keys, merging block IDs.
// The key for each bucket is the lower boundary of that bucket, truncated to
// rangeBucketKeyMaxLen bytes (see SPECS §5.2.1).
//
// Safety net: if the KLL reservoir missed early-arriving (alphabetically-first) values due
// to sorted-input eviction, bounds[bid] may be lexicographically greater than key. In that
// case we clamp the stored key to key itself so the bucket lower bound invariant holds —
// every stored key must be ≤ the actual value it covers (required for correct binary search).
func remapStringKeys(values map[string][]uint32, bounds []string) map[string][]uint32 {
	out := make(map[string][]uint32, len(bounds))
	for key, blockIDs := range values {
		bid := findBucketString(key, bounds)
		bk := truncateBoundaryKey(min(bounds[bid], key), rangeBucketKeyMaxLen)
		out[bk] = mergeBlockIDs(out[bk], blockIDs)
	}
	return out
}

// truncateBoundaryKey returns s truncated to maxLen bytes.
func truncateBoundaryKey(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// applyInt64Buckets applies int64 range buckets to a range column.
func applyInt64Buckets(cd *rangeColumnData, kll *KLL[int64], nBuckets int) {
	bounds := kll.Boundaries(nBuckets)
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeInt64
	cd.boundaries = make([]int64, len(bounds))
	copy(cd.boundaries, bounds)
	if len(bounds) > 0 {
		cd.bucketMin = bounds[0]
		cd.bucketMax = bounds[len(bounds)-1]
	}
	cd.values = remapInt64Keys(cd.values, bounds)
}

// applyUint64Buckets applies uint64 range buckets to a range column.
func applyUint64Buckets(cd *rangeColumnData, kll *KLL[uint64], nBuckets int) {
	bounds := kll.Boundaries(nBuckets)
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeUint64
	cd.boundaries = make([]int64, len(bounds))
	for i, b := range bounds {
		cd.boundaries[i] = int64(b) //nolint:gosec // safe: storing uint64 bits as int64 bits for wire format
	}
	if len(bounds) > 0 {
		cd.bucketMin = int64(bounds[0])             //nolint:gosec // safe: storing uint64 bits as int64 bits for wire format
		cd.bucketMax = int64(bounds[len(bounds)-1]) //nolint:gosec // safe: storing uint64 bits as int64 bits for wire format
	}
	cd.values = remapUint64Keys(cd.values, bounds)
}

// applyFloat64Buckets applies float64 range buckets to a range column.
func applyFloat64Buckets(cd *rangeColumnData, kll *KLL[float64], nBuckets int) {
	bounds := kll.Boundaries(nBuckets)
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeFloat64
	cd.float64Bounds = bounds
	cd.boundaries = make([]int64, len(bounds))
	for i, b := range bounds {
		cd.boundaries[i] = int64(math.Float64bits(b)) //nolint:gosec // safe: storing float64 bits as int64 bits for wire format
	}
	if len(bounds) > 0 {
		cd.bucketMin = int64(math.Float64bits(bounds[0]))             //nolint:gosec // safe: storing float64 bits as int64 bits for wire format
		cd.bucketMax = int64(math.Float64bits(bounds[len(bounds)-1])) //nolint:gosec // safe: storing float64 bits as int64 bits for wire format
	}
	cd.values = remapFloat64Keys(cd.values, bounds)
}

// applyStringBuckets applies string range buckets to a range column.
func applyStringBuckets(cd *rangeColumnData, kll *KLLString, nBuckets int) {
	bounds := kll.Boundaries(nBuckets)
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeString
	cd.stringBounds = bounds
	cd.boundaries = nil
	cd.bucketMin = 0
	cd.bucketMax = 0
	cd.values = remapStringKeys(cd.values, bounds)
}

// applyRangeBuckets applies range buckets to all columns in the range index.
// Every column uses KLL-derived boundaries to remap exact value keys to bucket IDs.
func applyRangeBuckets(idx rangeIndex, gkl *globalKLLState, nBuckets int) {
	for colName, cd := range idx {
		applyRangeBucketsForColumn(colName, cd, gkl, nBuckets)
	}
}

func applyRangeBucketsForColumn(
	colName string,
	cd *rangeColumnData,
	gkl *globalKLLState,
	nBuckets int,
) {
	switch cd.colType {
	case shared.ColumnTypeInt64, shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if kll, ok := gkl.int64[colName]; ok {
			applyInt64Buckets(cd, kll, nBuckets)
		}
	case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
		if kll, ok := gkl.uint64[colName]; ok {
			applyUint64Buckets(cd, kll, nBuckets)
		}
	case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
		if kll, ok := gkl.float64[colName]; ok {
			applyFloat64Buckets(cd, kll, nBuckets)
		}
	case shared.ColumnTypeString, shared.ColumnTypeRangeString:
		if kll, ok := gkl.str[colName]; ok {
			applyStringBuckets(cd, kll, nBuckets)
		}
	case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
		if kll, ok := gkl.bytes[colName]; ok {
			applyBytesBuckets(cd, kll, nBuckets)
		}
	}
}

// remapBytesKeys replaces bytes value keys with boundary-prefix keys, merging block IDs.
// The key for each bucket is the lower boundary of that bucket, truncated to
// rangeBucketKeyMaxLen bytes (see SPECS §5.2.1).
//
// Safety net: applies the same bucket lower bound invariant as remapStringKeys — clamps
// the stored key to key itself when the KLL-derived bound would exceed it.
//
// Preconverts [][]byte bounds to []string once to avoid per-key []byte→string allocations
// in the hot loop (up to O(unique_keys) savings per Flush call).
func remapBytesKeys(values map[string][]uint32, bounds [][]byte) map[string][]uint32 {
	// Convert bounds once: avoids alloc-per-key in the loop below.
	strBounds := make([]string, len(bounds))
	for i, b := range bounds {
		strBounds[i] = string(b)
	}
	out := make(map[string][]uint32, len(bounds))
	for key, blockIDs := range values {
		bid := findBucketString(key, strBounds)
		bk := truncateBoundaryKey(min(strBounds[bid], key), rangeBucketKeyMaxLen)
		out[bk] = mergeBlockIDs(out[bk], blockIDs)
	}
	return out
}

// buildKLLFromRangeIndex builds a globalKLLState by iterating over the unique keys
// already present in the deduped range index rather than accumulating per-span samples.
//
// Using deduped keys avoids two failure modes of per-span KLL accumulation:
//  1. Sorted-input eviction: the KLL reservoir uses idx = count % maxSamples. When spans are
//     sorted by (service_name, …), alphabetically-first values fill slots 0..N and are
//     overwritten during the second pass, leaving them absent from the reservoir.
//  2. Cardinality: the deduped index has at most (nBlocks × distinct_values) entries per
//     column, which is far below kllDefaultK for typical datasets.
//
// Go's map iteration order is randomized, so there is no systematic ordering bias regardless
// of how the index was populated.
func buildKLLFromRangeIndex(idx rangeIndex) globalKLLState {
	gkl := newGlobalKLLState()
	for colName, cd := range idx {
		switch cd.colType {
		case shared.ColumnTypeString, shared.ColumnTypeRangeString:
			if gkl.str[colName] == nil {
				gkl.str[colName] = NewKLLString()
			}
			for key := range cd.values {
				gkl.str[colName].Add(key)
			}
		case shared.ColumnTypeBytes, shared.ColumnTypeRangeBytes:
			if gkl.bytes[colName] == nil {
				gkl.bytes[colName] = NewKLLBytes()
			}
			for key := range cd.values {
				gkl.bytes[colName].Add([]byte(key))
			}
		case shared.ColumnTypeInt64, shared.ColumnTypeRangeDuration, shared.ColumnTypeRangeInt64:
			if gkl.int64[colName] == nil {
				gkl.int64[colName] = NewKLL[int64]()
			}
			for key := range cd.values {
				if len(key) >= 8 {
					v := int64(binary.LittleEndian.Uint64([]byte(key))) //nolint:gosec // safe: reinterpreting uint64 bits as int64
					gkl.int64[colName].Add(v)
				}
			}
		case shared.ColumnTypeUint64, shared.ColumnTypeRangeUint64:
			if gkl.uint64[colName] == nil {
				gkl.uint64[colName] = NewKLL[uint64]()
			}
			for key := range cd.values {
				if len(key) >= 8 {
					gkl.uint64[colName].Add(binary.LittleEndian.Uint64([]byte(key)))
				}
			}
		case shared.ColumnTypeFloat64, shared.ColumnTypeRangeFloat64:
			if gkl.float64[colName] == nil {
				gkl.float64[colName] = NewKLL[float64]()
			}
			for key := range cd.values {
				if len(key) >= 8 {
					gkl.float64[colName].Add(math.Float64frombits(binary.LittleEndian.Uint64([]byte(key))))
				}
			}
		}
	}
	return gkl
}

// applyBytesBuckets applies bytes range buckets to a range column.
func applyBytesBuckets(cd *rangeColumnData, kll *KLLBytes, nBuckets int) {
	bounds := kll.Boundaries(nBuckets)
	if len(bounds) < 2 {
		return
	}

	cd.colType = shared.ColumnTypeRangeBytes
	cd.bytesBounds = bounds
	cd.boundaries = nil
	cd.bucketMin = 0
	cd.bucketMax = 0
	cd.values = remapBytesKeys(cd.values, bounds)
}

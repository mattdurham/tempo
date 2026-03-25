package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"slices"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// blockRange records the [min, max] encoded key range for one column in one block.
// Used to populate range index buckets by range-overlap at Flush time.
type blockRange struct {
	minKey  string // encoded minimum observed value in this block
	maxKey  string // encoded maximum observed value in this block
	blockID uint32
}

// rangeColumnData holds accumulated per-block min/max ranges and the KLL sketch
// for one range column. At Flush time the KLL provides bucket boundaries; the
// blocks list is iterated to assign each block to all overlapping buckets.
//
// Field order: slices first (largest), then pointer, then int64s, then small
// types (betteralign).
type rangeColumnData struct {
	values map[string][]uint32 // bucket key → sorted block IDs (populated by applyRangeBuckets)
	// kllInt64, kllUint64, kllFloat64, kllStr, kllBytes hold the KLL sketch for
	// this column. Exactly one is non-nil depending on colType.
	kllInt64      *KLL[int64]
	kllUint64     *KLL[uint64]
	kllFloat64    *KLL[float64]
	kllStr        *KLLString
	kllBytes      *KLLBytes
	blocks        []blockRange // per-block [min, max] ranges; O(blocks × columns)
	boundaries    []int64      // KLL quantile boundaries as int64 bits (empty for String/Bytes)
	float64Bounds []float64    // type-specific boundaries for RangeFloat64 (nil otherwise)
	stringBounds  []string     // type-specific boundaries for RangeString (nil otherwise)
	bytesBounds   [][]byte     // type-specific boundaries for RangeBytes (nil otherwise)
	bucketMin     int64        // bucket_min as int64 bits
	bucketMax     int64        // bucket_max as int64 bits
	colType       shared.ColumnType
}

// rangeIndex is the final nested map after log deduplication.
// map[colName]*rangeColumnData
type rangeIndex map[string]*rangeColumnData

// normalizeToRangeType converts a plain column type to its Range* equivalent.
// The range index only ever stores Range* column types; plain types are
// promoted here so the rest of the range-index path never sees ColumnTypeInt64
// (or String/Bytes/etc.) in cd.colType.
func normalizeToRangeType(colType shared.ColumnType) shared.ColumnType {
	switch colType {
	case shared.ColumnTypeInt64:
		return shared.ColumnTypeRangeInt64
	case shared.ColumnTypeUint64:
		return shared.ColumnTypeRangeUint64
	case shared.ColumnTypeFloat64:
		return shared.ColumnTypeRangeFloat64
	case shared.ColumnTypeString:
		return shared.ColumnTypeRangeString
	case shared.ColumnTypeBytes:
		return shared.ColumnTypeRangeBytes
	default:
		return colType // already a Range* type (or Bool/unrecognized — not indexed)
	}
}

// newRangeColumnData allocates a rangeColumnData for the given column type,
// initializing the appropriate KLL sketch. colType is normalized to its Range*
// equivalent before storage so cd.colType is always a Range* type.
func newRangeColumnData(colType shared.ColumnType) *rangeColumnData {
	colType = normalizeToRangeType(colType)
	cd := &rangeColumnData{colType: colType}
	switch colType {
	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		cd.kllInt64 = NewKLL[int64]()
	case shared.ColumnTypeRangeUint64:
		cd.kllUint64 = NewKLL[uint64]()
	case shared.ColumnTypeRangeFloat64:
		cd.kllFloat64 = NewKLL[float64]()
	case shared.ColumnTypeRangeString:
		cd.kllStr = NewKLLString()
	case shared.ColumnTypeRangeBytes:
		cd.kllBytes = NewKLLBytes()
	}
	return cd
}

// addBlockRangeToColumn feeds the block's min and max encoded keys into the
// column's KLL sketch and records a blockRange entry.
func addBlockRangeToColumn(cd *rangeColumnData, mm *blockColMinMax, bid uint32) {
	// Append blockRange entry.
	cd.blocks = append(cd.blocks, blockRange{
		minKey:  mm.minKey,
		maxKey:  mm.maxKey,
		blockID: bid,
	})

	// Feed min and max into the KLL sketch.
	// Decoding back to typed values is required because KLL operates on typed
	// values (for correct quantile estimation) not raw encoded strings.
	// cd.colType is always a Range* type (guaranteed by newRangeColumnData).
	switch cd.colType {
	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if cd.kllInt64 != nil && len(mm.minKey) >= 8 && len(mm.maxKey) >= 8 {
			minV := int64(binary.LittleEndian.Uint64([]byte(mm.minKey))) //nolint:gosec
			maxV := int64(binary.LittleEndian.Uint64([]byte(mm.maxKey))) //nolint:gosec
			cd.kllInt64.Add(minV)
			cd.kllInt64.Add(maxV)
		}
	case shared.ColumnTypeRangeUint64:
		if cd.kllUint64 != nil && len(mm.minKey) >= 8 && len(mm.maxKey) >= 8 {
			cd.kllUint64.Add(binary.LittleEndian.Uint64([]byte(mm.minKey)))
			cd.kllUint64.Add(binary.LittleEndian.Uint64([]byte(mm.maxKey)))
		}
	case shared.ColumnTypeRangeFloat64:
		if cd.kllFloat64 != nil && len(mm.minKey) >= 8 && len(mm.maxKey) >= 8 {
			cd.kllFloat64.Add(math.Float64frombits(binary.LittleEndian.Uint64([]byte(mm.minKey))))
			cd.kllFloat64.Add(math.Float64frombits(binary.LittleEndian.Uint64([]byte(mm.maxKey))))
		}
	case shared.ColumnTypeRangeString:
		if cd.kllStr != nil {
			cd.kllStr.Add(mm.minKey)
			cd.kllStr.Add(mm.maxKey)
		}
	case shared.ColumnTypeRangeBytes:
		if cd.kllBytes != nil {
			cd.kllBytes.Add([]byte(mm.minKey))
			cd.kllBytes.Add([]byte(mm.maxKey))
		}
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
		binary.LittleEndian.PutUint64(
			tmp[:],
			uint64(val.Int), //nolint:gosec // safe: reinterpreting int64 bits as uint64
		)
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

// findBucket returns the bucket index for v given the sorted boundaries.
// Uses binary search: O(log n) comparisons instead of O(n).
// DUP-01: consolidated from findBucketInt64/Uint64/Float64/String — all had identical bodies.
func findBucket[T cmp.Ordered](v T, bounds []T) uint16 {
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
		return uint16(len(bounds) - 2) //nolint:gosec
	}
	return uint16(idx) //nolint:gosec // safe: idx is in range [0, len(bounds)-2] which fits uint16
}

func findBucketInt64(v int64, bounds []int64) uint16       { return findBucket(v, bounds) }
func findBucketUint64(v uint64, bounds []uint64) uint16    { return findBucket(v, bounds) }
func findBucketFloat64(v float64, bounds []float64) uint16 { return findBucket(v, bounds) }
func findBucketString(v string, bounds []string) uint16    { return findBucket(v, bounds) }

// truncateBoundaryKey returns s truncated to maxLen bytes.
func truncateBoundaryKey(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

// applyRangeBuckets finalizes KLL boundaries for every column and populates
// cd.values (the on-disk bucket map) using range-overlap: for each block's
// [min, max] interval, find all bucket boundaries the interval overlaps and
// add the block to each. This guarantees no false negatives (the invariant
// required for correct pruning) and produces bounded false positives.
func applyRangeBuckets(idx rangeIndex, nBuckets int) {
	for _, cd := range idx {
		applyRangeBucketsForColumn(cd, nBuckets)
	}
}

func applyRangeBucketsForColumn(cd *rangeColumnData, nBuckets int) {
	// NOTE-38: For low-cardinality columns, use exact-value index instead of KLL.
	// When a block's min == max for every block (single value per block), the exact
	// index maps each value directly to its block IDs with zero false positives.
	// For blocks where min != max, the block appears under both the min and max keys,
	// preserving the no-false-negatives invariant.
	if tryApplyExactValues(cd) {
		return
	}

	// cd.colType is always a Range* type (guaranteed by newRangeColumnData).
	switch cd.colType {
	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		if cd.kllInt64 != nil {
			applyOverlapInt64(cd, nBuckets)
		}
	case shared.ColumnTypeRangeUint64:
		if cd.kllUint64 != nil {
			applyOverlapUint64(cd, nBuckets)
		}
	case shared.ColumnTypeRangeFloat64:
		if cd.kllFloat64 != nil {
			applyOverlapFloat64(cd, nBuckets)
		}
	case shared.ColumnTypeRangeString:
		if cd.kllStr != nil {
			applyOverlapString(cd, nBuckets)
		}
	case shared.ColumnTypeRangeBytes:
		if cd.kllBytes != nil {
			applyOverlapBytes(cd, nBuckets)
		}
	}
}

// tryApplyExactValues checks if the column has low cardinality (≤ exactCardinalityThreshold
// distinct values across all blocks) and if so, builds an exact-value index that maps each
// distinct min/max boundary directly to the block IDs containing it. Returns true if the
// exact index was applied, false to fall through to the KLL path.
//
// The wire format is identical to the KLL path — the values map keys are just exact column
// values instead of KLL quantile boundaries. The reader's binary-search lookup works
// identically: it finds the entry whose key ≤ queryValue, which for an exact match
// returns precisely the right blocks.
//
// NOTE-38: exact-value index for low-cardinality columns.
// NOTE-38b / NOTE-42 / NOTE-43: the exact-value path is only safe when ALL blocks have
// min==max. When a block spans multiple values (minKey != maxKey), storing the block under
// only minKey and maxKey means intermediate values are not found by point lookup →
// false-negative block pruning / data loss. This applies to ALL range column types, not
// just strings: e.g., span:kind (uint64) with values {1,2,3,5} stored under keys 1 and 5
// would miss a query for kind=2. When any block has min!=max, fall through to the KLL
// overlap path which correctly assigns blocks to all overlapping bucket boundaries.
func tryApplyExactValues(cd *rangeColumnData) bool {
	if len(cd.blocks) == 0 {
		return false
	}

	// NOTE-43: For ALL range column types, when a block spans multiple values
	// (minKey != maxKey), storing the block under only minKey and maxKey means
	// intermediate values are missed by point lookup → false-negative pruning.
	// Bail out to the KLL overlap path which handles intervals correctly.
	for _, br := range cd.blocks {
		if br.minKey != br.maxKey {
			return false
		}
	}

	// Collect all distinct encoded values from block min/max keys.
	// All blocks have min==max at this point (enforced above).
	distinct := make(map[string]struct{})
	for _, br := range cd.blocks {
		distinct[br.minKey] = struct{}{}
		distinct[br.maxKey] = struct{}{}
		if len(distinct) > exactCardinalityThreshold {
			return false // too many distinct values — fall through to KLL
		}
	}

	// Low cardinality: build exact value → blockID mapping.
	// For each block, add it under both its min and max key. When min == max
	// (the common case for well-sorted data), the block appears under exactly
	// one key — giving zero false positives.
	cd.values = make(map[string][]uint32, len(distinct))
	for _, br := range cd.blocks {
		bk := truncateBoundaryKey(br.minKey, rangeBucketKeyMaxLen)
		cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		if br.maxKey != br.minKey {
			bk = truncateBoundaryKey(br.maxKey, rangeBucketKeyMaxLen)
			cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		}
	}

	// Populate typed boundaries so the reader can parse the wire format.
	// Use the distinct values as "boundaries" — the wire format is the same.
	switch cd.colType {
	case shared.ColumnTypeRangeString:
		sorted := make([]string, 0, len(distinct))
		for k := range distinct {
			sorted = append(sorted, k)
		}
		slices.Sort(sorted)
		cd.stringBounds = sorted
		cd.boundaries = nil
		cd.bucketMin = 0
		cd.bucketMax = 0

	case shared.ColumnTypeRangeBytes:
		sortedBytes := make([][]byte, 0, len(distinct))
		for k := range distinct {
			sortedBytes = append(sortedBytes, []byte(k))
		}
		slices.SortFunc(sortedBytes, bytes.Compare)
		cd.bytesBounds = sortedBytes
		cd.boundaries = nil
		cd.bucketMin = 0
		cd.bucketMax = 0

	case shared.ColumnTypeRangeInt64, shared.ColumnTypeRangeDuration:
		sorted := make([]int64, 0, len(distinct))
		for k := range distinct {
			if len(k) >= 8 {
				v := int64(binary.LittleEndian.Uint64([]byte(k))) //nolint:gosec
				sorted = append(sorted, v)
			}
		}
		slices.Sort(sorted)
		cd.boundaries = sorted
		if len(sorted) > 0 {
			cd.bucketMin = sorted[0]
			cd.bucketMax = sorted[len(sorted)-1]
		}

	case shared.ColumnTypeRangeUint64:
		// BUG-02: sort as uint64 (unsigned order) then reinterpret bits as int64 for wire format.
		// Sorting as int64 caused values >= 2^63 to appear negative, reversing their order.
		sortedU := make([]uint64, 0, len(distinct))
		for k := range distinct {
			if len(k) >= 8 {
				sortedU = append(sortedU, binary.LittleEndian.Uint64([]byte(k)))
			}
		}
		slices.Sort(sortedU)
		cd.boundaries = make([]int64, len(sortedU))
		for i, v := range sortedU {
			cd.boundaries[i] = int64(v) //nolint:gosec // bit-reinterpretation for wire format
		}
		if len(sortedU) > 0 {
			cd.bucketMin = int64(sortedU[0])              //nolint:gosec
			cd.bucketMax = int64(sortedU[len(sortedU)-1]) //nolint:gosec
		}

	case shared.ColumnTypeRangeFloat64:
		sorted := make([]float64, 0, len(distinct))
		for k := range distinct {
			if len(k) >= 8 {
				sorted = append(sorted, math.Float64frombits(binary.LittleEndian.Uint64([]byte(k))))
			}
		}
		slices.SortFunc(sorted, func(a, b float64) int {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		})
		cd.float64Bounds = sorted
		cd.boundaries = make([]int64, len(sorted))
		for i, v := range sorted {
			cd.boundaries[i] = int64(math.Float64bits(v)) //nolint:gosec // safe: storing float64 bits as int64
		}
		if len(sorted) > 0 {
			cd.bucketMin = int64(math.Float64bits(sorted[0]))             //nolint:gosec
			cd.bucketMax = int64(math.Float64bits(sorted[len(sorted)-1])) //nolint:gosec
		}
	}

	return true
}

func applyOverlapInt64(cd *rangeColumnData, nBuckets int) {
	bounds := slices.Compact(cd.kllInt64.Boundaries(nBuckets))
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeInt64
	cd.boundaries = make([]int64, len(bounds))
	copy(cd.boundaries, bounds)
	cd.bucketMin = bounds[0]
	cd.bucketMax = bounds[len(bounds)-1]

	cd.values = make(map[string][]uint32, len(bounds))
	for _, br := range cd.blocks {
		if len(br.minKey) < 8 || len(br.maxKey) < 8 {
			continue
		}
		minV := int64(binary.LittleEndian.Uint64([]byte(br.minKey))) //nolint:gosec
		maxV := int64(binary.LittleEndian.Uint64([]byte(br.maxKey))) //nolint:gosec
		if minV > maxV {
			minV, maxV = maxV, minV // guard against string-order vs numeric-order inversion
		}
		lo := int(findBucketInt64(minV, bounds))
		hi := int(findBucketInt64(maxV, bounds))
		for i := lo; i <= hi; i++ {
			bk := encodeInt64BoundaryKey(bounds[i])
			cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		}
	}
}

func applyOverlapUint64(cd *rangeColumnData, nBuckets int) {
	bounds := slices.Compact(cd.kllUint64.Boundaries(nBuckets))
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeUint64
	cd.boundaries = make([]int64, len(bounds))
	for i, b := range bounds {
		cd.boundaries[i] = int64(b) //nolint:gosec // safe: storing uint64 bits as int64 for wire format
	}
	cd.bucketMin = int64(bounds[0])             //nolint:gosec
	cd.bucketMax = int64(bounds[len(bounds)-1]) //nolint:gosec

	cd.values = make(map[string][]uint32, len(bounds))
	for _, br := range cd.blocks {
		if len(br.minKey) < 8 || len(br.maxKey) < 8 {
			continue
		}
		minV := binary.LittleEndian.Uint64([]byte(br.minKey))
		maxV := binary.LittleEndian.Uint64([]byte(br.maxKey))
		if minV > maxV {
			minV, maxV = maxV, minV
		}
		lo := int(findBucketUint64(minV, bounds))
		hi := int(findBucketUint64(maxV, bounds))
		for i := lo; i <= hi; i++ {
			bk := encodeUint64BoundaryKey(bounds[i])
			cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		}
	}
}

func applyOverlapFloat64(cd *rangeColumnData, nBuckets int) {
	bounds := slices.Compact(cd.kllFloat64.Boundaries(nBuckets))
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeFloat64
	cd.float64Bounds = bounds
	cd.boundaries = make([]int64, len(bounds))
	for i, b := range bounds {
		cd.boundaries[i] = int64(math.Float64bits(b)) //nolint:gosec
	}
	cd.bucketMin = int64(math.Float64bits(bounds[0]))             //nolint:gosec
	cd.bucketMax = int64(math.Float64bits(bounds[len(bounds)-1])) //nolint:gosec

	cd.values = make(map[string][]uint32, len(bounds))
	for _, br := range cd.blocks {
		if len(br.minKey) < 8 || len(br.maxKey) < 8 {
			continue
		}
		minV := math.Float64frombits(binary.LittleEndian.Uint64([]byte(br.minKey)))
		maxV := math.Float64frombits(binary.LittleEndian.Uint64([]byte(br.maxKey)))
		if math.IsNaN(minV) || math.IsNaN(maxV) {
			continue // skip blocks with undefined numeric ranges
		}
		if minV > maxV {
			minV, maxV = maxV, minV
		}
		lo := int(findBucketFloat64(minV, bounds))
		hi := int(findBucketFloat64(maxV, bounds))
		for i := lo; i <= hi; i++ {
			bk := encodeFloat64BoundaryKey(bounds[i])
			cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		}
	}
}

func applyOverlapString(cd *rangeColumnData, nBuckets int) {
	bounds := slices.Compact(cd.kllStr.Boundaries(nBuckets))
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeString
	cd.stringBounds = bounds
	cd.boundaries = nil
	cd.bucketMin = 0
	cd.bucketMax = 0

	cd.values = make(map[string][]uint32, len(bounds))
	for _, br := range cd.blocks {
		lo := int(findBucketString(br.minKey, bounds))
		hi := int(findBucketString(br.maxKey, bounds))
		if lo > hi {
			lo, hi = hi, lo // guard: string min/max are stored by string comparison
		}
		for i := lo; i <= hi; i++ {
			bk := truncateBoundaryKey(bounds[i], rangeBucketKeyMaxLen)
			cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		}
	}
}

func applyOverlapBytes(cd *rangeColumnData, nBuckets int) {
	rawBounds := cd.kllBytes.Boundaries(nBuckets)
	if len(rawBounds) < 2 {
		return
	}
	// Deduplicate consecutive equal bounds.
	bounds := slices.CompactFunc(rawBounds, bytes.Equal)
	if len(bounds) < 2 {
		return
	}
	cd.colType = shared.ColumnTypeRangeBytes
	cd.bytesBounds = bounds
	cd.boundaries = nil
	cd.bucketMin = 0
	cd.bucketMax = 0

	// Convert bounds once to avoid repeated []byte→string in the hot loop.
	strBounds := make([]string, len(bounds))
	for i, b := range bounds {
		strBounds[i] = string(b)
	}

	cd.values = make(map[string][]uint32, len(bounds))
	for _, br := range cd.blocks {
		lo := int(findBucketString(br.minKey, strBounds))
		hi := int(findBucketString(br.maxKey, strBounds))
		if lo > hi {
			lo, hi = hi, lo
		}
		for i := lo; i <= hi; i++ {
			bk := truncateBoundaryKey(strBounds[i], rangeBucketKeyMaxLen)
			cd.values[bk] = appendUniqueBlockID(cd.values[bk], br.blockID)
		}
	}
}

// appendUniqueBlockID appends bid to ids if it is not already present.
// Blocks are processed with monotonically increasing IDs, so checking only the
// last element is sufficient for deduplication and is O(1) instead of O(n).
func appendUniqueBlockID(ids []uint32, bid uint32) []uint32 {
	if len(ids) > 0 && ids[len(ids)-1] == bid {
		return ids
	}
	return append(ids, bid)
}

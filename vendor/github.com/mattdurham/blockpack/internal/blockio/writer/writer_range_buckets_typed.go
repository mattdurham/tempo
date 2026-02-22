package writer

import (
	"encoding/binary"

	ondisk "github.com/mattdurham/blockpack/internal/types"
)

// writer_range_buckets_typed.go contains KLL string/bytes processing helpers for computeRangeBuckets.

// processKLLStringByteSketches handles string and bytes columns using KLL[string] sketches.
// Both column types share kllStringSketches and rangeStringBlockMinMax.
func (w *Writer) processKLLStringByteSketches() error {
	for columnName, sketch := range w.kllStringSketches {
		typ, ok := w.autoDedicatedTypes[columnName]
		if !ok {
			continue
		}

		var isString bool
		switch typ {
		case ColumnTypeString, ColumnTypeRangeString:
			isString = true
		case ColumnTypeBytes, ColumnTypeRangeBytes:
			isString = false
		default:
			continue
		}

		// Use quantile-based range bucketing.
		boundaries := sketch.ComputeQuantiles(100) // []string
		if len(boundaries) == 0 {
			continue
		}

		// Count distinct boundary values to detect low-cardinality columns.
		// For low-cardinality columns (e.g., service names with 2-5 unique values),
		// ComputeQuantiles returns many repeated boundaries. In that case, keep the
		// exact-match dedicated index entries instead of upgrading to range bucketing.
		uniqueBoundaries := 1
		for i := 1; i < len(boundaries); i++ {
			if boundaries[i] != boundaries[i-1] {
				uniqueBoundaries++
			}
		}
		const minValuesForBucketing = ondisk.MinValuesForBucketing
		if uniqueBoundaries < minValuesForBucketing {
			// Low-cardinality: keep the column as-is (exact-match entries already in dedicatedIndex).
			continue
		}

		var rangeType ColumnType
		if isString {
			rangeType = ColumnTypeRangeString
		} else {
			rangeType = ColumnTypeRangeBytes
		}
		w.autoDedicatedTypes[columnName] = rangeType

		if rangeType == ColumnTypeRangeString {
			w.rangeMetadata.SetStringBoundaries(columnName, boundaries)
		} else {
			bytesBoundaries := make([][]byte, len(boundaries))
			for i, s := range boundaries {
				bytesBoundaries[i] = []byte(s)
			}
			w.rangeMetadata.SetBytesBoundaries(columnName, bytesBoundaries)
		}

		int64Boundaries := make([]int64, len(boundaries))
		for i, s := range boundaries {
			var buf [8]byte
			copy(buf[:], s)
			int64Boundaries[i] = int64(binary.LittleEndian.Uint64(buf[:])) //nolint:gosec
		}
		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        int64Boundaries[0],
			Max:        int64Boundaries[len(int64Boundaries)-1],
			Boundaries: int64Boundaries,
			ColumnType: rangeType,
		}

		bucketToBlocks := make(map[uint16]map[int]struct{})
		for blockID, minMax := range w.rangeStringBlockMinMax[columnName] {
			minBucket := ondisk.GetBucketIDString(minMax[0], boundaries)
			maxBucket := ondisk.GetBucketIDString(minMax[1], boundaries)
			for b := minBucket; b <= maxBucket; b++ {
				if bucketToBlocks[b] == nil {
					bucketToBlocks[b] = make(map[int]struct{})
				}
				bucketToBlocks[b][blockID] = struct{}{}
			}
		}

		// Replace any existing exact-match entries with bucket entries.
		// When a column upgrades from ColumnTypeString/ColumnTypeBytes to a range type,
		// the old exact-match keys would cause a type mismatch during serialization.
		w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, rangeType)
			w.dedicatedIndex[columnName][bucketKey.Encode()] = blocks
		}
	}
	return nil
}

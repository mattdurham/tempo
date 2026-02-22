package writer

import (
	"fmt"
	"math"

	ondisk "github.com/mattdurham/blockpack/internal/types"
)

// writer_range_buckets.go contains computeRangeBuckets and int64/float64-KLL processing helpers.

// computeRangeBuckets processes KLL sketches and per-block min/max to build bucket indexes.
// All range columns are always bucketed; no raw values are stored.
func (w *Writer) computeRangeBuckets() error {
	if err := w.processPrecomputedBoundaries(); err != nil {
		return err
	}
	if err := w.processKLLInt64Sketches(); err != nil {
		return err
	}
	if err := w.processKLLFloat64Sketches(); err != nil {
		return err
	}
	return w.processKLLStringByteSketches()
}

// processPrecomputedBoundaries handles columns with externally-supplied bucket boundaries
// (two-pass conversion mode). The bucket index is already built during recordRangeBucketValue.
func (w *Writer) processPrecomputedBoundaries() error {
	for columnName, boundaries := range w.precomputedBoundaries {
		typ, ok := w.autoDedicatedTypes[columnName]
		if !ok {
			return fmt.Errorf("range bucket column %s not in auto-detected types", columnName)
		}

		if len(boundaries) < 2 {
			continue // malformed/empty boundaries slice — skip to avoid index panic
		}

		bucketToBlocks := w.rangeBucketIndex[columnName]
		if bucketToBlocks == nil {
			continue // column configured but no values seen
		}

		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        boundaries[0],
			Max:        boundaries[len(boundaries)-1],
			Boundaries: boundaries,
			ColumnType: typ,
		}

		if w.dedicatedIndex[columnName] == nil {
			w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		}
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, typ)
			w.dedicatedIndex[columnName][bucketKey.Encode()] = blocks
		}
	}
	return nil
}

// processKLLInt64Sketches handles int64/uint64 columns using KLL sketches.
func (w *Writer) processKLLInt64Sketches() error {
	for columnName, sketch := range w.kllSketches {
		typ, ok := w.autoDedicatedTypes[columnName]
		if !ok {
			return fmt.Errorf("range bucket column %s not in auto-detected types", columnName)
		}

		boundaries := sketch.ComputeQuantiles(100)
		if len(boundaries) == 0 {
			continue
		}

		rangeType := typ
		switch typ {
		case ColumnTypeInt64:
			rangeType = ColumnTypeRangeInt64
		case ColumnTypeUint64:
			rangeType = ColumnTypeRangeUint64
		}
		w.autoDedicatedTypes[columnName] = rangeType

		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        boundaries[0],
			Max:        boundaries[len(boundaries)-1],
			Boundaries: boundaries,
			ColumnType: rangeType,
		}

		blockMinMax := w.rangeBlockMinMax[columnName]
		bucketToBlocks := make(map[uint16]map[int]struct{})
		for blockID, minMax := range blockMinMax {
			minBucket := GetBucketID(minMax[0], boundaries)
			maxBucket := GetBucketID(minMax[1], boundaries)
			for b := minBucket; b <= maxBucket; b++ {
				if bucketToBlocks[b] == nil {
					bucketToBlocks[b] = make(map[int]struct{})
				}
				bucketToBlocks[b][blockID] = struct{}{}
			}
		}

		if w.dedicatedIndex[columnName] == nil {
			w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		}
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, rangeType)
			w.dedicatedIndex[columnName][bucketKey.Encode()] = blocks
		}
	}
	return nil
}

// processKLLFloat64Sketches handles float64 columns using KLL[float64] sketches.
func (w *Writer) processKLLFloat64Sketches() error {
	for columnName, sketch := range w.kllFloat64Sketches {
		boundaries := sketch.ComputeQuantiles(100) // []float64
		if len(boundaries) == 0 {
			continue
		}

		rangeType := ColumnTypeRangeFloat64
		w.autoDedicatedTypes[columnName] = rangeType
		w.rangeMetadata.SetFloat64Boundaries(columnName, boundaries)

		int64Boundaries := make([]int64, len(boundaries))
		for i, v := range boundaries {
			int64Boundaries[i] = int64(math.Float64bits(v)) //nolint:gosec
		}
		w.rangeBucketMeta[columnName] = &RangeBucketMetadata{
			Min:        int64Boundaries[0],
			Max:        int64Boundaries[len(int64Boundaries)-1],
			Boundaries: int64Boundaries,
			ColumnType: rangeType,
		}

		bucketToBlocks := make(map[uint16]map[int]struct{})
		for blockID, minMax := range w.rangeFloat64BlockMinMax[columnName] {
			minBucket := ondisk.GetBucketIDFloat64(minMax[0], boundaries)
			maxBucket := ondisk.GetBucketIDFloat64(minMax[1], boundaries)
			for b := minBucket; b <= maxBucket; b++ {
				if bucketToBlocks[b] == nil {
					bucketToBlocks[b] = make(map[int]struct{})
				}
				bucketToBlocks[b][blockID] = struct{}{}
			}
		}

		if w.dedicatedIndex[columnName] == nil {
			w.dedicatedIndex[columnName] = make(map[string]map[int]struct{})
		}
		for bucketID, blocks := range bucketToBlocks {
			bucketKey := RangeBucketValueKey(bucketID, rangeType)
			w.dedicatedIndex[columnName][bucketKey.Encode()] = blocks
		}
	}
	return nil
}

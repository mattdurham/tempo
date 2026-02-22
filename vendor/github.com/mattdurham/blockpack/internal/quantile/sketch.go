// Package quantile provides quantile sketching algorithms.
package quantile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// QuantileSketch implements a DDSketch (Distributed Data Sketch) for approximate
// quantile computation. It uses logarithmic bucketing to provide relative accuracy
// guarantees with bounded memory usage.
//
// DDSketch properties:
// - Mergeable: Multiple sketches can be combined
// - Relative accuracy: Error is proportional to the quantile value
// - Bounded memory: Number of buckets is logarithmic in the data range
type QuantileSketch struct { //nolint:revive
	// buckets maps bucket index to count
	buckets map[int]int64

	// gamma is the relative accuracy parameter (1 + alpha)
	// Smaller gamma means higher accuracy but more buckets
	gamma float64

	// logGamma is the precomputed log(gamma) for efficiency
	logGamma float64

	// count is the total number of values added
	count int64

	// min and max track the exact minimum and maximum values
	min float64
	max float64

	// zeroCount tracks the count of exact zero values
	zeroCount int64
}

// NewQuantileSketch creates a new quantile sketch with the given relative accuracy.
// The accuracy parameter alpha controls the error bound: the returned quantile
// will be within (1 ± alpha) * true_quantile.
//
// Typical accuracy values:
// - 0.01 (1%) for high accuracy
// - 0.05 (5%) for medium accuracy
// - 0.1 (10%) for low accuracy
//
// Memory usage grows as O(log(max/min) / alpha).
func NewQuantileSketch(accuracy float64) *QuantileSketch {
	if accuracy <= 0 || accuracy >= 1 {
		// Default to 1% accuracy if invalid
		accuracy = 0.01
	}

	gamma := 1 + accuracy

	return &QuantileSketch{
		gamma:     gamma,
		logGamma:  math.Log(gamma),
		buckets:   make(map[int]int64),
		count:     0,
		min:       math.Inf(1),
		max:       math.Inf(-1),
		zeroCount: 0,
	}
}

// Add inserts a value into the sketch.
func (s *QuantileSketch) Add(value float64) {
	if s == nil {
		return
	}
	s.count++

	// Handle special cases
	if value == 0 {
		s.zeroCount++
		s.min = 0
		if s.max < 0 {
			s.max = 0
		}
		return
	}

	// Update min/max
	if value < s.min {
		s.min = value
	}
	if value > s.max {
		s.max = value
	}

	// Add to appropriate bucket
	idx := s.bucketIndex(value)
	s.buckets[idx]++
}

// Quantile returns the approximate value at the given quantile q (0 <= q <= 1).
// For example, q=0.5 returns the median, q=0.95 returns the 95th percentile.
func (s *QuantileSketch) Quantile(q float64) float64 {
	if s.count == 0 {
		return 0
	}

	if q <= 0 {
		return s.min
	}
	if q >= 1 {
		return s.max
	}

	// Calculate the target rank (0-indexed)
	targetRank := int64(math.Ceil(q * float64(s.count)))

	// Handle edge cases
	if targetRank <= s.zeroCount {
		return 0
	}

	// Find the bucket containing the target rank
	cumCount := s.zeroCount

	// We need to iterate through buckets in sorted order
	// Find the range of bucket indices
	minIdx := math.MaxInt
	maxIdx := math.MinInt
	for idx := range s.buckets {
		if idx < minIdx {
			minIdx = idx
		}
		if idx > maxIdx {
			maxIdx = idx
		}
	}

	// Iterate through buckets in order
	for idx := minIdx; idx <= maxIdx; idx++ {
		if count, exists := s.buckets[idx]; exists {
			cumCount += count
			if cumCount >= targetRank {
				// Return the representative value for this bucket
				return s.bucketValue(idx)
			}
		}
	}

	// Should not reach here, but return max as fallback
	return s.max
}

// Merge combines another sketch into this one.
// Both sketches must have the same gamma value.
func (s *QuantileSketch) Merge(other *QuantileSketch) {
	if other == nil || other.count == 0 {
		return
	}

	// For simplicity, we'll allow merging sketches with different gamma values
	// but this may reduce accuracy. In production, you might want to enforce
	// that gamma values match.

	// Merge counts
	s.count += other.count
	s.zeroCount += other.zeroCount

	// Update min/max
	if other.min < s.min {
		s.min = other.min
	}
	if other.max > s.max {
		s.max = other.max
	}

	// Merge buckets
	for idx, count := range other.buckets {
		s.buckets[idx] += count
	}
}

// Clone creates a deep copy of the sketch.
func (s *QuantileSketch) Clone() *QuantileSketch {
	newSketch := &QuantileSketch{
		gamma:     s.gamma,
		logGamma:  s.logGamma,
		buckets:   make(map[int]int64, len(s.buckets)),
		count:     s.count,
		min:       s.min,
		max:       s.max,
		zeroCount: s.zeroCount,
	}

	// Deep copy the buckets map
	for idx, count := range s.buckets {
		newSketch.buckets[idx] = count
	}

	return newSketch
}

// Count returns the total number of values added to the sketch.
func (s *QuantileSketch) Count() int64 {
	return s.count
}

// Min returns the minimum value seen.
func (s *QuantileSketch) Min() float64 {
	if s.count == 0 {
		return 0
	}
	return s.min
}

// Max returns the maximum value seen.
func (s *QuantileSketch) Max() float64 {
	if s.count == 0 {
		return 0
	}
	return s.max
}

// bucketIndex computes the bucket index for a given value.
// For positive values: floor(log(value) / log(gamma))
// For negative values: -ceil(log(-value) / log(gamma))
func (s *QuantileSketch) bucketIndex(value float64) int {
	if value > 0 {
		return int(math.Floor(math.Log(value) / s.logGamma))
	}
	// Negative values
	return -int(math.Ceil(math.Log(-value) / s.logGamma))
}

// bucketValue returns the representative value for a bucket index.
// This is the geometric mean of the bucket's lower and upper bounds.
// For positive buckets: gamma^(index + 0.5)
// For negative buckets: -gamma^(-index + 0.5)
func (s *QuantileSketch) bucketValue(index int) float64 {
	if index >= 0 {
		// Positive bucket: return gamma^(index + 0.5)
		return math.Pow(s.gamma, float64(index)+0.5)
	}
	// Negative bucket: return -gamma^(-index + 0.5)
	return -math.Pow(s.gamma, float64(-index)+0.5)
}

// Serialize encodes the sketch to bytes for persistence.
// Format:
//   - gamma (8 bytes, float64)
//   - count (8 bytes, int64)
//   - min (8 bytes, float64)
//   - max (8 bytes, float64)
//   - zeroCount (8 bytes, int64)
//   - bucketCount (4 bytes, uint32)
//   - For each bucket:
//   - index (4 bytes, int32)
//   - count (8 bytes, int64)
func (s *QuantileSketch) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Write header fields
	if err := binary.Write(&buf, binary.LittleEndian, s.gamma); err != nil {
		return nil, fmt.Errorf("write gamma: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.count); err != nil {
		return nil, fmt.Errorf("write count: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.min); err != nil {
		return nil, fmt.Errorf("write min: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.max); err != nil {
		return nil, fmt.Errorf("write max: %w", err)
	}
	if err := binary.Write(&buf, binary.LittleEndian, s.zeroCount); err != nil {
		return nil, fmt.Errorf("write zeroCount: %w", err)
	}

	// Write bucket count
	bucketCount := uint32(len(s.buckets)) //nolint:gosec
	if err := binary.Write(&buf, binary.LittleEndian, bucketCount); err != nil {
		return nil, fmt.Errorf("write bucketCount: %w", err)
	}

	// Collect and sort bucket indices for deterministic serialization
	// (Go map iteration order is randomized)
	indices := make([]int, 0, len(s.buckets))
	for idx := range s.buckets {
		indices = append(indices, idx)
	}
	sort.Ints(indices)

	// Write each bucket in sorted order
	for _, idx := range indices {
		count := s.buckets[idx]
		if err := binary.Write(&buf, binary.LittleEndian, int32(idx)); err != nil { //nolint:gosec // Reviewed and acceptable
			return nil, fmt.Errorf("write bucket index: %w", err)
		}
		if err := binary.Write(&buf, binary.LittleEndian, count); err != nil {
			return nil, fmt.Errorf("write bucket count: %w", err)
		}
	}

	return buf.Bytes(), nil
}

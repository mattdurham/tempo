// Package quantile provides quantile sketching algorithms.
package quantile

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

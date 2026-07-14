package cube

// NOTE: SPEC-CUBE-019 — Log2Bucketize/BucketIndex/BucketMax/Log2QuantileFromBuckets are a
// byte-for-byte port of tempo's pkg/traceql.Log2Bucketize/Log2QuantileWithBucket
// (tempo/pkg/traceql/engine_metrics.go:2135-2145, 2154-2219), kept dependency-free by design
// (cube must not import tempo). DO NOT reuse or confuse this with
// internal/modules/executor/intrinsic_helpers.go's pow2Floor: that function floors instead of
// ceilings and converts nanoseconds-to-seconds in the opposite order — a pre-existing, recorded
// divergence between blockpack's two histogram implementations, not something this package's
// bucketing may borrow from.

import (
	"math"
	"math/bits"
)

// BucketCount is the fixed number of dense histogram slots per aggAttr. Slot 0 is always zero
// (the smallest possible non-excluded bucket boundary is 2^1=2, matching Log2Bucketize's own
// v<2 exclusion); slots 1-63 hold counts of samples whose Log2Bucketize result equals 2^k.
const BucketCount = 64

// Log2Bucketize returns the ceiling power-of-two boundary for v, or -1 when v < 2 (the sample is
// excluded from any histogram entirely — ported byte-for-byte from tempo's
// pkg/traceql.Log2Bucketize) or when v >= 2^63+1 (the ceiling boundary would be 2^64, which
// overflows uint64's 1<<64 to 0 per Go's shift semantics rather than any in-range value — a
// pathological/corrupted input, unreachable via any real span duration or count, excluded via the
// same -1 sentinel as the v<2 case rather than silently wrapping into an out-of-bounds bucket).
func Log2Bucketize(v uint64) float64 {
	if v < 2 {
		return -1
	}

	shift := 64 - bits.LeadingZeros64(v-1)
	if shift >= 64 {
		return -1
	}

	return float64(uint64(1) << shift)
}

// BucketIndex returns the dense-array slot (1-63) for a boundary value produced by Log2Bucketize.
// boundary must be a power of two >= 2 (i.e. a real Log2Bucketize output, never the -1 sentinel —
// callers must check for -1 BEFORE calling BucketIndex).
func BucketIndex(boundary float64) int {
	return bits.TrailingZeros64(uint64(boundary))
}

// BucketMax is BucketIndex's inverse: BucketMax(BucketIndex(x)) == x for every valid boundary x.
// k must be in [0,63] (a valid bucket slot index) — callers never pass a negative or out-of-range
// index.
func BucketMax(k int) float64 {
	//nolint:gosec // G115: k is always in [0,63] (a valid bucket slot index), never negative.
	return float64(uint64(1) << uint(k))
}

// Log2QuantileFromBuckets ports tempo's Log2QuantileWithBucket exactly
// (engine_metrics.go:2154-2219): walks buckets[0:64] in ascending index order accumulating counts
// until ceil(p*total) samples are consumed (minimum 1), then interpolates exponentially between
// the containing bucket's Max and the EXACT PRIOR INDEX's boundary (index k-1, regardless of
// whether bucket k-1 itself had a nonzero count — tempo's own algorithm uses the literal previous
// slice element; cube's dense 64-slot array is already gap-free by construction, matching tempo's
// own dense HistogramBucket slice shape). Returns (0, -1) when p is invalid (NaN, <0, >1) or when
// every bucket is empty.
func Log2QuantileFromBuckets(p float64, buckets [BucketCount]uint64) (value float64, bucketIdx int) {
	if math.IsNaN(p) || p < 0 || p > 1 {
		return 0, -1
	}

	var totalCount uint64
	for _, c := range buckets {
		totalCount += c
	}
	if totalCount == 0 {
		return 0, -1
	}

	// Maximum amount of samples to include. We round up to better handle percentiles on low
	// sample counts (<100).
	maxSamples := uint64(math.Ceil(p * float64(totalCount)))
	if maxSamples == 0 {
		// We have to read at least one sample.
		maxSamples = 1
	}

	// Find the bucket where the percentile falls in.
	var total uint64
	bucket := 0
	for i, c := range buckets {
		bucket = i

		// If we can't fully consume the samples in this bucket then we are done.
		if total+c > maxSamples {
			break
		}

		// Consume all samples in this bucket.
		total += c

		// p100 or happen to read the exact number of samples. Quantile is the max range for the
		// bucket. No reason to enter interpolation below.
		if total == maxSamples {
			return BucketMax(bucket), bucket
		}
	}

	// Fraction to interpolate between buckets, sample-count wise. 0.5 means halfway.
	interp := float64(maxSamples-total) / float64(buckets[bucket])

	// Exponential interpolation between buckets. The current bucket represents the maximum
	// value.
	maxV := math.Log2(BucketMax(bucket))
	var minV float64
	if bucket > 0 {
		// Prior bucket represents the min.
		minV = math.Log2(BucketMax(bucket - 1))
	} else {
		// There is no prior bucket, assume powers of 2.
		minV = maxV - 1
	}
	return math.Pow(2, minV+(maxV-minV)*interp), bucket
}

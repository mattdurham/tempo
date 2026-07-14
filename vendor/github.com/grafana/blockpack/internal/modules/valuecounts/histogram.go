package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md/NOTES.md.

import "encoding/binary"

// DurationBucketBoundsMillis are the 16 fixed bucket lower-boundaries for the span:duration
// VCNT histogram (#205), in milliseconds. Hardcoded, not derived from any formula or shared
// with cube's own Log2Bucketize scheme (that reuse was explicitly considered and rejected —
// cube's per-cell histogram serves a different consumer, real histogram_over_time()/quantile
// ANSWERS, and reusing its exact boundaries wasted almost the entire bucket budget crossing
// from nanoseconds into low seconds, leaving no resolution for common thresholds like
// duration > 10s. See NOTE-VC-022).
//
// Resolution is deliberately concentrated below 1 hour (13 of the first 15 boundaries land
// under 1 minute) since that's where real span durations and real query thresholds
// concentrate; the LAST bucket (index 15, 3_600_000ms = 1hr) is the sole open-ended catch-all
// for everything >= 1hr — there is no separate tail bucket beyond it. Never drops a record:
// bucket 0's boundary is literally 0, so every non-negative duration always matches at least
// one bucket by construction — no v<2-style exclusion, no floor/clamp wrapper needed.
//
// SPEC-VC-8.
var DurationBucketBoundsMillis = [16]uint64{
	0, 1, 5, 10, 50, 100, 500,
	1_000, 5_000, 10_000, 30_000,
	60_000, 300_000, 600_000,
	1_800_000, 3_600_000,
}

// BucketIndex returns the index (0..15) of the ONE bucket valueMillis belongs to: the largest
// index i such that DurationBucketBoundsMillis[i] <= valueMillis. This is a discrete/density
// assignment — a caller incrementing a histogram increments exactly bucket[BucketIndex(v)]'s
// counter and no other bucket's, for every sample. It is NOT a cumulative/CDF assignment (which
// would increment every bucket <= v, or every bucket >= v). For example, a 12ms duration maps to
// bucket 3 only (10ms <= 12 < 50ms) — buckets 0-2 and 4-15 are unaffected by that sample.
// Index 15 (the 1hr boundary) naturally catches everything at or above it by construction of the
// floor rule — no clamp/ceiling special-case is needed at either end. Bucket 0 (boundary 0)
// already covers every non-negative value down to zero, so the never-drop requirement is
// satisfied by the array's own shape, not by extra guard code.
//
// SPEC-VC-8.
func BucketIndex(valueMillis uint64) int {
	idx := 0
	for i, b := range DurationBucketBoundsMillis {
		if b > valueMillis {
			break
		}
		idx = i
	}
	return idx
}

// boundaryToIndex is the exact-match inverse of DurationBucketBoundsMillis[i]: given a decoded
// boundary value, it returns the bucket index it names, or -1 if the value is not one of the 16
// known boundaries (a corrupt/unrecognized record — callers skip it rather than panicking,
// per SPEC-ROOT-001's no-panic rule).
func boundaryToIndex(boundaryMillis uint64) int {
	for i, b := range DurationBucketBoundsMillis {
		if b == boundaryMillis {
			return i
		}
	}
	return -1
}

// HistogramColumnName returns the synthetic VCNT column name a duration histogram's records are
// stored under for the given real column name (e.g. "span:duration" -> "span:duration#hist").
// The "#" suffix is collision-free against every existing VCNT column name (no existing column
// name contains "#"; OTLP attribute keys structurally cannot either).
//
// SPEC-VC-8.
func HistogramColumnName(column string) string {
	return column + "#hist"
}

// EncodeHistogramValue is the canonical Record.Value encoding for a histogram bucket's boundary:
// a fixed 8-byte little-endian uint64. Deliberately not valueindex.CanonicalValue — these 16
// synthetic values are never compared against or sorted alongside any other column's values,
// since ColumnName alone already scopes every VCNT read. Exported so the root package (vcnt.go)
// can re-export it for tempo's writer (#205).
func EncodeHistogramValue(boundaryMillis uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, boundaryMillis)
	return buf
}

// decodeHistogramValue is the exact inverse of EncodeHistogramValue. It returns ok=false (rather
// than panicking) for any value that isn't exactly 8 bytes — a corrupt/unrecognized record,
// per SPEC-ROOT-001's no-panic rule.
func decodeHistogramValue(value []byte) (uint64, bool) {
	if len(value) != 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(value), true
}

// DurationHistogram is the net live per-bucket span count for a column's duration histogram over
// a query window, index-aligned with DurationBucketBoundsMillis.
//
// SPEC-VC-8.
type DurationHistogram struct {
	// Counts holds the net live count per bucket. Never negative — a bucket whose net summed
	// Count is <= 0 reads as 0, mirroring sumLiveValues' liveness rule (NOTE-VC-001/004).
	Counts [16]int64
	// Covered reports whether any histogram record was found for the column in the window at
	// all, regardless of whether every bucket ended up net-zero (mirrors ColumnTotalInRange's
	// own Covered semantics exactly).
	Covered bool
}

// DurationHistogramInRange sums, per bucket, the net live span count for column's histogram in
// [minTS, maxTS] — the same shape as ColumnTotalInRange/sumLiveValues, applied to the 16 fixed
// synthetic values under HistogramColumnName(column) instead of column's own real values.
// Returns Covered=false when no histogram record exists for column in the window at all.
//
// SPEC-VC-8.
func DurationHistogramInRange(
	data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64,
) (DurationHistogram, error) {
	recs, err := DecodeTimeRange(data, dir, minTS, maxTS)
	if err != nil {
		return DurationHistogram{}, err
	}
	histColumn := HistogramColumnName(column)

	var sums [16]int64
	var covered bool
	for i := range recs {
		r := &recs[i]
		if r.ColumnName != histColumn {
			continue
		}
		covered = true
		boundary, ok := decodeHistogramValue(r.Value)
		if !ok {
			continue
		}
		idx := boundaryToIndex(boundary)
		if idx < 0 {
			continue
		}
		sums[idx] += r.Count
	}

	var out DurationHistogram
	out.Covered = covered
	for i, s := range sums {
		if s > 0 {
			out.Counts[i] = s
		}
	}
	return out, nil
}

// TimeCompareOp identifies the comparison a duration-range predicate uses against a fixed
// threshold, for EstimateThreshold. Exported so callers outside this package (e.g.
// internal/modules/queryplan's cost-function adapter) can construct one without depending on any
// other package's own, unrelated comparison-op type.
type TimeCompareOp uint8

// TimeCompareOp values. OpEQ is always unestimable by EstimateThreshold (finer than a bucket
// width); the other four drive the over-estimate range-summing rules documented on
// EstimateThreshold.
const (
	OpEQ TimeCompareOp = iota
	OpGT
	OpGTE
	OpLT
	OpLTE
)

// EstimateThreshold approximates the span count matching `duration <op> thresholdMillis` over
// h's window. Per the over-estimate rule (#205): the bucket straddling thresholdMillis is
// counted in full for GT/GTE/LT/LTE, since a value inside that bucket could be on either side of
// the threshold and over-estimating only ever costs a missed I/O-reduction opportunity, never a
// wrong answer (the downstream block-scan/value-index path always re-verifies). Equality is
// genuinely unestimable at any resolution finer than a bucket width, so it always returns
// known=false.
//
// SPEC-VC-8.
func (h DurationHistogram) EstimateThreshold(op TimeCompareOp, thresholdMillis uint64) (count int64, known bool) {
	switch op {
	case OpGT, OpGTE:
		k := BucketIndex(thresholdMillis)
		return sumRange(h.Counts[:], k, 15), true // over-estimate: bucket k counted in full
	case OpLT, OpLTE:
		k := BucketIndex(thresholdMillis)
		return sumRange(h.Counts[:], 0, k), true // over-estimate: bucket k counted in full
	case OpEQ:
		return 0, false // genuinely unestimable at any resolution finer than a bucket width
	default:
		return 0, false
	}
}

// EstimateBetween approximates the span count matching `loMillis <= duration <= hiMillis` over
// h's window, applying the same over-estimate rule as EstimateThreshold to both boundary
// buckets. When both bounds land in the same bucket, the result is capped at that bucket's own
// count so the over-count can never exceed it (the degenerate case — otherwise summing both
// "boundary buckets in full" would double-count a single bucket).
//
// SPEC-VC-8.
func (h DurationHistogram) EstimateBetween(loMillis, hiMillis uint64) (count int64, known bool) {
	kLo, kHi := BucketIndex(loMillis), BucketIndex(hiMillis)
	if kLo == kHi {
		return h.Counts[kLo], true // degenerate: both bounds in one bucket, capped at that bucket
	}
	return sumRange(h.Counts[:], kLo, kHi), true // both boundary buckets counted in full (over-estimate)
}

// sumRange sums counts[lo..hi] inclusive.
func sumRange(counts []int64, lo, hi int) int64 {
	var sum int64
	for i := lo; i <= hi; i++ {
		sum += counts[i]
	}
	return sum
}

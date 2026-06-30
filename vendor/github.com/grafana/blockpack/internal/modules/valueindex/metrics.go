package valueindex

// metrics.go — value-index based metrics computation (NOTE-VI-032, issue #440).
//
// This file implements count_over_time() and rate() directly from value-index
// results, without reading any blockpack data files.
//
// For a query like:
//
//	{ service.name = "svc-a" } | count_over_time()
//
// The executor:
//  1. Calls QueryFiles to get matching LookupResults (one per matching span, with TimeSec)
//  2. Calls CountOverTime to bucket the results by time step
//  3. Returns the resulting time series
//
// This avoids reading any blockpack block files for simple count queries.
// Sum/avg/min/max over non-indexed fields still require block reads.

// TimeBuckets configures the time bucketing for metrics queries.
type TimeBuckets struct {
	StartNano int64 // query start time in nanoseconds
	EndNano   int64 // query end time in nanoseconds
	StepNano  int64 // step size in nanoseconds
}

// NumBuckets returns the number of time buckets.
func (tb TimeBuckets) NumBuckets() int {
	if tb.StepNano <= 0 {
		return 0
	}
	n := (tb.EndNano - tb.StartNano + tb.StepNano - 1) / tb.StepNano
	if n < 0 {
		return 0
	}
	return int(n) //nolint:gosec // bounded
}

// BucketIndex returns the bucket index for a TimeSec value, or -1 if outside range.
func (tb TimeBuckets) BucketIndex(timeSec uint64) int {
	timeNano := int64(timeSec) * 1_000_000_000 //nolint:gosec // safe for reasonable timestamps
	if timeNano < tb.StartNano || timeNano >= tb.EndNano {
		return -1
	}
	return int((timeNano - tb.StartNano) / tb.StepNano) //nolint:gosec // bounded
}

// CountOverTime buckets LookupResults by time step and returns the count per bucket.
// Each unique (traceID, spanID) pair is counted at most once per bucket.
// Returns nil if tb has no buckets or results is empty.
//
// NOTE-VI-032: This is the zero-block-read fast path for count_over_time() queries
// on indexed columns. The entire computation uses only VI data (TimeSec from entries).
func CountOverTime(results []LookupResult, tb TimeBuckets) []float64 {
	n := tb.NumBuckets()
	if n == 0 || len(results) == 0 {
		return nil
	}
	counts := make([]float64, n)
	// Use a per-bucket seen set to count unique spans (not entries from multiple files).
	type spanKey struct {
		traceID [16]byte
		spanID  [8]byte
	}
	seen := make(map[int]map[spanKey]struct{})
	for _, r := range results {
		bi := tb.BucketIndex(r.TimeSec)
		if bi < 0 {
			continue
		}
		if seen[bi] == nil {
			seen[bi] = make(map[spanKey]struct{})
		}
		k := spanKey{r.TraceID, r.SpanID}
		if _, ok := seen[bi][k]; !ok {
			seen[bi][k] = struct{}{}
			counts[bi]++
		}
	}
	return counts
}

// RateOverTime computes rate() = count_over_time() / stepSecs per bucket.
// Returns nil if tb has no buckets or results is empty.
func RateOverTime(results []LookupResult, tb TimeBuckets) []float64 {
	counts := CountOverTime(results, tb)
	if counts == nil {
		return nil
	}
	stepSecs := float64(tb.StepNano) / 1e9
	if stepSecs <= 0 {
		return counts
	}
	for i := range counts {
		counts[i] /= stepSecs
	}
	return counts
}

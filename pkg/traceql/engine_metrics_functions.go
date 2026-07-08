package traceql

import "math"

func sumOverTime() func(curr float64, n float64) (res float64) {
	var comp float64 // Kahan compensation
	return func(sum, inc float64) (res float64) {
		// A span lacking the aggregated attribute FloatizeAttributes to NaN (TypeNil). Skip it
		// entirely rather than Kahan-adding it — mirrors avgOverTimeSpanAggregator.Observe's own
		// `if math.IsNaN(inc) { return }` guard. Without this, a missing attribute poisons the
		// running sum ORDER-DEPENDENTLY: once sum is real, sum+NaN=NaN forever; but if the NaN is
		// the ONLY value seen so far (sum still NaN-seeded), the branch below would instead
		// return it as the new sum — same value, two different code paths depending on
		// observation order, for what should be a single well-defined "ignore this sample" rule.
		if math.IsNaN(inc) {
			return sum
		}
		if math.IsNaN(sum) {
			return inc
		}
		y := inc - comp
		sum, c := kahanSumInc(y, sum, 0) // Compensation is applied on every step, hence we pass 0 to reset it
		comp = c
		return sum
	}
}

func minOverTime() func(curr float64, n float64) (res float64) {
	return func(curr, n float64) (res float64) {
		if math.IsNaN(curr) || n < curr {
			return n
		}
		return curr
	}
}

func maxOverTime() func(curr float64, n float64) (res float64) {
	return func(curr, n float64) (res float64) {
		if math.IsNaN(curr) || n > curr {
			return n
		}
		return curr
	}
}

package traceql

import (
	"math"
	"testing"
)

// TestSumOverTime_SkipsMissingAttrs_OrderIndependent (ENGINE-FIX-NAN, #491): a span lacking the
// aggregated attribute is FloatizeAttribute'd to NaN (TypeNil). sumOverTime must skip a NaN
// increment entirely, matching avgOverTimeSpanAggregator.Observe's own `if math.IsNaN(inc) {
// return }` guard — not unconditionally Kahan-add it, which poisons the running sum
// order-dependently: reviewer-e2's probe found [120.5, 80.25, missing] -> NaN, while the same
// three values in reverse order ([missing, 80.25, 120.5]) produced 200.75 — a nondeterministic
// production answer for the exact same input set, differing only by span iteration order.
func TestSumOverTime_SkipsMissingAttrs_OrderIndependent(t *testing.T) {
	nan := math.Float64frombits(normalNaN)

	orderings := [][]float64{
		{120.5, 80.25, nan},
		{nan, 80.25, 120.5},
		{80.25, nan, 120.5},
	}

	const want = 200.75
	for i, values := range orderings {
		agg := sumOverTime()
		sum := math.Float64frombits(normalNaN) // matches OverTimeAggregator's own NaN-seeded initial val
		for _, v := range values {
			sum = agg(sum, v)
		}
		if math.IsNaN(sum) {
			t.Fatalf("ordering %d %v: sum is NaN, want %v (a missing attr must be skipped, not poison the sum)", i, values, want)
		}
		if math.Abs(sum-want) > 1e-9 {
			t.Fatalf("ordering %d %v: sum = %v, want %v", i, values, sum, want)
		}
	}
}

// TestSumOverTime_AllValuesMissing_StaysNaN: when EVERY value is missing, the sum must NOT
// become 0 — it stays at the NaN-seeded initial state, matching avg's own "series never touched"
// behavior for a fully-absent attribute (uniform skip semantics between sum and avg).
func TestSumOverTime_AllValuesMissing_StaysNaN(t *testing.T) {
	nan := math.Float64frombits(normalNaN)
	agg := sumOverTime()
	sum := nan
	for range 3 {
		sum = agg(sum, nan)
	}
	if !math.IsNaN(sum) {
		t.Fatalf("sum = %v, want NaN (all-missing series must not become sum=0)", sum)
	}
}

// TestSumOverTime_MutationGuard: removing the NaN-skip guard reproduces the exact
// order-dependent poisoning this fix closes — pinned here as a mutation-verification reference so
// a future regression (accidentally reverting the guard) fails loudly rather than silently.
func TestSumOverTime_MutationGuard(t *testing.T) {
	nan := math.Float64frombits(normalNaN)
	forward := sumOverTime()
	reversed := sumOverTime()

	fSum := nan
	for _, v := range []float64{120.5, 80.25, nan} {
		fSum = forward(fSum, v)
	}
	rSum := nan
	for _, v := range []float64{nan, 80.25, 120.5} {
		rSum = reversed(rSum, v)
	}

	if math.IsNaN(fSum) || math.IsNaN(rSum) {
		t.Fatalf("both orderings must produce a real number, got forward=%v reversed=%v", fSum, rSum)
	}
	if math.Abs(fSum-rSum) > 1e-9 {
		t.Fatalf("order-dependent result: forward=%v reversed=%v must be equal", fSum, rSum)
	}
}

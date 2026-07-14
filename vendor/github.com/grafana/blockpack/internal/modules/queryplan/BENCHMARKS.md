# queryplan Benchmarks

## BENCH-QP-010: per-job dispatch overhead at scale (issue #217, Phase 2.3/4.4)

**Goal:** Get a real number for the per-job overhead of #217's literal, forced one-minute
DispatchTimeSliced slicing, at slice counts spanning the plan's originally-requested
`{10, 100, 500, 2000}` plus the raised `maxSlicesPerPlan` ceiling (`50000`) and its midpoint
(`10000`). This benchmark was explicitly decoupled from gating the "ship literal 1-minute now"
decision (the user overrode Phase 2.3's own written recommendation and ruled that ship decision
directly) — it exists to produce the measurement Phase 2.3's original analysis correctly
identified as missing, and to sanity-check the `maxSlicesPerPlan` raise (2000 → 50000) this task
made alongside the forced-width change.

**Scope split:** blockpack performs no network I/O, so it cannot honestly simulate a real
per-job HTTP/gRPC round-trip. This benchmark is split across both repos:

- **blockpack side** (`internal/modules/queryplan/slices_test.go`,
  `BenchmarkBuildTimeSlices_{10,100,500,2000,10000,50000}`): pure CPU/allocation cost of
  `BuildTimeSlices` constructing N one-minute `TimeSlice` values from a real per-minute VCNT
  signal.
- **tempo side** (`modules/frontend/combiner/dispatch_overhead_bench_test.go`,
  `BenchmarkSearchCombinerDispatchOverhead_{10,100,500,2000,10000,50000}`): the frontend's own
  per-job COMBINE overhead — unmarshaling each slice job's `*tempopb.SearchResponse` and folding
  it into the running result via `genericCombiner.AddResponse`. This is the actual in-process
  cost that scales with slice count on the query-frontend; the network RTT/querier-compute
  portion of real dispatch latency is dominated by factors unrelated to slice count (block size,
  querier load) and isn't something an in-process benchmark could honestly represent.

**Benchmark (blockpack, `slices_test.go`):**

```go
func benchmarkBuildTimeSlices(b *testing.B, n int) {
	minTS := uint64(0)
	maxTS := uint64(n)*minSliceWidthSeconds - 1
	var perMinute []valuecounts.MinuteCount
	for m := uint64(0); m < uint64(n)*minSliceWidthSeconds; m += minSliceWidthSeconds {
		perMinute = append(perMinute, mc(m, int64(m%7)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slices := BuildTimeSlices(perMinute, minTS, maxTS, 1000, DefaultK)
		if len(slices) != n {
			b.Fatalf("expected %d slices, got %d", n, len(slices))
		}
	}
}
```

**Benchmark (tempo, `dispatch_overhead_bench_test.go`):** feeds N synthetic single-trace 200-OK
`*tempopb.SearchResponse` job responses through a fresh `NewSearch` combiner's `AddResponse`.

**Measured results (2026-07-13, `13th Gen Intel(R) Core(TM) i5-13500`, `-benchtime=200x`):**

| Slice count | blockpack `BuildTimeSlices` (ns/op, B/op, allocs/op) | tempo combiner `AddResponse` fan-in (ns/op, B/op, allocs/op) | tempo ns/job |
|---|---|---|---|
| 10    | 545 ns, 648 B, 4 allocs      | 12,466 ns, 18,419 B, 187 allocs      | ~1,247 ns/job |
| 100   | 5,364 ns, 5,544 B, 4 allocs  | 100,530 ns, 178,440 B, 1,732 allocs  | ~1,005 ns/job |
| 500   | 15,992 ns, 34,856 B, 4 allocs | 1,053,570 ns, 928,964 B, 8,946 allocs | ~2,107 ns/job |
| 2000  | 61,408 ns, 139,424 B, 10 allocs | 4,480,216 ns, 3,744,143 B, 35,977 allocs | ~2,240 ns/job |
| 10000 | 797,796 ns, 623,261 B, 34 allocs | 14,109,401 ns, 18,452,004 B, 180,082 allocs | ~1,411 ns/job |
| 50000 | 3,176,323 ns, 2,788,031 B, 130 allocs | 46,944,584 ns, 91,905,806 B, 900,477 allocs | ~939 ns/job |

**Interpretation:**

- Both the plan construction cost (blockpack) and the combine cost (tempo) are linear in slice
  count, as expected, with no superlinear blowup at any measured scale up to the new 50,000-slice
  ceiling.
- Even at the new ceiling (50,000 one-minute slices, ≈34.7 days of window), the ENTIRE
  frontend-side combine overhead is ≈47ms and plan construction is ≈3.2ms — both negligible next
  to the real dispatch latency a 50,000-job fan-out would incur from actual querier round-trips
  (each real querier call costs single-digit-to-double-digit milliseconds minimum; frontend-side
  bookkeeping is 3-4 orders of magnitude cheaper per job). This supports the raised
  `maxSlicesPerPlan=50000` ceiling from the frontend-CPU/allocation angle: the bottleneck for a
  wide, forced-1-minute query is querier fleet capacity/concurrency (the "spend money" cost the
  user explicitly accepted), not frontend-side combining overhead.
- This benchmark does NOT measure real network/querier-fleet dispatch latency, only in-process
  overhead — see this entry's own "Scope split" note above. A future benchmark against a real (or
  simulated-latency) querier fleet would be needed to validate the ACTUAL end-to-end latency
  bound the "any range of data can return in a fixed amount of time" goal depends on; that
  remains a real gap this benchmark does not close, flagged here rather than glossed over.

**Acceptance:** no superlinear growth in ns/op or allocs/op up to 50,000 slices (confirmed); this
is a diagnostic/decision-support benchmark, not a pass/fail regression gate.

**Files:**
`internal/modules/queryplan/slices_test.go:BenchmarkBuildTimeSlices_*` (blockpack)
`tempo/modules/frontend/combiner/dispatch_overhead_bench_test.go:BenchmarkSearchCombinerDispatchOverhead_*` (tempo)

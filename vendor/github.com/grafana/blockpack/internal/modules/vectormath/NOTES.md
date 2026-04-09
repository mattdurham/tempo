# vectormath — Design Notes

## NOTE-001: Lloyd's K-Means with Reservoir Sampling (2026-04-02)
*Added: 2026-04-02*

*Addendum (2026-04-02): Package-level `pqRand` removed; `Train` now creates a per-call local
`*rand.Rand` (seeded at 42) for goroutine safety. Reservoir sampling moved to vectorAccumulator.*

**Decision:** PQ codebook training uses Lloyd's k-means for 20 iterations per subspace.
When the number of input vectors exceeds `maxTrainingSamples` (50,000), vectors are
reduced via Algorithm R reservoir sampling before training (in the writer's vectorAccumulator).
`Train` uses a per-call local `*rand.Rand` (seeded at 42) for centroid initialization,
avoiding data races when `Train` is called concurrently from multiple goroutines.

**Rationale:** Lloyd's k-means is the standard approach for PQ codebook training. 20
iterations is sufficient for convergence in practice (centroid movement becomes negligible
after ~10 iterations for typical embedding distributions). Reservoir sampling caps memory
usage at `50,000 × dim × 4` bytes regardless of file size. A per-call rng eliminates
the data race reported under `go test -race` for concurrent writer scenarios.

**Consequence:** Training results are deterministic for a fixed input set (same seed).
The seed is fixed at 42 within each `Train` call, not across calls. Any change to the
centroid initialization order changes codebook centroids.

**Back-ref:** `internal/modules/vectormath/vectormath.go:Train`,
`internal/modules/blockio/writer/vector_index.go:accumulateBlock`

---

## NOTE-002: Degenerate Case — Fewer Vectors than Centroids (2026-04-02)
*Added: 2026-04-02*

*Addendum (2026-04-02): The "all-zero centroids" claim was incorrect. See below.*

**Decision:** When `len(vectors) < numCentroids`, `Train` caps `effectiveK = len(vectors)`
and trains the codebook on all available vectors. No all-zero centroids are produced; the
codebook is computed from real training data.

**Rationale:** Writer callers (vectorAccumulator) may accumulate very few vectors for
files with only a handful of spans. Capping effectiveK ensures k-means always has at least
one training vector per centroid, avoiding empty clusters from the start. Queries against
such files produce valid (though lower-quality) ranked results. A degenerate codebook is
preferable to a write-time error that would make small files unwritable.

**Back-ref:** `internal/modules/vectormath/vectormath.go:Train`,
`internal/modules/vectormath/vectormath.go:trainSubspace`

---

## NOTE-003: vek32 for SIMD Acceleration in CosineDistance (2026-04-02)
*Added: 2026-04-02*

**Decision:** `CosineDistance` uses `vek32.Dot` and `vek32.Norm` from
`github.com/viterin/vek/vek32` for SIMD-accelerated float32 operations.

**Rationale:** Cosine distance is computed once per candidate vector during semantic
search result ranking. For 768-dim vectors, SIMD reduces this from ~384 multiplications
+ additions to ~48 AVX2 operations (~8× speedup on amd64). The vek32 package automatically
selects the best SIMD path (AVX512/AVX2/SSE2) at runtime.

**Consequence:** The `vek32` package is a required dependency of `vectormath`. On
platforms without SIMD support, vek32 falls back to scalar operations transparently.

**Back-ref:** `internal/modules/vectormath/vectormath.go:CosineDistance`

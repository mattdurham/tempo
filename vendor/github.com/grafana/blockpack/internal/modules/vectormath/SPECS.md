# vectormath — Specifications

## SPEC-001: Package Scope and Invariants
*Added: 2026-04-02*

`vectormath` provides pure Go implementations of Product Quantization (PQ) training and
encoding, Asymmetric Distance Computation (ADC), and cosine distance for float32 embedding
vectors. The package has **no blockio imports** and no I/O; it is a pure math/algorithm
library.

**Invariants:**
- All functions are stateless; `Train` creates a per-call local `*rand.Rand` for goroutine safety.
- `Codebook` is immutable after `Train` returns.
- `ADCScoreFromTable`: if a PQ code byte `code[m]` is out of range (corrupt data), that
  subspace contributes 0 to the score rather than panicking. Codes produced by `Encode` are
  always in-range (`code[m] < K`), so this guard is a corruption-defense only.

---

## SPEC-002: Codebook
*Added: 2026-04-02*

```go
type Codebook struct {
    Centroids [][]float32  // [M][K*subvecDim] flat row-major per subspace
    M         int          // number of subvectors
    K         int          // number of centroids per subspace
    Dim       int          // total vector dimension (must equal M * SubvecDim)
}

func (cb *Codebook) SubvecDim() int  // returns Dim / M
```

**Invariant:** `len(Centroids) == M` and `len(Centroids[i]) == K * SubvecDim()` for all `i`.
**Invariant:** `Dim == M * SubvecDim()` — the total dimension is evenly divisible by M.

---

## SPEC-003: Train
*Added: 2026-04-02*

```go
func Train(vectors [][]float32, numSubvecs, numCentroids int) (Codebook, error)
```

Trains a PQ codebook using Lloyd's k-means on each of the `numSubvecs` subspaces independently.

**Preconditions:**
- `numSubvecs > 0`, `numCentroids > 0`.
- `len(vectors) > 0`.
- All vectors must have the same length, which must be divisible by `numSubvecs`.

**Returns:** `error` if any precondition is violated. On success, returns a fully initialized
`Codebook`.

**Training procedure:**
1. Reservoir-sample up to `maxTrainingSamples` (50,000) vectors.
2. For each subspace `m = 0..numSubvecs-1`: extract the sub-slice `[m*subvecDim:(m+1)*subvecDim]`
   from each sampled vector, run Lloyd's k-means for 20 iterations with `numCentroids` initial
   centroids chosen randomly (without replacement), converge.
3. Store each subspace's centroids as a flat `[]float32` of length `numCentroids * subvecDim`.

**Invariant:** When `len(vectors) < numCentroids`, `effectiveK` is capped to `len(vectors)`
and the codebook is trained on all available vectors. No degenerate/zero case occurs unless
the training set is empty (which `Train` rejects with an error).

---

## SPEC-004: Encode
*Added: 2026-04-02*

```go
func Encode(v []float32, cb Codebook) []byte
```

Encodes vector `v` into a PQ code of `cb.M` bytes using nearest-centroid lookup in each subspace.

**Precondition:** `len(v) == cb.Dim`.
**Returns:** `[]byte` of length `cb.M`. Each byte `code[m]` is the index of the nearest centroid
in subspace `m` (0 ≤ code[m] < cb.K).

---

## SPEC-005: DistanceTable and ADCScoreFromTable
*Added: 2026-04-02*

```go
func DistanceTable(q []float32, cb Codebook) [][]float32
func ADCScoreFromTable(code []byte, table [][]float32) float32
```

`DistanceTable` precomputes an `[M][K]` table of squared Euclidean distances from query `q`
to each centroid in each subspace. Used for efficient ADC over many vectors sharing the same
codebook.

`ADCScoreFromTable` computes the approximate ADC distance for a PQ-encoded vector `code`
from a precomputed table. Returns the sum of `table[m][code[m]]` for `m = 0..M-1`.

**Invariant:** `ADCScore(q, code, cb) == ADCScoreFromTable(code, DistanceTable(q, cb))` for
any valid `q`, `code`, `cb`.

---

## SPEC-006: CosineDistance
*Added: 2026-04-02*

```go
func CosineDistance(a, b []float32) float32
```

Returns `1 - cosine_similarity(a, b)`. Range: `[0, 2]` (0 = identical direction,
1 = orthogonal, 2 = opposite direction).

**Zero-vector handling:** If either `norm(a) == 0` or `norm(b) == 0`, returns `1.0`
(neutral distance — neither close nor far).

**Implementation:** Uses `vek32.Dot` and `vek32.Norm` for SIMD acceleration on amd64/arm64.

---

## SPEC-007: Mean
*Added: 2026-04-02*

```go
func Mean(vectors [][]float32) []float32
```

Returns the component-wise arithmetic mean of `vectors`. All vectors must have the same
length. Returns an empty (non-nil) `[]float32{}` if `len(vectors) == 0`.

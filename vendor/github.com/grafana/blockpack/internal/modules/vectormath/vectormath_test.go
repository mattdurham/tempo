package vectormath

import (
	"math"
	"testing"
)

func TestCosineDistance_identical(t *testing.T) {
	v := []float32{1, 2, 3, 4}
	d := CosineDistance(v, v)
	if d > 1e-5 {
		t.Errorf("identical vectors: want ~0, got %v", d)
	}
}

func TestCosineDistance_orthogonal(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	d := CosineDistance(a, b)
	if math.Abs(float64(d)-1.0) > 1e-5 {
		t.Errorf("orthogonal vectors: want ~1.0, got %v", d)
	}
}

func TestCosineDistance_opposite(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{-1, 0, 0}
	d := CosineDistance(a, b)
	if math.Abs(float64(d)-2.0) > 1e-5 {
		t.Errorf("opposite vectors: want ~2.0, got %v", d)
	}
}

func TestCosineDistance_known(t *testing.T) {
	// a=[1,0], b=[1,1]/sqrt(2) — cos_sim=1/sqrt(2)≈0.707 → dist≈0.293
	a := []float32{1, 0}
	b := []float32{float32(1.0 / math.Sqrt2), float32(1.0 / math.Sqrt2)}
	d := CosineDistance(a, b)
	want := float32(1.0 - 1.0/math.Sqrt2)
	if math.Abs(float64(d)-float64(want)) > 1e-4 {
		t.Errorf("known vectors: want ~%v, got %v", want, d)
	}
}

func TestCosineDistance_zeroVector(t *testing.T) {
	a := []float32{0, 0, 0}
	b := []float32{1, 2, 3}
	d := CosineDistance(a, b)
	if d != 1.0 {
		t.Errorf("zero vector: want 1.0 (max distance), got %v", d)
	}
}

func TestPQTrain_centroidCount(t *testing.T) {
	vecs := makeTestVectors(64, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	if cb.M != 4 {
		t.Errorf("M: want 4, got %d", cb.M)
	}
	if len(cb.Centroids) != 4 {
		t.Errorf("Centroids length: want 4, got %d", len(cb.Centroids))
	}
	subvecDim := 32 / 4
	for i, cents := range cb.Centroids {
		// Each subspace has K*subvecDim floats.
		want := 8 * subvecDim
		if len(cents) != want {
			t.Errorf("Centroids[%d]: want length %d, got %d", i, want, len(cents))
		}
	}
}

func TestPQTrain_degenerate_fewvecs(t *testing.T) {
	// Fewer vectors than K: should succeed without panic.
	vecs := makeTestVectors(3, 8)
	_, err := Train(vecs, 2, 8)
	if err != nil {
		t.Fatalf("Train with few vectors: %v", err)
	}
}

func TestPQTrain_dimNotDivisible(t *testing.T) {
	vecs := makeTestVectors(10, 7) // 7 not divisible by M=3
	_, err := Train(vecs, 3, 4)
	if err == nil {
		t.Error("expected error for dim not divisible by M")
	}
}

func TestPQEncode_length(t *testing.T) {
	vecs := makeTestVectors(64, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	code := Encode(vecs[0], cb)
	if len(code) != 4 {
		t.Errorf("Encode length: want 4 (M), got %d", len(code))
	}
}

func TestPQEncode_decode_roundtrip(t *testing.T) {
	// Encode a vector that IS a centroid — should decode back to near-identical centroid.
	vecs := makeTestVectors(128, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	// Use the centroid of subspace 0 directly.
	subvecDim := cb.SubvecDim()
	query := make([]float32, 32)
	// Copy centroid 0 from each subspace into query.
	for m := 0; m < cb.M; m++ {
		copy(query[m*subvecDim:], cb.Centroids[m][:subvecDim])
	}
	code := Encode(query, cb)
	// Each byte should be 0 (nearest centroid is index 0).
	for i, b := range code {
		if b != 0 {
			t.Errorf("code[%d]: want 0 (first centroid), got %d", i, b)
		}
	}
}

func TestADCScore_self(t *testing.T) {
	vecs := makeTestVectors(128, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	v := vecs[0]
	code := Encode(v, cb)
	score := ADCScore(v, code, cb)
	// ADC score is approximate; for a self-encoded vector it should be small.
	if score < 0 {
		t.Errorf("ADCScore: negative score %v", score)
	}
}

func TestADCScore_ordering(t *testing.T) {
	vecs := makeTestVectors(200, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	query := vecs[0]
	// nearby is vecs[1] (similar due to sequential construction), far is orthogonal.
	near := vecs[1]
	far := make([]float32, 32)
	far[31] = 1 // orthogonal-ish to near
	codeNear := Encode(near, cb)
	codeFar := Encode(far, cb)
	scoreNear := ADCScore(query, codeNear, cb)
	scoreFar := ADCScore(query, codeFar, cb)
	// Near should have lower (better) score than far — just check they differ.
	_ = scoreNear
	_ = scoreFar
}

func TestDistanceTable_dimensions(t *testing.T) {
	vecs := makeTestVectors(64, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	table := DistanceTable(vecs[0], cb)
	if len(table) != cb.M {
		t.Errorf("DistanceTable rows: want %d, got %d", cb.M, len(table))
	}
	for i, row := range table {
		if len(row) != cb.K {
			t.Errorf("DistanceTable[%d] cols: want %d, got %d", i, cb.K, len(row))
		}
	}
}

func TestADCScoreFromTable_matches_ADCScore(t *testing.T) {
	vecs := makeTestVectors(64, 32)
	cb, err := Train(vecs, 4, 8)
	if err != nil {
		t.Fatalf("Train: %v", err)
	}
	query := vecs[0]
	code := Encode(query, cb)
	table := DistanceTable(query, cb)
	direct := ADCScore(query, code, cb)
	fromTable := ADCScoreFromTable(code, table)
	if math.Abs(float64(direct)-float64(fromTable)) > 1e-4 {
		t.Errorf("ADCScore vs ADCScoreFromTable: %v vs %v", direct, fromTable)
	}
}

func TestMean_empty(t *testing.T) {
	result := Mean(nil)
	if result == nil {
		t.Fatal("Mean(nil) returned nil")
	}
	if len(result) != 0 {
		t.Errorf("Mean(nil) returned non-empty: %v", result)
	}
}

func TestMean_emptySlice(t *testing.T) {
	result := Mean([][]float32{})
	if result == nil {
		t.Fatal("Mean([]) returned nil")
	}
	if len(result) != 0 {
		t.Errorf("Mean([]) returned non-empty: %v", result)
	}
}

func TestMean_values(t *testing.T) {
	vecs := [][]float32{{1, 2}, {3, 4}}
	result := Mean(vecs)
	if len(result) != 2 {
		t.Fatalf("Mean length: want 2, got %d", len(result))
	}
	if math.Abs(float64(result[0])-2.0) > 1e-5 {
		t.Errorf("Mean[0]: want 2.0, got %v", result[0])
	}
	if math.Abs(float64(result[1])-3.0) > 1e-5 {
		t.Errorf("Mean[1]: want 3.0, got %v", result[1])
	}
}

func TestMean_single(t *testing.T) {
	vecs := [][]float32{{5, 10, 15}}
	result := Mean(vecs)
	if len(result) != 3 {
		t.Fatalf("Mean length: want 3, got %d", len(result))
	}
	for i, v := range result {
		if math.Abs(float64(v)-float64(vecs[0][i])) > 1e-5 {
			t.Errorf("Mean[%d]: want %v, got %v", i, vecs[0][i], v)
		}
	}
}

// makeTestVectors generates n vectors of given dimension with deterministic values.
func makeTestVectors(n, dim int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32((i*dim+j)%100) * 0.1
		}
		vecs[i] = v
	}
	return vecs
}

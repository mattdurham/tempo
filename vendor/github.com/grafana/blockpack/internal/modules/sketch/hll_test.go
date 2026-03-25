package sketch_test

import (
	"fmt"
	"testing"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

// SK-T-01: TestHLL_EmptyCardinality
func TestHLL_EmptyCardinality(t *testing.T) {
	h := sketch.NewHyperLogLog()
	if h.Cardinality() != 0 {
		t.Fatalf("empty HLL cardinality: got %d, want 0", h.Cardinality())
	}
}

// SK-T-02: TestHLL_AddOne
func TestHLL_AddOne(t *testing.T) {
	h := sketch.NewHyperLogLog()
	h.Add("hello")
	if h.Cardinality() == 0 {
		t.Fatal("after adding one element, cardinality should be > 0")
	}
}

// SK-T-03: TestHLL_AddMany
func TestHLL_AddMany(t *testing.T) {
	h := sketch.NewHyperLogLog()
	const n = 1000
	for i := range n {
		h.Add(fmt.Sprintf("item-%d", i))
	}
	card := h.Cardinality()
	// p=4 has ~26% std error; allow 30% margin
	if card < 700 || card > 1300 {
		t.Fatalf("cardinality(%d distinct values) = %d, want in [700, 1300]", n, card)
	}
}

// SK-T-04: TestHLL_MarshalRoundTrip
func TestHLL_MarshalRoundTrip(t *testing.T) {
	h := sketch.NewHyperLogLog()
	for i := range 200 {
		h.Add(fmt.Sprintf("val-%d", i))
	}
	b := h.Marshal()
	if len(b) != 16 {
		t.Fatalf("Marshal length: got %d, want 16 (SPEC-SK-03)", len(b))
	}
	h2 := sketch.NewHyperLogLog()
	if err := h2.Unmarshal(b); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if h2.Cardinality() != h.Cardinality() {
		t.Fatalf("round-trip cardinality: got %d, want %d", h2.Cardinality(), h.Cardinality())
	}
}

// SK-T-05: TestHLL_UnmarshalBadLength
func TestHLL_UnmarshalBadLength(t *testing.T) {
	h := sketch.NewHyperLogLog()
	for _, bad := range [][]byte{
		{},
		make([]byte, 15),
		make([]byte, 17),
	} {
		if err := h.Unmarshal(bad); err == nil {
			t.Fatalf("Unmarshal(%d bytes): want error, got nil (SPEC-SK-04)", len(bad))
		}
	}
}

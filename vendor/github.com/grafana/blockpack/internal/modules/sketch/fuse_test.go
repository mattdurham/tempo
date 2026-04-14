package sketch_test

import (
	"testing"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

// SK-T-11: TestFuse8_EmptyFilter
func TestFuse8_EmptyFilter(t *testing.T) {
	f, err := sketch.NewBinaryFuse8(nil)
	if err != nil {
		t.Fatalf("NewBinaryFuse8(nil): %v", err)
	}
	if f.Contains(12345) {
		t.Fatal("empty filter: Contains should return false (SPEC-SK-15)")
	}
}

// SK-T-12: TestFuse8_ContainsAllKeys
func TestFuse8_ContainsAllKeys(t *testing.T) {
	keys := make([]uint64, 100)
	for i := range keys {
		keys[i] = uint64(i * 1000)
	}
	f, err := sketch.NewBinaryFuse8(keys)
	if err != nil {
		t.Fatalf("NewBinaryFuse8: %v", err)
	}
	for _, k := range keys {
		if !f.Contains(k) {
			t.Fatalf("Contains(%d) = false, want true (SPEC-SK-12, no false negatives)", k)
		}
	}
}

// SK-T-13: TestFuse8_NoFalseNegatives
func TestFuse8_NoFalseNegatives(t *testing.T) {
	keys := make([]uint64, 1000)
	for i := range keys {
		keys[i] = uint64(i)*2654435761 + 1
	}
	f, err := sketch.NewBinaryFuse8(keys)
	if err != nil {
		t.Fatalf("NewBinaryFuse8: %v", err)
	}
	for i, k := range keys {
		if !f.Contains(k) {
			t.Fatalf("key[%d]=%d: Contains = false, want true (SPEC-SK-12)", i, k)
		}
	}
}

// SK-T-14: TestFuse8_MarshalRoundTrip
func TestFuse8_MarshalRoundTrip(t *testing.T) {
	keys := []uint64{1, 2, 3, 100, 999}
	f, err := sketch.NewBinaryFuse8(keys)
	if err != nil {
		t.Fatalf("NewBinaryFuse8: %v", err)
	}
	b, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	f2 := &sketch.BinaryFuse8{}
	if err := f2.UnmarshalBinary(b); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	for _, k := range keys {
		if !f2.Contains(k) {
			t.Fatalf("after round-trip: Contains(%d) = false (SPEC-SK-14)", k)
		}
	}
}

// SK-T-15: TestFuse8_UnmarshalBadData
func TestFuse8_UnmarshalBadData(t *testing.T) {
	f := &sketch.BinaryFuse8{}
	if err := f.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
		t.Fatal("UnmarshalBinary with 3 bytes: want error")
	}
}

// SK-T-16: TestFuse8_DuplicateKeys
func TestFuse8_DuplicateKeys(t *testing.T) {
	// Duplicate keys must not cause construction failure (SPEC-SK-16 dedup requirement).
	keys := []uint64{1, 1, 1, 1, 1}
	f, err := sketch.NewBinaryFuse8(keys)
	if err != nil {
		t.Fatalf("NewBinaryFuse8 with duplicate keys: %v", err)
	}
	if !f.Contains(1) {
		t.Fatal("Contains(1) = false after adding 1 five times")
	}
}

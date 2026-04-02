package sketch_test

import (
	"fmt"
	"testing"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

// SK-T-06: TestCMS_EmptyEstimate
func TestCMS_EmptyEstimate(t *testing.T) {
	cms := sketch.NewCountMinSketch()
	if cms.Estimate("anything") != 0 {
		t.Fatal("empty CMS: Estimate should return 0 (SPEC-SK-07)")
	}
}

// SK-T-07: TestCMS_AddEstimate
func TestCMS_AddEstimate(t *testing.T) {
	cms := sketch.NewCountMinSketch()
	cms.Add("hot-key", 5)
	est := cms.Estimate("hot-key")
	if est < 5 {
		t.Fatalf("Estimate after Add(5): got %d, want >= 5 (SPEC-SK-08)", est)
	}
}

// SK-T-08: TestCMS_MarshalRoundTrip
func TestCMS_MarshalRoundTrip(t *testing.T) {
	cms := sketch.NewCountMinSketch()
	cms.Add("key-a", 10)
	cms.Add("key-b", 3)
	b := cms.Marshal()
	wantSize := sketch.CMSDepth * sketch.CMSWidth * 2 // 512 bytes
	if len(b) != wantSize {
		t.Fatalf("Marshal length: got %d, want %d (SPEC-SK-09)", len(b), wantSize)
	}
	cms2 := sketch.NewCountMinSketch()
	if err := cms2.Unmarshal(b); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if cms2.Estimate("key-a") != cms.Estimate("key-a") {
		t.Fatalf("round-trip Estimate(key-a): got %d, want %d", cms2.Estimate("key-a"), cms.Estimate("key-a"))
	}
}

// SK-T-09: TestCMS_UnmarshalBadLength
func TestCMS_UnmarshalBadLength(t *testing.T) {
	cms := sketch.NewCountMinSketch()
	marshalSize := sketch.CMSDepth * sketch.CMSWidth * 2
	for _, bad := range [][]byte{
		{},
		make([]byte, marshalSize-1),
		make([]byte, marshalSize+1),
	} {
		if err := cms.Unmarshal(bad); err == nil {
			t.Fatalf("Unmarshal(%d bytes): want error, got nil (SPEC-SK-10)", len(bad))
		}
	}
}

// SK-T-10: TestCMS_NeverFalseNegative
func TestCMS_NeverFalseNegative(t *testing.T) {
	cms := sketch.NewCountMinSketch()
	keys := make([]string, 100)
	for i := range 100 {
		keys[i] = fmt.Sprintf("unique-key-%d", i)
		cms.Add(keys[i], 1)
	}
	for _, k := range keys {
		if cms.Estimate(k) < 1 {
			t.Fatalf("Estimate(%q) = 0 after adding 1: false negative (SPEC-SK-08)", k)
		}
	}
}

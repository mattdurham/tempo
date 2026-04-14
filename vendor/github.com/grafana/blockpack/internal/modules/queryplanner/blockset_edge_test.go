// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

package queryplanner

import (
	"testing"
)

// TestBlockSetAndDifferentLengths verifies AND between blockSets of different word-count.
// Words beyond the shorter set in the longer set must be zeroed.
func TestBlockSetAndDifferentLengths(t *testing.T) {
	s1 := allBlocks(130) // 3 words
	s2 := allBlocks(10)  // 1 word

	s1.and(s2)

	// Bits 0..9 should survive (present in both).
	for i := range 10 {
		if !s1.test(i) {
			t.Errorf("and: bit %d should be set (in both sets)", i)
		}
	}
	// Bits 10..129 should be cleared (beyond s2's range).
	for i := 10; i < 130; i++ {
		if s1.test(i) {
			t.Errorf("and: bit %d should be cleared (beyond shorter set)", i)
		}
	}
	if got := s1.count(); got != 10 {
		t.Errorf("and: count = %d, want 10", got)
	}
}

// TestBlockSetOrDifferentLengths verifies OR between blockSets of different word-count.
// Words beyond the receiver's length in other are ignored (no panic).
func TestBlockSetOrDifferentLengths(t *testing.T) {
	s1 := allBlocks(10)  // 1 word, bits 0..9
	s2 := allBlocks(130) // 3 words, bits 0..129

	// s1 |= s2: only first word of s2 contributes; s1 can't grow.
	s1.or(s2)

	// s1 is still 1 word wide → bits 0..63 are all ones (from s2), bits 64+ unreachable.
	for i := range 64 {
		if !s1.test(i) {
			t.Errorf("or: bit %d should be set", i)
		}
	}
	if got := s1.count(); got != 64 {
		t.Errorf("or: count = %d, want 64", got)
	}
}

// TestBlockSetClearDuringIter verifies that clear() during iter() is safe.
// iter() copies each word before traversing its bits, so clearing bits
// during iteration must not skip any originally-set bit.
func TestBlockSetClearDuringIter(t *testing.T) {
	s := allBlocks(10) // bits 0..9

	var visited []int
	s.iter(func(i int) {
		visited = append(visited, i)
		// Clear every bit during iteration.
		s.clear(i)
	})

	// All 10 bits should have been visited despite clearing.
	if len(visited) != 10 {
		t.Fatalf("iter+clear: visited %d bits, want 10", len(visited))
	}
	for i, v := range visited {
		if v != i {
			t.Errorf("iter+clear: visited[%d] = %d, want %d", i, v, i)
		}
	}
	// After iteration, all bits should be cleared.
	if got := s.count(); got != 0 {
		t.Errorf("iter+clear: count after = %d, want 0", got)
	}
}

// TestBlockSetAndEmptyPreservesNothing verifies AND with an empty (nil) set zeroes all bits.
func TestBlockSetAndEmptyPreservesNothing(t *testing.T) {
	s := allBlocks(100)
	empty := blockSet(nil)
	s.and(empty)
	if got := s.count(); got != 0 {
		t.Errorf("and(nil): count = %d, want 0", got)
	}
}

// TestBlockSetOrEmptyPreservesAll verifies OR with an empty (nil) set is a no-op.
func TestBlockSetOrEmptyPreservesAll(t *testing.T) {
	s := allBlocks(100)
	empty := blockSet(nil)
	before := s.count()
	s.or(empty)
	if got := s.count(); got != before {
		t.Errorf("or(nil): count = %d, want %d", got, before)
	}
}

// TestBlockSetSetIdempotent verifies that setting an already-set bit is a no-op.
func TestBlockSetSetIdempotent(t *testing.T) {
	s := allBlocks(10)
	s.set(5) // already set
	if got := s.count(); got != 10 {
		t.Errorf("set(5) on already-set bit: count = %d, want 10", got)
	}
}

// TestBlockSetClearIdempotent verifies that clearing an already-cleared bit is a no-op.
func TestBlockSetClearIdempotent(t *testing.T) {
	s := allBlocks(10)
	s.clear(5)
	s.clear(5) // already cleared
	if got := s.count(); got != 9 {
		t.Errorf("clear(5) on already-cleared bit: count = %d, want 9", got)
	}
}

// TestBlockSetExactBoundary verifies blockSet with exactly 64 blocks (one full word, no tail).
func TestBlockSetExactBoundary(t *testing.T) {
	s := allBlocks(64)
	if len(s) != 1 {
		t.Fatalf("allBlocks(64): len = %d, want 1", len(s))
	}
	// All 64 bits should be set.
	if got := s.count(); got != 64 {
		t.Fatalf("allBlocks(64): count = %d, want 64", got)
	}
	// Bit 63 should be the last set bit.
	if !s.test(63) {
		t.Error("bit 63 should be set")
	}
	// Bit 64 is out of range.
	if s.test(64) {
		t.Error("bit 64 should be out of range")
	}
}

// TestBlockSetIterOrder verifies iter yields bits in strictly ascending order across word boundaries.
func TestBlockSetIterOrder(t *testing.T) {
	s := make(blockSet, 4) // 256 bits
	// Set scattered bits: 0, 63, 64, 127, 128, 255.
	s.set(0)
	s.set(63)
	s.set(64)
	s.set(127)
	s.set(128)
	s.set(255)

	var got []int
	s.iter(func(i int) { got = append(got, i) })

	want := []int{0, 63, 64, 127, 128, 255}
	if len(got) != len(want) {
		t.Fatalf("iter: got %v, want %v", got, want)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("iter[%d]: got %d, want %d", i, got[i], v)
		}
	}
}

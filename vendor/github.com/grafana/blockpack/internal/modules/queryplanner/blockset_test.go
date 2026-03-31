// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

package queryplanner

import (
	"testing"
)

func TestBlockSetAllBlocks(t *testing.T) {
	t.Run("zero blocks", func(t *testing.T) {
		s := allBlocks(0)
		if len(s) != 0 {
			t.Errorf("allBlocks(0): expected len 0, got %d", len(s))
		}
		if got := s.count(); got != 0 {
			t.Errorf("allBlocks(0).count(): got %d, want 0", got)
		}
	})

	t.Run("one block", func(t *testing.T) {
		s := allBlocks(1)
		if !s.test(0) {
			t.Error("allBlocks(1): bit 0 should be set")
		}
		if got := s.count(); got != 1 {
			t.Errorf("allBlocks(1).count(): got %d, want 1", got)
		}
	})

	t.Run("64 blocks fills exactly one word", func(t *testing.T) {
		s := allBlocks(64)
		if len(s) != 1 {
			t.Errorf("allBlocks(64): expected 1 word, got %d", len(s))
		}
		if got := s.count(); got != 64 {
			t.Errorf("allBlocks(64).count(): got %d, want 64", got)
		}
		for i := range 64 {
			if !s.test(i) {
				t.Errorf("allBlocks(64): bit %d should be set", i)
			}
		}
	})

	t.Run("65 blocks spans two words", func(t *testing.T) {
		s := allBlocks(65)
		if len(s) != 2 {
			t.Errorf("allBlocks(65): expected 2 words, got %d", len(s))
		}
		if got := s.count(); got != 65 {
			t.Errorf("allBlocks(65).count(): got %d, want 65", got)
		}
		for i := range 65 {
			if !s.test(i) {
				t.Errorf("allBlocks(65): bit %d should be set", i)
			}
		}
		// Bit 65 should not be set (out of allocated n).
		if s.test(65) {
			t.Error("allBlocks(65): bit 65 should not be set")
		}
	})
}

func TestBlockSetTest(t *testing.T) {
	s := allBlocks(10)
	for i := range 10 {
		if !s.test(i) {
			t.Errorf("test(%d): expected true", i)
		}
	}
	// Out-of-range index returns false.
	if s.test(100) {
		t.Error("test(100): expected false for out-of-range")
	}
	if s.test(-1) {
		t.Error("test(-1): expected false for negative index")
	}
}

func TestBlockSetClear(t *testing.T) {
	s := allBlocks(10)
	s.clear(3)
	if s.test(3) {
		t.Error("clear(3): bit 3 should be unset after clear")
	}
	// Surrounding bits should still be set.
	if !s.test(2) {
		t.Error("clear(3): bit 2 should still be set")
	}
	if !s.test(4) {
		t.Error("clear(3): bit 4 should still be set")
	}
	if got := s.count(); got != 9 {
		t.Errorf("after clear(3): count = %d, want 9", got)
	}
}

func TestBlockSetSet(t *testing.T) {
	s := allBlocks(10)
	s.clear(5)
	if s.test(5) {
		t.Fatal("pre-condition: bit 5 should be clear")
	}
	s.set(5)
	if !s.test(5) {
		t.Error("set(5): bit 5 should be set after set()")
	}
	if got := s.count(); got != 10 {
		t.Errorf("after set(5): count = %d, want 10", got)
	}
}

func TestBlockSetAnd(t *testing.T) {
	// s1 = {0,1,2,3}; s2 = {2,3,4,5}; intersection = {2,3}.
	s1 := allBlocks(4) // bits 0..3
	s2 := allBlocks(6) // bits 0..5
	s2.clear(0)        // remove 0
	s2.clear(1)        // remove 1
	s1.and(s2)         // s1 &= s2
	for i := range 6 {
		want := i == 2 || i == 3
		if s1.test(i) != want {
			t.Errorf("and: test(%d) = %v, want %v", i, s1.test(i), want)
		}
	}
	if got := s1.count(); got != 2 {
		t.Errorf("and: count = %d, want 2", got)
	}
}

func TestBlockSetOr(t *testing.T) {
	// s1 = {0,1}; s2 = {2,3}; union = {0,1,2,3}.
	s1 := allBlocks(2) // bits 0..1
	s2 := make(blockSet, 1)
	s2.set(2)
	s2.set(3)
	s1.or(s2)
	for i := range 4 {
		if !s1.test(i) {
			t.Errorf("or: test(%d) = false, want true", i)
		}
	}
	if got := s1.count(); got != 4 {
		t.Errorf("or: count = %d, want 4", got)
	}
}

func TestBlockSetIter(t *testing.T) {
	t.Run("empty bitset yields nothing", func(t *testing.T) {
		s := allBlocks(0)
		called := false
		s.iter(func(_ int) { called = true })
		if called {
			t.Error("iter on empty blockSet should not call fn")
		}
	})

	t.Run("iter yields all set bits in ascending order", func(t *testing.T) {
		s := allBlocks(5) // bits 0..4
		s.clear(1)
		s.clear(3)
		// Expected bits: 0, 2, 4.
		want := []int{0, 2, 4}
		var got []int
		s.iter(func(i int) { got = append(got, i) })
		if len(got) != len(want) {
			t.Fatalf("iter: got %v, want %v", got, want)
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("iter[%d]: got %d, want %d", i, got[i], v)
			}
		}
	})

	t.Run("iter across word boundary", func(t *testing.T) {
		s := allBlocks(130)
		// Only keep bits 63, 64, 129.
		for i := range 130 {
			if i != 63 && i != 64 && i != 129 {
				s.clear(i)
			}
		}
		var got []int
		s.iter(func(i int) { got = append(got, i) })
		want := []int{63, 64, 129}
		if len(got) != len(want) {
			t.Fatalf("iter across boundary: got %v, want %v", got, want)
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("iter[%d]: got %d, want %d", i, got[i], v)
			}
		}
	})
}

// TestBlockSetAndDifferentSizes — and() with a shorter RHS must not panic and must clip correctly.
func TestBlockSetAndDifferentSizes(t *testing.T) {
	// s1 has 3 words (192 bits), s2 has 1 word (64 bits).
	// and() iterates min(len) words, so bits beyond word 0 in s1 must be cleared.
	s1 := allBlocks(192)
	s2 := allBlocks(64)
	// Clear bit 5 in s2 so it's distinguishable.
	s2.clear(5)
	s1.and(s2)
	// Bit 5 should be clear (from s2).
	if s1.test(5) {
		t.Error("and: bit 5 should be clear after and with s2")
	}
	// Bits 64..191 should be clear (s2 shorter — words beyond len(s2) are zeroed).
	for i := 64; i < 192; i++ {
		if s1.test(i) {
			t.Errorf("and: bit %d should be clear (beyond s2 length)", i)
		}
	}
	// Bits 0..63 (except 5) should still be set.
	for i := range 64 {
		if i == 5 {
			continue
		}
		if !s1.test(i) {
			t.Errorf("and: bit %d should still be set", i)
		}
	}
}

// TestBlockSetOrDifferentSizes — or() with a shorter RHS must not panic.
func TestBlockSetOrDifferentSizes(t *testing.T) {
	// s1 has 1 word; s2 has 2 words.
	s1 := make(blockSet, 1) // empty 64-bit set
	s2 := allBlocks(128)    // bits 0..127
	// Clear a few bits in s2.
	s2.clear(10)
	s2.clear(70)
	s1.or(s2)
	// s1 only has 1 word — or() iterates min(len), so only bits 0..63 merged.
	for i := range 64 {
		want := i != 10
		if s1.test(i) != want {
			t.Errorf("or: bit %d = %v, want %v", i, s1.test(i), want)
		}
	}
	// Bits 64..127 from s2 are not visible in s1 (s1 has only 1 word).
	for i := 64; i < 128; i++ {
		if s1.test(i) {
			t.Errorf("or: bit %d should not be set (s1 only has 1 word)", i)
		}
	}
}

// TestBlockSetIterModifySafe — iter callback may call clear() without skipping other set bits.
// This verifies iter reads bits before calling fn (snapshot-before-callback pattern is not
// required — but clearing visited bits must not panic or skip remaining bits).
func TestBlockSetIterModifySafe(t *testing.T) {
	s := allBlocks(10)
	var visited []int
	s.iter(func(i int) {
		visited = append(visited, i)
		s.clear(i)
	})
	// All 10 bits should have been visited.
	if len(visited) != 10 {
		t.Errorf("iter with clear: visited %d bits, want 10; visited=%v", len(visited), visited)
	}
	// After clearing all bits, count should be 0.
	if got := s.count(); got != 0 {
		t.Errorf("after clearing all via iter callback: count = %d, want 0", got)
	}
}

// TestBlockSetLargeN — blockSet handles n > 64 (multi-word) correctly.
func TestBlockSetLargeN(t *testing.T) {
	const n = 200
	s := allBlocks(n)
	if got := s.count(); got != n {
		t.Errorf("allBlocks(%d).count() = %d, want %d", n, got, n)
	}
	// Clear every other bit.
	for i := 0; i < n; i += 2 {
		s.clear(i)
	}
	if got := s.count(); got != n/2 {
		t.Errorf("after clearing even bits: count = %d, want %d", got, n/2)
	}
	// Verify only odd bits remain.
	var got []int
	s.iter(func(i int) { got = append(got, i) })
	for idx, b := range got {
		if b%2 == 0 {
			t.Errorf("iter[%d]: got even bit %d, expected only odd bits", idx, b)
		}
	}
}

func TestBlockSetNumBlocks(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{0, 0},
		{1, 64},
		{63, 64},
		{64, 64},
		{65, 128},
		{128, 128},
		{129, 192},
	}
	for _, tt := range tests {
		s := allBlocks(tt.n)
		if got := s.numBlocks(); got != tt.want {
			t.Errorf("allBlocks(%d).numBlocks() = %d, want %d", tt.n, got, tt.want)
		}
	}
}

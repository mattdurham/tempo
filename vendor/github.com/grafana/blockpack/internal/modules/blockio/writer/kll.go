package writer

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"bytes"
	"cmp"
	"slices"
)

const (
	kllDefaultK      = 200 // accuracy parameter for numeric/string sketches
	kllBytesDefaultK = 200 // accuracy parameter for bytes sketches
	kllMinCap        = 8   // minimum level capacity (M in the KLL'16 paper)
)

// kllPow3 holds 3^i for i in [0,30], used by kllLevelCap.
var kllPow3 = [31]uint64{
	1, 3, 9, 27, 81, 243, 729, 2187, 6561, 19683, 59049, 177147, 531441,
	1594323, 4782969, 14348907, 43046721, 129140163, 387420489, 1162261467,
	3486784401, 10460353203, 31381059609, 94143178827, 282429536481,
	847288609443, 2541865828329, 7625597484987, 22876792454961, 68630377364883,
	205891132094649,
}

// kllLevelCap returns the item capacity for level h in a sketch with numLevels
// total levels and accuracy parameter k.
//
// Formula from KLL'16 §4: max(M, floor(k * (2/3)^depth))
// where depth = numLevels - h - 1 (depth 0 = highest level = most compacted).
func kllLevelCap(k, numLevels, h int) int {
	depth := numLevels - h - 1
	if depth <= 0 {
		return max(kllMinCap, k)
	}
	if depth >= len(kllPow3) {
		return kllMinCap
	}
	// floor(k * (2/3)^depth) via integer arithmetic (from DataSketches):
	//   floor((2k * 2^depth / 3^depth + 1) / 2)
	twok := uint64(k) * 2 //nolint:gosec // k is bounded by uint16 (65535); safe conversion
	tmp := (twok << uint(depth)) / kllPow3[depth]
	c := int((tmp + 1) >> 1)
	return max(kllMinCap, c)
}

// kllRandomBit returns 0 or 1 using xorshift64 on the per-instance state rng.
// Each KLL/KLLBytes struct holds its own rng so that concurrent Writers do not
// race on a shared global (two Writers may call Flush() — and therefore
// compactLevel() — concurrently on different goroutines).
func kllRandomBit(rng *uint64) int {
	*rng ^= *rng << 13
	*rng ^= *rng >> 7
	*rng ^= *rng << 17
	return int(*rng & 1)
}

// KLL is a correct multi-level compaction quantile sketch.
// Implements the KLL algorithm from "Optimal Quantile Approximation in Streams"
// (Karnin, Lang, Liberty, FOCS 2016).
//
// Memory: O(k * log(n)) items retained.
// Quantile error: O(1/k) with high probability.
// Handles sorted and adversarial input correctly via random halving on compaction.
type KLL[T cmp.Ordered] struct {
	// levels[0] is the unsorted input buffer.
	// levels[h > 0] are always sorted (maintained by compactLevel).
	levels [][]T
	k      int    // accuracy parameter; higher = more accurate + more memory
	n      int    // total items added (not just retained)
	rng    uint64 // per-instance xorshift64 state; never zero after init
}

// NewKLL creates a KLL sketch for any ordered type using the default k parameter.
func NewKLL[T cmp.Ordered]() *KLL[T] {
	return newKLLWithK[T](kllDefaultK)
}

func newKLLWithK[T cmp.Ordered](k int) *KLL[T] {
	sk := &KLL[T]{k: k, rng: 0xdeadbeefcafebabe}
	sk.levels = [][]T{make([]T, 0, kllLevelCap(k, 1, 0))}
	return sk
}

// Add inserts a value into the sketch.
func (sk *KLL[T]) Add(val T) {
	sk.n++
	if len(sk.levels[0]) >= kllLevelCap(sk.k, len(sk.levels), 0) {
		sk.compactLevel(0)
	}
	sk.levels[0] = append(sk.levels[0], val)
}

// compactLevel sorts level h if needed (h==0 only), randomly halves it, and
// merges the survivors (sorted) into level h+1. If h+1 then overflows,
// recursively compacts up the chain.
func (sk *KLL[T]) compactLevel(h int) {
	// Ensure level h+1 exists.
	if h+1 >= len(sk.levels) {
		newCap := kllLevelCap(sk.k, len(sk.levels)+1, h+1)
		sk.levels = append(sk.levels, make([]T, 0, newCap))
	}

	items := sk.levels[h]

	// Level 0 is the unsorted input buffer; sort before halving.
	// Higher levels are always sorted by the invariant.
	if h == 0 {
		slices.Sort(items)
	}

	// Odd count: keep the first item at this level unchanged (the "leftover").
	var leftover T
	hasLeftover := len(items)%2 == 1
	if hasLeftover {
		leftover = items[0]
		items = items[1:]
	}

	// Randomly keep every other item starting at offset 0 or 1.
	// Striding through a sorted slice preserves sort order.
	offset := kllRandomBit(&sk.rng)
	half := make([]T, 0, len(items)/2)
	for i := offset; i < len(items); i += 2 {
		half = append(half, items[i])
	}

	// Merge the sorted half into the sorted level h+1.
	sk.levels[h+1] = kllMerge(half, sk.levels[h+1])

	// Reset level h, retaining the leftover item if present.
	if hasLeftover {
		sk.levels[h] = sk.levels[h][:1]
		sk.levels[h][0] = leftover
	} else {
		sk.levels[h] = sk.levels[h][:0]
	}

	// Recurse if the level above now overflows.
	if len(sk.levels[h+1]) >= kllLevelCap(sk.k, len(sk.levels), h+1) {
		sk.compactLevel(h + 1)
	}
}

// kllMerge merges two sorted slices into a new sorted slice.
func kllMerge[T cmp.Ordered](a, b []T) []T {
	out := make([]T, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] <= b[j] {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// Count returns the total number of values added.
func (sk *KLL[T]) Count() int { return sk.n }

// Boundaries returns up to nBuckets+1 boundary values that divide the observed
// distribution into approximately equal-weight buckets.
// Returns nil if no values have been added.
func (sk *KLL[T]) Boundaries(nBuckets int) []T {
	if sk.n == 0 || nBuckets <= 0 {
		return nil
	}

	// Build a sorted weighted view across all levels.
	// An item at level h represents 2^h original items (it survived h compactions).
	type entry struct {
		val    T
		weight int64
	}

	total := 0
	for _, lv := range sk.levels {
		total += len(lv)
	}

	all := make([]entry, 0, total)
	for h, lv := range sk.levels {
		w := int64(1) << h //nolint:gosec // h is bounded by log2(n) ≈ 50; safe shift
		if h == 0 {
			// Sort a copy so the live buffer stays unsorted.
			tmp := make([]T, len(lv))
			copy(tmp, lv)
			slices.Sort(tmp)
			for _, v := range tmp {
				all = append(all, entry{v, w})
			}
		} else {
			// Higher levels are already sorted.
			for _, v := range lv {
				all = append(all, entry{v, w})
			}
		}
	}

	// Merge all levels into one sorted sequence.
	slices.SortStableFunc(all, func(a, b entry) int { return cmp.Compare(a.val, b.val) })

	// Convert to cumulative weights.
	totalWeight := int64(0)
	for i := range all {
		totalWeight += all[i].weight
		all[i].weight = totalWeight
	}
	if totalWeight == 0 {
		return nil
	}

	// Extract nBuckets+1 evenly-spaced quantile boundaries.
	bounds := make([]T, 0, nBuckets+1)
	for i := range nBuckets + 1 {
		target := int64(float64(i) * float64(totalWeight) / float64(nBuckets))
		target = min(target, totalWeight)
		// Binary search: first entry with cumWeight >= target.
		lo, hi := 0, len(all)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if all[mid].weight < target {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo >= len(all) {
			lo = len(all) - 1
		}
		bounds = append(bounds, all[lo].val)
	}

	return bounds
}

// KLLBytes is a KLL sketch for []byte values using lexicographic ordering.
type KLLBytes struct {
	levels [][][]byte
	k      int
	n      int
	rng    uint64 // per-instance xorshift64 state; never zero after init
}

// NewKLLBytes creates a new KLLBytes sketch.
func NewKLLBytes() *KLLBytes {
	sk := &KLLBytes{k: kllBytesDefaultK, rng: 0xdeadbeefcafebabe}
	sk.levels = [][][]byte{make([][]byte, 0, kllLevelCap(kllBytesDefaultK, 1, 0))}
	return sk
}

// Add inserts a copy of val into the sketch.
func (sk *KLLBytes) Add(val []byte) {
	sk.n++
	if len(sk.levels[0]) >= kllLevelCap(sk.k, len(sk.levels), 0) {
		sk.compactLevel(0)
	}
	cp := make([]byte, len(val))
	copy(cp, val)
	sk.levels[0] = append(sk.levels[0], cp)
}

func (sk *KLLBytes) compactLevel(h int) {
	if h+1 >= len(sk.levels) {
		newCap := kllLevelCap(sk.k, len(sk.levels)+1, h+1)
		sk.levels = append(sk.levels, make([][]byte, 0, newCap))
	}

	items := sk.levels[h]

	if h == 0 {
		slices.SortFunc(items, bytes.Compare)
	}

	var leftover []byte
	hasLeftover := len(items)%2 == 1
	if hasLeftover {
		leftover = items[0]
		items = items[1:]
	}

	offset := kllRandomBit(&sk.rng)
	half := make([][]byte, 0, len(items)/2)
	for i := offset; i < len(items); i += 2 {
		half = append(half, items[i])
	}

	sk.levels[h+1] = kllMergeBytes(half, sk.levels[h+1])

	if hasLeftover {
		sk.levels[h] = sk.levels[h][:1]
		sk.levels[h][0] = leftover
	} else {
		sk.levels[h] = sk.levels[h][:0]
	}

	if len(sk.levels[h+1]) >= kllLevelCap(sk.k, len(sk.levels), h+1) {
		sk.compactLevel(h + 1)
	}
}

func kllMergeBytes(a, b [][]byte) [][]byte {
	out := make([][]byte, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if bytes.Compare(a[i], b[j]) <= 0 {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// Boundaries returns up to nBuckets+1 lexicographically-ordered boundary values.
// Returns nil if no values have been added.
func (sk *KLLBytes) Boundaries(nBuckets int) [][]byte {
	if sk.n == 0 || nBuckets <= 0 {
		return nil
	}

	type entry struct {
		val    []byte
		weight int64
	}

	total := 0
	for _, lv := range sk.levels {
		total += len(lv)
	}

	all := make([]entry, 0, total)
	for h, lv := range sk.levels {
		w := int64(1) << h //nolint:gosec // h is bounded by log2(n) ≈ 50; safe shift
		if h == 0 {
			tmp := make([][]byte, len(lv))
			copy(tmp, lv)
			slices.SortFunc(tmp, bytes.Compare)
			for _, v := range tmp {
				all = append(all, entry{v, w})
			}
		} else {
			for _, v := range lv {
				all = append(all, entry{v, w})
			}
		}
	}

	slices.SortStableFunc(all, func(a, b entry) int { return bytes.Compare(a.val, b.val) })

	totalWeight := int64(0)
	for i := range all {
		totalWeight += all[i].weight
		all[i].weight = totalWeight
	}
	if totalWeight == 0 {
		return nil
	}

	bounds := make([][]byte, 0, nBuckets+1)
	for i := range nBuckets + 1 {
		target := int64(float64(i) * float64(totalWeight) / float64(nBuckets))
		target = min(target, totalWeight)
		lo, hi := 0, len(all)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if all[mid].weight < target {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo >= len(all) {
			lo = len(all) - 1
		}
		bounds = append(bounds, all[lo].val)
	}

	return bounds
}

// KLLString wraps KLL[string] for API compatibility with the range index builder.
type KLLString struct {
	inner *KLL[string]
}

// NewKLLString creates a new KLLString sketch.
func NewKLLString() *KLLString {
	return &KLLString{inner: NewKLL[string]()}
}

// Add inserts a string value into the sketch.
func (k *KLLString) Add(val string) {
	k.inner.Add(val)
}

// Boundaries returns up to nBuckets+1 lexicographically-ordered string boundary values.
func (k *KLLString) Boundaries(nBuckets int) []string {
	return k.inner.Boundaries(nBuckets)
}

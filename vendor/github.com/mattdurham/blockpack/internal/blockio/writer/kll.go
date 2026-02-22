package writer

import (
	"cmp"
	"math"
	"math/rand"
	"sort"
)

// KLL implements the KLL (Karnin-Lang-Liberty) sketch for streaming quantiles.
// This is a space-optimal algorithm for approximating quantiles over data streams.
// Space complexity: O(k + log n) where k is the sketch size parameter and n is stream length.
//
// Based on: https://github.com/edoliberty/streaming-quantiles
// Paper: "Optimal Quantile Approximation in Streams" (FOCS 2016)
type KLL[T cmp.Ordered] struct {
	compactors []*Compactor[T] // Array of compactors at different heights
	k          int             // Controls sketch size (at most 3k+log2(n) elements)
	c          float64         // Compaction parameter (default 2/3)
	H          int             // Current height (number of levels)
	size       int             // Current total number of items
	maxSize    int             // Maximum capacity before compression needed
	lazy       bool            // If true, only compact one level per compress
	alternate  bool            // If true, alternate offset direction
}

// Compactor stores items at a specific level in the KLL sketch.
// When full, it compacts by sorting and randomly sampling half the items.
type Compactor[T cmp.Ordered] struct {
	items         []T
	numCompaction int
	offset        int
	alternate     bool
}

// NewKLL creates a new KLL sketch with the given size parameter k.
// The sketch will use at most 3k+log2(n) elements where n is the stream length.
// Larger k values provide better accuracy but use more memory.
// Typical values: k=128 for moderate accuracy, k=256 for high accuracy.
func NewKLL[T cmp.Ordered](k int) *KLL[T] {
	if k <= 0 {
		k = 128 // Default value
	}
	kll := &KLL[T]{
		k:          k,
		c:          2.0 / 3.0,
		lazy:       true,
		alternate:  true,
		compactors: make([]*Compactor[T], 0),
	}
	kll.grow()
	return kll
}

// grow adds a new compactor level and updates capacity
func (kll *KLL[T]) grow() {
	kll.compactors = append(kll.compactors, &Compactor[T]{
		items:     make([]T, 0),
		alternate: kll.alternate,
	})
	kll.H = len(kll.compactors)
	kll.maxSize = 0
	for h := 0; h < kll.H; h++ {
		kll.maxSize += kll.capacity(h)
	}
}

// capacity returns the maximum capacity at a given height
func (kll *KLL[T]) capacity(height int) int {
	depth := kll.H - height - 1
	return int(math.Ceil(math.Pow(kll.c, float64(depth))*float64(kll.k))) + 1
}

// Update adds a new item to the sketch
func (kll *KLL[T]) Update(item T) {
	kll.compactors[0].items = append(kll.compactors[0].items, item)
	kll.size++
	if kll.size >= kll.maxSize {
		kll.compress()
	}
}

// compress compacts levels when capacity is exceeded
func (kll *KLL[T]) compress() {
	for h := 0; h < len(kll.compactors); h++ {
		if len(kll.compactors[h].items) >= kll.capacity(h) {
			// Grow if needed
			if h+1 >= kll.H {
				kll.grow()
			}
			// Compact this level and promote to next
			compacted := kll.compactors[h].compact()
			kll.compactors[h+1].items = append(kll.compactors[h+1].items, compacted...)
			kll.size = 0
			for _, c := range kll.compactors {
				kll.size += len(c.items)
			}
			// Lazy: only compact one level per call
			if kll.lazy {
				break
			}
		}
	}
}

// compact sorts and samples half the items from this compactor
func (c *Compactor[T]) compact() []T {
	// Determine offset (alternates or random)
	if c.numCompaction%2 == 1 && c.alternate {
		c.offset = 1 - c.offset
	} else {
		if rand.Float64() < 0.5 { //nolint:gosec
			c.offset = 1
		} else {
			c.offset = 0
		}
	}

	// Sort items
	sort.Slice(c.items, func(i, j int) bool {
		return c.items[i] < c.items[j]
	})

	// Handle odd length
	var lastItem *T
	if len(c.items)%2 == 1 {
		last := c.items[len(c.items)-1]
		lastItem = &last
		c.items = c.items[:len(c.items)-1]
	}

	// Sample every other item starting at offset
	result := make([]T, 0, len(c.items)/2)
	for i := c.offset; i < len(c.items); i += 2 {
		result = append(result, c.items[i])
	}

	// Clear and keep last item if any
	c.items = c.items[:0]
	if lastItem != nil {
		c.items = append(c.items, *lastItem)
	}

	c.numCompaction++
	return result
}

// itemWeight represents an item with its weight in the sketch
type itemWeight[T cmp.Ordered] struct {
	item   T
	weight int
}

// Ranks returns all items with their cumulative ranks (weights)
func (kll *KLL[T]) Ranks() []itemWeight[T] { //nolint:revive
	// Collect all items with their weights (2^height)
	itemsAndWeights := make([]itemWeight[T], 0, kll.size)
	for h, compactor := range kll.compactors {
		weight := 1 << h // 2^h
		for _, item := range compactor.items {
			itemsAndWeights = append(itemsAndWeights, itemWeight[T]{item, weight})
		}
	}

	// Sort by item value
	sort.Slice(itemsAndWeights, func(i, j int) bool {
		return itemsAndWeights[i].item < itemsAndWeights[j].item
	})

	// Compute cumulative weights
	cumWeight := 0
	result := make([]itemWeight[T], len(itemsAndWeights))
	for i, iw := range itemsAndWeights {
		cumWeight += iw.weight
		result[i] = itemWeight[T]{iw.item, cumWeight}
	}

	return result
}

// ComputeQuantiles returns quantile boundaries for numBuckets.
// Returns boundaries array of length numBuckets-1.
// Example: numBuckets=256 returns 255 boundary values that split the distribution.
// ComputeQuantiles returns exactly numBuckets+1 boundary values defining numBuckets
// equal-weight buckets: [b[0],b[1]), [b[1],b[2]), ..., [b[n-1],b[n]].
// The format matches CalculateBucketsFromValues for consistent downstream usage.
func (kll *KLL[T]) ComputeQuantiles(numBuckets int) []T {
	if numBuckets <= 0 || kll.size == 0 {
		return nil
	}

	ranks := kll.Ranks()
	if len(ranks) == 0 {
		return nil
	}

	minVal := ranks[0].item
	maxVal := ranks[len(ranks)-1].item

	if numBuckets == 1 || minVal == maxVal {
		return []T{minVal, maxVal}
	}

	totalWeight := ranks[len(ranks)-1].weight
	boundaries := make([]T, numBuckets+1)
	boundaries[0] = minVal
	boundaries[numBuckets] = maxVal

	for i := 1; i < numBuckets; i++ {
		targetRank := int(float64(totalWeight) * float64(i) / float64(numBuckets))

		idx := sort.Search(len(ranks), func(j int) bool {
			return ranks[j].weight >= targetRank
		})

		if idx < len(ranks) {
			boundary := ranks[idx].item
			// Ensure monotonic non-decreasing
			if boundary < boundaries[i-1] {
				boundary = boundaries[i-1]
			}
			boundaries[i] = boundary
		} else {
			boundaries[i] = maxVal
		}
	}

	return boundaries
}

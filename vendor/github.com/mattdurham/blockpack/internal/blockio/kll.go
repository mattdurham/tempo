package blockio

import (
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
type KLL struct {
	k          int          // Controls sketch size (at most 3k+log2(n) elements)
	c          float64      // Compaction parameter (default 2/3)
	lazy       bool         // If true, only compact one level per compress
	alternate  bool         // If true, alternate offset direction
	compactors []*Compactor // Array of compactors at different heights
	H          int          // Current height (number of levels)
	size       int          // Current total number of items
	maxSize    int          // Maximum capacity before compression needed
}

// Compactor stores items at a specific level in the KLL sketch.
// When full, it compacts by sorting and randomly sampling half the items.
type Compactor struct {
	items         []int64
	numCompaction int
	offset        int
	alternate     bool
}

// NewKLL creates a new KLL sketch with the given size parameter k.
// The sketch will use at most 3k+log2(n) elements where n is the stream length.
// Larger k values provide better accuracy but use more memory.
// Typical values: k=128 for moderate accuracy, k=256 for high accuracy.
func NewKLL(k int) *KLL {
	if k <= 0 {
		k = 128 // Default value
	}
	kll := &KLL{
		k:          k,
		c:          2.0 / 3.0,
		lazy:       true,
		alternate:  true,
		compactors: make([]*Compactor, 0),
	}
	kll.grow()
	return kll
}

// grow adds a new compactor level and updates capacity
func (kll *KLL) grow() {
	kll.compactors = append(kll.compactors, &Compactor{
		items:     make([]int64, 0),
		alternate: kll.alternate,
	})
	kll.H = len(kll.compactors)
	kll.maxSize = 0
	for h := 0; h < kll.H; h++ {
		kll.maxSize += kll.capacity(h)
	}
}

// capacity returns the maximum capacity at a given height
func (kll *KLL) capacity(height int) int {
	depth := kll.H - height - 1
	return int(math.Ceil(math.Pow(kll.c, float64(depth))*float64(kll.k))) + 1
}

// Update adds a new item to the sketch
func (kll *KLL) Update(item int64) {
	kll.compactors[0].items = append(kll.compactors[0].items, item)
	kll.size++
	if kll.size >= kll.maxSize {
		kll.compress()
	}
}

// compress compacts levels when capacity is exceeded
func (kll *KLL) compress() {
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
func (c *Compactor) compact() []int64 {
	// Determine offset (alternates or random)
	if c.numCompaction%2 == 1 && c.alternate {
		c.offset = 1 - c.offset
	} else {
		if rand.Float64() < 0.5 {
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
	var lastItem *int64
	if len(c.items)%2 == 1 {
		last := c.items[len(c.items)-1]
		lastItem = &last
		c.items = c.items[:len(c.items)-1]
	}

	// Sample every other item starting at offset
	result := make([]int64, 0, len(c.items)/2)
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
type itemWeight struct {
	item   int64
	weight int
}

// Ranks returns all items with their cumulative ranks (weights)
func (kll *KLL) Ranks() []itemWeight {
	// Collect all items with their weights (2^height)
	itemsAndWeights := make([]itemWeight, 0, kll.size)
	for h, compactor := range kll.compactors {
		weight := 1 << h // 2^h
		for _, item := range compactor.items {
			itemsAndWeights = append(itemsAndWeights, itemWeight{item, weight})
		}
	}

	// Sort by item value
	sort.Slice(itemsAndWeights, func(i, j int) bool {
		return itemsAndWeights[i].item < itemsAndWeights[j].item
	})

	// Compute cumulative weights
	cumWeight := 0
	result := make([]itemWeight, len(itemsAndWeights))
	for i, iw := range itemsAndWeights {
		cumWeight += iw.weight
		result[i] = itemWeight{iw.item, cumWeight}
	}

	return result
}

// ComputeQuantiles returns quantile boundaries for numBuckets.
// Returns boundaries array of length numBuckets-1.
// Example: numBuckets=256 returns 255 boundary values that split the distribution.
func (kll *KLL) ComputeQuantiles(numBuckets int) []int64 {
	if numBuckets <= 1 || kll.size == 0 {
		return nil
	}

	ranks := kll.Ranks()
	if len(ranks) == 0 {
		return nil
	}

	totalWeight := ranks[len(ranks)-1].weight
	boundaries := make([]int64, 0, numBuckets-1)

	// For each bucket boundary, find the item at that quantile
	for i := 1; i < numBuckets; i++ {
		targetRank := int(float64(totalWeight) * float64(i) / float64(numBuckets))

		// Binary search for the item at this rank
		idx := sort.Search(len(ranks), func(j int) bool {
			return ranks[j].weight >= targetRank
		})

		if idx < len(ranks) {
			// Avoid duplicate boundaries
			if len(boundaries) == 0 || ranks[idx].item != boundaries[len(boundaries)-1] {
				boundaries = append(boundaries, ranks[idx].item)
			}
		}
	}

	return boundaries
}

// Size returns the current number of items in the sketch
func (kll *KLL) Size() int {
	return kll.size
}

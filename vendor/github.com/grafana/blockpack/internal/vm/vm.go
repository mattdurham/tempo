package vm

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"time"
)

// AggBucket holds aggregation state for a single group.
// A JSON-friendly payload used to exist but was removed; this is the sole bucket struct.
type AggBucket struct {
	GroupKey GroupKey
	Sum      float64
	Count    int64
	Rate     float64
	Min      float64
	Max      float64
}

// Merge combines another AggBucket into this one.
// This is used to merge aggregation results from multiple files.
func (b *AggBucket) Merge(other *AggBucket) {
	if other == nil {
		return
	}

	// Sum the counts
	b.Count += other.Count

	// Sum the sums
	b.Sum += other.Sum

	// Take the minimum
	if other.Count > 0 {
		if b.Count == other.Count {
			// This is the first bucket being merged
			b.Min = other.Min
		} else if other.Min < b.Min {
			b.Min = other.Min
		}
	}

	// Take the maximum
	if other.Count > 0 {
		if b.Count == other.Count {
			// This is the first bucket being merged
			b.Max = other.Max
		} else if other.Max > b.Max {
			b.Max = other.Max
		}
	}

	// Rate is recalculated after merge, not merged directly
}

// MergeAggregationResults combines multiple aggregation result sets into one.
// This is used to merge results from multiple blockpack files.
// Returns a new map with merged buckets.
func MergeAggregationResults(results ...map[string]*AggBucket) map[string]*AggBucket {
	if len(results) == 0 {
		return make(map[string]*AggBucket)
	}

	if len(results) == 1 {
		return results[0]
	}

	merged := make(map[string]*AggBucket)

	for _, resultSet := range results {
		for key, bucket := range resultSet {
			if existing, ok := merged[key]; ok {
				// Merge into existing bucket
				existing.Merge(bucket)
			} else {
				// Shallow copy is safe: GroupKey.Values slice is read-only after construction.
				// Using struct copy ensures new AggBucket fields are automatically included.
				b := *bucket
				merged[key] = &b
			}
		}
	}

	return merged
}

// GroupKey represents a unique combination of group-by field values
type GroupKey struct {
	Values []Value // Ordered by GROUP BY fields in query; read-only after construction — shared by shallow-copied AggBuckets.
}

// DateBinInfo holds date_bin configuration
type DateBinInfo struct {
	Interval        time.Duration // e.g., 5*time.Minute for "5m"
	OriginTimestamp int64         // Query start time (Unix nanoseconds)
}

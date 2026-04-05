// Package sketch provides HyperLogLog, Count-Min Sketch, and BinaryFuse8
// implementations for per-block cardinality estimation, frequency estimation,
// and value membership testing.
//
// NOTE: TopK tracks the top-K most frequent values per column per block using the
// Space-Saving algorithm (Metwally, Agrawal, Abbadi 2005).
// SPEC-SK-17 through SPEC-SK-18 govern this file.
//
// Space-Saving guarantees:
//   - Any fp with true frequency > n/K is guaranteed to appear in the output.
//   - Reported counts are overestimates by at most n/K (n = total observations).
//   - Memory is bounded to K=20 entries regardless of column cardinality.
//
// Implementation uses a fixed [TopKSize]TopKEntry array instead of a map.
// This eliminates all hash-table allocations and GC scanning on the hot write path.
// Memory: 20 × 16 bytes = 320 bytes per column per block (uint64+uint32+4-byte pad), stack-friendly.
package sketch

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"slices"
)

const (
	// TopKSize is the maximum number of top-K entries stored per column per block.
	TopKSize = 20
)

// TopKEntry holds one value's fingerprint and its approximate count.
type TopKEntry struct {
	FP    uint64 // hash fingerprint of the original key (via HashForFuse)
	Count uint32
}

// TopK tracks the top-K most frequent values using the Space-Saving algorithm.
// The internal array is fixed at TopKSize entries. When full, the entry with
// the lowest count is evicted and replaced by the new fp at evicted_count+1,
// ensuring no true top-K member is missed.
// Not safe for concurrent use.
type TopK struct {
	entries [TopKSize]TopKEntry
	n       int
}

// NewTopK creates an empty TopK accumulator.
func NewTopK() *TopK {
	return &TopK{}
}

// Reset clears all entries without allocating.
func (t *TopK) Reset() {
	t.n = 0
}

// Add hashes key via HashForFuse and calls AddFP. (SPEC-SK-16)
func (t *TopK) Add(key string) {
	t.AddFP(HashForFuse(key))
}

// AddFP applies the Space-Saving algorithm for a pre-computed fingerprint.
// If fp is already tracked, its count is incremented exactly.
// If the table has room, fp is inserted with count=1.
// If the table is full, the minimum-count entry is replaced by fp at min_count+1,
// bounding the overestimate error to at most n/K across all entries.
// All operations are O(K) linear scans over at most TopKSize=20 entries — no map overhead.
func (t *TopK) AddFP(fp uint64) {
	// Fast path: fp already tracked — O(n) scan over at most TopKSize entries.
	for i := range t.n {
		if t.entries[i].FP == fp {
			t.entries[i].Count++
			return
		}
	}
	if t.n < TopKSize {
		t.entries[t.n] = TopKEntry{FP: fp, Count: 1}
		t.n++
		return
	}
	// Table full: find the minimum-count entry — O(K) scan over exactly TopKSize entries.
	minIdx := 0
	for i := 1; i < TopKSize; i++ {
		if t.entries[i].Count < t.entries[minIdx].Count {
			minIdx = i
		}
	}
	// Replace minimum with new entry at min_count+1 (Space-Saving invariant).
	t.entries[minIdx] = TopKEntry{FP: fp, Count: t.entries[minIdx].Count + 1}
}

// Entries returns the top-K entries sorted by count descending. (SPEC-SK-17)
// Returns at most TopKSize entries.
func (t *TopK) Entries() []TopKEntry {
	if t.n == 0 {
		return nil
	}
	entries := make([]TopKEntry, t.n)
	copy(entries, t.entries[:t.n])
	slices.SortFunc(entries, func(a, b TopKEntry) int {
		if a.Count != b.Count {
			return cmp.Compare(b.Count, a.Count)
		}
		return cmp.Compare(a.FP, b.FP) // deterministic secondary sort by fingerprint
	})
	return entries
}

// MarshalBinary serializes the top-K entries to bytes.
// Wire format: topk_count[1] + per_entry { fp[8 LE uint64] + count[4 LE uint32] }
// Returns at most TopKSize entries.
func (t *TopK) MarshalBinary() []byte {
	entries := t.Entries()
	n := len(entries)
	buf := make([]byte, 0, 1+n*12) // 1 byte count + n×(8 FP + 4 count)
	buf = append(buf, byte(n))     //nolint:gosec // safe: n <= TopKSize = 20
	for _, e := range entries {
		var tmp8 [8]byte
		binary.LittleEndian.PutUint64(tmp8[:], e.FP)
		buf = append(buf, tmp8[:]...)
		var tmp4 [4]byte
		binary.LittleEndian.PutUint32(tmp4[:], e.Count)
		buf = append(buf, tmp4[:]...)
	}
	return buf
}

// UnmarshalTopKBinary deserializes top-K entries from data.
// Returns the deserialized entries and the number of bytes consumed.
func UnmarshalTopKBinary(data []byte) ([]TopKEntry, int, error) {
	if len(data) < 1 {
		return nil, 0, fmt.Errorf("topk: too short for entry count")
	}
	n := int(data[0])
	pos := 1
	if n > TopKSize {
		return nil, 0, fmt.Errorf("topk: entry count %d exceeds TopKSize %d", n, TopKSize)
	}
	entries := make([]TopKEntry, 0, n)
	for i := range n {
		if pos+12 > len(data) {
			return nil, 0, fmt.Errorf("topk: entry %d: too short for fp+count", i)
		}
		fp := binary.LittleEndian.Uint64(data[pos:])
		pos += 8
		count := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		entries = append(entries, TopKEntry{FP: fp, Count: count})
	}
	return entries, pos, nil
}

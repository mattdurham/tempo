// Package sketch provides HyperLogLog, Count-Min Sketch, and BinaryFuse8
// implementations for per-block cardinality estimation, frequency estimation,
// and value membership testing.
//
// NOTE: TopK tracks the top-K most frequent values per column per block.
// SPEC-SK-17 through SPEC-SK-18 govern this file.
// Simple exact-count approach (map[string]uint32); block size bounds memory.
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
	// TopKMaxKeyLen is the maximum byte length for a TopK key. Longer keys are truncated.
	TopKMaxKeyLen = 100
)

// TopKEntry holds one value and its exact count.
type TopKEntry struct {
	Key   string
	Count uint32
}

// TopK tracks the top-K most frequent values using an exact frequency map.
// Not safe for concurrent use. NOTE-SK-06: bounded by block size, so exact counts are safe.
type TopK struct {
	counts map[string]uint32
}

// NewTopK creates an empty TopK accumulator.
func NewTopK() *TopK {
	return &TopK{counts: make(map[string]uint32, TopKSize*2)}
}

// Add increments the count for key. Keys longer than TopKMaxKeyLen are truncated. (SPEC-SK-18)
func (t *TopK) Add(key string) {
	if len(key) > TopKMaxKeyLen {
		key = key[:TopKMaxKeyLen]
	}
	t.counts[key]++
}

// Entries returns the top-K entries sorted by count descending. (SPEC-SK-17)
// Returns at most TopKSize entries.
func (t *TopK) Entries() []TopKEntry {
	if len(t.counts) == 0 {
		return nil
	}
	entries := make([]TopKEntry, 0, len(t.counts))
	for k, c := range t.counts {
		entries = append(entries, TopKEntry{Key: k, Count: c})
	}
	slices.SortFunc(entries, func(a, b TopKEntry) int {
		if a.Count != b.Count {
			return cmp.Compare(b.Count, a.Count)
		}
		return cmp.Compare(a.Key, b.Key) // deterministic secondary sort by key
	})
	if len(entries) > TopKSize {
		entries = entries[:TopKSize]
	}
	return entries
}

// MarshalBinary serializes the top-K entries to bytes.
// Wire format: topk_count[1] + per_entry { key_len[2 LE] + key[N] + count[4 LE] }
// Keys longer than TopKMaxKeyLen are truncated. (SPEC-SK-18)
// Returns at most TopKSize entries.
func (t *TopK) MarshalBinary() []byte {
	entries := t.Entries()
	n := len(entries)
	// Estimate capacity: 1 (count byte) + n * (2 + TopKMaxKeyLen + 4)
	buf := make([]byte, 0, 1+n*(2+TopKMaxKeyLen+4))
	buf = append(buf, byte(n)) //nolint:gosec // safe: n <= TopKSize = 20
	for _, e := range entries {
		key := e.Key
		if len(key) > TopKMaxKeyLen {
			key = key[:TopKMaxKeyLen]
		}
		kl := len(key)
		buf = append(buf, byte(kl), byte(kl>>8)) //nolint:gosec // safe: kl <= 100
		buf = append(buf, key...)
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
		if pos+2 > len(data) {
			return nil, 0, fmt.Errorf("topk: entry %d: too short for key_len", i)
		}
		kl := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if kl > TopKMaxKeyLen {
			return nil, 0, fmt.Errorf("topk: entry %d: key_len %d exceeds max %d", i, kl, TopKMaxKeyLen)
		}
		if pos+kl > len(data) {
			return nil, 0, fmt.Errorf("topk: entry %d: key data too short: need %d, have %d", i, kl, len(data)-pos)
		}
		key := string(data[pos : pos+kl])
		pos += kl
		if pos+4 > len(data) {
			return nil, 0, fmt.Errorf("topk: entry %d: too short for count", i)
		}
		count := binary.LittleEndian.Uint32(data[pos:])
		pos += 4
		entries = append(entries, TopKEntry{Key: key, Count: count})
	}
	return entries, pos, nil
}

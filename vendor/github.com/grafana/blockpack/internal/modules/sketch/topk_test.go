package sketch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/sketch"
)

// SK-T-17: TestTopK_Empty — empty TopK returns nil entries.
func TestTopK_Empty(t *testing.T) {
	tk := sketch.NewTopK()
	assert.Nil(t, tk.Entries(), "empty TopK must return nil entries")
}

// SK-T-18: TestTopK_AddAndEntries — basic add and retrieval, sorted by count descending.
func TestTopK_AddAndEntries(t *testing.T) {
	tk := sketch.NewTopK()
	tk.Add("a")
	tk.Add("b")
	tk.Add("b")
	tk.Add("c")
	tk.Add("c")
	tk.Add("c")

	entries := tk.Entries()
	require.Len(t, entries, 3)
	assert.Equal(t, sketch.HashForFuse("c"), entries[0].FP)
	assert.Equal(t, uint32(3), entries[0].Count)
	assert.Equal(t, sketch.HashForFuse("b"), entries[1].FP)
	assert.Equal(t, uint32(2), entries[1].Count)
	assert.Equal(t, sketch.HashForFuse("a"), entries[2].FP)
	assert.Equal(t, uint32(1), entries[2].Count)
}

// SK-T-19: TestTopK_ExceedsK — only top-K returned when more than K distinct values added.
func TestTopK_ExceedsK(t *testing.T) {
	tk := sketch.NewTopK()
	// Add 30 distinct values; top-K=20 should return only 20.
	for i := range 30 {
		for range i + 1 {
			tk.Add(string(rune('A' + i%26)))
		}
	}
	entries := tk.Entries()
	assert.LessOrEqual(t, len(entries), sketch.TopKSize, "must return at most TopKSize entries")
}

// SK-T-21: TestTopK_MarshalRoundTrip — marshal/unmarshal preserves all entries.
func TestTopK_MarshalRoundTrip(t *testing.T) {
	tk := sketch.NewTopK()
	tk.Add("alpha")
	tk.Add("alpha")
	tk.Add("beta")
	tk.Add("gamma")
	tk.Add("gamma")
	tk.Add("gamma")

	data := tk.MarshalBinary()
	entries, consumed, err := sketch.UnmarshalTopKBinary(data)
	require.NoError(t, err)
	assert.Equal(t, len(data), consumed, "all bytes should be consumed")
	require.Len(t, entries, 3)
	assert.Equal(t, sketch.HashForFuse("gamma"), entries[0].FP)
	assert.Equal(t, uint32(3), entries[0].Count)
	assert.Equal(t, sketch.HashForFuse("alpha"), entries[1].FP)
	assert.Equal(t, uint32(2), entries[1].Count)
	assert.Equal(t, sketch.HashForFuse("beta"), entries[2].FP)
	assert.Equal(t, uint32(1), entries[2].Count)
}

// SK-T-22: TestTopK_EmptyMarshalRoundTrip — empty TopK marshals to 1 byte (count=0).
func TestTopK_EmptyMarshalRoundTrip(t *testing.T) {
	tk := sketch.NewTopK()
	data := tk.MarshalBinary()
	assert.Len(t, data, 1, "empty TopK must marshal to 1 byte")
	entries, consumed, err := sketch.UnmarshalTopKBinary(data)
	require.NoError(t, err)
	assert.Equal(t, 1, consumed)
	assert.Empty(t, entries)
}

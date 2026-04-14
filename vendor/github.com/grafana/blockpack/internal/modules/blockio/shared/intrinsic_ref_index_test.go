package shared

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"sync"
	"testing"
)

// TestEnsureRefIndex_Flat builds a flat IntrinsicColumn with known uint64 values and
// refs, calls EnsureRefIndex, then verifies LookupRefFast returns correct values for
// known refs and (nil, false) for unknown refs.
func TestEnsureRefIndex_Flat(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatFlat,
		Type:   ColumnTypeUint64,
		Uint64Values: []uint64{
			1000,
			2000,
			3000,
		},
		BlockRefs: []BlockRef{
			{BlockIdx: 0, RowIdx: 5},
			{BlockIdx: 1, RowIdx: 10},
			{BlockIdx: 2, RowIdx: 15},
		},
		Count: 3,
	}

	col.EnsureRefIndex()

	tests := []struct {
		name      string
		wantVal   uint64
		blockIdx  uint16
		rowIdx    uint16
		wantFound bool
	}{
		{"first ref", 1000, 0, 5, true},
		{"middle ref", 2000, 1, 10, true},
		{"last ref", 3000, 2, 15, true},
		{"unknown ref", 0, 9, 9, false},
		{"wrong row", 0, 0, 6, false},
		{"wrong block", 0, 3, 5, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := uint32(tt.blockIdx)<<16 | uint32(tt.rowIdx)
			val, found := col.LookupRefFast(packed)
			if found != tt.wantFound {
				t.Fatalf("LookupRefFast: found=%v, want %v", found, tt.wantFound)
			}
			if tt.wantFound {
				v, ok := val.(uint64)
				if !ok {
					t.Fatalf("val type = %T, want uint64", val)
				}
				if v != tt.wantVal {
					t.Errorf("val = %d, want %d", v, tt.wantVal)
				}
			}
		})
	}
}

// TestEnsureRefIndex_Dict builds a dict IntrinsicColumn with known string entries,
// calls EnsureRefIndex, then verifies LookupRefFast returns correct dict values.
func TestEnsureRefIndex_Dict(t *testing.T) {
	col := &IntrinsicColumn{
		Format: IntrinsicFormatDict,
		Type:   ColumnTypeString,
		DictEntries: []IntrinsicDictEntry{
			{
				Value:     "alpha",
				BlockRefs: []BlockRef{{BlockIdx: 0, RowIdx: 1}, {BlockIdx: 0, RowIdx: 2}},
			},
			{
				Value:     "beta",
				BlockRefs: []BlockRef{{BlockIdx: 1, RowIdx: 0}},
			},
			{
				Value:     "gamma",
				BlockRefs: []BlockRef{{BlockIdx: 2, RowIdx: 3}, {BlockIdx: 2, RowIdx: 4}},
			},
		},
		Count: 5,
	}

	col.EnsureRefIndex()

	tests := []struct {
		name      string
		wantVal   string
		blockIdx  uint16
		rowIdx    uint16
		wantFound bool
	}{
		{"alpha ref 0", "alpha", 0, 1, true},
		{"alpha ref 1", "alpha", 0, 2, true},
		{"beta ref", "beta", 1, 0, true},
		{"gamma ref 0", "gamma", 2, 3, true},
		{"gamma ref 1", "gamma", 2, 4, true},
		{"unknown", "", 9, 9, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			packed := uint32(tt.blockIdx)<<16 | uint32(tt.rowIdx)
			val, found := col.LookupRefFast(packed)
			if found != tt.wantFound {
				t.Fatalf("LookupRefFast: found=%v, want %v", found, tt.wantFound)
			}
			if tt.wantFound {
				s, ok := val.(string)
				if !ok {
					t.Fatalf("val type = %T, want string", val)
				}
				if s != tt.wantVal {
					t.Errorf("val = %q, want %q", s, tt.wantVal)
				}
			}
		})
	}
}

// TestEnsureRefIndex_Nil verifies that EnsureRefIndex on a nil column is a no-op
// and LookupRefFast on nil returns (nil, false).
func TestEnsureRefIndex_Nil(t *testing.T) {
	var col *IntrinsicColumn
	col.EnsureRefIndex() // must not panic

	val, found := col.LookupRefFast(0x00010001)
	if found {
		t.Errorf("LookupRefFast on nil: found=true, want false")
	}
	if val != nil {
		t.Errorf("LookupRefFast on nil: val=%v, want nil", val)
	}
}

// TestEnsureRefIndex_Concurrent calls EnsureRefIndex from 10 goroutines simultaneously
// on the same column and verifies no panic and correct results after all goroutines finish.
func TestEnsureRefIndex_Concurrent(t *testing.T) {
	col := &IntrinsicColumn{
		Format:       IntrinsicFormatFlat,
		Type:         ColumnTypeUint64,
		Uint64Values: []uint64{100, 200, 300},
		BlockRefs: []BlockRef{
			{BlockIdx: 0, RowIdx: 0},
			{BlockIdx: 0, RowIdx: 1},
			{BlockIdx: 0, RowIdx: 2},
		},
		Count: 3,
	}

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			col.EnsureRefIndex()
		}()
	}
	wg.Wait()

	// After all goroutines complete, index must be valid and lookups must succeed.
	packed := uint32(0)<<16 | uint32(1)
	val, found := col.LookupRefFast(packed)
	if !found {
		t.Fatal("LookupRefFast after concurrent EnsureRefIndex: not found")
	}
	v, ok := val.(uint64)
	if !ok || v != 200 {
		t.Errorf("LookupRefFast: val=%v, want uint64(200)", val)
	}
}

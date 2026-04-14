package writer

import (
	"testing"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// TestIntrinsicAccumMerge verifies that merging two accumulators produces the
// same result as feeding all rows into a single accumulator.
func TestIntrinsicAccumMerge(t *testing.T) {
	// Build a combined accumulator (ground truth).
	combined := newIntrinsicAccumulator()
	combined.feedUint64("span:duration", shared.ColumnTypeUint64, 100, 0, 0)
	combined.feedUint64("span:duration", shared.ColumnTypeUint64, 200, 1, 0)
	combined.feedString("span:name", shared.ColumnTypeString, "GET /foo", 0, 0)
	combined.feedString("span:name", shared.ColumnTypeString, "POST /bar", 1, 0)
	combined.feedBytes("trace:id", shared.ColumnTypeBytes, []byte{0x01, 0x02}, 0, 0)
	combined.feedInt64("span:kind", shared.ColumnTypeInt64, 2, 0, 0)
	combined.feedInt64("span:kind", shared.ColumnTypeInt64, 3, 1, 0)

	// Build two partial accumulators that mirror the same data.
	a := newIntrinsicAccumulator()
	a.feedUint64("span:duration", shared.ColumnTypeUint64, 100, 0, 0)
	a.feedString("span:name", shared.ColumnTypeString, "GET /foo", 0, 0)
	a.feedBytes("trace:id", shared.ColumnTypeBytes, []byte{0x01, 0x02}, 0, 0)
	a.feedInt64("span:kind", shared.ColumnTypeInt64, 2, 0, 0)

	b := newIntrinsicAccumulator()
	b.feedUint64("span:duration", shared.ColumnTypeUint64, 200, 1, 0)
	b.feedString("span:name", shared.ColumnTypeString, "POST /bar", 1, 0)
	b.feedInt64("span:kind", shared.ColumnTypeInt64, 3, 1, 0)

	// Merge b into a.
	a.merge(b)

	// Verify flat column counts match.
	for name, wantFlat := range combined.flatCols {
		gotFlat, ok := a.flatCols[name]
		if !ok {
			t.Errorf("flat col %q missing after merge", name)
			continue
		}
		if len(wantFlat.uint64Values) != len(gotFlat.uint64Values) {
			t.Errorf("flat col %q: uint64 len want %d got %d",
				name, len(wantFlat.uint64Values), len(gotFlat.uint64Values))
		}
		if len(wantFlat.bytesValues) != len(gotFlat.bytesValues) {
			t.Errorf("flat col %q: bytes len want %d got %d",
				name, len(wantFlat.bytesValues), len(gotFlat.bytesValues))
		}
		if len(wantFlat.refs) != len(gotFlat.refs) {
			t.Errorf("flat col %q: refs len want %d got %d",
				name, len(wantFlat.refs), len(gotFlat.refs))
		}
	}

	// Verify dict column ref counts match.
	for name, wantDict := range combined.dictCols {
		gotDict, ok := a.dictCols[name]
		if !ok {
			t.Errorf("dict col %q missing after merge", name)
			continue
		}
		wantTotal := 0
		for _, e := range wantDict.entries {
			wantTotal += len(e.refs)
		}
		gotTotal := 0
		for _, e := range gotDict.entries {
			gotTotal += len(e.refs)
		}
		if wantTotal != gotTotal {
			t.Errorf("dict col %q: total refs want %d got %d", name, wantTotal, gotTotal)
		}
	}

	// Edge case: merge an empty accumulator — must be a no-op.
	beforeFlat := len(a.flatCols)
	a.merge(newIntrinsicAccumulator())
	if len(a.flatCols) != beforeFlat {
		t.Errorf("merge of empty accumulator changed flatCols count")
	}

	// Edge case: merge into empty accumulator — must copy all columns.
	empty := newIntrinsicAccumulator()
	src := newIntrinsicAccumulator()
	src.feedUint64("span:start", shared.ColumnTypeUint64, 999, 0, 0)
	empty.merge(src)
	if _, ok := empty.flatCols["span:start"]; !ok {
		t.Errorf("merge into empty: span:start not present")
	}
}

// TestIntrinsicAccumMergeDictDedup verifies that merging two accumulators that both
// contain the same dict key produces ONE entry with refs from both sources.
func TestIntrinsicAccumMergeDictDedup(t *testing.T) {
	// Both accumulators feed the same span:name value.
	a := newIntrinsicAccumulator()
	a.feedString("span:name", shared.ColumnTypeString, "GET /foo", 0, 0)

	b := newIntrinsicAccumulator()
	b.feedString("span:name", shared.ColumnTypeString, "GET /foo", 1, 0)

	a.merge(b)

	dictCol, ok := a.dictCols["span:name"]
	if !ok {
		t.Fatal("span:name missing after merge")
	}
	if len(dictCol.entries) != 1 {
		t.Errorf("want 1 dict entry after dedup merge, got %d", len(dictCol.entries))
	}
	if len(dictCol.entries[0].refs) != 2 {
		t.Errorf("want 2 refs for deduplicated entry, got %d", len(dictCol.entries[0].refs))
	}
}

// TestIntrinsicAccumMergeRangeInt64 verifies the ColumnTypeRangeInt64 branch in merge().
// Two partial accumulators feed the same int64 value; after merge there should be
// ONE dict entry with TWO refs.
func TestIntrinsicAccumMergeRangeInt64(t *testing.T) {
	a := newIntrinsicAccumulator()
	a.feedInt64("span:kind", shared.ColumnTypeRangeInt64, 5, 0, 0)

	b := newIntrinsicAccumulator()
	b.feedInt64("span:kind", shared.ColumnTypeRangeInt64, 5, 2, 0)

	a.merge(b)

	dictCol, ok := a.dictCols["span:kind"]
	if !ok {
		t.Fatal("span:kind missing after merge")
	}
	if len(dictCol.entries) != 1 {
		t.Errorf("want 1 dict entry after RangeInt64 dedup, got %d", len(dictCol.entries))
	}
	if len(dictCol.entries[0].refs) != 2 {
		t.Errorf("want 2 refs for deduplicated RangeInt64 entry, got %d", len(dictCol.entries[0].refs))
	}
}

// TestIntrinsicAccumMergeBlockIDPreserved verifies that refs from partial accumulators
// with different block IDs are preserved unchanged after merge — no re-labeling occurs.
func TestIntrinsicAccumMergeBlockIDPreserved(t *testing.T) {
	// a uses blockIdx=0; b uses blockIdx=5 (simulating separate goroutines in flushBlocks).
	a := newIntrinsicAccumulator()
	a.feedUint64("span:duration", shared.ColumnTypeUint64, 100, 0, 0)

	b := newIntrinsicAccumulator()
	b.feedUint64("span:duration", shared.ColumnTypeUint64, 200, 5, 0)

	a.merge(b)

	flatCol, ok := a.flatCols["span:duration"]
	if !ok {
		t.Fatal("span:duration missing after merge")
	}
	// Expect 2 refs: one with BlockIdx=0 and one with BlockIdx=5.
	if len(flatCol.refs) != 2 {
		t.Fatalf("want 2 refs, got %d", len(flatCol.refs))
	}
	foundBlock0 := false
	foundBlock5 := false
	for _, ref := range flatCol.refs {
		switch ref.BlockIdx {
		case 0:
			foundBlock0 = true
		case 5:
			foundBlock5 = true
		}
	}
	if !foundBlock0 {
		t.Error("no ref with BlockIdx=0 after merge")
	}
	if !foundBlock5 {
		t.Error("no ref with BlockIdx=5 after merge; block ID was re-labeled or dropped")
	}
}

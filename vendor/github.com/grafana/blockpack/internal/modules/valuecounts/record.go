package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"bytes"

	"github.com/grafana/blockpack/internal/modules/blockio/shared"
)

// Record is one value-count row in a VCNT section (issue #400).
//
// Records are sorted by (ColumnName ASC, TimeStart ASC, Value ASC, Count ASC). The sort key
// (ColumnName, TimeStart) lets a reader binary-search to a time window and read one column's
// records sequentially; Value ordering within a (column, time) group enables merge-sort
// deduplication during count compaction.
type Record struct {
	ColumnName string
	Value      []byte // canonical-encoded column value
	TimeStart  uint64 // unix seconds, inclusive window start
	TimeEnd    uint64 // unix seconds, inclusive window end
	Count      int64  // positive: value present in Count spans; negative: delta accounting
}

// compareRecords orders by (ColumnName, TimeStart, Value, Count). It is the canonical sort
// order for VCNT records and the merge order for compaction.
func compareRecords(a, b *Record) int {
	if c := bytesCompareString(a.ColumnName, b.ColumnName); c != 0 {
		return c
	}
	if a.TimeStart != b.TimeStart {
		if a.TimeStart < b.TimeStart {
			return -1
		}
		return 1
	}
	if c := bytes.Compare(a.Value, b.Value); c != 0 {
		return c
	}
	if a.Count != b.Count {
		if a.Count < b.Count {
			return -1
		}
		return 1
	}
	return 0
}

// bytesCompareString compares two strings as byte sequences (UTF-8 lexicographic), avoiding
// a []byte allocation per comparison.
func bytesCompareString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// mergeKey identifies records that are summed together during compaction: a value within a
// specific (column, time window). TimeEnd is part of the key (NOTE-VC-002) so that two windows
// starting at the same second but ending differently are not conflated.
type mergeKey struct {
	column    string
	value     string
	timeStart uint64
	timeEnd   uint64
}

func keyOf(r *Record) mergeKey {
	return mergeKey{
		column:    r.ColumnName,
		value:     string(r.Value),
		timeStart: r.TimeStart,
		timeEnd:   r.TimeEnd,
	}
}

// Negate returns a copy of r with its Count sign flipped. The compactor emits negated copies
// of each consumed input block's records as delta-accounting entries (NOTE-VC-001, issue #400).
func Negate(r Record) Record {
	out := r
	out.Count = -r.Count
	if r.Value != nil {
		out.Value = make([]byte, len(r.Value))
		copy(out.Value, r.Value)
	}
	return out
}

// recordsPerChunk normalises a caller-supplied chunk size, falling back to the package default.
func recordsPerChunk(n int) int {
	if n <= 0 {
		return shared.ValueCountsRecordsPerChunk
	}
	return n
}

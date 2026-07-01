package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"bytes"
	"sort"
)

// ValueCount is one distinct value and its summed live span count over a query window.
type ValueCount struct {
	Value []byte
	Count int64
}

// sumLiveValues decodes the chunks overlapping [minTS, maxTS], sums Count per value for the
// requested column, and returns the values whose net count is > 0 paired with their summed
// count. order preserves first-seen value order so that callers needing a deterministic but
// unsorted listing (or a specific re-sort) have a stable basis. Values with net count <= 0 are
// dropped — they no longer exist in any live block (NOTE-VC-001, NOTE-VC-004).
func sumLiveValues(data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64) ([]ValueCount, error) {
	recs, err := DecodeTimeRange(data, dir, minTS, maxTS)
	if err != nil {
		return nil, err
	}
	sums := make(map[string]int64)
	order := make([]string, 0)
	for i := range recs {
		r := &recs[i]
		if r.ColumnName != column {
			continue
		}
		key := string(r.Value)
		if _, seen := sums[key]; !seen {
			order = append(order, key)
		}
		sums[key] += r.Count
	}

	out := make([]ValueCount, 0, len(order))
	for _, key := range order {
		if sums[key] > 0 {
			out = append(out, ValueCount{Value: []byte(key), Count: sums[key]})
		}
	}
	return out, nil
}

// ValuesInRange answers "what unique values does column have in [minTS, maxTS]?" against a
// single VCNT section (data + dir). It decodes only the chunks whose window can overlap the
// query, sums Count per value for the requested column, and returns values with Count > 0,
// sorted by Value ascending.
//
// This is the read-path primitive behind tag-value lookups (issue #400): operating on a
// consolidated .vcnt file it answers the query without opening any blockpack data files.
func ValuesInRange(data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64) ([]ValueCount, error) {
	out, err := sumLiveValues(data, dir, column, minTS, maxTS)
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Value, out[j].Value) < 0
	})
	return out, nil
}

// TopNInRange answers "what are the N most frequent values of column in [minTS, maxTS]?" — the
// tag-value autocomplete / dropdown-population primitive (issue #400, reopened requirement #3).
// It sums live counts per value (dropping net <= 0), then returns at most n values sorted by
// Count descending. Ties are broken by Value ascending so the result is deterministic for a
// given input. n <= 0 returns all live values in that ranked order.
//
// Like ValuesInRange this opens no blockpack data files — it reads only the consolidated .vcnt
// section, so a dropdown lookahead never touches a span.
func TopNInRange(data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64, n int) ([]ValueCount, error) {
	out, err := sumLiveValues(data, dir, column, minTS, maxTS)
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return bytes.Compare(out[i].Value, out[j].Value) < 0
	})
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out, nil
}

// CardinalityInRange answers "how many distinct values does column have in [minTS, maxTS]?" —
// the cardinality gate primitive for cube creation (issue #400, reopened requirement; #445).
// It counts the distinct values whose net live count is > 0, opening no blockpack data files.
func CardinalityInRange(data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64) (int, error) {
	out, err := sumLiveValues(data, dir, column, minTS, maxTS)
	if err != nil {
		return 0, err
	}
	return len(out), nil
}

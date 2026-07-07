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

// SelectivityEstimate is the result of a per-(column, value) selectivity lookup against a VCNT
// section (issue #484, Phase 1). It approximates how many spans a leaf predicate
// `column = value` would match over the query window, for cost-based leaf-resolution ordering.
//
// Covered distinguishes "the value index has no data for this (column, value, window) — treat
// as unknown selectivity" from "the value index knows this value matches zero live spans".
// The two are directionally opposite for ordering: a Covered zero-count leaf is maximally
// selective (resolve it first, it can short-circuit an AND), whereas an uncovered leaf carries
// no signal (resolve it last / by the caller's fallback policy). Callers must not collapse them.
type SelectivityEstimate struct {
	// Count is the net live span count for the value over the window. Only meaningful when
	// Covered is true. It is clamped to >= 0: a net-negative sum (more deletions than
	// introductions seen so far, an async/retention artifact) reads as zero live spans.
	Count int64
	// Covered reports whether any VCNT record was found for the (column, value, window). When
	// false, Count is zero and carries no selectivity signal — the caller should apply its
	// no-coverage fallback (e.g. "resolve last / unknown").
	Covered bool
}

// SelectivityInRange approximates the span count of the leaf predicate `column = value` over
// [minTS, maxTS] against a single VCNT section (data + dir). It is the selectivity oracle
// behind cost-based AND-leaf resolution ordering (issue #484, Phase 1, NOTE-VC-013): a cheaper,
// value-scoped counterpart to ValuesInRange that sums Count for exactly one value rather than
// enumerating every distinct value of the column.
//
// value is the canonical-encoded column value, matched byte-for-byte against Record.Value (the
// same encoding ValuesInRange returns in ValueCount.Value). Records outside the column, the
// value, or the time window are ignored; records that overlap the window are summed following
// the same signed delta-accounting liveness rule as the rest of this package (NOTE-VC-001).
//
// Estimation accuracy only needs to be directionally correct for ordering purposes — VCNT's
// async/approximate nature (batch writes, retention deltas) is acceptable (issue #484). Like
// the other read primitives it opens no blockpack data files.
func SelectivityInRange(
	data []byte,
	dir []ChunkDirEntry,
	column string,
	value []byte,
	minTS, maxTS uint64,
) (SelectivityEstimate, error) {
	recs, err := DecodeTimeRange(data, dir, minTS, maxTS)
	if err != nil {
		return SelectivityEstimate{}, err
	}
	var (
		sum     int64
		covered bool
	)
	for i := range recs {
		r := &recs[i]
		if r.ColumnName != column {
			continue
		}
		if !bytes.Equal(r.Value, value) {
			continue
		}
		covered = true
		sum += r.Count
	}
	if sum < 0 {
		sum = 0
	}
	return SelectivityEstimate{Count: sum, Covered: covered}, nil
}

// ColumnTotalInRange answers "how many live spans does column have across ALL its values in
// [minTS, maxTS]?" — the selectivity DENOMINATOR primitive (issue #486, NOTE-VC-014). Where
// SelectivityInRange gives the numerator (the net live count for one `column = value` pair),
// this gives the population that count is a fraction of, so a caller can compute a leaf's
// selectivity fraction and recognize a low-selectivity predicate (one whose value accounts for
// most of the column's spans, so index pruning would skip almost nothing).
//
// It follows the same signed delta-accounting liveness rule as the rest of this package
// (NOTE-VC-001): per-value net counts <= 0 are dropped before summing, so the total is the sum
// of the LIVE per-value counts, never a raw sum that a retention delta could push negative.
// Covered reports whether any live record was found for the column at all — when false, Total
// is zero and carries no signal (the caller applies its own no-coverage policy, mirroring
// SelectivityEstimate.Covered). It opens no blockpack data files.
func ColumnTotalInRange(data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64) (ColumnTotal, error) {
	out, err := sumLiveValues(data, dir, column, minTS, maxTS)
	if err != nil {
		return ColumnTotal{}, err
	}
	if len(out) == 0 {
		return ColumnTotal{}, nil
	}
	var total int64
	for i := range out {
		total += out[i].Count
	}
	return ColumnTotal{Total: total, Covered: true}, nil
}

// ColumnTotal is the result of ColumnTotalInRange: the live-span population of a column over a
// window. Like SelectivityEstimate it carries a Covered flag distinct from a zero Total so a
// no-coverage result (no signal) is never mistaken for a genuine zero population.
type ColumnTotal struct {
	// Total is the sum of the live per-value counts for the column over the window. Only
	// meaningful when Covered is true; always >= 0 (each summand is a live count > 0).
	Total int64
	// Covered reports whether any live record was found for the column. When false, Total is
	// zero and carries no signal.
	Covered bool
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

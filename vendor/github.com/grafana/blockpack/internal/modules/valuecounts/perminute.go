package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding NOTES.md.

import (
	"bytes"
	"sort"
)

// MinuteCount is one live minute-bucket's net span count for a single column=value leaf.
// Minute is a unix-second, 60-aligned bucket boundary — the same minuteBucket floor tempo's
// vcntwriter.go and blockpack's own VI write path already use (see NOTES.md).
type MinuteCount struct {
	Minute uint64
	Count  int64
}

// SelectivityPerMinute is SelectivityInRange's per-minute sibling: instead of collapsing the
// window into one scalar, it buckets by Record.TimeStart (every VCNT record already carries a
// single-minute TimeStart==TimeEnd, tempo vcntwriter.go:minuteBucket) so a caller can see WHICH
// minutes within [minTS, maxTS] actually carry live matches for column=value — the signal
// #487's slice construction needs to build adaptive-width, empty-tail-ordered time slices.
// Minutes whose net summed Count is <= 0 are dropped (NOTE-VC-001 liveness rule, same as every
// other read primitive in this package) — a slice-construction caller only wants live minutes.
// Opens no blockpack data files, same contract as every other function in this file.
//
// SPEC-VC-6: dropped-non-positive-minutes rule and minute-floor alignment contract.
// NOTE-VC-016
func SelectivityPerMinute(
	data []byte, dir []ChunkDirEntry, column string, value []byte, minTS, maxTS uint64,
) ([]MinuteCount, error) {
	recs, err := DecodeTimeRange(data, dir, minTS, maxTS)
	if err != nil {
		return nil, err
	}
	sums := make(map[uint64]int64)
	var order []uint64
	for i := range recs {
		r := &recs[i]
		if r.ColumnName != column || !bytes.Equal(r.Value, value) {
			continue
		}
		if _, seen := sums[r.TimeStart]; !seen {
			order = append(order, r.TimeStart)
		}
		sums[r.TimeStart] += r.Count
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	out := make([]MinuteCount, 0, len(order))
	for _, m := range order {
		if sums[m] > 0 {
			out = append(out, MinuteCount{Minute: m, Count: sums[m]})
		}
	}
	return out, nil
}

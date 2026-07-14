package valuecounts

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md/NOTES.md.

import "sort"

// MinuteDurationHistogram is one live minute-bucket's full 16-bucket duration histogram for a
// single histogram-eligible column — DurationHistogram's per-minute sibling, mirroring
// MinuteCount's own relationship to SelectivityInRange's scalar output (perminute.go).
//
// SPEC-VC-9.
type MinuteDurationHistogram struct {
	Minute    uint64
	Histogram DurationHistogram
}

// DurationHistogramPerMinuteInRange is DurationHistogramInRange's per-minute sibling: instead of
// collapsing [minTS, maxTS] into one DurationHistogram, it buckets the SAME histogram records by
// Record.TimeStart and returns one per-minute histogram per distinct LIVE minute, sorted
// ascending. A minute whose every bucket nets to <= 0 is dropped entirely (mirrors
// SelectivityPerMinute's liveness rule, NOTE-VC-001/016) — never retained as an all-zero,
// Covered=true entry; the downstream oracle (VCNTDurationPerMinuteFunc) only ever wants live
// minutes, and dropping here keeps that filtering in one place rather than two.
//
// SPEC-VC-9.
func DurationHistogramPerMinuteInRange(
	data []byte, dir []ChunkDirEntry, column string, minTS, maxTS uint64,
) ([]MinuteDurationHistogram, error) {
	recs, err := DecodeTimeRange(data, dir, minTS, maxTS)
	if err != nil {
		return nil, err
	}
	histColumn := HistogramColumnName(column)

	sums := make(map[uint64]*[16]int64)
	var order []uint64
	for i := range recs {
		r := &recs[i]
		if r.ColumnName != histColumn {
			continue
		}
		boundary, ok := decodeHistogramValue(r.Value)
		if !ok {
			continue
		}
		idx := boundaryToIndex(boundary)
		if idx < 0 {
			continue
		}
		s, seen := sums[r.TimeStart]
		if !seen {
			s = &[16]int64{}
			sums[r.TimeStart] = s
			order = append(order, r.TimeStart)
		}
		s[idx] += r.Count
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	out := make([]MinuteDurationHistogram, 0, len(order))
	for _, m := range order {
		s := sums[m]
		var h DurationHistogram
		var anyLive bool
		for i, c := range s {
			if c > 0 {
				h.Counts[i] = c
				anyLive = true
			}
		}
		if !anyLive {
			continue // decision #4: drop an all-zero-everywhere minute entirely
		}
		h.Covered = true
		out = append(out, MinuteDurationHistogram{Minute: m, Histogram: h})
	}
	return out, nil
}

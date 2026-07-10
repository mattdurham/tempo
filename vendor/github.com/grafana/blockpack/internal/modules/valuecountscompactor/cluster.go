package valuecountscompactor

// NOTE: see internal/modules/valuecountscompactor/NOTES.md.
// Any changes to this file must be reflected there.

import "sort"

// clusterByTimeRange sorts files by (minSec ASC, maxSec ASC, key ASC) for deterministic
// ordering, then greedily walks the sorted slice, starting a new cluster whenever including
// the next file would push the running cluster's [minSec, maxSec] span past maxSpan. maxSpan
// == 0 means no cap (always one cluster) — this is a pure-function property only; in
// production maxSpan comes from Config.MaxTimeSpanPerMerge, which withDefaults() never leaves
// at 0 (0 there means "apply DefaultMaxTimeSpanPerMerge", not "disable the cap" — see
// config.go). No file is ever split across two clusters (issue #494, R2).
func clusterByTimeRange(files []levelFile, maxSpan uint64) [][]levelFile {
	if len(files) == 0 {
		return nil
	}
	sorted := make([]levelFile, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].minSec != sorted[j].minSec {
			return sorted[i].minSec < sorted[j].minSec
		}
		if sorted[i].maxSec != sorted[j].maxSec {
			return sorted[i].maxSec < sorted[j].maxSec
		}
		return sorted[i].key < sorted[j].key
	})

	var clusters [][]levelFile
	start := 0
	runMin, runMax := sorted[0].minSec, sorted[0].maxSec
	for i := 1; i < len(sorted); i++ {
		f := sorted[i]
		candMin, candMax := runMin, runMax
		if f.minSec < candMin {
			candMin = f.minSec
		}
		if f.maxSec > candMax {
			candMax = f.maxSec
		}
		// candMax >= candMin relies on every input file having minSec <= maxSec (guaranteed by
		// valuecounts.ParseFilenameV2, which rejects a reversed range at parse time) -- but this
		// is a caller precondition, not something this pure function can enforce on its own
		// levelFile inputs, so guard the subtraction explicitly rather than assume it.
		if maxSpan > 0 && candMax > candMin && candMax-candMin > maxSpan {
			clusters = append(clusters, sorted[start:i])
			start = i
			runMin, runMax = f.minSec, f.maxSec
			continue
		}
		runMin, runMax = candMin, candMax
	}
	clusters = append(clusters, sorted[start:])
	return clusters
}

// pickCluster selects one cluster to pass to mergeLevel: the cluster with the most files;
// ties are broken by the smallest minSec (earliest-starting cluster) — see plan.md Design
// Decision 1 for rationale (issue #494, R2).
func pickCluster(clusters [][]levelFile) []levelFile {
	if len(clusters) == 0 {
		return nil
	}
	best := clusters[0]
	bestMin := clusterMinSec(best)
	for _, c := range clusters[1:] {
		cMin := clusterMinSec(c)
		if len(c) > len(best) || (len(c) == len(best) && cMin < bestMin) {
			best = c
			bestMin = cMin
		}
	}
	return best
}

// clusterMinSec returns the smallest minSec across a cluster's files. Clusters produced by
// clusterByTimeRange are already sorted by minSec ASC, so this is just the first element, but
// computed defensively (no assumption on caller-supplied ordering).
func clusterMinSec(cluster []levelFile) uint64 {
	m := cluster[0].minSec
	for _, f := range cluster[1:] {
		if f.minSec < m {
			m = f.minSec
		}
	}
	return m
}

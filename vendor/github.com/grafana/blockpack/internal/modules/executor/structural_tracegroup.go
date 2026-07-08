package executor

// structural_tracegroup.go — D4 (plan-d.md, issue #489): candidate TraceGroup discovery.
//
// FindTraceGroupInCandidates is relocated here from root's unexported findTraceGroupInCandidates
// (reader.go) under the same rule team-lead applied to D3's resolve algorithm (2026-07-07): it has
// zero root-package dependency (valueindex only), and both GetTraceByID's single-trace lookup and
// ExecuteStructuralFromIndex's many-candidate-trace lookup need it, so it is the canonical
// implementation here; root's findTraceGroupInCandidates becomes a thin delegate.

import (
	"context"
	"fmt"

	"github.com/grafana/blockpack/internal/modules/valueindex"
)

// FindTraceGroupInCandidates fetches and decodes every candidate trace-by-id index file in keys,
// merging every matching group found for traceID across ALL of them — candidates are NOT
// short-circuited on the first match, since DiscoverIndexFiles can legitimately return multiple
// valid, not-yet-compacted L0 files for the same TraceID with DISJOINT Spans (e.g. a trace's root
// span flushed in one consumer window and its child span in another, before compaction). Every
// matching group is merged (spans deduplicated by SpanID, first occurrence wins; TimeSec is the
// minimum across matches) — the same live-merge semantics as valueindex.MergeTraceGroups.
//
// Because the trace-by-id index is AUTHORITATIVE (NOTE-VI-071), a fetch or decode failure on any
// candidate is index/data inconsistency and is returned as an ERROR — never silently skipped as
// "unreadable, try the next" (that behavior is safe only while a full scan could still produce the
// correct answer, which is no longer true under the authoritative contract).
//
// Returns (group, true, nil) on a hit, (zero, false, nil) when no candidate holds the trace (an
// authoritative miss), or (zero, false, err) on any fetch/decode failure.
func FindTraceGroupInCandidates(
	ctx context.Context,
	lister valueindex.LookupStore,
	keys []string,
	traceID [16]byte,
	queryMinSec, queryMaxSec uint64,
) (valueindex.TraceGroup, bool, error) {
	var merged valueindex.TraceGroup
	found := false
	seenSpan := make(map[[8]byte]struct{})

	mergeGroup := func(g *valueindex.TraceGroup) {
		if !found {
			merged.TraceID = g.TraceID
			merged.TimeSec = g.TimeSec
			found = true
		} else if g.TimeSec < merged.TimeSec {
			merged.TimeSec = g.TimeSec
		}
		for i := range g.Spans {
			s := g.Spans[i]
			if _, dup := seenSpan[s.SpanID]; dup {
				continue
			}
			seenSpan[s.SpanID] = struct{}{}
			merged.Spans = append(merged.Spans, s)
		}
	}

	for _, key := range keys {
		g, ok, lerr := valueindex.LookupTraceGroupPartial(ctx, lister, key, traceID, queryMinSec, queryMaxSec)
		if lerr != nil {
			return valueindex.TraceGroup{}, false, fmt.Errorf(
				"FindTraceGroupInCandidates: partial lookup index candidate %q: %w", key, lerr,
			)
		}
		if ok {
			mergeGroup(&g)
		}
	}
	if !found {
		return valueindex.TraceGroup{}, false, nil
	}
	return merged, true, nil
}

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
	"golang.org/x/sync/errgroup"
)

// candidateFetchConcurrency bounds how many candidate trace-by-id index files
// FindTraceGroupInCandidates resolves in parallel (task #199, NOTE-VI-106). It mirrors
// vibuilder.downloadConcurrency's convention exactly (same value, same errgroup.SetLimit
// mechanism) but is its OWN constant rather than an import: executor cannot import vibuilder
// without an import cycle (vibuilder itself imports executor — see structural_index.go's own
// "internal import" note), so this package-level constant is the sanctioned mirror, not a new
// unbounded goroutine-per-candidate scheme. Kept in lockstep with vibuilder.downloadConcurrency
// (currently 4) since both bound the same kind of work: an object-store round trip per candidate
// immediately followed by CPU-bound decode (LookupTraceGroupPartial's footer/TOC parse plus, on a
// bloom hit, a snappy block decode) under the querier's own CPU limit.
const candidateFetchConcurrency = 4

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
// NOTE-VI-106 (task #199): every candidate's LookupTraceGroupPartial call is fanned out
// CONCURRENTLY, bounded by candidateFetchConcurrency, mirroring vibuilder.queryKeysRanged's
// errgroup fan-out for the sibling search/metrics value-index path — resolving candidates one at a
// time made wall-clock latency scale linearly with the candidate count instead of being bounded by
// the slowest single one. Concurrency is fetch-only: results are collected into a slice indexed by
// each candidate's ORIGINAL position in keys and merged back in that same order once every
// candidate has resolved, so the "first occurrence wins" SpanID-dedup and minimum-TimeSec merge
// semantics stay byte-identical to the prior sequential loop regardless of which goroutine happens
// to finish first.
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
	if len(keys) == 0 {
		return valueindex.TraceGroup{}, false, nil
	}

	type candidateResult struct {
		group valueindex.TraceGroup
		ok    bool
	}
	results := make([]candidateResult, len(keys))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(candidateFetchConcurrency)
	for i, key := range keys {
		g.Go(func() (err error) {
			// SPEC-ROOT-001: goroutine panics must not crash the process — mirrors
			// MaterializeTraceGroupMultiFile's own recover pattern (structural_multifile.go).
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf(
						"FindTraceGroupInCandidates: partial lookup index candidate %q: panic: %v", key, rec,
					)
				}
			}()
			if gctx.Err() != nil {
				// A sibling candidate already hit a real error; don't start new work, but
				// don't report a spurious error either — the goroutine that found the real
				// error reports it (mirrors vibuilder.queryKeysRanged's identical guard).
				return nil //nolint:nilerr // intentional: gctx.Err() belongs to a sibling goroutine's failure, not this one's
			}
			gr, ok, lerr := valueindex.LookupTraceGroupPartial(gctx, lister, key, traceID, queryMinSec, queryMaxSec)
			if lerr != nil {
				return fmt.Errorf("FindTraceGroupInCandidates: partial lookup index candidate %q: %w", key, lerr)
			}
			if ok {
				results[i] = candidateResult{group: gr, ok: true}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return valueindex.TraceGroup{}, false, err
	}

	var merged valueindex.TraceGroup
	found := false
	seenSpan := make(map[[8]byte]struct{})
	for i := range results {
		if !results[i].ok {
			continue
		}
		g := &results[i].group
		if !found {
			merged.TraceID = g.TraceID
			merged.TimeSec = g.TimeSec
			found = true
		} else if g.TimeSec < merged.TimeSec {
			merged.TimeSec = g.TimeSec
		}
		for si := range g.Spans {
			s := g.Spans[si]
			if _, dup := seenSpan[s.SpanID]; dup {
				continue
			}
			seenSpan[s.SpanID] = struct{}{}
			merged.Spans = append(merged.Spans, s)
		}
	}
	if !found {
		return valueindex.TraceGroup{}, false, nil
	}
	return merged, true, nil
}

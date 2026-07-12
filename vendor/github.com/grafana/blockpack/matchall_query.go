package blockpack

// matchall_query.go — Phase 6b (plan-scan-fallback.md, tempo repo) bounded newest-first
// materializer for match-all / intrinsic-only queries: a TraceQL query (or intrinsic-only
// condition set, e.g. tempo's own SearchMetaConditions()) that has ZERO leaf predicates and
// ZERO listed columns has nothing for the value index -- or any predicate evaluator -- to
// check. There is no coverage gap here (NOTE-VI-096/SPEC-ROOT-019's authoritative-index
// contract does not apply: there is no predicate to be authoritative ABOUT), and this is
// deliberately NOT a revival of the retired RecentFirstBudget/DispatchBoundedRecentFirst
// (that mechanism bounded the COST of evaluating an EXPENSIVE FILTER via a scan; a match-all
// query has no filter to evaluate at any cost -- every span in every visited block matches
// trivially). The only correctness requirement is a positive Limit (there is no other
// dimension to bound the read by), and the only work is walking blocks newest-first and
// decoding them directly until Limit spans are materialized.

import (
	"context"
	"fmt"
	"math"
)

// IsMatchAllProgram reports whether prog has zero leaf predicates AND zero listed columns --
// the "truly nothing referenced" shape vibuilder.BuildSource's own early return already
// detects internally for the value-index path. Exposed here so tempo's Fetch can route this
// shape to QueryNewestFirstMatchAll instead of ever attempting a value-index build for a
// query that has no predicate to look up in the first place.
func IsMatchAllProgram(prog *Program) bool {
	return prog == nil || prog.Predicates == nil ||
		(len(prog.Predicates.Nodes) == 0 && len(prog.Predicates.Columns) == 0)
}

// matchAllProgram is compiled once (package-level, immutable per SPEC-VM-001) and reused by
// every QueryNewestFirstMatchAll call -- "{}" is deterministic, so there is nothing
// query-specific to recompile per call.
var matchAllProgram = func() *Program {
	prog, err := CompileTraceQL("{}", QueryOptions{})
	if err != nil {
		// "{}" is a fixed, always-valid filter expression; a compile failure here would be a
		// genuine internal bug, not a runtime condition any caller can react to differently.
		panic(fmt.Sprintf("blockpack: internal error: failed to compile the match-all program: %v", err))
	}
	return prog
}()

// QueryNewestFirstMatchAll materializes up to opts.Limit spans from r, newest-first by span
// start time, with NO predicate evaluation at all -- every span in a visited block is a
// match by definition, since there is no filter to check it against. Walks
// r.BlockIndicesNewestFirst's block order one block at a time, decoding via the same
// QueryTraceQLWithProgram machinery every other query path already uses (SPEC-007's
// single-I/O-per-block invariant, unchanged), stopping as soon as opts.Limit spans have been
// collected -- no further blocks are read past that point.
//
// opts.Limit must be positive: there is no bounded-COST concept for a query with nothing to
// filter (unlike the retired RecentFirstBudget's expensive-filter case), so a caller MUST
// supply a ceiling or this returns an error immediately without reading any block.
func QueryNewestFirstMatchAll(
	ctx context.Context,
	r *Reader,
	opts QueryOptions,
) (matches []SpanMatch, stats QueryStats, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return nil, QueryStats{}, fmt.Errorf("QueryNewestFirstMatchAll: reader cannot be nil")
	}
	if opts.Limit <= 0 {
		return nil, QueryStats{}, fmt.Errorf(
			"QueryNewestFirstMatchAll: opts.Limit must be positive, got %d",
			opts.Limit,
		)
	}

	// Mirror normalizeTimeRange's own "0 means unbounded" convention (api.go):
	// BlockIndicesNewestFirst takes a literal window, so a zero EndNano must be widened to
	// math.MaxUint64 here, or a genuinely unbounded caller (the common case -- most match-all
	// requests have no explicit time window at all) would incorrectly match nothing.
	maxNano := opts.EndNano
	if maxNano == 0 {
		maxNano = math.MaxUint64
	}
	blockIdxs := r.BlockIndicesNewestFirst(opts.StartNano, maxNano)
	matches = make([]SpanMatch, 0, opts.Limit)
	for _, idx := range blockIdxs {
		if len(matches) >= opts.Limit {
			break
		}
		blockOpts := opts
		blockOpts.StartBlock = idx
		blockOpts.BlockCount = 1
		blockOpts.Limit = opts.Limit - len(matches)

		blockMatches, blockStats, blockErr := QueryTraceQLWithProgram(ctx, r, matchAllProgram, blockOpts)
		if blockErr != nil {
			return nil, stats, fmt.Errorf("QueryNewestFirstMatchAll: block %d: %w", idx, blockErr)
		}
		matches = append(matches, blockMatches...)
		stats.Steps = append(stats.Steps, blockStats.Steps...)
		stats.TotalDuration += blockStats.TotalDuration
	}
	stats.ExecutionPath = "newest-first-match-all"
	return matches, stats, nil
}

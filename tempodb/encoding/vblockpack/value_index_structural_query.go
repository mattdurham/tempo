package vblockpack

// value_index_structural_query.go — querier-side index-driven STRUCTURAL query wiring (blockpack
// issue #489, plan-d.md DT1/DT2). Mirrors value_index_query.go's tryIndexFetch shape for the new
// 2-node structural path (blockpack.QueryStructuralFromIndex, D5).
//
// Dispatch-safety gate (plan-d.md DT1, checkpoint raised with team-lead, 2026-07-08 — see task
// tracker #4 comments): blockpack.ExecuteStructuralFromIndex has NO per-block ownership/sourceRef
// restriction, unlike the plain filter path's tryIndexFetch (which restricts materialized output
// to spans physically stored in ITS OWN block via the sourceRef parameter it passes to
// QueryTraceQLFromIndex). Option A's multi-file trace materialization (plan-d.md D3) means a
// structural match's spans can legitimately live in a DIFFERENT block than "this" one — so there
// is no sound way to partition a structural answer across the "one job per (block, slice) pair"
// dispatch model #487's timeSlicedJobsFunc already uses for every other index-driven job type
// today: N blocks overlapping one slice would each independently compute and return the SAME
// complete answer for that slice's whole window, not a partition of it (verified: the frontend's
// own metadata combiner, traceql.anyCombiner.AddMetadata, merges by TraceID rather than
// discarding a "duplicate" job response — so this is a real correctness/cost defect, not silently
// absorbed). tryStructuralIndexFetch therefore ONLY ever attempts the index-driven answer when
// indexOnly is true (a genuine #487 slice job) — this is now a PERMANENT, correctness-required
// boundary, not an interim gate pending a future fix: structural per-block dispatch can never
// safely answer from the index, with or without early-stopping (a future early-stopping
// resolution change would make duplicate per-block jobs non-deterministic at the limit boundary,
// strictly worse than today's wasteful-but-deterministic duplication). `structural_sharder.go`'s
// `structuralTimeSlicedJobsFunc` already builds `DispatchTimeSliced` plans for coverage-eligible
// structural queries today (plan-d.md Option B, shipped) — indexOnly is true for those, so the
// index-driven path IS reachable in production, just never through the default block-sharded
// dispatch, where the duplication would remain unconditional.
//
// indexOnly's OTHER role (mirrors #487's own IndexOnly/ErrSliceIndexCoverageGap contract, plan-d.md
// DT2): a slice job has no safe scan fallback across its narrowed window, so EVERY decline reason
// below (not a 2-node structural query, no left-leg VI coverage, a genuine build error) converts to
// blockpack.ErrStructuralIndexCoverageGap instead of a routine (nil, false, nil) decline — the SAME
// universal decline-to-hard-error mapping tryIndexFetch's own declineOutcome already applies for the
// filter path, just keyed to blockpack's own sentinel (D4/D5b) instead of ErrSliceIndexCoverageGap.

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
)

func (b *blockpackBlock) tryStructuralIndexFetch(
	ctx context.Context,
	query string,
	opts blockpack.QueryOptions,
	indexOnly bool,
) ([]blockpack.SpanMatch, bool, indexFetchStats, error) {
	var stats indexFetchStats

	if !indexOnly {
		// See this file's package doc comment: the index-driven structural path is only safe to
		// attempt under a genuine #487 slice job today. A plain (non-slice) Fetch call always
		// routes this routine decline through Fetch's own vr==nil hard-error /
		// IsStructuralQuery-and-boundedAuthorized bounded-read / hard-error switch — never an
		// unconditional string-based scan.
		return nil, false, stats, nil
	}

	vr := getValueIndexQueryReader()
	if vr == nil {
		return structuralDeclineOutcome(indexOnly, stats)
	}

	leftProg, rightProg, op, legsOK, legsErr := blockpack.CompileStructuralLegs(query)
	if legsErr != nil || !legsOK {
		// Not a 2-node structural query this path can build sources for (a plain filter/pipeline
		// query, an unsupported >2-node chain, or a genuine compile error on one of the legs) --
		// a routine decline for any other caller, but this IS a slice job (indexOnly), so it
		// converts to the typed coverage-gap error below.
		return structuralDeclineOutcome(indexOnly, stats)
	}

	minSec, maxSec := nanoWindowToSec(opts.StartNano, opts.EndNano)
	cache := vr.cacheFor(b.meta.TenantID)
	watermarks := watermarksForOrNil(ctx, b.meta.TenantID)

	if isNegatedStructuralOp(op) {
		// DT1b (issue #489 holistic review, HIGH finding): negated ops (!>>, !>, !~) route to
		// D6's own root wrapper instead of falling through to "for D6" (a permanent decline).
		return b.tryNegatedStructuralIndexFetch(ctx, query, rightProg, vr, cache, minSec, maxSec, indexOnly, opts, stats, watermarks)
	}

	leftSrc, leftOK, buildErr := blockpack.BuildValueIndexSource(ctx, cache, vr.store, leftProg, minSec, maxSec, watermarks)
	if buildErr != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: structural index fetch: build left source error",
			"block", b.meta.BlockID, "err", buildErr)
		return structuralDeclineOutcome(indexOnly, stats)
	}
	if !leftOK {
		// #496/B1: every leaf's shape was unindexable (R3's permanent-decline case) --
		// recordUsageForDeclinedQuery's own Indexable check correctly never records
		// this. The right leg's own decline is deliberately not recorded (see the
		// doc comment below): it is optional/best-effort, never a hard decline.
		recordUsageForDeclinedQuery(ctx, b.meta.TenantID, leftProg, dedicatedColumnSet(b.meta.DedicatedColumns), time.Now())
		return structuralDeclineOutcome(indexOnly, stats)
	}
	bs := leftSrc.Stats()
	stats.FilesRead = bs.FilesRead
	stats.BytesRead = bs.BytesRead
	stats.Hits = bs.Hits
	// #496/B1: NOTE-VI-033's "Add even when empty" contract means leftOK=true only
	// means "at least one leaf has an indexable shape" -- bs.FilesRead == 0 is the
	// real "genuinely missing index" signal (see value_index_query.go's
	// tryIndexFetch for the fuller explanation of this same gate).
	if bs.FilesRead == 0 {
		recordUsageForDeclinedQuery(ctx, b.meta.TenantID, leftProg, dedicatedColumnSet(b.meta.DedicatedColumns), time.Now())
	}

	// rightSource is optional (D4/D5's own documented contract): R is only ever used for the
	// cost-based intersection prefilter, never required for correctness -- confirmation of R's
	// real filter always happens later inside ExecuteStructuralFromIndex regardless (D3B). A
	// build failure/decline for R is therefore silently ignored, never a hard error/decline for
	// the whole attempt.
	var rightSrc blockpack.ValueIndexSource
	if rightProg != nil {
		if rs, rightOK, rightErr := blockpack.BuildValueIndexSource(ctx, cache, vr.store, rightProg, minSec, maxSec, watermarks); rightErr == nil &&
			rightOK {
			rightSrc = rs
			rbs := rs.Stats()
			stats.FilesRead += rbs.FilesRead
			stats.BytesRead += rbs.BytesRead
			stats.Hits += rbs.Hits
		}
	}

	matches, structOK, structErr := blockpack.QueryStructuralFromIndex(
		ctx, query, leftSrc, rightSrc, nil, nil,
		vr.store, b.meta.TenantID, vr.indexPrefix,
		b.readerForSourceRef,
		minSec, maxSec, indexOnly, opts,
	)
	if structErr != nil {
		if errors.Is(structErr, blockpack.ErrStructuralIndexCoverageGap) {
			level.Error(util_log.Logger).Log("msg", "vblockpack: structural index fetch: index-only slice job hit a coverage gap",
				"block", b.meta.BlockID, "err", structErr)
			return nil, false, stats, structErr
		}
		level.Error(util_log.Logger).Log("msg", "vblockpack: structural index fetch: index/data inconsistency, failing query",
			"block", b.meta.BlockID, "err", structErr)
		return nil, false, stats, structErr
	}
	if !structOK {
		return structuralDeclineOutcome(indexOnly, stats)
	}
	stats.Used = true
	return matches, true, stats, nil
}

// isNegatedStructuralOp reports whether op is one of the three negated structural operators D6
// supports (!>>, !>, !~) — the only StructuralOp values blockpack.QueryNegatedStructuralFromIndex
// answers. Single-source-of-truth for tryStructuralIndexFetch's own dispatch branch, so it can
// never independently drift from D6's own op set.
func isNegatedStructuralOp(op blockpack.StructuralOp) bool {
	switch op {
	case blockpack.OpNotDescendant, blockpack.OpNotChild, blockpack.OpNotSibling:
		return true
	default:
		return false
	}
}

// tryNegatedStructuralIndexFetch is tryStructuralIndexFetch's D6 sibling (issue #489 holistic
// review, HIGH finding: the negated engine had no root wrapper, so tempo could never reach it).
// Mirrors the positive path's shape, but D6's candidate discovery is driven EXCLUSIVELY by the
// RIGHT (tested) side (blockpack.ExecuteNegatedStructuralFromIndex's own package doc comment,
// internal/modules/executor/structural_index_negated.go) -- there is no leftSource parameter for
// QueryNegatedStructuralFromIndex at all, so only rightProg's source is ever built here. Same
// indexOnly/coverage-gap typed-error contract as the positive path (DT2) -- both branches convert
// EVERY decline reason to blockpack.ErrStructuralIndexCoverageGap under indexOnly via the SAME
// structuralDeclineOutcome helper.
func (b *blockpackBlock) tryNegatedStructuralIndexFetch(
	ctx context.Context,
	query string,
	rightProg *blockpack.Program,
	vr *viQueryReader,
	cache *blockpack.IndexFileCache,
	minSec, maxSec uint64,
	indexOnly bool,
	opts blockpack.QueryOptions,
	stats indexFetchStats,
	watermarks map[string]blockpack.ColumnWatermark,
) ([]blockpack.SpanMatch, bool, indexFetchStats, error) {
	rightSrc, rightOK, buildErr := blockpack.BuildValueIndexSource(ctx, cache, vr.store, rightProg, minSec, maxSec, watermarks)
	if buildErr != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: negated structural index fetch: build right source error",
			"block", b.meta.BlockID, "err", buildErr)
		return structuralDeclineOutcome(indexOnly, stats)
	}
	if !rightOK {
		// #496/B1: every leaf's shape was unindexable (R3's permanent-decline case) --
		// recordUsageForDeclinedQuery's own Indexable check correctly never records
		// this.
		recordUsageForDeclinedQuery(ctx, b.meta.TenantID, rightProg, dedicatedColumnSet(b.meta.DedicatedColumns), time.Now())
		return structuralDeclineOutcome(indexOnly, stats)
	}
	bs := rightSrc.Stats()
	stats.FilesRead = bs.FilesRead
	stats.BytesRead = bs.BytesRead
	stats.Hits = bs.Hits
	// #496/B1: NOTE-VI-033's "Add even when empty" contract means rightOK=true only
	// means "at least one leaf has an indexable shape" -- bs.FilesRead == 0 is the
	// real "genuinely missing index" signal.
	if bs.FilesRead == 0 {
		recordUsageForDeclinedQuery(ctx, b.meta.TenantID, rightProg, dedicatedColumnSet(b.meta.DedicatedColumns), time.Now())
	}

	matches, structOK, structErr := blockpack.QueryNegatedStructuralFromIndex(
		ctx, query, rightSrc,
		vr.store, b.meta.TenantID, vr.indexPrefix,
		b.readerForSourceRef,
		minSec, maxSec, indexOnly, opts,
	)
	if structErr != nil {
		if errors.Is(structErr, blockpack.ErrStructuralIndexCoverageGap) {
			level.Error(util_log.Logger).Log("msg", "vblockpack: negated structural index fetch: index-only slice job hit a coverage gap",
				"block", b.meta.BlockID, "err", structErr)
			return nil, false, stats, structErr
		}
		level.Error(util_log.Logger).Log("msg", "vblockpack: negated structural index fetch: index/data inconsistency, failing query",
			"block", b.meta.BlockID, "err", structErr)
		return nil, false, stats, structErr
	}
	if !structOK {
		return structuralDeclineOutcome(indexOnly, stats)
	}
	stats.Used = true
	return matches, true, stats, nil
}

// structuralDeclineOutcome mirrors declineOutcome's routine-decline/indexOnly-hard-error shape
// (value_index_query.go) for the structural path, but returns blockpack's OWN
// ErrStructuralIndexCoverageGap sentinel instead of ErrSliceIndexCoverageGap (plan-d.md DT2) --
// one typed-error family for every structural coverage gap, whether detected here (tempo's own
// leg-compile/VI-build pre-checks) or inside blockpack.QueryStructuralFromIndex/
// ExecuteStructuralFromIndex itself (leftSource/trace-by-id/Partial-tree gaps): never a silent
// scan fallback under indexOnly. tryStructuralIndexFetch only ever calls this with indexOnly=true
// today (see its own doc comment), but it takes the bool explicitly so its own contract stays
// self-evident and symmetric with declineOutcome.
func structuralDeclineOutcome(indexOnly bool, stats indexFetchStats) ([]blockpack.SpanMatch, bool, indexFetchStats, error) {
	if indexOnly {
		return nil, false, stats, blockpack.ErrStructuralIndexCoverageGap
	}
	return nil, false, stats, nil
}

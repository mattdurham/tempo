package executor

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.
// NOTE-449: OTel span helpers for the executor query path (issue #368).
// SPEC-OBS-002: implements mandatory blockpack.planner and blockpack.block spans.
// SPEC-OBS-003: all span.SetAttributes calls are guarded by span.IsRecording() to prevent
// attribute.Int()/attribute.String() allocations on unsampled queries.
// SPEC-OBS-004: attachCacheStats uses CacheStats aggregate counts, not per-fetch spans.

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// PlannerSpanStats carries the two-phase (issue #383) execution observables that the
// block-level Plan cannot know on its own: the candidate-row selectivity produced by the
// dedicated/intrinsic-column pre-filter, and whether the query was answered without ever
// fetching a full block payload.
//
// NOTE-464 (issue #383): these are EXECUTION-time quantities — the candidate rowIdx bitmap
// is produced by the intrinsic-TOC pre-filter (BlockRefsFromIntrinsicTOC /
// blockRefsFromIntrinsicPartial), which runs against the ~50KB-per-block ToC data already in
// memory, before any GetBlockWithBytes. The block-level planner (planBlocks) only counts
// block pruning; row-level bitmap selectivity and the full-fetch-skipped decision are decided
// later in the executor. PlannerSpanStats lets the executor surface them on the same
// blockpack.planner span so a distributed trace shows whether the two-phase model engaged.
type PlannerSpanStats struct {
	// CandidateRows is the number of rows that passed the intrinsic/dedicated pre-filter
	// (the size of the candidate rowIdx bitmap). -1 means "not computed" (no pre-filter ran),
	// in which case BitmapSelectivity is omitted from the span.
	CandidateRows int
	// TotalSpans is the total number of spans across the selected blocks — the denominator
	// for BitmapSelectivity. 0 means unknown; the ratio is omitted to avoid divide-by-zero.
	TotalSpans int
	// FullFetchSkipped reports whether the query was answered entirely from ToC/intrinsic
	// data with zero full block payload fetches (the strongest form of the issue #383 win).
	FullFetchSkipped bool
}

// bitmapSelectivity returns CandidateRows/TotalSpans, or (0, false) when it cannot be
// computed (no pre-filter ran, or total span count is unknown).
func (s PlannerSpanStats) bitmapSelectivity() (float64, bool) {
	if s.CandidateRows < 0 || s.TotalSpans <= 0 {
		return 0, false
	}
	return float64(s.CandidateRows) / float64(s.TotalSpans), true
}

// emitPlannerSpan creates, populates, and immediately ends a blockpack.planner span
// as a child of ctx. All planner attributes are set inside a span.IsRecording() guard
// to avoid attribute allocations on unsampled queries.
//
// NOTE-464: pass stats == nil for the block-scan path (no row-level bitmap was built — the
// query reads full blocks). On that path full_fetch_skipped is reported false and the
// selectivity attribute is omitted. On the intrinsic fast paths, pass a non-nil stats so the
// span records the candidate-bitmap selectivity and full_fetch_skipped=true.
func emitPlannerSpan(ctx context.Context, plan *queryplanner.Plan, stats *PlannerSpanStats) {
	if plan == nil {
		return
	}
	_, span := tracer.Start(ctx, "blockpack.planner")
	defer span.End()
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(
		attribute.Int("blockpack.planner.total_blocks", plan.TotalBlocks),
		attribute.Int("blockpack.planner.selected_blocks", len(plan.SelectedBlocks)),
		attribute.Int("blockpack.planner.pruned_by_time", plan.PrunedByTime),
		attribute.Int("blockpack.planner.pruned_by_index", plan.PrunedByIndex),
		attribute.Int("blockpack.planner.pruned_by_colstats", plan.PrunedByColStats),
		attribute.Int("blockpack.planner.pruned_by_file_bounds", plan.PrunedByFileBounds),
		attribute.String("blockpack.planner.explain", plan.Explain),
	)
	fullFetchSkipped := false
	if stats != nil {
		fullFetchSkipped = stats.FullFetchSkipped
		if sel, ok := stats.bitmapSelectivity(); ok {
			span.SetAttributes(attribute.Float64("blockpack.planner.bitmap_selectivity", sel))
		}
	}
	span.SetAttributes(attribute.Bool("blockpack.planner.full_fetch_skipped", fullFetchSkipped))
}

// startBlockSpan starts a blockpack.block span as a child of ctx.
// The caller must call span.End() when block processing is complete.
// attachCacheStats should be called before span.End() to add cache attributes.
func startBlockSpan(ctx context.Context, blockIdx int) (context.Context, trace.Span) {
	ctx, span := tracer.Start(ctx, "blockpack.block")
	if span.IsRecording() {
		span.SetAttributes(attribute.Int("blockpack.block.index", blockIdx))
	}
	return ctx, span
}

// attachCacheStats sets cache hit/miss attributes on span from cs.
// All attribute calls are guarded by span.IsRecording() and cs != nil.
func attachCacheStats(span trace.Span, cs *modules_reader.CacheStats) {
	if cs == nil || !span.IsRecording() {
		return
	}
	span.SetAttributes(
		attribute.Int("blockpack.cache.toc.hits", int(cs.Hits[modules_reader.CacheStatsSectionToc])),
		attribute.Int("blockpack.cache.toc.misses", int(cs.Misses[modules_reader.CacheStatsSectionToc])),
		attribute.Int("blockpack.cache.col.hits", int(cs.Hits[modules_reader.CacheStatsSectionCol])),
		attribute.Int("blockpack.cache.col.misses", int(cs.Misses[modules_reader.CacheStatsSectionCol])),
	)
}

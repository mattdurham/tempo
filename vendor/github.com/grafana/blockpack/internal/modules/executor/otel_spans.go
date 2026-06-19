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

// emitPlannerSpan creates, populates, and immediately ends a blockpack.planner span
// as a child of ctx. All planner attributes are set inside a span.IsRecording() guard
// to avoid attribute allocations on unsampled queries.
func emitPlannerSpan(ctx context.Context, plan *queryplanner.Plan) {
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
		attribute.Int("blockpack.planner.pruned_by_bloom", plan.PrunedByFuse),
		attribute.Int("blockpack.planner.pruned_by_colstats", plan.PrunedByColStats),
		attribute.Int("blockpack.planner.pruned_by_intrinsic_toc", plan.PrunedByIntrinsicTOC),
		attribute.String("blockpack.planner.explain", plan.Explain),
	)
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

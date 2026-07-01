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

// PlannerSpanStats carries the one execution-time observable the block-level Plan cannot
// know on its own: whether the query was answered without ever fetching a full block payload.
//
// NOTE-440 (issue #440): the row-level candidate-bitmap selectivity that NOTE-464 recorded came
// from the intrinsic-TOC pre-filter (BlockRefsFromIntrinsicTOC / blockRefsFromIntrinsicPartial).
// That pre-filter was removed with the IntrinsicTOC (#433/#436) and the executor rewrite (#440),
// so candidate_rows / total_spans are never computed anymore — bitmap_selectivity was
// permanently omitted and its inputs were dead. Only FullFetchSkipped survives: the value-index
// query path (#430) still answers some queries (metrics-intrinsic, all-blocks-pruned) with zero
// block payload fetches, and that is the observable worth surfacing on the planner span.
type PlannerSpanStats struct {
	// FullFetchSkipped reports whether the query was answered with zero full block payload
	// fetches (the strongest form of the issue #383 win).
	FullFetchSkipped bool
}

// emitPlannerSpan creates, populates, and immediately ends a blockpack.planner span
// as a child of ctx. All planner attributes are set inside a span.IsRecording() guard
// to avoid attribute allocations on unsampled queries.
//
// NOTE-440: pass stats == nil for the block-scan and structural paths (query reads full
// blocks), which reports full_fetch_skipped=false. Pass a non-nil stats on the zero-fetch
// paths (metrics-intrinsic, all-blocks-pruned) so the span records full_fetch_skipped=true.
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

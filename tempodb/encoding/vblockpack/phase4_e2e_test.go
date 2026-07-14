package vblockpack

// phase4_e2e_test.go — issue #217 Phase 4.1's primary proof test, via the REAL local
// write→backfill→dispatch pipeline (this project's established preference over hand-built
// fixtures — memory: "Local-backend integration test found 3 bugs"). No live cluster is
// available in this environment, so this test substitutes the closest achievable equivalent: a
// real block written through the production write path (writeSvcBlock), a real in-progress
// backfill watermark seeded through the REAL registry production methods
// (RecordUseAndMaybeTrigger/UpdateWatermark, vi_watermark_cache_wiring_test.go's
// seedTriggeredWatermarkEntry), and a real #217-forced one-minute BuildQueryPlan/BuildTimeSlices
// partition dispatched as real per-slice block.QueryRange calls (IndexOnly=true, exactly what
// the frontend's sharder would send to a querier).
//
// IMPORTANT CONSTRAINT (discovered while writing this test, not glossed over): this test does
// NOT also drive the real modules/frontend/combiner.NewQueryRange merge in the SAME process.
// tempodb/encoding/vblockpack cannot import modules/frontend/combiner at all — combiner imports
// pkg/api, which imports tempodb, which imports tempodb/encoding/vblockpack, a genuine
// structural import cycle (confirmed via `go vet`, not a hypothetical concern). The dispatch
// half (real write→backfill→per-slice-QueryRange, this file) and the combine half (real
// combiner merging exactly this input shape) are therefore proven by two SEPARATE real tests:
// this one, and modules/frontend/combiner/metrics_query_range_test.go's
// TestQueryRangeCombiner_PropagatesPartialFromOneJobWithoutDiscardingOthers. Together they cover
// the full pipeline; neither alone is a complete substitute for a live-cluster run, which this
// environment does not have access to.
import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestPhase4_RealPipeline_PartialBackfillServesCoveredSlicesAsPartial is the primary #217
// Phase 4.1 proof test (dispatch half — see this file's own package doc comment for the
// combine half's separate location and why they cannot be one test).
//
// Setup: a real block (writeSvcBlock) with one real span for "svc-alpha" written at "now". A
// real, in-progress backfill watermark for "resource.service.name" confirms coverage only from
// windowStart+6min onward (minutes 0-5 of a 10-minute window are genuinely UNCOVERED; minutes
// 6-9 are COVERED, and "now" — the span's real timestamp — falls in minute 9, the most recent
// covered minute).
//
// Dispatch: a real blockpack.BuildQueryPlan call (the SAME entry point tempo's frontend uses,
// buildQueryPlanFromProgram) over the 10-minute window produces exactly 10 forced-one-minute
// TimeSlices (#217/NOTE-QP-012). Each slice is dispatched as a real, independent
// block.QueryRange(IndexOnly=true) call — exactly what the frontend's sharder would send to a
// querier for a #487/#217 slice job.
//
// Assertion: of the 10 REAL per-slice responses, the 6 genuinely-uncovered minutes report
// PartialStatus_PARTIAL with a non-empty message (Phase 1.1's tolerance, never a hard error —
// pre-#217 this whole query would have failed outright), the 3 covered-but-empty minutes report
// PartialStatus_COMPLETE with zero series (Phase 1.5's boundary — never conflated with a
// coverage gap), and the 1 covered minute containing the real span reports
// PartialStatus_COMPLETE with real series data (#217's core guarantee: the query serves what it
// genuinely has).
func TestPhase4_RealPipeline_PartialBackfillServesCoveredSlicesAsPartial(t *testing.T) {
	const tenant = "test-tenant"
	const col = "resource.service.name"
	const query = `{ resource.service.name = "svc-alpha" } | count_over_time()`

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, viStore, tenant, uuid.New(), "svc-alpha", 1)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	// 10-minute window, minute-floored, ending at the real span's own minute so "now" (the
	// span's real timestamp) falls in the LAST (most recent) slice.
	now := time.Now()
	windowEnd := (uint64(now.Unix()) / 60) * 60 // floor to minute
	windowStart := windowEnd - 9*60             // 10 one-minute slices: [windowStart, windowEnd+60)

	// Real, in-progress backfill: covers only the newest 4 minutes (windowStart+6min onward).
	watermarkSec := windowStart + 6*60
	objStore := newFakeViObjectStore()
	seedTriggeredWatermarkEntry(t, objStore, tenant, col, "string", watermarkSec)
	withViWatermarkCache(t, newViWatermarkCache(objStore, time.Minute))

	// Real #217 dispatch: the SAME entry point tempo's frontend uses (buildQueryPlanFromProgram
	// mirrors this exactly) — forces exactly 10 one-minute TimeSlices for this window.
	prog, _, err := blockpack.CompileTraceQLMetricsFilter(query)
	require.NoError(t, err)
	cost, perMinuteForLead := blockpack.TimeSliceOracle(nil, nil, windowStart, windowEnd+59)
	plan := blockpack.BuildQueryPlan(prog, cost, true, perMinuteForLead, windowStart, windowEnd+59, 1, blockpack.DefaultK)
	require.Equal(t, blockpack.DispatchTimeSliced, plan.Strategy)
	require.Len(t, plan.Slices, 10, "a 10-minute window must force exactly 10 one-minute slices (#217/NOTE-QP-012)")

	// Real per-slice dispatch: each slice becomes its own IndexOnly=true block.QueryRange call,
	// exactly as the frontend's sharder would issue to a querier.
	var partialCount, completeEmptyCount, completeWithDataCount int
	for _, s := range plan.Slices {
		req := &tempopb.QueryRangeRequest{
			Query: query,
			Start: s.Start * 1_000_000_000,
			End:   s.End * 1_000_000_000,
			Step:  uint64(time.Minute.Nanoseconds()),
		}
		resp, err := block.QueryRange(context.Background(), req, common.SearchOptions{IndexOnly: true})
		require.NoError(t, err, "#217 Phase 1: an IndexOnly slice job must never hard-error, even for a genuinely uncovered minute")
		require.NotNil(t, resp)

		// count_over_time() always emits one series per queried window regardless of whether
		// anything matched (a zero-valued sample for a covered-but-empty minute) — real vs.
		// empty is distinguished by the sample VALUE, not by Series presence.
		hasNonZeroSample := false
		for _, ser := range resp.Series {
			for _, sample := range ser.Samples {
				if sample.Value != 0 {
					hasNonZeroSample = true
				}
			}
		}

		switch {
		case resp.Status == tempopb.PartialStatus_PARTIAL:
			require.NotEmpty(t, resp.Message, "a PARTIAL slice must explain why (%d,%d)", s.Start, s.End)
			require.False(t, hasNonZeroSample, "a tolerated coverage-gap slice must carry no real match data (%d,%d)", s.Start, s.End)
			partialCount++
		case hasNonZeroSample:
			completeWithDataCount++
		default:
			require.Empty(t, resp.Message, "a covered-but-empty slice must carry no message (%d,%d)", s.Start, s.End)
			completeEmptyCount++
		}
	}

	require.Equal(t, 6, partialCount, "the 6 genuinely uncovered minutes must each report tolerated PARTIAL")
	require.Equal(t, 1, completeWithDataCount, "exactly the one minute containing the real span must report real, confident data")
	require.Equal(t, 3, completeEmptyCount, "the 3 covered-but-empty minutes must report a confident, non-PARTIAL zero")
}

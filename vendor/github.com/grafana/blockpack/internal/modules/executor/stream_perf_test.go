package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// buildPerfTestReader writes 3 spans for targetSvc and 5 spans for "other-svc".
// Span timestamps are set via spanIdx to give distinct, predictable ordering.
// Returns the reader data and a cleanup func.
func buildPerfTestReader(t *testing.T, targetSvc string, targetCount, otherCount int) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0) // single block — no sharding noise

	tid := [16]byte{0xAA}
	for i := range targetCount {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
			map[string]any{"service.name": targetSvc})
	}
	for i := range otherCount {
		addSpan(t, w, tid, targetCount+i, "op", tracev1.Span_SPAN_KIND_CLIENT, nil,
			map[string]any{"service.name": "other-svc"})
	}
	mustFlush(t, w)
	return buf.Bytes()
}

// EX-PERF-01: TestCollect_IntrinsicTopK_MapPath_Stats
//
// Verifies that Case B (pure intrinsic + sort, map path) populates OnStats with:
//   - ExecutionPath == "intrinsic-topk-sort"
//   - IntrinsicRefCount == number of matching refs (3)
//   - IntrinsicScanCount == 0 (map path does not scan the blob)
//
// 3 < SortScanThreshold (8000) so the map path is always taken.
func TestCollect_IntrinsicTopK_MapPath_Stats(t *testing.T) {
	// Not parallel: reads SortScanThreshold but does not modify it.
	// No parallel hazard, but kept serial to avoid masking EX-PERF-02/03 state.

	data := buildPerfTestReader(t, "rare-svc", 3, 5)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "rare-svc" }`)

	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		Limit:           2,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err)
	assert.Len(t, rows, 2, "limit=2 should return exactly 2 rows")

	assert.Equal(t, "intrinsic-topk-kll", qs.ExecutionPath,
		"Case B KLL path should set ExecutionPath=intrinsic-topk-kll")
	intrStep := findStep(qs, "intrinsic")
	require.NotNil(t, intrStep, "intrinsic step must be present")
	assert.Equal(t, 3, intrStep.Metadata["ref_count"],
		"ref_count should equal the number of matching refs")
	scanCount, _ := intrStep.Metadata["scan_count"].(int)
	assert.Equal(t, 0, scanCount,
		"scan_count should be 0 for the KLL map path")

	// Descending order: rows[0] should be newer than rows[1].
	if len(rows) == 2 {
		require.NotNil(t, rows[0].Block, "Block should be populated for Case B")
		require.NotNil(t, rows[1].Block, "Block should be populated for Case B")
		ts0Col := rows[0].Block.GetColumn("span:start")
		ts1Col := rows[1].Block.GetColumn("span:start")
		require.NotNil(t, ts0Col, "span:start column should be present in row 0")
		require.NotNil(t, ts1Col, "span:start column should be present in row 1")
		ts0u, ok0 := ts0Col.Uint64Value(rows[0].RowIdx)
		ts1u, ok1 := ts1Col.Uint64Value(rows[1].RowIdx)
		require.True(t, ok0, "span:start should have a value in row 0")
		require.True(t, ok1, "span:start should have a value in row 1")
		assert.Greater(t, ts0u, ts1u, "row[0] should have a newer timestamp than row[1] (descending)")
	}
}

// EX-PERF-02: TestCollect_IntrinsicTopK_ScanPath_Stats
//
// Verifies Case B scan path by forcing SortScanThreshold = 2, which means M >= 2 refs
// triggers the scan path. Confirms ExecutionPath="intrinsic-topk-scan" and
// IntrinsicScanCount > 0.
//
// Must NOT call t.Parallel() — SortScanThreshold is a package-level variable.
func TestCollect_IntrinsicTopK_ScanPath_Stats(t *testing.T) {
	// No t.Parallel() — modifies package-level SortScanThreshold.
	oldT := executor.SortScanThreshold
	executor.SortScanThreshold = 2
	t.Cleanup(func() { executor.SortScanThreshold = oldT })

	data := buildPerfTestReader(t, "target", 3, 3)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "target" }`)

	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		Limit:           2,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	assert.Equal(t, "intrinsic-topk-scan", qs.ExecutionPath,
		"scan path (large M) should set ExecutionPath=intrinsic-topk-scan")
	assert.Greater(t, intrinsicStep(qs).Metadata["scan_count"].(int), 0,
		"IntrinsicScanCount should be > 0 for the scan path")
}

// EX-PERF-03: TestCollect_ExecutionPath_AllPaths
//
// Table-driven sub-tests confirming each of the seven primary ExecutionPath values
// is produced by the correct query shape.
//
// Sub-tests that override SortScanThreshold must NOT call t.Parallel().
func TestCollect_ExecutionPath_AllPaths(t *testing.T) {
	// Build a shared reader with both intrinsic (service.name) and non-intrinsic
	// (span.http.method) attributes so all cases can be exercised.
	//
	// maxBlockSpans=2 forces multiple internal blocks. The mixed cases (C/D) require
	// the selectivity guard to pass: uniqueBlocks*2 <= r.BlockCount(). With 4 spans of
	// "svc" across >=3 blocks and only 1 target block, the guard passes.
	// We write 2 spans of "svc" and 6 spans of "other-svc" (maxBlockSpans=2 → 4 blocks
	// total). The "svc" spans land in 1 block; 1*2 <= 4 satisfies the guard.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 2) // maxBlockSpans=2 → multiple internal blocks
	tid := [16]byte{0xBB}
	// 2 spans with "svc" + http.method=GET (target for mixed queries)
	for i := range 2 {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{strAttr("http.method", "GET")},
			map[string]any{"service.name": "svc"})
	}
	// 6 spans with "other-svc" — populate extra blocks so BlockCount >= 4
	tid2 := [16]byte{0xCC}
	for i := range 6 {
		addSpan(t, w, tid2, 100+i, "op", tracev1.Span_SPAN_KIND_CLIENT,
			[]*commonv1.KeyValue{strAttr("http.method", "POST")},
			map[string]any{"service.name": "other-svc"})
	}
	mustFlush(t, w)
	data := buf.Bytes()

	t.Run("intrinsic-plain", func(t *testing.T) {
		// Case A: pure intrinsic + no TimestampColumn + Limit
		r := openReader(t, data)
		program := compileQuery(t, `{ resource.service.name = "svc" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, "intrinsic-plain", qs.ExecutionPath)
		intrStep := mustFindStep(t, qs, "intrinsic")
		assert.Greater(t, intrStep.Duration, time.Duration(0), "intrinsic step duration must be positive")
	})

	t.Run("intrinsic-topk-kll", func(t *testing.T) {
		// Case B KLL path: pure intrinsic + TimestampColumn + Limit + small M
		r := openReader(t, data)
		program := compileQuery(t, `{ resource.service.name = "svc" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{
			Limit:           10,
			TimestampColumn: "span:start",
			Direction:       queryplanner.Backward,
		})
		require.NoError(t, err)
		assert.Equal(t, "intrinsic-topk-kll", qs.ExecutionPath)
		intrStep := mustFindStep(t, qs, "intrinsic")
		assert.Greater(t, intrStep.Duration, time.Duration(0), "intrinsic step duration must be positive")
		assert.Greater(t, intrStep.Metadata["ref_count"].(int), 0,
			"IntrinsicRefCount should be set for Case B")
	})

	t.Run("intrinsic-topk-scan", func(t *testing.T) {
		// No t.Parallel() — modifies SortScanThreshold.
		oldT := executor.SortScanThreshold
		executor.SortScanThreshold = 1
		t.Cleanup(func() { executor.SortScanThreshold = oldT })

		r := openReader(t, data)
		program := compileQuery(t, `{ resource.service.name = "svc" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{
			Limit:           10,
			TimestampColumn: "span:start",
			Direction:       queryplanner.Backward,
		})
		require.NoError(t, err)
		assert.Equal(t, "intrinsic-topk-scan", qs.ExecutionPath)
		intrStep := mustFindStep(t, qs, "intrinsic")
		assert.Greater(t, intrStep.Duration, time.Duration(0), "intrinsic step duration must be positive")
		assert.Greater(t, intrStep.Metadata["scan_count"].(int), 0,
			"IntrinsicScanCount should be > 0 for the scan path")
	})

	t.Run("mixed-plain", func(t *testing.T) {
		// Case C: mixed (intrinsic + non-intrinsic) + no TimestampColumn
		r := openReader(t, data)
		program := compileQuery(t, `{ resource.service.name = "svc" && span.http.method = "GET" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{
			Limit: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, "mixed-plain", qs.ExecutionPath)
		prefilterStep := mustFindStep(t, qs, "mixed-prefilter")
		assert.Greater(t, prefilterStep.Duration, time.Duration(0))
		assert.GreaterOrEqual(t, prefilterStep.Metadata["candidate_blocks"].(int), 1,
			"MixedCandidateBlocks should be >= 1 for mixed queries")
		// Mixed paths fetch candidate blocks via forEachBlockInGroups but do not emit
		// a separate "block-scan" step — I/O is bundled inside the mixed prefilter phase.
	})

	t.Run("mixed-topk", func(t *testing.T) {
		// Case D: mixed + TimestampColumn + Limit
		r := openReader(t, data)
		program := compileQuery(t, `{ resource.service.name = "svc" && span.http.method = "GET" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{
			Limit:           2,
			TimestampColumn: "span:start",
			Direction:       queryplanner.Backward,
		})
		require.NoError(t, err)
		assert.Equal(t, "mixed-topk", qs.ExecutionPath)
		prefilterStep := mustFindStep(t, qs, "mixed-prefilter")
		assert.Greater(t, prefilterStep.Duration, time.Duration(0))
		assert.GreaterOrEqual(t, prefilterStep.Metadata["candidate_blocks"].(int), 1,
			"MixedCandidateBlocks should be >= 1 for mixed queries")
		// Mixed paths fetch candidate blocks via forEachBlockInGroups but do not emit
		// a separate "block-scan" step — I/O is bundled inside the mixed prefilter phase.
	})

	t.Run("block-plain", func(t *testing.T) {
		// No intrinsic predicates → full block scan, plain path.
		r := openReader(t, data)
		program := compileQuery(t, `{ span.http.method = "GET" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{})
		require.NoError(t, err)
		assert.Equal(t, "block-plain", qs.ExecutionPath)
		planStep := mustFindStep(t, qs, "plan")
		assert.Greater(t, planStep.Duration, time.Duration(0), "plan step duration must be positive")
		assert.Greater(t, planStep.Metadata["total_blocks"].(int), 0)
		assert.Greater(t, planStep.Metadata["selected_blocks"].(int), 0)
		scanStep := mustFindStep(t, qs, "block-scan")
		assert.Greater(t, scanStep.Duration, time.Duration(0), "scan step duration must be positive")
		assert.Greater(t, scanStep.BytesRead, int64(0), "BytesRead must be positive for block reads")
		assert.Greater(t, scanStep.IOOps, 0, "IOOps must be positive for block reads")
		assert.Greater(t, scanStep.Metadata["fetched_blocks"].(int), 0)
		assert.Greater(t, scanStep.Metadata["matched_rows"].(int), 0)
	})

	t.Run("block-topk", func(t *testing.T) {
		// No intrinsic predicates + TimestampColumn + Limit → block-scan topK path.
		r := openReader(t, data)
		program := compileQuery(t, `{ span.http.method = "GET" }`)
		_, qs, err := executor.Collect(r, program, executor.CollectOptions{
			Limit:           2,
			TimestampColumn: "span:start",
			Direction:       queryplanner.Backward,
		})
		require.NoError(t, err)
		assert.Equal(t, "block-topk", qs.ExecutionPath)
		planStep := mustFindStep(t, qs, "plan")
		assert.Greater(t, planStep.Duration, time.Duration(0), "plan step duration must be positive")
		assert.Greater(t, planStep.Metadata["total_blocks"].(int), 0)
		assert.Greater(t, planStep.Metadata["selected_blocks"].(int), 0)
		scanStep := mustFindStep(t, qs, "block-scan")
		assert.Greater(t, scanStep.Duration, time.Duration(0), "scan step duration must be positive")
		assert.Greater(t, scanStep.BytesRead, int64(0), "BytesRead must be positive for block reads")
		assert.Greater(t, scanStep.IOOps, 0, "IOOps must be positive for block reads")
		assert.Greater(t, scanStep.Metadata["fetched_blocks"].(int), 0)
		assert.Greater(t, scanStep.Metadata["matched_rows"].(int), 0)
	})
}

// EX-PERF-04: TestCollect_IntrinsicTopK_KLLPath
//
// Verifies that Case B (pure intrinsic + sort, KLL path) uses block-level MaxStart ordering
// and correctly retrieves all matching refs even when they reside in the oldest internal block.
//
// Setup: 5 internal blocks (MaxBlockSpans=100, 500 total spans).
//   - 3 rare-svc spans written first (spanIdx 0,1,2 → lowest timestamps) → land in block 0
//     with the lowest MaxStart across all 5 blocks.
//   - 497 common-svc spans (spanIdx 3..499 → higher timestamps) → fill blocks 0–4.
//
// After MaxStart DESC sort, block 0 (oldest) is processed LAST, but the final global sort
// over all M=3 pairs still returns all 3 rare-svc spans. Limit=10 is non-binding (M=3 < 10).
//
// Also verifies SPEC-STREAM-3: KLL path reads no block bytes (FetchedBlocks==0).
//
// 3 < SortScanThreshold so the KLL path is always taken.
func TestCollect_IntrinsicTopK_KLLPath(t *testing.T) {
	// Not parallel: reads SortScanThreshold but does not modify it.

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 100) // MaxBlockSpans=100 → 5 internal blocks for 500 spans

	tid := [16]byte{0xAA}
	// Write 3 rare-svc spans first with the lowest spanIdx values (lowest timestamps).
	// These land in block 0, giving block 0 the lowest MaxStart of all 5 blocks.
	for i := range 3 {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
			map[string]any{"service.name": "rare-svc"})
	}
	// Write 497 common-svc spans with higher spanIdx values (higher timestamps).
	// These fill blocks 0–4 and give blocks 1–4 higher MaxStart values.
	for i := range 497 {
		addSpan(t, w, tid, 3+i, "op", tracev1.Span_SPAN_KIND_CLIENT, nil,
			map[string]any{"service.name": "common-svc"})
	}
	mustFlush(t, w)

	r := openReader(t, buf.Bytes())
	program := compileQuery(t, `{ resource.service.name = "rare-svc" }`)

	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		Limit:           10, // non-binding: only 3 rare-svc spans exist
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err)
	// All 3 rare-svc spans must be returned despite living in the oldest block (lowest
	// MaxStart). Block-level MaxStart ordering only affects traversal order, not correctness:
	// the final global sort over all 3 pairs yields all results regardless of block order.
	assert.Len(t, rows, 3, "all 3 rare-svc spans must be returned from the oldest block")

	assert.Equal(t, "intrinsic-topk-kll", qs.ExecutionPath,
		"Case B KLL path should set ExecutionPath=intrinsic-topk-kll")
	assert.Equal(t, 3, intrinsicStep(qs).Metadata["ref_count"].(int),
		"IntrinsicRefCount should equal the number of matching refs")
	assert.Equal(t, 0, intrinsicStep(qs).Metadata["scan_count"].(int),
		"IntrinsicScanCount should be 0 for the KLL path")
	// Intrinsic KLL path does not produce a block-scan step (no full blocks read).
	assert.Nil(t, blockScanStep(qs), "KLL path must not read full blocks")

	// Descending order: rows[0] should be newer than rows[1].
	if len(rows) >= 2 {
		require.NotNil(t, rows[0].Block, "Block should be populated for Case B")
		require.NotNil(t, rows[1].Block, "Block should be populated for Case B")
		ts0Col := rows[0].Block.GetColumn("span:start")
		ts1Col := rows[1].Block.GetColumn("span:start")
		require.NotNil(t, ts0Col, "span:start column should be present in row 0")
		require.NotNil(t, ts1Col, "span:start column should be present in row 1")
		ts0u, ok0 := ts0Col.Uint64Value(rows[0].RowIdx)
		ts1u, ok1 := ts1Col.Uint64Value(rows[1].RowIdx)
		require.True(t, ok0, "span:start should have a value in row 0")
		require.True(t, ok1, "span:start should have a value in row 1")
		assert.Greater(t, ts0u, ts1u, "row[0] should have a newer timestamp than row[1] (descending)")
	}
}

// GAP-21: TestCollect_IntrinsicTopK_EmptyResult verifies that a query matching no spans
// returns an empty result on both KLL and scan paths without panicking.
// Must NOT call t.Parallel() — modifies SortScanThreshold.
func TestCollect_IntrinsicTopK_EmptyResult(t *testing.T) {
	// No t.Parallel() — modifies package-level SortScanThreshold.
	data := buildPerfTestReader(t, "existing-service", 5, 0)
	prog := compileQuery(t, `{ resource.service.name = "impossible-service-xyz" }`)

	// KLL path (default threshold).
	r := openReader(t, data)
	rows, _, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err, "KLL path: must not error on empty result")
	require.Empty(t, rows, "KLL path: must return empty result for non-matching query")

	// Scan path (force with SortScanThreshold=1).
	oldT := executor.SortScanThreshold
	executor.SortScanThreshold = 1
	t.Cleanup(func() { executor.SortScanThreshold = oldT })

	r2 := openReader(t, data)
	rows2, _, err := executor.Collect(r2, prog, executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err, "scan path: must not error on empty result")
	require.Empty(t, rows2, "scan path: must return empty result for non-matching query")
}

// GAP-1: TestCollect_SortScanThreshold_Boundary verifies that KLL and scan paths
// produce the same results when the threshold is overridden to force the scan path.
// Must NOT call t.Parallel() — modifies SortScanThreshold.
func TestCollect_SortScanThreshold_Boundary(t *testing.T) {
	// No t.Parallel() — modifies package-level SortScanThreshold.
	const targetCount = 4
	data := buildPerfTestReader(t, "target-svc", targetCount, 2)
	prog := compileQuery(t, `{ resource.service.name = "target-svc" }`)

	// KLL path (high threshold — 4 refs < 10000).
	oldT := executor.SortScanThreshold
	executor.SortScanThreshold = 10000
	t.Cleanup(func() { executor.SortScanThreshold = oldT })

	r1 := openReader(t, data)
	rowsKLL, _, err := executor.Collect(r1, prog, executor.CollectOptions{
		Limit:           20,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err)

	// Scan path (force with threshold=3 — 4 refs >= 3).
	executor.SortScanThreshold = 3

	r2 := openReader(t, data)
	rowsScan, _, err := executor.Collect(r2, prog, executor.CollectOptions{
		Limit:           20,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
	})
	require.NoError(t, err)

	require.Equal(t, len(rowsKLL), len(rowsScan),
		"KLL and scan paths must return the same number of rows")
}

// GAP-25: TestCollect_TimestampFilterBoundary verifies that TimeRange boundaries are inclusive:
// a point query [T, T] returns exactly the span whose start equals T and no others.
// Uses the intrinsic fast path (Limit > 0 + TimestampColumn) to populate Block.
func TestCollect_TimestampFilterBoundary(t *testing.T) {
	// Build 3 spans at distinct timestamps using spanIdx 0, 1, 2.
	// makeSpan sets StartTimeUnixNano = 1_000_000_000 + spanIdx*1000.
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0xD0}
	for i := range 3 {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil,
			map[string]any{"service.name": "ts-svc"})
	}
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())
	prog := compileQuery(t, `{ resource.service.name = "ts-svc" }`)

	// spanIdx=1 → StartTimeUnixNano = 1_000_001_000.
	const targetTS = uint64(1_000_001_000)
	rows, _, err := executor.Collect(r, prog, executor.CollectOptions{
		Limit:           10, // Limit triggers intrinsic fast path, populating Block.
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
		TimeRange: queryplanner.TimeRange{
			MinNano: targetTS,
			MaxNano: targetTS,
		},
	})
	require.NoError(t, err)
	require.Len(t, rows, 1, "point query [T,T] must return exactly 1 span")
	require.NotNil(t, rows[0].Block, "Block must be populated by intrinsic path")
	tsCol := rows[0].Block.GetColumn("span:start")
	require.NotNil(t, tsCol, "span:start column must be present in Block")
	ts, ok := tsCol.Uint64Value(rows[0].RowIdx)
	require.True(t, ok, "span:start must have a value")
	require.Equal(t, targetTS, ts, "returned span must have timestamp == T")
}

// GAP-27 (shard): TestCollect_ShardFilter verifies that two non-overlapping shards together
// cover all blocks and return the combined full result set.
func TestCollect_ShardFilter(t *testing.T) {
	// Write 10 spans with MaxBlockSpans=2 → 5 internal blocks (indices 0-4).
	var buf bytes.Buffer
	const totalSpans = 10
	w := mustNewWriter(t, &buf, 2)
	tid := [16]byte{0xD1}
	for i := range totalSpans {
		addSpan(t, w, tid, i, "op", tracev1.Span_SPAN_KIND_CLIENT, nil,
			map[string]any{"service.name": "shard-svc"})
	}
	mustFlush(t, w)
	data := buf.Bytes()
	prog := compileQuery(t, `{ resource.service.name = "shard-svc" }`)

	// Shard A: blocks [0, 3) — first 3 blocks.
	rA := openReader(t, data)
	rowsA, _, err := executor.Collect(rA, prog, executor.CollectOptions{
		StartBlock: 0,
		BlockCount: 3,
	})
	require.NoError(t, err)

	// Shard B: blocks [3, 2) — last 2 blocks.
	rB := openReader(t, data)
	rowsB, _, err := executor.Collect(rB, prog, executor.CollectOptions{
		StartBlock: 3,
		BlockCount: 2,
	})
	require.NoError(t, err)

	// Together they must cover all spans with no overlap.
	combined := len(rowsA) + len(rowsB)
	require.Equal(t, totalSpans, combined,
		"shards [0,3) and [3,2) must together return all %d spans", totalSpans)
}

// EX-PERF-05: TestCollect_IntrinsicTopK_ScanPath_TimeRange
//
// Regression test for the bug where collectIntrinsicTopKScan did not apply
// opts.TimeRange filtering. The KLL path correctly filtered by time range;
// the scan path did not — rows outside the window were silently included.
//
// Setup: 5 target spans with timestamps [1_000_000_000, 1_000_001_000, ..., 1_000_004_000].
// TimeRange: [1_000_001_000, 1_000_003_000] — only spans 1, 2, 3 are in range (3 rows).
// SortScanThreshold = 1 forces the scan path even for 5 refs.
//
// Must NOT call t.Parallel() — modifies SortScanThreshold.
func TestCollect_IntrinsicTopK_ScanPath_TimeRange(t *testing.T) {
	// No t.Parallel() — modifies package-level SortScanThreshold.
	oldT := executor.SortScanThreshold
	executor.SortScanThreshold = 1 // force scan path for any ref count >= 1
	t.Cleanup(func() { executor.SortScanThreshold = oldT })

	// 5 target spans with spanIdx 0..4 → timestamps 1_000_000_000..1_000_004_000.
	data := buildPerfTestReader(t, "target-svc", 5, 0)
	r := openReader(t, data)
	program := compileQuery(t, `{ resource.service.name = "target-svc" }`)

	// TimeRange: [1_000_001_000, 1_000_003_000] — spans 1, 2, 3 qualify (3 rows).
	const minNano = uint64(1_000_001_000)
	const maxNano = uint64(1_000_003_000)

	rows, qs, err := executor.Collect(r, program, executor.CollectOptions{
		Limit:           10,
		TimestampColumn: "span:start",
		Direction:       queryplanner.Backward,
		TimeRange: queryplanner.TimeRange{
			MinNano: minNano,
			MaxNano: maxNano,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "intrinsic-topk-scan", qs.ExecutionPath,
		"SortScanThreshold=1 forces scan path")

	// Critical assertion: only rows within [minNano, maxNano] are returned.
	require.Len(t, rows, 3, "expect exactly 3 rows in time window [1_000_001_000, 1_000_003_000]")
	for _, row := range rows {
		require.NotNil(t, row.Block, "Block must be populated")
		tsCol := row.Block.GetColumn("span:start")
		require.NotNil(t, tsCol, "span:start column must be present")
		tsVal, ok := tsCol.Uint64Value(row.RowIdx)
		require.True(t, ok, "span:start must have a value")
		assert.GreaterOrEqual(t, tsVal, minNano, "timestamp must be >= MinNano")
		assert.LessOrEqual(t, tsVal, maxNano, "timestamp must be <= MaxNano")
	}
}

// Test step helpers.
func intrinsicStep(qs executor.QueryStats) *executor.StepStats { return findStep(qs, "intrinsic") }
func blockScanStep(qs executor.QueryStats) *executor.StepStats { return findStep(qs, "block-scan") }

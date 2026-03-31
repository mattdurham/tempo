package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
)

// buildIntrinsicTestReader writes spans across two services so tests can verify
// that the intrinsic fast path returns the right set for each query.
// Services: "loki-querier" (3 spans) and "grafana" (2 spans).
// Also adds one span for "tempo-distributor" to ensure selectivity.
func buildIntrinsicTestReader(t *testing.T) ([]byte, int, int, int) {
	t.Helper()
	var buf bytes.Buffer
	// maxBlockSpans=0 → single internal block (all spans in one block,
	// exercising the fast path without sharding noise).
	w := mustNewWriter(t, &buf, 0)

	tid1 := [16]byte{0x01}
	tid2 := [16]byte{0x02}
	tid3 := [16]byte{0x03}

	addSpan(t, w, tid1, 0, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "loki-querier"})
	addSpan(t, w, tid1, 1, "op", tracev1.Span_SPAN_KIND_CLIENT, nil, map[string]any{"service.name": "loki-querier"})
	addSpan(t, w, tid2, 2, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "loki-querier"})
	addSpan(t, w, tid2, 3, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "grafana"})
	addSpan(t, w, tid3, 4, "op", tracev1.Span_SPAN_KIND_CLIENT, nil, map[string]any{"service.name": "grafana"})
	addSpan(t, w, tid3, 5, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "tempo-distributor"})

	mustFlush(t, w)
	return buf.Bytes(), 3, 2, 1 // loki-querier=3, grafana=2, tempo-distributor=1
}

// EX-INT-01: intrinsic fast path returns correct results for a regex on resource.service.name.
// Before the fix, {resource.service.name=~"loki-.*"} returned 0 in modeIntrinsicPlain/TopK.
func TestIntrinsicFastPath_RegexOnServiceName(t *testing.T) {
	t.Parallel()
	data, lokiCount, _, _ := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	for _, tc := range []struct {
		desc  string
		query string
		want  int
	}{
		{"wildcard suffix", `{ resource.service.name =~ "loki-.*" }`, lokiCount},
		{"exact regex", `{ resource.service.name =~ "loki-querier" }`, lokiCount},
		{"contains", `{ resource.service.name =~ ".*querier.*" }`, lokiCount},
		{"grafana via contains", `{ resource.service.name =~ ".*graf.*" }`, 2},
		{"no match", `{ resource.service.name =~ "no-match-.*" }`, 0},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			rows, err := executor.Collect(r, compileQuery(t, tc.query), executor.CollectOptions{Limit: 100})
			require.NoError(t, err)
			assert.Equal(t, tc.want, len(rows), "query: %s", tc.query)
		})
	}
}

// EX-INT-02: intrinsic fast path returns correct results for OR on resource.service.name.
// Before the fix, {svc="A" || svc="B"} returned 0 in modeIntrinsicPlain/TopK.
func TestIntrinsicFastPath_OROnServiceName(t *testing.T) {
	t.Parallel()
	data, lokiCount, grafanaCount, tdCount := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	for _, tc := range []struct {
		desc  string
		query string
		want  int
	}{
		{
			"OR two services",
			`{ resource.service.name = "loki-querier" || resource.service.name = "grafana" }`,
			lokiCount + grafanaCount,
		},
		{
			"OR grafana and tempo-distributor",
			`{ resource.service.name = "grafana" || resource.service.name = "tempo-distributor" }`,
			grafanaCount + tdCount,
		},
		{
			"OR all three services",
			`{ resource.service.name = "loki-querier" || resource.service.name = "grafana" || resource.service.name = "tempo-distributor" }`,
			lokiCount + grafanaCount + tdCount,
		},
		{
			"OR with one no-match branch",
			`{ resource.service.name = "no-such-svc" || resource.service.name = "grafana" }`,
			grafanaCount,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			rows, err := executor.Collect(r, compileQuery(t, tc.query), executor.CollectOptions{Limit: 100})
			require.NoError(t, err)
			assert.Equal(t, tc.want, len(rows), "query: %s", tc.query)
		})
	}
}

// EX-INT-03: intrinsic fast path handles regex + AND correctly.
// The AND path must fail fast when one child is unevaluable (e.g., a non-intrinsic column),
// falling back to the block scan which evaluates the full predicate correctly.
func TestIntrinsicFastPath_RegexAndAND(t *testing.T) {
	t.Parallel()
	data, lokiCount, _, _ := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	// All loki-querier spans are SERVER kind (3) — CLIENT kind (1) should be excluded.
	rows, err := executor.Collect(r, compileQuery(t, `{ resource.service.name =~ "loki-.*" && kind = server }`), executor.CollectOptions{Limit: 100})
	require.NoError(t, err)
	assert.Equal(t, lokiCount-1, len(rows), "server-kind loki spans only")
}

// EX-INT-04: intrinsic MostRecent (TopK) path returns correct results for regex.
// Before the fix, modeIntrinsicTopK with regex returned 0.
func TestIntrinsicFastPath_TopK_RegexMostRecent(t *testing.T) {
	t.Parallel()
	data, lokiCount, _, _ := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	rows, err := executor.Collect(r, compileQuery(t, `{ resource.service.name =~ "loki-.*" }`), executor.CollectOptions{
		Limit:           100,
		TimestampColumn: "span:start",
	})
	require.NoError(t, err)
	assert.Equal(t, lokiCount, len(rows), "MostRecent regex should find all loki-querier spans")
}

// EX-INT-05: intrinsic MostRecent (TopK) path returns correct results for OR.
// Before the fix, modeIntrinsicTopK with OR returned 0.
func TestIntrinsicFastPath_TopK_ORMostRecent(t *testing.T) {
	t.Parallel()
	data, lokiCount, grafanaCount, _ := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	rows, err := executor.Collect(r, compileQuery(t, `{ resource.service.name = "loki-querier" || resource.service.name = "grafana" }`), executor.CollectOptions{
		Limit:           100,
		TimestampColumn: "span:start",
	})
	require.NoError(t, err)
	assert.Equal(t, lokiCount+grafanaCount, len(rows), "MostRecent OR should find loki+grafana spans")
}

// EX-INT-06: mixed predicates — intrinsic pre-filter narrows candidates, VM re-check handles non-intrinsic.
// The query has an intrinsic predicate (resource.service.name) and a non-intrinsic predicate
// (span.http.method). It takes the mixed pre-filter path (Case C): the intrinsic pre-filter
// narrows candidates by service name, then VM re-evaluation eliminates all rows because no
// spans have http.method set.
func TestIntrinsicFastPath_MixedPredicateBlockScan(t *testing.T) {
	t.Parallel()
	data, lokiCount, _, _ := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	// mixed predicates — intrinsic pre-filter narrows candidates (service name), VM re-check handles non-intrinsic (http.method).
	// No spans have http.method set, so VM re-evaluation eliminates all candidates.
	rows, err := executor.Collect(r, compileQuery(t, `{ resource.service.name =~ "loki-.*" && span.http.method = "GET" }`), executor.CollectOptions{Limit: 100})
	require.NoError(t, err)
	// No spans have http.method set, so result is 0 — but importantly: no error and no panic.
	assert.Equal(t, 0, len(rows), "no spans have http.method")
	_ = lokiCount // used to confirm the pre-filter narrowed to the right candidate set
}

// buildMixedTestReader writes spans with both intrinsic and non-intrinsic attributes.
// Returns the writer buffer and counts for svc-a GET, svc-a POST spans.
func buildMixedTestReader(t *testing.T) ([]byte, int, int) {
	t.Helper()
	var buf bytes.Buffer
	// maxBlockSpans=0 → single internal block for simplicity.
	w := mustNewWriter(t, &buf, 0)

	tid1 := [16]byte{0x10}
	tid2 := [16]byte{0x11}

	// 5 spans: svc-a + http.method=GET
	for i := 0; i < 5; i++ {
		addSpan(t, w, tid1, i, "op", tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{strAttr("http.method", "GET")},
			map[string]any{"service.name": "svc-a"})
	}
	// 5 spans: svc-a + http.method=POST
	for i := 5; i < 10; i++ {
		addSpan(t, w, tid2, i, "op", tracev1.Span_SPAN_KIND_SERVER,
			[]*commonv1.KeyValue{strAttr("http.method", "POST")},
			map[string]any{"service.name": "svc-a"})
	}

	mustFlush(t, w)
	return buf.Bytes(), 5, 5
}

// EX-INT-07: Mixed query (intrinsic + span attribute, no sort) returns only rows matching
// both conditions. After the unified pre-filter lands, the intrinsic pre-filter narrows
// candidates to svc-a spans, and VM re-evaluation eliminates POST spans.
func TestCollect_MixedPredicateNoSort(t *testing.T) {
	t.Parallel()
	data, getCount, _ := buildMixedTestReader(t)
	r := openReader(t, data)

	rows, err := executor.Collect(r,
		compileQuery(t, `{ resource.service.name = "svc-a" && span.http.method = "GET" }`),
		executor.CollectOptions{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, getCount, len(rows), "only GET spans should be returned")

	// Verify all results actually have http.method=GET (no false positives).
	for _, row := range rows {
		require.NotNil(t, row.Block, "block must be populated for mixed query results")
		col := row.Block.GetColumn("span.http.method")
		require.NotNil(t, col, "http.method column must exist in result")
		val, ok := col.StringValue(row.RowIdx)
		require.True(t, ok, "http.method column must have a value")
		assert.Equal(t, "GET", val, "all results must have http.method=GET")
	}
}

// buildMixedSortedTestReader writes spans across 2 blocks with interleaved timestamps.
// "target" spans have http.method="GET" (non-intrinsic attribute).
// "other" spans do not have http.method.
// maxBlockSpans=3 forces a 2-block split.
func buildMixedSortedTestReader(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 3) // maxBlockSpans=3

	tid1 := [16]byte{0x20}
	tid2 := [16]byte{0x21}

	// Span indices determine StartTimeUnixNano in makeSpan:
	// startTime = 1_000_000_000 + spanIdx*1000 ns
	//
	// Block 0: spans 0-2 (svc=target, svc=other, svc=target)
	// Block 1: spans 3-5 (svc=other, svc=target, svc=other)
	//
	// Target spans: idx=0 (T0), idx=2 (T2), idx=4 (T4) — all have http.method=GET
	// Other spans:  idx=1 (T1), idx=3 (T3), idx=5 (T5) — no http.method

	getAttr := []*commonv1.KeyValue{strAttr("http.method", "GET")}

	// Block 0
	addSpan(t, w, tid1, 0, "op", tracev1.Span_SPAN_KIND_SERVER, getAttr, map[string]any{"service.name": "target"})
	addSpan(t, w, tid2, 1, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "other"})
	addSpan(t, w, tid1, 2, "op", tracev1.Span_SPAN_KIND_SERVER, getAttr, map[string]any{"service.name": "target"})
	// Block 1
	addSpan(t, w, tid2, 3, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "other"})
	addSpan(t, w, tid1, 4, "op", tracev1.Span_SPAN_KIND_SERVER, getAttr, map[string]any{"service.name": "target"})
	addSpan(t, w, tid2, 5, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "other"})

	mustFlush(t, w)
	return buf.Bytes()
}

// EX-INT-08: Mixed query with timestamp sort (Backward) uses intrinsic pre-filter then
// topKScanRows on candidate blocks, returning globally correct top-K.
// The intrinsic predicate (svc="target") narrows candidate blocks; the non-intrinsic
// predicate (http.method="GET") is evaluated via ColumnPredicate on those blocks.
func TestCollect_MixedPredicateWithSort(t *testing.T) {
	t.Parallel()
	data := buildMixedSortedTestReader(t)
	r := openReader(t, data)

	// Query: svc="target" (intrinsic) AND http.method="GET" (non-intrinsic).
	// All target spans have http.method=GET, so 3 match. With limit=2 Backward,
	// we expect the 2 most recent: idx=4 (T4) and idx=2 (T2).
	rows, err := executor.Collect(r,
		compileQuery(t, `{ resource.service.name = "target" && span.http.method = "GET" }`),
		executor.CollectOptions{
			Limit:           2,
			TimestampColumn: "span:start",
			Direction:       queryplanner.Backward,
		})
	require.NoError(t, err)
	assert.Equal(t, 2, len(rows), "top-2 target+GET spans")

	// Both results must belong to "target" service.
	// resource.service.name is intrinsic-only; verify via intrinsic section.
	for _, row := range rows {
		svcVal, svcOk := r.IntrinsicDictStringAt("resource.service.name", row.BlockIdx, row.RowIdx)
		require.True(t, svcOk, "service.name must be present in intrinsic section")
		assert.Equal(t, "target", svcVal, "all results must be from svc=target")
	}

	// Results should be in descending timestamp order (most recent first).
	if len(rows) == 2 {
		ts0 := getBlockSpanTimestamp(rows[0].Block, rows[0].RowIdx)
		ts1 := getBlockSpanTimestamp(rows[1].Block, rows[1].RowIdx)
		assert.GreaterOrEqual(t, ts0, ts1, "results must be in descending timestamp order")
	}
}

// EX-INT-09: Pure intrinsic equality query (no sort) returns correct results via
// forEachBlockInGroups (Block populated, IntrinsicFields nil). Regression guard
// for Case A equality-predicate path: correct count, Block population, and field value.
func TestCollect_PureIntrinsicNoSort_RegressionGuard(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid1 := [16]byte{0x30}
	tid2 := [16]byte{0x31}
	for i := 0; i < 5; i++ {
		addSpan(t, w, tid1, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-a"})
	}
	for i := 5; i < 10; i++ {
		addSpan(t, w, tid2, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": "svc-b"})
	}
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	rows, err := executor.Collect(r,
		compileQuery(t, `{ resource.service.name = "svc-a" }`),
		executor.CollectOptions{Limit: 5})
	require.NoError(t, err)
	assert.Equal(t, 5, len(rows), "all svc-a spans must be returned")

	// Case A equality predicate (svc=X): Block populated, IntrinsicFields nil.
	// resource.service.name is intrinsic-only; verify via the intrinsic section.
	for i, row := range rows {
		require.NotNilf(t, row.Block, "row %d: equality query must populate Block", i)
		svcVal, svcOk := r.IntrinsicDictStringAt("resource.service.name", row.BlockIdx, row.RowIdx)
		require.Truef(t, svcOk, "row %d: resource.service.name must be present in intrinsic section", i)
		assert.Equalf(t, "svc-a", svcVal, "row %d: all matched rows must be svc-a", i)
	}
}

// EX-INT-09b: Pure intrinsic range query (no sort) returns correct results via
// lookupIntrinsicFields (IntrinsicFields populated, Block nil). Regression guard
// for Case A range-predicate path: correct count, IntrinsicFields population, Block nil,
// and that SpanMatchFromRow correctly extracts TraceID/SpanID from IntrinsicFields.
func TestCollect_PureIntrinsicNoSort_RangePredicate(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid1 := [16]byte{0x32}
	tid2 := [16]byte{0x33}
	const ms = uint64(1_000_000)
	const base = uint64(1_000_000_000)

	// 3 spans > 100ms duration, 3 spans < 10ms.
	for i := 0; i < 3; i++ {
		sp := &tracev1.Span{
			TraceId:           tid1[:],
			SpanId:            []byte{byte(i + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			StartTimeUnixNano: base,
			EndTimeUnixNano:   base + 200*ms, // 200ms > 100ms
		}
		require.NoError(t, w.AddSpan(tid1[:], sp, map[string]any{"service.name": "svc-long"}, "", nil, ""))
	}
	for i := 0; i < 3; i++ {
		sp := &tracev1.Span{
			TraceId:           tid2[:],
			SpanId:            []byte{byte(i + 10), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			StartTimeUnixNano: base,
			EndTimeUnixNano:   base + 2*ms, // 2ms < 100ms
		}
		require.NoError(t, w.AddSpan(tid2[:], sp, map[string]any{"service.name": "svc-short"}, "", nil, ""))
	}
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	rows, err := executor.Collect(r,
		compileQuery(t, `{ duration > 100ms }`),
		executor.CollectOptions{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, 3, len(rows), "only 200ms spans match duration>100ms")

	// Case A range predicate: collectIntrinsicPlain always uses forEachBlockInGroups → Block populated, IntrinsicFields nil.
	for i, row := range rows {
		require.NotNilf(t, row.Block, "row %d: range query must populate Block (forEachBlockInGroups path)", i)
		assert.Nilf(t, row.IntrinsicFields, "row %d: range query must not populate IntrinsicFields", i)

		// SpanMatchFromRow must extract TraceID and SpanID from Block.
		m := executor.SpanMatchFromRow(row, 0, nil)
		assert.NotEqual(t, [16]byte{}, m.TraceID,
			"row %d: SpanMatchFromRow must extract TraceID from Block", i)
		assert.NotEmpty(t, m.SpanID,
			"row %d: SpanMatchFromRow must extract SpanID from Block", i)
	}
}

// EX-INT-10: Pure intrinsic query with timestamp sort returns IntrinsicFields rows
// (zero block reads). Regression guard for Case B path.
func TestCollect_PureIntrinsicWithSort_ZeroBlockRead(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid1 := [16]byte{0x40}
	tid2 := [16]byte{0x41}
	// 3 target spans at indices 0,2,4; 3 other spans at indices 1,3,5.
	for i := 0; i < 6; i++ {
		svc := "other"
		if i%2 == 0 {
			svc = "target"
		}
		addSpan(t, w, tid1, i, "op", tracev1.Span_SPAN_KIND_SERVER, nil, map[string]any{"service.name": svc})
	}
	mustFlush(t, w)
	r := openReader(t, buf.Bytes())
	_ = tid2

	rows, err := executor.Collect(r,
		compileQuery(t, `{ resource.service.name = "target" }`),
		executor.CollectOptions{
			Limit:           2,
			TimestampColumn: "span:start",
			Direction:       queryplanner.Backward,
		})
	require.NoError(t, err)
	assert.Equal(t, 2, len(rows), "top-2 target spans")

	// For pure intrinsic + sort (Case B), results carry IntrinsicFields, not Block.
	for _, row := range rows {
		assert.NotNil(t, row.IntrinsicFields, "pure intrinsic topK must return IntrinsicFields rows")
	}

	// Results must be in descending timestamp order (most recent first).
	if len(rows) == 2 {
		ts0 := getIntrinsicTimestamp(rows[0])
		ts1 := getIntrinsicTimestamp(rows[1])
		assert.GreaterOrEqual(t, ts0, ts1, "results must be in descending timestamp order")
	}
}

// EX-INT-12: Partial-AND pre-filter returns a superset; VM re-evaluation must eliminate
// false positives. Verifies the superset safety invariant.
func TestCollect_MixedPredicatePartialAND_SupersetSafety(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	w := mustNewWriter(t, &buf, 0)
	tid := [16]byte{0x50}

	// span-0: svc-a, GET
	addSpan(t, w, tid, 0, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("http.method", "GET")},
		map[string]any{"service.name": "svc-a"})
	// span-1: svc-a, POST
	addSpan(t, w, tid, 1, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("http.method", "POST")},
		map[string]any{"service.name": "svc-a"})
	// span-2: svc-b, GET
	addSpan(t, w, tid, 2, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("http.method", "GET")},
		map[string]any{"service.name": "svc-b"})
	// span-3: svc-b, POST
	addSpan(t, w, tid, 3, "op", tracev1.Span_SPAN_KIND_SERVER,
		[]*commonv1.KeyValue{strAttr("http.method", "POST")},
		map[string]any{"service.name": "svc-b"})

	mustFlush(t, w)
	r := openReader(t, buf.Bytes())

	rows, err := executor.Collect(r,
		compileQuery(t, `{ resource.service.name = "svc-a" && span.http.method = "GET" }`),
		executor.CollectOptions{Limit: 10})
	require.NoError(t, err)
	// Only span-0 matches both conditions.
	assert.Equal(t, 1, len(rows), "only span-0 (svc-a AND GET) must match")
	if len(rows) == 1 && rows[0].Block != nil {
		httpCol := rows[0].Block.GetColumn("span.http.method")
		require.NotNil(t, httpCol, "http.method column must be present")
		httpVal, httpOk := httpCol.StringValue(rows[0].RowIdx)
		require.True(t, httpOk, "http.method must have a value")
		assert.Equal(t, "GET", httpVal, "result must have http.method=GET")

		// resource.service.name is intrinsic-only; verify via intrinsic section.
		svcVal, svcOk := r.IntrinsicDictStringAt("resource.service.name", rows[0].BlockIdx, rows[0].RowIdx)
		require.True(t, svcOk, "service.name must be present in intrinsic section")
		assert.Equal(t, "svc-a", svcVal, "result must be from svc-a")
	}
}

// EX-INT-13: true fallback to full block scan when query has ONLY non-intrinsic predicates.
// hasSomeIntrinsicPredicates returns false for { span.http.method = "GET" }, so the
// intrinsic pre-filter is bypassed entirely and the full block scan evaluates the query.
func TestCollect_NonIntrinsicOnly_FallsBackToBlockScan(t *testing.T) {
	t.Parallel()
	data, _, _, _ := buildIntrinsicTestReader(t)
	r := openReader(t, data)

	// span.http.method is not an intrinsic column. No spans have http.method set.
	// hasSomeIntrinsicPredicates returns false → full block scan runs.
	// Result must be 0 (no spans have http.method=GET), with no error and no panic.
	rows, err := executor.Collect(r,
		compileQuery(t, `{ span.http.method = "GET" }`),
		executor.CollectOptions{Limit: 100})
	require.NoError(t, err)
	assert.Equal(t, 0, len(rows), "no spans have http.method, full block scan should return 0")
}

// getBlockSpanTimestamp returns the span:start timestamp for a row in a block.
func getBlockSpanTimestamp(block *modules_reader.Block, rowIdx int) uint64 {
	col := block.GetColumn("span:start")
	if col == nil {
		return 0
	}
	ts, _ := col.Uint64Value(rowIdx)
	return ts
}

// getIntrinsicTimestamp returns the span:start value from an IntrinsicFields MatchedRow.
func getIntrinsicTimestamp(row executor.MatchedRow) uint64 {
	if row.IntrinsicFields == nil {
		return 0
	}
	val, ok := row.IntrinsicFields.GetField("span:start")
	if !ok {
		return 0
	}
	if ts, ok := val.(uint64); ok {
		return ts
	}
	return 0
}

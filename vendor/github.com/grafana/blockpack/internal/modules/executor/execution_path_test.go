package executor_test

// NOTE: Any changes to this file must be reflected in the corresponding SPECS.md or NOTES.md.

// EP — Execution Path Tests.
// Verifies that queries route to the correct internal execution path and populate
// MatchedRow fields correctly. These are regression guards for collectIntrinsicPlain:
//
//   - All intrinsic predicates (range, equality, regex) → forEachBlockInGroups →
//     Block populated, IntrinsicFields nil.
//   - User attribute predicates (span.http.method) → full block scan →
//     FetchedBlocks>0 (only this outer block-scan path increments FetchedBlocks).
//
// Note: CollectStats.FetchedBlocks only counts ReadGroup calls in the outer block-scan
// path. The intrinsic sub-path (forEachBlockInGroups) keeps FetchedBlocks=0 —
// it does not go through the outer ReadGroup loop.
//
// Each test also verifies result correctness so path and count regressions are
// caught in a single run.

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	"github.com/grafana/blockpack/internal/modules/executor"
)

// buildEPReader writes 5 spans with deliberately varied intrinsic and user attributes:
//
//	span 0: svc-a, SERVER, unset-status, 2ms
//	span 1: svc-a, CLIENT, error,        50ms
//	span 2: svc-b, SERVER, unset-status, 200ms
//	span 3: svc-b, CLIENT, error,        300ms
//	span 4: svc-c, SERVER, unset-status, 2ms,  span.http.method="GET"
//
// Expected counts by predicate:
//
//	duration > 100ms              → spans 2, 3        → 2
//	duration > 10ms               → spans 1, 2, 3     → 3
//	duration > 1s                 → none              → 0
//	status = error                → spans 1, 3        → 2
//	kind = server                 → spans 0, 2, 4     → 3
//	resource.service.name = svc-a → spans 0, 1        → 2
//	resource.service.name = svc-b → spans 2, 3        → 2
//	span.http.method = "GET"      → span 4            → 1
//	duration > 100ms && status = error → span 3       → 1
func buildEPReader(t *testing.T) *modules_reader.Reader {
	t.Helper()

	var buf bytes.Buffer
	w, err := modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  &buf,
		MaxBlockSpans: 10, // explicit capacity > span count — single internal block for I/O assertions
	})
	require.NoError(t, err)

	const ms = uint64(1_000_000) // nanoseconds per millisecond
	const base = uint64(1_000_000_000)

	addEPSpan := func(
		traceID [16]byte,
		idx int,
		kind tracev1.Span_SpanKind,
		status tracev1.Status_StatusCode,
		startNs, endNs uint64,
		spanAttrs []*commonv1.KeyValue,
		resAttrs map[string]any,
	) {
		sp := &tracev1.Span{
			TraceId:           traceID[:],
			SpanId:            []byte{byte(idx + 1), 0, 0, 0, 0, 0, 0, 0}, //nolint:gosec
			Name:              "op",
			StartTimeUnixNano: startNs,
			EndTimeUnixNano:   endNs,
			Kind:              kind,
			Status:            &tracev1.Status{Code: status},
			Attributes:        spanAttrs,
		}
		require.NoError(t, w.AddSpan(traceID[:], sp, resAttrs, "", nil, ""))
	}

	tid := func(i byte) [16]byte { return [16]byte{i} }
	svc := func(name string) map[string]any { return map[string]any{"service.name": name} }

	// span 0: svc-a, SERVER, unset, 2ms
	addEPSpan(tid(1), 0, tracev1.Span_SPAN_KIND_SERVER, tracev1.Status_STATUS_CODE_UNSET,
		base, base+2*ms, nil, svc("svc-a"))
	// span 1: svc-a, CLIENT, error, 50ms
	addEPSpan(tid(1), 1, tracev1.Span_SPAN_KIND_CLIENT, tracev1.Status_STATUS_CODE_ERROR,
		base+1000, base+1000+50*ms, nil, svc("svc-a"))
	// span 2: svc-b, SERVER, unset, 200ms
	addEPSpan(tid(2), 2, tracev1.Span_SPAN_KIND_SERVER, tracev1.Status_STATUS_CODE_UNSET,
		base+2000, base+2000+200*ms, nil, svc("svc-b"))
	// span 3: svc-b, CLIENT, error, 300ms
	addEPSpan(tid(2), 3, tracev1.Span_SPAN_KIND_CLIENT, tracev1.Status_STATUS_CODE_ERROR,
		base+3000, base+3000+300*ms, nil, svc("svc-b"))
	// span 4: svc-c, SERVER, unset, 2ms, span.http.method="GET"
	addEPSpan(tid(3), 4, tracev1.Span_SPAN_KIND_SERVER, tracev1.Status_STATUS_CODE_UNSET,
		base+4000, base+4000+2*ms, []*commonv1.KeyValue{strAttr("http.method", "GET")}, svc("svc-c"))

	_, err = w.Flush()
	require.NoError(t, err)
	return openReader(t, buf.Bytes())
}

// collectWithPath runs a query and captures the execution stats alongside results.
// Limit is set to a large value so the intrinsic fast path is eligible (it requires Limit > 0).
func collectWithPath(t *testing.T, r *modules_reader.Reader, query string) ([]executor.MatchedRow, executor.CollectStats) {
	t.Helper()
	var stats executor.CollectStats
	rows, err := executor.Collect(r, compileQuery(t, query), executor.CollectOptions{
		Limit:   10000,
		OnStats: func(s executor.CollectStats) { stats = s },
	})
	require.NoErrorf(t, err, "Collect(%q)", query)
	return rows, stats
}

// EP-01: Range predicates on intrinsic columns route to intrinsic-plain and populate
// MatchedRow.Block (not IntrinsicFields).
//
// {duration>X} triggers collectIntrinsicPlain which always uses forEachBlockInGroups,
// returning results via MatchedRow.Block. FetchedBlocks counts outer ReadGroup calls
// only (block-scan path), so it remains 0 for all intrinsic-path queries.
func TestExecutionPath_RangePredicate_BlockPopulated(t *testing.T) {
	t.Parallel()
	r := buildEPReader(t)

	for _, tc := range []struct {
		query string
		want  int
	}{
		{`{ duration > 100ms }`, 2}, // spans 2, 3
		{`{ duration > 10ms }`, 3},  // spans 1, 2, 3
	} {
		tc := tc
		t.Run(tc.query, func(t *testing.T) {
			rows, stats := collectWithPath(t, r, tc.query)
			assert.Equal(t, tc.want, len(rows), "result count")
			assert.Equal(t, "intrinsic-plain", stats.ExecutionPath, "must route to intrinsic-plain")
			assert.Equal(t, 0, stats.FetchedBlocks, "intrinsic path must not increment outer ReadGroup counter")
			for i, row := range rows {
				assert.NotNil(t, row.Block,
					"row %d: range predicate must populate Block (forEachBlockInGroups path)", i)
				assert.Nil(t, row.IntrinsicFields,
					"row %d: range predicate must not populate IntrinsicFields", i)
			}
		})
	}
}

// EP-02: Equality predicates on intrinsic columns route to intrinsic-plain and populate
// MatchedRow.Block (not IntrinsicFields).
//
// {status=error}, {kind=server}, {resource.service.name=X} are equality (Values) predicates.
// collectIntrinsicPlain uses forEachBlockInGroups, returning results via MatchedRow.Block.
// FetchedBlocks remains 0 because it only counts outer ReadGroup calls (block-scan path),
// not the inner forEachBlockInGroups reads. Same execution path as range predicates.
func TestExecutionPath_EqualityPredicate_BlockPopulated(t *testing.T) {
	t.Parallel()
	r := buildEPReader(t)

	for _, tc := range []struct {
		query string
		want  int
	}{
		{`{ status = error }`, 2},                  // spans 1, 3
		{`{ kind = server }`, 3},                   // spans 0, 2, 4
		{`{ resource.service.name = "svc-a" }`, 2}, // spans 0, 1
		{`{ resource.service.name = "svc-b" }`, 2}, // spans 2, 3
	} {
		tc := tc
		t.Run(tc.query, func(t *testing.T) {
			rows, stats := collectWithPath(t, r, tc.query)
			assert.Equal(t, tc.want, len(rows), "result count")
			assert.Equal(t, "intrinsic-plain", stats.ExecutionPath, "must route to intrinsic-plain")
			for i, row := range rows {
				assert.NotNil(t, row.Block,
					"row %d: equality predicate must populate Block (forEachBlockInGroups path)", i)
				assert.Nil(t, row.IntrinsicFields,
					"row %d: equality predicate must not populate IntrinsicFields (lookupIntrinsicFields path)", i)
			}
		})
	}
}

// EP-03: Range+equality combination routes to intrinsic-plain and populates Block.
//
// collectIntrinsicPlain always uses forEachBlockInGroups for the entire group,
// regardless of whether any predicate is a range predicate. Results are returned
// via MatchedRow.Block, not IntrinsicFields.
func TestExecutionPath_RangeAndEquality_BlockPopulated(t *testing.T) {
	t.Parallel()
	r := buildEPReader(t)

	rows, stats := collectWithPath(t, r, `{ duration > 100ms && status = error }`)
	assert.Equal(t, 1, len(rows), "only span 3 has both duration>100ms and status=error")
	assert.Equal(t, "intrinsic-plain", stats.ExecutionPath, "must route to intrinsic-plain")
	for i, row := range rows {
		assert.NotNil(t, row.Block,
			"row %d: range+equality must populate Block (forEachBlockInGroups path)", i)
		assert.Nil(t, row.IntrinsicFields,
			"row %d: range+equality must not populate IntrinsicFields", i)
	}
}

// EP-04: User attribute query uses block scan, not the intrinsic fast path.
//
// span.http.method is a user attribute stored in internal block data, not in the
// intrinsic flat blob. There is no intrinsic predicate, so the intrinsic fast path
// is bypassed entirely and the query uses the block-scan path.
func TestExecutionPath_UserAttribute_UsesBlockScan(t *testing.T) {
	t.Parallel()
	r := buildEPReader(t)

	rows, stats := collectWithPath(t, r, `{ span.http.method = "GET" }`)
	assert.Equal(t, 1, len(rows), "one span has http.method=GET")
	assert.Equal(t, "block-plain", stats.ExecutionPath,
		"user attribute with no intrinsic predicate must use block-plain path")
	assert.Greater(t, stats.FetchedBlocks, 0, "block-scan path must read internal blocks")
}

// EP-05: Result count correctness across all query types.
//
// Regression guard: ensures that path optimizations do not change observable
// query results. Each assertion must hold regardless of which execution path runs.
func TestExecutionPath_Correctness(t *testing.T) {
	t.Parallel()
	r := buildEPReader(t)

	for _, tc := range []struct {
		query string
		want  int
	}{
		{`{ duration > 100ms }`, 2},
		{`{ duration > 10ms }`, 3},
		{`{ duration > 1s }`, 0},
		{`{ status = error }`, 2},
		{`{ kind = server }`, 3},
		{`{ kind = client }`, 2},
		{`{ resource.service.name = "svc-a" }`, 2},
		{`{ resource.service.name = "svc-b" }`, 2},
		{`{ span.http.method = "GET" }`, 1},
		{`{ duration > 100ms && status = error }`, 1},
		{`{ status = error && kind = server }`, 0},
		{`{ status = error || kind = server }`, 5},
		{`{ resource.service.name = "svc-a" && status = error }`, 1},
	} {
		tc := tc
		t.Run(tc.query, func(t *testing.T) {
			rows, _ := collectWithPath(t, r, tc.query)
			assert.Equal(t, tc.want, len(rows), "query: %s", tc.query)
		})
	}
}

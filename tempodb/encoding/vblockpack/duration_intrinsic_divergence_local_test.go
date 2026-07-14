package vblockpack

// duration_intrinsic_divergence_local_test.go — task #201 LOCAL reproduction.
//
// Background: live testing on tempo-dev-test-03 found two structurally identical
// intrinsic-only TraceQL queries ({duration > 0ms} and {duration > 1ms}, both
// zero-attribute-leaf real comparison predicates against the SAME intrinsic column) got
// DIFFERENT HTTP responses in "the same" time window -- one hard-errored (422 coverage-gap),
// one returned 200. A prior code-read investigation found no reachable branch in
// backend_block.go's Fetch that treats these two query shapes differently: both compile to a
// single Min-only RangeNode on "span:duration" (extractRangeNode, internal/vm/traceql_compiler.go
// in the blockpack repo), both are classified isMatchAll=false (IsMatchAllProgram requires ZERO
// leaf nodes -- a real comparison always has exactly one), and both take the identical
// `!isMatchAll && compiledProgram != nil` dispatch branch into tryIndexFetch with the same
// column, differing only in the literal threshold value passed to the range predicate.
//
// RESULT: TestIsMatchAllProgram_DurationComparisons_BothFalse empirically confirms the
// isMatchAll claim (compiles both queries for real, calls the real function -- does not trust
// the prior code-read). TestFetch_DurationComparisons_SameClassification_{NoCoverage,
// FullCoverage} then prove -- through the REAL blockpackBlock.Fetch call chain (real CreateBlock
// write path, real tryIndexFetch/BuildValueIndexSource(Bounded)/QueryTraceQLFromIndex dispatch,
// no hand-constructed QueryOptions/tryIndexFetch calls) -- that under IDENTICAL, FIXED
// (non-sliding, non-NOW-relative) time windows and IDENTICAL known VI coverage state for
// span:duration, the two queries ALWAYS receive the SAME classification. NO divergence was
// found in either state this task could construct: this rules out "the Fetch dispatch code
// itself treats these two threshold values differently" as the live incident's explanation.
//
// BONUS FINDING, NOW FIXED (task #203, CRITICAL) -- not the original task #201 hypothesis,
// discovered while building this repro; TestValueIndexQuery_DurationColumnTypeMismatch_
// AlwaysMasksRealCoverage below is the standalone regression pin for it, updated in place to
// assert the FIXED behavior rather than the bug it originally pinned:
//
// tryIndexFetch's own "no coverage" decline (ErrSearchNoCoverage) can basically NEVER fire for a
// real range/eq/regex leaf on ANY column, regardless of whether that column has ANY value-index
// files at all. vibuilder.BuildSource's `ok` return is a query-SHAPE check only (buildPredicate
// succeeds for any real comparison), and NOTE-VI-033's "Add even when empty" contract (builder.go's
// leaf-resolution loop, unconditional src.Add even when lookupColumn finds ZERO files) makes
// "genuinely no VI files ever written for this column" indistinguishable, at the executor level,
// from "VI files exist and confirm zero matches" -- both collapse to (nil, true, nil) = a silent,
// definitive-looking empty success. This part of the finding remains a known, separate, systemic
// property of the value-index coverage contract and is NOT what task #203 fixed.
//
// What WAS the bug (task #203's actual root cause, now fixed): the real on-disk block column type
// for span:duration (and span:start) is Uint64 (confirmed directly against a real
// CreateBlock+Reader round trip, not assumed), but blockpack/internal/modules/vibuilder/builder.go's
// valueAsColType used to map every TraceQL Duration/Int literal to ColumnTypeInt64 unconditionally,
// with no column-name awareness -- so a `{duration > Xms}` leaf ALWAYS resolved against the wrong
// (Int64) value-index type-bucket directory and ALWAYS found zero files, even when real,
// correctly-typed Uint64 VI files existed for span:duration right next to it. This meant, in
// production, `{duration > Xms}` (and `{start > X}`) answered via the value-index path could NEVER
// return real matches and could NEVER decline with a coverage-gap error either -- it always
// silently succeeded empty, a silent wrong-answer bug, not a decline/timeout.
//
// FIX (task #203): valueAsColType is now column-name-aware (dedicatedNumericColumnTypes,
// blockpack/internal/modules/vibuilder/builder.go) -- it resolves span:duration/span:start to
// ColumnTypeUint64, matching their real on-disk type, while every other column keeps the
// unchanged ColumnTypeInt64 default. TestFetch_DurationComparisons_SameClassification_FullCoverage
// and TestValueIndexQuery_DurationColumnTypeMismatch_AlwaysMasksRealCoverage below were updated in
// place (rather than left pinning the old buggy expectation) to assert the CORRECT real matches
// this fix now returns. Mutation-verified: reverting builder.go's fix reproduces both tests'
// original failure mode (asserting non-empty results against the unfixed code returns empty,
// i.e. the tests fail exactly as they would have caught this bug before it shipped).
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	tempocommon "github.com/grafana/tempo/pkg/tempopb/common/v1"
	temporesource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tempotrace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// TestIsMatchAllProgram_DurationComparisons_BothFalse is the direct, empirical check task #201
// explicitly asked for: do NOT trust the prior investigation's code-read claim that
// IsMatchAllProgram is false for a real duration comparison -- compile both queries for real
// and call the real function.
func TestIsMatchAllProgram_DurationComparisons_BothFalse(t *testing.T) {
	for _, q := range []string{"{ duration > 0ms }", "{ duration > 1ms }"} {
		prog, err := blockpack.CompileTraceQL(q, blockpack.QueryOptions{})
		require.NoError(t, err, "query %q must compile as a plain filter program", q)
		require.NotNil(t, prog.Predicates, "a real comparison leaf must produce non-nil Predicates")
		assert.Len(t, prog.Predicates.Nodes, 1, "query %q must compile to exactly one RangeNode leaf", q)
		assert.False(t, blockpack.IsMatchAllProgram(prog),
			"query %q must NOT classify as match-all -- it has a real comparison leaf, which is the exact"+
				" opposite of IsMatchAllProgram's own contract (zero leaf predicates AND zero listed columns)", q)
	}
}

// fixedDurationWindowStartNano is a hardcoded absolute epoch nanosecond timestamp
// (2025-01-01T00:00:00Z), never derived from time.Now() anywhere in this file -- deliberately
// NOT a sliding/NOW-relative window, unlike the live cluster's two original test invocations
// (NOW-7200 to NOW-3600). Both queries below are issued against the literal same [start, end)
// bound, back to back, in the same test, against the same already-written block: there is no
// way for "the underlying data changed between the two calls" to explain a hypothetical
// divergence here, unlike the live incident.
const fixedDurationWindowStartNano = uint64(1735689600_000_000_000)

// durationFixtureSpan describes one span's duration for writeDurationFixtureBlock, each landing
// in its own trace so a caller can assert per-trace match/no-match independently.
type durationFixtureSpan struct {
	traceIDByte byte
	durationNs  uint64
}

// buildDurationFixtureTraces builds one trace per entry in spans, each with a single span whose
// duration is exactly spans[i].durationNs. All spans share the same fixed StartTime
// (fixedDurationWindowStartNano) -- only EndTime varies -- so every trace lands in a single
// fixed time window with no sliding/NOW-relative component anywhere.
func buildDurationFixtureTraces(spans []durationFixtureSpan) ([]*tempopb.Trace, [][]byte) {
	traces := make([]*tempopb.Trace, len(spans))
	ids := make([][]byte, len(spans))
	for i, s := range spans {
		tid := []byte{s.traceIDByte, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		ids[i] = tid
		traces[i] = &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{{
					Key:   "service.name",
					Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-201"}},
				}}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId:           tid,
					SpanId:            []byte{s.traceIDByte, 0, 0, 0, 0, 0, 0, 1},
					Name:              "op",
					StartTimeUnixNano: fixedDurationWindowStartNano,
					EndTimeUnixNano:   fixedDurationWindowStartNano + s.durationNs,
				}}}},
			}},
		}
	}
	return traces, ids
}

// writeDurationFixtureBlockWithPolicy writes one real block (via the real CreateBlock write
// path, exactly mirroring fetch_bounded_and_integration_test.go's own precedent for "real write
// path, not a hand-built value-index fixture") containing spans, gated by dedicatedColumnsEnabled
// (blockpack valueindex_policy.go's ColumnPolicy R12 safety valve):
//   - false disables the ENTIRE dedicated-column policy layer, indexing every column
//     unconditionally (including span:duration, which is never in
//     viusage.DefaultDedicatedColumns) -- this test file's "known, deliberate FULL coverage"
//     state.
//   - true (the real production default, per common/config.go's applyDefaults) restricts VI
//     writes to viusage.DefaultDedicatedColumns, which -- confirmed directly against
//     blockpack/internal/modules/viusage/dedicated_columns.go -- does NOT include span:duration
//     or any other intrinsic column; with no usage-triggered backfill ever run for this fresh
//     tenant, span:duration is excluded from the standard per-column value-index write path
//     entirely -- this test file's "known, deliberate NO coverage" state.
func writeDurationFixtureBlockWithPolicy(t *testing.T, dir string, spans []durationFixtureSpan, dedicatedColumnsEnabled bool) *backend.BlockMeta {
	t.Helper()
	const tenant = "test-tenant-201"
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	traces, ids := buildDurationFixtureTraces(spans)
	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta(tenant, uuid.New(), VersionString)
	meta.StartTime = time.Unix(0, int64(fixedDurationWindowStartNano)).Add(-time.Minute)
	meta.EndTime = time.Unix(0, int64(fixedDurationWindowStartNano)).Add(time.Hour)

	cfg := &common.BlockConfig{}
	cfg.Blockpack.ViUsage.DedicatedColumnsEnabled = dedicatedColumnsEnabled

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	require.Equal(t, int64(len(spans)), resultMeta.TotalObjects)
	return resultMeta
}

// fetchDuration issues query against block over the FIXED window
// [fixedDurationWindowStartNano-1min, fixedDurationWindowStartNano+1hour) with a real,
// search-shaped limit (MaxTraces: 20, mirroring the live incident's own search request, which
// is what makes boundedAuthorized true and routes tryIndexFetch through
// BuildValueIndexSourceBounded -- the same real early-stopping path the live queries took).
func fetchDuration(t *testing.T, block *blockpackBlock, query string) (traceql.FetchSpansResponse, error) {
	t.Helper()
	ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
	req := traceql.FetchSpansRequest{
		StartTimeUnixNanos: fixedDurationWindowStartNano - uint64(time.Minute),
		EndTimeUnixNanos:   fixedDurationWindowStartNano + uint64(time.Hour),
	}
	opts := common.SearchOptions{MaxTraces: 20}
	return block.Fetch(ctx, req, opts)
}

// classifyFetchErr reduces err to a short, stable tag for cross-query comparison -- the actual
// assertion this whole test makes is "do these two queries classify the same way", not "do they
// return byte-identical error values" (they never would: Fetch wraps each sentinel with
// fmt.Errorf, so errors.Is is required either way; this just makes the comparison symmetric and
// readable in a test failure message).
func classifyFetchErr(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, ErrSliceIndexCoverageGap):
		return "ErrSliceIndexCoverageGap"
	case errors.Is(err, ErrSearchNoCoverage):
		return "ErrSearchNoCoverage"
	case errors.Is(err, ErrMaterializedIndexBuilding):
		return "ErrMaterializedIndexBuilding"
	default:
		return "other:" + err.Error()
	}
}

// durationFixtureSpans is the shared 3-trace fixture both coverage-state tests below use:
// trace 1 has an exact-zero duration (the strict-`>` boundary case), trace 2 has exactly 1ms,
// trace 3 has 5ms -- enough spread to distinguish {duration>0ms} from {duration>1ms} BY RESULT
// (not just by error/no-error) if the index path were actually answering correctly.
var durationFixtureSpans = []durationFixtureSpan{
	{traceIDByte: 1, durationNs: 0},
	{traceIDByte: 2, durationNs: 1_000_000},
	{traceIDByte: 3, durationNs: 5_000_000},
}

// TestFetch_DurationComparisons_SameClassification_NoCoverage is the first of the two required
// controlled repros (task #201 step 1b/1c): under a KNOWN, deliberate NO-coverage state for
// span:duration, issue {duration >= 0ms} and {duration >= 1ms} against the IDENTICAL fixed time
// window, back to back, in this same test, against the same already-written block.
//
// Uses `>=`, not `>` (task #204, CRITICAL, corrects this file's own original assumption below):
// a millisecond-aligned threshold (both 0ms and 1ms are) is only decidable from a
// millisecond-truncated bucket for `>=`/`<` -- `>` is genuinely UNDECIDABLE at that exact
// alignment (blockpack's vibuilder/builder.go:decidableTimeBucketThreshold, SPEC-VB-7) and now
// correctly declines via ErrSearchNoCoverage instead of the silent wrong answer #203's fix
// produced. `>=0ms`/`>=1ms` keep this test's original two-structurally-identical-queries
// classification-parity property while remaining in the decidable operator/alignment pair.
//
// ACTUAL behavior (re-confirmed by running this test and reading its own t.Logf output, not
// predicted from memory -- flagged wrong for 3 consecutive review passes before this fix):
// both queries classify as "success" (err == nil), NOT ErrSearchNoCoverage. `>=0ms`/`>=1ms`
// are both decidable at the millisecond-aligned boundary (task #204's decidability gate never
// declines them), so buildPredicate/BuildSource resolve a real leaf predicate for span:duration
// regardless of whether any VI files exist for that column. Since DedicatedColumnsEnabled=true
// excludes span:duration from the value-index write path entirely (no VI files exist for it in
// this state), BuildSource's own "Add even when empty" contract (NOTE-VI-033) adds a covered,
// zero-match result for the leaf -- the exact "genuinely no VI files ever written" vs. "VI
// files exist and confirm zero matches" indistinguishability this file's own package doc
// comment (top of file, the "BONUS FINDING" paragraph) already documents as a known, separate,
// systemic property of the value-index coverage contract. Both queries therefore succeed with
// an EMPTY result (0 matched traces), never ErrSearchNoCoverage, in this coverage state. The
// two queries' CLASSIFICATION still matches exactly either way -- that is the one property this
// task needed to check, and it still holds. The test's own assertions below (both must classify
// identically; a nil-err response must drain empty) are accurate as written and unchanged by
// this comment fix -- only this comment's description of the observed behavior was wrong.
func TestFetch_DurationComparisons_SameClassification_NoCoverage(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta := writeDurationFixtureBlockWithPolicy(t, dir, durationFixtureSpans, true /* DedicatedColumnsEnabled: span:duration excluded */)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	respGt0, errGt0 := fetchDuration(t, block, "{ duration >= 0ms }")
	respGt1, errGt1 := fetchDuration(t, block, "{ duration >= 1ms }")

	classGt0 := classifyFetchErr(errGt0)
	classGt1 := classifyFetchErr(errGt1)
	t.Logf("no-coverage state: {duration>=0ms} classified as %q, {duration>=1ms} classified as %q", classGt0, classGt1)

	assert.Equal(t, classGt0, classGt1,
		"BUG WOULD BE CONFIRMED if this fails: two structurally identical intrinsic-only comparison"+
			" queries against the SAME column, SAME fixed window, SAME coverage state diverged in"+
			" classification -- exactly the live symptom task #201 investigated, now reproduced without"+
			" live-cluster timing")

	// Both queries decline (err != nil), so resp is the zero value -- only drain a response when
	// its own query actually succeeded (task #204: a nil/zero FetchSpansResponse cannot be
	// drained, unlike the pre-#204 assumption that a real comparison leaf always "succeeds empty").
	if errGt0 == nil {
		assert.Empty(t, drainTraceIDBytes(t, respGt0))
	}
	if errGt1 == nil {
		assert.Empty(t, drainTraceIDBytes(t, respGt1))
	}
}

// TestFetch_DurationComparisons_SameClassification_FullCoverage is the second required
// controlled repro: under a KNOWN, deliberate FULL-coverage state for span:duration, issue the
// same two queries against the same fixed window.
//
// Uses `>=`, not `>` (task #204 -- see TestFetch_DurationComparisons_SameClassification_
// NoCoverage's own doc comment for why `>` at a millisecond-aligned threshold is genuinely
// undecidable and must decline, not answer with reduced precision).
//
// ACTUAL behavior: both queries again succeed (indexAnswered=true for both, no hard error for
// either) -- confirming "same classification" once more, this time in the OTHER coverage state.
// Real VI files exist here for span:duration (confirmed via
// TestValueIndexQuery_DurationColumnTypeMismatch_AlwaysMasksRealCoverage's direct sink
// inspection) with real matching entries, and -- since task #203's type/unit fix plus task
// #204's decidability fix -- both queries now correctly return their true matches (all 3 for
// `>=0ms`, 2 for `>=1ms`) instead of the old type-bucket-mismatch bug's silent empty result
// documented in this file's package doc comment.
func TestFetch_DurationComparisons_SameClassification_FullCoverage(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta := writeDurationFixtureBlockWithPolicy(t, dir, durationFixtureSpans, false /* DedicatedColumnsEnabled=false: everything indexed */)

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	respGt0, errGt0 := fetchDuration(t, block, "{ duration >= 0ms }")
	respGt1, errGt1 := fetchDuration(t, block, "{ duration >= 1ms }")

	classGt0 := classifyFetchErr(errGt0)
	classGt1 := classifyFetchErr(errGt1)
	t.Logf("full-coverage state: {duration>=0ms} classified as %q, {duration>=1ms} classified as %q", classGt0, classGt1)

	require.NoError(t, errGt0)
	require.NoError(t, errGt1)
	assert.Equal(t, classGt0, classGt1,
		"BUG WOULD BE CONFIRMED if this fails: two structurally identical intrinsic-only comparison"+
			" queries against the SAME column, SAME fixed window, SAME coverage state diverged in"+
			" classification")

	// Task #203/#204 fix verification: real matches now come back correctly instead of the old
	// type-bucket-mismatch bug's silent empty result (see package doc comment). Trace 1 has
	// duration 0ms, trace 2 has 1ms, trace 3 has 5ms (durationFixtureSpans) -- `>=0ms` matches
	// all three traces; `>=1ms` matches traces 2 and 3.
	assert.ElementsMatch(t, []byte{1, 2, 3}, drainTraceIDBytes(t, respGt0), "fixed: span:duration's real Uint64 VI coverage must now be found")
	assert.ElementsMatch(t, []byte{2, 3}, drainTraceIDBytes(t, respGt1), "fixed: span:duration's real Uint64 VI coverage must now be found")
}

// TestValueIndexQuery_DurationColumnTypeMismatch_AlwaysMasksRealCoverage isolates and pins the
// bonus bug this investigation surfaced as a side effect of building the task #201 repro above,
// NOW FIXED by task #203: a real, correctly-written VI file for span:duration used to be
// completely invisible to a real {duration > Xms} filter query, unconditionally, because of a
// value-index type-bucket mismatch between the write side (real on-disk block column type,
// Uint64) and the read side (vibuilder.valueAsColType's TraceQL-literal-to-ColumnType mapping,
// which used to be unconditionally Int64 for any Duration/Int literal, with no column-name
// awareness). This is independent of task #201's original divergence question -- both
// {duration>0ms} and {duration>1ms} were equally, silently wrong before the fix -- but was a
// genuine production correctness bug (silently empty results, never an error, regardless of real
// data) this task's own repro construction accidentally uncovered, and which
// blockpack/internal/modules/vibuilder/builder.go's dedicatedNumericColumnTypes-based fix now
// resolves: the query below now finds its own column's real Uint64 file and returns its true 2
// matches instead of a silent 0.
func TestValueIndexQuery_DurationColumnTypeMismatch_AlwaysMasksRealCoverage(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	// Three real matches: traces 1 (0ms), 2 (1ms), 3 (5ms) all genuinely satisfy
	// `duration >= 0ms` (task #204: `>` is undecidable at this millisecond-aligned threshold).
	meta := writeDurationFixtureBlockWithPolicy(t, dir, durationFixtureSpans, false)

	// Confirm the real VI write: span:duration's file was written under the "uint64" type
	// bucket, not "int64" -- read directly off the fake sink's own object keys, not asserted
	// from memory.
	durationColHash := "9dccc3850476f25115f4f20e0b0be693" // valueindex.ColHash("span:duration"), computed once and pinned here
	foundUint64 := false
	foundInt64 := false
	for key := range viStore.objs {
		switch {
		case strings.Contains(key, durationColHash+"/uint64/"):
			foundUint64 = true
		case strings.Contains(key, durationColHash+"/int64/"):
			foundInt64 = true
		}
	}
	require.True(t, foundUint64, "span:duration's real on-disk column type is Uint64 -- WriteValueIndexL0 must write it under the uint64 type bucket")
	require.False(t, foundInt64, "no int64-bucket file exists for span:duration -- confirms the read side's Int64 lookup can never find it")

	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(meta, backend.NewReader(rawR))

	// `>=`, not `>` (task #204): `duration > 0ms` is exactly the undecidable
	// millisecond-aligned-`>` shape SPEC-VB-7 declines -- see
	// TestFetch_DurationComparisons_SameClassification_NoCoverage's doc comment.
	resp, err := fetchDuration(t, block, "{ duration >= 0ms }")
	require.NoError(t, err, "the query succeeds and now returns real coverage (see package doc comment)")

	matched := drainTraceIDBytes(t, resp)
	assert.ElementsMatch(t, []byte{1, 2, 3}, matched,
		"FIXED (task #203/#204): real VI coverage exists for span:duration (all 3 traces genuinely"+
			" satisfy duration>=0ms) -- valueAsColType resolves span:duration's real Uint64 type-bucket"+
			" instead of the wrong Int64 one, and the millisecond-aligned `>=` comparison is decidable,"+
			" so the query finds its own column's real file and returns the true matches instead of"+
			" silently returning zero")
}

// spanStartFixtureSpan describes one span's StartTime offset (in raw nanoseconds, relative to
// fixedDurationWindowStartNano) for buildSpanStartFixtureTraces -- the span:start analog of
// durationFixtureSpan, needed because durationFixtureSpan/buildDurationFixtureTraces
// deliberately hold StartTime IDENTICAL across every trace (only EndTime/duration varies) and
// so cannot exercise a span:start comparison at all.
type spanStartFixtureSpan struct {
	traceIDByte byte
	startOffset uint64
}

// buildSpanStartFixtureTraces mirrors buildDurationFixtureTraces but varies StartTime (not
// EndTime) per span, each landing at fixedDurationWindowStartNano+spans[i].startOffset, with a
// fixed 1ms duration (arbitrary -- span:start is this fixture's subject, not span:duration).
func buildSpanStartFixtureTraces(spans []spanStartFixtureSpan) ([]*tempopb.Trace, [][]byte) {
	traces := make([]*tempopb.Trace, len(spans))
	ids := make([][]byte, len(spans))
	for i, s := range spans {
		tid := []byte{s.traceIDByte, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		ids[i] = tid
		start := fixedDurationWindowStartNano + s.startOffset
		traces[i] = &tempopb.Trace{
			ResourceSpans: []*tempotrace.ResourceSpans{{
				Resource: &temporesource.Resource{Attributes: []*tempocommon.KeyValue{{
					Key:   "service.name",
					Value: &tempocommon.AnyValue{Value: &tempocommon.AnyValue_StringValue{StringValue: "svc-203-start"}},
				}}},
				ScopeSpans: []*tempotrace.ScopeSpans{{Spans: []*tempotrace.Span{{
					TraceId:           tid,
					SpanId:            []byte{s.traceIDByte, 0, 0, 0, 0, 0, 0, 1},
					Name:              "op",
					StartTimeUnixNano: start,
					EndTimeUnixNano:   start + 1_000_000, // fixed 1ms duration, irrelevant to this fixture
				}}}},
			}},
		}
	}
	return traces, ids
}

// TestFetch_SpanStartComparison_FindsRealUint64MillisecondTruncatedCoverage is task #203's
// real-write-path regression test for span:start -- the SECOND column, alongside
// span:duration, this task's audit found affected by BOTH stacked bugs (type-bucket mismatch
// AND millisecond-truncation unit mismatch; see dedicatedNumericColumnTypes' doc comment in
// blockpack/internal/modules/vibuilder/builder.go). Unlike
// TestValueIndexQuery_DurationColumnTypeMismatch_AlwaysMasksRealCoverage (which reuses the
// duration fixture's identical-StartTime traces), this uses buildSpanStartFixtureTraces so
// StartTime itself varies per trace and a `{ start >= X }` comparison has something real to
// select against.
//
// Uses `>=`, not `>` (task #204): thresholdNs below is millisecond-aligned, exactly the shape
// `>` cannot decide from a millisecond-truncated bucket (SPEC-VB-7) -- see
// TestFetch_DurationComparisons_SameClassification_NoCoverage's doc comment for the full
// derivation. The fixture's two spans (500ms and 1500ms after base, straddling the 1000ms
// threshold comfortably on either side) select the identical single match under `>=` as they did
// under `>`, so only the operator token changes here, not the expected result.
func TestFetch_SpanStartComparison_FindsRealUint64MillisecondTruncatedCoverage(t *testing.T) {
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	const tenant = "test-tenant-203-start"
	dir := t.TempDir()
	rawR, rawW, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)

	spans := []spanStartFixtureSpan{
		{traceIDByte: 10, startOffset: 500_000_000},   // 500ms after the fixed window base
		{traceIDByte: 11, startOffset: 1_500_000_000}, // 1500ms after the fixed window base
	}
	traces, ids := buildSpanStartFixtureTraces(spans)
	iter := &mockIterator{traces: traces, ids: ids}
	meta := backend.NewBlockMeta(tenant, uuid.New(), VersionString)
	meta.StartTime = time.Unix(0, int64(fixedDurationWindowStartNano)).Add(-time.Minute)
	meta.EndTime = time.Unix(0, int64(fixedDurationWindowStartNano)).Add(time.Hour)

	cfg := &common.BlockConfig{}
	cfg.Blockpack.ViUsage.DedicatedColumnsEnabled = false // span:start is never in DefaultDedicatedColumns either

	resultMeta, err := CreateBlock(context.Background(), cfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
	require.Equal(t, int64(len(spans)), resultMeta.TotalObjects)

	// Confirm the real VI write landed under the uint64 bucket for span:start too, exactly
	// like span:duration's own sink-inspection assertion above.
	startColHash := "85851d77265e31ba7536ec568f5607e5" // valueindex.ColHash("span:start"), computed once and pinned here
	foundUint64 := false
	for key := range viStore.objs {
		if strings.Contains(key, startColHash+"/uint64/") {
			foundUint64 = true
		}
	}
	require.True(t, foundUint64, "span:start's real on-disk column type is Uint64 -- WriteValueIndexL0 must write it under the uint64 type bucket")

	rawR2, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(resultMeta, backend.NewReader(rawR2))

	thresholdNs := fixedDurationWindowStartNano + 1_000_000_000 // 1000ms after base, in raw nanoseconds
	ctx := common.WithOriginalTraceQLQuery(context.Background(), fmt.Sprintf("{ start >= %d }", thresholdNs), false)
	req := traceql.FetchSpansRequest{
		StartTimeUnixNanos: fixedDurationWindowStartNano - uint64(time.Minute),
		EndTimeUnixNanos:   fixedDurationWindowStartNano + uint64(time.Hour),
	}
	resp, err := block.Fetch(ctx, req, common.SearchOptions{MaxTraces: 20})
	require.NoError(t, err)

	matched := drainTraceIDBytes(t, resp)
	assert.ElementsMatch(t, []byte{11}, matched,
		"FIXED (task #203/#204): span:start's real Uint64, millisecond-truncated VI coverage must"+
			" now be found -- only the trace starting 1500ms after base is >= the 1000ms threshold")
}

// drainTraceIDBytes extracts each spanset's first trace-ID byte (this fixture's traces are
// distinguished solely by that leading byte, see durationFixtureSpan.traceIDByte) from resp, via
// this package's own drainSpansets helper (structural_dispatch_test.go).
func drainTraceIDBytes(t *testing.T, resp traceql.FetchSpansResponse) []byte {
	t.Helper()
	spansets := drainSpansets(context.Background(), t, resp)
	var out []byte
	for _, ss := range spansets {
		require.NotEmpty(t, ss.TraceID)
		out = append(out, ss.TraceID[0])
	}
	return out
}

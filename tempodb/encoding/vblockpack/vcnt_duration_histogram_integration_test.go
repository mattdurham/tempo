package vblockpack

// vcnt_duration_histogram_integration_test.go — issue #205 Phase D1: real
// write -> compact -> read -> classify pipeline test for the span:duration VCNT
// histogram. Every step drives real production code:
//
//   - write:    vcntAccumulator.addTrace + flush (vcntwriter.go, Phase B) — the
//               exact code the block-builder calls per real trace.
//   - compact:  blockpack.VCNTBuildSectionFromObjects, which internally decodes
//               each raw .vcnt object via valuecounts.DecodeVCNTObject and merges
//               via valuecounts.Compact (the SAME compactor
//               TestVCNTFlush_DurationHistogram_CrossBlockMinuteCoalescing already
//               proves needs no changes for histogram rows) — this is also the
//               exact call fetchVCNTSection (modules/frontend/vcnt_fetch.go) makes
//               against real S3 objects, so this test's "compact" step is not a
//               reimplementation, it is the real production merge path.
//   - read:     the resulting (data, dir) is the same in-memory decoded-section
//               shape ClassifyProgramVCNTWithDetail consumes directly, with zero
//               extra decode step invented for this test.
//   - classify: blockpack.ClassifyProgramVCNTWithDetail + blockpack.
//               SelectSearchStrategy (Phase C), against a real compiled
//               *blockpack.Program from blockpack.CompileTraceQL, never a
//               hand-built Selectivity value.
//
// This proves the #205 plan's whole point end to end: a low-selectivity,
// no-limit duration predicate now classifies LowSelectivity and
// SelectSearchStrategy's planTimeDecline signal fires — the actual mechanism
// buildQueryPlanFromProgram's existing, unmodified decline gate
// (vcnt_fetch.go:299-324) uses to skip dispatching any block job at all (see
// vcnt_fetch_test.go's sibling Phase D2 tests for the job-count-level proof of
// that specific gate).

import (
	"testing"

	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
)

// buildDurationSpans returns n synthetic durationSpan fixtures, all starting at the same
// startNano (so every span lands in the identical minute bucket, isolating the test to the
// duration-bucket axis only), each carrying the given duration in nanoseconds.
func buildDurationSpans(n int, startNano, durNanos uint64, namePrefix string) []durationSpan {
	spans := make([]durationSpan, n)
	for i := range spans {
		spans[i] = durationSpan{startNano: startNano, endNano: startNano + durNanos, name: namePrefix}
	}
	return spans
}

// flushDurationHistogramSpans runs one real accumulator+flush cycle (the exact per-block-builder
// pass vcntwriter.go's addTrace/flush implement) over spans, writing real encoded .vcnt objects
// into store.
func flushDurationHistogramSpans(store *fakeVCNTStore, tenant string, spans ...durationSpan) {
	acc := newVCNTAccumulator()
	acc.addTrace(traceWithDurationSpansAt(spans...))
	acc.flush(store, tenant)
}

// rawObjectsForColumn returns the raw (still-encoded) .vcnt object bytes store.Put received for
// colName, keyed by colHash exactly like the real object-storage key layout — the same shape
// fetchVCNTSection's own S3 Find+Read fan-out hands to blockpack.VCNTBuildSectionFromObjects in
// production. Unlike recordsForColumn (which decodes for direct record inspection), this stays
// at the raw-bytes layer so VCNTBuildSectionFromObjects itself performs the real decode+compact.
func rawObjectsForColumn(store *fakeVCNTStore, colName string) [][]byte {
	store.mu.Lock()
	defer store.mu.Unlock()
	colHash := blockpack.VCNTColHash(colName)
	var out [][]byte
	for key, data := range store.objs {
		if !stringsContains(key, colHash) {
			continue
		}
		out = append(out, data)
	}
	return out
}

// classifyDurationQuery compiles query via the real TraceQL compiler, builds a real merged VCNT
// section from store's raw objects for "span:duration"'s histogram column (real decode+compact,
// see VCNTBuildSectionFromObjects), and returns the real classification and decline signal.
func classifyDurationQuery(
	t *testing.T, store *fakeVCNTStore, query string, minTS, maxTS uint64,
) (blockpack.Selectivity, bool) {
	t.Helper()

	histCol := blockpack.VCNTDurationHistogramColumnName("span:duration")
	objs := rawObjectsForColumn(store, histCol)
	require.NotEmpty(t, objs, "expected real flushed .vcnt objects for the duration histogram column")

	data, dir, skipped := blockpack.VCNTBuildSectionFromObjects(objs)
	require.Zero(t, skipped, "no flushed object should be undecodable")

	prog, err := blockpack.CompileTraceQL(query, blockpack.QueryOptions{})
	require.NoError(t, err)
	require.NotNil(t, prog)

	sel, _ := blockpack.ClassifyProgramVCNTWithDetail(prog, data, dir, minTS, maxTS)
	_, planTimeDecline := blockpack.SelectSearchStrategy(sel, false /* hasLimit */)
	return sel, planTimeDecline
}

// TestVCNTDurationHistogramPipeline_LowSelectivity_DeclinesAtPlanTime is #205 Phase D1
// assertions 1/2: 1000 real spans, 900 with duration 2ms (> 1ms, landing in bucket index 1 —
// boundary 1ms <= 2ms < 5ms) and 100 with duration 0ms (<= 1ms, landing in bucket index 0) — a
// deliberately clean, non-straddling split at the query's own threshold (1ms is exactly boundary
// index 1). Split across two independent accumulator+flush passes (mirroring two separate
// block-builder flushes) so the real cross-block compaction path is genuinely exercised, not
// just a single-flush no-op merge.
func TestVCNTDurationHistogramPipeline_LowSelectivity_DeclinesAtPlanTime(t *testing.T) {
	store := newFakeVCNTStore()
	const startNano = 65 * 1_000_000_000 // bucket 60
	const twoMs = 2 * 1_000_000
	const zeroMs = 0

	flushDurationHistogramSpans(store, "tenant1",
		append(buildDurationSpans(500, startNano, twoMs, "above"),
			buildDurationSpans(50, startNano, zeroMs, "below")...)...)
	flushDurationHistogramSpans(store, "tenant1",
		append(buildDurationSpans(400, startNano, twoMs, "above"),
			buildDurationSpans(50, startNano, zeroMs, "below")...)...)

	sel, planTimeDecline := classifyDurationQuery(t, store, `{ duration > 1ms }`, 0, 200)
	require.Equal(t, blockpack.LowSelectivity, sel,
		"900/1000 spans (90%%) exceed the 1ms threshold -- must classify LowSelectivity at the default 0.5 fraction")
	require.True(t, planTimeDecline,
		"LowSelectivity with no limit must set SelectSearchStrategy's planTimeDecline signal -- "+
			"the actual I/O-avoiding signal buildQueryPlanFromProgram's existing decline gate consumes")
}

// TestVCNTDurationHistogramPipeline_Selective_NoDecline is #205 Phase D1 assertion 3: a sibling
// fixture where only 10/1000 spans (1%) exceed the 1ms threshold must classify Selective and
// must NOT set planTimeDecline -- proving the feature discriminates real selectivity rather than
// unconditionally declining (a trivial, useless implementation would still pass the LowSelectivity
// fixture alone).
func TestVCNTDurationHistogramPipeline_Selective_NoDecline(t *testing.T) {
	store := newFakeVCNTStore()
	const startNano = 65 * 1_000_000_000 // bucket 60
	const twoMs = 2 * 1_000_000
	const zeroMs = 0

	flushDurationHistogramSpans(store, "tenant1",
		append(buildDurationSpans(5, startNano, twoMs, "above"),
			buildDurationSpans(495, startNano, zeroMs, "below")...)...)
	flushDurationHistogramSpans(store, "tenant1",
		append(buildDurationSpans(5, startNano, twoMs, "above"),
			buildDurationSpans(495, startNano, zeroMs, "below")...)...)

	sel, planTimeDecline := classifyDurationQuery(t, store, `{ duration > 1ms }`, 0, 200)
	require.Equal(t, blockpack.Selective, sel,
		"10/1000 spans (1%%) exceed the 1ms threshold -- must classify Selective, well under the default 0.5 fraction")
	require.False(t, planTimeDecline,
		"Selective must never set planTimeDecline -- proves the classifier isn't just always declining")
}

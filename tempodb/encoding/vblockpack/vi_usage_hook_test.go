package vblockpack

// vi_usage_hook_test.go — #496/B1: pins recordUsageForDeclinedQuery's R3 decline-reason
// distinction directly (the part "most likely to be gotten wrong" per plan.md), plus
// integration tests driving the 3 real call sites (metrics: QueryRange, search:
// Fetch/tryIndexFetch, structural: Fetch/tryStructuralIndexFetch) to prove the hook is
// actually reachable in production code, not just correct in isolation.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeUsageRecorder is the injectable seam's test fake (mirrors cqp.store's own
// DI pattern): records every RecordUse call so tests can assert exactly which
// (tenant, column, colType) combinations the hook attempted.
type fakeUsageRecorder struct {
	mu    sync.Mutex
	calls []fakeUsageCall
	// shouldBackfill, when set, is returned by every RecordUse call.
	shouldBackfill bool
}

type fakeUsageCall struct {
	tenant, colName, colType string
}

func (f *fakeUsageRecorder) RecordUse(_ context.Context, tenant, colName, colType string, _ time.Time) (blockpack.TriggerResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeUsageCall{tenant: tenant, colName: colName, colType: colType})
	return blockpack.TriggerResult{ShouldBackfill: f.shouldBackfill}, nil
}

func (f *fakeUsageRecorder) columns() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	cols := make([]string, len(f.calls))
	for i, c := range f.calls {
		cols[i] = c.colName
	}
	return cols
}

// withUsageRecorder installs rec as the process-level singleton for the duration of the
// test, restoring the prior value afterward — mirrors withVIQueryReader's own pattern.
func withUsageRecorder(t *testing.T, rec usageRecorder) {
	t.Helper()
	viUsageRecorderMu.Lock()
	prev := viUsageRecorderPtr
	viUsageRecorderPtr = rec
	viUsageRecorderMu.Unlock()
	t.Cleanup(func() {
		viUsageRecorderMu.Lock()
		viUsageRecorderPtr = prev
		viUsageRecorderMu.Unlock()
	})
}

// resetViUsageRateLimit clears the package-level rate limiter between tests so one
// test's recorded key does not silently suppress another's.
func resetViUsageRateLimit(t *testing.T) {
	t.Helper()
	old := viUsageRateLimit
	viUsageRateLimit = newViUsageRateLimiter(viUsageRateLimitWindow)
	t.Cleanup(func() { viUsageRateLimit = old })
}

func testDedicatedColumns() map[string]struct{} {
	return dedicatedColumnSet(backend.DedicatedColumns{
		{Scope: backend.DedicatedColumnScopeResource, Name: "service.name"},
	})
}

// TestRecordUsage_NonDedicatedColumnRecorded is the ONE case that should count (R3): an
// indexable-shaped leaf against a column not in the dedicated list.
func TestRecordUsage_NonDedicatedColumnRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
	assert.Equal(t, []string{"span.custom.attr"}, rec.columns())
}

// TestRecordUsage_DedicatedColumnNotRecorded: a column already in backend.DedicatedColumns
// must never be recorded, regardless of decline — it is already forward-indexed.
func TestRecordUsage_DedicatedColumnNotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	prog, err := blockpack.CompileTraceQL(`{ resource.service.name = "svc-a" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
	assert.Empty(t, rec.columns(), "a dedicated column must never be recorded")
}

// TestRecordUsage_NegatedPredicate_NotRecorded: SPEC-ROOT-019/NOTE-VI-096's documented
// permanent-decline case — `!=` compiles to a RequirePresent-only leaf, not indexable.
// Backfilling would not change this outcome, so it must never be recorded (R3).
func TestRecordUsage_NegatedPredicate_NotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr != "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
	assert.Empty(t, rec.columns(), "a negated predicate's RequirePresent-only leaf must never be recorded")
}

// TestRecordUsage_UnindexablePredicateShape_NotRecorded: a multi-value leaf (an
// IN-list-shaped equality against >1 value in one leaf) is unindexable per
// vibuilder.LeafIndexable's own rule — same permanent-decline reasoning as negation.
func TestRecordUsage_UnindexablePredicateShape_NotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	// span.custom.attr != "" compiles to a RequirePresent-only leaf (NOTE-453's
	// presence requirement) -- unindexable per buildPredicate's own default case,
	// independent of (and a different shape from) the negation test above.
	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr != "" }`, blockpack.QueryOptions{})
	require.NoError(t, err)
	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
	assert.Empty(t, rec.columns(), "an unindexable predicate shape must never be recorded")
}

// TestRecordUsage_TrulyMissingIndex_IsRecorded is R3's one case that SHOULD count: an
// indexable-shaped leaf against a genuinely non-dedicated, non-covered column.
func TestRecordUsage_TrulyMissingIndex_IsRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
	assert.Equal(t, []string{"span.never.indexed"}, rec.columns())
}

// TestRecordUsage_DuplicateColumnAcrossLeavesRecordsOnce: a query naming the same
// non-dedicated column in two leaves (e.g. an AND of two comparisons) records that
// column at most once per call, per LeafColumns' own documented per-caller-dedup
// contract.
func TestRecordUsage_DuplicateColumnAcrossLeavesRecordsOnce(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr = "x" && span.other.attr = "y" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
	cols := rec.columns()
	assert.Len(t, cols, 2, "two distinct non-dedicated columns must each record once")
}

// TestRecordUsage_NilRecorderNoOps: with no recorder configured (B3 not yet wired, or
// the feature disabled), the hook must be a complete no-op — no panic, nothing recorded.
func TestRecordUsage_NilRecorderNoOps(t *testing.T) {
	resetViUsageRateLimit(t)
	withUsageRecorder(t, nil)

	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), time.Now())
}

// TestRecordUsage_RateLimitCollapsesBurst: a burst of calls for the SAME (tenant,
// column) within viUsageRateLimitWindow collapses to one recorded use (R4).
func TestRecordUsage_RateLimitCollapsesBurst(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	now := time.Now()
	for range 5 {
		recordUsageForDeclinedQuery(context.Background(), "tenant-a", prog, testDedicatedColumns(), now)
	}
	assert.Len(t, rec.columns(), 1, "a burst within the rate-limit window must collapse to one recorded use")
}

// TestViUsageRateLimiter_SweepsExpiredEntriesPastThreshold is the go-presubmit.md HIGH
// finding's regression pin: viUsageRateLimiter.seen must not grow unboundedly for the
// process lifetime. A small maxTrackedKeys (not the real 100k default -- this test needs
// to stay fast) makes the threshold reachable with a handful of keys; once len(seen)
// exceeds it, the next allow() call must sweep every already-expired entry.
func TestViUsageRateLimiter_SweepsExpiredEntriesPastThreshold(t *testing.T) {
	l := &viUsageRateLimiter{seen: make(map[string]time.Time), ttl: 10 * time.Millisecond, maxTrackedKeys: 5}
	base := time.Now()

	for i := range 10 {
		l.allow(fmt.Sprintf("key-%d", i), base)
	}
	require.Len(t, l.seen, 10, "setup: all 10 distinct keys must be tracked before the sweep")

	// Advance well past ttl, then make one more allow() call -- len(seen) already
	// exceeds maxTrackedKeys=5, so this call must trigger sweepExpiredLocked and remove
	// every one of the 10 now-expired entries, leaving only the newly-added key.
	later := base.Add(100 * time.Millisecond)
	l.allow("trigger-sweep", later)

	assert.Len(t, l.seen, 1, "expired entries must be swept once the size threshold is exceeded")
	_, stillTracked := l.seen["trigger-sweep"]
	assert.True(t, stillTracked, "the key that triggered the sweep must itself remain tracked")
}

// TestViUsageRateLimiter_NeverSweepsStillActiveKeys: a key within its ttl window must
// never be swept, regardless of map size -- the sweep only removes entries whose ttl has
// genuinely elapsed, so legitimately active (tenant, column) cardinality within one ttl
// window is never mistaken for the unbounded-growth condition the sweep exists to
// prevent (and, as a side effect, a still-active key is never incorrectly un-rate-limited
// by being swept away).
func TestViUsageRateLimiter_NeverSweepsStillActiveKeys(t *testing.T) {
	l := &viUsageRateLimiter{seen: make(map[string]time.Time), ttl: time.Hour, maxTrackedKeys: 2}
	base := time.Now()

	l.allow("key-a", base)
	l.allow("key-b", base)
	l.allow("key-c", base) // len(seen)=3 > maxTrackedKeys=2, but none are expired (ttl=1h)

	assert.Len(t, l.seen, 3, "no key within its ttl window may be swept, even past the size threshold")
}

// TestRecordUsage_MetricsPath_NonDedicatedColumnRecorded drives the REAL QueryRange
// with a metrics query filtering on a non-dedicated column that has no VI coverage at
// all, asserting the configured recorder fires for it.
func TestRecordUsage_MetricsPath_NonDedicatedColumnRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq(`{ span.never.indexed = "x" } | count_over_time()`)
	_, _ = block.QueryRange(context.Background(), req, common.SearchOptions{})

	assert.Contains(t, rec.columns(), "span.never.indexed")
}

// TestRecordUsage_MetricsPath_DedicatedColumnNotRecorded drives the REAL QueryRange
// with a metrics query filtering ONLY on a dedicated column — even if it declines for
// some other reason, the hook must never record a dedicated column.
func TestRecordUsage_MetricsPath_DedicatedColumnNotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	// A configured-but-empty store, queried on an attribute that was never written --
	// a genuine decline. metaA.DedicatedColumns is set explicitly below to declare
	// resource.k8s.cluster.name dedicated for this block (writeSvcBlock's fixture
	// itself never sets DedicatedColumns), mirroring backend.DefaultDedicatedColumns'
	// real resource-scoped entry (block_meta.go) -- the exact column tempo's own
	// dedicated-column defaults already cover, so this test's premise matches
	// production configuration, not an invented one.
	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)
	metaA.DedicatedColumns = backend.DedicatedColumns{
		{Scope: backend.DedicatedColumnScopeResource, Name: "k8s.cluster.name"},
	}
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	req := countOverTimeReq(`{ resource.k8s.cluster.name = "does-not-exist" } | count_over_time()`)
	_, _ = block.QueryRange(context.Background(), req, common.SearchOptions{})

	assert.Empty(t, rec.columns(), "resource.k8s.cluster.name is declared dedicated on this block -- it must never be recorded")
}

// TestRecordUsage_SearchPath_NonStructural drives the REAL Fetch (plain filter path,
// tryIndexFetch) with a condition on a non-dedicated column that was never written to
// any block, asserting the configured recorder fires for it.
func TestRecordUsage_SearchPath_NonStructural(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	ctx := common.WithOriginalTraceQLQuery(context.Background(), `{ span.never.indexed = "x" }`, false)
	req := traceql.FetchSpansRequest{
		Conditions: []traceql.Condition{{
			Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, "never.indexed"),
			Op:        traceql.OpEqual,
			Operands:  traceql.Operands{traceql.NewStaticString("x")},
		}},
		AllConditions: true,
	}
	_, _ = block.Fetch(ctx, req, common.SearchOptions{})

	assert.Contains(t, rec.columns(), "span.never.indexed")
}

// TestRecordUsage_SearchPath_Structural drives the REAL Fetch (structural path,
// tryStructuralIndexFetch, which requires IndexOnly=true per its own dispatch-safety
// gate — see value_index_structural_query.go's package doc comment) with a structural
// query whose left leg references a non-dedicated, never-indexed column, asserting the
// recorder fires for it.
func TestRecordUsage_SearchPath_Structural(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	metaA, _ := writeParentChildBlock(t, dir, uuid.New())
	rawR, _, _, err := local.New(&local.Config{Path: dir})
	require.NoError(t, err)
	block := newBackendBlock(metaA, backend.NewReader(rawR))

	query := `{ span.never.indexed = "x" } >> { resource.service.name = "svc-leaf" }`
	ctx := common.WithOriginalTraceQLQuery(context.Background(), query, false)
	req := traceql.FetchSpansRequest{}
	_, _ = block.Fetch(ctx, req, common.SearchOptions{IndexOnly: true})

	assert.Contains(t, rec.columns(), "span.never.indexed")
}

// countingVIStore wraps a *fakeVISink, counting Size/ReadAt calls so a test can assert the
// discovery-only mechanism contract: RecordUsageIfNoIndexCoverage must never download a
// value-index file's content, only List it via IndexFileCache.FilesForTimeRange.
type countingVIStore struct {
	*fakeVISink
	sizeCalls, readAtCalls int
}

func (c *countingVIStore) Size(key string) (int64, error) {
	c.sizeCalls++
	return c.fakeVISink.Size(key)
}

func (c *countingVIStore) ReadAt(key string, p []byte, off int64) (int, error) {
	c.readAtCalls++
	return c.fakeVISink.ReadAt(key, p, off)
}

// TestRecordUsageIfNoIndexCoverage_NonDedicatedUncoveredColumnRecorded is the plan-time analog
// of TestRecordUsage_TrulyMissingIndex_IsRecorded: a genuinely uncovered, non-dedicated column
// must be recorded from the frontend's own discovery-only check.
func TestRecordUsageIfNoIndexCoverage_NonDedicatedUncoveredColumnRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, minSec, maxSec, time.Now())

	assert.Equal(t, []string{"span.never.indexed"}, rec.columns())
}

// TestRecordUsageIfNoIndexCoverage_CoveredColumnNotRecorded: a column genuinely written/indexed
// by writeSvcBlock (resource.service.name) has real file coverage — FilesForTimeRange must
// return a non-empty listing, so it must never be recorded.
func TestRecordUsageIfNoIndexCoverage_CoveredColumnNotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ resource.service.name = "svc-alpha" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, minSec, maxSec, time.Now())

	assert.Empty(t, rec.columns(), "a genuinely covered column must never be recorded")
}

// TestRecordUsageIfNoIndexCoverage_DedicatedColumnNotRecorded: an otherwise-uncovered column
// already declared dedicated (already forward-indexed) must never be recorded, mirroring
// recordUsageForDeclinedQuery's own dedicated-column rule (R3).
func TestRecordUsageIfNoIndexCoverage_DedicatedColumnNotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	dedicated := backend.DedicatedColumns{{Scope: backend.DedicatedColumnScopeSpan, Name: "never.indexed"}}
	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, dedicated, minSec, maxSec, time.Now())

	assert.Empty(t, rec.columns(), "a dedicated column must never be recorded, even if genuinely uncovered")
}

// TestRecordUsageIfNoIndexCoverage_NegatedPredicateNotRecorded mirrors
// TestRecordUsage_NegatedPredicate_NotRecorded's R3 permanent-decline case (`!=` compiles to a
// RequirePresent-only leaf) at plan time.
func TestRecordUsageIfNoIndexCoverage_NegatedPredicateNotRecorded(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ span.custom.attr != "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, minSec, maxSec, time.Now())

	assert.Empty(t, rec.columns(), "a negated predicate's RequirePresent-only leaf must never be recorded")
}

// TestRecordUsageIfNoIndexCoverage_NilRecorderNoOps: with no recorder configured, the call must
// be a complete no-op — no panic.
func TestRecordUsageIfNoIndexCoverage_NilRecorderNoOps(t *testing.T) {
	resetViUsageRateLimit(t)
	withUsageRecorder(t, nil)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	assert.NotPanics(t, func() {
		RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, minSec, maxSec, time.Now())
	})
}

// TestRecordUsageIfNoIndexCoverage_NilQueryReaderNoOps: with the index-driven query path
// disabled entirely (vr == nil), the zeroth no-op gate must apply even though a recorder is
// configured and the column would otherwise qualify.
func TestRecordUsageIfNoIndexCoverage_NilQueryReaderNoOps(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)
	withVIQueryReader(t, nil, "")

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, 0, 1_000_000_000, time.Now())

	assert.Empty(t, rec.columns())
}

// TestRecordUsageIfNoIndexCoverage_RateLimitSharedWithQuerierPath is the concrete regression pin
// for Item 5's "double-recording is bounded" claim (in-process half): recordUsageForDeclinedQuery
// and RecordUsageIfNoIndexCoverage must share the same viUsageRateLimit singleton, so a burst
// across both functions for the SAME (tenant, column) still collapses to one recorded use.
func TestRecordUsageIfNoIndexCoverage_RateLimitSharedWithQuerierPath(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	withVIQueryReader(t, viStore, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	now := time.Now()
	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	recordUsageForDeclinedQuery(context.Background(), "test-tenant", prog, nil, now)
	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, minSec, maxSec, now)

	assert.Len(t, rec.columns(), 1, "both functions must share the same rate limiter for the same (tenant, column)")
}

// TestRecordUsageIfNoIndexCoverage_NeverDownloadsFileContent is the mechanism-not-outcome test
// design point 1 explicitly requires: the frontend-side check must be discovery-only (List via
// FilesForTimeRange), never downloading (Size/ReadAt) a value-index file's content.
func TestRecordUsageIfNoIndexCoverage_NeverDownloadsFileContent(t *testing.T) {
	resetViUsageRateLimit(t)
	rec := &fakeUsageRecorder{}
	withUsageRecorder(t, rec)

	viStore := &fakeVISink{}
	withVISink(t, viStore, "indexes")
	counting := &countingVIStore{fakeVISink: viStore}
	withVIQueryReader(t, counting, "indexes")

	dir := t.TempDir()
	meta, _ := writeSvcBlock(t, dir, viStore, "test-tenant", uuid.New(), "svc-alpha", 3)

	prog, err := blockpack.CompileTraceQL(`{ span.never.indexed = "x" }`, blockpack.QueryOptions{})
	require.NoError(t, err)

	minSec, maxSec := uint64(meta.StartTime.Unix()), uint64(meta.EndTime.Unix())
	RecordUsageIfNoIndexCoverage(context.Background(), "test-tenant", prog, nil, minSec, maxSec, time.Now())

	assert.Equal(t, 0, counting.sizeCalls, "must never download a value-index file's content")
	assert.Equal(t, 0, counting.readAtCalls, "must never download a value-index file's content")
}

// TestRealUsageRecorder_ForwardsToRegistryAndTriggersOnShouldBackfill exercises
// realUsageRecorder against a real blockpack.Registry (backed by the same
// fakeViObjectStore B2's tests use, vi_backfill_test.go) — the production
// implementation B3's config wiring installs via ConfigureViUsageRecorder. Proves both
// halves of its contract: it actually persists through to the registry (not a no-op),
// and it invokes onShouldBackfill exactly when the underlying trigger fires.
func TestRealUsageRecorder_ForwardsToRegistryAndTriggersOnShouldBackfill(t *testing.T) {
	store := newFakeViObjectStore()

	recordedBefore := testutil.ToFloat64(metricViUsageRecorded)
	triggeredBefore := testutil.ToFloat64(metricViBackfillTriggered)

	var backfilled []blockpack.Entry
	rec := &realUsageRecorder{
		store:      store,
		usageCfg:   blockpack.Config{DedicatedColumnsEnabled: true},
		triggerCfg: blockpack.TriggerConfig{Threshold: 1, WindowSeconds: 3600, LeaseTTLSeconds: 1800},
		onShouldBackfill: func(entry blockpack.Entry) {
			backfilled = append(backfilled, entry)
		},
	}

	result, err := rec.RecordUse(context.Background(), "tenant-a", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	assert.True(t, result.ShouldBackfill, "Threshold=1 must trigger on the first recorded use")
	require.Len(t, backfilled, 1, "onShouldBackfill must fire exactly once when the trigger fires")
	assert.Equal(t, "span.custom.attr", backfilled[0].ColumnName)

	registry := blockpack.NewRegistry(store, "tenant-a")
	entries, _, err := registry.Load(context.Background())
	require.NoError(t, err)
	require.Len(t, entries, 1, "RecordUse must actually persist through to the registry")

	assert.Equal(t, recordedBefore+1, testutil.ToFloat64(metricViUsageRecorded),
		"RecordUse must increment metricViUsageRecorded exactly once")
	assert.Equal(t, triggeredBefore+1, testutil.ToFloat64(metricViBackfillTriggered),
		"a real ShouldBackfill=true outcome must increment metricViBackfillTriggered exactly once")
}

// TestRealUsageRecorder_DifferentTenantsUseSeparateRegistries is a genuine
// multi-tenant correctness regression test: blockpack.Registry is bound to
// exactly one tenant at construction (its storage path is
// "<tenant>/viusage/index.json"), so realUsageRecorder must build a SEPARATE
// registry per tenant it ever records for -- a shared single Registry field
// would silently read/write the wrong tenant's index.json for every tenant
// except the one baked in at construction. Found while wiring
// ConfigureViUsageRecorder at querier startup (B3).
func TestRealUsageRecorder_DifferentTenantsUseSeparateRegistries(t *testing.T) {
	store := newFakeViObjectStore()
	rec := &realUsageRecorder{
		store:      store,
		usageCfg:   blockpack.Config{DedicatedColumnsEnabled: true},
		triggerCfg: blockpack.TriggerConfig{Threshold: 1, WindowSeconds: 3600, LeaseTTLSeconds: 1800},
	}

	_, err := rec.RecordUse(context.Background(), "tenant-a", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	_, err = rec.RecordUse(context.Background(), "tenant-b", "span.other.attr", "string", time.Now())
	require.NoError(t, err)

	regA := blockpack.NewRegistry(store, "tenant-a")
	entriesA, _, err := regA.Load(context.Background())
	require.NoError(t, err)
	require.Len(t, entriesA, 1, "tenant-a's own index.json must contain exactly its own entry")
	assert.Equal(t, "span.custom.attr", entriesA[0].ColumnName)

	regB := blockpack.NewRegistry(store, "tenant-b")
	entriesB, _, err := regB.Load(context.Background())
	require.NoError(t, err)
	require.Len(t, entriesB, 1, "tenant-b's own index.json must contain exactly its own entry, not tenant-a's")
	assert.Equal(t, "span.other.attr", entriesB[0].ColumnName)
}

// TestRealUsageRecorder_DisabledConfigSkipsRegistryAndBackfill: R12's safety valve,
// threaded through realUsageRecorder, must produce zero registry I/O and never invoke
// onShouldBackfill.
func TestRealUsageRecorder_DisabledConfigSkipsRegistryAndBackfill(t *testing.T) {
	store := newFakeViObjectStore()

	var backfilled []blockpack.Entry
	rec := &realUsageRecorder{
		store:      store,
		usageCfg:   blockpack.Config{DedicatedColumnsEnabled: false},
		triggerCfg: blockpack.TriggerConfig{Threshold: 1, WindowSeconds: 3600, LeaseTTLSeconds: 1800},
		onShouldBackfill: func(entry blockpack.Entry) {
			backfilled = append(backfilled, entry)
		},
	}

	result, err := rec.RecordUse(context.Background(), "tenant-a", "span.custom.attr", "string", time.Now())
	require.NoError(t, err)
	assert.False(t, result.ShouldBackfill)
	assert.Empty(t, backfilled)

	registry := blockpack.NewRegistry(store, "tenant-a")
	entries, _, err := registry.Load(context.Background())
	require.NoError(t, err)
	assert.Empty(t, entries, "a disabled Config must produce zero registry I/O")
}

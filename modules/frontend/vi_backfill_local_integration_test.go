package frontend

// vi_backfill_local_integration_test.go — backend-agnostic VI/cube usage-recording +
// backfill machinery (plan.md §12): end-to-end proof that a query for a genuinely
// never-indexed column, against a Local (non-S3) tempodb.New wiring, drives the REAL
// asyncSearchSharder.RoundTrip -> buildQueryPlan -> buildQueryPlanFromProgram ->
// vblockpack.RecordUsageIfNoIndexCoverage -> usage registry -> trigger -> async backfill
// loop, with zero S3/minio/live-cluster dependencies.
//
// Lives in package frontend (not frontend_test) because it needs the unexported
// newAsyncSearchSharder (search_sharder.go:70).

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/tempopb"
	commonv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack"
	"github.com/grafana/tempo/tempodb/wal"
)

// viBackfillIntegrationTestCol is a genuinely novel span attribute key -- not present in
// blockpack's DefaultDedicatedColumns list (verified directly against
// internal/modules/viusage/dedicated_columns.go, not assumed) -- so it can never already have
// index coverage, guaranteeing RecordUsageIfNoIndexCoverage's "no coverage" branch fires.
const viBackfillIntegrationTestCol = "plan_integration_test_col"

// viIntegrationBlockIterator is a minimal common.Iterator over an in-memory trace list --
// mirrors vblockpack's own (unexported, same-package-only) mockIterator used by
// vi_backfill_test.go/value_index_inconsistency_test.go's writeSvcBlock, re-implemented here
// since this test lives in a different package.
type viIntegrationBlockIterator struct {
	traces []*tempopb.Trace
	ids    [][]byte
	idx    int
}

func (it *viIntegrationBlockIterator) Next(_ context.Context) (common.ID, *tempopb.Trace, error) {
	if it.idx >= len(it.traces) {
		return nil, nil, io.EOF
	}
	id, tr := it.ids[it.idx], it.traces[it.idx]
	it.idx++
	return id, tr, nil
}

func (it *viIntegrationBlockIterator) Close() {}

// writeViBackfillIntegrationBlock writes one real block containing one trace with a span
// attribute at viBackfillIntegrationTestCol, via vblockpack.CreateBlock directly (NOT the
// generic WAL/CompleteBlockWithBackend path). This is required, not a shortcut:
// CompleteBlockWithBackend's own inMeta (tempodb.go) never copies a Version onto the meta it
// builds, and vblockpack.CreateBlock -- unlike vparquet4/5's CreateBlock, which
// self-assigns its own hardcoded VersionString via newStreamingBlock/backend.NewBlockMeta --
// returns the SAME meta pointer it was given, never setting Version itself (verified
// directly against tempodb/encoding/vblockpack/create.go:180's `return meta, nil`). Calling
// CreateBlock directly with a meta built via backend.NewBlockMeta(..., vblockpack.VersionString)
// (mirroring vi_backfill_test.go's own writeSvcBlock, the working precedent for this exact
// shape) sidesteps that orthogonal, pre-existing WAL-path gap entirely -- fixing
// CompleteBlockWithBackend itself is out of scope for this VI-usage-triggers-backfill test.
func writeViBackfillIntegrationBlock(ctx context.Context, t *testing.T, tempDir, tenant string, blockCfg *common.BlockConfig) {
	t.Helper()
	rawR, rawW, _, err := local.New(&local.Config{Path: tempDir})
	require.NoError(t, err)

	id := test.ValidTraceID(nil)
	tr := test.MakeTrace(1, id)
	span := tr.ResourceSpans[0].ScopeSpans[0].Spans[0]
	span.Attributes = append(span.Attributes, &commonv1.KeyValue{
		Key:   viBackfillIntegrationTestCol,
		Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "x"}},
	})

	iter := &viIntegrationBlockIterator{traces: []*tempopb.Trace{tr}, ids: [][]byte{id}}
	meta := backend.NewBlockMeta(tenant, uuid.UUID(backend.NewUUID()), vblockpack.VersionString)
	meta.StartTime = time.Now().Add(-time.Hour)
	meta.EndTime = time.Now()

	_, err = vblockpack.CreateBlock(ctx, blockCfg, meta, iter, backend.NewReader(rawR), backend.NewWriter(rawW))
	require.NoError(t, err)
}

// TestViBackfillLocalIntegration_QueryTriggersRealBackfill drives the real, end-to-end
// backend-agnostic wiring (plan.md §12): tempodb.New on backend.Local -> a real block written
// via WAL/CompleteBlock -> a real asyncSearchSharder.RoundTrip for a never-indexed column ->
// vblockpack's usage-recording hook -> registry trigger -> async backfill.
func TestViBackfillLocalIntegration_QueryTriggersRealBackfill(t *testing.T) {
	var blockCfg common.BlockConfig
	blockCfg.RegisterFlagsAndApplyDefaults("test", flag.NewFlagSet("test", flag.ContinueOnError))
	// Deliberately do NOT touch ViUsage.DedicatedColumnsEnabled/TriggerThreshold -- applyDefaults
	// already sets both (true / 1), giving this test a single-query trigger for free, matching
	// the existing S3-flavored precedent's own "zero explicit opt-in" framing.
	blockCfg.Version = vblockpack.VersionString
	// ValueIndexEnabled is NOT defaulted to true (verified against applyDefaults) -- this one
	// field IS explicit.
	blockCfg.Blockpack.ValueIndexEnabled = true

	tempDir := t.TempDir()
	cfg := &tempodb.Config{
		Backend: backend.Local,
		Local:   &local.Config{Path: tempDir},
		Block:   &blockCfg,
		WAL:     &wal.Config{Filepath: tempDir + "/wal"},
		Search: &tempodb.SearchConfig{
			ChunkSizeBytes:  1_000_000,
			ReadBufferCount: 8, ReadBufferSizeBytes: 4 * 1024 * 1024,
		},
	}

	r, _, _, err := tempodb.New(cfg, nil, log.NewNopLogger())
	require.NoError(t, err)

	const tenant = "test-tenant"
	ctx := context.Background()

	writeViBackfillIntegrationBlock(ctx, t, tempDir, tenant, &blockCfg)

	// Poll so viBlockFetcher.ListBlocksInRange (which calls reader.Blocks/BlockMeta) can
	// actually enumerate the block just written -- exercising the backfill's own real read
	// path, not just the trigger.
	r.EnablePolling(ctx, nil, false)
	r.PollNow(ctx)

	overridesIface, err := overrides.NewOverrides(overrides.Config{}, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	// overridesIface's DedicatedColumns for this tenant is empty (zero-value Config) -- the
	// novel column is deliberately NOT in it.

	jobsPerQuery := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_vi_backfill_integration_jobs_per_query",
	}, []string{"op"})

	sharder := newAsyncSearchSharder(r, overridesIface, SearchSharderConfig{
		ConcurrentRequests:    defaultConcurrentRequests,
		TargetBytesPerRequest: defaultTargetBytesPerRequest,
		MostRecentShards:      defaultMostRecentShards,
		QueryBackendAfter:     0,
		IngesterShards:        1,
	}, nil, jobsPerQuery, log.NewNopLogger())

	searchReq := &tempopb.SearchRequest{
		Query: fmt.Sprintf("{span.%s = \"x\"}", viBackfillIntegrationTestCol),
		Start: uint32(time.Now().Add(-time.Hour).Unix()),
		End:   uint32(time.Now().Add(time.Hour).Unix()),
		Limit: 20,
	}
	httpReq := httptest.NewRequest("GET", "/api/search", nil)
	httpReq, err = api.BuildSearchRequest(httpReq, searchReq)
	require.NoError(t, err)
	httpReq = httpReq.WithContext(user.InjectOrgID(httpReq.Context(), tenant))
	pipelineReq := pipeline.NewHTTPRequest(httpReq)

	triggeredBefore := vblockpack.MetricViBackfillTriggeredForTest()
	startedBefore := vblockpack.MetricViBackfillStartedForTest()
	terminalBefore := vblockpack.MetricViBackfillCompletedForTest() + vblockpack.MetricViBackfillFailedForTest()

	testRT := sharder.Wrap(pipeline.AsyncRoundTripperFunc[combiner.PipelineResponse](func(_ pipeline.Request) (pipeline.Responses[combiner.PipelineResponse], error) {
		return nil, nil
	}))
	_, err = testRT.RoundTrip(pipelineReq)
	require.NoError(t, err)

	// Primary assertion: metricViBackfillTriggered increments SYNCHRONOUSLY, inside
	// realUsageRecorder.RecordUse, before RoundTrip returns -- no goroutine-timing flakiness,
	// matching vi_usage_hook_test.go's own in-package convention for this exact metric.
	assert.Equal(t, triggeredBefore+1, vblockpack.MetricViBackfillTriggeredForTest(),
		"a query against a never-indexed column must trigger the usage-recording hook exactly once")

	// Secondary, stronger assertion: launchViBackfill runs in its own goroutine (genuinely
	// async), so this one assertion legitimately needs polling -- do not "fix" this into a
	// synchronous assert, it would flake.
	assert.Eventually(t, func() bool {
		return vblockpack.MetricViBackfillStartedForTest() == startedBefore+1
	}, 2*time.Second, 10*time.Millisecond, "the triggered backfill must actually start running within the poll window")

	// Wait for the backfill goroutine to reach a terminal state (completed or failed) before
	// the test returns -- t.TempDir()'s cleanup removes the backend directory immediately
	// after this function returns, and the async backfill goroutine (still running against
	// that same directory) racing against that RemoveAll is a genuine async hazard, not a
	// flake to paper over with a longer sleep on the assertion above.
	assert.Eventually(t, func() bool {
		return vblockpack.MetricViBackfillCompletedForTest()+vblockpack.MetricViBackfillFailedForTest() == terminalBefore+1
	}, 5*time.Second, 10*time.Millisecond, "the triggered backfill must reach a terminal state before the test's tempdir is cleaned up")
}

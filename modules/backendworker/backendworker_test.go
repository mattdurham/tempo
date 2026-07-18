package backendworker

import (
	"context"
	"encoding/binary"
	"flag"
	"net"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	backendscheduler_client "github.com/grafana/tempo/modules/backendscheduler/client"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/modules/storage"
	"github.com/grafana/tempo/pkg/model"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/wal"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var tenant = "test-tenant"

func TestWorker(t *testing.T) {
	limitCfg := overrides.Config{}
	limitCfg.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})

	ctx, cancel := context.WithCancel(context.Background())

	workerCfg, schedulerClientCfg, overridesSvc, scheduler, store := setupDependencies(ctx, t, limitCfg)

	defer func() {
		cancel()
		// Explicitly stop the store to avoid race condition on test fixture shutdown
		store.StopAsync()
		_ = store.AwaitTerminated(context.Background())
	}()

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, prometheus.DefaultRegisterer)
	require.NoError(t, err)
	require.NotNil(t, w)

	w.backendScheduler = scheduler

	err = w.processJobs(ctx)
	require.Error(t, err, "no jobs found")

	w.backendScheduler = &mockScheduler{
		next:      nextFuncWithJob(store, tenant),
		updateJob: updateJobNoop,
	}

	err = w.processJobs(ctx)
	require.NoError(t, err)

	err = services.StopAndAwaitTerminated(ctx, w)
	require.NoError(t, err)
}

func setupDependencies(ctx context.Context, t *testing.T, limits overrides.Config) (Config, backendscheduler_client.Config, overrides.Service, *mockScheduler, storage.Store) {
	t.Helper()

	var (
		workerConfig Config
		clientConfig backendscheduler_client.Config
	)
	flagext.DefaultValues(&clientConfig)

	f := flag.NewFlagSet("", flag.PanicOnError)
	workerConfig.RegisterFlagsAndApplyDefaults("backendworker", f)

	workerConfig.BackendSchedulerAddr = "localhost:1234"
	workerConfig.Ring.KVStore.Store = "inmemory"
	workerConfig.Ring.KVStore.Mock = nil
	ifaces, err := net.Interfaces()
	require.NoError(t, err)
	netWorkInteraces := make([]string, len(ifaces))
	for i, iface := range ifaces {
		netWorkInteraces[i] = iface.Name
	}
	workerConfig.Ring.InstanceInterfaceNames = netWorkInteraces

	overrides, err := overrides.NewOverrides(limits, nil, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	scheduler := &mockScheduler{
		next:      nextNoop,
		updateJob: updateJobNoop,
	}

	store, _, _ := newStore(ctx, t, t.TempDir())
	cutTestBlocks(t, store, tenant, 10, 10)

	time.Sleep(200 * time.Millisecond)

	return workerConfig, clientConfig, overrides, scheduler, store
}

var _ tempopb.BackendSchedulerClient = (*mockScheduler)(nil)

type mockScheduler struct {
	grpc_health_v1.HealthClient
	// next mock to be overridden in test scenarios if needed
	next func(ctx context.Context, in *tempopb.NextJobRequest, opts ...grpc.CallOption) (*tempopb.NextJobResponse, error)
	// next mock to be overridden in test scenarios if needed
	updateJob func(ctx context.Context, in *tempopb.UpdateJobStatusRequest, opts ...grpc.CallOption) (*tempopb.UpdateJobStatusResponse, error)
}

func (i *mockScheduler) Next(ctx context.Context, req *tempopb.NextJobRequest, _ ...grpc.CallOption) (*tempopb.NextJobResponse, error) {
	return i.next(ctx, req)
}

func (i *mockScheduler) UpdateJob(ctx context.Context, req *tempopb.UpdateJobStatusRequest, _ ...grpc.CallOption) (*tempopb.UpdateJobStatusResponse, error) {
	return i.updateJob(ctx, req)
}

func (i *mockScheduler) SubmitRedaction(_ context.Context, _ *tempopb.SubmitRedactionRequest, _ ...grpc.CallOption) (*tempopb.SubmitRedactionResponse, error) {
	return &tempopb.SubmitRedactionResponse{}, nil
}

func nextNoop(_ context.Context, _ *tempopb.NextJobRequest, _ ...grpc.CallOption) (*tempopb.NextJobResponse, error) {
	return &tempopb.NextJobResponse{}, nil
}

func updateJobNoop(_ context.Context, _ *tempopb.UpdateJobStatusRequest, _ ...grpc.CallOption) (*tempopb.UpdateJobStatusResponse, error) {
	return &tempopb.UpdateJobStatusResponse{}, nil
}

func nextFuncWithJob(store storage.Store, tenant string) func(context.Context, *tempopb.NextJobRequest, ...grpc.CallOption) (*tempopb.NextJobResponse, error) {
	var input []string

	metas := store.BlockMetas(tenant)
	for _, meta := range metas {
		input = append(input, meta.BlockID.String())
		if len(input) == 4 {
			break
		}
	}

	if len(input) == 0 {
		return nextNoop
	}

	return func(_ context.Context, _ *tempopb.NextJobRequest, _ ...grpc.CallOption) (*tempopb.NextJobResponse, error) {
		return &tempopb.NextJobResponse{
			JobId: uuid.New().String(),
			Type:  tempopb.JobType_JOB_TYPE_COMPACTION,
			Detail: tempopb.JobDetail{
				Tenant: tenant,
				Compaction: &tempopb.CompactionDetail{
					Input: input,
				},
			},
		}, nil
	}
}

func newStore(ctx context.Context, t testing.TB, tmpDir string) (storage.Store, backend.RawReader, backend.RawWriter) {
	rr, ww, _, err := local.New(&local.Config{
		Path: tmpDir + "/traces",
	})
	require.NoError(t, err)

	return newStoreWithLogger(ctx, t, test.NewTestingLogger(t), tmpDir), rr, ww
}

func newStoreWithLogger(ctx context.Context, t testing.TB, log log.Logger, tmpDir string) storage.Store {
	s, err := storage.NewStore(storage.Config{
		Trace: tempodb.Config{
			Backend: backend.Local,
			Local: &local.Config{
				Path: tmpDir + "/traces",
			},
			Block: &common.BlockConfig{
				BloomFP:             0.01,
				BloomShardSizeBytes: 100_000,
				Version:             encoding.LatestEncoding().Version(),
			},
			WAL: &wal.Config{
				Filepath: tmpDir + "/wal",
			},
			BlocklistPoll: 100 * time.Millisecond,
		},
	}, nil, log)
	require.NoError(t, err)

	s.EnablePolling(ctx, &ownsEverythingSharder{}, false)

	t.Cleanup(func() {
		s.StopAsync()
		require.NoError(t, s.AwaitTerminated(context.Background()))
	})
	return s
}

func cutTestBlocks(t testing.TB, w tempodb.Writer, tenantID string, blockCount int, recordCount int) []common.BackendBlock {
	blocks := make([]common.BackendBlock, 0)
	dec := model.MustNewSegmentDecoder(model.CurrentEncoding)

	wal := w.WAL()
	for i := 0; i < blockCount; i++ {
		meta := &backend.BlockMeta{BlockID: backend.NewUUID(), TenantID: tenantID}
		head, err := wal.NewBlock(meta, model.CurrentEncoding)
		require.NoError(t, err)

		for j := 0; j < recordCount; j++ {
			id := makeTraceID(i, j)
			tr := test.MakeTrace(1, id)
			now := uint32(time.Now().Unix())
			writeTraceToWal(t, head, dec, id, tr, now, now)
		}

		b, err := w.CompleteBlock(context.Background(), head)
		require.NoError(t, err)
		blocks = append(blocks, b)
	}

	return blocks
}

func makeTraceID(i int, j int) []byte {
	id := make([]byte, 16)
	binary.LittleEndian.PutUint64(id, uint64(i))
	binary.LittleEndian.PutUint64(id[8:], uint64(j))
	return id
}

func writeTraceToWal(t require.TestingT, b common.WALBlock, dec model.SegmentDecoder, id common.ID, tr *tempopb.Trace, start, end uint32) {
	b1, err := dec.PrepareForWrite(tr, 0, 0)
	require.NoError(t, err)

	b2, err := dec.ToObject([][]byte{b1})
	require.NoError(t, err)

	err = b.Append(id, b2, start, end, true)
	require.NoError(t, err, "unexpected error writing req")
}

func TestIsSharded(t *testing.T) {
	tests := []struct {
		name     string
		store    string
		expected bool
	}{
		{
			name:     "empty store is not sharded",
			store:    "",
			expected: false,
		},
		{
			name:     "inmemory store is not sharded",
			store:    "inmemory",
			expected: false,
		},
		{
			name:     "memberlist store is sharded",
			store:    "memberlist",
			expected: true,
		},
		{
			name:     "consul store is sharded",
			store:    "consul",
			expected: true,
		},
		{
			name:     "etcd store is sharded",
			store:    "etcd",
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &BackendWorker{
				cfg: Config{
					Ring: RingConfig{
						KVStore: kv.Config{
							Store: tc.store,
						},
					},
				},
			}
			assert.Equal(t, tc.expected, w.isSharded())
		})
	}
}

// storeWithoutPolling constructs a storage.Store like newStoreWithLogger, but WITHOUT
// calling EnablePolling -- for tests that call w.starting() directly, since starting()
// itself unconditionally calls store.EnablePolling exactly once. Passing it a store that
// was never separately polling-enabled means starting()'s own call is the only one,
// avoiding a race between two concurrent pollers on the same underlying blocklist state.
func storeWithoutPolling(t *testing.T) storage.Store {
	t.Helper()
	tmpDir := t.TempDir()
	s, err := storage.NewStore(storage.Config{
		Trace: tempodb.Config{
			Backend: backend.Local,
			Local: &local.Config{
				Path: tmpDir + "/traces",
			},
			Block: &common.BlockConfig{
				BloomFP:             0.01,
				BloomShardSizeBytes: 100_000,
				Version:             encoding.LatestEncoding().Version(),
			},
			WAL: &wal.Config{
				Filepath: tmpDir + "/wal",
			},
			BlocklistPoll: 100 * time.Millisecond,
		},
	}, nil, test.NewTestingLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() {
		s.StopAsync()
		require.NoError(t, s.AwaitTerminated(context.Background()))
	})
	return s
}

// nilKVClient is a local no-op kv.Client used by
// TestStarting_RingNeverReachesActive_ReturnsNilNotError. dskit's own built-in
// Store: "mock" client (vendor/github.com/grafana/dskit/kv/mock.go) returns ("", nil)
// from Get, which violates kv.Client's own documented contract ("If the key does not
// exist, Get will return nil and no error.") and crashes ring.Ring.starting()
// ("interface conversion: interface {} is string, not *ring.Desc") the moment the ring
// subservice starts -- confirmed by running this test against the vendored mock client
// directly. nilKVClient fixes that (Get returns (nil, nil)) and also fixes a second,
// related gap: WatchKey/WatchPrefix must BLOCK until ctx is done, exactly like every
// real backend (consul/etcd/memberlist) -- ring.Ring's own running() loop calls
// WatchKey synchronously as its entire body, so a WatchKey that returns immediately
// (as a naive no-op would) makes running() return nil right away, and the ring
// subservice reaches Terminated before Manager.AwaitHealthy ever observes it as
// healthy, failing STARTUP for an unrelated reason before ring.WaitInstanceState is
// even reached. CAS is a true no-op (never invokes the callback, never persists), so
// ring.WaitInstanceState can structurally never observe this instance as ACTIVE -- a
// deterministic timeout, not a flaky one.
type nilKVClient struct{}

var _ kv.Client = nilKVClient{}

func (nilKVClient) List(_ context.Context, _ string) ([]string, error)   { return nil, nil }
func (nilKVClient) Get(_ context.Context, _ string) (interface{}, error) { return nil, nil }
func (nilKVClient) Delete(_ context.Context, _ string) error             { return nil }
func (nilKVClient) CAS(_ context.Context, _ string, _ func(in interface{}) (out interface{}, retry bool, err error)) error {
	return nil
}
func (nilKVClient) WatchKey(ctx context.Context, _ string, _ func(interface{}) bool) {
	<-ctx.Done()
}
func (nilKVClient) WatchPrefix(ctx context.Context, _ string, _ func(string, interface{}) bool) {
	<-ctx.Done()
}

// TestStarting_RingNeverReachesActive_ReturnsNilNotError is #516's lifecycle-level pin:
// starting() must not fail the dskit service lifecycle just because the ring never
// reaches ACTIVE. Owns() IS consulted live today (via store.EnablePolling's
// blocklist.Poller.tenantIndexBuilder, see the Owns() doc comment in backendworker.go),
// but it already fails closed on ring errors and the poller's PollFallback safely builds
// the tenant index anyway when ownership can't be determined -- this test's own logs
// show "writing tenant index" succeeding every cycle despite the ring never reaching
// ACTIVE. nilKVClient above never persists anything, so ring.WaitInstanceState can
// structurally never observe ACTIVE: a deterministic timeout, not a flaky one.
func TestStarting_RingNeverReachesActive_ReturnsNilNotError(t *testing.T) {
	limitCfg := overrides.Config{}
	limitCfg.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})

	ctx := context.Background()
	// setupDependencies's own fixture already enables blocklist polling on its returned
	// store (see newStoreWithLogger) -- w.starting(ctx) below unconditionally enables
	// polling a SECOND time on whatever store it's given, which would race two concurrent
	// pollers against the same underlying blocklist state if given that same store (this
	// test doesn't need blocklist data at all, only the Postgres/ring behavior).
	// storeWithoutPolling gives w.starting() a store that has never had EnablePolling
	// called, so its own call is the only one -- avoiding the race deterministically
	// rather than racing two pollers and hoping a canceled context stops the first one
	// in time.
	workerCfg, schedulerClientCfg, overridesSvc, _, _ := setupDependencies(ctx, t, limitCfg)
	store := storeWithoutPolling(t)

	workerCfg.Ring.KVStore.Store = "mock"
	workerCfg.Ring.KVStore.Mock = nilKVClient{}
	workerCfg.Ring.WaitActiveInstanceTimeout = 500 * time.Millisecond
	workerCfg.Ring.WaitStabilityMinDuration = 0

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NotNil(t, w)

	require.True(t, w.isSharded(), "precondition: the ring-gated branch of starting() must actually be exercised")

	err = w.starting(ctx)
	t.Cleanup(func() {
		if w.subservices != nil {
			_ = services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
		}
	})
	require.NoError(t, err, "starting() must return nil even when the ring never reaches ACTIVE")
}

// TestStarting_RingReachesActive_ReturnsNilAsToday is the healthy-ring regression guard
// for #516: a single instance genuinely reaching ACTIVE must still let starting() return
// nil, unchanged from today's behavior. Mirrors modules/livestore/live_store_test.go's
// consul.NewInMemoryClient + Ring.KVStore.Mock pattern for a real (in-memory) healthy
// ring -- kv.Config.Mock is consulted before Store's string value during client
// construction (see dskit/kv/client.go's NewClient), so Store is set to a real backend
// name ("consul") purely so isSharded() reports true; the actual client used is the
// in-memory mock below regardless.
func TestStarting_RingReachesActive_ReturnsNilAsToday(t *testing.T) {
	limitCfg := overrides.Config{}
	limitCfg.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})

	ctx := context.Background()
	// See TestStarting_RingNeverReachesActive_ReturnsNilNotError's own comment: give
	// starting() a store that was never separately polling-enabled (storeWithoutPolling),
	// so its own EnablePolling call is the only one.
	workerCfg, schedulerClientCfg, overridesSvc, _, _ := setupDependencies(ctx, t, limitCfg)
	store := storeWithoutPolling(t)

	mockStore, _ := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	workerCfg.Ring.KVStore.Store = "consul"
	workerCfg.Ring.KVStore.Mock = mockStore
	workerCfg.Ring.WaitActiveInstanceTimeout = 10 * time.Second
	workerCfg.Ring.WaitStabilityMinDuration = 0

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NotNil(t, w)

	require.True(t, w.isSharded(), "precondition: the ring-gated branch of starting() must actually be exercised")

	err = w.starting(ctx)
	t.Cleanup(func() {
		if w.subservices != nil {
			_ = services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
		}
	})
	require.NoError(t, err, "starting() must return nil once the instance genuinely reaches ACTIVE, same as today")
}

// TestEffectiveBlockRetentionMinutes pins the 2026-07-17 fix (follow-up to blockpack#512): a
// cube backfill's window is now bounded by the tenant's actual effective retention instead of
// an unconditional math.MaxUint32. Mirrors tempodb.go's retainTenant precedence exactly:
// per-tenant override wins when nonzero, else the compactor's configured default; a resolved
// retention of zero (both unset) means "unbounded" (0), matching RunCubeBackfill's own
// zero-means-unbounded convention.
func TestEffectiveBlockRetentionMinutes(t *testing.T) {
	tests := []struct {
		name           string
		cfgDefault     time.Duration
		tenantOverride time.Duration
		want           uint32
	}{
		{"both unset means unbounded", 0, 0, 0},
		{"cfg default only", 30 * 24 * time.Hour, 0, 43200},
		{"tenant override wins over cfg default", 30 * 24 * time.Hour, 24 * time.Hour, 1440},
		{"tenant override alone with no cfg default", 0, 24 * time.Hour, 1440},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := effectiveBlockRetentionMinutes(tc.cfgDefault, tc.tenantOverride)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestFailJob_CompactionJobWithEmptyTenant_ReportsFailureViaSchedulerUpdateJob closes failJob's
// coverage gap (0% before this test): failJob is real, live production code for the
// gRPC-scheduler-dispatched job types (compaction, redaction) that were never migrated to the
// Postgres job queue the way vi_backfill/cube_backfill were (see processCompactionJob/
// processRedactionJob's own resp *tempopb.NextJobResponse signature -- they're reached only via
// the gRPC Next() path, never dispatchPostgresJob) -- it was simply never exercised by a test.
// An empty-tenant compaction job is the cheapest real trigger: processCompactionJob's very first
// guard calls failJob before touching the store/compactor at all.
func TestFailJob_CompactionJobWithEmptyTenant_ReportsFailureViaSchedulerUpdateJob(t *testing.T) {
	limitCfg := overrides.Config{}
	limitCfg.RegisterFlagsAndApplyDefaults(&flag.FlagSet{})

	ctx := context.Background()
	workerCfg, schedulerClientCfg, overridesSvc, _, store := setupDependencies(ctx, t, limitCfg)

	w, err := New(workerCfg, schedulerClientCfg, nil, store, overridesSvc, nil)
	require.NoError(t, err)

	var captured *tempopb.UpdateJobStatusRequest
	jobID := uuid.New().String()
	w.backendScheduler = &mockScheduler{
		next: func(context.Context, *tempopb.NextJobRequest, ...grpc.CallOption) (*tempopb.NextJobResponse, error) {
			return &tempopb.NextJobResponse{
				JobId: jobID,
				Type:  tempopb.JobType_JOB_TYPE_COMPACTION,
				Detail: tempopb.JobDetail{
					Tenant: "", // triggers processCompactionJob's empty-tenant guard -> failJob
				},
			}, nil
		},
		updateJob: func(_ context.Context, req *tempopb.UpdateJobStatusRequest, _ ...grpc.CallOption) (*tempopb.UpdateJobStatusResponse, error) {
			captured = req
			return &tempopb.UpdateJobStatusResponse{}, nil
		},
	}

	err = w.processJobs(ctx)
	require.Error(t, err, "failJob must surface the failure to its caller, not swallow it")
	require.Contains(t, err.Error(), "received compaction job with empty tenant")

	require.NotNil(t, captured, "failJob must report the failure via the scheduler's real UpdateJob RPC")
	assert.Equal(t, jobID, captured.JobId)
	assert.Equal(t, tempopb.JobStatus_JOB_STATUS_FAILED, captured.Status)
	assert.Equal(t, "received compaction job with empty tenant", captured.Error)
}

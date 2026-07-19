package vblockpack

// vi_backfill_test.go — #496 B2 tests. Mirrors cube_backfill.go's own test
// coverage (there is none -- verified: no cube_backfill_test.go/
// cubebackfill_test.go exist in this repo, which is exactly the R9 gap this
// task must not repeat). runViBackfillCore's dependency-injected split
// (vi_backfill.go) makes this testable against fakes without a live
// S3/minio server, unlike RunCubeBackfill.

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	blockpack "github.com/grafana/blockpack"
)

// TestNewViBackfillDepsS3_NilConfigReturnsZeroValue preserves RunViBackfill's
// pre-existing "s3cfg == nil -> no-op" contract at the deps-construction
// level, now that the nil check has moved out of RunViBackfill itself (plan
// §9 item 5).
func TestNewViBackfillDepsS3_NilConfigReturnsZeroValue(t *testing.T) {
	deps, err := NewViBackfillDepsS3(nil)
	require.NoError(t, err)
	assert.Nil(t, deps.Fetcher)
	assert.Nil(t, deps.ObjStore)
	assert.Nil(t, deps.Putter)
}

// TestNewViBackfillDepsRaw_ConstructsGenericDeps proves the generic
// constructor wires up the exact Track A/B/C types this task introduces
// (viBlockFetcher + Track A's newObjectStoreForBackend + Track C's
// rawObjectPutter) against a real local.NewBackend -- no S3/minio required.
func TestNewViBackfillDepsRaw_ConstructsGenericDeps(t *testing.T) {
	rawR, rawW := newLocalRawBackend(t)
	deps := NewViBackfillDepsRaw(rawR, rawW)

	require.NotNil(t, deps.Fetcher)
	require.NotNil(t, deps.ObjStore)
	require.NotNil(t, deps.Putter)

	_, ok := deps.Fetcher.(*viBlockFetcher)
	assert.True(t, ok, "expected *viBlockFetcher, got %T", deps.Fetcher)
	// local.Backend has no GCS versioned capability, so newObjectStoreForBackend
	// must fall back to the generic rawObjectStore, not the GCS-native path.
	_, ok = deps.ObjStore.(*rawObjectStore)
	assert.True(t, ok, "expected *rawObjectStore, got %T", deps.ObjStore)
	_, ok = deps.Putter.(*rawObjectPutter)
	assert.True(t, ok, "expected *rawObjectPutter, got %T", deps.Putter)

	// Round-trip the putter against the real backend to prove it is genuinely wired,
	// not just the right type.
	require.NoError(t, deps.Putter.Put("t/indexes/roundtrip.blockpack", []byte("hello")))
}

// TestRunViBackfill_ZeroValueDepsIsNoop mirrors the old RunViBackfill's
// "s3cfg == nil" no-op contract: a zero-value RunViBackfillDeps (as returned
// by NewViBackfillDepsS3(nil)) must return nil immediately without touching
// metricViBackfillStarted.
func TestRunViBackfill_ZeroValueDepsIsNoop(t *testing.T) {
	before := testutil.ToFloat64(metricViBackfillStarted)
	err := RunViBackfill(context.Background(), blockpack.Entry{Tenant: "t"}, RunViBackfillDeps{}, 0)
	require.NoError(t, err)
	assert.Equal(t, before, testutil.ToFloat64(metricViBackfillStarted),
		"zero-value deps must not increment metricViBackfillStarted")
}

// TestRunViBackfill_UsesProvidedRegistry_NotFreshFromObjStore is the direct
// regression guard for a live bug found 2026-07-11 (tenant 11638,
// tempo-dev-test-03): RunViBackfill used to always build a FRESH registry
// from deps.ObjStore inside runViBackfillCore, ignoring any registry the
// caller already used to create/trigger the entry (e.g. ConfigureViUsage's
// Postgres-backed registryFor(tenant)). That meant the watermark-persist call
// could never find the entry the trigger had just created in a DIFFERENT
// (Postgres) registry, failing every real backfill run on its very first
// progress callback with "entry ... not found".
func TestRunViBackfill_UsesProvidedRegistry_NotFreshFromObjStore(t *testing.T) {
	block := writeViBackfillTestBlock(t, 1_000_000_000, "a")
	fetcher := &fakeViBlockFetcher{refs: []string{"ref-a"}, blocks: map[string][]byte{"ref-a": block}}
	putter := newFakeViPutter()

	// entryStore has the triggered entry -- this is the one that must be used.
	entryStore := newFakeViObjectStore()
	entry := seedTriggeredEntry(t, entryStore, "tenant-a", "span.custom.attr", "string")
	providedRegistry := blockpack.NewRegistry(entryStore, entry.Tenant)

	// staleObjStore is a SEPARATE, empty store -- if RunViBackfill ever falls
	// back to building a fresh registry from ObjStore instead of using
	// deps.Registry, the watermark-persist call finds nothing here and fails
	// with "entry ... not found", exactly reproducing the live bug.
	staleObjStore := newFakeViObjectStore()

	deps := RunViBackfillDeps{
		Fetcher:  fetcher,
		ObjStore: staleObjStore,
		Putter:   putter,
		Registry: providedRegistry,
	}

	err := RunViBackfill(context.Background(), entry, deps, 0)
	require.NoError(t, err, "must use deps.Registry, not rebuild one from the stale ObjStore")

	assert.Equal(t, 0, staleObjStore.putCalls, "the stale ObjStore must never be touched when Registry is provided")
	assert.Greater(t, entryStore.putCalls, 1, "the provided registry's own store must receive the watermark persist")
}

// fakeViObjectStore is an in-memory blockpack.ObjectStore for tests, mirroring
// the same shape internal/modules/viusage's own registry tests use in
// blockpack. Tracks every successful ConditionalPut so tests can assert how
// many times the registry was actually written to.
type fakeViObjectStore struct {
	mu       sync.Mutex
	objects  map[string]fakeViObj
	putCalls int
}

type fakeViObj struct {
	data []byte
	etag string
}

func newFakeViObjectStore() *fakeViObjectStore {
	return &fakeViObjectStore{objects: map[string]fakeViObj{}}
}

func (s *fakeViObjectStore) Get(_ context.Context, path string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[path]
	if !ok {
		return nil, "", nil
	}
	return append([]byte(nil), obj.data...), obj.etag, nil
}

func (s *fakeViObjectStore) ConditionalPut(_ context.Context, path string, data []byte, etag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.objects[path]
	if obj.etag != etag {
		return blockpack.ErrConflict
	}
	s.objects[path] = fakeViObj{data: append([]byte(nil), data...), etag: etag + "x"}
	s.putCalls++
	return nil
}

// fakeViPutter is an in-memory blockpack.ObjectPutter for tests.
type fakeViPutter struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newFakeViPutter() *fakeViPutter { return &fakeViPutter{objs: map[string][]byte{}} }

func (p *fakeViPutter) Put(key string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.objs[key] = append([]byte(nil), data...)
	return nil
}

// fakeViBlockFetcher implements blockpack.BlockFetcher against an in-memory
// list of (sourceRef, blockBytes), in the exact order given.
type fakeViBlockFetcher struct {
	refs       []string
	blocks     map[string][]byte
	lastMinSec uint64
	lastMaxSec uint64
}

func (f *fakeViBlockFetcher) ListBlocksInRange(_ context.Context, _ string, minSec, maxSec uint64) ([]string, error) {
	f.lastMinSec = minSec
	f.lastMaxSec = maxSec
	return f.refs, nil
}

func (f *fakeViBlockFetcher) FetchBlock(_ context.Context, sourceRef string) (*blockpack.Reader, error) {
	data, ok := f.blocks[sourceRef]
	if !ok {
		return nil, errors.New("fakeViBlockFetcher: no block registered for " + sourceRef)
	}
	return blockpack.NewReaderFromProvider(&fakeViBytesProvider{data: data})
}

// fakeViBytesProvider adapts an in-memory []byte to blockpack.ReaderProvider.
type fakeViBytesProvider struct {
	data []byte
}

func (p *fakeViBytesProvider) Size() (int64, error) { return int64(len(p.data)), nil }

func (p *fakeViBytesProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	if off >= int64(len(p.data)) {
		return 0, nil
	}
	return copy(buf, p.data[off:]), nil
}

// writeViBackfillTestBlock writes a small blockpack block with a single
// span carrying the target attribute, for use as a fake fetcher's payload.
func writeViBackfillTestBlock(t *testing.T, startNano uint64, attrValue string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := blockpack.NewWriter(&buf, 0)
	require.NoError(t, err)

	td := &tracev1.TracesData{
		ResourceSpans: []*tracev1.ResourceSpans{{
			Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
				Key: "service.name",
				Value: &commonv1.AnyValue{
					Value: &commonv1.AnyValue_StringValue{StringValue: "svc-backfill"},
				},
			}}},
			ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
				TraceId:           []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				SpanId:            []byte{1, 2, 3, 4, 5, 6, 7, 8},
				Name:              "op",
				StartTimeUnixNano: startNano,
				EndTimeUnixNano:   startNano + 1_000_000,
				Attributes: []*commonv1.KeyValue{{
					Key:   "custom.attr",
					Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: attrValue}},
				}},
			}}}},
		}},
	}
	require.NoError(t, w.AddTracesData(td))
	_, err = w.Flush()
	require.NoError(t, err)
	return buf.Bytes()
}

// seedTriggeredEntry records one use with a threshold=1 TriggerConfig so the
// resulting entry is immediately Triggered=true, backed by store, ready for
// runViBackfillCore's UpdateWatermark calls (which error on a not-found
// entry).
func seedTriggeredEntry(t *testing.T, store blockpack.ObjectStore, tenant, colName, colType string) blockpack.Entry {
	t.Helper()
	registry := blockpack.NewRegistry(store, tenant)
	result, err := blockpack.RecordUseAndMaybeTrigger(
		context.Background(), registry, tenant, colName, colType, time.Unix(1000, 0),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill, "threshold=1 must trigger immediately on the first use")
	return result.Entry
}

// TestRunViBackfillCore_CallsUpdateWatermarkOnEachProgress is THE critical
// R9 regression test: cube's own launchBackfill/RunCubeBackfill progressFn
// callbacks were found (plan.md Section 1) to ONLY log, never persist a
// watermark. VI's wiring must not repeat that gap -- assert
// Registry.UpdateWatermark (via the fake store's ConditionalPut) is actually
// called once per fetched block, not just once at the end.
func TestRunViBackfillCore_CallsUpdateWatermarkOnEachProgress(t *testing.T) {
	blockA := writeViBackfillTestBlock(t, 3_000_000_000, "a")
	blockB := writeViBackfillTestBlock(t, 1_000_000_000, "b")
	fetcher := &fakeViBlockFetcher{
		refs:   []string{"ref-a", "ref-b"},
		blocks: map[string][]byte{"ref-a": blockA, "ref-b": blockB},
	}
	store := newFakeViObjectStore()
	entry := seedTriggeredEntry(t, store, "tenant-a", "span.custom.attr", "string")
	putter := newFakeViPutter()
	registry := blockpack.NewRegistry(store, entry.Tenant)

	completedBefore := testutil.ToFloat64(metricViBackfillCompleted)

	err := runViBackfillCore(context.Background(), entry, fetcher, registry, putter, "indexes", math.MaxUint64)
	require.NoError(t, err)

	// One ConditionalPut for the initial seedTriggeredEntry call, plus one per
	// fetched block's progress callback (2 blocks) -- i.e. strictly more than
	// one, proving persistence happens on EVERY callback, not just the last.
	assert.Equal(t, 3, store.putCalls, "expected 1 (seed) + 2 (one per block) ConditionalPut calls")
	entries, _, loadErr := registry.Load(context.Background())
	require.NoError(t, loadErr)
	require.Len(t, entries, 1)
	assert.True(t, entries[0].Backfill.Done, "the final progress callback must mark the entry Done")
	assert.False(t, entries[0].Backfill.BackfillInProgress, "Done must release the lease in the same PUT")

	assert.Equal(t, completedBefore+1, testutil.ToFloat64(metricViBackfillCompleted),
		"reaching prog.Done must increment metricViBackfillCompleted exactly once")
}

// TestRunViBackfillCore_PersistFailureAbortsRun verifies a watermark-persist
// failure aborts the run rather than silently continuing to write more L0
// files the registry cannot yet account for.
func TestRunViBackfillCore_PersistFailureAbortsRun(t *testing.T) {
	blockA := writeViBackfillTestBlock(t, 3_000_000_000, "a")
	blockB := writeViBackfillTestBlock(t, 1_000_000_000, "b")
	fetcher := &fakeViBlockFetcher{
		refs:   []string{"ref-a", "ref-b"},
		blocks: map[string][]byte{"ref-a": blockA, "ref-b": blockB},
	}
	store := newFakeViObjectStore()
	entry := seedTriggeredEntry(t, store, "tenant-a", "span.custom.attr", "string")
	putter := newFakeViPutter()

	// A store that always conflicts makes every UpdateWatermark call exhaust
	// its retries and fail.
	failing := &alwaysConflictStore{inner: store}
	failingRegistry := blockpack.NewRegistry(failing, entry.Tenant)

	err := runViBackfillCore(context.Background(), entry, fetcher, failingRegistry, putter, "indexes", math.MaxUint64)
	require.Error(t, err)
}

// TestRunViBackfillCore_UsesEntryWatermarkAsAnchor pins issue #518's fix: a
// chained continuation job must anchor its window to the column's
// already-persisted watermark, not to wall-clock now -- otherwise every
// chained job would reprocess the same "most recent WindowSeconds" slice
// forever and never make backward progress into older history.
func TestRunViBackfillCore_UsesEntryWatermarkAsAnchor(t *testing.T) {
	store := newFakeViObjectStore()
	entry := seedTriggeredEntry(t, store, "tenant-a", "span.custom.attr", "string")
	entry.Backfill.WatermarkSec = 5000 // simulates resuming from a previously-persisted watermark

	fetcher := &fakeViBlockFetcher{}
	putter := newFakeViPutter()
	registry := blockpack.NewRegistry(store, entry.Tenant)

	err := runViBackfillCore(context.Background(), entry, fetcher, registry, putter, "indexes", 100)
	require.NoError(t, err)
	assert.Equal(t, uint64(5000), fetcher.lastMaxSec, "maxSec must come from entry.Backfill.WatermarkSec, not now()")
	assert.Equal(t, uint64(4900), fetcher.lastMinSec, "minSec must be WatermarkSec-windowSeconds")
}

type alwaysConflictStore struct {
	inner *fakeViObjectStore
}

func (s *alwaysConflictStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	return s.inner.Get(ctx, path)
}

func (s *alwaysConflictStore) ConditionalPut(context.Context, string, []byte, string) error {
	return blockpack.ErrConflict
}

// TestLaunchViBackfill_StartsGoroutine verifies launchViBackfill returns
// immediately (async) rather than blocking the caller -- mirrors
// launchBackfill's own fire-and-forget contract. A zero-value
// RunViBackfillDeps is a safe no-op (RunViBackfill's own contract), so this
// only pins the "does not block" property, not real backfill I/O.
func TestLaunchViBackfill_StartsGoroutine(t *testing.T) {
	done := make(chan struct{})
	go func() {
		launchViBackfill(blockpack.Entry{Tenant: "t", ColumnName: "span.x"}, RunViBackfillDeps{}, 0)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("launchViBackfill must return promptly, not block on the backfill itself")
	}
}

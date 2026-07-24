package vblockpack

// vi_watermark_cache_test.go — #496 B3 tests for viWatermarkCache.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blockpack "github.com/grafana/blockpack"
)

// countingObjectStore wraps a fakeViObjectStore-shaped in-memory store,
// counting every Get call so tests can assert how many times the registry
// was actually loaded from the backing store.
type countingObjectStore struct {
	mu       sync.Mutex
	objects  map[string]countingObj
	getCalls int
}

type countingObj struct {
	data []byte
	etag string
}

func newCountingObjectStore() *countingObjectStore {
	return &countingObjectStore{objects: map[string]countingObj{}}
}

func (s *countingObjectStore) Get(_ context.Context, path string) ([]byte, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	obj, ok := s.objects[path]
	if !ok {
		return nil, "", nil
	}
	return append([]byte(nil), obj.data...), obj.etag, nil
}

func (s *countingObjectStore) ConditionalPut(_ context.Context, path string, data []byte, etag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj := s.objects[path]
	if obj.etag != etag {
		return blockpack.ErrConflict
	}
	s.objects[path] = countingObj{data: append([]byte(nil), data...), etag: etag + "x"}
	return nil
}

func seedViWatermarkEntry(t *testing.T, store blockpack.ObjectStore, tenant, colName string) {
	t.Helper()
	registry := blockpack.NewRegistry(store, tenant)
	result, err := blockpack.RecordUseAndMaybeTrigger(
		context.Background(), registry, tenant, colName, "string", time.Unix(1000, 0),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill)
	require.NoError(t, registry.UpdateWatermark(
		context.Background(), tenant, result.Entry.ColumnHash, result.Entry.ColumnType, 500, 100, 1000, false,
	))
}

// seedViWatermarkEntryTyped mirrors seedViWatermarkEntry but takes an explicit colType and
// watermarkSec, so a test can seed TWO independent registry entries sharing the same colName
// but differing colType -- the exact live collision shape issue #536 fixes (confirmed on
// tenant 11638: span:duration exists as both a stale int64 entry and the active, real-coverage
// uint64 entry).
func seedViWatermarkEntryTyped(t *testing.T, store blockpack.ObjectStore, tenant, colName, colType string, watermarkSec uint64) {
	t.Helper()
	registry := blockpack.NewRegistry(store, tenant)
	result, err := blockpack.RecordUseAndMaybeTrigger(
		context.Background(), registry, tenant, colName, colType, time.Unix(1000, 0),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill)
	require.NoError(t, registry.UpdateWatermark(
		context.Background(), tenant, result.Entry.ColumnHash, result.Entry.ColumnType, watermarkSec, 0, 100_000, false,
	))
}

// TestViWatermarkCache_SameNameDifferentType_BothPreservedIndependently is issue #536's mandatory
// adversarial reproduction of the confirmed live bug (tenant 11638): a column with the SAME name
// but TWO distinct registry entries by type -- one stale/essentially-abandoned with near-empty
// real coverage (int64, WatermarkSec close to "now" -- almost nothing of the [0, windowEnd)
// history is confirmed complete), and one active with real, near-complete coverage (uint64,
// WatermarkSec == 0 -- fully covered back to the beginning of the window). Before the #536 fix,
// WatermarksFor kept exactly ONE map entry for this colName (whichever registry row Load()
// visited last silently overwrote the other's coverage state) -- a query resolving to the
// uint64 column could get gated by the int64 entry's stale, near-empty coverage (declining a
// query that DOES have real coverage), or vice versa. After the fix, both entries are
// independently, correctly preserved and addressable via their own composite key.
func TestViWatermarkCache_SameNameDifferentType_BothPreservedIndependently(t *testing.T) {
	const (
		tenant    = "tenant-collision"
		col       = "span:duration"
		windowEnd = uint64(100_000)
	)
	store := newCountingObjectStore()
	// The stale, essentially-abandoned int64 entry: WatermarkSec close to windowEnd means almost
	// none of [0, windowEnd) is confirmed complete -- near-empty real backfill history.
	seedViWatermarkEntryTyped(t, store, tenant, col, "int64", windowEnd-1_000)
	// The active, real-coverage uint64 entry: WatermarkSec == 0 means the ENTIRE window is
	// confirmed complete -- near-complete real coverage.
	seedViWatermarkEntryTyped(t, store, tenant, col, "uint64", 0)

	cache := newViWatermarkCache(store, nil, time.Minute)
	wm, err := cache.WatermarksFor(context.Background(), tenant)
	require.NoError(t, err)

	// The #536 regression itself: a name-only key would collapse these two entries into one --
	// this length check alone fails under the pre-fix (name-only) keying.
	require.Len(t, wm, 2, "both the int64 and uint64 entries for the SAME column name must be "+
		"preserved independently, not collapsed into one map entry")

	int64Key := blockpack.ColumnWatermarkKey(col, "int64")
	uint64Key := blockpack.ColumnWatermarkKey(col, "uint64")
	require.Contains(t, wm, int64Key)
	require.Contains(t, wm, uint64Key)

	assert.False(t, wm[int64Key].CoversRange(0, windowEnd),
		"the stale int64 entry's near-empty coverage must correctly decline the full window")
	assert.True(t, wm[uint64Key].CoversRange(0, windowEnd),
		"the active uint64 entry's real, near-complete coverage must correctly cover the full "+
			"window -- and must NEVER be gated by the unrelated int64 entry's stale state")
}

func TestViWatermarkCache_ReturnsTriggeredColumnsOnly(t *testing.T) {
	store := newCountingObjectStore()
	seedViWatermarkEntry(t, store, "tenant-a", "span.custom.attr")

	cache := newViWatermarkCache(store, nil, time.Minute)
	wm, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)

	// Issue #536: keyed by blockpack.ColumnWatermarkKey(colName, colType), not colName alone.
	wmKey := blockpack.ColumnWatermarkKey("span.custom.attr", "string")
	require.Contains(t, wm, wmKey)
	assert.True(t, wm[wmKey].Triggered)
	assert.Equal(t, uint64(500), wm[wmKey].WatermarkSec)
}

func TestViWatermarkCache_EmptyRegistryReturnsEmptyMap(t *testing.T) {
	store := newCountingObjectStore()
	cache := newViWatermarkCache(store, nil, time.Minute)

	wm, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)
	assert.Empty(t, wm)
}

// TestViWatermarkCache_PgConfigured_UsesPostgresRegistryNotObjectStore is the direct
// regression guard for the 2026-07-22 fix: before it, this cache ALWAYS read the
// object-store-backed registry regardless of pg, silently disconnected from the
// Postgres-backed registry every other component (ConfigureViUsage's write-path recorder,
// compaction-worker's backfill execution) actually reads/writes for a Postgres-configured
// deployment -- making the R7 partial-coverage gate a permanent no-op in production. Seeds
// ONLY the Postgres-backed registry (via the exact same pg.ViUsageRegistry construction
// realUsageRecorder.registryFor uses when pg != nil) and leaves the object store genuinely
// empty, so a pass here is only possible if WatermarksFor actually read from Postgres.
func TestViWatermarkCache_PgConfigured_UsesPostgresRegistryNotObjectStore(t *testing.T) {
	pool := newTestPostgresPool(t)
	pg := blockpack.NewPostgresFromPool(pool)

	pgRegistry := pg.ViUsageRegistry("tenant-a")
	result, err := blockpack.RecordUseAndMaybeTrigger(
		context.Background(), pgRegistry, "tenant-a", "span.custom.attr", "string", time.Unix(1000, 0),
		blockpack.TriggerConfig{LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill)
	require.NoError(t, pgRegistry.UpdateWatermark(
		context.Background(), "tenant-a", result.Entry.ColumnHash, result.Entry.ColumnType, 500, 100, 1000, false,
	))

	objStore := newCountingObjectStore() // deliberately left empty

	cache := newViWatermarkCache(objStore, pg, time.Minute)
	wm, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)

	// Issue #536: keyed by blockpack.ColumnWatermarkKey(colName, colType), not colName alone.
	wmKey := blockpack.ColumnWatermarkKey("span.custom.attr", "string")
	require.Contains(t, wm, wmKey,
		"must read the Postgres-backed registry when pg is configured, not the empty object store")
	assert.True(t, wm[wmKey].Triggered)
	assert.Equal(t, uint64(500), wm[wmKey].WatermarkSec)

	objStore.mu.Lock()
	gets := objStore.getCalls
	objStore.mu.Unlock()
	assert.Zero(t, gets, "must never touch the object store when pg is configured")
}

// TestWatermarkCache_ShortTTL_CollapsesRepeatedLoads mirrors
// backfillListTTL/newCachingStoreWithListTTL's existing pattern: a burst of
// concurrent queries against the same tenant must collapse to one registry
// object-storage GET within the TTL window.
func TestWatermarkCache_ShortTTL_CollapsesRepeatedLoads(t *testing.T) {
	store := newCountingObjectStore()
	seedViWatermarkEntry(t, store, "tenant-a", "span.custom.attr")
	store.mu.Lock()
	store.getCalls = 0 // reset after seeding (seeding itself performs Gets via the registry's own retry loop)
	store.mu.Unlock()

	cache := newViWatermarkCache(store, nil, time.Minute)

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			_, err := cache.WatermarksFor(context.Background(), "tenant-a")
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	store.mu.Lock()
	gets := store.getCalls
	store.mu.Unlock()
	assert.Equal(t, 1, gets, "a burst of concurrent queries against the same tenant must collapse to one registry GET")

	// A second call within the TTL window must not trigger another Get either.
	_, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)
	store.mu.Lock()
	gets = store.getCalls
	store.mu.Unlock()
	assert.Equal(t, 1, gets, "a call within the TTL window must reuse the cached snapshot")
}

func TestViWatermarkCache_RefreshesAfterTTLExpiry(t *testing.T) {
	store := newCountingObjectStore()
	seedViWatermarkEntry(t, store, "tenant-a", "span.custom.attr")
	store.mu.Lock()
	store.getCalls = 0
	store.mu.Unlock()

	fixedNow := time.Now()
	cache := newViWatermarkCache(store, nil, time.Second)
	cache.now = func() time.Time { return fixedNow }

	_, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)
	store.mu.Lock()
	firstGets := store.getCalls
	store.mu.Unlock()
	assert.Equal(t, 1, firstGets)

	// Advance the clock past the TTL.
	cache.now = func() time.Time { return fixedNow.Add(2 * time.Second) }
	_, err = cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)
	store.mu.Lock()
	secondGets := store.getCalls
	store.mu.Unlock()
	assert.Equal(t, 2, secondGets, "a call after the TTL window must re-fetch from the registry")
}

func TestConfigureViWatermarkCache_DisabledIsNoop(t *testing.T) {
	setViWatermarkCache(nil)
	err := ConfigureViWatermarkCache(nil, nil, nil, false, time.Second, nil)
	require.NoError(t, err)
	assert.Nil(t, getViWatermarkCache())
}

func TestConfigureViWatermarkCache_GenericBackendInstallsRawBackedCache(t *testing.T) {
	setViWatermarkCache(nil)
	t.Cleanup(func() { setViWatermarkCache(nil) })
	rawR, rawW := newLocalRawBackend(t)

	err := ConfigureViWatermarkCache(nil, rawR, rawW, true, time.Second, nil)
	require.NoError(t, err)

	cache := getViWatermarkCache()
	require.NotNil(t, cache, "generic rawR/rawW path must configure a cache")
	_, ok := cache.store.(*rawObjectStore)
	assert.True(t, ok, "expected the generic path to install a *rawObjectStore-backed cache, got %T", cache.store)
}

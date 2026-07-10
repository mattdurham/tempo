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
		blockpack.TriggerConfig{Threshold: 1, WindowSeconds: 3600, LeaseTTLSeconds: 1800},
	)
	require.NoError(t, err)
	require.True(t, result.ShouldBackfill)
	require.NoError(t, registry.UpdateWatermark(
		context.Background(), tenant, result.Entry.ColumnHash, result.Entry.ColumnType, 500, 100, 1000, false,
	))
}

func TestViWatermarkCache_ReturnsTriggeredColumnsOnly(t *testing.T) {
	store := newCountingObjectStore()
	seedViWatermarkEntry(t, store, "tenant-a", "span.custom.attr")

	cache := newViWatermarkCache(store, time.Minute)
	wm, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)

	require.Contains(t, wm, "span.custom.attr")
	assert.True(t, wm["span.custom.attr"].Triggered)
	assert.Equal(t, uint64(500), wm["span.custom.attr"].WatermarkSec)
}

func TestViWatermarkCache_EmptyRegistryReturnsEmptyMap(t *testing.T) {
	store := newCountingObjectStore()
	cache := newViWatermarkCache(store, time.Minute)

	wm, err := cache.WatermarksFor(context.Background(), "tenant-a")
	require.NoError(t, err)
	assert.Empty(t, wm)
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

	cache := newViWatermarkCache(store, time.Minute)

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
	cache := newViWatermarkCache(store, time.Second)
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

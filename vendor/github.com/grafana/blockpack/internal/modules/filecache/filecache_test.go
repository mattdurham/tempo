package filecache_test

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/modules/filecache"
)

func openTestCache(t *testing.T, maxBytes int64) *filecache.FileCache {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache.db")
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: maxBytes, Path: path})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDisabledCacheIsNil(t *testing.T) {
	c, err := filecache.Open(filecache.Config{Enabled: false})
	require.NoError(t, err)
	require.Nil(t, c)
}

func TestNilCacheFallsThrough(t *testing.T) {
	var c *filecache.FileCache
	called := false
	val, err := c.GetOrFetch("key", func() ([]byte, error) {
		called = true
		return []byte("value"), nil
	})
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, []byte("value"), val)
}

func TestGetMissReturnsNil(t *testing.T) {
	c := openTestCache(t, 1<<20)
	val, ok, err := c.Get("noexist")
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, val)
}

func TestPutAndGet(t *testing.T) {
	c := openTestCache(t, 1<<20)
	require.NoError(t, c.Put("k1", []byte("hello")))

	val, ok, err := c.Get("k1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("hello"), val)
}

func TestPutIsIdempotent(t *testing.T) {
	c := openTestCache(t, 1<<20)
	require.NoError(t, c.Put("k", []byte("first")))
	require.NoError(t, c.Put("k", []byte("second"))) // should no-op

	val, _, _ := c.Get("k")
	require.Equal(t, []byte("first"), val)
}

func TestFIFOEviction(t *testing.T) {
	// Allow 2.5 entries of 10 bytes each (key 2 bytes + value 8 bytes = 10 bytes).
	// With maxBytes=25, adding a third entry triggers bulk eviction targeting
	// curBytes ≤ 25×90%−10 = 12. Evicting k1 (oldest, 10 bytes) brings curBytes
	// to 10 ≤ 12 — one eviction suffices, leaving k2 intact.
	maxBytes := int64(25)
	c := openTestCache(t, maxBytes)

	require.NoError(t, c.Put("k1", []byte("value001")))
	require.NoError(t, c.Put("k2", []byte("value002")))
	// Third entry should evict k1 (oldest) and leave k2 intact.
	require.NoError(t, c.Put("k3", []byte("value003")))

	_, ok1, _ := c.Get("k1")
	_, ok2, _ := c.Get("k2")
	_, ok3, _ := c.Get("k3")

	assert.False(t, ok1, "k1 should have been evicted")
	assert.True(t, ok2, "k2 should still be present")
	assert.True(t, ok3, "k3 should be present")
}

func TestOversizedEntrySkipped(t *testing.T) {
	c := openTestCache(t, 5) // only 5 bytes total
	err := c.Put("k", []byte("toolarge"))
	require.NoError(t, err) // no-op, not an error

	_, ok, _ := c.Get("k")
	assert.False(t, ok)
}

func TestGetOrFetchCachesResult(t *testing.T) {
	c := openTestCache(t, 1<<20)
	calls := 0
	for range 3 {
		val, err := c.GetOrFetch("key", func() ([]byte, error) {
			calls++
			return []byte("data"), nil
		})
		require.NoError(t, err)
		require.Equal(t, []byte("data"), val)
	}
	assert.Equal(t, 1, calls, "fetch should be called exactly once")
}

func TestGetOrFetchSingleflight(t *testing.T) {
	c := openTestCache(t, 1<<20)

	var fetchCount atomic.Int64
	var wg sync.WaitGroup
	const goroutines = 50

	// Block all goroutines until they're all ready.
	start := make(chan struct{})

	for range goroutines {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			_, err := c.GetOrFetch("shared-key", func() ([]byte, error) {
				fetchCount.Add(1)
				return []byte("result"), nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	close(start)
	wg.Wait()

	// All 50 goroutines start simultaneously. The first triggers one fetch;
	// all others that arrive while the fetch is in-flight are coalesced by
	// singleflight. Once the result is cached, any late arrivals find a hit.
	// Exactly one fetch should occur.
	assert.Equal(t, int64(1), fetchCount.Load(), "singleflight should perform exactly one underlying fetch")
}

func TestGetOrFetchIndependentCopies(t *testing.T) {
	c := openTestCache(t, 1<<20)

	v1, _ := c.GetOrFetch("k", func() ([]byte, error) { return []byte("original"), nil })
	v2, _ := c.GetOrFetch("k", func() ([]byte, error) { return nil, fmt.Errorf("should not be called") })

	// Mutating v1 must not affect v2.
	v1[0] = 'X'
	assert.Equal(t, byte('o'), v2[0], "copies must be independent")
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")

	c1, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: path})
	require.NoError(t, err)
	require.NoError(t, c1.Put("persistent", []byte("data")))
	require.NoError(t, c1.Close())

	c2, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: path})
	require.NoError(t, err)
	defer func() { _ = c2.Close() }()

	val, ok, err := c2.Get("persistent")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("data"), val)
}

func TestGetOrFetchFetchErrorNotCached(t *testing.T) {
	c := openTestCache(t, 1<<20)
	calls := 0

	for range 2 {
		_, err := c.GetOrFetch("failing", func() ([]byte, error) {
			calls++
			return nil, fmt.Errorf("transient error")
		})
		require.Error(t, err)
	}

	assert.Equal(t, 2, calls, "failed fetches must not be cached — each call must retry")
}

func TestCloseNilIsNoop(t *testing.T) {
	var c *filecache.FileCache
	require.NoError(t, c.Close())
}

func TestOpenInvalidConfig(t *testing.T) {
	_, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: -1, Path: "/tmp/x.db"})
	require.Error(t, err)

	_, err = filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: ""})
	require.Error(t, err)

	// Non-existent directory.
	_, err = filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: "/does/not/exist/cache.db"})
	require.Error(t, err)
}

func TestMultipleKeyTypes(t *testing.T) {
	c := openTestCache(t, 1<<20)

	keys := []string{
		"/data/block.blockpack/footer",
		"/data/block.blockpack/header",
		"/data/block.blockpack/metadata",
		"/data/block.blockpack/compact",
		"/data/block.blockpack/block/0",
		"/data/block.blockpack/block/1",
	}

	for i, k := range keys {
		require.NoError(t, c.Put(k, []byte(fmt.Sprintf("value-%d", i))))
	}

	for i, k := range keys {
		val, ok, err := c.Get(k)
		require.NoError(t, err)
		require.True(t, ok, "key %s should be present", k)
		require.Equal(t, []byte(fmt.Sprintf("value-%d", i)), val)
	}
}

func TestConcurrentPuts(t *testing.T) {
	c := openTestCache(t, 1<<20)
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", n)
			if err := c.Put(key, []byte(fmt.Sprintf("value-%d", n))); err != nil {
				errCh <- fmt.Errorf("put %d: %w", n, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestCacheDir(t *testing.T) {
	// Verify the cache directory is created at the specified path.
	// The file-per-entry implementation creates the directory automatically
	// (including any missing parents), so Open always succeeds for valid paths.
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "cache")

	// Open with a nested path — directory is created automatically.
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: path})
	require.NoError(t, err)
	_ = c.Close()
	info, err := os.Stat(path)
	require.NoError(t, err, "cache directory should exist")
	require.True(t, info.IsDir(), "cache path should be a directory")
}

// --- File-per-entry specific tests ---

func TestFileLayoutUsesSha256Subdirs(t *testing.T) {
	// Verify the cache creates 2-char hex subdirectories.
	dir := t.TempDir()
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	_, err = c.GetOrFetch("mykey", func() ([]byte, error) { return []byte("value"), nil })
	require.NoError(t, err)

	// Find the written .bin file — it should be under a 2-char subdirectory.
	var found []string
	require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if filepath.Ext(path) == ".bin" {
			found = append(found, path)
		}
		return nil
	}))
	require.Len(t, found, 1)
	// Parent directory name should be exactly 2 hex chars.
	subdir := filepath.Base(filepath.Dir(found[0]))
	require.Len(t, subdir, 2)
	_, parseErr := hex.DecodeString(subdir)
	require.NoError(t, parseErr, "subdir should be valid hex: %s", subdir)
}

func TestSurvivesReopen(t *testing.T) {
	// Verify entries survive a full Close + Open cycle (startup load).
	dir := t.TempDir()

	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: dir})
	require.NoError(t, err)

	keys := []string{"a", "b", "c", "some/longer/key"}
	for _, k := range keys {
		require.NoError(t, c.Put(k, []byte("val-"+k)))
	}
	require.NoError(t, c.Close())

	// Reopen — should rebuild from disk.
	c2, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, c2.Close()) }()

	for _, k := range keys {
		val, ok, err := c2.Get(k)
		require.NoError(t, err)
		assert.True(t, ok, "key %q should survive reopen", k)
		assert.Equal(t, []byte("val-"+k), val)
	}
}

func TestBulkEvictionTo90Percent(t *testing.T) {
	// Verify that when eviction triggers, it frees enough space to bring
	// curBytes+needed to ≤ 90% of maxBytes — not just the bare minimum.
	//
	// Setup: maxBytes=100, 10 entries × 9 bytes = 90 bytes (90% full).
	// Add one entry of 17 bytes (key="new"=3 + value=14): 90+17=107 > 100 → eviction needed.
	// Target: curBytes ≤ 90%×100 − 17 = 73.
	// At 9 bytes/entry, evict 2 entries (18 bytes → curBytes=72 ≤ 73).
	// With 2+ evictions, 8 or fewer original entries survive.
	const maxBytes = 100
	dir := t.TempDir()
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: maxBytes, Path: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	// 10 entries × 9 bytes each (key=1 char "0"–"9", value=8 bytes) = 90 bytes.
	for i := range 10 {
		require.NoError(t, c.Put(fmt.Sprintf("%d", i), make([]byte, 8)))
	}

	// Add a 17-byte entry (key="new"=3 + val=14): triggers bulk eviction targeting curBytes ≤ 73.
	// At 9 bytes/entry, we evict ≥ 2 entries (18 bytes → curBytes=72 ≤ 73).
	require.NoError(t, c.Put("new", make([]byte, 14)))

	// Count how many of the original 10 survive.
	var survived int
	for i := range 10 {
		if v, ok, _ := c.Get(fmt.Sprintf("%d", i)); ok && v != nil {
			survived++
		}
	}
	// Bulk eviction should have removed at least 2 entries.
	assert.LessOrEqual(t, survived, 8, "bulk eviction should remove ≥ 2 old entries, got %d surviving", survived)
	// The new entry should be present.
	v, ok, _ := c.Get("new")
	assert.True(t, ok, "newly added entry must be present after eviction")
	assert.Equal(t, make([]byte, 14), v)
}

func TestConcurrentPutsAndGetsNoRace(t *testing.T) {
	// Stress test: concurrent puts and gets for same or different keys.
	dir := t.TempDir()
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 10 << 20, Path: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	const goroutines = 100
	const keys = 50
	var wg sync.WaitGroup
	start := make(chan struct{})

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			key := fmt.Sprintf("key-%d", g%keys)
			val, _ := c.GetOrFetch(key, func() ([]byte, error) {
				return []byte("data-" + key), nil
			})
			assert.Equal(t, []byte("data-"+key), val)
		}(g)
	}
	close(start)
	wg.Wait()
}

func TestCorruptFileSkippedOnLoad(t *testing.T) {
	// Verify corrupt files are silently removed during startup load.
	dir := t.TempDir()
	subdir := filepath.Join(dir, "ab")
	require.NoError(t, os.MkdirAll(subdir, 0o700))

	// Write a corrupt file.
	corrupt := filepath.Join(subdir, "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.bin")
	require.NoError(t, os.WriteFile(corrupt, []byte("not a valid cache entry"), 0o600))

	// Open should succeed and skip the corrupt file.
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	// Corrupt file should be deleted.
	_, statErr := os.Stat(corrupt)
	assert.True(t, os.IsNotExist(statErr), "corrupt file should be removed on load")
}

func TestConcurrentSameKeyPutNoCorruption(t *testing.T) {
	// Verify that concurrent Put calls for the same key don't corrupt the cache:
	// the loser must not delete the winner's file, leaving a stale index entry.
	dir := t.TempDir()
	c, err := filecache.Open(filecache.Config{Enabled: true, MaxBytes: 1 << 20, Path: dir})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	const goroutines = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = c.Put("shared", []byte("value"))
		}()
	}
	close(start)
	wg.Wait()

	// After all concurrent Puts, the key must be present and readable.
	val, ok, err := c.Get("shared")
	require.NoError(t, err)
	assert.True(t, ok, "shared key must be present after concurrent Puts")
	assert.Equal(t, []byte("value"), val)
}

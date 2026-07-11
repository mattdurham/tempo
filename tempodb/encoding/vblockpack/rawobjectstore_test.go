package vblockpack

// rawobjectstore_test.go — Track A (backend-agnostic VI/cube usage-recording
// backfill, plan.md §11) unit tests for rawobjectstore.go's content-hash+mutex
// CAS emulation over backend.RawReader/RawWriter (Local/Azure). Uses a real
// local.NewBackend (t.TempDir()) so the tests exercise the real WriteAtomic
// path, not a hand-rolled fake.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/stretchr/testify/require"
)

// casObjectStore is the minimal interface both rawObjectStore and
// rawCubeObjectStore satisfy — used by the concurrent-writer test helper so
// it can drive either implementation identically.
type casObjectStore interface {
	Get(ctx context.Context, path string) ([]byte, string, error)
	ConditionalPut(ctx context.Context, path string, data []byte, etag string) error
}

// appendUniqueIDWithRetry mirrors blockpack viusage/registry.go's real
// updateEntryWithRetry/Add shape (Load -> mutate -> ConditionalPut ->
// retry-on-conflict, exponential backoff): loads the current JSON array
// stored at key (treating a not-yet-existing key as an empty array), appends
// id, and retries on ErrConflict until it succeeds or maxRetries is
// exhausted. mutate re-derives from freshly-loaded state on every retry
// attempt, exactly like the real registry's own retry loop.
func appendUniqueIDWithRetry(ctx context.Context, store casObjectStore, key, id string, notFoundErr error) error {
	const maxRetries = 20
	backoff := time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		var entries []string
		etag := ""
		data, curEtag, err := store.Get(ctx, key)
		if err != nil {
			if !errors.Is(err, notFoundErr) {
				return err
			}
		} else {
			etag = curEtag
			if unmarshalErr := json.Unmarshal(data, &entries); unmarshalErr != nil {
				return unmarshalErr
			}
		}

		entries = append(entries, id)
		newData, err := json.Marshal(entries)
		if err != nil {
			return err
		}

		if err := store.ConditionalPut(ctx, key, newData, etag); err != nil {
			if errors.Is(err, blockpack.ErrConflict) || errors.Is(err, blockpack.CubeErrConflict) {
				time.Sleep(backoff)
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("appendUniqueIDWithRetry: exceeded %d retries for key %s", maxRetries, key)
}

func newLocalRawBackend(t *testing.T) (backend.RawReader, backend.RawWriter) {
	t.Helper()
	rawR, rawW, _, err := local.New(&local.Config{Path: t.TempDir()})
	require.NoError(t, err)
	return rawR, rawW
}

func TestRawObjectStore_ConditionalPut_RejectsStaleETag(t *testing.T) {
	ctx := context.Background()
	rawR, rawW := newLocalRawBackend(t)
	store := newRawObjectStore(rawR, rawW)

	key := "t/viusage/index.json"
	dataA := []byte(`{"entries":["a"]}`)
	dataB := []byte(`{"entries":["a","b"]}`)
	dataC := []byte(`{"entries":["a","c"]}`)

	// First create: no existing object, empty etag -> succeeds.
	require.NoError(t, store.ConditionalPut(ctx, key, dataA, ""))

	data, etagA, err := store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, dataA, data)
	require.NotEmpty(t, etagA)

	// Correct etag -> succeeds, content updates to dataB.
	require.NoError(t, store.ConditionalPut(ctx, key, dataB, etagA))
	data, _, err = store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, dataB, data)

	// Core assertion: etagA is now stale (content moved to dataB). A write
	// against the stale etag must be rejected with blockpack.ErrConflict, and
	// the stored content must remain dataB, never dataC.
	err = store.ConditionalPut(ctx, key, dataC, etagA)
	require.Error(t, err)
	require.True(t, errors.Is(err, blockpack.ErrConflict), "expected ErrConflict, got %v", err)

	data, _, err = store.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, dataB, data, "stale conditional put must not have overwritten the stored content")
}

// TestRawObjectStore_ConditionalPut_ConcurrentWritersNoLostUpdates proves the
// pathMutexMap's per-key lock is doing real work: N goroutines each running a
// real Load->mutate->ConditionalPut->retry-on-ErrConflict loop against the
// SAME key must all converge, with zero lost updates, on a final object
// containing exactly N unique entries. Run with -race to also catch any data
// race on the shared underlying store.
func TestRawObjectStore_ConditionalPut_ConcurrentWritersNoLostUpdates(t *testing.T) {
	ctx := context.Background()
	rawR, rawW := newLocalRawBackend(t)
	store := newRawObjectStore(rawR, rawW)

	const n = 20
	key := "t/viusage/index.json"

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = appendUniqueIDWithRetry(ctx, store, key, fmt.Sprintf("writer-%d", i), blockpack.ErrNotFound)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "writer %d failed", i)
	}

	data, _, err := store.Get(ctx, key)
	require.NoError(t, err)
	var final []string
	require.NoError(t, json.Unmarshal(data, &final))

	require.Len(t, final, n, "expected exactly %d entries, no lost updates", n)
	seen := make(map[string]bool, n)
	for _, id := range final {
		require.False(t, seen[id], "duplicate entry %q", id)
		seen[id] = true
	}
}

// TestRawCubeObjectStore_UsesIndependentSentinelErrors proves
// rawCubeObjectStore.ConditionalPut's conflict returns blockpack.CubeErrConflict
// (not blockpack.ErrConflict) — the two registries are independent types (R1)
// with their own conflict sentinels.
func TestRawCubeObjectStore_UsesIndependentSentinelErrors(t *testing.T) {
	ctx := context.Background()
	rawR, rawW := newLocalRawBackend(t)
	store := newRawCubeObjectStore(rawR, rawW)

	key := "t/cubes/index.json"
	dataA := []byte(`{"cubes":["a"]}`)
	dataB := []byte(`{"cubes":["a","b"]}`)

	require.NoError(t, store.ConditionalPut(ctx, key, dataA, ""))
	_, etagA, err := store.Get(ctx, key)
	require.NoError(t, err)

	require.NoError(t, store.ConditionalPut(ctx, key, dataB, etagA))

	// etagA is now stale; the resulting conflict must be blockpack.CubeErrConflict,
	// NOT blockpack.ErrConflict.
	err = store.ConditionalPut(ctx, key, []byte(`{"cubes":["a","c"]}`), etagA)
	require.Error(t, err)
	require.True(t, errors.Is(err, blockpack.CubeErrConflict), "expected CubeErrConflict, got %v", err)
	require.False(t, errors.Is(err, blockpack.ErrConflict), "must not be viusage's ErrConflict")
}

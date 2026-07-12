package vblockpack

// content_cache_flight_isolation_test.go — task #200 regression.
//
// cachingStore.readAtCtx (content_cache.go) threads a real per-query ctx down to the
// inner store on a cache miss, but the singleflight flightKey ("key|off|len") has no ctx
// component and is shared by every concurrent caller requesting the same range — the
// documented common case for candidate trace-by-ID index files (NOTE-VI-076): many
// overlapping blocks resolve to the SAME compacted index file, so concurrent trace-by-ID
// queries frequently collide on the exact same (key, off, len).
//
// Before this fix, the FIRST caller to arrive for a given flightKey became the
// singleflight "leader," and the leader's OWN ctx governed the actual object-store I/O for
// EVERY caller sharing that key: if the leader's ctx was cancelled (a totally unrelated
// request timing out, or the client disconnecting), every other concurrent waiter for the
// same range — including one whose own ctx was nowhere near expiring — incorrectly
// received that same cancellation error instead of its own real result.
//
// This test proves two concurrent callers sharing the same flightKey — one whose ctx gets
// cancelled while the shared flight is in flight, one with a live ctx that is never
// cancelled — resolve independently: the live caller must succeed with the real data,
// completely unaffected by the other caller's unrelated cancellation.
//
// Mutation-verified: reverting readAtCtx to thread the calling goroutine's own ctx into
// the shared singleflight flight body (the pre-fix behavior) makes TestReadAtCtx_
// UnrelatedPeerCancellation_DoesNotFailLiveWaiter fail with the live waiter incorrectly
// observing context.Canceled; restoring the fix makes it pass.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flightIsolationStore is a valueIndexStore implementing ctxAwareStore whose readAtCtx
// blocks until either the caller-supplied ctx it was actually invoked with is cancelled,
// or the test releases it — modeling a real object-store ranged read that can be aborted
// mid-flight by context cancellation. Because singleflight guarantees exactly one
// underlying call per deduplicated flight (regardless of how many callers share the key),
// readAtCtx is invoked at most once per sub-test; entered is closed exactly once to signal
// the test that the flight has started and registered.
type flightIsolationStore struct {
	data    []byte
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *flightIsolationStore) List(context.Context, string) ([]string, error) { return nil, nil }
func (s *flightIsolationStore) Get(context.Context, string) ([]byte, error)    { return nil, nil }
func (s *flightIsolationStore) Size(string) (int64, error)                     { return int64(len(s.data)), nil }
func (s *flightIsolationStore) ReadAt(string, []byte, int64) (int, error)      { return 0, nil }

func (s *flightIsolationStore) sizeCtx(_ context.Context, _ string) (int64, error) {
	return int64(len(s.data)), nil
}

// readAtCtx is invoked with whatever ctx the singleflight flight body was closed over — the
// exact ctx this test cares about observing. Pre-fix, that is the LEADER caller's own ctx;
// post-fix, it is always context.Background(), detached from any single waiter.
func (s *flightIsolationStore) readAtCtx(ctx context.Context, _ string, p []byte, off int64) (int, error) {
	s.calls.Add(1)
	close(s.entered)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.release:
		return copy(p, s.data[off:off+int64(len(p))]), nil
	}
}

var _ ctxAwareStore = (*flightIsolationStore)(nil)
var _ valueIndexStore = (*flightIsolationStore)(nil)

// TestReadAtCtx_UnrelatedPeerCancellation_DoesNotFailLiveWaiter is the direct regression
// test for task #200: two concurrent readAtCtx calls sharing the exact same flightKey
// (same key, offset, length) must resolve independently. The first caller (the
// singleflight leader) has its ctx cancelled WHILE the shared flight is in flight; the
// second caller's ctx is never cancelled. The live caller must succeed with the real data
// regardless of the leader's cancellation, and the underlying store must be hit exactly
// once (dedup must still work — this fix must not reintroduce issue #475's N-way
// redundant-download problem).
func TestReadAtCtx_UnrelatedPeerCancellation_DoesNotFailLiveWaiter(t *testing.T) {
	data := []byte("0123456789ABCDEF")
	inner := &flightIsolationStore{
		data:    data,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	cs, ok := newCachingStore(inner, 1<<20).(*cachingStore)
	require.True(t, ok, "newCachingStore must return a *cachingStore for a positive budget")

	const key = "shared-candidate-index-file"
	const off = int64(0)
	const readLen = 4

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	liveCtx := context.Background()

	type result struct {
		buf []byte
		n   int
		err error
	}
	leaderResCh := make(chan result, 1)
	liveResCh := make(chan result, 1)

	// Launch the leader first and wait for its underlying store call to actually start
	// (== registered with the singleflight group and now blocked), guaranteeing it — not
	// the live caller below — becomes the leader for this flightKey.
	leaderStarted := make(chan struct{})
	go func() {
		close(leaderStarted)
		buf := make([]byte, readLen)
		n, err := cs.readAtCtx(leaderCtx, key, buf, off)
		leaderResCh <- result{buf, n, err}
	}()
	<-leaderStarted
	<-inner.entered

	// Launch the live-ctx caller for the exact same (key, off, len). Because the leader's
	// flight is still in flight (blocked on inner.release), this MUST join as a
	// singleflight waiter on the same flight rather than starting an independent fetch.
	liveStarted := make(chan struct{})
	go func() {
		close(liveStarted)
		buf := make([]byte, readLen)
		n, err := cs.readAtCtx(liveCtx, key, buf, off)
		liveResCh <- result{buf, n, err}
	}()
	<-liveStarted
	// The live caller has nothing to block on before reaching the singleflight group
	// (a cache miss check + map lookup, no I/O) -- give its goroutine a moment to actually
	// register before the leader is cancelled below.
	time.Sleep(20 * time.Millisecond)

	// Cancel the LEADER's own ctx while the shared flight is still in flight. Pre-fix,
	// this ctx governs the shared flight body itself, so cancelling it aborts the flight
	// for EVERY waiter, including the live one. Post-fix, the shared flight body is
	// detached (context.Background()) and only this caller's own wait is affected.
	cancelLeader()
	time.Sleep(20 * time.Millisecond)

	// Let the underlying store's read actually complete. Pre-fix this is a no-op (the
	// flight already finished with a cancellation error above). Post-fix, the detached
	// flight is still blocked on this release and needs it to deliver real data to the
	// live waiter.
	close(inner.release)

	leaderRes := <-leaderResCh
	liveRes := <-liveResCh

	// The leader cancelled its OWN request -- an error here is correct and expected.
	require.Error(t, leaderRes.err)

	// The live caller's own ctx was never cancelled. It must succeed with the real data,
	// completely unaffected by the leader's unrelated cancellation.
	require.NoError(t, liveRes.err,
		"an unrelated peer's cancellation must not fail this caller's own live-ctx read")
	assert.Equal(t, readLen, liveRes.n)
	assert.Equal(t, data[:readLen], liveRes.buf)

	// The dedup this cache exists for (issue #475) must still hold: exactly one
	// underlying store call for both callers sharing the same flightKey, not two.
	assert.Equal(t, int32(1), inner.calls.Load(),
		"two callers sharing the same flightKey must still collapse to one underlying fetch")
}

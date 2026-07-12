package vblockpack

// content_cache_ctx_test.go — task #199 follow-up: verifies bindQueryCtx/ctxAwareStore
// actually thread a real per-query ctx down to Size/ReadAt instead of the previous
// unconditional context.Background(), and that a store which does NOT implement the
// optional ctxAwareStore capability (e.g. a plain test fake) still works unchanged via
// sizeWithCtx/readAtWithCtx's fallback to the plain, ctx-less Size/ReadAt methods.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ctxCapturingStore is a valueIndexStore that implements ctxAwareStore and records the
// ctx it was actually called with on each sizeCtx/readAtCtx call, so a test can assert
// bindQueryCtx propagates the CALLER's ctx all the way down rather than substituting
// context.Background() (the task #199 bug).
type ctxCapturingStore struct {
	sizeCtxSeen   context.Context
	readAtCtxSeen context.Context
}

func (s *ctxCapturingStore) List(context.Context, string) ([]string, error) { return nil, nil }
func (s *ctxCapturingStore) Get(context.Context, string) ([]byte, error)    { return nil, nil }

// Size/ReadAt must never be hit directly in this test (bindQueryCtx should always prefer
// the ctxAwareStore capability) — returning an error makes an accidental fallback to
// these fail loudly instead of silently passing.
func (s *ctxCapturingStore) Size(string) (int64, error) {
	return 0, errors.New("ctxCapturingStore.Size called directly: bindQueryCtx should have used sizeCtx")
}

func (s *ctxCapturingStore) ReadAt(string, []byte, int64) (int, error) {
	return 0, errors.New("ctxCapturingStore.ReadAt called directly: bindQueryCtx should have used readAtCtx")
}

func (s *ctxCapturingStore) sizeCtx(ctx context.Context, _ string) (int64, error) {
	s.sizeCtxSeen = ctx
	return 42, nil
}

func (s *ctxCapturingStore) readAtCtx(ctx context.Context, _ string, p []byte, _ int64) (int, error) {
	s.readAtCtxSeen = ctx
	return len(p), nil
}

var _ ctxAwareStore = (*ctxCapturingStore)(nil)

type ctxKeyT string

const ctxProbeKey ctxKeyT = "probe"

// TestBindQueryCtx_PropagatesRealCtxToSizeAndReadAt is the direct regression test for
// task #199's tempo-side fix: minioVIStore.Size/ReadAt (and rawFileStore's) previously
// always used context.Background() regardless of the caller's own query deadline, so an
// in-flight object-store call could never be cancelled by a real timeout. bindQueryCtx
// must deliver the EXACT ctx passed to it down to the underlying store's ctxAwareStore
// methods, not a substitute.
func TestBindQueryCtx_PropagatesRealCtxToSizeAndReadAt(t *testing.T) {
	inner := &ctxCapturingStore{}
	queryCtx := context.WithValue(context.Background(), ctxProbeKey, "the-real-query-ctx")

	bound := bindQueryCtx(queryCtx, inner)

	_, err := bound.Size("some-key")
	require.NoError(t, err)
	require.NotNil(t, inner.sizeCtxSeen)
	assert.Equal(t, "the-real-query-ctx", inner.sizeCtxSeen.Value(ctxProbeKey),
		"Size must thread the caller's own ctx down to sizeCtx, not context.Background()")

	_, err = bound.ReadAt("some-key", make([]byte, 4), 0)
	require.NoError(t, err)
	require.NotNil(t, inner.readAtCtxSeen)
	assert.Equal(t, "the-real-query-ctx", inner.readAtCtxSeen.Value(ctxProbeKey),
		"ReadAt must thread the caller's own ctx down to readAtCtx, not context.Background()")
}

// TestBindQueryCtx_CancelledCtxObservableInSizeCtx proves the plumbing actually carries a
// cancellation signal end to end: a ctx cancelled before the call still resolves (this
// fake does no real I/O), but ctx.Err() is non-nil exactly where a real
// minioVIStore.sizeCtx/readAtCtx would check it before/while issuing the network call —
// confirming the bound ctx is genuinely live, not a disconnected copy.
func TestBindQueryCtx_CancelledCtxObservableInSizeCtx(t *testing.T) {
	inner := &ctxCapturingStore{}
	queryCtx, cancel := context.WithCancel(context.Background())
	cancel()

	bound := bindQueryCtx(queryCtx, inner)
	_, err := bound.Size("some-key")
	require.NoError(t, err) // the fake itself doesn't fail on a cancelled ctx
	require.NotNil(t, inner.sizeCtxSeen)
	assert.Error(t, inner.sizeCtxSeen.Err(), "sizeCtx must observe the caller's cancellation, not context.Background()")
}

// TestSizeWithCtx_FallsBackForNonCtxAwareStore ensures a store that does NOT implement
// ctxAwareStore (e.g. countingStore, used throughout this package's other tests) keeps
// working unchanged via the plain, ctx-less Size/ReadAt methods -- the task #199 fix must
// never regress a store lacking the new optional capability.
func TestSizeWithCtx_FallsBackForNonCtxAwareStore(t *testing.T) {
	inner := newCountingStore()
	inner.put("k", []byte("hello"))

	var _ valueIndexStore = inner
	_, isCtxAware := (valueIndexStore)(inner).(ctxAwareStore)
	require.False(t, isCtxAware, "countingStore must NOT implement ctxAwareStore for this fallback test to be meaningful")

	sz, err := sizeWithCtx(context.Background(), inner, "k")
	require.NoError(t, err)
	assert.Equal(t, int64(0), sz) // countingStore.Size is a stub returning 0, nil

	buf := make([]byte, 2)
	n, err := readAtWithCtx(context.Background(), inner, "k", buf, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, n) // countingStore.ReadAt is a stub returning 0, nil
}

// TestMinioVIStore_And_RawFileStore_SatisfyCtxAwareStore is a compile-time-shaped
// regression guard: both production store implementations must keep exposing the
// package-private ctxAwareStore capability so bindQueryCtx's real cancellation benefit
// actually reaches production traffic, not just tests.
func TestMinioVIStore_And_RawFileStore_SatisfyCtxAwareStore(t *testing.T) {
	var _ ctxAwareStore = (*minioVIStore)(nil)
	var _ ctxAwareStore = (*rawFileStore)(nil)
	var _ ctxAwareStore = (*cachingStore)(nil)
}

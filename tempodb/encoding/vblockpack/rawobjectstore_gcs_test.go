package vblockpack

// rawobjectstore_gcs_test.go — Track A (plan.md §7, §11 Test 4) GCS-native-path
// tests for rawobjectstore_gcs.go. Uses a fake versionedRawReaderWriter whose
// WriteVersioned returns a raw *googleapi.Error{Code: 412} — mirroring the
// real, verified gap in tempo's own gcs.go: its WriteVersioned does NOT
// translate a 412 precondition failure into backend.ErrVersionDoesNotMatch
// (unlike s3.go/azure.go, which do). gcsCASCore.conditionalPut must detect the
// conflict directly via googleapi.Error.Code == 412, not solely via
// errors.Is(err, backend.ErrVersionDoesNotMatch).

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/uuid"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

// fakeVersionedRawReaderWriter is a minimal, in-memory versionedRawReaderWriter
// fake for exercising gcsCASCore without a live GCS backend.
type fakeVersionedRawReaderWriter struct {
	data    map[string][]byte
	version map[string]backend.Version
	// writeVersionedErr, when set, is returned unconditionally by
	// WriteVersioned (simulating gcs.go's own precondition-failure path,
	// which returns the raw client error rather than a mapped sentinel).
	writeVersionedErr error
}

func newFakeVersionedRawReaderWriter() *fakeVersionedRawReaderWriter {
	return &fakeVersionedRawReaderWriter{
		data:    make(map[string][]byte),
		version: make(map[string]backend.Version),
	}
}

func (f *fakeVersionedRawReaderWriter) fullKey(name string, keypath backend.KeyPath) string {
	key := ""
	for _, p := range keypath {
		key += p + "/"
	}
	return key + name
}

func (f *fakeVersionedRawReaderWriter) Read(_ context.Context, name string, keypath backend.KeyPath, _ *backend.CacheInfo) (io.ReadCloser, int64, error) {
	key := f.fullKey(name, keypath)
	data, ok := f.data[key]
	if !ok {
		return nil, -1, backend.ErrDoesNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
}

func (f *fakeVersionedRawReaderWriter) List(context.Context, backend.KeyPath) ([]string, error) {
	return nil, nil
}

func (f *fakeVersionedRawReaderWriter) ListBlocks(context.Context, string) ([]uuid.UUID, []uuid.UUID, error) {
	return nil, nil, nil
}

func (f *fakeVersionedRawReaderWriter) Find(context.Context, backend.KeyPath, backend.FindFunc) error {
	return nil
}

func (f *fakeVersionedRawReaderWriter) ReadRange(context.Context, string, backend.KeyPath, uint64, []byte, *backend.CacheInfo) error {
	return nil
}
func (f *fakeVersionedRawReaderWriter) Shutdown() {}

func (f *fakeVersionedRawReaderWriter) WriteVersioned(
	_ context.Context, name string, keypath backend.KeyPath, data io.Reader, _ int64, version backend.Version,
) (backend.Version, error) {
	if f.writeVersionedErr != nil {
		return "", f.writeVersionedErr
	}
	key := f.fullKey(name, keypath)
	b, err := io.ReadAll(data)
	if err != nil {
		return "", err
	}
	f.data[key] = b
	newVersion := backend.Version("1")
	if cur, ok := f.version[key]; ok {
		_ = cur
		newVersion = backend.Version("2")
	}
	_ = version
	f.version[key] = newVersion
	return newVersion, nil
}

func (f *fakeVersionedRawReaderWriter) ReadVersioned(_ context.Context, name string, keypath backend.KeyPath) (io.ReadCloser, backend.Version, error) {
	key := f.fullKey(name, keypath)
	data, ok := f.data[key]
	if !ok {
		return nil, "", backend.ErrDoesNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), f.version[key], nil
}

// The methods below satisfy backend.RawWriter (Write/Append/CloseAppend/Delete),
// alongside RawReader and WriteVersioned/ReadVersioned above -- exactly the same
// method set azure.Azure exposes on one concrete type (task #151), needed so this
// fake can stand in for "any real backend whose concrete type structurally
// satisfies versionedRawReaderWriter but is NOT actually GCS" in
// TestNewObjectStoreForBackend_NonGCSPackageTypeFallsBackToRawPath below.

func (f *fakeVersionedRawReaderWriter) Write(_ context.Context, name string, keypath backend.KeyPath, data io.Reader, _ int64, _ *backend.CacheInfo) error {
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	f.data[f.fullKey(name, keypath)] = b
	return nil
}

func (f *fakeVersionedRawReaderWriter) Append(context.Context, string, backend.KeyPath, backend.AppendTracker, []byte) (backend.AppendTracker, error) {
	return nil, nil
}

func (f *fakeVersionedRawReaderWriter) CloseAppend(context.Context, backend.AppendTracker) error {
	return nil
}

func (f *fakeVersionedRawReaderWriter) Delete(_ context.Context, name string, keypath backend.KeyPath, _ *backend.CacheInfo) error {
	delete(f.data, f.fullKey(name, keypath))
	return nil
}

func TestGCSObjectStore_ConditionalPut_DetectsPreconditionFailureVia412(t *testing.T) {
	ctx := context.Background()
	vrw := newFakeVersionedRawReaderWriter()
	store := newGCSObjectStore(vrw)

	key := "t/viusage/index.json"
	require.NoError(t, store.ConditionalPut(ctx, key, []byte(`{"entries":["a"]}`), ""))

	// Simulate gcs.go's own verified gap: a real precondition failure surfaces
	// as a raw *googleapi.Error{Code: 412}, NOT backend.ErrVersionDoesNotMatch.
	vrw.writeVersionedErr = &googleapi.Error{Code: 412, Message: "conditionNotMet"}

	err := store.ConditionalPut(ctx, key, []byte(`{"entries":["a","b"]}`), "some-stale-etag")
	require.Error(t, err)
	require.True(t, errors.Is(err, blockpack.ErrConflict), "expected ErrConflict despite gcs.go never populating backend.ErrVersionDoesNotMatch, got %v", err)
}

func TestGCSObjectStore_ConditionalPut_DetectsPreconditionFailureViaSentinel(t *testing.T) {
	ctx := context.Background()
	vrw := newFakeVersionedRawReaderWriter()
	store := newGCSObjectStore(vrw)

	key := "t/viusage/index.json"
	require.NoError(t, store.ConditionalPut(ctx, key, []byte(`{"entries":["a"]}`), ""))

	// A hypothetical future GCS client (or another VersionedReaderWriter
	// implementation) that DOES map to the documented sentinel must also be
	// detected correctly.
	vrw.writeVersionedErr = backend.ErrVersionDoesNotMatch

	err := store.ConditionalPut(ctx, key, []byte(`{"entries":["a","b"]}`), "some-stale-etag")
	require.Error(t, err)
	require.True(t, errors.Is(err, blockpack.ErrConflict))
}

// TestNewObjectStoreForBackend_NonGCSPackageTypeFallsBackToRawPath is the
// regression test for task #151 (CRITICAL): *azure.Azure structurally
// satisfies versionedRawReaderWriter (it has its own WriteVersioned/
// ReadVersioned, azure.go:341/382) exactly like GCS's *readerWriter does, but
// Azure's WriteVersioned is a documented, non-atomic read-then-write
// emulation (azure.go:342 "TODO use conditional if-match API") -- it has NO
// real generation-match precondition. gcsObjectStore/gcsCubeObjectStore add
// no mutex of their own (they rely entirely on WriteVersioned being atomic,
// true for GCS, false for Azure), so a structural type-assertion dispatch
// would silently route Azure through the mutex-free GCS path, losing Track
// A's concurrency-safety guarantee for Azure (the exact scenario Test 2,
// rawobjectstore_test.go, exists to catch).
//
// fakeVersionedRawReaderWriter structurally satisfies versionedRawReaderWriter
// (RawReader + WriteVersioned + ReadVersioned) exactly like azure.Azure does,
// while its concrete type lives in package vblockpack, not
// tempodb/backend/gcs -- proving dispatch is based on the backend's real
// package identity (isGCSBackend), not structural duck-typing alone.
func TestNewObjectStoreForBackend_NonGCSPackageTypeFallsBackToRawPath(t *testing.T) {
	rawR, _ := newLocalRawBackend(t)
	azureShaped := newFakeVersionedRawReaderWriter()

	store := newObjectStoreForBackend(rawR, azureShaped)
	_, ok := store.(*rawObjectStore)
	require.True(t, ok, "a non-GCS-package versionedRawReaderWriter-shaped backend must fall back to *rawObjectStore, not *gcsObjectStore; got %T", store)

	cubeStore := newCubeObjectStoreForBackend(rawR, azureShaped)
	_, ok = cubeStore.(*rawCubeObjectStore)
	require.True(t, ok, "a non-GCS-package versionedRawReaderWriter-shaped backend must fall back to *rawCubeObjectStore, not *gcsCubeObjectStore; got %T", cubeStore)
}

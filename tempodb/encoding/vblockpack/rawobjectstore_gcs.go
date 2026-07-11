package vblockpack

// rawobjectstore_gcs.go — Track A (plan.md §7, §11) GCS-native conditional-write
// path: unlike Local/Azure's content-hash+mutex emulation (rawobjectstore.go),
// GCS has a real atomic generation-match precondition via
// backend.VersionedReaderWriter.WriteVersioned (gcs.go:397-424,
// createPreconditions), so this adapter uses that native primitive directly
// instead of emulating one — a genuine correctness IMPROVEMENT over the
// pre-existing S3/Local/Azure "etag==\"\" is unconditional" gap (deliberate,
// documented asymmetry, not an oversight — see plan.md §7/§19 item 5).
//
// Known, pre-existing gap in tempo's own gcs.go (verified, NOT fixed here —
// out of scope, would need re-verification against
// modules/overrides/userconfigurable's existing GCS usage of the same
// method): WriteVersioned does NOT translate a 412 precondition-failure
// response into backend.ErrVersionDoesNotMatch (unlike s3.go/azure.go, which
// do translate their own equivalent conflict responses). This adapter works
// around that gap directly: gcsCASCore.conditionalPut detects a conflict via
// EITHER errors.Is(err, backend.ErrVersionDoesNotMatch) (the documented
// contract, satisfied by any future/other VersionedReaderWriter that honors
// it) OR a raw *googleapi.Error with Code == 412 (gcs.go's actual current
// behavior). Do not remove the googleapi.Error check without first fixing
// gcs.go itself.
//
// newObjectStoreForBackend/newCubeObjectStoreForBackend are the single
// backend-dispatch points: they check rawW's concrete type actually
// originates from tempodb/backend/gcs (isGCSBackend below — NOT a structural
// type assertion alone; see its doc comment for why: *azure.Azure also
// structurally satisfies versionedRawReaderWriter, task #151) and fall back
// to the generic rawCASCore-backed implementation otherwise. Neither is ever
// called when cfg.Backend == backend.S3 — callers gate on that before
// reaching here (S3 keeps its own, untouched
// minioObjectStore/viUsageObjectStore construction).

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	"google.golang.org/api/googleapi"
)

// gcsBackendPkgPath is tempodb/backend/gcs's own package path, used by
// isGCSBackend to disambiguate GCS from any other backend whose concrete type
// happens to structurally satisfy versionedRawReaderWriter (task #151,
// CRITICAL: *azure.Azure has its own WriteVersioned/ReadVersioned, azure.go:
// 341/382, and is therefore structurally indistinguishable from GCS's own
// *readerWriter via type assertion alone -- but Azure's WriteVersioned is a
// documented, non-atomic read-then-write emulation, azure.go:342 "TODO use
// conditional if-match API", with no real generation-match precondition, so
// routing it through the mutex-free gcsObjectStore/gcsCubeObjectStore would
// silently lose Track A's concurrency-safety guarantee for Azure). Checking
// the concrete type's package path (rather than importing
// tempodb/backend/gcs directly, which this package does not otherwise
// depend on) avoids adding a new import purely to name one type.
const gcsBackendPkgPath = "github.com/grafana/tempo/tempodb/backend/gcs"

// isGCSBackend reports whether rawW's concrete type actually originates from
// tempodb/backend/gcs, as opposed to merely satisfying
// versionedRawReaderWriter's method shape. See gcsBackendPkgPath's doc
// comment for why structural type assertion alone is unsafe here.
func isGCSBackend(rawW backend.RawWriter) bool {
	if rawW == nil {
		return false
	}
	t := reflect.TypeOf(rawW)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.PkgPath() == gcsBackendPkgPath
}

// versionedRawReaderWriter is the minimal capability this adapter needs from
// a GCS-backed backend.RawReader/backend.RawWriter pair: read/write plus
// generation-matched versioned writes and reads. Mirrors
// backend.VersionedReaderWriter's own shape (versioned.go) but scoped to only
// the methods actually used here.
type versionedRawReaderWriter interface {
	backend.RawReader
	WriteVersioned(ctx context.Context, name string, keypath backend.KeyPath, data io.Reader, size int64, version backend.Version) (backend.Version, error)
	ReadVersioned(ctx context.Context, name string, keypath backend.KeyPath) (io.ReadCloser, backend.Version, error)
}

// gcsCASCore holds the shared Get/ConditionalPut logic for GCS's native
// versioned path, mirroring rawCASCore's shape (rawobjectstore.go) but
// backed by generation-match preconditions instead of content-hash+mutex
// emulation.
type gcsCASCore struct {
	vrw         versionedRawReaderWriter
	errNotFound error
	errConflict error
}

// get reads the object at key via ReadVersioned, returning its bytes plus its
// GCS generation (used as this adapter's etag). A missing object is reported
// via c.errNotFound, matching rawCASCore.get's contract.
func (c *gcsCASCore) get(ctx context.Context, key string) ([]byte, string, error) {
	name, keypath := splitKeyForRaw(key)
	rc, version, err := c.vrw.ReadVersioned(ctx, name, keypath)
	if err != nil {
		if errors.Is(err, backend.ErrDoesNotExist) {
			return nil, "", c.errNotFound
		}
		return nil, "", err
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", err
	}
	return data, string(version), nil
}

// conditionalPut writes via WriteVersioned, using the caller's etag as the
// expected generation (backend.VersionNew — an empty etag maps to
// VersionNew's real create-if-not-exists precondition, backend.go's
// createPreconditions, genuinely fixing the "etag==” is unconditional
// create" gap for GCS specifically — see this file's package doc comment).
// A conflict is detected via EITHER the documented sentinel OR a raw 412
// googleapi.Error (gcs.go's actual, verified behavior) — see package doc
// comment for why both checks are needed.
func (c *gcsCASCore) conditionalPut(ctx context.Context, key string, data []byte, etag string) error {
	name, keypath := splitKeyForRaw(key)
	version := backend.VersionNew
	if etag != "" {
		version = backend.Version(etag)
	}
	_, err := c.vrw.WriteVersioned(ctx, name, keypath, bytes.NewReader(data), int64(len(data)), version)
	if err != nil {
		if isGCSPreconditionFailure(err) {
			return c.errConflict
		}
		return err
	}
	return nil
}

// isGCSPreconditionFailure reports whether err represents a generation-match
// (or does-not-exist) precondition failure, checking both the documented
// backend.ErrVersionDoesNotMatch sentinel and a raw *googleapi.Error with
// Code == 412 — gcs.go's actual current behavior never populates the
// sentinel (verified gap, see package doc comment), so both checks are
// required for this to work correctly against the real backend.
func isGCSPreconditionFailure(err error) bool {
	if errors.Is(err, backend.ErrVersionDoesNotMatch) {
		return true
	}
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == 412 {
		return true
	}
	return false
}

// gcsObjectStore satisfies blockpack.ObjectStore (viusage.Registry's backing
// store) over GCS's native versioned path.
type gcsObjectStore struct{ core *gcsCASCore }

func newGCSObjectStore(vrw versionedRawReaderWriter) *gcsObjectStore {
	return &gcsObjectStore{core: &gcsCASCore{
		vrw:         vrw,
		errNotFound: blockpack.ErrNotFound,
		errConflict: blockpack.ErrConflict,
	}}
}

func (s *gcsObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	return s.core.get(ctx, path)
}

func (s *gcsObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	return s.core.conditionalPut(ctx, path, data, etag)
}

// gcsCubeObjectStore satisfies blockpack.CubeObjectStore (cube.Registry's
// backing store) over GCS's native versioned path — an independent type from
// gcsObjectStore (R1: two registries, independent sentinel errors), mirroring
// rawObjectStore/rawCubeObjectStore's existing split.
type gcsCubeObjectStore struct{ core *gcsCASCore }

func newGCSCubeObjectStore(vrw versionedRawReaderWriter) *gcsCubeObjectStore {
	return &gcsCubeObjectStore{core: &gcsCASCore{
		vrw:         vrw,
		errNotFound: blockpack.CubeErrNotFound,
		errConflict: blockpack.CubeErrConflict,
	}}
}

func (s *gcsCubeObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	return s.core.get(ctx, path)
}

func (s *gcsCubeObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	return s.core.conditionalPut(ctx, path, data, etag)
}

// newObjectStoreForBackend is the single dispatch point for constructing a
// blockpack.ObjectStore for any non-S3 backend: it checks rawW is actually
// GCS's own concrete type (isGCSBackend — NOT a structural type assertion
// alone, see that function's doc comment for why: task #151 found
// *azure.Azure also structurally satisfies versionedRawReaderWriter) and
// falls back to the generic, content-hash+mutex rawCASCore-backed
// implementation otherwise (Local/Azure). NEVER called for cfg.Backend ==
// backend.S3 — callers gate on that before reaching here.
func newObjectStoreForBackend(rawR backend.RawReader, rawW backend.RawWriter) blockpack.ObjectStore {
	if isGCSBackend(rawW) {
		if vrw, ok := rawW.(versionedRawReaderWriter); ok {
			return newGCSObjectStore(vrw)
		}
	}
	return newRawObjectStore(rawR, rawW)
}

// newCubeObjectStoreForBackend mirrors newObjectStoreForBackend for
// blockpack.CubeObjectStore.
func newCubeObjectStoreForBackend(rawR backend.RawReader, rawW backend.RawWriter) blockpack.CubeObjectStore {
	if isGCSBackend(rawW) {
		if vrw, ok := rawW.(versionedRawReaderWriter); ok {
			return newGCSCubeObjectStore(vrw)
		}
	}
	return newRawCubeObjectStore(rawR, rawW)
}

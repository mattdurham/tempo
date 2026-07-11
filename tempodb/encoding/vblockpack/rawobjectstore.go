package vblockpack

// rawobjectstore.go — Track A (backend-agnostic VI/cube usage-recording +
// backfill machinery, plan.md §11): a content-hash+mutex emulation of
// conditional-write (ETag/If-Match) semantics over backend.RawReader/RawWriter,
// for backends with no native conditional-write primitive (Local, Azure).
//
// This targets exactly two tiny JSON registry files per tenant
// (<tenant>/viusage/index.json, <tenant>/cubes/index.json) via
// rawObjectStore/rawCubeObjectStore, mirroring minioObjectStore/
// viUsageObjectStore's existing "near-identical copies, independent sentinel
// types" shape (cubemanager.go, vi_backfill.go) — S3 itself is never routed
// through this file; every Configure* call site branches to the existing S3
// types unchanged when cfg.Backend == backend.S3.
//
// Correctness scope (deliberate, documented): the pathMutexMap only
// serializes writers within THIS process. Multi-replica/multi-process Local
// deployments are not made safe by this file — this targets the stated
// single-process integration-test correctness bar (plan.md §18), not a
// general distributed guarantee. GCS gets a genuinely atomic native path
// instead (rawobjectstore_gcs.go); S3 already has real ETag semantics.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
)

// atomicWriter is an optional capability probed for on a backend.RawWriter —
// mirrors this codebase's existing extra-method-beyond-RawWriter pattern
// (local.go:78's WriteAtomic). When present (true for local.Backend),
// conditionalPut uses it so concurrent writers to the same path never leave a
// partial/interleaved file on disk. When absent (Azure), conditionalPut falls
// back to plain RawWriter.Write — non-atomic but functionally correct for the
// single-process correctness bar this file targets (documented, not silently
// degraded).
type atomicWriter interface {
	WriteAtomic(ctx context.Context, name string, keypath backend.KeyPath, data io.Reader, size int64) error
}

// pathMutexMap lazily allocates one *sync.Mutex per key, serializing
// conditionalPut's read-compare-write sequence for concurrent same-process
// writers to the same key.
type pathMutexMap struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newPathMutexMap() *pathMutexMap {
	return &pathMutexMap{locks: make(map[string]*sync.Mutex)}
}

func (m *pathMutexMap) lockFor(key string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.locks[key]
	if !ok {
		l = &sync.Mutex{}
		m.locks[key] = l
	}
	return l
}

// processCASLocks is a single process-level pathMutexMap instance shared by
// both viusage and cube registries — safe because the two registries write to
// disjoint key namespaces (<tenant>/viusage/index.json vs
// <tenant>/cubes/index.json), so there is no cross-registry lock contention
// concern from sharing one map.
var processCASLocks = newPathMutexMap()

// rawCASCore holds the shared Get/ConditionalPut logic used by both
// rawObjectStore and rawCubeObjectStore — the two registries differ only in
// which sentinel errors they return (errNotFound/errConflict), mirroring
// minioObjectStore/viUsageObjectStore's existing shape.
type rawCASCore struct {
	rawR        backend.RawReader
	rawW        backend.RawWriter
	locks       *pathMutexMap
	errNotFound error
	errConflict error
}

// splitKeyForRaw splits a full "<tenant>/.../<file>" key into the RawWriter/
// RawReader (name, keypath) shape those interfaces expect — mirrors
// modules/frontend/vcnt_fetch.go's existing splitObjectKey helper exactly
// (same call shape, same rationale: every path segment except the last is the
// keypath, the last segment is the object name).
func splitKeyForRaw(fullKey string) (name string, keypath backend.KeyPath) {
	parts := strings.Split(fullKey, "/")
	if len(parts) == 0 {
		return fullKey, nil
	}
	return parts[len(parts)-1], backend.KeyPath(parts[:len(parts)-1])
}

// contentETag computes a stable, content-derived ETag emulation: hex-encoded
// SHA-256 of the object's bytes. Two writes with byte-identical content
// produce the same etag (matching real ETag semantics closely enough for this
// module's conditional-write use, which only ever compares "does the content
// I last read still match what's stored now").
func contentETag(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// get reads the object at key and returns its bytes plus its content-derived
// etag. A missing object is reported via c.errNotFound (bare, matching
// minioObjectStore/viUsageObjectStore's existing Get contract).
func (c *rawCASCore) get(ctx context.Context, key string) ([]byte, string, error) {
	name, keypath := splitKeyForRaw(key)
	rc, _, err := c.rawR.Read(ctx, name, keypath, nil)
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
	return data, contentETag(data), nil
}

// conditionalPut performs the emulated conditional write: lock the key,
// re-read the current state, compare its content-derived etag against the
// caller's expected etag, and only write on a match — see this file's package
// doc comment for the deliberate "etag==” is an unconditional create" gap
// preserved here (matches the pre-existing S3 behavior, not a regression).
func (c *rawCASCore) conditionalPut(ctx context.Context, key string, data []byte, etag string) error {
	mu := c.locks.lockFor(key)
	mu.Lock()
	defer mu.Unlock()

	name, keypath := splitKeyForRaw(key)
	curData, curErr := readRawKey(ctx, c.rawR, name, keypath)
	switch {
	case curErr == nil:
		if contentETag(curData) != etag {
			return c.errConflict
		}
	case errors.Is(curErr, backend.ErrDoesNotExist):
		// Preserve the pre-existing "etag=='' is an unconditional create"
		// gap (plan.md §11 step 4 / §17 edge case) — deliberate, not fixed
		// here. A non-empty etag against a now-missing object IS a real
		// conflict (caller expected an existing object that's gone).
		if etag != "" {
			return c.errConflict
		}
	default:
		return curErr
	}

	return c.write(ctx, name, keypath, data)
}

// write dispatches to atomicWriter.WriteAtomic when c.rawW implements it
// (local.Backend), else falls back to plain RawWriter.Write (Azure) — see
// this file's package doc comment.
func (c *rawCASCore) write(ctx context.Context, name string, keypath backend.KeyPath, data []byte) error {
	if aw, ok := c.rawW.(atomicWriter); ok {
		return aw.WriteAtomic(ctx, name, keypath, bytes.NewReader(data), int64(len(data)))
	}
	return c.rawW.Write(ctx, name, keypath, bytes.NewReader(data), int64(len(data)), nil)
}

// readRawKey is a small helper isolating the Read+ReadAll+Close sequence used
// by conditionalPut's re-read step (kept separate from rawCASCore.get so
// conditionalPut's error handling — which must distinguish ErrDoesNotExist
// from every other error, not translate it to errNotFound — stays explicit).
func readRawKey(ctx context.Context, rawR backend.RawReader, name string, keypath backend.KeyPath) ([]byte, error) {
	rc, _, err := rawR.Read(ctx, name, keypath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// rawObjectStore satisfies blockpack.ObjectStore (viusage.Registry's backing
// store) over backend.RawReader/RawWriter.
type rawObjectStore struct{ core *rawCASCore }

func newRawObjectStore(rawR backend.RawReader, rawW backend.RawWriter) *rawObjectStore {
	return &rawObjectStore{core: &rawCASCore{
		rawR:        rawR,
		rawW:        rawW,
		locks:       processCASLocks,
		errNotFound: blockpack.ErrNotFound,
		errConflict: blockpack.ErrConflict,
	}}
}

func (s *rawObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	return s.core.get(ctx, path)
}

func (s *rawObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	return s.core.conditionalPut(ctx, path, data, etag)
}

// rawCubeObjectStore satisfies blockpack.CubeObjectStore (cube.Registry's
// backing store) over backend.RawReader/RawWriter — an independent type from
// rawObjectStore (R1: two registries, independent sentinel errors), mirroring
// minioObjectStore/viUsageObjectStore's existing split.
type rawCubeObjectStore struct{ core *rawCASCore }

func newRawCubeObjectStore(rawR backend.RawReader, rawW backend.RawWriter) *rawCubeObjectStore {
	return &rawCubeObjectStore{core: &rawCASCore{
		rawR:        rawR,
		rawW:        rawW,
		locks:       processCASLocks,
		errNotFound: blockpack.CubeErrNotFound,
		errConflict: blockpack.CubeErrConflict,
	}}
}

func (s *rawCubeObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	return s.core.get(ctx, path)
}

func (s *rawCubeObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	return s.core.conditionalPut(ctx, path, data, etag)
}

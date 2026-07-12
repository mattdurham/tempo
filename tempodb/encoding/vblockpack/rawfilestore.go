package vblockpack

// rawfilestore.go — Track B: backend-agnostic, read-only VI-postings file store
// (List/Get/Size/ReadAt) over backend.RawReader, backing ConfigureValueIndexQueryRaw's
// querier-side read path for non-S3 backends (minioVIStore, value_index_query.go, remains
// the S3 implementation and is untouched by this file).
//
// List mirrors modules/frontend/vcnt_fetch.go's fetchVCNTSection pattern: it uses
// RawReader.Find, not List, because List's contract on every tempodb backend is a
// directory/common-prefix listing (never a leaf-level file at the queried keypath), while
// Find recursively walks and reports every actual file — exactly the semantics
// blockpack.Lister's "full keys of all objects whose name begins with prefix" needs.
import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
)

// rawFileStore satisfies blockpack.Lister (List), blockpack.ValueIndexFileStore
// (Size + ReadAt), and blockpack.LookupStore (List + Get + Size + ReadAt) over a generic
// backend.RawReader — the non-S3 counterpart to minioVIStore.
type rawFileStore struct {
	rawR backend.RawReader
}

func newRawFileStore(rawR backend.RawReader) *rawFileStore {
	return &rawFileStore{rawR: rawR}
}

// List returns the full object keys whose name begins with prefix, found by recursively
// walking beneath prefix's directory segments via RawReader.Find. A prefix whose directory
// was never created (e.g. a column that has never been indexed yet) returns an EMPTY list
// with a nil error, mirroring S3's minioVIStore.List semantics (S3 has no real directory
// concept, so listing a nonexistent prefix naturally yields zero objects, never an error) --
// local.Backend.Find's own fs.WalkDir/os.DirFS implementation returns a real
// "no such file or directory" error for a nonexistent path, which callers like
// RecordUsageIfNoIndexCoverage must not mistake for "genuine coverage found".
func (s *rawFileStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	err := s.rawR.Find(ctx, splitPrefixForRaw(prefix), func(m backend.FindMatch) {
		if strings.HasPrefix(m.Key, prefix) {
			keys = append(keys, m.Key)
		}
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, backend.ErrDoesNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return keys, nil
}

// Get fetches the full bytes of the object at key, satisfying the fetch half of
// blockpack.LookupStore.
func (s *rawFileStore) Get(ctx context.Context, key string) ([]byte, error) {
	name, keypath := splitKeyForRaw(key)
	rc, _, err := s.rawR.Read(ctx, name, keypath, nil)
	if err != nil {
		return nil, mapRawNotFound(err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, mapRawNotFound(err)
	}
	return data, nil
}

// Size returns the byte length of the object at key. Satisfies
// blockpack.ValueIndexFileStore's fixed (ctx-less) Size(key) signature by delegating to
// sizeCtx with context.Background() (task #199) — see minioVIStore.sizeCtx's doc
// comment (value_index_query.go) for why the plain, ctx-less signature is preserved.
func (s *rawFileStore) Size(key string) (int64, error) {
	return s.sizeCtx(context.Background(), key)
}

// sizeCtx is Size's ctx-aware core (task #199, NOTE-VI-106 follow-up), reached via the
// package-private ctxAwareStore capability (content_cache.go) when a real per-query ctx
// is available.
func (s *rawFileStore) sizeCtx(ctx context.Context, key string) (int64, error) {
	name, keypath := splitKeyForRaw(key)
	rc, size, err := s.rawR.Read(ctx, name, keypath, nil)
	if err != nil {
		return 0, mapRawNotFound(err)
	}
	_ = rc.Close()
	return size, nil
}

// ReadAt fills p from the object at key starting at off, following io.ReaderAt
// semantics. Delegates to readAtCtx with context.Background() (task #199).
func (s *rawFileStore) ReadAt(key string, p []byte, off int64) (int, error) {
	return s.readAtCtx(context.Background(), key, p, off)
}

// readAtCtx is ReadAt's ctx-aware core (task #199). See sizeCtx's doc comment.
func (s *rawFileStore) readAtCtx(ctx context.Context, key string, p []byte, off int64) (int, error) {
	name, keypath := splitKeyForRaw(key)
	err := s.rawR.ReadRange(ctx, name, keypath, uint64(off), p, nil)
	if err != nil {
		return 0, mapRawNotFound(err)
	}
	return len(p), nil
}

// splitPrefixForRaw converts a List prefix (a slash-joined directory prefix, typically
// trailing-slash-terminated per DiscoverIndexFiles' path.Join(...)+"/" callers) into the
// KeyPath RawReader.Find walks from — every segment, since a List prefix names a directory,
// never a leaf file.
func splitPrefixForRaw(prefix string) backend.KeyPath {
	trimmed := strings.TrimSuffix(prefix, "/")
	if trimmed == "" {
		return nil
	}
	return backend.KeyPath(strings.Split(trimmed, "/"))
}

// mapRawNotFound translates backend.ErrDoesNotExist into blockpack.ErrValueIndexFileNotFound
// so the builder's downloadAll can recognise a retention/compaction race (a file the listing
// cache still names but the backend has already deleted) and skip the file instead of failing
// the index build (blockpack issue #399 point 5), mirroring minioVIStore's existing
// mapNotFound. All other errors pass through unchanged so transient failures still abort the
// build — this is the existing 404-skip contract, not a scan fallback.
func mapRawNotFound(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, backend.ErrDoesNotExist) {
		return blockpack.ErrValueIndexFileNotFound
	}
	return err
}

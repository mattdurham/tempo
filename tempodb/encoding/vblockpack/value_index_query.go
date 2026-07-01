package vblockpack

// value_index_query.go — querier-side index-driven query wiring (blockpack issue
// #461). When configured, blockpackBlock.Fetch / QueryRange build a value-index
// source (discover + download + per-leaf predicate) and try the index path before
// falling back to a full block scan.
//
// The reader is a process-level singleton, configured once at querier startup via
// ConfigureValueIndexQuery, mirroring the embedder/cache singletons. When it is
// nil (the default, value_index_query.enabled=false) every query path is
// byte-identical to before — the index path is simply skipped.

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	minio "github.com/minio/minio-go/v7"
	util_log "github.com/grafana/tempo/pkg/util/log"
)

// viQueryReader holds the process-level state needed to answer queries from the
// value index. The querier serves many tenants, so the file-listing cache is
// per-tenant (issue #462's IndexFileCache bakes in the tenant); reader lazily
// builds and memoises one cache per tenant. The object store is shared — it
// addresses objects by full key, which already embeds the tenant.
type viQueryReader struct {
	store       *minioVIStore
	caches      map[string]*blockpack.IndexFileCache
	indexPrefix string
	ttl         time.Duration
	mu          sync.Mutex
}

// cacheFor returns the per-tenant file-listing cache, creating (and starting the
// background refresh for) it on first use.
func (vr *viQueryReader) cacheFor(tenant string) *blockpack.IndexFileCache {
	vr.mu.Lock()
	defer vr.mu.Unlock()
	if c, ok := vr.caches[tenant]; ok {
		return c
	}
	c := blockpack.NewIndexFileCache(vr.store, tenant, vr.indexPrefix, vr.ttl)
	c.Background(context.Background())
	vr.caches[tenant] = c
	return c
}

var (
	viQueryReaderMu  sync.RWMutex
	viQueryReaderPtr *viQueryReader
)

// ConfigureValueIndexQuery installs the process-level index-driven query reader.
// Call once at querier startup. A nil client or disabled config leaves the reader
// unset, so the query path falls back to a full block scan (the prior behaviour).
func ConfigureValueIndexQuery(client *minio.Client, bucket, indexPrefix string, ttl time.Duration) {
	viQueryReaderMu.Lock()
	defer viQueryReaderMu.Unlock()

	if client == nil {
		viQueryReaderPtr = nil
		return
	}
	if indexPrefix == "" {
		indexPrefix = "indexes"
	}
	viQueryReaderPtr = &viQueryReader{
		store:       &minioVIStore{client: client, bucket: bucket},
		caches:      make(map[string]*blockpack.IndexFileCache),
		indexPrefix: indexPrefix,
		ttl:         ttl,
	}
}

// getValueIndexQueryReader returns the configured reader, or nil when the
// index-driven path is disabled.
func getValueIndexQueryReader() *viQueryReader {
	viQueryReaderMu.RLock()
	defer viQueryReaderMu.RUnlock()
	return viQueryReaderPtr
}

// indexFetchStats records the observable I/O of one index-path attempt so the
// querier can attach it to its OTel span and log line (issue #465). It is
// populated whether or not the index answered the query — a declined attempt that
// still downloaded files reports the bytes it spent before falling back.
type indexFetchStats struct {
	// FilesRead is the number of value-index files downloaded building the source.
	FilesRead int
	// BytesRead is the total byte size of those files.
	BytesRead int64
	// Hits is the number of span entries the per-column predicates matched.
	Hits int
	// Used is true when the index path answered the query (the caller skipped the
	// full block scan).
	Used bool
}

// tryIndexFetch attempts to answer a compiled filter program from the value index.
// It returns (matches, true) when the index fully answered the query for this
// block, and (nil, false) when the caller must fall back to a full block scan
// (index disabled, no coverage, unsupported predicate, or any error — the index
// path never fails a query, it only declines). The returned indexFetchStats
// captures the download I/O of this attempt regardless of outcome (issue #465).
func (b *blockpackBlock) tryIndexFetch(
	ctx context.Context,
	r *blockpack.Reader,
	prog *blockpack.Program,
	query string,
	opts blockpack.QueryOptions,
) ([]blockpack.SpanMatch, bool, indexFetchStats) {
	var stats indexFetchStats
	vr := getValueIndexQueryReader()
	if vr == nil {
		return nil, false, stats
	}

	// Derive the query's second-granularity window. A zero bound means "unbounded"
	// in QueryOptions; map that to the full uint64 range so discovery includes all
	// files (the per-file time filter then prunes by wall clock).
	minSec, maxSec := nanoWindowToSec(opts.StartNano, opts.EndNano)

	cache := vr.cacheFor(b.meta.TenantID)
	src, ok, err := blockpack.BuildValueIndexSource(ctx, cache, vr.store, prog, minSec, maxSec)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: index fetch: build source error",
			"block", b.meta.BlockID, "err", err)
		return nil, false, stats
	}
	if !ok {
		level.Info(util_log.Logger).Log("msg", "vblockpack: index fetch: no coverage",
			"block", b.meta.BlockID, "tenant", b.meta.TenantID, "minSec", minSec, "maxSec", maxSec)
		return nil, false, stats
	}
	level.Info(util_log.Logger).Log("msg", "vblockpack: index fetch: coverage found",
		"block", b.meta.BlockID, "tenant", b.meta.TenantID, "files", src.Stats().FilesRead)
	// Capture the build-time I/O even if the query later declines: those bytes were
	// spent and are worth reporting (issue #465).
	bs := src.Stats()
	stats.FilesRead = bs.FilesRead
	stats.BytesRead = bs.BytesRead
	stats.Hits = bs.Hits

	sourceRef := blockObjectKey(b.meta.TenantID, b.meta.BlockID.String())
	matches, indexOK, err := blockpack.QueryTraceQLFromIndex(
		ctx, r, src, query, sourceRef, opts, 0, /* maxIndexHits: 0 = default */
	)
	if err != nil || !indexOK {
		return nil, false, stats
	}
	stats.Used = true
	return matches, true, stats
}

// nanoWindowToSec converts a [startNano, endNano] window to whole seconds for
// value-index file discovery. A zero bound is treated as unbounded.
func nanoWindowToSec(startNano, endNano uint64) (uint64, uint64) {
	const nanosPerSec = 1_000_000_000
	minSec := uint64(0)
	if startNano > 0 {
		minSec = startNano / nanosPerSec
	}
	maxSec := ^uint64(0)
	if endNano > 0 {
		maxSec = endNano / nanosPerSec
	}
	return minSec, maxSec
}

// minioVIStore satisfies both blockpack.Lister (List) and
// blockpack.ValueIndexFileStore (Size + ReadAt) over a minio client. The value
// index lives in the same bucket as the trace blocks.
type minioVIStore struct {
	client *minio.Client
	bucket string
}

// List returns the full object keys under prefix.
func (s *minioVIStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// mapNotFound translates a minio "no such key" / HTTP 404 into
// blockpack.ErrValueIndexFileNotFound so the builder's downloadAll can recognise a
// retention/compaction race (a file the listing cache still names but object
// storage has already deleted) and skip the file instead of failing the index
// build (blockpack issue #399 point 5). All other errors pass through unchanged so
// transient failures still abort and fall back to a correct full scan.
func mapNotFound(err error) error {
	if err == nil {
		return nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == 404 || resp.Code == "NoSuchKey" {
		return blockpack.ErrValueIndexFileNotFound
	}
	return err
}

// Size returns the byte length of the object at key.
func (s *minioVIStore) Size(key string) (int64, error) {
	info, err := s.client.StatObject(context.Background(), s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return 0, mapNotFound(err)
	}
	return info.Size, nil
}

// ReadAt fills p from the object at key starting at off, following io.ReaderAt
// semantics. Value-index files are small (one column directory's merged postings),
// so a ranged GET per call is acceptable; the file-listing cache already removes
// the per-query LIST cost (issue #462).
func (s *minioVIStore) ReadAt(key string, p []byte, off int64) (int, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(off, off+int64(len(p))-1); err != nil {
		return 0, err
	}
	obj, err := s.client.GetObject(context.Background(), s.bucket, key, opts)
	if err != nil {
		return 0, mapNotFound(err)
	}
	defer func() { _ = obj.Close() }()
	// minio's GetObject is lazy: a 404 surfaces on the first read, not on the
	// GetObject call, so map the read error too.
	n, err := io.ReadFull(obj, p)
	return n, mapNotFound(err)
}

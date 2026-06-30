package blockpack

// valueindex_query.go — public glue for the index-driven query path (issue #461,
// NOTE-VI-036).
//
// The index-driven search (QueryTraceQLFromIndex) and metrics
// (ExecuteMetricsTraceQL with TraceMetricOptions.ValueIndex) functions consume a
// pre-populated ValueIndexSource. Tempo's querier (blockpackBlock) needs to build
// that source per query: discover the relevant value-index files for the query's
// time window, download them, apply each leaf predicate, and assemble the source.
//
// This file exposes the minimum surface for that:
//   - Lister + IndexFileCache: cached per-column file discovery (issue #462).
//   - ValueIndexFileStore: the download contract (Size + ReadAt by object key).
//   - BuildValueIndexSource: the orchestration that ties them together.
//
// The querier flow is then:
//
//	prog, _ := blockpack.CompileTraceQL(query, opts)
//	src, ok, err := blockpack.BuildValueIndexSource(ctx, cache, store, prog, minSec, maxSec)
//	if ok && err == nil {
//	    matches, indexOK, _ := blockpack.QueryTraceQLFromIndex(ctx, r, src, query, sourceRef, opts, max)
//	    if indexOK { return matches }
//	}
//	// fall back to QueryTraceQL full scan

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/blockpack/internal/modules/valueindex"
	"github.com/grafana/blockpack/internal/modules/vibuilder"
	"github.com/grafana/blockpack/internal/vm"
)

// Lister lists object keys under a prefix. It is the discovery half of the
// querier's object-storage backend (tempo's S3 reader, the local folder wrapper
// in tests). Both blockpack.WritableStorage and the value-index compactor's store
// already satisfy it.
type Lister = valueindex.Lister

// IndexFileCache caches the per-(colHash, colType) value-index file listing so the
// querier does not perform an S3 LIST on every query (issue #462). One instance
// per querier process; call Background(ctx) once to start the periodic refresh.
type IndexFileCache = valueindex.IndexFileCache

// NewIndexFileCache builds an IndexFileCache over lister for one tenant and index
// prefix. A non-positive ttl uses the package default (30s).
func NewIndexFileCache(lister Lister, tenant, indexPrefix string, ttl time.Duration) *IndexFileCache {
	return valueindex.NewIndexFileCache(lister, tenant, indexPrefix, ttl)
}

// ValueIndexFileStore downloads a single value-index file by its full object key.
// It is the read half of the querier's storage backend. Size + ReadAt mirror the
// Storage interface; the whole object is read into memory because value-index
// query execution is a pure in-memory pass over the file bytes.
type ValueIndexFileStore = vibuilder.FileStore

// ErrValueIndexFileNotFound is the sentinel a ValueIndexFileStore returns (wrapped
// or bare) when the requested value-index object does not exist — an S3 404 /
// NoSuchKey. The querier treats a not-found file as an empty miss and skips it
// rather than failing the index build, so the compactor's write-then-delete cycle
// plus a stale listing cache cannot turn a benign race into a query error
// (NOTE-VI-041, issue #399). Store implementations map their backend's 404 to this
// sentinel; recognize it with errors.Is(err, ErrValueIndexFileNotFound).
var ErrValueIndexFileNotFound = vibuilder.ErrFileNotFound

// BuildValueIndexSource assembles a SliceValueIndexSource for prog over the time
// window [minSec, maxSec] using disc for cached file discovery and store for
// downloads.
//
// Returns (source, true, nil) when at least one column resolved against the index
// (the source may still report per-column no-coverage to the executor, which then
// falls back for those columns). Returns (nil, false, nil) when no column could be
// resolved at all — the caller should skip the index path and go straight to a
// full block scan. A non-nil error means discovery or download failed; the caller
// should fall back to a full scan rather than fail the query.
//
// disc is typically an *IndexFileCache; any FilesForTimeRange implementation works.
func BuildValueIndexSource(
	ctx context.Context,
	disc *IndexFileCache,
	store ValueIndexFileStore,
	prog *Program,
	minSec, maxSec uint64,
) (*SliceValueIndexSource, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if disc == nil {
		return nil, false, nil
	}
	return vibuilder.BuildSource(ctx, disc, store, prog, minSec, maxSec)
}

// BuildValueIndexSourceForMetrics is the metrics analog of BuildValueIndexSource:
// it compiles the metrics query's filter prefix and assembles a source so the
// querier can pass it via TraceMetricOptions.ValueIndex to ExecuteMetricsTraceQL.
//
// metrics queries (e.g. `{ x = "y" } | rate()`) parse as a distinct AST from
// filter expressions, so the filter-only CompileTraceQL rejects them; this wrapper
// uses the metrics compiler. The time bounds passed to the compiler do not affect
// the predicate tree the source is built from, so a zero window is fine here — the
// caller supplies the discovery window via minSec/maxSec.
//
// Returns the same (source, ok, err) contract as BuildValueIndexSource.
func BuildValueIndexSourceForMetrics(
	ctx context.Context,
	disc *IndexFileCache,
	store ValueIndexFileStore,
	query string,
	minSec, maxSec uint64,
) (*SliceValueIndexSource, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if disc == nil {
		return nil, false, nil
	}
	prog, _, err := vm.CompileTraceQLMetrics(query, 0, 0)
	if err != nil {
		// Not a metrics query (or a compile error): the caller falls back to the
		// full-scan metrics path.
		return nil, false, fmt.Errorf("compile metrics query: %w", err)
	}
	return vibuilder.BuildSource(ctx, disc, store, prog, minSec, maxSec)
}

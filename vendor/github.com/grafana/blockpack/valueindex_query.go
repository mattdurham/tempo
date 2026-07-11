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
// The value index is authoritative for the columns it covers (NOTE-VI-096, issue
// #474): the querier falls back to a full scan only when the index genuinely cannot
// answer (no coverage for a leaf, or a non-filter query), never speculatively when
// it already produced a correct answer.
//
// The querier flow is then:
//
//	prog, _ := blockpack.CompileTraceQL(query, opts)
//	src, ok, err := blockpack.BuildValueIndexSource(ctx, cache, store, prog, minSec, maxSec)
//	if ok && err == nil {
//	    matches, indexOK, err := blockpack.QueryTraceQLFromIndex(ctx, r, src, query, sourceRef, opts)
//	    if err != nil { return err }        // index/data inconsistency — surfaced, not masked
//	    if indexOK { return matches }        // authoritative answer
//	}
//	// no coverage: fall back to QueryTraceQL full scan

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

// TraceIndexGetter fetches the full bytes of a discovered trace-by-ID index file
// by its full object key. It is the fetch half of the LookupStore GetTraceByID
// needs. Re-exported at the root so external consumers (tempo's vblockpack S3
// adapter) can implement it without importing blockpack's internal packages
// (NOTE-ROOT-021, issue #468).
type TraceIndexGetter = valueindex.TraceIndexGetter

// LookupStore is the read-only object-storage surface GetTraceByID consults to
// resolve a trace via the trace-by-ID value index: list candidate index files
// (Lister) and fetch a candidate's bytes (TraceIndexGetter). Re-exported at the
// root so external consumers can pass a real, non-nil store into GetTraceByID
// without importing blockpack's internal packages (NOTE-ROOT-021, issue #468).
type LookupStore = valueindex.LookupStore

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

// ColumnWatermark is the query-time-relevant coverage state for one
// non-dedicated, usage-triggered column mid-backfill (#496 R7, plan.md
// Section 4.7): re-exported from vibuilder (not defined here directly) to
// avoid an import cycle, since vibuilder.BuildSource -- which this package
// already imports -- is the actual consumer of the gate.
type ColumnWatermark = vibuilder.ColumnWatermark

// LeafColumnInfo describes one leaf's column name and whether its predicate shape is
// representable against the value index at all (#496/B1, SPEC-VB-5): re-exported from
// vibuilder for the same reason ColumnWatermark is -- vibuilder.LeafIndexable is the
// same per-leaf rule this repo's own index-source builder already relies on, so external
// consumers needing per-leaf/per-column indexability (e.g. tempo's usage-recording hook,
// which must distinguish "this column's predicate shape is permanently unindexable" from
// "this column's shape is fine but genuinely has no coverage yet") get the exact same
// decision buildPredicate makes, never an independently re-derived one.
type LeafColumnInfo = vibuilder.LeafColumnInfo

// LeafColumns enumerates every leaf's {Column, Indexable} in prog's predicate tree, one
// entry per leaf occurrence (not deduplicated by column name -- see
// vibuilder.LeafColumns' own doc comment for the full contract, including the match-all
// and OR-composite cases).
func LeafColumns(prog *Program) []LeafColumnInfo {
	return vibuilder.LeafColumns(prog)
}

// ColHash returns the 32-char lower-hex column-directory hash used in the value index's file
// layout (the same hash valueindex.ColHash produces internally) — re-exported here so external
// callers needing to call IndexFileCache.FilesForTimeRange directly (e.g. tempo's frontend-side
// plan-time usage-recording check, RecordUsageIfNoIndexCoverage) can compute the lookup key
// without importing blockpack's internal valueindex package. Mirrors ColTypeName's identical
// re-export rationale (valueindex_usage.go).
func ColHash(colName string) string {
	return valueindex.ColHash(colName)
}

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
//
// watermarks (#496 R7) gates coverage for non-dedicated, usage-triggered
// columns mid-backfill; nil means no gating (today's behavior, dedicated
// columns) -- see vibuilder.BuildSource's own doc comment for the full
// contract.
func BuildValueIndexSource(
	ctx context.Context,
	disc *IndexFileCache,
	store ValueIndexFileStore,
	prog *Program,
	minSec, maxSec uint64,
	watermarks map[string]ColumnWatermark,
) (*SliceValueIndexSource, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if disc == nil {
		return nil, false, nil
	}
	return vibuilder.BuildSource(ctx, disc, store, prog, minSec, maxSec, watermarks)
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
// watermarks has the same #496 R7 contract as BuildValueIndexSource's.
func BuildValueIndexSourceForMetrics(
	ctx context.Context,
	disc *IndexFileCache,
	store ValueIndexFileStore,
	query string,
	minSec, maxSec uint64,
	watermarks map[string]ColumnWatermark,
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
	return vibuilder.BuildSource(ctx, disc, store, prog, minSec, maxSec, watermarks)
}

package blockpack

// reader.go — public type aliases, provider interfaces, cache configuration,
// Reader/Writer constructors, GetTraceByID, and file layout analysis.
// These are the core I/O primitives that storage backends and integrations build on.

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"

	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_reader "github.com/grafana/blockpack/internal/modules/blockio/reader"
	modules_shared "github.com/grafana/blockpack/internal/modules/blockio/shared"
	modules_chaincache "github.com/grafana/blockpack/internal/modules/chaincache"
	modules_filecache "github.com/grafana/blockpack/internal/modules/filecache"
	modules_memcache "github.com/grafana/blockpack/internal/modules/memcache"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
	modules_sectioncache "github.com/grafana/blockpack/internal/modules/sectioncache"
	modules_tieredcache "github.com/grafana/blockpack/internal/modules/tieredcache"
	"github.com/grafana/blockpack/internal/modules/valueindex"
	vm "github.com/grafana/blockpack/internal/vm"
)

// AGENT: Reader types - these provide access to blockpack data.
// Do not expose any internal reader implementation details.

// WantColumns controls which columns ParseBlockFromBytes eagerly decodes.
// Use WantAll() to load all columns, or WantOnly(cols) for query-plan-driven selection.
type WantColumns = modules_reader.WantColumns

// WantAll returns a WantColumns that eagerly decodes every column.
// Used by tempo's backend_block.go via the blockpack public API.
func WantAll() WantColumns { return modules_reader.WantAll() }

// WantOnly returns a WantColumns that eagerly decodes only the named columns.
// Used by tempo's backend_block.go via the blockpack public API.
func WantOnly(cols map[string]struct{}) WantColumns { return modules_reader.WantOnly(cols) }

// Reader reads modules-format blockpack files and provides query execution.
// This is a thin type alias for the internal modules reader.
type Reader = modules_reader.Reader

// ErrUnsupportedFormatVersion is returned by the reader constructors when a file
// carries a valid blockpack magic but a footer format version this build does not
// understand (a stale pre-v2 block). Callers can use errors.Is against this to route
// around such a block during a mixed-cluster v1→v2 rollout instead of failing the
// whole query (NOTE-V2-004, issue #425).
var ErrUnsupportedFormatVersion = modules_reader.ErrUnsupportedFormatVersion

// UnsupportedFormatVersionError carries the offending footer version alongside
// ErrUnsupportedFormatVersion (which it wraps). Recover it with errors.As.
type UnsupportedFormatVersionError = modules_reader.UnsupportedFormatVersionError

// Writer encodes OTLP spans into the modules blockpack format.
// This is a thin type alias for the internal modules writer.
type Writer = modules_blockio.Writer

// Block represents a decoded block of spans.
type Block = modules_reader.Block

// Column represents a decoded column.
type Column = modules_reader.Column

// DataType represents the type of data being read for caching optimization.
type DataType = modules_rw.DataType

// DataType constants for read optimization hints.
// Priority order (highest → lowest): Footer ≈ Header > Metadata ≈ TraceBloomFilter >
// TimestampIndex > Block. Caching layers use this ordering for eviction decisions.
const (
	DataTypeUnknown          = modules_rw.DataTypeUnknown
	DataTypeFooter           = modules_rw.DataTypeFooter
	DataTypeHeader           = modules_rw.DataTypeHeader
	DataTypeMetadata         = modules_rw.DataTypeMetadata
	DataTypeTraceBloomFilter = modules_rw.DataTypeTraceBloomFilter
	DataTypeTimestampIndex   = modules_rw.DataTypeTimestampIndex
	DataTypeBlock            = modules_rw.DataTypeBlock
)

// SharedLRUCache is a byte-bounded, priority-tiered LRU cache shared across readers.
// Higher-priority data types (Footer, Header, TraceBloomFilter) survive cache pressure
// from block reads and are evicted last.
type SharedLRUCache = modules_rw.SharedLRUCache

// NewSharedLRUCache creates a SharedLRUCache with the given total byte capacity.
// The cache is safe for concurrent use and can be shared across multiple readers.
func NewSharedLRUCache(maxBytes int64) *SharedLRUCache {
	return modules_rw.NewSharedLRUCache(maxBytes)
}

// NewSharedLRUProvider wraps underlying with a caching layer backed by a shared LRU cache.
// readerID uniquely identifies this reader within the cache (e.g. file path or object key).
// The returned provider satisfies ReaderProvider and is safe for concurrent use.
func NewSharedLRUProvider(
	underlying ReaderProvider,
	readerID string,
	cache *SharedLRUCache,
) ReaderProvider {
	return modules_rw.NewSharedLRUProvider(underlying, readerID, cache)
}

// Cache is the common interface for all blockpack cache tiers.
// Implementations: FileCache (disk), MemCache (remote), ChainedCache (multi-tier).
// Use NewChainedCache to compose tiers:
//
//	chain := NewChainedCache(diskCache, remoteCache)
type Cache = modules_filecache.Cache

// FileCache is a disk-backed, size-bounded byte cache for blockpack file sections
// (footer, header, metadata, blocks). It deduplicates concurrent fetches for the
// same key so that many goroutines opening the same file share a single I/O.
// A nil *FileCache is safe to use; all operations become pass-throughs.
type FileCache = modules_filecache.FileCache

// FileCacheConfig configures a disk-backed FileCache.

// Registerer is an optional Prometheus registerer.
// When non-nil, cache metrics are registered and incremented on cache operations.

// Path is the directory path used for cache storage.

// MaxBytes is the maximum total bytes stored on disk.
// Oldest entries (FIFO) are evicted when the limit is exceeded.

// Enabled controls whether the cache is active.
// When false, OpenFileCache returns (nil, nil) and readers skip all caching.

// OpenFileCache opens (or creates) a FileCache with the given configuration.
// Returns (nil, nil) when cfg.Enabled is false.
// The caller must call FileCache.Close() when done.
func OpenFileCache(cfg FileCacheConfig) (*FileCache, error) {
	return modules_filecache.Open(modules_filecache.Config{
		Enabled:    cfg.Enabled,
		MaxBytes:   cfg.MaxBytes,
		Path:       cfg.Path,
		Registerer: cfg.Registerer,
	})
}

// MemCache is a remote memcache-backed cache that implements Cache.
// It is intended as the outermost (largest) tier in a multi-tier chain.
// Keys are hashed with SHA-256 before transmission, so arbitrary-length
// blockpack cache keys are always valid memcache keys.
type MemCache = modules_memcache.MemCache

// MemCacheConfig configures a remote MemCache.

// Registerer is an optional Prometheus registerer.
// When non-nil, cache metrics are registered and incremented on cache operations.

// TierLabel overrides the "tier" Prometheus label (default "remote").
// Set distinct values when opening multiple MemCache instances with the
// same Registerer to avoid label collisions.

// Servers is the list of memcache server addresses (host:port).

// Expiration is the TTL in seconds for stored items. 0 = no expiration.

// Enabled controls whether the cache is active.
// When false, OpenMemCache returns (nil, nil).

// OpenMemCache creates a MemCache connecting to the configured servers.
// Returns (nil, nil) when cfg.Enabled is false.
// The caller must call MemCache.Close() when done.
func OpenMemCache(cfg MemCacheConfig) (*MemCache, error) {
	return modules_memcache.Open(modules_memcache.Config{
		Servers:    cfg.Servers,
		Expiration: cfg.Expiration,
		Enabled:    cfg.Enabled,
		Registerer: cfg.Registerer,
		TierLabel:  cfg.TierLabel,
	})
}

// ChainedCache is a multi-tier Cache that searches tiers in order and writes
// fetched values back to faster tiers on a hit. Use NewChainedCache to build one.
type ChainedCache = modules_chaincache.ChainedCache

// NewChainedCache creates a ChainedCache from the given tiers ordered fastest-first.
// Recommended order: FileCache (disk) → MemCache (remote).
//
// Example:
//
//	disk, _ := blockpack.OpenFileCache(blockpack.FileCacheConfig{Path: "/tmp/bpcache", MaxBytes: 1 << 30, Enabled: true})
//	remote, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Servers: []string{"localhost:11211"}, Enabled: true})
//	chain := blockpack.NewChainedCache(disk, remote)
//	reader, _ := blockpack.NewReaderWithCache(provider, fileID, chain)
func NewChainedCache(tiers ...Cache) *ChainedCache {
	return modules_chaincache.New(tiers...)
}

// TypedConfig holds one cache per section type for TypedTieredCache.
// Use DefaultTypedConfig for the recommended tier mapping, or set each field individually.
type TypedConfig = modules_tieredcache.TypedConfig

// TypedTieredCache routes cache operations to one of seven sub-caches based on
// section type (Footer, TOC, Bloom, Metadata, TraceIdx, Block, Intrinsic).
// It implements sectioncache.SectionCache via typed method dispatch — no key parsing.
type TypedTieredCache = modules_tieredcache.TypedTieredCache

// DefaultTypedConfig returns a TypedConfig with the recommended tier mapping:
//   - hot: Footer, TOC, Bloom, Block (low-latency, high-reuse small blobs)
//   - warm: Metadata, TraceIdx (large blobs; disk round-trip acceptable)
//
// #466 removed the in-process memory tier; both slots are typically a disk FileCache
// and/or a remote MemCache chain.
//
// Example:
//
//	disk, _ := blockpack.OpenFileCache(blockpack.FileCacheConfig{Path: "/var/cache/bp", MaxBytes: 10 << 30, Enabled: true})
//	remote, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Servers: []string{"localhost:11211"}, Enabled: true})
//	tiered  := blockpack.NewTypedTieredCache(blockpack.DefaultTypedConfig(disk, blockpack.NewChainedCache(disk, remote)))
//	reader, _ := blockpack.NewReaderWithCache(provider, fileID, tiered)
func DefaultTypedConfig(hot, warm Cache) TypedConfig {
	return modules_tieredcache.DefaultTypedConfig(hot, warm)
}

// TwoTierTypedConfig returns a TypedConfig that splits caching across two remote caches:
//   - meta: Footer, TOC, Bloom, Metadata, TraceIdx, Intrinsic — small, high-reuse entries
//     that benefit from a shared cache with low eviction pressure (e.g. memcached-01).
//   - page: Block — large column-page blobs cached separately to prevent small metadata
//     entries from being evicted by large page data (e.g. memcached-blockpack-page-01).
//
// Example:
//
//	meta, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Addresses: []string{"memcached-01:11211"}})
//	page, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{Addresses: []string{"memcached-blockpack-page-01:11211"}})
//	tiered  := blockpack.NewTypedTieredCache(blockpack.TwoTierTypedConfig(meta, page))
func TwoTierTypedConfig(meta, page Cache) TypedConfig {
	return modules_tieredcache.TwoTierTypedConfig(meta, page)
}

// NewTypedTieredCache constructs a TypedTieredCache from cfg.
// Nil sub-cache fields are normalized to NopCache.
func NewTypedTieredCache(cfg TypedConfig) *TypedTieredCache {
	return modules_tieredcache.NewTypedTieredCache(cfg)
}

// SectionCache is the typed cache interface used internally by Reader.
// TypedTieredCache implements this interface. Pass a *TypedTieredCache directly
// to NewReaderWithSectionCache / NewLeanReaderWithSectionCache to bypass
// the FilecacheAdapter wrapping layer used by NewReaderWithCache.
type SectionCache = modules_sectioncache.SectionCache

// NewReaderWithSectionCache creates a Reader using a SectionCache directly.
// Use this when passing a *TypedTieredCache to avoid the FilecacheAdapter wrapper.
func NewReaderWithSectionCache(
	provider ReaderProvider,
	fileID string,
	sc SectionCache,
) (*Reader, error) {
	return modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewLeanReaderWithSectionCache creates a lean Reader using a SectionCache directly.
// Use this when passing a *TypedTieredCache to avoid the FilecacheAdapter wrapper.
func NewLeanReaderWithSectionCache(
	provider ReaderProvider,
	fileID string,
	sc SectionCache,
) (*Reader, error) {
	return modules_reader.NewLeanReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// SignalTypeTrace is the signal type for trace blockpack files (the only signal type;
// also the default for legacy files with version < 12).
const SignalTypeTrace = modules_shared.SignalTypeTrace

// ReaderProvider supplies random access to blockpack data.
// Implementations can use files, memory, cloud storage, etc.
type ReaderProvider = modules_rw.ReaderProvider

// CloseableReaderProvider extends ReaderProvider with resource cleanup.
// Implementations that hold open file descriptors or network connections
// should implement Close to release them.

// AGENT: Reader constructors - minimal set needed for creating readers.

// NewReaderFromProvider creates a modules-format reader from a ReaderProvider.
func NewReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	return modules_reader.NewReaderFromProvider(provider)
}

// NewReaderWithCache creates a Reader that caches footer, header, metadata, and block
// reads using the provided Cache. fileID must uniquely identify the file within the
// cache namespace — typically the file path or object storage key.
// A nil cache falls back to uncached reads. cache may be any Cache implementation:
// FileCache, MemCache, or a ChainedCache.
func NewReaderWithCache(provider ReaderProvider, fileID string, cache Cache) (*Reader, error) {
	var sc modules_sectioncache.SectionCache
	if cache != nil {
		sc = modules_sectioncache.NewFilecacheAdapter(cache)
	}
	return modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewLeanReaderFromProvider creates a lean Reader optimized for GetTraceByID workloads.
// V13 files: 2 I/Os (footer + compact trace index).
// V14 files: ≥3 I/Os on open (footer + section directory + block_index); remaining sections
// are loaded lazily. Trace index deferred to first bloom hit (+1 I/O). See TestLeanReader_ThreeIO.
// Falls back to NewReaderFromProvider for files without a compact trace index (v3 footer).
func NewLeanReaderFromProvider(provider ReaderProvider) (*Reader, error) {
	return modules_reader.NewLeanReaderFromProvider(provider)
}

// NewLeanReaderWithCache creates a lean Reader with caching. Uses the same lean path
// as NewLeanReaderFromProvider (version-dependent I/O count; see its doc for details)
// but caches footer and section reads. fileID must uniquely identify the file within the cache namespace.
// cache may be any Cache implementation: FileCache, MemCache, or ChainedCache.
func NewLeanReaderWithCache(provider ReaderProvider, fileID string, cache Cache) (*Reader, error) {
	var sc modules_sectioncache.SectionCache
	if cache != nil {
		sc = modules_sectioncache.NewFilecacheAdapter(cache)
	}
	return modules_reader.NewLeanReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewReaderForProgram returns the optimal Reader for the given program.
// Programs that require column data (column predicates, streaming, vector scoring)
// get a full reader. All other programs — including nil — get a lean reader
// that reads only the compact trace index on bloom hit.
func NewReaderForProgram(
	prog *vm.Program,
	provider ReaderProvider,
	fileID string,
	cache Cache,
) (*Reader, error) {
	if prog.NeedsColumnData() {
		return NewReaderWithCache(provider, fileID, cache)
	}
	return NewLeanReaderWithCache(provider, fileID, cache)
}

// GetTraceByID looks up all spans for the given trace ID via the trace-by-ID value index
// and returns them. traceIDHex must be a 32-character hex string (16 bytes); upper or
// lower case is accepted. Returns an empty slice (not an error) when the index is
// consulted and finds no covering entry — an authoritative "not found".
//
// lister (a LookupStore, re-exported at the root as blockpack.LookupStore for external
// consumers — NOTE-ROOT-021) and tenant are now REQUIRED (NOTE-VI-073, issue #473
// follow-up): the value index is the only supported path — there is no scan fallback for
// callers that omit them. Passing a nil lister or empty tenant is a caller error, not a
// signal to fall back; it returns an error immediately. This is safe because the only
// caller that used to omit them (WAL blocks, which structurally cannot be indexed) no
// longer routes through this function at all: live-store — the sole reader of vblockpack
// WAL blocks — was moved to vParquet4 (2026-07-06), so blockpack's WAL/no-index path has
// no remaining caller.
//
// The trace-by-ID value index (internal/modules/valueindex/traceindex.go,
// TraceGroup/SpanEntry) is AUTHORITATIVE (NOTE-VI-071, SPEC-ROOT-018 rev. 2026-07-06,
// issue #473) — not a hint with a speculative scan fallback. When the index is consulted
// and resolves the trace, its answer is complete. queryMinSec/queryMaxSec scope the index
// file discovery window; pass (0, math.MaxUint64) when no tighter hint is available (e.g.
// a source block's own wall-clock range). Zero candidate index files for that window is an
// indexing coverage gap (NOTE-VI-072) and surfaces as an error, not "not found" — the
// window comes from a block known to hold real data. A discovery-time or decode-time
// failure, or any index/data skew (an index-named block that does not resolve in r, or a
// resolved row whose own trace:id column does not match), is likewise surfaced as an
// ERROR rather than silently masked. The accepted, known consequence (issue #473) is that
// any trace written before the trace-by-ID index began building coverage (NOTE-VI-070) has
// no entry and reads as "not found"; there is no backfill and none is planned — retention
// ages out the uncovered window.
//
// v1 scope decision (plan Open Question 3): GetTraceByID has exactly one Reader for one
// file. An index entry naming a block that does not resolve in r — what a genuinely
// cross-file span looks like, since a different file's page geometry is unrelated to r's
// block layout — is index/data skew and surfaces as an error. Genuine cross-file trace
// assembly is out of scope for v1.
//
// sourceRef (NOTE-VI-076, issue #479): the object key of the data file r was opened
// against — exactly what the write path stamps on each SpanEntry.SourceRef
// (blockObjectKey(tenant, blockID) in tempo). A compacted trace-by-ID index file commonly
// spans many source blocks, so DiscoverIndexFiles returns the SAME wide file as a
// candidate for EVERY block whose time window overlaps it. Without sourceRef, every one of
// those parallel per-block GetTraceByID calls resolves the same TraceGroup, then tries to
// materialize sibling-block spans against r — whose page geometry is unrelated — producing
// a spurious "index/data skew" error for every block except the one that actually owns the
// spans. Because tempo's querier fails the whole trace-by-id query on ANY single block
// error, that turned the normal (shared-file) case into a guaranteed 500. When sourceRef
// is non-empty, materializeTraceGroup keeps only SpanEntries whose SourceRef matches it;
// spans belonging to sibling blocks are dropped before resolution (that sibling's own
// GetTraceByID call resolves them against its own reader). If zero spans remain after the
// filter, that is an authoritative "not found in this block" (nil, nil), NOT skew. A span
// that DOES claim this sourceRef but still fails to resolve remains genuine skew (error).
// An empty sourceRef disables the filter (v1 back-compat / callers with no per-block key).
func GetTraceByID(
	ctx context.Context,
	r *Reader,
	traceIDHex string,
	lister LookupStore,
	tenant, indexPrefix string,
	queryMinSec, queryMaxSec uint64,
	sourceRef string,
) (results []SpanMatch, err error) {
	if r == nil {
		return nil, fmt.Errorf("GetTraceByID: reader cannot be nil")
	}

	if len(traceIDHex) != 32 {
		return nil, fmt.Errorf(
			"GetTraceByID: traceIDHex must be 32 hex chars, got %d",
			len(traceIDHex),
		)
	}

	// A genuinely empty file has no spans and therefore nothing the trace-by-ID index
	// could ever have covered -- zero index files for its window is expected, not a
	// coverage gap (NOTE-VI-072's "caller only asks about known-non-empty blocks"
	// premise does not hold here). This is checked ahead of the lister requirement below:
	// an empty file has nothing to look up regardless of whether the index is configured
	// (NOTE-VI-074).
	if r.BlockCount() == 0 {
		return nil, nil
	}

	if lister == nil || tenant == "" {
		return nil, fmt.Errorf(
			"GetTraceByID: lister and tenant are required (NOTE-VI-073) -- there is no scan fallback",
		)
	}

	traceIDBytes, decErr := hex.DecodeString(traceIDHex)
	if decErr != nil {
		return nil, fmt.Errorf("GetTraceByID: invalid trace ID hex: %w", decErr)
	}

	var traceID [16]byte
	copy(traceID[:], traceIDBytes)

	return getTraceByIDViaIndex(
		ctx, r, traceID, lister, tenant, indexPrefix, queryMinSec, queryMaxSec, sourceRef,
	)
}

// getTraceByIDViaIndex resolves traceID using the AUTHORITATIVE trace-by-ID value index
// (NOTE-VI-071). It never falls back to a scan. The three possible outcomes:
//
//   - (spans, nil): the index covers the trace and every entry resolves in r.
//   - (nil, nil): the index was consulted and holds no covering entry for this specific
//     trace — an authoritative "not found."
//   - (nil, err): the index could not be trusted — a discovery-time failure (e.g. an
//     object-store List error), zero candidate index files covering the window (NOTE-VI-072:
//     an indexing coverage gap for a block GetTraceByID's caller knows is non-empty), a decode
//     failure on a candidate file (corrupt index), or index/data skew (see
//     materializeTraceGroup). The index and the data file are out of sync, or were never built
//     for this window; the caller should observe the error rather than a wrong/partial result.
func getTraceByIDViaIndex(
	ctx context.Context,
	r *Reader,
	traceID [16]byte,
	lister LookupStore,
	tenant, indexPrefix string,
	queryMinSec, queryMaxSec uint64,
	sourceRef string,
) ([]SpanMatch, error) {
	colHash := valueindex.ColHash(modules_shared.TraceIDColumnName)
	colTypeName := valueindex.ColTypeName(modules_shared.ColumnTypeUUID)

	keys, discoverErr := valueindex.DiscoverIndexFiles(
		ctx, lister, tenant, indexPrefix, colHash, colTypeName, queryMinSec, queryMaxSec,
	)
	if discoverErr != nil {
		return nil, fmt.Errorf("GetTraceByID: discover index files: %w", discoverErr)
	}
	if len(keys) == 0 {
		// queryMinSec/queryMaxSec come from the caller's own block metadata (a block
		// known to hold real data -- that's why GetTraceByID was called against it), so
		// zero candidate index files for that exact window is not "no traces exist
		// here": it means the trace-by-ID index never covered a block we know is
		// non-empty. That's an indexing coverage gap, not an authoritative absence --
		// surface it as an error so it doesn't get silently misread as "not found."
		return nil, fmt.Errorf(
			"GetTraceByID: no trace-by-ID index files cover window [%d,%d] for tenant %q -- index coverage gap, not an authoritative not-found",
			queryMinSec,
			queryMaxSec,
			tenant,
		)
	}

	group, found, findErr := findTraceGroupInCandidates(ctx, lister, keys, traceID, queryMinSec, queryMaxSec)
	if findErr != nil {
		return nil, findErr
	}
	if !found {
		// The index was readable and covers the window but holds no entry for this trace.
		return nil, nil
	}

	return materializeTraceGroup(r, group, traceID, sourceRef)
}

// findTraceGroupInCandidates fetches and decodes every candidate index file. Because the
// index is now AUTHORITATIVE (NOTE-VI-071), a fetch or decode failure on any candidate is
// index/data inconsistency and is returned as an ERROR — it is NOT skipped as "unreadable,
// try the next" (the pre-NOTE-VI-071 behavior, safe only while a full scan could still
// produce the correct answer). Silently skipping a corrupt candidate would let the index
// under-report a trace's spans with no observable signal, which the authoritative contract
// forbids.
//
// Candidates are NOT short-circuited on the first match: DiscoverIndexFiles can
// legitimately return multiple valid, not-yet-compacted L0 files for the same TraceID with
// DISJOINT Spans (e.g. a trace's root span flushed in one consumer window and its child
// span flushed in another, before the compactor has merged them). Stopping at the first
// match would silently return a partial trace. Every matching group across every candidate
// is merged (spans deduplicated by SpanID, first occurrence wins; TimeSec is the minimum
// across matches) — the same live-merge semantics as valueindex.MergeTraceGroups, minus
// its RefChecker/retention-drop machinery, which isn't needed here since
// materializeTraceGroup's BlockIndexForPage-failure and defensive trace:id re-verify
// surface any stale reference in the result as an error.
//
// Returns (group, true, nil) on a hit, (zero, false, nil) when no candidate holds the
// trace (an authoritative miss), or (zero, false, err) on any fetch/decode failure.
func findTraceGroupInCandidates(
	ctx context.Context,
	lister LookupStore,
	keys []string,
	traceID [16]byte,
	queryMinSec, queryMaxSec uint64,
) (valueindex.TraceGroup, bool, error) {
	var merged valueindex.TraceGroup
	found := false
	seenSpan := make(map[[8]byte]struct{})

	// mergeGroup folds a matching group's spans into merged, deduplicating by
	// SpanID (first wins) and keeping the minimum TimeSec — the live-merge
	// semantics shared with valueindex.MergeTraceGroups and
	// LookupTraceGroupPartial.
	mergeGroup := func(g *valueindex.TraceGroup) {
		if !found {
			merged.TraceID = g.TraceID
			merged.TimeSec = g.TimeSec
			found = true
		} else if g.TimeSec < merged.TimeSec {
			merged.TimeSec = g.TimeSec
		}
		for i := range g.Spans {
			s := g.Spans[i]
			if _, dup := seenSpan[s.SpanID]; dup {
				continue
			}
			seenSpan[s.SpanID] = struct{}{}
			merged.Spans = append(merged.Spans, s)
		}
	}

	for _, key := range keys {
		// v2 batched files (issue #476) resolve with targeted partial reads
		// (footer + block directory + only the surviving block), so an oversized
		// index file never forces a whole-object download + full decode. A
		// candidate whose footer magic doesn't match v2 (e.g. a pre-#476 legacy
		// v1 flat-blob file) is a hard error — v1 read support was removed once
		// the v2 rollover window closed (NOTE-VI-047 -> retired).
		g, ok, lerr := valueindex.LookupTraceGroupPartial(
			ctx, lister, key, traceID, queryMinSec, queryMaxSec,
		)
		if lerr != nil {
			return valueindex.TraceGroup{}, false, fmt.Errorf(
				"GetTraceByID: partial lookup index candidate %q: %w", key, lerr,
			)
		}
		if ok {
			mergeGroup(&g)
		}
	}
	if !found {
		return valueindex.TraceGroup{}, false, nil
	}
	return merged, true, nil
}

// materializeTraceGroup resolves every SpanEntry in group to an exact block+row in r and
// materializes the matching spans. Because the index is AUTHORITATIVE (NOTE-VI-071), any
// inconsistency between the index and the data file is returned as an ERROR — never a
// partial result and never a silent degrade to a scan: an entry that fails to resolve to a
// real block in r (index/data skew or staleness), a block the reader returns no bytes for,
// a block that fails to parse, or a resolved row whose own trace:id column does not
// actually match traceID (defensive re-verify: an index hit must never produce a wrong
// span). The error tells the caller the index and the data file are out of sync so the
// inconsistency is observable rather than masked (cf. SPEC-ROOT-019 for the search/metrics
// path).
//
// sourceRef filter (NOTE-VI-076, issue #479): a compacted TraceGroup index file spans many
// source blocks, so the group commonly carries SpanEntries belonging to sibling blocks, not
// to r. When sourceRef is non-empty, only entries whose SpanEntry.SourceRef equals it are
// resolved against r; sibling-block entries are dropped up front (their own block's parallel
// GetTraceByID call resolves them against its own reader). This distinguishes "this entry is
// for a sibling block, ignore it" from "this entry claims to be for me but doesn't resolve"
// — only the latter is genuine skew. If no span survives the filter, that is an
// authoritative "not found in THIS block" ((nil, nil)), not an error: some sibling block owns
// the trace and will resolve it. An empty sourceRef disables the filter (v1 back-compat).
func materializeTraceGroup(
	r *Reader,
	group valueindex.TraceGroup,
	traceID [16]byte,
	sourceRef string,
) ([]SpanMatch, error) {
	rowsByBlock := make(map[int][]int, len(group.Spans))
	blockOrder := make([]int, 0, len(group.Spans))
	seen := make(map[[2]int]struct{}, len(group.Spans))
	for _, span := range group.Spans {
		// Drop spans owned by a sibling block: they are addressed against a
		// different file's page geometry, so resolving them against r would be a
		// spurious skew error. The sibling's own GetTraceByID call handles them.
		if sourceRef != "" && span.SourceRef != sourceRef {
			continue
		}
		blockIdx, ok := r.BlockIndexForPage(span.BlockRef.PageNum)
		if !ok {
			return nil, fmt.Errorf(
				"GetTraceByID: index/data skew: index-named page %d does not resolve in file",
				span.BlockRef.PageNum,
			)
		}
		key := [2]int{blockIdx, int(span.RowIdx)}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if _, exists := rowsByBlock[blockIdx]; !exists {
			blockOrder = append(blockOrder, blockIdx)
		}
		rowsByBlock[blockIdx] = append(rowsByBlock[blockIdx], int(span.RowIdx))
	}
	if len(blockOrder) == 0 {
		if sourceRef != "" {
			// Every span in the group belonged to a sibling block (filtered out
			// above). This block simply does not hold the trace -- an authoritative
			// "not found in THIS block," not skew. The sibling block whose SourceRef
			// matches resolves the trace in its own parallel GetTraceByID call
			// (NOTE-VI-076, issue #479).
			return nil, nil
		}
		return nil, fmt.Errorf("GetTraceByID: index/data skew: trace group resolved to zero blocks")
	}

	rawBlocks, readErr := r.ReadBlocks(blockOrder)
	if readErr != nil {
		return nil, fmt.Errorf("GetTraceByID: read index-named blocks: %w", readErr)
	}

	traceIDStr := hex.EncodeToString(traceID[:])
	results := make([]SpanMatch, 0, len(group.Spans))
	for _, blockIdx := range blockOrder {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			return nil, fmt.Errorf(
				"GetTraceByID: index/data skew: block %d missing from read result", blockIdx,
			)
		}
		bwb, parseErr := r.ParseBlockFromBytes(raw, modules_reader.WantAll(), r.BlockMeta(blockIdx))
		if parseErr != nil {
			return nil, fmt.Errorf("GetTraceByID: parse block %d: %w", blockIdx, parseErr)
		}
		traceIDCol := bwb.Block.GetColumn(modules_shared.TraceIDColumnName)
		for _, rowIdx := range rowsByBlock[blockIdx] {
			if !rowMatchesTraceID(traceIDCol, rowIdx, traceID) {
				return nil, fmt.Errorf(
					"GetTraceByID: index/data skew: block %d row %d trace:id does not match index entry",
					blockIdx,
					rowIdx,
				)
			}
			results = append(results, buildSpanMatch(bwb.Block, rowIdx, traceIDStr))
		}
	}
	return results, nil
}

// rowMatchesTraceID reports whether col's value at rowIdx equals traceID. Used as the
// defensive re-verify step after direct index-addressed row access: an index entry is a
// hint, so the row it names is re-checked against the actual data before being trusted.
func rowMatchesTraceID(col *modules_reader.Column, rowIdx int, traceID [16]byte) bool {
	if col == nil {
		return false
	}
	v, ok := col.BytesValue(rowIdx)
	if !ok {
		return false
	}
	return bytes.Equal(v, traceID[:])
}

// buildSpanMatch materializes the SpanMatch for the span at (block, rowIdx). block must
// have been decoded with WantAll() — NewSpanFieldsAdapterWithReader exposes whatever
// columns are present in the decoded block, an open-ended per-span attribute set, not a
// fixed identity-column list (blockio/span_fields.go).
func buildSpanMatch(block *modules_reader.Block, rowIdx int, traceIDStr string) SpanMatch {
	fields := modules_blockio.NewSpanFieldsAdapterWithReader(block, nil, 0, rowIdx, nil)
	spanIDStr := ""
	if col := block.GetColumn(modules_shared.SpanIDColumnName); col != nil {
		if v, ok := col.BytesValue(rowIdx); ok {
			spanIDStr = hex.EncodeToString(v)
		}
	}
	match := SpanMatch{
		Fields:  fields,
		TraceID: traceIDStr,
		SpanID:  spanIDStr,
	}
	cloned := match.Clone()
	modules_blockio.ReleaseSpanFieldsAdapter(fields)
	return cloned
}

// AGENT: Writer constructors - minimal set needed for creating writers.

// DedicatedColumn describes an attribute column to be stored in the intrinsic section
// in addition to the standard block columns. Dedicated columns enable the zero-block-read
// fast path for metrics queries that filter or group-by on them.
// Name must be the full blockpack column name including prefix, e.g. "span.http.method".
type DedicatedColumn = modules_blockio.DedicatedColumn

// WriterConfig configures a blockpack Writer.
// It is a type alias for the internal WriterConfig.
type WriterConfig = modules_blockio.WriterConfig

// NewWriter creates a streaming modules-format blockpack writer that writes to output.
// maxSpansPerBlock controls block granularity (0 uses the default of 2000).
func NewWriter(output io.Writer, maxSpansPerBlock int) (*Writer, error) {
	return modules_blockio.NewWriterWithConfig(modules_blockio.WriterConfig{
		OutputStream:  output,
		MaxBlockSpans: maxSpansPerBlock,
	})
}

// NewWriterWithConfig creates a Writer with full configuration control.
// Use this when advanced settings are needed.
func NewWriterWithConfig(cfg WriterConfig) (*Writer, error) {
	return modules_blockio.NewWriterWithConfig(cfg)
}

// ReadBlockByRef fetches a v2 block by direct page reference (PR6, issue #417).
// Only valid on v2 files (IsV2Format == true). Fetches offset=pageNum*4096,
// length=lenPages*4096 in a single provider.ReadAt, with no TOC lookup.
func ReadBlockByRef(r *Reader, pageNum uint32, lenPages uint16) ([]byte, error) {
	return r.ReadBlockByRef(pageNum, lenPages)
}

// FileLayoutReport describes the byte-level structure of a blockpack file.
type FileLayoutReport = modules_reader.FileLayoutReport

// FileLayoutSection describes one contiguous byte range in a blockpack file.
type FileLayoutSection = modules_reader.FileLayoutSection

// AnalyzeFileLayout inspects every byte in a blockpack file and returns a
// JSON-serializable report organized by section. Each entry carries its
// absolute offset, on-disk (compressed) size, column name, column type, and
// encoding kind for column data blobs.
//
// The returned Sections slice is sorted by Offset ascending and satisfies the
// invariant: sum(section.CompressedSize) == FileLayoutReport.FileSize.
func AnalyzeFileLayout(r *Reader) (report *FileLayoutReport, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			report = nil
			err = fmt.Errorf("internal error in AnalyzeFileLayout: %v", rec)
		}
	}()

	if r == nil {
		return nil, fmt.Errorf("AnalyzeFileLayout: reader cannot be nil")
	}

	return r.FileLayout()
}

// ClearReaderCaches is a no-op retained for API compatibility. #466 removed all process-level
// in-process reader caches (decoded columns, parsed ToC, sparse/chunk trace indexes), leaving
// only the disk + remote memcache tiers, so there is nothing to clear. Intended for test
// isolation; callers may remove it.
func ClearReaderCaches() {}

// ToCSection describes one entry in the file's Table of Contents.
type ToCSection = modules_reader.ToCSection

// ColMeta holds per-column metadata from an inner block without decoding column data.
type ColMeta = modules_reader.ColMeta

// ParseColMetas extracts per-column metadata from block bytes without fully decoding columns.
func ParseColMetas(blockBytes []byte, meta modules_shared.BlockMeta) ([]ColMeta, error) {
	return modules_reader.ParseColMetas(blockBytes, meta)
}

// ColumnType is the type for column data type identifiers. It is the same underlying
// type as the internal shared.ColumnType (uint8) and is re-exported here so external
// callers (e.g. tempo) can reference it without importing internal packages.
type ColumnType = modules_shared.ColumnType

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
	"runtime"
	"sort"
	"sync"

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
	"golang.org/x/sync/errgroup"
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
func NewSharedLRUProvider(underlying ReaderProvider, readerID string, cache *SharedLRUCache) ReaderProvider {
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
// Prefer this over the deprecated NewTieredCache binary router.
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
func NewReaderWithSectionCache(provider ReaderProvider, fileID string, sc SectionCache) (*Reader, error) {
	return modules_reader.NewReaderFromProviderWithOptions(provider, modules_reader.Options{
		Cache:  sc,
		FileID: fileID,
	})
}

// NewLeanReaderWithSectionCache creates a lean Reader using a SectionCache directly.
// Use this when passing a *TypedTieredCache to avoid the FilecacheAdapter wrapper.
func NewLeanReaderWithSectionCache(provider ReaderProvider, fileID string, sc SectionCache) (*Reader, error) {
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
func NewReaderForProgram(prog *vm.Program, provider ReaderProvider, fileID string, cache Cache) (*Reader, error) {
	if prog.NeedsColumnData() {
		return NewReaderWithCache(provider, fileID, cache)
	}
	return NewLeanReaderWithCache(provider, fileID, cache)
}

// GetTraceByID looks up all spans for the given trace ID and returns them.
// traceIDHex must be a 32-character hex string (16 bytes); upper or lower case is accepted.
// Returns an empty slice (not an error) when the trace is not found.
//
// lister is a LookupStore (re-exported at the root as blockpack.LookupStore for
// external consumers — NOTE-ROOT-021). When it is non-nil and tenant is non-empty,
// GetTraceByID first consults the
// TraceGroup trace-by-ID index (internal/modules/valueindex/traceindex.go) for exact
// block+row addressing, avoiding a full-file scan for any trace the index covers.
// queryMinSec/queryMaxSec scope the index file discovery window; pass (0, math.MaxUint64)
// when no tighter hint is available (e.g. from a source block's own wall-clock range).
// A nil lister, an empty tenant, any index miss, any decode failure, or any index/data
// skew unconditionally falls back to a full, exact block scan — the index is a hint,
// never authoritative for absence.
//
// v1 scope decision (plan Open Question 3): GetTraceByID has exactly one Reader for one
// file. An index entry naming a block that does not resolve in r — which is what a
// genuinely cross-file span looks like, since a different file's page geometry is
// unrelated to r's block layout — aborts the index attempt entirely (never a partial
// result) and falls back to getTraceByIDFullScan, which has only ever been able to see
// r's own file. Genuine cross-file trace assembly is out of scope for v1.
func GetTraceByID(
	ctx context.Context,
	r *Reader,
	traceIDHex string,
	lister LookupStore,
	tenant, indexPrefix string,
	queryMinSec, queryMaxSec uint64,
) (results []SpanMatch, err error) {
	if r == nil {
		return nil, fmt.Errorf("GetTraceByID: reader cannot be nil")
	}

	if len(traceIDHex) != 32 {
		return nil, fmt.Errorf("GetTraceByID: traceIDHex must be 32 hex chars, got %d", len(traceIDHex))
	}

	traceIDBytes, decErr := hex.DecodeString(traceIDHex)
	if decErr != nil {
		return nil, fmt.Errorf("GetTraceByID: invalid trace ID hex: %w", decErr)
	}

	var traceID [16]byte
	copy(traceID[:], traceIDBytes)

	if lister != nil && tenant != "" {
		if matches, ok := getTraceByIDViaIndex(
			ctx, r, traceID, lister, tenant, indexPrefix, queryMinSec, queryMaxSec,
		); ok {
			return matches, nil
		}
	}

	return getTraceByIDFullScan(r, traceID)
}

// getTraceByIDViaIndex attempts to resolve traceID using the trace-by-ID value index.
// Returns ok=false for any indeterminacy (no coverage, decode failure, no match, or
// index/data skew) so the caller falls back to a full scan — the index is a hint, never
// authoritative for absence.
func getTraceByIDViaIndex(
	ctx context.Context,
	r *Reader,
	traceID [16]byte,
	lister LookupStore,
	tenant, indexPrefix string,
	queryMinSec, queryMaxSec uint64,
) ([]SpanMatch, bool) {
	colHash := valueindex.ColHash(modules_shared.TraceIDColumnName)
	colTypeName := valueindex.ColTypeName(modules_shared.ColumnTypeUUID)

	keys, discoverErr := valueindex.DiscoverIndexFiles(
		ctx, lister, tenant, indexPrefix, colHash, colTypeName, queryMinSec, queryMaxSec,
	)
	if discoverErr != nil || len(keys) == 0 {
		return nil, false
	}

	group, found := findTraceGroupInCandidates(ctx, lister, keys, traceID)
	if !found {
		return nil, false
	}

	return materializeTraceGroup(r, group, traceID)
}

// findTraceGroupInCandidates fetches and decodes every candidate index file — a fetch or
// decode failure on one file is treated as unreadable, skip and try the next candidate,
// never as "trace not found." Candidates are NOT short-circuited on the first match:
// DiscoverIndexFiles can legitimately return multiple valid, not-yet-compacted L0 files
// for the same TraceID with DISJOINT Spans (e.g. a trace's root span flushed in one
// consumer window and its child span flushed in another, before the compactor has merged
// them). Stopping at the first match would silently return a partial trace with no error,
// violating "the index is a hint, any doubt falls back to full scan." Every matching
// group across every candidate is merged (spans deduplicated by SpanID, first occurrence
// wins; TimeSec is the minimum across matches) — the same live-merge semantics as
// valueindex.MergeTraceGroups, minus its RefChecker/retention-drop machinery, which isn't
// needed here since materializeTraceGroup's BlockIndexForPage-failure and defensive
// trace:id re-verify already provide the safety net for any stale reference in the result.
func findTraceGroupInCandidates(
	ctx context.Context,
	lister LookupStore,
	keys []string,
	traceID [16]byte,
) (valueindex.TraceGroup, bool) {
	var merged valueindex.TraceGroup
	found := false
	seenSpan := make(map[[8]byte]struct{})
	for _, key := range keys {
		data, getErr := lister.Get(ctx, key)
		if getErr != nil {
			continue
		}
		groups, decErr := valueindex.DecodeTraceGroups(data)
		if decErr != nil {
			continue
		}
		for _, g := range groups {
			if g.TraceID != traceID {
				continue
			}
			if !found {
				merged.TraceID = g.TraceID
				merged.TimeSec = g.TimeSec
				found = true
			} else if g.TimeSec < merged.TimeSec {
				merged.TimeSec = g.TimeSec
			}
			for _, s := range g.Spans {
				if _, dup := seenSpan[s.SpanID]; dup {
					continue
				}
				seenSpan[s.SpanID] = struct{}{}
				merged.Spans = append(merged.Spans, s)
			}
		}
	}
	if !found {
		return valueindex.TraceGroup{}, false
	}
	return merged, true
}

// materializeTraceGroup resolves every SpanEntry in group to an exact block+row in r and
// materializes the matching spans. Returns ok=false — never a partial result — the moment
// any entry fails to resolve to a real block in r (index/data skew, staleness, or a
// genuinely cross-file span; see GetTraceByID's v1 scope decision) or a resolved row's own
// trace:id column does not actually match traceID (defensive re-verify: an index hit must
// never produce a wrong span).
func materializeTraceGroup(r *Reader, group valueindex.TraceGroup, traceID [16]byte) ([]SpanMatch, bool) {
	rowsByBlock := make(map[int][]int, len(group.Spans))
	blockOrder := make([]int, 0, len(group.Spans))
	seen := make(map[[2]int]struct{}, len(group.Spans))
	for _, span := range group.Spans {
		blockIdx, ok := r.BlockIndexForPage(span.BlockRef.PageNum)
		if !ok {
			return nil, false
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
		return nil, false
	}

	rawBlocks, readErr := r.ReadBlocks(blockOrder)
	if readErr != nil {
		return nil, false
	}

	traceIDStr := hex.EncodeToString(traceID[:])
	results := make([]SpanMatch, 0, len(group.Spans))
	for _, blockIdx := range blockOrder {
		raw, ok := rawBlocks[blockIdx]
		if !ok {
			return nil, false
		}
		bwb, parseErr := r.ParseBlockFromBytes(raw, modules_reader.WantAll(), r.BlockMeta(blockIdx))
		if parseErr != nil {
			return nil, false
		}
		traceIDCol := bwb.Block.GetColumn(modules_shared.TraceIDColumnName)
		for _, rowIdx := range rowsByBlock[blockIdx] {
			if !rowMatchesTraceID(traceIDCol, rowIdx, traceID) {
				return nil, false
			}
			results = append(results, buildSpanMatch(bwb.Block, rowIdx, traceIDStr))
		}
	}
	return results, true
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

// getTraceByIDFullScan is GetTraceByID's unconditional fallback: an exact scan of every
// block in r for rows matching traceID. This is the pre-Stage-4 GetTraceByID body, moved
// here verbatim in logic (Decision 1: the old full-scan path is kept as the internal
// fallback, not deleted) and split into a two-phase match/materialize decode (Finding 3):
// a cheap WantOnly({"trace:id"}) pass finds matching rows per block, and only blocks with
// at least one match are re-decoded with WantAll() for full field materialization —
// avoiding the WantAll() memory cost (SPEC-ROOT invariant: WantAll() eagerly decodes every
// column) for blocks that don't contain the trace at all.
func getTraceByIDFullScan(r *Reader, traceID [16]byte) (results []SpanMatch, err error) {
	blockCount := r.BlockCount()
	if blockCount == 0 {
		return nil, nil
	}
	blockIDs := make([]int, blockCount)
	for i := range blockCount {
		blockIDs[i] = i
	}

	// NOTE-293 (Lever B): resolve matching rows and span IDs from the block payloads that are
	// fetched anyway. Each block carries per-row trace:id and span:id columns, so the
	// whole-file intrinsic trace:id and span:id columns — whose size scales with the file's
	// total span count, not the looked-up trace — no longer need to be read on this path.
	rawMap, fetchErr := fetchAllBlockBytes(r, blockIDs)
	if fetchErr != nil {
		return nil, fetchErr
	}

	rowsByBlock, scopeErr := scopeMatchingBlocks(r, blockIDs, rawMap, traceID)
	if scopeErr != nil {
		return nil, scopeErr
	}

	matchingBlockIDs := make([]int, 0, len(rowsByBlock))
	for blockID := range rowsByBlock {
		matchingBlockIDs = append(matchingBlockIDs, blockID)
	}
	sort.Ints(matchingBlockIDs)

	// NOTE-291: parse each matching span-block concurrently (see parseBlocksWithWant).
	parsedBlocks, parseErr := parseBlocksWithWant(r, matchingBlockIDs, rawMap, modules_reader.WantAll())
	if parseErr != nil {
		return nil, parseErr
	}

	traceIDStr := hex.EncodeToString(traceID[:])
	for _, blockID := range matchingBlockIDs {
		bwb := parsedBlocks[blockID]
		for _, rowIdx := range rowsByBlock[blockID] {
			results = append(results, buildSpanMatch(bwb.Block, rowIdx, traceIDStr))
		}
	}

	return results, nil
}

// fetchAllBlockBytes fetches the raw bytes for every block in blockIDs using aggressive
// coalescing (a single logical fetch pass regardless of how many blocks end up matching).
func fetchAllBlockBytes(r *Reader, blockIDs []int) (map[int][]byte, error) {
	rawMap := make(map[int][]byte, len(blockIDs))
	for _, group := range r.CoalescedGroups(blockIDs) {
		groupRaw, fetchErr := r.ReadGroup(group)
		if fetchErr != nil {
			return nil, fmt.Errorf("GetTraceByID: read group: %w", fetchErr)
		}
		for bi, raw := range groupRaw {
			rawMap[bi] = raw
		}
	}
	return rawMap, nil
}

// scopeMatchingBlocks is getTraceByIDFullScan's match phase (Finding 3): decode every
// block with WantOnly({"trace:id"}) — cheap, single-column — and scan for matching rows.
// Blocks with zero matches are omitted from the result so the materialize phase never
// re-decodes them with WantAll().
func scopeMatchingBlocks(
	r *Reader,
	blockIDs []int,
	rawMap map[int][]byte,
	traceID [16]byte,
) (map[int][]int, error) {
	want := modules_reader.WantOnly(map[string]struct{}{modules_shared.TraceIDColumnName: {}})
	parsedBlocks, err := parseBlocksWithWant(r, blockIDs, rawMap, want)
	if err != nil {
		return nil, err
	}

	rowsByBlock := make(map[int][]int, len(blockIDs))
	for _, blockID := range blockIDs {
		traceIDCol := parsedBlocks[blockID].Block.GetColumn(modules_shared.TraceIDColumnName)
		if traceIDCol == nil {
			continue
		}
		// NOTE-419: MatchingBytesRows scans the per-block trace:id column for matching rows
		// while paying the lazy-decode atomic and dense-index expansion ONCE for the whole
		// block, instead of per row as a BytesValue+bytes.Equal loop would.
		if rows := traceIDCol.MatchingBytesRows(traceID[:], nil); len(rows) > 0 {
			rowsByBlock[blockID] = rows
		}
	}
	return rowsByBlock, nil
}

// parseBlocksWithWant decodes each of blockIDs concurrently (NOTE-291) using want to
// control which columns are eagerly decoded. Each ParseBlockFromBytes call decodes an
// independent input blob (rawMap[blockID]) into an independent output Block — there is no
// shared mutable state between calls (ParseBlockFromBytes allocates a per-call intern map
// and reads the Reader's pre-decoded/pre-compressed lookups under their own mutexes).
//
// The fan-out is bounded by traceByIDParseConcurrency so a single trace-by-ID call cannot
// saturate every core and starve concurrent metrics queries on the same querier. Each
// goroutine writes only its own map entry under mu; all writes happen-before the
// gParse.Wait() return — no data race.
func parseBlocksWithWant(
	r *Reader,
	blockIDs []int,
	rawMap map[int][]byte,
	want modules_reader.WantColumns,
) (map[int]*modules_reader.BlockWithBytes, error) {
	parsedBlocks := make(map[int]*modules_reader.BlockWithBytes, len(blockIDs))
	var mu sync.Mutex
	var gParse errgroup.Group
	gParse.SetLimit(traceByIDParseConcurrency())
	for _, blockID := range blockIDs {
		blockID := blockID
		raw, ok := rawMap[blockID]
		if !ok {
			return nil, fmt.Errorf("GetTraceByID: block %d missing from coalesced read", blockID)
		}
		gParse.Go(func() error {
			bwb, blockErr := r.ParseBlockFromBytes(raw, want, r.BlockMeta(blockID))
			if blockErr != nil {
				return fmt.Errorf("GetTraceByID: block %d: %w", blockID, blockErr)
			}
			mu.Lock()
			parsedBlocks[blockID] = bwb
			mu.Unlock()
			return nil
		})
	}
	if waitErr := gParse.Wait(); waitErr != nil {
		return nil, waitErr
	}
	return parsedBlocks, nil
}

// traceByIDParseConcurrency bounds the number of span-blocks GetTraceByID decodes
// concurrently (NOTE-291). Trace-by-ID is an interactive, relatively rare lookup that
// shares the querier with background metrics scans, so the per-trace decode fan-out is
// capped to a small fraction of available cores. The cap scales with GOMAXPROCS but is
// clamped to [2, 4]: 2 guarantees within-file parallelism even on small queriers, and 4
// caps the CPU a single trace-by-ID call can claim so it cannot starve concurrent
// metrics queries. A trace spanning fewer blocks than the cap simply uses fewer workers.
func traceByIDParseConcurrency() int {
	const (
		minLimit = 2
		maxLimit = 4
	)
	n := runtime.GOMAXPROCS(0) / 2
	if n < minLimit {
		n = minLimit
	}
	if n > maxLimit {
		n = maxLimit
	}
	return n
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
// Use this when VectorDimension or other advanced settings are needed.
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

package vblockpack

// vi_watermark_cache.go — #496 B3: short-TTL cache for the querier-side
// watermarks map[string]blockpack.ColumnWatermark argument
// BuildValueIndexSource/ForMetrics need (A5/R7's gate). Mirrors
// newCachingStoreWithListTTL's identical short-TTL-collapses-concurrent-loads
// pattern (content_cache.go), but caches the DERIVED watermarks map per
// tenant rather than raw object bytes: one viusage registry Load(ctx) per
// tenant per TTL window, deduplicated across concurrent callers via
// singleflight.
//
// Keyed by blockpack.ColumnWatermarkKey(e.ColumnName, e.ColumnType), NOT e.ColumnName alone
// (issue #536): the registry's own Entry rows are keyed by (Tenant, ColumnHash, ColumnType) --
// the same column name can legitimately exist as two independent, live entries observed as two
// distinct types (confirmed live on tenant 11638: span:duration exists as both a stale,
// essentially-abandoned int64 entry and the active, near-completely-backfilled uint64 entry). A
// name-only key silently collapsed those two entries' coverage state into one, discarding
// whichever entry WatermarksFor's construction loop visited first -- see
// blockpack.ColumnWatermark's own doc comment for the full rationale.

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"golang.org/x/sync/singleflight"

	blockpack "github.com/grafana/blockpack"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// defaultWatermarkCacheTTL mirrors ViUsageConfig.WatermarkCacheTTL's own
// default (common/config.go) for callers that construct a cache directly.
const defaultWatermarkCacheTTL = 30 * time.Second

type viWatermarkCacheEntry struct {
	watermarks map[string]blockpack.ColumnWatermark
	fetched    time.Time
}

// viWatermarkCache caches, per tenant, the map[string]blockpack.ColumnWatermark
// derived from that tenant's usage registry: built from every entry with
// Backfill.Triggered=true, keyed by blockpack.ColumnWatermarkKey(e.ColumnName, e.ColumnType) --
// NOT e.ColumnName alone (issue #536, see WatermarksFor's own doc comment). Dedicated columns
// and never-triggered columns are simply absent from the map, matching BuildValueIndexSource's
// own nil/absent-means-no-gating contract (A5).
//
// registryFor resolves per tenant exactly like realUsageRecorder.registryFor
// (vi_usage_hook.go) -- pg preferred when configured, falling back to the
// object-store-backed registry otherwise. Before this fix (2026-07-22), this
// cache ALWAYS used the object-store registry regardless of pg, silently
// disconnected from the Postgres-backed registry every other component
// (ConfigureViUsage's write-path recorder, compaction-worker's backfill
// execution, the reactive first-trigger bulk insert) actually reads/writes for
// a Postgres-configured deployment -- the R7 "never trust partial coverage as
// complete" gate was a no-op in production: every column was absent from the
// (permanently empty) object-store registry's map, and absence means
// no-gating (A5's own contract), so any VI files discovered for a range were
// trusted as complete regardless of whether that range's backfill had
// actually finished.
type viWatermarkCache struct {
	store   blockpack.ObjectStore
	pg      *blockpack.Postgres
	ttl     time.Duration
	now     func() time.Time
	mu      sync.Mutex
	entries map[string]viWatermarkCacheEntry
	group   singleflight.Group
}

// newViWatermarkCache creates a viWatermarkCache, refreshing each tenant's
// snapshot after ttl (a ttl <= 0 uses defaultWatermarkCacheTTL). pg, when
// non-nil, takes priority over store for registry construction (registryFor's
// own doc comment) -- store is still recorded either way since it costs
// nothing extra (the caller already built it for other uses) and remains the
// fallback for a deployment that never configures Postgres.
func newViWatermarkCache(store blockpack.ObjectStore, pg *blockpack.Postgres, ttl time.Duration) *viWatermarkCache {
	if ttl <= 0 {
		ttl = defaultWatermarkCacheTTL
	}
	return &viWatermarkCache{
		store:   store,
		pg:      pg,
		ttl:     ttl,
		now:     time.Now,
		entries: make(map[string]viWatermarkCacheEntry),
	}
}

// registryFor mirrors realUsageRecorder.registryFor (vi_usage_hook.go) exactly -- pg preferred
// when configured, so the read-side watermark cache and the write-side usage recorder/backfill
// machinery always agree on which registry is authoritative for a given deployment.
func (c *viWatermarkCache) registryFor(tenant string) *blockpack.Registry {
	if c.pg != nil {
		return c.pg.ViUsageRegistry(tenant)
	}
	return blockpack.NewRegistry(c.store, tenant)
}

// WatermarksFor returns tenant's current watermarks map, refreshing from the
// registry if the cached snapshot is older than ttl (or absent). Concurrent
// callers for the SAME tenant during a cache miss collapse to one registry
// Load via singleflight, so a burst of concurrent queries against the same
// tenant issues at most one registry object-storage GET within the TTL
// window.
//
// The returned map is keyed by blockpack.ColumnWatermarkKey(e.ColumnName, e.ColumnType), never
// e.ColumnName alone (issue #536): the registry can return two independent Entry rows sharing
// the same ColumnName but differing ColumnType (Entry is keyed by (Tenant, ColumnHash,
// ColumnType), not name alone), and a name-only map key would silently discard one entry's
// entire coverage state in favor of the other's -- see blockpack.ColumnWatermark's own doc
// comment for the full rationale and a confirmed live example.
func (c *viWatermarkCache) WatermarksFor(ctx context.Context, tenant string) (map[string]blockpack.ColumnWatermark, error) {
	if wm, ok := c.cached(tenant); ok {
		return wm, nil
	}

	v, err, _ := c.group.Do(tenant, func() (any, error) {
		// Re-check under the flight: an earlier flight for the same tenant may
		// have already populated the cache while we waited to become the leader.
		if wm, ok := c.cached(tenant); ok {
			return wm, nil
		}

		registry := c.registryFor(tenant)
		entries, _, lerr := registry.Load(ctx)
		if lerr != nil {
			return nil, lerr
		}

		watermarks := make(map[string]blockpack.ColumnWatermark, len(entries))
		for _, e := range entries {
			if !e.Backfill.Triggered {
				continue
			}
			gapRanges, gerr := c.gapRangesFor(ctx, tenant, e)
			if gerr != nil {
				return nil, gerr
			}
			// Issue #536: composite (name, type) key -- see WatermarksFor's own doc comment.
			watermarks[blockpack.ColumnWatermarkKey(e.ColumnName, e.ColumnType)] = blockpack.ColumnWatermark{
				Triggered:    e.Backfill.Triggered,
				Done:         e.Backfill.Done,
				WatermarkSec: e.Backfill.WatermarkSec,
				GapRanges:    gapRanges,
			}
		}

		c.mu.Lock()
		c.entries[tenant] = viWatermarkCacheEntry{watermarks: watermarks, fetched: c.now()}
		c.mu.Unlock()
		return watermarks, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(map[string]blockpack.ColumnWatermark), nil //nolint:forcetypeassert // group.Do's fn always returns this type
}

// gapRangesFor resolves the not-yet-covered ranges for e (issue #529): when pg is configured,
// the compaction_jobs queue is the sole source of truth (every 1-minute window in e's retention
// was bulk-inserted on first trigger, and only compaction-worker's own success marks a window
// covered), so this fetches the real per-window gaps via Postgres.ViBackfillGapRanges --
// e.Backfill.WatermarkSec is NOT consulted in this case, since it only reflects the separate,
// best-effort in-process quick-start goroutine (launchViBackfill), not the job queue's
// authoritative state. Without pg, there is no job queue at all -- e.Backfill.WatermarkSec is the
// only coverage signal that exists, so it's synthesized into a single gap [0, WatermarkSec),
// reproducing this cache's pre-#529 behavior exactly.
func (c *viWatermarkCache) gapRangesFor(ctx context.Context, tenant string, e blockpack.Entry) ([]blockpack.ColumnWatermarkGapRange, error) {
	if c.pg == nil {
		if e.Backfill.WatermarkSec == 0 {
			return nil, nil
		}
		return []blockpack.ColumnWatermarkGapRange{{StartSec: 0, EndSec: e.Backfill.WatermarkSec}}, nil
	}
	ranges, err := c.pg.ViBackfillGapRanges(ctx, tenant, blockpack.ViBackfillColumn{
		ColumnHash: e.ColumnHash,
		ColumnName: e.ColumnName,
		ColumnType: e.ColumnType,
	})
	if err != nil {
		return nil, err
	}
	gaps := make([]blockpack.ColumnWatermarkGapRange, len(ranges))
	for i, r := range ranges {
		gaps[i] = blockpack.ColumnWatermarkGapRange{
			StartSec: uint64(r.StartSec), //nolint:gosec // G115: Unix time is non-negative for any real clock
			EndSec:   uint64(r.EndSec),   //nolint:gosec // G115: Unix time is non-negative for any real clock
		}
	}
	return gaps, nil
}

// cached returns tenant's cached watermarks map if present and within ttl.
func (c *viWatermarkCache) cached(tenant string) (map[string]blockpack.ColumnWatermark, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[tenant]
	if !ok || c.now().Sub(e.fetched) >= c.ttl {
		return nil, false
	}
	return e.watermarks, true
}

var (
	viWatermarkCacheMu  sync.RWMutex
	viWatermarkCachePtr *viWatermarkCache
)

// ConfigureViWatermarkCache installs the process-level watermark cache (B3's
// fix for go-presubmit.md's top CRITICAL finding: this cache was fully built
// and tested but never instantiated at startup or called from any real query
// call site, making R7's gate dead code in production). Mirrors
// ConfigureViUsageRecorder/ConfigureValueIndexQuery's exact singleton pattern
// -- called from tempodb.go's NewV2Backend alongside ConfigureViUsage. Delegates
// to newViUsageObjectStoreForBackend (vi_usage_hook.go) for the backing store --
// the SAME single construction point ConfigureViUsage uses, so the usage-recording
// hook and this query-time gate never drift on which store they read/write.
//
// R12: when enabled is false, this installs a NIL cache (not a cache that happens
// to return empty maps) so getViWatermarkCache() == nil and every one of the 6 real
// call sites' watermarksForOrNil helper short-circuits to nil without ever
// constructing a store or touching the registry -- "no usage-tracking/backfill
// machinery engaged at all" applies to the query-time gate exactly as it already
// does to the B1 usage-recording hook (ConfigureViUsage).
//
// pg (2026-07-22 fix): threaded through exactly like ConfigureViUsage's own pg param, so this
// cache's registryFor resolves to the SAME Postgres-backed registry the write-path recorder and
// backfill machinery use for a Postgres-configured deployment -- see viWatermarkCache's own doc
// comment for why this was a real, silent gap before this fix (the R7 gate was reading a
// permanently-empty, disconnected object-store registry instead).
func ConfigureViWatermarkCache(
	s3cfg *s3backend.Config, rawR backend.RawReader, rawW backend.RawWriter, enabled bool, ttl time.Duration,
	pg *blockpack.Postgres,
) error {
	if !enabled {
		setViWatermarkCache(nil)
		return nil
	}
	store, err := newViUsageObjectStoreForBackend(s3cfg, rawR, rawW)
	if err != nil {
		return err
	}
	setViWatermarkCache(newViWatermarkCache(store, pg, ttl))
	return nil
}

func setViWatermarkCache(c *viWatermarkCache) {
	viWatermarkCacheMu.Lock()
	defer viWatermarkCacheMu.Unlock()
	viWatermarkCachePtr = c
}

// ConfigureViWatermarkCacheForTest installs a pre-seeded watermarks map for tenant as the
// process-level watermark cache, for tests in OTHER packages that need
// watermarksForOrNil/CheckIndexCoverage-adjacent behavior against a genuine (if hand-seeded)
// watermark state, without a live registry object store (#217/Phase 2.2 — mirrors
// ConfigureValueIndexQueryForTest's cross-package test-injection pattern 1:1; exposed here for
// the identical reason that helper is exposed: this package's own withVIQueryReader-style
// helpers cannot be called across a package boundary). A nil watermarks map installs an entry
// with no columns (i.e. no gating for any column), matching a tenant with no
// usage-triggered/mid-backfill columns at all. Returns a restore function the caller MUST defer
// to reset prior process-level state — this is a shared package-level singleton.
//
// watermarks MUST be keyed by blockpack.ColumnWatermarkKey(colName, colType), not colName alone
// (issue #536) — the caller builds this map by hand, so it carries the same composite-key
// contract WatermarksFor's own real construction does.
func ConfigureViWatermarkCacheForTest(tenant string, watermarks map[string]blockpack.ColumnWatermark) (restore func()) {
	viWatermarkCacheMu.Lock()
	prev := viWatermarkCachePtr
	viWatermarkCachePtr = &viWatermarkCache{
		ttl: time.Hour,
		now: time.Now,
		entries: map[string]viWatermarkCacheEntry{
			tenant: {watermarks: watermarks, fetched: time.Now()},
		},
	}
	viWatermarkCacheMu.Unlock()
	return func() {
		viWatermarkCacheMu.Lock()
		viWatermarkCachePtr = prev
		viWatermarkCacheMu.Unlock()
	}
}

func getViWatermarkCache() *viWatermarkCache {
	viWatermarkCacheMu.RLock()
	defer viWatermarkCacheMu.RUnlock()
	return viWatermarkCachePtr
}

// watermarksForOrNil is the safe, call-site-facing accessor every real query
// path uses: nil whenever the cache is disabled/unset OR the underlying
// registry Load fails (a transient error must never block or fail a query --
// it just means this call falls back to "no gating," identical to today's
// pre-#496 behavior for that one query, not a false-complete answer -- R7's
// gate only ever narrows coverage, never widens it, so a nil/empty map here
// is always the SAFE direction to fail toward). Logs the error at Warn so a
// sustained registry outage is observable without ever surfacing as a query
// failure.
func watermarksForOrNil(ctx context.Context, tenant string) map[string]blockpack.ColumnWatermark {
	wc := getViWatermarkCache()
	if wc == nil {
		return nil
	}
	watermarks, err := wc.WatermarksFor(ctx, tenant)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: watermark cache load failed; querying without R7 gating for this call", "tenant", tenant, "err", err)
		return nil
	}
	return watermarks
}

// BuildViColumnPolicyForTenant resolves #496 R2/R12's write-path ColumnPolicy
// for tenant: the dedicated list is vu.DedicatedColumnsOverride when set,
// else blockpack.DefaultDedicatedColumns (R2's provisional bootstrap list);
// the triggered list is tenant's current usage-triggered columns via the
// process-level watermark cache (nil/absent whenever the cache is disabled
// or the registry load transiently fails -- falling back to "only the
// dedicated list," never to "index nothing" or "fail the write," matching
// WriteValueIndexL0's own best-effort posture). Exported so both
// create.go/compactor.go (this package) and cmd/tempo/app/value_index.go
// (the async Redis-consumer's ExtractValueIndexEntries call, a separate
// module) share one resolution instead of three independent copies.
func BuildViColumnPolicyForTenant(ctx context.Context, vu common.ViUsageConfig, tenant string) blockpack.ColumnPolicy {
	dedicated := vu.DedicatedColumnsOverride
	if len(dedicated) == 0 {
		dedicated = blockpack.DefaultDedicatedColumns
	}
	triggered := triggeredColumnsOrNil(ctx, tenant)
	return blockpack.BuildColumnPolicy(vu.DedicatedColumnsEnabled, dedicated, triggered)
}

// triggeredColumnsOrNil returns tenant's current set of usage-triggered
// column NAMES (#496 Fix B, R2/R12's write-path ColumnPolicy) -- derived from every key of
// watermarksForOrNil's result, since that map is already built from
// Backfill.Triggered=true registry entries only (viWatermarkCache.WatermarksFor's
// own construction). nil under the exact same safe conditions
// watermarksForOrNil returns nil: a disabled/unset cache, or a transient
// registry-load failure -- in both cases the write path falls back to
// "index only the dedicated list," never to "index nothing" or "fail the
// write," matching WriteValueIndexL0's existing best-effort posture.
//
// Issue #536: watermarksForOrNil's keys are now blockpack.ColumnWatermarkKey(name, type)
// composites, not plain names -- this strips the type suffix and deduplicates, since
// BuildColumnPolicy's own allow-set (valueindex_policy.go) is name-keyed: a column name
// observed as two distinct types is still ONE name eligible for write-path indexing, so this
// must never return the SAME name twice (harmless for BuildColumnPolicy's own set-assignment,
// but a duplicate-free slice keeps this function's own contract honest).
func triggeredColumnsOrNil(ctx context.Context, tenant string) []string {
	watermarks := watermarksForOrNil(ctx, tenant)
	if len(watermarks) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(watermarks))
	cols := make([]string, 0, len(watermarks))
	for key := range watermarks {
		name, _, _ := strings.Cut(key, "\x00")
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		cols = append(cols, name)
	}
	return cols
}

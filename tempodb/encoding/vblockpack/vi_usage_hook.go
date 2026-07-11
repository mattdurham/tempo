package vblockpack

// vi_usage_hook.go — #496/B1: usage-recording hook wired into the 3 query-path
// "genuinely missing index" points (metrics: QueryRange; search: tryIndexFetch;
// structural: tryStructuralIndexFetch/tryNegatedStructuralIndexFetch). Implements R3's
// decline-reason distinction: only records a use for a column when (1) it is not
// already in the tenant's configured backend.DedicatedColumns (already indexed
// forward, no backfill needed), and (2) EVERY leaf referencing it has an indexable
// shape per blockpack.LeafColumns (a negated/unindexable shape is a PERMANENT decline —
// SPEC-ROOT-019/NOTE-VI-096 — backfilling would never fix it, so recording it would
// only trigger a wasted backfill). Call sites invoke recordUsageForDeclinedQuery
// whenever the build's Stats().FilesRead is 0 — NOT merely whenever ok is false, since
// NOTE-VI-033's "Add even when empty" contract means BuildSource's ok is satisfied by
// ANY indexable-shaped leaf regardless of whether any file was ever discovered for it
// (see recordUsageForDeclinedQuery's own doc comment for the full explanation).
//
// Rate-limited per (tenant, column) to at most once per viUsageRateLimit.ttl, mirroring
// cubequerypath.go's maybeCreateCube pattern (R4) — prevents a burst of concurrent
// identical queries (one per overlapping block) from firing a registry round-trip each.

import (
	"context"
	"sync"
	"time"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
)

// usageRecorder is the injectable seam for #496's usage-recording hook (R3/R4),
// mirroring cqp.objectStore()'s existing DI pattern: production code goes through the
// process-level singleton (getViUsageRecorder/ConfigureViUsageRecorder, wired by B3),
// tests inject a fake implementing this interface directly.
type usageRecorder interface {
	RecordUse(ctx context.Context, tenant, colName, colType string, now time.Time) (blockpack.TriggerResult, error)
}

// realUsageRecorder wraps a real blockpack.ObjectStore + Config + TriggerConfig,
// routing through blockpack.MaybeRecordUseAndMaybeTrigger so
// Config.DedicatedColumnsEnabled=false (R12) disables this hook's registry I/O
// entirely, not just A1's write-path policy. onShouldBackfill, when non-nil, is
// invoked with the entry whenever the underlying registry call reports
// ShouldBackfill=true -- B3's config wiring sets this to hand off to B2's
// launchViBackfill with the real S3 config, keeping this hook itself unaware of
// S3/backfill-launch mechanics (plan.md's "when ShouldBackfill=true, hand off to B2's
// launcher").
//
// registryFor lazily builds and memoises one *blockpack.Registry PER TENANT
// (mirrors viQueryReader.cacheFor's identical per-tenant lazy-cache pattern,
// value_index_query.go) -- blockpack.Registry is bound to exactly one tenant at
// construction (its indexPath() is "<tenant>/viusage/index.json"), and Tempo's
// querier serves many tenants dynamically, so a single shared Registry field
// would silently read/write the WRONG tenant's index.json for every tenant
// except whichever one happened to be baked in at construction time. Found this
// while wiring ConfigureViUsageRecorder at querier startup (B3): both existing
// direct realUsageRecorder tests happened to record for the exact same tenant
// the Registry was constructed with, which is exactly why this multi-tenant gap
// was never caught -- see TestRealUsageRecorder_DifferentTenantsUseSeparateRegistries.
type realUsageRecorder struct {
	store            blockpack.ObjectStore
	usageCfg         blockpack.Config
	triggerCfg       blockpack.TriggerConfig
	onShouldBackfill func(entry blockpack.Entry)

	mu         sync.Mutex
	registries map[string]*blockpack.Registry
}

// registryFor returns tenant's memoised Registry, constructing one lazily on
// first use. Safe to call on a realUsageRecorder built as a bare struct
// literal (registries starts nil).
func (r *realUsageRecorder) registryFor(tenant string) *blockpack.Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.registries == nil {
		r.registries = make(map[string]*blockpack.Registry)
	}
	if reg, ok := r.registries[tenant]; ok {
		return reg
	}
	reg := blockpack.NewRegistry(r.store, tenant)
	r.registries[tenant] = reg
	return reg
}

func (r *realUsageRecorder) RecordUse(
	ctx context.Context, tenant, colName, colType string, now time.Time,
) (blockpack.TriggerResult, error) {
	registry := r.registryFor(tenant)
	result, err := blockpack.MaybeRecordUseAndMaybeTrigger(ctx, r.usageCfg, registry, tenant, colName, colType, now, r.triggerCfg)
	if err == nil && result.ShouldBackfill && r.onShouldBackfill != nil {
		r.onShouldBackfill(result.Entry)
	}
	return result, err
}

// ConfigureViUsage builds the real, S3-backed usage recorder (ObjectStore +
// Config + TriggerConfig, per B3) and installs it via ConfigureViUsageRecorder
// -- the querier-startup counterpart to ConfigureValueIndexQuery/
// ConfigureCubeQueryPath, called from tempodb.go's NewV2Backend (mirrors those
// two call sites exactly: same gating, same S3 client construction helper).
// onShouldBackfill is wired to launchViBackfill (B2) with the same s3cfg, so
// #496's query -> usage hook -> registry -> trigger -> backfill launcher ->
// watermark-persist -> watermark cache -> back into the query path loop
// closes end-to-end from this one call.
//
// KNOWN, ACCEPTED, PRE-EXISTING BEHAVIOR (not introduced or changed by the frontend-side
// RecordUsageIfNoIndexCoverage call site): onShouldBackfill is wired unconditionally here, so
// the first process -- querier OR frontend -- whose RecordUse call wins the trigger lease
// launches BackfillEngine.Run's background goroutine IN THAT PROCESS. This was already true for
// the frontend process before the frontend ever called into this hook (ConfigureViUsage runs
// identically in both processes via tempodb.New/modules/storage), simply dormant because nothing
// called RecordUseAndMaybeTrigger from frontend code. Team-lead ruling (2026-07-11): inherit
// this as-is; the cross-process lease (SPEC-VIUSAGE-3) is the correctness boundary regardless of
// which process launches the goroutine, bounded by updateEntryWithRetry's 5-attempt retry cap
// (SPEC-VIUSAGE-4) rather than an unconditional guarantee. Do not add a disabling variant.
//
// enabled gates the WHOLE hook, not just Config.DedicatedColumnsEnabled's
// internal no-op: when false, ConfigureViUsageRecorder is never called at
// all, so viUsageRecorderPtr stays nil and every one of the 3 call sites'
// getViUsageRecorder()==nil check makes recordUsageForDeclinedQuery a
// zero-function-call no-op -- R12's "no usage-tracking/backfill machinery
// engaged at all" requirement, satisfied the same way viQueryReaderPtr's own
// nil disables its entire path.
func ConfigureViUsage(s3cfg *s3backend.Config, usageCfg blockpack.Config, triggerCfg blockpack.TriggerConfig) error {
	if !usageCfg.DedicatedColumnsEnabled || s3cfg == nil {
		ConfigureViUsageRecorder(nil)
		return nil
	}
	client, err := newViBackfillMinioClient(s3cfg)
	if err != nil {
		return err
	}
	store := &viUsageObjectStore{client: client, bucket: s3cfg.Bucket}
	rec := &realUsageRecorder{
		store:      store,
		usageCfg:   usageCfg,
		triggerCfg: triggerCfg,
		onShouldBackfill: func(entry blockpack.Entry) {
			launchViBackfill(entry, s3cfg)
		},
	}
	ConfigureViUsageRecorder(rec)
	return nil
}

var (
	viUsageRecorderMu  sync.RWMutex
	viUsageRecorderPtr usageRecorder
)

// ConfigureViUsageRecorder installs the process-level usage recorder (B3). A nil
// recorder disables the hook entirely -- every call site below no-ops when unset,
// mirroring viQueryReaderPtr's own nil-disables-the-path convention.
func ConfigureViUsageRecorder(rec usageRecorder) {
	viUsageRecorderMu.Lock()
	defer viUsageRecorderMu.Unlock()
	viUsageRecorderPtr = rec
}

func getViUsageRecorder() usageRecorder {
	viUsageRecorderMu.RLock()
	defer viUsageRecorderMu.RUnlock()
	return viUsageRecorderPtr
}

// viUsageRateLimiter collapses a burst of concurrent identical (tenant, column)
// usage-record attempts into one registry round-trip within ttl, mirroring
// cubequerypath.go's maybeCreateCube rate-limit pattern (R4's explicit reuse ask).
//
// go-presubmit.md HIGH finding: seen is written on every allow() call and, without
// maxTrackedKeys, would never be pruned -- in a long-running querier serving many
// tenants over many ad-hoc, non-dedicated attribute names (exactly this feature's target
// scenario), the map would grow monotonically for the process lifetime. allow() sweeps
// every already-expired entry once len(seen) exceeds maxTrackedKeys, bounding the map's
// long-run size to roughly the number of DISTINCT (tenant, column) keys actually active
// within one ttl window, not the lifetime total.
type viUsageRateLimiter struct {
	mu             sync.Mutex
	seen           map[string]time.Time
	ttl            time.Duration
	maxTrackedKeys int
}

func newViUsageRateLimiter(ttl time.Duration) *viUsageRateLimiter {
	return &viUsageRateLimiter{
		seen:           make(map[string]time.Time),
		ttl:            ttl,
		maxTrackedKeys: viUsageRateLimitMaxTrackedKeys,
	}
}

// allow reports whether key may proceed now, recording now as key's last-seen time when
// it does. A key seen within the last ttl is rate-limited (returns false) without
// updating its timestamp, so a sustained burst is throttled to one attempt per ttl, not
// one attempt every call that happens to land ttl apart from the FIRST call.
func (l *viUsageRateLimiter) allow(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if last, ok := l.seen[key]; ok && now.Sub(last) < l.ttl {
		return false
	}
	l.seen[key] = now
	if len(l.seen) > l.maxTrackedKeys {
		l.sweepExpiredLocked(now)
	}
	return true
}

// sweepExpiredLocked removes every entry whose ttl has already elapsed relative to now.
// Caller must hold l.mu. A key still within its ttl window is never swept, regardless of
// map size -- this bounds long-run growth without ever rate-limiting a key early.
func (l *viUsageRateLimiter) sweepExpiredLocked(now time.Time) {
	for k, last := range l.seen {
		if now.Sub(last) >= l.ttl {
			delete(l.seen, k)
		}
	}
}

// viUsageRateLimitWindow mirrors R4's explicit ~10s suggestion for collapsing
// concurrent-query fan-out into one registry round-trip.
const viUsageRateLimitWindow = 10 * time.Second

// viUsageRateLimitMaxTrackedKeys bounds viUsageRateLimiter.seen's size before a sweep of
// expired entries is triggered -- large enough that legitimate, currently-active
// (tenant, column) cardinality within one ttl window is never mistaken for the
// unbounded-growth condition this exists to prevent.
const viUsageRateLimitMaxTrackedKeys = 100_000

var viUsageRateLimit = newViUsageRateLimiter(viUsageRateLimitWindow)

// dedicatedColumnSet builds a lookup set of blockpack scope-prefixed column names (e.g.
// "resource.service.name", "span.http.request.method") from dcs, for R3's "column not in
// backend.DedicatedColumns" check. Mirrors the same Scope+"."+Name convention
// viusage.DefaultDedicatedColumns documents (dedicated_columns.go).
func dedicatedColumnSet(dcs backend.DedicatedColumns) map[string]struct{} {
	set := make(map[string]struct{}, len(dcs))
	for _, dc := range dcs {
		set[string(dc.Scope)+"."+dc.Name] = struct{}{}
	}
	return set
}

// viUsageColInfo is one column's ALL-not-ANY indexability reduction over every leaf in a
// program that references it, shared by recordUsageForDeclinedQuery and
// RecordUsageIfNoIndexCoverage -- see reduceLeafColumnsAllIndexable's own doc comment for
// the full rationale (mirrors AllLeavesIndexable/SPEC-QP-5's own aggregation rule).
type viUsageColInfo struct {
	colType      blockpack.ColumnType
	allIndexable bool
}

// reduceLeafColumnsAllIndexable reduces prog's blockpack.LeafColumns to one decision per
// distinct column: allIndexable is true only when EVERY leaf referencing that column has an
// indexable shape (ALL-not-ANY) -- see recordUsageForDeclinedQuery's own doc comment
// (unchanged) for the full negation-safety rationale this exists to preserve. order
// preserves first-seen column order so callers iterate deterministically.
func reduceLeafColumnsAllIndexable(prog *blockpack.Program) (order []string, byCol map[string]viUsageColInfo) {
	byCol = make(map[string]viUsageColInfo)
	order = make([]string, 0)
	for _, lc := range blockpack.LeafColumns(prog) {
		info, ok := byCol[lc.Column]
		if !ok {
			order = append(order, lc.Column)
			info = viUsageColInfo{allIndexable: true}
		}
		if !lc.Indexable {
			info.allIndexable = false
		} else if info.colType == 0 {
			info.colType = lc.ColType
		}
		byCol[lc.Column] = info
	}
	return order, byCol
}

// recordUsageForDeclinedQuery is the shared R3 decision function for all 3 call sites.
// For every column blockpack.LeafColumns finds leaves for in prog, it records a use IFF:
//   - EVERY leaf referencing that column has an indexable shape (ALL-not-ANY, mirroring
//     AllLeavesIndexable/SPEC-QP-5's own aggregation rule) -- a permanent-decline shape
//     (negation, RequirePresent-only, multi-value) on even ONE leaf for a column means
//     that column is never recorded, since backfilling would not change that outcome
//     (R3/SPEC-ROOT-019/NOTE-VI-096). This matters because TraceQL's `!=` compiles to a
//     RequirePresent-only leaf ANDed with an OR-of-two-ranges leaf on the SAME column --
//     recording on the first indexable occurrence alone would wrongly record a negated
//     query via its indexable sibling leaf; and
//   - the column is NOT already in dedicated (already forward-indexed, no backfill
//     needed).
//
// Call sites invoke this whenever the build's Stats().FilesRead is 0 -- see
// tryIndexFetch's own doc comment for why ok=true/leftOK=true/rightOK=true alone is NOT
// sufficient evidence of real coverage (NOTE-VI-033's "Add even when empty" contract
// means BuildSource's ok only requires an indexable SHAPE, not any actually-discovered
// file) -- FilesRead==0 is the real "genuinely missing index" signal, independent of ok.
func recordUsageForDeclinedQuery(
	ctx context.Context,
	tenant string,
	prog *blockpack.Program,
	dedicated map[string]struct{},
	now time.Time,
) {
	rec := getViUsageRecorder()
	if rec == nil {
		return
	}
	order, byCol := reduceLeafColumnsAllIndexable(prog)
	for _, col := range order {
		info := byCol[col]
		if !info.allIndexable {
			continue // R3: permanent decline -- negation/RequirePresent-only/multi-value
		}
		if _, ok := dedicated[col]; ok {
			continue // already forward-indexed -- no backfill needed
		}
		key := tenant + "|" + col
		if !viUsageRateLimit.allow(key, now) {
			continue
		}
		colTypeName := blockpack.ColTypeName(info.colType)
		_, _ = rec.RecordUse(ctx, tenant, col, colTypeName, now)
	}
}

// RecordUsageIfNoIndexCoverage is the frontend plan-time counterpart to
// recordUsageForDeclinedQuery: it checks each qualifying column's value-index file coverage
// directly via the frontend process's own already-live IndexFileCache (discovery only, no
// download) and records a "missing index" use on the frontend's own request-scoped ctx when
// coverage is genuinely absent. This fixes the root cause that recordUsageForDeclinedQuery's
// existing querier-side call sites run inside the querier's per-block Fetch/QueryRange, whose
// ctx is starved/canceled before a slow registry conditional-PUT round-trip can complete.
//
// dedicated is the raw backend.DedicatedColumns the caller's RoundTrip already has (no
// conversion needed at the call site) -- dedicatedColumnSet is built once, internally, since
// modules/frontend cannot call the unexported helper itself.
//
// Reuses the SAME package-level viUsageRateLimit singleton recordUsageForDeclinedQuery uses:
// a burst from both the frontend's call and the querier's own call for the same (tenant,
// column) within the 10s window collapses via the same rate limiter only when both calls
// happen to run in the SAME process; cross-process it is 2 independent rate limiters --
// bounded double-cost only, never a correctness issue, per SPEC-VIUSAGE-3's lease mechanism.
func RecordUsageIfNoIndexCoverage(
	ctx context.Context,
	tenant string,
	prog *blockpack.Program,
	dedicated backend.DedicatedColumns,
	minSec, maxSec uint64,
	now time.Time,
) {
	rec := getViUsageRecorder()
	if rec == nil {
		return
	}
	vr := getValueIndexQueryReader()
	if vr == nil {
		return
	}
	cache := vr.cacheFor(tenant)
	dedicatedSet := dedicatedColumnSet(dedicated)

	order, byCol := reduceLeafColumnsAllIndexable(prog)
	for _, col := range order {
		info := byCol[col]
		if !info.allIndexable {
			continue // R3: permanent decline -- negation/RequirePresent-only/multi-value
		}
		if _, ok := dedicatedSet[col]; ok {
			continue // already forward-indexed
		}
		colTypeName := blockpack.ColTypeName(info.colType)
		colHash := blockpack.ColHash(col)
		files, err := cache.FilesForTimeRange(ctx, colHash, colTypeName, minSec, maxSec)
		if err != nil || len(files) > 0 {
			continue // discovery error, or genuine coverage: not a "missing index" signal
		}
		key := tenant + "|" + col
		if !viUsageRateLimit.allow(key, now) {
			continue
		}
		_, _ = rec.RecordUse(ctx, tenant, col, colTypeName, now)
	}
}

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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"

	util_log "github.com/grafana/tempo/pkg/util/log"
)

// usageRecorder is the injectable seam for #496's usage-recording hook (R3/R4):
// production code goes through the process-level singleton
// (getViUsageRecorder/ConfigureViUsageRecorder, wired by B3), tests inject a fake
// implementing this interface directly.
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

	// pgPool is the opt-in Postgres backend for the viusage registry
	// (2026-07-11). Nil means "not configured" -- registryFor falls back to
	// the existing store-backed (S3/Local/GCS/Azure) path unconditionally.
	pgPool *pgxpool.Pool

	mu         sync.Mutex
	registries map[string]*blockpack.Registry
}

// registryFor returns tenant's memoised Registry, constructing one lazily on
// first use. Safe to call on a realUsageRecorder built as a bare struct
// literal (registries starts nil). Per-tenant, never baked in at
// construction -- mirrors viQueryReader.cacheFor's identical pattern. The
// pgPool branch below is evaluated per tenant too, not baked into a single
// process-wide choice, though in practice pgPool is a single shared pool.
func (r *realUsageRecorder) registryFor(tenant string) *blockpack.Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.registries == nil {
		r.registries = make(map[string]*blockpack.Registry)
	}
	if reg, ok := r.registries[tenant]; ok {
		return reg
	}
	var reg *blockpack.Registry
	if r.pgPool != nil {
		reg = blockpack.NewRegistryFromEntryStore(newPgViUsageEntryStore(r.pgPool), tenant)
	} else {
		reg = blockpack.NewRegistry(r.store, tenant)
	}
	r.registries[tenant] = reg
	return reg
}

func (r *realUsageRecorder) RecordUse(
	ctx context.Context, tenant, colName, colType string, now time.Time,
) (blockpack.TriggerResult, error) {
	metricViUsageRecorded.Inc()
	registry := r.registryFor(tenant)
	result, err := blockpack.MaybeRecordUseAndMaybeTrigger(ctx, r.usageCfg, registry, tenant, colName, colType, now, r.triggerCfg)
	if err == nil && result.ShouldBackfill {
		metricViBackfillTriggered.Inc()
		if r.onShouldBackfill != nil {
			r.onShouldBackfill(result.Entry)
		}
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
// pgPool is the opt-in Postgres backend for the viusage registry (2026-07-11,
// part of the same opt-in Postgres backend as ConfigureCubeManager's new
// param). Nil means "not configured" -- the existing S3/Local/GCS/Azure
// object-store construction below is UNTOUCHED either way (it's simply
// unused by registryFor when pgPool != nil, costing nothing extra since it
// was already being built for the backfill deps regardless).
func ConfigureViUsage(
	s3cfg *s3backend.Config, rawR backend.RawReader, rawW backend.RawWriter,
	usageCfg blockpack.Config, triggerCfg blockpack.TriggerConfig, pgPool *pgxpool.Pool,
) error {
	if !usageCfg.DedicatedColumnsEnabled {
		ConfigureViUsageRecorder(nil)
		return nil
	}
	store, err := newViUsageObjectStoreForBackend(s3cfg, rawR, rawW)
	if err != nil {
		return err
	}
	var backfillDeps RunViBackfillDeps
	if s3cfg != nil {
		backfillDeps, err = NewViBackfillDepsS3(s3cfg)
		if err != nil {
			return err
		}
	} else {
		backfillDeps = NewViBackfillDepsRaw(rawR, rawW)
	}
	// jobStore is the opt-in durable backend_jobs queue (#181) -- nil when
	// Postgres isn't configured, the same nil-means-disabled convention as
	// pgPool itself. Inserting via jobStore in onShouldBackfill below is
	// purely additive: it does NOT replace launchViBackfill's in-process
	// goroutine, both run (see the "does NOT replace" note on ConfigureViUsage's
	// own doc comment) -- the durable row makes a crash mid-backfill
	// recoverable, while the goroutine keeps today's zero-added-latency
	// common case.
	var jobStore *jobstore.Store
	if pgPool != nil {
		jobStore = jobstore.New(pgPool)
	}
	// var + separate assignment (not :=) is required here: onShouldBackfill's
	// closure below calls rec.registryFor, which needs rec in scope -- a
	// short variable declaration's RHS cannot see its own LHS identifier.
	var rec *realUsageRecorder
	rec = &realUsageRecorder{
		store:      store,
		usageCfg:   usageCfg,
		triggerCfg: triggerCfg,
		pgPool:     pgPool,
		onShouldBackfill: func(entry blockpack.Entry) {
			if jobStore != nil {
				if ierr := jobStore.InsertViBackfill(context.Background(), entry.Tenant, jobstore.ViBackfillDetail{
					ColumnHash: entry.ColumnHash,
					ColumnName: entry.ColumnName,
					ColumnType: entry.ColumnType,
					// WindowSeconds: 0 -- this reactive first-trigger path is
					// unbounded (issue #518 decision (c): the very first backfill of a
					// column always does everything; job-planner's chained
					// continuation jobs are the ones that pass a real bound).
					WindowSeconds: 0,
				}); ierr != nil {
					level.Warn(util_log.Logger).Log(
						"msg", "vblockpack: failed to insert durable vi_backfill job",
						"tenant", entry.Tenant, "column", entry.ColumnName, "err", ierr,
					)
				}
			}
			deps := backfillDeps
			// registryFor(tenant) returns the SAME memoised registry the
			// triggering RecordUse call just used (Postgres-backed when
			// pgPool != nil, else the existing blob-backed path) -- fixes a
			// live bug (2026-07-11, tenant 11638) where runViBackfillCore
			// built a FRESH blob-backed registry from ObjStore regardless of
			// Postgres config, so the just-created Postgres entry was
			// invisible to the backfill's own watermark-persist calls.
			deps.Registry = rec.registryFor(entry.Tenant)
			if pgPool != nil {
				deps = NewViBackfillDepsCatalogOverride(deps, pgPool, entry, backend.NewReader(rawR))
			}
			// windowSeconds: 0 -- unbounded, matching the InsertViBackfill call
			// above (this is the reactive first-trigger path, issue #518
			// decision (c)).
			launchViBackfill(entry, deps, 0)
		},
	}
	ConfigureViUsageRecorder(rec)
	return nil
}

// newViUsageObjectStoreForBackend is the single construction point both
// ConfigureViUsage and ConfigureViWatermarkCache call for the viusage
// registry's backing store -- mirrors cubequerypath.go's existing
// objectStore() single-construction-point pattern (two call sites must never
// drift on which store they use). s3cfg != nil selects the existing,
// untouched S3 construction (viUsageObjectStore); otherwise falls back to the
// generic backend-dispatch factory (newObjectStoreForBackend,
// rawobjectstore_gcs.go), which itself probes for GCS's native
// conditional-write capability before falling back to the Local/Azure
// content-hash+mutex emulation (rawobjectstore.go).
func newViUsageObjectStoreForBackend(
	s3cfg *s3backend.Config, rawR backend.RawReader, rawW backend.RawWriter,
) (blockpack.ObjectStore, error) {
	if s3cfg != nil {
		client, err := newMinioClientFromS3Config(s3cfg)
		if err != nil {
			return nil, err
		}
		return &viUsageObjectStore{client: client, bucket: s3cfg.Bucket}, nil
	}
	if rawR == nil || rawW == nil {
		// Should be unreachable given tempodb.go's gates, which always supply either s3cfg or
		// both rawR/rawW -- but a nil rawR/rawW here would otherwise silently construct a
		// rawObjectStore wrapping nil fields, deferring the failure to a nil-pointer panic on
		// its first real Get/ConditionalPut call. Fail fast and clearly instead (plan.md §17).
		return nil, errors.New("vblockpack: newViUsageObjectStoreForBackend: no backend configured (s3cfg, rawR, and rawW are all nil)")
	}
	return newObjectStoreForBackend(rawR, rawW), nil
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

// ViUsageRecorderConfiguredForTest reports whether the process-level usage recorder
// singleton is currently installed (non-nil) — i.e. whether ConfigureViUsage has run and
// installed a real recorder (not the disabled/nil case). See
// ValueIndexQueryConfiguredForTest's doc comment for why this exists.
func ViUsageRecorderConfiguredForTest() bool {
	return getViUsageRecorder() != nil
}

// ViUsageObjectStoreRawWriterTypeForTest reports the concrete Go type (via %T) of the
// backend.RawWriter backing the installed viusage object store's Local/Azure/GCS-generic
// path (rawObjectStore.core.rawW / gcsObjectStore.core.vrw), or "" when no recorder is
// installed or the installed recorder is the S3 path (viUsageObjectStore, which never uses
// rawR/rawW at all). TEST-ONLY: exists to prove, from outside this package (tempodb_test.go),
// whether tempodb.go's New() passed a genuine backend.RawWriter (e.g. "*local.Backend") or a
// cache-wrapped one (e.g. "*cache.readerWriter") into ConfigureViUsage -- the wrapper hides
// the WriteAtomic/WriteVersioned capability probes this package's dispatch depends on.
func ViUsageObjectStoreRawWriterTypeForTest() string {
	rec, ok := getViUsageRecorder().(*realUsageRecorder)
	if !ok || rec == nil {
		return ""
	}
	switch s := rec.store.(type) {
	case *rawObjectStore:
		return fmt.Sprintf("%T", s.core.rawW)
	case *gcsObjectStore:
		return fmt.Sprintf("%T", s.core.vrw)
	default:
		return ""
	}
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

var (
	dedicatedColumnsLookupMu sync.RWMutex
	dedicatedColumnsLookupFn func(tenantID string) backend.DedicatedColumns
)

// ConfigureDedicatedColumnsLookup installs the process-level accessor recordCubeColumnUsage's
// OnCreateAttempt wiring (cubequerypath.go) uses to obtain a tenant's CURRENT
// backend.DedicatedColumns at query time -- NOT at ConfigureCubeQueryPath's construction time
// (which runs from tempodb.New(), before the Overrides module is guaranteed to exist; the Store
// module has no dependency edge on Overrides in cmd/tempo/app/modules.go's DAG). Wired from
// cmd/tempo/app's initQuerier: the module-init function confirmed (by direct trace, #511 Fix 2)
// to have BOTH t.store and t.Overrides already constructed, and confirmed to be the ONLY
// production caller reaching OnCreateAttempt at all (tryQueryFromCube's sole caller is
// backend_block.go, querier-side; initQueryFrontend does not need this wiring). A nil fn
// (never configured, e.g. non-S3 backends where ConfigureCubeQueryPath itself never installs a
// callback) makes getDedicatedColumnsForTenant return nil, matching every other *ColumnsLookup/
// singleton's "unconfigured means no-op" convention in this file.
func ConfigureDedicatedColumnsLookup(fn func(tenantID string) backend.DedicatedColumns) {
	dedicatedColumnsLookupMu.Lock()
	defer dedicatedColumnsLookupMu.Unlock()
	dedicatedColumnsLookupFn = fn
}

func getDedicatedColumnsForTenant(tenantID string) backend.DedicatedColumns {
	dedicatedColumnsLookupMu.RLock()
	fn := dedicatedColumnsLookupFn
	dedicatedColumnsLookupMu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(tenantID)
}

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

// recordColumnUsageIfDue is the shared per-column "check dedicated -> check rate limit -> call
// RecordUse" primitive both recordUsageForDeclinedQuery and RecordUsageIfNoIndexCoverage's loops
// used to duplicate inline. Extracted (Fix 2 for #511) so a 3rd, differently-shaped caller
// (recordCubeColumnUsage) can reuse the SAME dedicated-check/rate-limit-key convention without a
// 3rd independently-drifting copy. No-ops when no recorder is installed, mirroring every other
// call site's rec==nil convention.
func recordColumnUsageIfDue(
	ctx context.Context, tenant, col, colType string, dedicated map[string]struct{}, now time.Time,
) {
	rec := getViUsageRecorder()
	if rec == nil {
		return
	}
	if _, ok := dedicated[col]; ok {
		return // already forward-indexed -- no backfill needed
	}
	key := tenant + "|" + col
	if !viUsageRateLimit.allow(key, now) {
		return
	}
	_, _ = rec.RecordUse(ctx, tenant, col, colType, now)
}

// recordCubeColumnUsage records a usage-recording attempt for every column a cube's dims/AggAttrs
// actually need VI coverage for -- the columns Fix 1's LookupColumn(DurationColumn)-anchored
// backfill (blockpack's internal/modules/cube/backfill.go) depends on to find any data at all.
// This is NOT a 4th call to recordUsageForDeclinedQuery/RecordUsageIfNoIndexCoverage: those take
// a compiled *blockpack.Program and walk its LEAVES (blockpack.LeafColumns), which requires a
// comparison operator to exist -- a bare group-by dim or a materialized AggAttr column has no
// operator and no leaf shape at all, so this calls the shared recordColumnUsageIfDue primitive
// directly per column instead (#511 Fix 2, Approach B). dims-length-agnostic: works identically
// for a zero-dimension entry (Fix 1's exact new shape), a single-dim, or a two-dim entry.
//
// colType defaulting (accepted, documented risk -- #511 plan.md): every dim defaults to "string"
// (no counter-example of a non-string TraceQL group-by dimension in this codebase today); every
// AggAttr uses the SAME Duration-int64/else-float64 convention cube.AggAttrDefsFor and
// extractAggAttr already use elsewhere (not a new risk, an existing one this function inherits).
// Literal "string"/"int64"/"float64" strings are used rather than blockpack.ColTypeName because
// that function takes a blockpack.ColumnType (shared.ColumnType), a DIFFERENT type from
// blockpack.CubeAggAttrType (cube.AggAttrType) with a different zero-value meaning -- converting
// between them would silently misname the type, not just add friction.
func recordCubeColumnUsage(
	ctx context.Context, tenant string, entry blockpack.CubeRegistryEntry,
	dedicated map[string]struct{}, now time.Time,
) {
	for _, dim := range entry.Dimensions {
		recordColumnUsageIfDue(ctx, tenant, dim, "string", dedicated, now)
	}
	for _, attr := range entry.AggAttrs {
		colType := "float64"
		if attr == blockpack.CubeDurationColumn {
			colType = "int64"
		}
		recordColumnUsageIfDue(ctx, tenant, attr, colType, dedicated, now)
	}
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
		colTypeName := blockpack.ColTypeName(info.colType)
		recordColumnUsageIfDue(ctx, tenant, col, colTypeName, dedicated, now)
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
		colTypeName := blockpack.ColTypeName(info.colType)
		colHash := blockpack.ColHash(col)
		files, err := cache.FilesForTimeRange(ctx, colHash, colTypeName, minSec, maxSec)
		if err != nil || len(files) > 0 {
			continue // discovery error, or genuine coverage: not a "missing index" signal
		}
		recordColumnUsageIfDue(ctx, tenant, col, colTypeName, dedicatedSet, now)
	}
}

package blockpack

// cube_query_path.go — public API for the metrics-cube query path (#508).
// Ports the registry-cache/route/fan-out/rollup/creation-trigger orchestration
// previously living in tempo's tempodb/encoding/vblockpack/cubequerypath.go
// into blockpack root, so tempo constructs one CubeQueryPath and calls QueryRange.
// SPEC-CUBE-032: orchestration contract (no blockpack singleton, caller-launched backfill,
// partial-coverage pass-through, Lister-not-CubeFileStore.List file discovery).

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

// CubeQueryPathRequest describes a single cube query, with TraceQL-string
// parsing and dims/filters extraction already done by the caller (tempo).
type CubeQueryPathRequest struct {
	Tenant                     string
	NeededAttr                 string
	Dims                       []string
	Filters                    []CubeColumnFilter
	MinTS, MaxTS               uint64
	RequestedResolutionMinutes uint32
	MinMinute, MaxMinute       uint32
	NeededAttrType             CubeAggAttrType
	NeededAttrOK               bool
}

// CubeQueryPathResult is the routed+fetched+rolled-up result of a CubeQueryPath.QueryRange
// call. Tempo builds its own tempopb response from this.
type CubeQueryPathResult struct {
	CubeID                             string
	Cells                              []CubeMergedCell
	Dimensions, AggAttrNames           []string
	BytesRead                          int64
	Resolution                         uint32
	CoveredMinMinute, CoveredMaxMinute uint32
	PartialCoverage                    bool
}

// CubeErrWarming signals "no usable cube coverage for this query shape/window; cube
// creation was just attempted in the background." Callers should retry shortly. Deliberately
// Cube-prefixed, not ErrCube-, to group with this file's other Cube* exported symbols
// (CubeQueryPath, CubeQueryPathRequest, etc.) — mirrors tempo's own pre-move ErrCubeWarming's
// naming intent within blockpack's existing Cube*-prefix convention.
//
//nolint:revive,staticcheck // see the naming-convention rationale above
//lint:ignore ST1012 deliberately Cube-prefixed, not ErrCube- (see rationale above)
var CubeErrWarming = errors.New(
	"blockpack: cube not yet backfilled for this shape/window; creation triggered, retry shortly",
)

// CubeQueryPathConfig configures a CubeQueryPath's caching, creation-cooldown, and
// backfill-launch behavior. All fields have sane zero-value defaults applied by
// NewCubeQueryPath.
type CubeQueryPathConfig struct {
	// OnCreateAttempt, if non-nil, is invoked synchronously from the background
	// goroutine QueryRange launches on a cache miss, after every TryCreate attempt
	// (both a fresh registration AND a re-evaluation of an already-existing-but-
	// never-backfilled cube). Tempo wires this to its own jobstore.InsertCubeBackfill
	// call and to spawning blockpack.RunCubeBackfill — blockpack must never import
	// tempo's jobstore package or own logging/metrics.
	OnCreateAttempt func(ctx context.Context, entry CubeRegistryEntry, created, hasL0Watermark bool, triggerErr error)
	// BackfillIndexPrefix is the value-index object-key prefix RunCubeBackfill reads
	// from. Caller-supplied; blockpack makes no default guess.
	BackfillIndexPrefix string
	// TriggerConfig configures the cardinality-gated cube-creation trigger.
	TriggerConfig CubeTriggerConfig
	// RegistryCacheTTL bounds how long a tenant's loaded registry entries are reused
	// before being re-fetched from Postgres. Defaults to 5 minutes.
	RegistryCacheTTL time.Duration
	// CreateCooldown bounds how often TryCreate is re-attempted for the same
	// (tenant, dims, filters) shape after a miss. Defaults to 1 minute.
	CreateCooldown time.Duration
	// BackfillWorkers bounds concurrency inside RunCubeBackfill. Defaults to 4.
	BackfillWorkers int
}

// cubeTenantCacheEntry caches one tenant's loaded registry entries for
// CubeQueryPathConfig.RegistryCacheTTL.
type cubeTenantCacheEntry struct {
	loadedAt time.Time
	entries  []CubeRegistryEntry
}

// CubeQueryPath orchestrates the metrics-cube query path: registry-entry caching,
// query routing, fan-out file fetch + rollup, and first-query creation triggering.
// Constructed once per tempo process via NewCubeQueryPath (no package-level
// singleton lives in blockpack).
type CubeQueryPath struct {
	files      CubeFileStore
	lister     Lister
	vi         LookupStore
	pgPool     *pgxpool.Pool
	tenants    map[string]cubeTenantCacheEntry
	createSeen map[string]time.Time
	cfg        CubeQueryPathConfig

	mu sync.RWMutex
}

// NewCubeQueryPath constructs a CubeQueryPath. files is used for cube-file Get/Put;
// lister is used for the query path's own raw prefix listing (deliberately narrower
// than CubeFileStore.List — see Decision 2 in the #508 implementation plan); vi is
// the value-index LookupStore used by the creation trigger's VCNT lookups; pgPool
// may be nil, in which case registry-backed operations decline silently.
func NewCubeQueryPath(
	files CubeFileStore,
	lister Lister,
	vi LookupStore,
	pgPool *pgxpool.Pool,
	cfg CubeQueryPathConfig,
) *CubeQueryPath {
	if cfg.RegistryCacheTTL <= 0 {
		cfg.RegistryCacheTTL = 5 * time.Minute
	}
	if cfg.CreateCooldown <= 0 {
		cfg.CreateCooldown = time.Minute
	}
	if cfg.BackfillWorkers <= 0 {
		cfg.BackfillWorkers = 4
	}
	return &CubeQueryPath{
		files:      files,
		lister:     lister,
		vi:         vi,
		pgPool:     pgPool,
		cfg:        cfg,
		tenants:    make(map[string]cubeTenantCacheEntry),
		createSeen: make(map[string]time.Time),
	}
}

// loadEntries returns cached (or freshly-loaded) cube registry entries for tenant.
// Issue #504 removed the blob/index.json registry fallback entirely, so there is no
// backend left to serve this call without Postgres -- decline with an error (the
// caller, QueryRange, already treats any loadEntries error as "cube path not
// applicable") rather than let NewPgCubeRegistry's underlying nil pool panic.
func (q *CubeQueryPath) loadEntries(ctx context.Context, tenant string) ([]CubeRegistryEntry, error) {
	if q.pgPool == nil {
		return nil, errors.New("blockpack: cube registry requires postgres, none configured")
	}

	q.mu.Lock()
	st, ok := q.tenants[tenant]
	if ok && time.Since(st.loadedAt) < q.cfg.RegistryCacheTTL {
		entries := st.entries
		q.mu.Unlock()
		return entries, nil
	}
	q.mu.Unlock()

	reg := NewPgCubeRegistry(q.pgPool, tenant)
	entries, _, err := reg.Load(ctx)
	if err != nil {
		return nil, err
	}

	q.mu.Lock()
	q.tenants[tenant] = cubeTenantCacheEntry{entries: entries, loadedAt: time.Now()}
	q.mu.Unlock()
	return entries, nil
}

// invalidateCache forces a registry reload on the next loadEntries call for tenant.
func (q *CubeQueryPath) invalidateCache(tenant string) {
	q.mu.Lock()
	delete(q.tenants, tenant)
	q.mu.Unlock()
}

// QueryRange attempts to answer req from cube files. Returns (result, true, nil) when the
// cube path answered the query. Returns (nil, false, err) to fall back to block scan/VI — err
// is CubeErrWarming specifically when cube creation was just triggered (see its own doc
// comment), nil for every other "cannot answer from cube" reason (not self-healing, no retry
// signal — a repeat query would hit the identical permanent condition).
func (q *CubeQueryPath) QueryRange(ctx context.Context, req CubeQueryPathRequest) (*CubeQueryPathResult, bool, error) {
	entries, err := q.loadEntries(ctx, req.Tenant)
	if err != nil {
		return nil, false, nil //nolint:nilerr // intentional: registry load failure silently falls back to VI/scan, matching loadEntries' own decline contract
	}

	router := NewCubeQueryRouter(entries)
	result, routeErr := router.Route(
		req.Tenant,
		req.Dims,
		req.Filters,
		req.NeededAttr,
		req.RequestedResolutionMinutes,
		req.MinMinute,
		req.MaxMinute,
	)
	if routeErr != nil || !result.Found {
		// Cube not found, or found but with NO overlap at all with the requested window. Attempt
		// cube creation on first query. Fire cube creation in a background goroutine so QueryRange
		// is not blocked.
		go q.maybeCreateCube(
			context.Background(),
			req.Tenant,
			req.Dims,
			req.Filters,
			req.NeededAttr,
			req.NeededAttrType,
			req.NeededAttrOK,
			req.MinTS,
			req.MaxTS,
		)
		return nil, false, CubeErrWarming
	}

	// #217/SPEC-CUBE-028 (ruling 4(b) revisit): partial coverage — the cube's watermark overlaps
	// only PART of [minMinute, maxMinute]. Narrow the actual cell read to
	// result.CoveredMinMinute/CoveredMaxMinute (never read cells outside the confirmed-covered
	// range).
	minMinute, maxMinute, partialCoverage := cubeCoveredWindow(result, req.MinMinute, req.MaxMinute)

	// Cube found: list and download L0 files for the time window. Uses the narrower Lister, not
	// CubeFileStore.List, deliberately (Decision 2 in the #508 plan) — CubeFileStore.List's
	// filename-parsing fallback issues a ranged GET per unmerged L0 file, which combined with the
	// full-file GET below would be two GETs per file for the exact common, high-file-count case
	// this fan-out targets.
	//
	// NOTE-CUBE-034: the cube-file write path (Accumulator.FlushTo -> cube.Filename) always
	// encodes the FULL [16]byte cube ID (32 hex chars, zero-padded from the registry's
	// 16-hex-char/8-byte CubeID) as the S3 directory name -- the SAME "registry stores
	// 16-hex-char IDs (8 bytes); S3 dirs use 32-hex-char (16 bytes, zero-padded)" convention
	// cube_compactor.go's cubeFileStore.List already pads for. Using the unpadded registry
	// CubeID directly here (as tempo's now-superseded tryQueryFromCube did) would never match
	// any real file's actual directory, silently declining every cube query into "not found"
	// forever -- caught here (real bug, found via this package's own Phase 8 end-to-end test),
	// not carried forward.
	cubeDirID := result.Entry.CubeID
	if len(cubeDirID) == 16 {
		cubeDirID += strings.Repeat("0", 16)
	}
	prefix := path.Join(req.Tenant, "cubes", cubeDirID) + "/"
	keys, listErr := q.lister.List(ctx, prefix)
	if listErr != nil || len(keys) == 0 {
		return nil, false, nil //nolint:nilerr // intentional: a list failure/empty listing silently falls back to VI/scan, not self-healing
	}

	inputs, cubeBytesRead, fanOutErr := q.fetchCubeFileInputs(ctx, keys, result.Entry)
	if fanOutErr != nil {
		// A goroutine panicked -- treat as no cube coverage rather than propagating, so a single
		// corrupt file can't take down the query handler.
		return nil, false, nil //nolint:nilerr // intentional: fan-out panic/failure silently falls back to VI/scan
	}
	if len(inputs) == 0 {
		return nil, false, nil
	}

	cells, rollupErr := rollupCubeInputs(inputs, result, minMinute, maxMinute)
	if rollupErr != nil {
		return nil, false, nil //nolint:nilerr // intentional: a rollup failure silently falls back to VI/scan
	}

	return &CubeQueryPathResult{
		Cells:            cells,
		Dimensions:       result.Entry.Dimensions,
		AggAttrNames:     result.Entry.AggAttrs,
		Resolution:       result.Resolution,
		BytesRead:        cubeBytesRead,
		PartialCoverage:  partialCoverage,
		CoveredMinMinute: minMinute,
		CoveredMaxMinute: maxMinute,
		CubeID:           result.Entry.CubeID,
	}, true, nil
}

// cubeFileFetchResult is one fan-out goroutine's outcome, indexed by its key's ORIGINAL
// position so ordering stays deterministic regardless of which goroutine finishes first.
type cubeFileFetchResult struct {
	input CubeRollupInput
	ok    bool
}

// fetchCubeFileInputs downloads, opens, and validates cube-file readers for keys concurrently
// (one goroutine and one Get round trip per file, unconditionally) — mirrors
// executor.FindTraceGroupInCandidates' own no-cap fan-out: this is I/O-bound (object-store round
// trip dominates), and a wide time-range query can have dozens of L0 files, so fetching them one
// at a time serialized the query's whole latency on round-trip count. Results are collected into
// a slice indexed by each key's original position and filtered back into inputs in that same
// order once every fetch has resolved.
func (q *CubeQueryPath) fetchCubeFileInputs(
	ctx context.Context,
	keys []string,
	entry CubeRegistryEntry,
) ([]CubeRollupInput, int64, error) {
	results := make([]cubeFileFetchResult, len(keys))
	var cubeBytesRead int64
	g, gctx := errgroup.WithContext(ctx)
	for i, key := range keys {
		i, key := i, key
		g.Go(func() (err error) {
			// A panic in a goroutine (unlike one in this call's own stack) is NOT caught by the
			// request handler's own recover middleware and crashes the whole process -- this
			// guard is load-bearing for the fan-out, not just defensive mirroring.
			defer func() {
				if rec := recover(); rec != nil {
					err = fmt.Errorf("fetchCubeFileInputs: fetch cube file %q: panic: %v", key, rec)
				}
			}()
			r, getErr := q.files.Get(gctx, key)
			if getErr != nil {
				return nil // routine download/open failure — not a registry/file drift
			}
			atomic.AddInt64(&cubeBytesRead, r.BytesRead())
			if ok, _ := classifyCubeFile(r, entry); !ok {
				// A registry/file drift (corruption, a buggy writer, or a stale registry entry) —
				// excluded from CubeRollup's inputs with the same posture as the routine failures
				// above.
				return nil
			}
			results[i] = cubeFileFetchResult{input: CubeNewRollupInput(r), ok: true}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		// A goroutine panicked -- treat as no cube coverage rather than propagating, so a single
		// corrupt file can't take down the query handler.
		return nil, cubeBytesRead, err
	}
	inputs := make([]CubeRollupInput, 0, len(keys))
	for _, res := range results {
		if res.ok {
			inputs = append(inputs, res.input)
		}
	}
	return inputs, cubeBytesRead, nil
}

// rollupCubeInputs merges opened cube readers using the ROUTED resolution level (result.Resolution
// — Route's own decision) as CubeRollup's target level, never a hardcoded value. Extracted into
// its own function so a test can construct a fake CubeRoutingResult and real cube readers and
// assert CubeRollup was invoked with THAT resolution, independently of QueryRange's I/O wiring.
func rollupCubeInputs(
	inputs []CubeRollupInput,
	result CubeRoutingResult,
	minMinute, maxMinute uint32,
) ([]CubeMergedCell, error) {
	return CubeRollup(inputs, result.Resolution, minMinute, maxMinute)
}

// cubeCoveredWindow (#217/SPEC-CUBE-028, ruling 4(b) revisit) narrows [reqMinMinute, reqMaxMinute]
// to result's actual covered sub-range (CoveredMinMinute/CoveredMaxMinute), reporting whether
// coverage is partial. Pure — no I/O.
func cubeCoveredWindow(
	result CubeRoutingResult,
	reqMinMinute, reqMaxMinute uint32,
) (minMinute, maxMinute uint32, partial bool) {
	partial = result.CoveredMinMinute > reqMinMinute || result.CoveredMaxMinute < reqMaxMinute
	if !partial {
		return reqMinMinute, reqMaxMinute, false
	}
	return result.CoveredMinMinute, result.CoveredMaxMinute, true
}

// classifyCubeFile validates an already-opened reader's declared NumAggAttrs against entry (a
// registry-vs-file consistency check) — a pure comparison, no I/O. Returns ok=false when they
// mismatch; the caller must exclude the file from CubeRollup's inputs.
func classifyCubeFile(r *CubeReader, entry CubeRegistryEntry) (ok bool, mismatchErr error) {
	if err := CubeValidateFileMatchesRegistry(r.NumAggAttrs(), entry); err != nil {
		return false, err
	}
	return true, nil
}

// filterDedupKey renders a canonical filter set into a stable string for use in the
// per-(tenant+dims+filters) creation-cooldown dedup key.
func filterDedupKey(filters []CubeColumnFilter) string {
	if len(filters) == 0 {
		return ""
	}
	var b strings.Builder
	for i, f := range filters {
		if i > 0 {
			b.WriteByte(';')
		}
		v, _ := f.Value.(string)
		b.WriteString(f.Column)
		b.WriteByte(':')
		b.WriteString(string(f.Op))
		b.WriteByte(':')
		b.WriteString(v)
	}
	return b.String()
}

// buildAggAttrs builds the AggAttrs set a newly-created cube materializes: duration
// unconditionally, plus the triggering query's own needed attribute when extractAggAttr found
// one and it isn't duration itself (avoiding a duplicate entry).
func buildAggAttrs(neededAttr string, neededAttrType CubeAggAttrType, neededAttrOK bool) []CubeAggAttrDef {
	aggAttrs := []CubeAggAttrDef{{Column: CubeDurationColumn, Type: CubeAggAttrTypeInt64}}
	if neededAttrOK && neededAttr != CubeDurationColumn {
		aggAttrs = append(aggAttrs, CubeAggAttrDef{Column: neededAttr, Type: neededAttrType})
	}
	return aggAttrs
}

// maybeCreateCube fires TryCreate for a (tenant, dims, filters) pattern that had no usable cube
// coverage. Rate-limited to at most once per q.cfg.CreateCooldown per (tenant+dims+filters) key
// to prevent a per-block fan-out from creating a storm of concurrent creation attempts. The
// filters are part of the cube identity: two queries with the same group-by dims but different
// filters must create (and route to) distinct cubes.
//
// neededAttr/neededAttrType/neededAttrOK are the triggering query's own needed-attribute
// extraction result — when neededAttrOK, the newly-created cube's AggAttrs is {duration} ∪
// {neededAttr}, so the very first query for a pattern needing e.g. sum_over_time(x) creates a
// cube that can actually answer it, not a count-only cube requiring a second creation later.
//
// minTS/maxTS are the query window in unix seconds; they scope the VCNT read used by the
// cardinality gate to the same window the query asked for.
//
// q.cfg.OnCreateAttempt, if non-nil, is invoked exactly once per call that reaches TryCreate
// (i.e. not on a cooldown-suppressed call), with the outcome of that attempt -- this is the
// ONLY point at which blockpack surfaces the result to the caller (tempo), since blockpack owns
// no logging/metrics and must never import tempo's jobstore package directly.
func (q *CubeQueryPath) maybeCreateCube(
	ctx context.Context,
	tenant string,
	dims []string,
	filters []CubeColumnFilter,
	neededAttr string,
	neededAttrType CubeAggAttrType,
	neededAttrOK bool,
	minTS, maxTS uint64,
) {
	if q.pgPool == nil {
		return
	}

	key := tenant + "|" + strings.Join(dims, ",") + "|" + filterDedupKey(filters)

	q.mu.Lock()
	if last, ok := q.createSeen[key]; ok && time.Since(last) < q.cfg.CreateCooldown {
		q.mu.Unlock()
		return // already attempted recently
	}
	q.createSeen[key] = time.Now()
	q.mu.Unlock()

	reg := NewPgCubeRegistry(q.pgPool, tenant)
	trigger := NewCubeCreationTrigger(reg, q.cfg.TriggerConfig)
	// Fetch real VCNT data for the proposed dimensions over the query window so the cardinality
	// gate runs against actual per-dimension distinct-value counts instead of nil. If no VCNT
	// coverage exists for the dims/window, the section is empty and the gate passes by default.
	// q.vi may be nil (NewCubeQueryPath allows it); buildVCNTSection is the ONE shared free
	// function also used by cube_backfill_runner.go's RunCubeBackfill path (avoids the
	// two-independently-typed-copy drift risk NOTE-CUBE-030 already warns about), which has no
	// nil-store guard of its own, so that check stays here at the call site.
	var vcntData []byte
	var vcntDir []VCNTChunkDirEntry
	if q.vi != nil {
		vcntData, vcntDir = buildVCNTSection(ctx, q.vi, tenant, dims, minTS, maxTS)
	}
	// Every v2 cube always materializes duration; the triggering query's own attribute (if any)
	// joins the set so the cube this query creates can immediately answer it.
	aggAttrs := buildAggAttrs(neededAttr, neededAttrType, neededAttrOK)
	result, err := trigger.TryCreate(ctx, tenant, dims, filters, aggAttrs, vcntData, vcntDir, minTS, maxTS)
	if err != nil {
		if q.cfg.OnCreateAttempt != nil {
			q.cfg.OnCreateAttempt(ctx, CubeRegistryEntry{}, false, false, err)
		}
		return
	}
	if result.Created {
		q.invalidateCache(tenant) // reload registry next time
		if q.cfg.OnCreateAttempt != nil {
			q.cfg.OnCreateAttempt(ctx, result.Entry, true, false, nil)
		}
		return
	}

	// The cube already exists, but may never have completed a single successful backfill pass.
	// A cube's RegistryEntry has no explicit "backfill done" flag; the absence of an L0
	// watermark is the cheapest available heuristic for "never completed a single successful
	// backfill pass" -- a cube that finished at least one pass has a CubeRollupL0 watermark
	// entry, so its absence here means backfill either never ran or never succeeded even once.
	_, hasL0 := result.Entry.Watermarks[CubeRollupL0]
	if q.cfg.OnCreateAttempt != nil {
		q.cfg.OnCreateAttempt(ctx, result.Entry, false, hasL0, nil)
	}
}

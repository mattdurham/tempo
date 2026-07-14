package vblockpack

// cubequerypath.go — cube query path for metrics queries.
//
// On every QueryRange call, CubeQueryPath:
//  1. Parses the query to extract group-by dimensions.
//  2. Looks up the registry for a matching cube.
//  3. If found → reads cube files from S3 and returns exact counts.
//  4. If not found → fires TryCreate (cardinality gate via VCNT data).
//
// On any error or cache miss, falls through to the VI/metrics decline path (ExecuteMetricsTraceQL's
// own decline contract), which itself hard-errors with one of blockpack's F-4 sentinels — there is
// no full block scan fallback.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/tempo/pkg/tempopb"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/traceql"
	util_log "github.com/grafana/tempo/pkg/util/log"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	"github.com/grafana/tempo/tempodb/encoding/vblockpack/jobstore"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
)

// cubeQueryPath is the process-level cube query path manager.
type cubeQueryPath struct {
	client *minio.Client
	bucket string
	// store, when non-nil, is used in place of a minioObjectStore{client, bucket} wrapper —
	// a test-only dependency-injection seam (F-10, issue #481 part 3) so tryQueryFromCube's
	// cube-not-found/warming branch is testable through the REAL production entry point
	// without a live S3/minio server. ConfigureCubeQueryPath (production) never sets this;
	// ordinary production behavior is completely unchanged (objectStore() falls back to
	// wrapping client/bucket exactly as before this field existed).
	store blockpack.CubeObjectStore
	// per-tenant registry cache (refreshed every 5m)
	mu      sync.RWMutex
	tenants map[string]*tenantCubeState
	// createCooldown rate-limits cube creation to at most once per minute
	// per (tenant+dims) key, preventing per-block fan-out storms.
	createSeen map[string]time.Time
	// jobStore is the opt-in durable backend_jobs queue (#181) -- nil when
	// Postgres isn't configured. Inserting via jobStore in maybeCreateCube's
	// Created branch is purely additive: it does NOT replace launchBackfill,
	// both run.
	jobStore *jobstore.Store
}

// objectStore returns cqp.store if injected (tests), otherwise the real minio-backed store —
// the SINGLE construction point both loadEntries and maybeCreateCube use, so the two call
// sites can never drift on which store a cqp instance actually talks to.
func (cqp *cubeQueryPath) objectStore() blockpack.CubeObjectStore {
	if cqp.store != nil {
		return cqp.store
	}
	return &minioObjectStore{client: cqp.client, bucket: cqp.bucket}
}

type tenantCubeState struct {
	entries     []blockpack.CubeRegistryEntry
	lastRefresh time.Time
}

var (
	processCubeQueryPath   *cubeQueryPath
	processCubeQueryPathMu sync.RWMutex
	cubeQueryPathOnce      sync.Once
)

// ConfigureCubeQueryPath sets up the cube query path on the querier at startup.
// pgPool is the opt-in Postgres backend for the durable backend_jobs queue
// (#181) -- nil means "not configured," mirroring ConfigureCubeManager/
// ConfigureViUsage's own pgPool convention.
func ConfigureCubeQueryPath(enabled bool, s3cfg *s3backend.Config, pgPool *pgxpool.Pool) {
	if !enabled || s3cfg == nil {
		return
	}
	cubeQueryPathOnce.Do(func() {
		client, err := newMinioClientFromS3Config(s3cfg)
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube query path disabled", "err", err)
			return
		}
		var jobStore *jobstore.Store
		if pgPool != nil {
			jobStore = jobstore.New(pgPool)
		}
		processCubeQueryPathMu.Lock()
		processCubeQueryPath = &cubeQueryPath{
			client:     client,
			bucket:     s3cfg.Bucket,
			tenants:    make(map[string]*tenantCubeState),
			createSeen: make(map[string]time.Time),
			jobStore:   jobStore,
		}
		processCubeQueryPathMu.Unlock()
		level.Info(util_log.Logger).Log("msg", "vblockpack: cube query path configured")
	})
}

func getCubeQueryPath() *cubeQueryPath {
	processCubeQueryPathMu.RLock()
	defer processCubeQueryPathMu.RUnlock()
	return processCubeQueryPath
}

// loadEntries returns cached (or freshly-loaded) cube entries for a tenant.
func (cqp *cubeQueryPath) loadEntries(ctx context.Context, tenant string) ([]blockpack.CubeRegistryEntry, error) {
	cqp.mu.Lock()
	st, ok := cqp.tenants[tenant]
	if ok && time.Since(st.lastRefresh) < 5*time.Minute {
		entries := st.entries
		cqp.mu.Unlock()
		return entries, nil
	}
	cqp.mu.Unlock()

	os := cqp.objectStore()
	reg := blockpack.NewCubeRegistry(os, tenant)
	entries, _, err := reg.Load(ctx)
	if err != nil {
		return nil, err
	}

	cqp.mu.Lock()
	cqp.tenants[tenant] = &tenantCubeState{entries: entries, lastRefresh: time.Now()}
	cqp.mu.Unlock()
	return entries, nil
}

// invalidateCache forces a registry reload on the next call for this tenant.
func (cqp *cubeQueryPath) invalidateCache(tenant string) {
	cqp.mu.Lock()
	delete(cqp.tenants, tenant)
	cqp.mu.Unlock()
}

// ErrCubeWarming (issue #481 part 3, F-10, R1's self-healing story) is returned by
// tryQueryFromCube ONLY for the specific "cube not found (or resolution-incomplete), cube
// creation just fired" case — cubequerypath.go's `!result.Found` branch below, which triggers
// maybeCreateCube. This is a tempo-side sentinel, not a shared enum value with blockpack's F-4
// family (team-lead ruling: cubequerypath.go is tempo code, cube-side declines are tempo's own
// taxonomy) — it distinguishes "this shape IS cube-answerable, just not backfilled yet, retry
// shortly" from every OTHER false reason (no group-by dims, filter not cube-representable,
// registry load failure, list/download/decode failure, rollup failure) — those remain a silent
// (nil, false, nil) fallback to the VI/typed-error path below with no actionable "retry" signal,
// since they are not self-healing in the same way (a repeat query would hit the identical
// permanent condition, not a transient warming window). Joins F-10's declineErrorToHTTPResponse
// mapper as a 4xx-class, actionable "retry shortly" case.
var ErrCubeWarming = errors.New("vblockpack: cube not yet backfilled for this shape/window; creation triggered, retry shortly")

// tryQueryFromCube attempts to answer req from cube files. Returns (result, true, nil) when the
// cube path answered the query. Returns (nil, false, err) to fall back to block scan/VI — err is
// ErrCubeWarming specifically when cube creation was just triggered (see its own doc comment),
// nil for every other "cannot answer from cube" reason (not self-healing, no retry signal).
func (cqp *cubeQueryPath) tryQueryFromCube(
	ctx context.Context,
	tenant string,
	req *tempopb.QueryRangeRequest,
) (*tempopb.QueryRangeResponse, bool, error) {
	dims := extractGroupByDims(req.Query)
	if len(dims) == 0 {
		level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: no group-by dims in query", "query", req.Query)
		return nil, false, nil // no group-by → cube not applicable
	}

	// The query's {...} predicate is part of the cube identity (#480): a cube built for
	// one filter must not answer a query with a different filter. If the predicate cannot
	// be faithfully canonicalized into cube filters, the cube path is unsafe — fall back.
	filters, filtersOK := extractFilters(req.Query)
	if !filtersOK {
		level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: filter not cube-representable; falling back", "query", req.Query)
		return nil, false, nil
	}
	level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: found dims", "dims", strings.Join(dims, ","), "filters", filterDedupKey(filters), "tenant", tenant)

	entries, err := cqp.loadEntries(ctx, tenant)
	if err != nil {
		return nil, false, nil
	}

	// The materialized attribute (if any) this query's function needs — "" for count_over_time/
	// rate, which match any candidate cube regardless of its AggAttrs set (E-10).
	neededAttr, neededAttrType, neededAttrOK := extractAggAttr(req.Query)

	router := blockpack.NewCubeQueryRouter(entries)
	minMinute := uint32(req.Start / 60_000_000_000)
	maxMinute := uint32(req.End / 60_000_000_000)
	// req.Step is the query's requested granularity — the hardcoded resolution=1 (always L0)
	// this call used before E-10 ignored it entirely, serving every query at minute resolution
	// regardless of what it actually asked for.
	requestedResolution := requestedResolutionMinutes(req.Step)
	result, routeErr := router.Route(tenant, dims, filters, neededAttr, requestedResolution, minMinute, maxMinute)
	if routeErr != nil || !result.Found {
		// Cube not found, or found but with NO overlap at all with the requested window
		// (ruling 4(b) revisit, #217/SPEC-CUBE-028 — Route only returns Found=false now for "no
		// usable overlap," not merely "incomplete" coverage; see PARTIAL coverage handling below
		// for the edge-truncated case). Attempt cube creation on first query. Fire cube creation
		// in a background goroutine so QueryRange is not blocked. req.Start/req.End are
		// nanoseconds; VCNT records are keyed in unix seconds (minute-floored), so pass the
		// query window in seconds for the cardinality gate.
		minTS := req.Start / 1_000_000_000
		maxTS := req.End / 1_000_000_000
		go cqp.maybeCreateCube(context.Background(), tenant, dims, filters, neededAttr, neededAttrType, neededAttrOK, minTS, maxTS)
		return nil, false, ErrCubeWarming
	}
	// #217/SPEC-CUBE-028 (ruling 4(b) revisit, Phase 3.3): partial coverage — the cube's watermark
	// overlaps only PART of [minMinute, maxMinute]. Narrow the actual cell read to
	// result.CoveredMinMinute/CoveredMaxMinute (never read cells outside the confirmed-covered
	// range — reading past it would silently return an incomplete/wrong rollup for that edge) and
	// tag the response PartialStatus=PARTIAL with a message identifying the uncovered edge(s).
	//
	// Scope note (assessed and deliberately deferred, not silently dropped): the plan doc's own
	// Phase 3.3 additionally proposed dispatching the existing VI/scan fallback for exactly the
	// uncovered edge and merging it with the cube's answer within this same call. Investigated
	// and NOT implemented here: merging a second raw metrics result into an already-cube-rolled-
	// up answer requires selecting a correct re-aggregation mode (traceql.AggregateMode) generic
	// across every metrics function this path supports (count_over_time/rate/sum_over_time/
	// histogram_over_time/quantile_over_time) — sum-like functions merge safely, but rate() and
	// quantile_over_time() do NOT correctly re-aggregate via simple concatenation or summation
	// without re-deriving them from finer-grained inputs, and no existing call site in either
	// repo merges two already-computed metrics results this way today (grep-confirmed). Shipping
	// that merge without being able to verify numeric correctness for every supported function
	// risked a SILENT wrong-answer bug, which is worse than the honest partial answer served
	// here. Serving ONLY the cube's own covered sub-range (this fix) already delivers #217's
	// core guarantee — never decline a query we have coverage for — for the covered majority of
	// the window; the uncovered edge simply isn't answered by the cube path, exactly as if the
	// query had asked for only the covered sub-range. Flagged as a real, explicit follow-up.
	origMinMinute, origMaxMinute := minMinute, maxMinute
	minMinute, maxMinute, partialCoverage := cubeCoveredWindow(result, minMinute, maxMinute)
	if partialCoverage {
		level.Info(util_log.Logger).Log(
			"msg", "vblockpack: cube: partial coverage, serving covered sub-range only",
			"tenant", tenant, "cube_id", result.Entry.CubeID,
			"requested_min_minute", origMinMinute, "requested_max_minute", origMaxMinute,
			"covered_min_minute", minMinute, "covered_max_minute", maxMinute,
		)
	}

	// Cube found: list and download L0 files for the time window.
	prefix := path.Join(tenant, "cubes", result.Entry.CubeID) + "/"
	keys, listErr := cqp.listObjects(ctx, prefix)
	if listErr != nil || len(keys) == 0 {
		return nil, false, nil
	}

	// Download, open, and validate readers (APPENDIX 3: registry-vs-file consistency check).
	// cubeBytesRead (issue #218, Phase 5) accumulates every successfully-opened cube reader's
	// exact byte count (blockpack.CubeReader.BytesRead) — including files later excluded by
	// classifyCubeFile's registry-vs-file mismatch check, since the decode/download cost was
	// genuinely incurred regardless of whether the file ends up contributing to the rollup.
	var cubeBytesRead int64
	inputs := make([]blockpack.CubeRollupInput, 0, len(keys))
	for _, key := range keys {
		data, getErr := cqp.getObject(ctx, key)
		if getErr != nil {
			continue // routine download failure — not a registry/file drift, not logged as one
		}
		r, openErr := blockpack.OpenCubeReaderFromBytes(data)
		if openErr != nil {
			continue // routine decode failure — same posture as above
		}
		cubeBytesRead += r.BytesRead()
		if ok, mismatchErr := classifyCubeFile(r, result.Entry); !ok {
			// A registry/file drift (corruption, a buggy writer, or a stale registry entry) —
			// excluded from CubeRollup's inputs with the SAME posture as the routine failures
			// above, but logged DISTINCTLY so the drift is operationally discoverable, never
			// silently indistinguishable "noise" (APPENDIX 3).
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: cube: registry-vs-file aggAttrs mismatch, excluding file",
				"tenant", tenant, "cube_id", result.Entry.CubeID, "key", key, "err", mismatchErr,
			)
			continue
		}
		inputs = append(inputs, blockpack.CubeNewRollupInput(r))
	}
	if len(inputs) == 0 {
		return nil, false, nil
	}

	cells, rollupErr := rollupCubeInputs(inputs, result, minMinute, maxMinute)
	if rollupErr != nil {
		return nil, false, nil
	}

	resp := buildCubeQueryResponse(cells, result.Entry.Dimensions, result.Entry.AggAttrs, req, cubeBytesRead)
	if partialCoverage {
		resp.Status = tempopb.PartialStatus_PARTIAL
		resp.Message = cubePartialCoverageMessage(minMinute, maxMinute, origMinMinute, origMaxMinute)
	}
	return resp, true, nil
}

// rollupCubeInputs merges opened cube readers using the ROUTED resolution level
// (result.Resolution — Route's own decision, E-10) as CubeRollup's target level, never a
// hardcoded value. Extracted into its own function (mirroring #44's filterValidCubeDefs
// extraction pattern) so a test can construct a fake CubeRoutingResult and real cube readers (via
// the production write path, Lesson 2) and assert CubeRollup was invoked with THAT resolution,
// independently of tryQueryFromCube's S3/minio wiring which is otherwise untestable in isolation.
func rollupCubeInputs(
	inputs []blockpack.CubeRollupInput,
	result blockpack.CubeRoutingResult,
	minMinute, maxMinute uint32,
) ([]blockpack.CubeMergedCell, error) {
	return blockpack.CubeRollup(inputs, result.Resolution, minMinute, maxMinute)
}

// cubeCoveredWindow (#217/SPEC-CUBE-028, ruling 4(b) revisit, Phase 3.3) narrows
// [reqMinMinute, reqMaxMinute] to result's actual covered sub-range
// (CoveredMinMinute/CoveredMaxMinute), reporting whether coverage is partial. Pure — no I/O —
// extracted into its own function for the SAME reason rollupCubeInputs was (see its own doc
// comment): tryQueryFromCube's S3/minio wiring is otherwise untestable in isolation, but the
// narrowing/partial-detection logic itself is worth testing directly against a real
// router.Route result.
func cubeCoveredWindow(result blockpack.CubeRoutingResult, reqMinMinute, reqMaxMinute uint32) (minMinute, maxMinute uint32, partial bool) {
	partial = result.CoveredMinMinute > reqMinMinute || result.CoveredMaxMinute < reqMaxMinute
	if !partial {
		return reqMinMinute, reqMaxMinute, false
	}
	return result.CoveredMinMinute, result.CoveredMaxMinute, true
}

// cubePartialCoverageMessage builds the PartialStatus_PARTIAL message for a cube answer that
// only covers [coveredMin,coveredMax] of the originally requested [reqMin,reqMax]. Pure — no
// I/O — for the same isolation-testability reason as cubeCoveredWindow above.
func cubePartialCoverageMessage(coveredMin, coveredMax, reqMin, reqMax uint32) string {
	return fmt.Sprintf(
		"cube covers minutes [%d,%d] of the requested [%d,%d]; the uncovered edge is not answered by this cube",
		coveredMin, coveredMax, reqMin, reqMax,
	)
}

// maybeCreateCube fires TryCreate for a (tenant, dims, filters) pattern that had no
// cube. It is rate-limited to at most once per minute per (tenant+dims+filters) key to
// prevent the per-block fan-out from creating a storm of concurrent S3 ConditionalPuts.
// The filters are part of the cube identity (#480): two queries with the same group-by
// dims but different filters must create (and route to) distinct cubes.
//
// neededAttr/neededAttrType/neededAttrOK are extractAggAttr's result for the triggering query
// (E-10) — when neededAttrOK, the newly-created cube's AggAttrs is {duration} ∪
// {neededAttr}, so the very first query for a pattern needing e.g. sum_over_time(x) creates a
// cube that can actually answer it, not a count-only cube requiring a second creation later.
//
// minTS/maxTS are the query window in unix seconds; they scope the VCNT read used by
// the cardinality gate to the same window the query asked for.
func (cqp *cubeQueryPath) maybeCreateCube(
	ctx context.Context,
	tenant string,
	dims []string,
	filters []blockpack.CubeColumnFilter,
	neededAttr string,
	neededAttrType blockpack.CubeAggAttrType,
	neededAttrOK bool,
	minTS, maxTS uint64,
) {
	key := tenant + "|" + strings.Join(dims, ",") + "|" + filterDedupKey(filters)

	cqp.mu.Lock()
	if last, ok := cqp.createSeen[key]; ok && time.Since(last) < time.Minute {
		cqp.mu.Unlock()
		return // already attempted recently
	}
	cqp.createSeen[key] = time.Now()
	cqp.mu.Unlock()

	os := cqp.objectStore()
	reg := blockpack.NewCubeRegistry(os, tenant)
	trigger := blockpack.NewCubeCreationTrigger(reg, blockpack.CubeTriggerConfig{})
	// Fetch real VCNT data for the proposed dimensions over the query window so the
	// cardinality gate runs against actual per-dimension distinct-value counts
	// instead of nil (#483). Reads go through the same shared cachingStore-wrapped
	// minioVIStore the backfill/query paths use. If no VCNT coverage exists for the
	// dims/window, the section is empty and the gate passes by default — matching
	// the prior best-effort behaviour rather than blocking cube creation.
	vcntData, vcntDir := cqp.fetchVCNTSection(ctx, tenant, dims, minTS, maxTS)
	// Every v2 cube always materializes duration (ruling 3); the triggering query's own
	// attribute (if any) joins the set so the cube this query creates can immediately answer
	// it, per E-10.
	aggAttrs := buildAggAttrs(neededAttr, neededAttrType, neededAttrOK)
	result, err := trigger.TryCreate(ctx, tenant, dims, filters, aggAttrs, vcntData, vcntDir, minTS, maxTS)
	if err != nil {
		level.Warn(util_log.Logger).Log("msg", "vblockpack: cube TryCreate failed", "tenant", tenant, "dims", dims, "err", err)
		return
	}
	if result.Created {
		level.Info(util_log.Logger).Log(
			"msg", "vblockpack: cube created on first query",
			"tenant", tenant, "dims", dims,
			"cube_id", result.Entry.CubeID,
		)
		cqp.invalidateCache(tenant) // reload registry next time
		if cqp.jobStore != nil {
			if ierr := cqp.jobStore.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
				CubeID:        result.Entry.CubeID,
				WindowMinutes: math.MaxUint32,
			}); ierr != nil {
				level.Warn(util_log.Logger).Log(
					"msg", "vblockpack: failed to insert durable cube_backfill job",
					"tenant", tenant, "cube_id", result.Entry.CubeID, "err", ierr,
				)
			}
		}
		// Kick off historical backfill from the value index.
		launchBackfill(result.Entry)
		return
	}

	// #181 §6.3/§9 Phase 3: the cube already exists, but a cube whose single backfill
	// attempt exhausted its retries has no OTHER path back into backend_jobs now that
	// the poll is gone (§5.3's coverage-gap finding) -- this query-path re-evaluation
	// (already firing at the createSeen cadence above) is the only remaining trigger
	// point. Cube's RegistryEntry has no explicit "backfill done" flag (unlike VI's
	// Entry.Backfill.Done); the absence of an L0 watermark is the cheapest available
	// heuristic for "never completed a single successful backfill pass" -- a cube that
	// finished at least one pass has a CubeRollupL0 watermark entry (E-12a's
	// Compactor.Execute populates it on every successful rollup write), so its absence
	// here means backfill either never ran or never succeeded even once.
	if cqp.jobStore == nil {
		return
	}
	if _, hasL0 := result.Entry.Watermarks[blockpack.CubeRollupL0]; hasL0 {
		return
	}
	if ierr := cqp.jobStore.InsertCubeBackfill(ctx, tenant, jobstore.CubeBackfillDetail{
		CubeID:        result.Entry.CubeID,
		WindowMinutes: math.MaxUint32,
	}); ierr != nil {
		level.Warn(util_log.Logger).Log(
			"msg", "vblockpack: failed to insert durable cube_backfill retry job",
			"tenant", tenant, "cube_id", result.Entry.CubeID, "err", ierr,
		)
	}
}

// requestedResolutionMinutes converts a query's Step (nanoseconds) into the router's requested
// resolution unit (minutes) — E-10 replaces the previous hardcoded resolution=1 (always L0) with
// the query's own actual requested granularity, so router.ResolutionLevel can select L1/L2 for
// coarse-step queries instead of always serving minute resolution.
func requestedResolutionMinutes(stepNanos uint64) uint32 {
	return uint32(stepNanos / 60_000_000_000) //nolint:gosec // step fits uint32 for any realistic query window
}

// buildAggAttrs builds the AggAttrs set a newly-created cube materializes: duration
// unconditionally (ruling 3), plus the triggering query's own needed attribute when
// extractAggAttr found one and it isn't duration itself (avoiding a duplicate entry).
func buildAggAttrs(neededAttr string, neededAttrType blockpack.CubeAggAttrType, neededAttrOK bool) []blockpack.CubeAggAttrDef {
	aggAttrs := []blockpack.CubeAggAttrDef{{Column: blockpack.CubeDurationColumn, Type: blockpack.CubeAggAttrTypeInt64}}
	if neededAttrOK && neededAttr != blockpack.CubeDurationColumn {
		aggAttrs = append(aggAttrs, blockpack.CubeAggAttrDef{Column: neededAttr, Type: neededAttrType})
	}
	return aggAttrs
}

// classifyCubeFile validates an already-opened reader's declared NumAggAttrs against entry
// (APPENDIX 3's registry-vs-file consistency check) — a pure comparison, no I/O. Returns
// ok=false when they mismatch; the caller must exclude the file from CubeRollup's inputs but log
// the mismatch DISTINCTLY from a routine download/decode failure (which never reaches this
// function at all, being a categorically different code path), so a real registry/file drift is
// operationally discoverable rather than indistinguishable "noise."
func classifyCubeFile(r *blockpack.CubeReader, entry blockpack.CubeRegistryEntry) (ok bool, mismatchErr error) {
	if err := blockpack.CubeValidateFileMatchesRegistry(r.NumAggAttrs(), entry); err != nil {
		return false, err
	}
	return true, nil
}

// listObjects returns all object keys under prefix.
func (cqp *cubeQueryPath) listObjects(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range cqp.client.ListObjects(ctx, cqp.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// getObject downloads one object from S3.
func (cqp *cubeQueryPath) getObject(ctx context.Context, key string) ([]byte, error) {
	obj, err := cqp.client.GetObject(ctx, cqp.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	var buf bytes.Buffer
	_, err = io.Copy(&buf, obj)
	return buf.Bytes(), err
}

// byDimGroupRe matches "by (\s*col1\s*,\s*col2\s*)" in a TraceQL metrics query.
var byDimGroupRe = regexp.MustCompile(`\|\s*(?:rate|count_over_time|sum_over_time|min_over_time|max_over_time)\s*\([^)]*\)\s*by\s*\(([^)]+)\)`)

// aggFuncArgRe matches "func_name(attr" for the metrics functions that take a materialized
// aggregate attribute as their first argument (sum/min/max/avg/histogram/quantile_over_time).
// Captures the attribute up to the first ',' or ')' — quantile_over_time's second argument (the
// percentile) is deliberately excluded by this boundary.
var aggFuncArgRe = regexp.MustCompile(
	`\|\s*(?:sum_over_time|min_over_time|max_over_time|avg_over_time|histogram_over_time|quantile_over_time)\s*\(\s*([^,)]+)\s*[,)]`,
)

// extractAggAttr extracts the materialized aggregate attribute column from a TraceQL metrics
// query, e.g. "sum_over_time(span:duration)" -> ("span:duration", CubeAggAttrTypeInt64, true).
// Returns ("", _, false) for count_over_time/rate (#490: no specific attribute is needed — Route's
// own neededAttr=="" convention matches any candidate) or when no recognized function is present.
//
// aggType is a query-text-only heuristic: the mandatory duration column is always Int64-typed
// (matching ruling 1's own duration/int64 pairing); any OTHER extracted attribute defaults to
// Float64 — a safe default since a Float64-typed attribute still gets full Sum/Min/Max/Avg
// support and only loses Buckets[] population (ruling 1's own, upstream-imposed TypeFloat
// decline), never an incorrect value. The query text alone cannot know a general attribute's
// runtime type; guessing Int64 for a value that is actually a float would silently corrupt
// Buckets[] with a bad cast, which is the worse failure mode of the two.
func extractAggAttr(query string) (col string, aggType blockpack.CubeAggAttrType, ok bool) {
	m := aggFuncArgRe.FindStringSubmatch(query)
	if len(m) < 2 {
		return "", blockpack.CubeAggAttrTypeFloat64, false
	}
	col = strings.TrimSpace(m[1])
	if col == blockpack.CubeDurationColumn {
		return col, blockpack.CubeAggAttrTypeInt64, true
	}
	return col, blockpack.CubeAggAttrTypeFloat64, true
}

// metricsFunctionRe captures which metrics function a TraceQL query uses.
var metricsFunctionRe = regexp.MustCompile(
	`\|\s*(rate|count_over_time|sum_over_time|min_over_time|max_over_time|avg_over_time|histogram_over_time|quantile_over_time)\s*\(`,
)

// extractMetricsFunction returns the metrics function name in query (e.g. "sum_over_time"), or
// "" if none of the recognized functions is present.
func extractMetricsFunction(query string) string {
	m := metricsFunctionRe.FindStringSubmatch(query)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// aggAttrIndex returns the index of col within aggAttrNames (RegistryEntry.AggAttrs — the SAME
// ordering every CubeMergedCell.AggAttrs slice uses, E-5/E-9), or -1 if absent.
func aggAttrIndex(aggAttrNames []string, col string) int {
	for i, name := range aggAttrNames {
		if name == col {
			return i
		}
	}
	return -1
}

// quantilePercentileRe captures quantile_over_time's second argument, the requested percentile
// (e.g. "quantile_over_time(span:duration, .5)" -> "0.5"). extractAggAttr's own aggFuncArgRe
// deliberately stops at the first ',' to capture only the attribute; this regex captures the
// percentile that comes after it.
var quantilePercentileRe = regexp.MustCompile(`\|\s*quantile_over_time\s*\([^,]+,\s*([0-9.]+)\s*\)`)

// extractQuantilePercentile returns quantile_over_time's requested percentile p, or (0, false)
// when the query is not quantile_over_time or the percentile cannot be parsed.
func extractQuantilePercentile(query string) (float64, bool) {
	m := quantilePercentileRe.FindStringSubmatch(query)
	if len(m) < 2 {
		return 0, false
	}
	p, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, false
	}
	return p, true
}

// cellValueForFunction extracts the scalar value buildCubeQueryResponse serves for one merged
// cell, dispatched by the query's metrics function (E-11a/E-11b). sum/avg/min/max all short-
// circuit to NaN when the target aggAttr's SampleCount==0 (no valid samples merged into this cell
// at this resolution). sum's NaN short-circuit (task #50) matches pkg/traceql's own post-#47
// sumOverTime(): a running sum that never observes a real value stays NaN-seeded forever
// (TestSumOverTime_AllValuesMissing_StaysNaN) rather than reporting the additive identity 0 —
// parity requires the cube path to answer identically to a query that falls back to the full
// scan path on the same all-missing-attribute data (min/max/avg already followed this
// convention; sum previously did not, predating #47). quantile_over_time short-circuits to NaN
// the same way when Log2QuantileFromBuckets finds every bucket empty (bucketIdx==-1) — the
// natural "decline" case for a Float64-typed aggAttr, which ruling 1 never populates Buckets[]
// for, with no separate type-check needed (E-11b). count_over_time/rate are unaffected,
// continuing to read c.Count directly — E-11a's own stated, unchanged baseline — which is also
// what a targetIdx of -1 (no recognized attribute, or the function needs none) falls back to.
// histogram_over_time is NOT dispatched here — its response shape fans out into multiple series
// per cell (buildHistogramResponse) rather than one scalar.
func cellValueForFunction(c blockpack.CubeMergedCell, function string, targetIdx int, quantileP float64) float64 {
	if targetIdx < 0 || targetIdx >= len(c.AggAttrs) {
		return float64(c.Count)
	}
	agg := c.AggAttrs[targetIdx]
	switch function {
	case "sum_over_time":
		if agg.SampleCount == 0 {
			return math.NaN()
		}
		return agg.Sum
	case "min_over_time":
		if agg.SampleCount == 0 {
			return math.NaN()
		}
		return agg.Min
	case "max_over_time":
		if agg.SampleCount == 0 {
			return math.NaN()
		}
		return agg.Max
	case "avg_over_time":
		if agg.SampleCount == 0 {
			return math.NaN()
		}
		return agg.Sum / float64(agg.SampleCount)
	case "quantile_over_time":
		v, bucketIdx := blockpack.CubeLog2QuantileFromBuckets(quantileP, agg.Buckets)
		if bucketIdx == -1 {
			return math.NaN()
		}
		return v
	default:
		return float64(c.Count)
	}
}

// extractGroupByDims parses dimension column names from a TraceQL metrics query's
// "by (dim1, dim2)" clause. Returns nil when there is no group-by.
func extractGroupByDims(query string) []string {
	m := byDimGroupRe.FindStringSubmatch(query)
	if len(m) < 2 {
		return nil
	}
	raw := m[1]
	parts := regexp.MustCompile(`\s*,\s*`).Split(raw, -1)
	dims := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			dims = append(dims, p)
		}
	}
	sort.Strings(dims)
	return dims
}

// filterOpFromTraceQL maps a TraceQL comparison operator to its cube-definition
// equivalent. Only operators a cube can bake in as an equality/range predicate are
// supported. Unsupported operators (!=, regex, etc.) return ok=false, which forces
// the caller to skip the cube path entirely rather than risk routing to a cube whose
// baked-in filter does not match the query — the silent-wrong-answer gap from #480.
func filterOpFromTraceQL(op traceql.Operator) (blockpack.CubeDefFilterOp, bool) {
	switch op {
	case traceql.OpEqual:
		return blockpack.CubeDefFilterOpEQ, true
	case traceql.OpGreater:
		return blockpack.CubeDefFilterOpGT, true
	case traceql.OpGreaterEqual:
		return blockpack.CubeDefFilterOpGTE, true
	case traceql.OpLess:
		return blockpack.CubeDefFilterOpLT, true
	case traceql.OpLessEqual:
		return blockpack.CubeDefFilterOpLTE, true
	default:
		return "", false
	}
}

// extractFilters canonicalizes the query's {...} predicate into the cube-definition
// filter set used as part of the cube routing/creation key (#480). It returns
// (filters, true) when every predicate in the query maps cleanly to a cube filter,
// and (nil, false) when the query contains any predicate that a cube cannot faithfully
// represent — in which case the caller must NOT use the cube path (fall back to a full
// scan) rather than route to a cube whose filter differs from the query's.
//
// A query with an empty {} predicate yields (nil-length-slice, true): an unfiltered
// cube is a distinct, valid identity (issue #480, decision (2)).
//
// The returned slice is deterministic (sorted by column, then op, then value) so the
// same query always produces the same routing key regardless of predicate ordering.
func extractFilters(query string) ([]blockpack.CubeColumnFilter, bool) {
	req, err := traceql.ExtractFetchSpansRequest(query)
	if err != nil {
		// Unparseable / unsupported metrics shape — cannot canonicalize the filter,
		// so the cube path is unsafe. Fall back.
		return nil, false
	}
	// AllConditions is false when the predicate uses OR semantics (or is otherwise not a
	// pure conjunction). A cube bakes in a conjunction of filters; anything else cannot be
	// represented and must fall back.
	if !req.AllConditions {
		return nil, false
	}

	filters := make([]blockpack.CubeColumnFilter, 0, len(req.Conditions))
	for _, c := range req.Conditions {
		// OpNone conditions carry no predicate (e.g. the synthetic spanStartTime
		// condition emitted for an empty {} query, or bare attribute-existence probes).
		if c.Op == traceql.OpNone {
			continue
		}
		op, ok := filterOpFromTraceQL(c.Op)
		if !ok {
			return nil, false // unsupported operator → unsafe to use a cube.
		}
		if len(c.Operands) != 1 {
			return nil, false // multi/zero-operand predicate → not a simple filter.
		}
		// Canonical value: the operand's stable encoded string form. Using the encoded
		// string uniformly (string, number, duration, status, kind) keeps the routing key
		// deterministic and JSON-serializable in the RegistryEntry.
		col := strings.TrimPrefix(c.Attribute.String(), ".")
		filters = append(filters, blockpack.CubeColumnFilter{
			Column: col,
			Op:     op,
			Value:  c.Operands[0].EncodeToString(false),
		})
	}

	sort.Slice(filters, func(i, j int) bool {
		if filters[i].Column != filters[j].Column {
			return filters[i].Column < filters[j].Column
		}
		if filters[i].Op != filters[j].Op {
			return filters[i].Op < filters[j].Op
		}
		vi, _ := filters[i].Value.(string)
		vj, _ := filters[j].Value.(string)
		return vi < vj
	})
	return filters, true
}

// filterDedupKey renders a canonical filter set into a stable string for use in the
// per-(tenant+dims+filters) creation-cooldown dedup key.
func filterDedupKey(filters []blockpack.CubeColumnFilter) string {
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

// dimLabels splits dims into its (dim1, dim2) label names — dim2 is "" when dims has fewer than
// 2 elements, the shared convention buildCubeQueryResponse and buildHistogramResponse both use.
func dimLabels(dims []string) (dim1Label, dim2Label string) {
	if len(dims) >= 1 {
		dim1Label = dims[0]
	}
	if len(dims) >= 2 {
		dim2Label = dims[1]
	}
	return dim1Label, dim2Label
}

// dimLabelKVs builds the dim1/dim2 KeyValue label pair for one series — shared by
// buildCubeQueryResponse (scalar functions) and buildHistogramResponse (histogram_over_time).
func dimLabelKVs(dim1Label, dim2Label, d1, d2 string) []commonpbv1.KeyValue {
	labels := []commonpbv1.KeyValue{
		{Key: dim1Label, Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: d1}}},
	}
	if dim2Label != "" {
		labels = append(labels, commonpbv1.KeyValue{
			Key:   dim2Label,
			Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: d2}},
		})
	}
	return labels
}

// buildCubeQueryResponse builds a QueryRangeResponse from rolled-up cube cells. cubeBytesRead
// (issue #218, Phase 5) is the exact total byte count of every cube file opened to answer this
// query (tryQueryFromCube's own accumulator) — set on the response's SearchMetrics.CubeBytesRead
// unconditionally; IndexBytesRead/DataFileBytesRead/VcntBytesRead are correctly left at their
// zero default here, since a cube-answered response never touches VI/scan/VCNT.
func buildCubeQueryResponse(
	cells []blockpack.CubeMergedCell,
	dims []string,
	aggAttrNames []string,
	req *tempopb.QueryRangeRequest,
	cubeBytesRead int64,
) *tempopb.QueryRangeResponse {
	// function/targetIdx select WHICH merged value each cell contributes (E-11a/E-11b) —
	// count_over_time and rate are unaffected, continuing to read c.Count directly (targetIdx
	// stays -1 for them, since extractAggAttr's ok is false).
	function := extractMetricsFunction(req.Query)
	targetIdx := -1
	if neededAttr, _, ok := extractAggAttr(req.Query); ok {
		targetIdx = aggAttrIndex(aggAttrNames, neededAttr)
	}

	// histogram_over_time's response shape fans out into multiple (dims, bucket) series per
	// cell rather than one scalar per cell — a structurally different shape from every other
	// function, so it gets its own builder (E-11b).
	if function == "histogram_over_time" {
		resp := buildHistogramResponse(cells, dims, targetIdx)
		resp.Metrics = &tempopb.SearchMetrics{CubeBytesRead: uint64(cubeBytesRead)} //nolint:gosec
		return resp
	}

	quantileP, _ := extractQuantilePercentile(req.Query)

	// Group cells into series by (dim1, dim2) label pair.
	type seriesKey struct{ d1, d2 string }
	seriesMap := make(map[seriesKey][]tempopb.Sample)

	for _, c := range cells {
		k := seriesKey{d1: c.Dim1Val, d2: c.Dim2Val}
		ts := int64(c.Minute) * 60 * 1_000_000_000 //nolint:gosec // minute fits int64
		seriesMap[k] = append(seriesMap[k], tempopb.Sample{
			TimestampMs: ts / 1_000_000,
			Value:       cellValueForFunction(c, function, targetIdx, quantileP),
		})
	}

	dim1Label, dim2Label := dimLabels(dims)

	var series []*tempopb.TimeSeries
	for k, samples := range seriesMap {
		series = append(series, &tempopb.TimeSeries{
			Labels:  dimLabelKVs(dim1Label, dim2Label, k.d1, k.d2),
			Samples: samples,
		})
	}
	return &tempopb.QueryRangeResponse{
		Series:  series,
		Metrics: &tempopb.SearchMetrics{CubeBytesRead: uint64(cubeBytesRead)}, //nolint:gosec
	}
}

// buildHistogramResponse builds histogram_over_time's response shape: one series per (dims,
// bucket boundary) tuple carrying the cubeBucketLabel TraceQL's own histogram/quantile
// aggregators expect, with per-minute sample values equal to that bucket's merged count. A cell
// whose target aggAttr is missing (targetIdx out of range) or whose Buckets[] is entirely empty
// (e.g. a Float64-typed aggAttr, which ruling 1 never populates) contributes no series — the
// histogram naturally declines rather than emitting a misleading all-zero series (E-11b). The
// bucket label uses traceql.LabelBucket directly (E-11b polish item 2) rather than a hardcoded
// duplicate string, so the two can never silently drift apart.
func buildHistogramResponse(cells []blockpack.CubeMergedCell, dims []string, targetIdx int) *tempopb.QueryRangeResponse {
	type seriesKey struct {
		d1, d2 string
		bucket float64
	}
	seriesMap := make(map[seriesKey][]tempopb.Sample)

	for _, c := range cells {
		if targetIdx < 0 || targetIdx >= len(c.AggAttrs) {
			continue
		}
		agg := c.AggAttrs[targetIdx]
		ts := int64(c.Minute) * 60 * 1_000_000_000 //nolint:gosec // minute fits int64
		for i, count := range agg.Buckets {
			if count == 0 {
				continue
			}
			k := seriesKey{d1: c.Dim1Val, d2: c.Dim2Val, bucket: blockpack.CubeBucketMax(i)}
			seriesMap[k] = append(seriesMap[k], tempopb.Sample{
				TimestampMs: ts / 1_000_000,
				Value:       float64(count),
			})
		}
	}

	dim1Label, dim2Label := dimLabels(dims)

	var series []*tempopb.TimeSeries
	for k, samples := range seriesMap {
		labels := dimLabelKVs(dim1Label, dim2Label, k.d1, k.d2)
		labels = append(labels, commonpbv1.KeyValue{
			Key:   traceql.LabelBucket,
			Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_DoubleValue{DoubleValue: k.bucket}},
		})
		series = append(series, &tempopb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		})
	}
	return &tempopb.QueryRangeResponse{Series: series}
}

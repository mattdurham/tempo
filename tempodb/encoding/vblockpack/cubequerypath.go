package vblockpack

// cubequerypath.go — cube query path for metrics queries.
//
// #508: the registry cache, routing, fan-out file fetch/rollup, and first-query creation
// trigger all moved into blockpack.CubeQueryPath (constructor NewCubeQueryPath, method
// QueryRange). This file is now a thin wrapper: it constructs blockpack.CubeQueryPath once
// (ConfigureCubeQueryPath), extracts TraceQL-string query shape (dims/filters/neededAttr —
// a hard type dependency on tempo's own tempopb/traceql packages, stays tempo-side), and
// builds the tempopb response from blockpack's result (also stays tempo-side).
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
	"context"
	"fmt"
	"math"
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

// cubeQueryPath is the process-level cube query path manager: a thin tempo-side wrapper
// around blockpack.CubeQueryPath.
type cubeQueryPath struct {
	client *minio.Client
	bucket string
	// jobStore is the opt-in durable backend_jobs queue (#181) -- nil when Postgres isn't
	// configured. Inserted from qp's OnCreateAttempt callback below; purely additive (does
	// NOT replace launchBackfill, both run).
	jobStore *jobstore.Store
	// pg backs the cube registry (issue #504: Postgres is now the only supported cube
	// registry backend, no blob/index.json fallback) -- also used by cube_backfill.go's
	// launchBackfill (which reads it off the shared *cubeQueryPath singleton via
	// getCubeQueryPath()).
	pg *blockpack.Postgres
	// qp owns the registry cache, routing, fan-out fetch/rollup, and creation-trigger
	// orchestration (#508) -- see blockpack.CubeQueryPath's own doc comment.
	qp *blockpack.CubeQueryPath
}

var (
	processCubeQueryPath   *cubeQueryPath
	processCubeQueryPathMu sync.RWMutex
	cubeQueryPathOnce      sync.Once
)

// ConfigureCubeQueryPath sets up the cube query path on the querier at startup.
// pg is the opt-in Postgres backend for the durable backend_jobs queue
// (#181) -- nil means "not configured," mirroring ConfigureCubeManager/
// ConfigureViUsage's own pg convention.
func ConfigureCubeQueryPath(enabled bool, s3cfg *s3backend.Config, pg *blockpack.Postgres) {
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
		var pgPool *pgxpool.Pool
		if pg != nil {
			pgPool = pg.Pool()
			jobStore = jobstore.New(pgPool)
		}
		bucket := s3cfg.Bucket
		// #508 Decision 2: files (Get/Put) uses cubeFileStore (already satisfies
		// blockpack.CubeFileStore, cube_compactor.go); lister is a plain raw prefix-lister
		// over the SAME client/bucket cubeFileStore uses, deliberately NOT going through
		// cubeFileStore.List's filename-parsing (would double the GET count per unmerged L0
		// file) -- minioVIStore.List is already a generic, prefix-parameterized lister, reused
		// here purely for its List method. vi reuses the same shared cachingStore-wrapped
		// minioVIStore the backfill/cardinality-gate path already reads through (issue #478).
		fileStore := &cubeFileStore{client: client, bucket: bucket}
		lister := &minioVIStore{client: client, bucket: bucket}
		viStore := newBackfillVIStore(client, bucket)
		qp := blockpack.NewCubeQueryPath(fileStore, lister, viStore, pgPool, blockpack.CubeQueryPathConfig{
			BackfillIndexPrefix: defaultValueIndexPref,
			// OnCreateAttempt fires synchronously from blockpack's own background trigger
			// goroutine, once per TryCreate attempt that isn't cooldown-suppressed --
			// mirrors the pre-#508 maybeCreateCube's Created/re-evaluation branches exactly,
			// just via a callback instead of inline logic (blockpack has no logging/metrics
			// dependency and must never import tempo's jobstore package directly).
			OnCreateAttempt: func(ctx context.Context, entry blockpack.CubeRegistryEntry, created, hasL0 bool, triggerErr error) {
				if triggerErr != nil {
					level.Warn(util_log.Logger).Log("msg", "vblockpack: cube TryCreate failed", "err", triggerErr)
					return
				}
				if created {
					level.Info(util_log.Logger).Log(
						"msg", "vblockpack: cube created on first query",
						"tenant", entry.Tenant, "cube_id", entry.CubeID,
					)
					dedicated := dedicatedColumnSet(getDedicatedColumnsForTenant(entry.Tenant))
					recordCubeColumnUsage(ctx, entry.Tenant, entry, dedicated, time.Now())
					if jobStore != nil {
						if ierr := jobStore.InsertCubeBackfill(ctx, entry.Tenant, jobstore.CubeBackfillDetail{
							CubeID:        entry.CubeID,
							WindowMinutes: math.MaxUint32,
						}); ierr != nil {
							level.Warn(util_log.Logger).Log(
								"msg", "vblockpack: failed to insert durable cube_backfill job",
								"tenant", entry.Tenant, "cube_id", entry.CubeID, "err", ierr,
							)
						}
					}
					// Kick off historical backfill from the value index. #508 Phase 12: inlined
					// from the former launchBackfill (cube_backfill.go) -- verified via grep that
					// this was its only remaining call site after this file's own Phase 11
					// rewrite, so a separate function added indirection with no other caller.
					go func() {
						metricCubeBackfillStarted.Inc()
						level.Info(util_log.Logger).Log(
							"msg", "vblockpack: cube backfill started",
							"tenant", entry.Tenant, "cube_id", entry.CubeID,
						)
						backfillCfg := blockpack.CubeBackfillConfig{
							Workers: 4,
							// WindowMinutes: full history, not an artificial cap (2026-07-11
							// ruling) -- math.MaxUint32 minutes trivially exceeds any real
							// currentMinute, so Backfiller.Run's endMinute always resolves to 0.
							WindowMinutes: math.MaxUint32,
						}
						backfillErr := blockpack.RunCubeBackfill(
							context.Background(), entry, viStore, &s3ObjectPutter{client: client, bucket: bucket},
							pgPool, backfillCfg, 0, defaultValueIndexPref,
						)
						if backfillErr != nil {
							metricCubeBackfillFailed.Inc()
							level.Warn(util_log.Logger).Log(
								"msg", "vblockpack: cube backfill error",
								"tenant", entry.Tenant, "cube_id", entry.CubeID, "err", backfillErr,
							)
							return
						}
						// blockpack.RunCubeBackfill returns nil if and only if the full backfill
						// window was genuinely exhausted (SPEC-CUBE-032) -- the metric increment
						// moves here, out of the per-minute progress callback, since blockpack
						// owns no metrics dependency.
						metricCubeBackfillCompleted.Inc()
					}()
					return
				}

				// #181 §6.3/§9 Phase 3: the cube already exists, but a cube whose single
				// backfill attempt exhausted its retries has no OTHER path back into
				// backend_jobs now that the poll is gone (§5.3's coverage-gap finding) --
				// this query-path re-evaluation is the only remaining trigger point. Cube's
				// RegistryEntry has no explicit "backfill done" flag; the absence of an L0
				// watermark (hasL0) is the cheapest available heuristic for "never completed
				// a single successful backfill pass."
				if jobStore == nil || hasL0 {
					return
				}
				dedicated := dedicatedColumnSet(getDedicatedColumnsForTenant(entry.Tenant))
				recordCubeColumnUsage(ctx, entry.Tenant, entry, dedicated, time.Now())
				if ierr := jobStore.InsertCubeBackfill(ctx, entry.Tenant, jobstore.CubeBackfillDetail{
					CubeID:        entry.CubeID,
					WindowMinutes: math.MaxUint32,
				}); ierr != nil {
					level.Warn(util_log.Logger).Log(
						"msg", "vblockpack: failed to insert durable cube_backfill retry job",
						"tenant", entry.Tenant, "cube_id", entry.CubeID, "err", ierr,
					)
				}
			},
		})
		processCubeQueryPathMu.Lock()
		processCubeQueryPath = &cubeQueryPath{
			client:   client,
			bucket:   bucket,
			jobStore: jobStore,
			pg:       pg,
			qp:       qp,
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

// ErrCubeWarming (issue #481 part 3, F-10, R1's self-healing story) is returned by
// tryQueryFromCube ONLY for the specific "cube not found (or resolution-incomplete), cube
// creation just fired" case. This is a tempo-side sentinel, not a shared enum value with
// blockpack's F-4 family (team-lead ruling: cubequerypath.go is tempo code, cube-side
// declines are tempo's own taxonomy) — it distinguishes "this shape IS cube-answerable, just
// not backfilled yet, retry shortly" from every OTHER false reason (no group-by dims, filter
// not cube-representable, registry load failure, list/download/decode failure, rollup
// failure) — those remain a silent (nil, false, nil) fallback to the VI/typed-error path
// below with no actionable "retry" signal, since they are not self-healing in the same way (a
// repeat query would hit the identical permanent condition, not a transient warming window).
// Joins F-10's declineErrorToHTTPResponse mapper as a 4xx-class, actionable "retry shortly"
// case.
//
// #508: aliased directly to blockpack.CubeErrWarming (same underlying value, not a separate
// wrapped error) so callers using errors.Is(err, ErrCubeWarming) keep working unchanged
// against the value blockpack.CubeQueryPath.QueryRange actually returns.
var ErrCubeWarming = blockpack.CubeErrWarming

// tryQueryFromCube attempts to answer req from cube files. Returns (result, true, nil) when the
// cube path answered the query. Returns (nil, false, err) to fall back to block scan/VI — err is
// ErrCubeWarming specifically when cube creation was just triggered (see its own doc comment),
// nil for every other "cannot answer from cube" reason (not self-healing, no retry signal).
func (cqp *cubeQueryPath) tryQueryFromCube(
	ctx context.Context,
	tenant string,
	req *tempopb.QueryRangeRequest,
) (*tempopb.QueryRangeResponse, bool, error) {
	// #508 Zero-Dimension Cube Support: the `if len(dims) == 0 { return nil, false, nil }`
	// early return that used to live here is REMOVED. An ungrouped query's empty dims flows
	// straight into CubeQueryPathRequest.Dims and on into CreationTrigger.TryCreate/
	// QueryRouter.Route unchanged — both are already dims-length-agnostic (verified in
	// blockpack, SPEC-CUBE-033).
	dims := extractGroupByDims(req.Query)

	// The query's {...} predicate is part of the cube identity (#480): a cube built for
	// one filter must not answer a query with a different filter. If the predicate cannot
	// be faithfully canonicalized into cube filters, the cube path is unsafe — fall back.
	filters, filtersOK := extractFilters(req.Query)
	if !filtersOK {
		level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: filter not cube-representable; falling back", "query", req.Query)
		return nil, false, nil
	}
	level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: found dims", "dims", strings.Join(dims, ","), "filters", filterDedupKey(filters), "tenant", tenant)

	// The materialized attribute (if any) this query's function needs — "" for count_over_time/
	// rate, which match any candidate cube regardless of its AggAttrs set (E-10).
	neededAttr, neededAttrType, neededAttrOK := extractAggAttr(req.Query)

	minMinute := uint32(req.Start / 60_000_000_000) //nolint:gosec // minute fits uint32 for any realistic query window
	maxMinute := uint32(req.End / 60_000_000_000)   //nolint:gosec
	// req.Step is the query's requested granularity, in nanoseconds — converted inline here
	// (not a dedicated requestedResolutionMinutes function; #508 moved every OTHER piece of
	// this file's logic into blockpack, but CubeQueryPathRequest.RequestedResolutionMinutes is
	// deliberately pre-computed by the caller, so this one-line conversion has nowhere to live
	// except tempo's own thin wrapper).
	requestedResolution := uint32(req.Step / 60_000_000_000) //nolint:gosec // step fits uint32 for any realistic query window

	result, found, qErr := cqp.qp.QueryRange(ctx, blockpack.CubeQueryPathRequest{
		Tenant:                     tenant,
		Dims:                       dims,
		Filters:                    filters,
		NeededAttr:                 neededAttr,
		NeededAttrType:             neededAttrType,
		NeededAttrOK:               neededAttrOK,
		RequestedResolutionMinutes: requestedResolution,
		MinMinute:                  minMinute,
		MaxMinute:                  maxMinute,
		// req.Start/req.End are nanoseconds; VCNT records are keyed in unix seconds
		// (minute-floored), so pass the query window in seconds for the cardinality gate.
		MinTS: req.Start / 1_000_000_000,
		MaxTS: req.End / 1_000_000_000,
	})
	if qErr != nil {
		return nil, false, qErr
	}
	if !found {
		return nil, false, nil
	}

	resp := buildCubeQueryResponse(result.Cells, result.Dimensions, result.AggAttrNames, req, result.BytesRead)
	if result.PartialCoverage {
		resp.Status = tempopb.PartialStatus_PARTIAL
		resp.Message = cubePartialCoverageMessage(result.CoveredMinMinute, result.CoveredMaxMinute, minMinute, maxMinute)
	}
	return resp, true, nil
}

// cubePartialCoverageMessage builds the PartialStatus_PARTIAL message for a cube answer that
// only covers [coveredMin,coveredMax] of the originally requested [reqMin,reqMax]. Pure — no
// I/O — stays tempo-side since it only formats a tempopb response field.
func cubePartialCoverageMessage(coveredMin, coveredMax, reqMin, reqMax uint32) string {
	return fmt.Sprintf(
		"cube covers minutes [%d,%d] of the requested [%d,%d]; the uncovered edge is not answered by this cube",
		coveredMin, coveredMax, reqMin, reqMax,
	)
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

// filterDedupKey renders a canonical filter set into a stable string, used only for this
// file's own debug logging. #508: blockpack.CubeQueryPath keeps its own private copy of this
// exact logic for its creation-cooldown dedup key — the two never need to be the SAME
// instance (this one is pure/deterministic and used only for a log line), just the same
// logic, so no drift risk from keeping both.
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
// query (blockpack.CubeQueryPathResult.BytesRead) — set on the response's SearchMetrics.CubeBytesRead
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

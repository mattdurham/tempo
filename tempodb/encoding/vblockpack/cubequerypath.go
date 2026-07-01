package vblockpack

// cubequerypath.go — cube query path for metrics queries.
//
// On every QueryRange call, CubeQueryPath:
//  1. Parses the query to extract group-by dimensions.
//  2. Looks up the registry for a matching cube.
//  3. If found → reads cube files from S3 and returns exact counts.
//  4. If not found → fires TryCreate (cardinality gate via VCNT data).
//
// Falls back to the full block scan on any error or cache miss.

import (
	"bytes"
	"context"
	"io"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/tempopb"
	util_log "github.com/grafana/tempo/pkg/util/log"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
)

// cubeQueryPath is the process-level cube query path manager.
type cubeQueryPath struct {
	client *minio.Client
	bucket string
	// per-tenant registry cache (refreshed every 5m)
	mu           sync.RWMutex
	tenants      map[string]*tenantCubeState
	// createCooldown rate-limits cube creation to at most once per minute
	// per (tenant+dims) key, preventing per-block fan-out storms.
	createSeen   map[string]time.Time
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
func ConfigureCubeQueryPath(enabled bool, s3cfg *s3backend.Config) {
	if !enabled || s3cfg == nil {
		return
	}
	cubeQueryPathOnce.Do(func() {
		endpoint := s3cfg.Endpoint
		if endpoint == "" {
			endpoint = "s3." + s3cfg.Region + ".amazonaws.com"
		}
		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewEnvAWS(),
			Secure: !s3cfg.Insecure,
			Region: s3cfg.Region,
		})
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube query path disabled", "err", err)
			return
		}
		processCubeQueryPathMu.Lock()
		processCubeQueryPath = &cubeQueryPath{
			client:     client,
			bucket:     s3cfg.Bucket,
			tenants:    make(map[string]*tenantCubeState),
			createSeen: make(map[string]time.Time),
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

	os := &minioObjectStore{client: cqp.client, bucket: cqp.bucket}
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

// tryQueryFromCube attempts to answer req from cube files. Returns (result, true)
// when the cube path answered the query, (nil, false) to fall back to block scan.
func (cqp *cubeQueryPath) tryQueryFromCube(
	ctx context.Context,
	tenant string,
	req *tempopb.QueryRangeRequest,
) (*tempopb.QueryRangeResponse, bool) {
	dims := extractGroupByDims(req.Query)
	if len(dims) == 0 {
		level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: no group-by dims in query", "query", req.Query)
		return nil, false // no group-by → cube not applicable
	}
	level.Debug(util_log.Logger).Log("msg", "vblockpack: cube: found dims", "dims", strings.Join(dims, ","), "tenant", tenant)

	entries, err := cqp.loadEntries(ctx, tenant)
	if err != nil {
		return nil, false
	}

	router := blockpack.NewCubeQueryRouter(entries)
	minMinute := uint32(req.Start / 60_000_000_000)
	maxMinute := uint32(req.End / 60_000_000_000)
	result, routeErr := router.Route(tenant, dims, nil, 1, minMinute, maxMinute)
	if routeErr != nil || !result.Found {
		// Cube not found — attempt to create it on first query.
		// Fire cube creation in a background goroutine so QueryRange is not blocked.
		go cqp.maybeCreateCube(context.Background(), tenant, dims, req)
		return nil, false
	}

	// Cube found: list and download L0 files for the time window.
	prefix := path.Join(tenant, "cubes", result.Entry.CubeID) + "/"
	keys, listErr := cqp.listObjects(ctx, prefix)
	if listErr != nil || len(keys) == 0 {
		return nil, false
	}

	// Download and open readers.
	inputs := make([]blockpack.CubeRollupInput, 0, len(keys))
	for _, key := range keys {
		data, getErr := cqp.getObject(ctx, key)
		if getErr != nil {
			continue
		}
		r, openErr := blockpack.OpenCubeReaderFromBytes(data)
		if openErr != nil {
			continue
		}
		inputs = append(inputs, blockpack.CubeNewRollupInput(r))
	}
	if len(inputs) == 0 {
		return nil, false
	}

	cells, rollupErr := blockpack.CubeRollup(inputs, 1, minMinute, maxMinute)
	if rollupErr != nil {
		return nil, false
	}

	return buildCubeQueryResponse(cells, result.Entry.Dimensions, req), true
}

// maybeCreateCube fires TryCreate for a (tenant, dims) pattern that had no cube.
// It is rate-limited to at most once per minute per (tenant+dims) key to prevent
// the per-block fan-out from creating a storm of concurrent S3 ConditionalPuts.
func (cqp *cubeQueryPath) maybeCreateCube(
	ctx context.Context,
	tenant string,
	dims []string,
	req *tempopb.QueryRangeRequest,
) {
	key := tenant + "|" + strings.Join(dims, ",")

	cqp.mu.Lock()
	if last, ok := cqp.createSeen[key]; ok && time.Since(last) < time.Minute {
		cqp.mu.Unlock()
		return // already attempted recently
	}
	cqp.createSeen[key] = time.Now()
	cqp.mu.Unlock()

	os := &minioObjectStore{client: cqp.client, bucket: cqp.bucket}
	reg := blockpack.NewCubeRegistry(os, tenant)
	trigger := blockpack.NewCubeCreationTrigger(reg, blockpack.CubeTriggerConfig{})
	// Pass empty VCNT data — cardinality gate is best-effort; if no VCNT data
	// is available yet the gate passes by default.
	result, err := trigger.TryCreate(ctx, tenant, dims, nil, nil, nil, 0, 0)
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
	}
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

// buildCubeQueryResponse builds a QueryRangeResponse from rolled-up cube cells.
func buildCubeQueryResponse(
	cells []blockpack.CubeMergedCell,
	dims []string,
	req *tempopb.QueryRangeRequest,
) *tempopb.QueryRangeResponse {
	// Group cells into series by (dim1, dim2) label pair.
	type seriesKey struct{ d1, d2 string }
	seriesMap := make(map[seriesKey][]tempopb.Sample)

	for _, c := range cells {
		k := seriesKey{d1: c.Dim1Val, d2: c.Dim2Val}
		ts := int64(c.Minute) * 60 * 1_000_000_000 //nolint:gosec // minute fits int64
		seriesMap[k] = append(seriesMap[k], tempopb.Sample{
			TimestampMs: ts / 1_000_000,
			Value:       float64(c.Count),
		})
	}

	dim1Label := ""
	dim2Label := ""
	if len(dims) >= 1 {
		dim1Label = dims[0]
	}
	if len(dims) >= 2 {
		dim2Label = dims[1]
	}

	var series []*tempopb.TimeSeries
	for k, samples := range seriesMap {
		labels := []commonpbv1.KeyValue{
			{Key: dim1Label, Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: k.d1}}},
		}
		if dim2Label != "" {
			labels = append(labels, commonpbv1.KeyValue{
				Key: dim2Label,
				Value: &commonpbv1.AnyValue{Value: &commonpbv1.AnyValue_StringValue{StringValue: k.d2}},
			})
		}
		series = append(series, &tempopb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		})
	}
	return &tempopb.QueryRangeResponse{Series: series}
}

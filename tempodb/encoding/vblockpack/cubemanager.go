package vblockpack

// cubemanager.go — metrics-cube ingest manager for the block-builder.
//
// The CubeManager loads active cube definitions from the registry at startup
// and on periodic refresh, maintains one Accumulator per cube per minute,
// and flushes cube files to S3 at each minute boundary and block flush.
//
// Usage in CreateBlock: call cm.addTrace(tr) per trace, then cm.flush(tenantID)
// after WriteBlockMeta. The manager is a process-level singleton configured via
// ConfigureCubeManager alongside ConfigureValueIndex.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	blockpack "github.com/grafana/blockpack"

	"github.com/grafana/tempo/pkg/tempopb"
	commonpbv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcepbv1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tracepbv1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	minio "github.com/minio/minio-go/v7"
)

const (
	// cubeDimAll is the sentinel dimension-column value for single-dimension cubes. #508:
	// single-sourced from blockpack.CubeAllDimSentinel (internal/modules/cube.AllDimSentinel)
	// so this literal can never independently drift from the SAME sentinel forward-ingest
	// (CubeRegistryEntryToDefinition) and backfill (Backfiller.processMinute) now use — see
	// NOTE-CUBE-030's addendum, which flagged exactly this two-independently-typed-literal
	// drift risk for a different pair of strings.
	cubeDimAll = blockpack.CubeAllDimSentinel
	// cubeColSpanName is the intrinsic column name for a span's name.
	cubeColSpanName = "span:name"
	// cubeColServiceName is the resource attribute key for a service's name.
	cubeColServiceName = "service.name"
)

// cubeManager manages cube accumulators for one tenant.
type cubeManager struct {
	store    blockpack.CubeObjectPutter // for writing .cube files
	objStore blockpack.CubeObjectStore  // for registry Get/ConditionalPut
	tenant   string

	// pg is the opt-in Postgres backend for the cube registry (2026-07-11).
	// Nil means "not configured" -- loadDefs falls back to objStore-backed
	// (S3/Local/GCS/Azure) NewCubeRegistry unconditionally. Inherits this
	// package's PRE-EXISTING single-tenant limitation (cubeManagerOnce below):
	// this task does not fix that, only adds an orthogonal storage backend
	// choice on top of it.
	pg *blockpack.Postgres

	mu          sync.Mutex
	defs        []blockpack.CubeDefinition
	accs        []*blockpack.CubeAccumulator // one per def, same index
	currentMin  uint32                       // wall-clock minute of current accumulators
	lastRefresh time.Time
}

// minioObjectStore satisfies blockpack.CubeObjectStore over minio.
type minioObjectStore struct {
	client *minio.Client
	bucket string
}

func (s *minioObjectStore) Get(ctx context.Context, path string) ([]byte, string, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		if isMinioNoSuchKey(err) {
			// bonus fix (mirrors #496's viusage.ErrNotFound fix, NOTE-VIUSAGE-10): must
			// return blockpack.CubeErrNotFound (not a nil error) so cube.Registry.Load
			// can distinguish a genuine miss from a real transient failure -- both
			// previously had the identical (nil, "", ?) shape, which caused every real
			// error to be silently treated as an empty index.
			return nil, "", blockpack.CubeErrNotFound
		}
		return nil, "", err
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
		// minio-go's GetObject is lazy -- see isMinioNoSuchKey's doc comment
		// (valueindex.go) for the full explanation; the real 404 for a nonexistent key
		// surfaces here, not from GetObject itself, and needs the identical
		// classification or the registry can never be created for a brand-new tenant.
		if isMinioNoSuchKey(err) {
			return nil, "", blockpack.CubeErrNotFound
		}
		return nil, "", err
	}
	info, err := s.client.StatObject(ctx, s.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return data, "", nil
	}
	return data, info.ETag, nil
}

func (s *minioObjectStore) ConditionalPut(ctx context.Context, path string, data []byte, etag string) error {
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag != "" {
		// S3 conditional write: only update if ETag matches.
		opts.SetMatchETag(etag)
	}
	_, err := s.client.PutObject(ctx, s.bucket, path, bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == 412 {
			return blockpack.CubeErrConflict
		}
		return err
	}
	return nil
}

var (
	processCubeManager   *cubeManager
	processCubeManagerMu sync.RWMutex
	cubeManagerOnce      sync.Once
)

// ConfigureCubeManager installs the process-level cube ingest manager.
// Called at startup alongside ConfigureValueIndex. No-op when not enabled, tenant is empty, or
// neither an S3 config nor a generic (rawR, rawW) backend pair is supplied. When s3cfg is
// non-nil the S3 branch below is byte-identical to before (same minio client construction,
// same *minioObjectStore/*s3ObjectPutter types); the generic branch (rawR/rawW) is new,
// backing Local/GCS/Azure via newRawObjectPutter and newCubeObjectStoreForBackend.
func ConfigureCubeManager(enabled bool, s3cfg *s3backend.Config, rawR backend.RawReader, rawW backend.RawWriter, tenant string, pg *blockpack.Postgres) {
	if !enabled || tenant == "" || (s3cfg == nil && (rawR == nil || rawW == nil)) {
		return
	}
	cubeManagerOnce.Do(func() {
		var cm *cubeManager
		if s3cfg != nil {
			client, err := newMinioClientFromS3Config(s3cfg)
			if err != nil {
				level.Warn(util_log.Logger).Log("msg", "vblockpack: cube manager disabled — S3 client init failed", "err", err)
				return
			}
			cm = &cubeManager{
				store:    &s3ObjectPutter{client: client, bucket: s3cfg.Bucket},
				objStore: &minioObjectStore{client: client, bucket: s3cfg.Bucket},
				tenant:   tenant,
				pg:       pg,
			}
		} else {
			cm = &cubeManager{
				store:    newRawObjectPutter(rawW),
				objStore: newCubeObjectStoreForBackend(rawR, rawW),
				tenant:   tenant,
				pg:       pg,
			}
		}
		if err := cm.loadDefs(context.Background()); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube manager: initial registry load failed (will retry)", "err", err)
		}
		processCubeManagerMu.Lock()
		processCubeManager = cm
		processCubeManagerMu.Unlock()
		level.Info(util_log.Logger).Log("msg", "vblockpack: cube manager configured", "tenant", tenant, "cubes", len(cm.defs))
	})
}

func getCubeManager() *cubeManager {
	processCubeManagerMu.RLock()
	defer processCubeManagerMu.RUnlock()
	return processCubeManager
}

// CubeObjectStoreRawWriterTypeForTest is the cube-registry counterpart to
// ViUsageObjectStoreRawWriterTypeForTest (vi_usage_hook.go) -- see that function's doc
// comment for the full rationale. TEST-ONLY.
func CubeObjectStoreRawWriterTypeForTest() string {
	cm := getCubeManager()
	if cm == nil {
		return ""
	}
	switch s := cm.objStore.(type) {
	case *rawCubeObjectStore:
		return fmt.Sprintf("%T", s.core.rawW)
	case *gcsCubeObjectStore:
		return fmt.Sprintf("%T", s.core.vrw)
	default:
		return ""
	}
}

// loadDefs reads the cube registry (Postgres-backed, issue #504: no blob/index.json
// fallback) and rebuilds defs + accs. cm.pg is nil-guarded here rather than assumed
// non-nil: tempodb/config.go's validateConfig hard-fails at startup if CubeTenants is
// non-empty with cfg.Postgres == nil, so ConfigureCubeManager's own production call site
// (tempodb.go, gated on CubeTenants) never actually reaches this with a nil pg -- but
// ConfigureCubeManager itself has no such gate (it's called once per already-validated
// tenant), and defensively degrading here (return an error, which the caller already logs
// as "will retry" rather than crashing the process) is strictly safer than trusting every
// current and future caller to respect that invariant.
func (cm *cubeManager) loadDefs(ctx context.Context) error {
	if cm.pg == nil {
		return errors.New("vblockpack: cube manager: postgres not configured")
	}
	reg := cm.pg.CubeRegistry(cm.tenant)
	// CubeColumnFilterToFilter is the single source of truth (shared with blockpack's own
	// backfill.go) for converting a RegistryEntry's baked-in filter into a runtime predicate
	// (#491 Phase E fix pass, review.md Issue 2). Previously nil here, so def.Filters was always
	// empty — a filtered cube (#480) would have silently counted every span matching its
	// dimensions once Issue 1's AggAttrs fix let forward ingest start activating cubes at all.
	defs, err := blockpack.LoadCubeDefinitions(ctx, reg, blockpack.CubeColumnFilterToFilter)
	if err != nil {
		return err
	}
	minute := wallMinute()
	activeDefs, accs := filterValidCubeDefs(defs, minute, cm.tenant)
	cm.mu.Lock()
	cm.defs = activeDefs
	cm.accs = accs
	cm.currentMin = minute
	cm.lastRefresh = time.Now()
	cm.mu.Unlock()
	level.Info(util_log.Logger).Log("msg", "vblockpack: cube manager refreshed", "tenant", cm.tenant, "cubes", len(activeDefs))
	return nil
}

// filterValidCubeDefs builds the lockstep (activeDefs, accs) pair loadDefs needs: for each def,
// attempt to construct its accumulator (which validates the mandatory-duration invariant, #491
// E-4 — a definition failing that check is skipped from active ingest, not a fatal registry-load
// error, matching this loop's existing best-effort tolerance for individual bad entries).
// activeDefs/accs are built in LOCKSTEP so index i always corresponds to the same cube in both
// slices (rotateLocked and addTrace rely on that correspondence) — a def is never appended to
// one slice without its accumulator being appended to the other in the same iteration. Split out
// of loadDefs so this invariant is independently unit-testable without a real registry/S3 backend.
func filterValidCubeDefs(
	defs []blockpack.CubeDefinition, minute uint32, tenant string,
) ([]blockpack.CubeDefinition, []*blockpack.CubeAccumulator) {
	activeDefs := make([]blockpack.CubeDefinition, 0, len(defs))
	accs := make([]*blockpack.CubeAccumulator, 0, len(defs))
	for _, d := range defs {
		acc, accErr := blockpack.NewCubeAccumulator(d, minute)
		if accErr != nil {
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: cube manager: skipping definition failing validation",
				"tenant", tenant, "err", accErr,
			)
			continue
		}
		activeDefs = append(activeDefs, d)
		accs = append(accs, acc)
	}
	return activeDefs, accs
}

// maybeRefresh reloads cube definitions every 5 minutes.
func (cm *cubeManager) maybeRefresh() {
	cm.mu.Lock()
	stale := time.Since(cm.lastRefresh) > 5*time.Minute
	cm.mu.Unlock()
	if stale {
		if err := cm.loadDefs(context.Background()); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube manager: registry refresh failed", "err", err)
		}
	}
}

// addTrace feeds all spans in one trace to every active cube accumulator.
//
// #491 Phase E fix pass (go-presubmit.md #2): a minute rollover swaps out the affected
// accumulators under cm.mu (cheap, no I/O — see rotateLocked) but does NOT flush them while
// holding the lock. The lock is released before flushing so a minute-boundary trace does not
// stall every concurrent addTrace call for the tenant behind synchronous S3 PUT latency.
func (cm *cubeManager) addTrace(trace *tempopb.Trace) {
	if trace == nil {
		return
	}
	cm.mu.Lock()
	if len(cm.accs) == 0 {
		cm.mu.Unlock()
		return
	}
	// Rotate minute if needed. rotateLocked swaps in fresh accumulators under the lock and
	// returns the swapped-out (now exclusively ours) ones with data, to be flushed after unlock.
	var rotatedOut []flushableAccumulator
	now := wallMinute()
	if now != cm.currentMin {
		rotatedOut = cm.rotateLocked(now)
	}
	for _, rs := range trace.ResourceSpans {
		if rs == nil {
			continue
		}
		for _, ss := range rs.ScopeSpans {
			if ss == nil {
				continue
			}
			for _, span := range ss.Spans {
				if span == nil {
					continue
				}
				sv := &tempoSpanValues{span: span, resource: rs.Resource}
				for i := range cm.accs {
					if _, err := cm.accs[i].Add(sv); err != nil {
						level.Warn(util_log.Logger).Log("msg", "vblockpack: cube add failed", "err", err)
					}
				}
			}
		}
	}
	tenant := cm.tenant
	cm.mu.Unlock()

	for _, r := range rotatedOut {
		if key, err := r.acc.FlushTo(cm.store, tenant); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube rotate flush failed", "cube", r.name, "err", err)
		} else if key != "" {
			level.Debug(util_log.Logger).Log("msg", "vblockpack: cube flushed", "key", key)
		}
	}
}

// flushableAccumulator pairs an accumulator that is exclusively owned by the caller (already
// swapped/snapshotted out from cm.accs under the lock) with its cube ID, for flushing after the
// lock has been released (#491 Phase E fix pass, go-presubmit.md #2).
type flushableAccumulator struct {
	acc  *blockpack.CubeAccumulator
	name [16]byte
}

// flush writes all non-empty cube accumulators to S3 and resets them. Called after each block
// flush.
//
// #491 Phase E fix pass (go-presubmit.md #2): the S3 PUT (acc.FlushTo -> store.Put ->
// minio.PutObject) previously ran synchronously while holding cm.mu for the entire loop,
// serializing every concurrent addTrace call for the tenant behind N sequential S3 PUTs.
//
// #491 Phase E re-review Iteration 3 (NEW Issue R1): the first fix pass only snapshotted WHICH
// accumulators had data under the lock, then called FlushTo (Encode+Reset) on those SAME
// *blockpack.CubeAccumulator pointers AFTER releasing the lock, while they remained live in
// cm.accs — a concurrent addTrace() could still call Add() on the identical pointer, mutating the
// same a.cells/a.dict maps FlushTo's unlocked Encode()/Reset() was touching (Accumulator is "not
// safe for concurrent use"). Fixed by reusing rotateLocked's ownership-transfer pattern via
// swapOutAccumulatorsLocked: a fresh accumulator is swapped into cm.accs[i] BEFORE the lock is
// released, so the old accumulator handed to the post-unlock flush loop is exclusively owned by
// this goroutine — no concurrent addTrace can ever observe or mutate it again.
func (cm *cubeManager) flush(tenant string) {
	cm.mu.Lock()
	toFlush := cm.swapOutAccumulatorsLocked(cm.currentMin)
	cm.mu.Unlock()

	for _, f := range toFlush {
		if key, err := f.acc.FlushTo(cm.store, tenant); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube flush failed", "cube", f.name, "err", err)
		} else if key != "" {
			level.Debug(util_log.Logger).Log("msg", "vblockpack: cube flushed", "key", key)
		}
	}
}

// rotateLocked swaps cm.accs for fresh accumulators bound to newMinute and returns the
// swapped-out accumulators that had accumulated data, for the caller to flush AFTER releasing
// cm.mu. Must be called with cm.mu held.
//
// #491 Phase E fix pass (go-presubmit.md #2): previously flushed accumulators synchronously
// in-place (S3 PUT under the lock) and reset them via Accumulator.Reset. Accumulator is
// documented "not safe for concurrent use" (accumulator.go), so a flushing goroutine must
// exclusively own whatever it flushes once the lock is released — swapping the accumulator
// POINTER out (rather than resetting the shared one in place and flushing it after unlock, which
// a concurrent addTrace could then also be mutating) satisfies that.
func (cm *cubeManager) rotateLocked(newMinute uint32) []flushableAccumulator {
	rotatedOut := cm.swapOutAccumulatorsLocked(newMinute)
	cm.currentMin = newMinute
	return rotatedOut
}

// swapOutAccumulatorsLocked swaps a fresh accumulator into cm.accs[i] — bound to targetMinute —
// for every accumulator with data, and returns the swapped-out accumulators (now exclusively
// owned by the caller) for flushing AFTER cm.mu is released. Must be called with cm.mu held.
//
// #491 Phase E re-review Iteration 3 (NEW Issue R1): extracted out of rotateLocked so flush()
// could adopt the exact same ownership-transfer pattern without the two call sites drifting apart
// again — rotateLocked passes a NEW minute (the accumulator moves to the next bucket); flush()
// passes cm.currentMin unchanged (the accumulator is reset in place for the same bucket). Either
// way, building a fresh accumulator via NewCubeAccumulator is cheap (no I/O, just allocates a
// map+dict), so doing it under the lock for every active def is not a meaningful cost.
func (cm *cubeManager) swapOutAccumulatorsLocked(targetMinute uint32) []flushableAccumulator {
	swappedOut := make([]flushableAccumulator, 0, len(cm.accs))
	for i, acc := range cm.accs {
		// Build the fresh replacement FIRST, before deciding acc's fate: only once a fresh
		// accumulator is confirmed available do we hand acc off to the caller (via swappedOut) as
		// exclusively theirs and swap it out of cm.accs. If NewCubeAccumulator fails, acc is
		// NEVER added to swappedOut, so it is never handed to two owners at once (the bug this
		// whole restructure exists to prevent) — it stays in cm.accs and is reset in place
		// instead, matching pre-fix behavior for this (expected-unreachable) error path.
		fresh, err := blockpack.NewCubeAccumulator(cm.defs[i], targetMinute)
		if err != nil {
			// cm.defs[i] already passed NewCubeAccumulator's validation once in loadDefs, and a
			// Definition never changes between calls for the same cube, so this should be
			// unreachable in practice.
			level.Warn(util_log.Logger).Log(
				"msg", "vblockpack: cube manager: NewCubeAccumulator failed, resetting in place",
				"tenant", cm.tenant, "err", err,
			)
			acc.Reset(targetMinute)
			continue
		}
		if acc.CellCount() > 0 {
			swappedOut = append(swappedOut, flushableAccumulator{acc: acc, name: cm.defs[i].ID})
		}
		cm.accs[i] = fresh
	}
	return swappedOut
}

// wallMinute returns the current wall-clock minute as uint32.
func wallMinute() uint32 {
	return uint32(time.Now().Unix() / 60) //nolint:gosec
}

// tempoSpanValues adapts an OTLP span + resource to blockpack.CubeSpanValues.
type tempoSpanValues struct {
	span     *tracepbv1.Span
	resource *resourcepbv1.Resource
}

func (s *tempoSpanValues) String(col string) (string, bool) {
	// cubeDimAll is the sentinel for single-dimension cubes — always present.
	if col == cubeDimAll {
		return cubeDimAll, true
	}
	// Span attributes: "span.<key>"
	if len(col) > 5 && col[:5] == "span." {
		return attrString(s.span.Attributes, col[5:])
	}
	// Resource attributes: "resource.<key>"
	if len(col) > 9 && col[:9] == "resource." {
		if s.resource != nil {
			return attrString(s.resource.Attributes, col[9:])
		}
		return "", false
	}
	// Intrinsics
	switch col {
	case cubeColSpanName:
		return s.span.Name, s.span.Name != ""
	case cubeColServiceName:
		if s.resource != nil {
			return attrString(s.resource.Attributes, cubeColServiceName)
		}
	}
	return "", false
}

func (s *tempoSpanValues) Int64(col string) (int64, bool) {
	if len(col) > 5 && col[:5] == "span." {
		return attrInt64(s.span.Attributes, col[5:])
	}
	if len(col) > 9 && col[:9] == "resource." {
		if s.resource != nil {
			return attrInt64(s.resource.Attributes, col[9:])
		}
		return 0, false
	}
	switch col {
	case "span:kind":
		return int64(s.span.Kind), true
	case "span:duration":
		if s.span.EndTimeUnixNano > s.span.StartTimeUnixNano {
			return int64(s.span.EndTimeUnixNano - s.span.StartTimeUnixNano), true //nolint:gosec
		}
	case "span:status":
		if s.span.Status != nil {
			return int64(s.span.Status.Code), true
		}
	}
	return 0, false
}

// Float64 satisfies blockpack.CubeSpanValues' Float64 method (#491, E-4) — the single numeric-
// extraction path every materialized aggregate attribute (Sum/Min/Max/Buckets) reads from.
// span:duration mirrors Int64's own duration calculation, expressed as float64 nanoseconds
// (the cube accumulator's own single numeric-extraction convention, cube.DurationColumn).
func (s *tempoSpanValues) Float64(col string) (float64, bool) {
	if len(col) > len(attrPrefixSpanDot) && col[:len(attrPrefixSpanDot)] == attrPrefixSpanDot {
		return attrFloat64(s.span.Attributes, col[len(attrPrefixSpanDot):])
	}
	if len(col) > len(attrPrefixResourceDot) && col[:len(attrPrefixResourceDot)] == attrPrefixResourceDot {
		if s.resource != nil {
			return attrFloat64(s.resource.Attributes, col[len(attrPrefixResourceDot):])
		}
		return 0, false
	}
	if col == "span:duration" && s.span.EndTimeUnixNano > s.span.StartTimeUnixNano {
		return float64(s.span.EndTimeUnixNano - s.span.StartTimeUnixNano), true //nolint:gosec
	}
	return 0, false
}

func attrString(attrs []*commonpbv1.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if kv != nil && kv.Key == key && kv.Value != nil {
			if sv, ok := kv.Value.Value.(*commonpbv1.AnyValue_StringValue); ok {
				return sv.StringValue, sv.StringValue != ""
			}
		}
	}
	return "", false
}

func attrInt64(attrs []*commonpbv1.KeyValue, key string) (int64, bool) {
	for _, kv := range attrs {
		if kv != nil && kv.Key == key && kv.Value != nil {
			if iv, ok := kv.Value.Value.(*commonpbv1.AnyValue_IntValue); ok {
				return iv.IntValue, true
			}
		}
	}
	return 0, false
}

// attrFloat64 extracts a numeric attribute value as float64, accepting both OTLP double and
// int attribute encodings — a numeric aggAttr may legitimately arrive as either wire type
// depending on how the instrumenting SDK encoded it.
func attrFloat64(attrs []*commonpbv1.KeyValue, key string) (float64, bool) {
	for _, kv := range attrs {
		if kv == nil || kv.Key != key || kv.Value == nil {
			continue
		}
		switch v := kv.Value.Value.(type) {
		case *commonpbv1.AnyValue_DoubleValue:
			return v.DoubleValue, true
		case *commonpbv1.AnyValue_IntValue:
			return float64(v.IntValue), true
		}
	}
	return 0, false
}

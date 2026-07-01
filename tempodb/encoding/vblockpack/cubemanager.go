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
	"encoding/json"
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
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// cubeManager manages cube accumulators for one tenant.
type cubeManager struct {
	store    blockpack.CubeObjectPutter // for writing .cube files
	objStore *minioObjectStore          // for registry Get/ConditionalPut
	tenant   string

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
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" || resp.StatusCode == 404 {
			return nil, "", nil // empty index — first write
		}
		return nil, "", err
	}
	defer func() { _ = obj.Close() }()
	data, err := io.ReadAll(obj)
	if err != nil {
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
// Called at startup alongside ConfigureValueIndex. No-op when not enabled.
func ConfigureCubeManager(enabled bool, s3cfg *s3backend.Config, tenant string) {
	if !enabled || s3cfg == nil || tenant == "" {
		return
	}
	cubeManagerOnce.Do(func() {
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
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube manager disabled — S3 client init failed", "err", err)
			return
		}
		os := &minioObjectStore{client: client, bucket: s3cfg.Bucket}
		cm := &cubeManager{
			store:    &s3ObjectPutter{client: client, bucket: s3cfg.Bucket},
			objStore: os,
			tenant:   tenant,
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

// loadDefs reads the cube index.json and rebuilds defs + accs.
func (cm *cubeManager) loadDefs(ctx context.Context) error {
	reg := blockpack.NewCubeRegistry(cm.objStore, cm.tenant)
	defs, err := blockpack.LoadCubeDefinitions(ctx, reg, nil)
	if err != nil {
		return err
	}
	minute := wallMinute()
	accs := make([]*blockpack.CubeAccumulator, len(defs))
	for i, d := range defs {
		accs[i] = blockpack.NewCubeAccumulator(d, minute)
	}
	cm.mu.Lock()
	cm.defs = defs
	cm.accs = accs
	cm.currentMin = minute
	cm.lastRefresh = time.Now()
	cm.mu.Unlock()
	level.Info(util_log.Logger).Log("msg", "vblockpack: cube manager refreshed", "tenant", cm.tenant, "cubes", len(defs))
	return nil
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
func (cm *cubeManager) addTrace(trace *tempopb.Trace) {
	if trace == nil {
		return
	}
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if len(cm.accs) == 0 {
		return
	}
	// Rotate minute if needed.
	now := wallMinute()
	if now != cm.currentMin {
		cm.rotateLocked(now)
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
}

// flush writes all non-empty cube accumulators to S3 and resets them.
// Called after each block flush.
func (cm *cubeManager) flush(tenant string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for i, acc := range cm.accs {
		if acc.CellCount() == 0 {
			continue
		}
		if key, err := acc.FlushTo(cm.store, tenant); err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: cube flush failed", "cube", cm.defs[i].ID, "err", err)
		} else if key != "" {
			level.Debug(util_log.Logger).Log("msg", "vblockpack: cube flushed", "key", key)
		}
	}
}

// rotateLocked replaces accumulators with fresh ones for the new minute.
// Must be called with cm.mu held.
func (cm *cubeManager) rotateLocked(newMinute uint32) {
	for i, acc := range cm.accs {
		if acc.CellCount() > 0 {
			if _, err := acc.FlushTo(cm.store, cm.tenant); err != nil {
				level.Warn(util_log.Logger).Log("msg", "vblockpack: cube rotate flush failed", "err", err)
			}
		}
		cm.accs[i] = blockpack.NewCubeAccumulator(cm.defs[i], newMinute)
	}
	cm.currentMin = newMinute
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
	case "span:name":
		return s.span.Name, s.span.Name != ""
	case "service.name":
		if s.resource != nil {
			return attrString(s.resource.Attributes, "service.name")
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

// Compile-time check that json and sync are used.
var (
	_ = json.Marshal
)

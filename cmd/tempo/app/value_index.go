package app

// Value-index pipeline module targets for Tempo.
//
// Wires the blockpack value-index consumer (-target=value-index-consumer) and
// compactor (-target=value-index-compactor) as Tempo module targets.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/tempo/modules/postgres"
	"github.com/grafana/tempo/pkg/util/log"
	s3cfg "github.com/grafana/tempo/tempodb/backend/s3"
	common "github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/jackc/pgx/v5/pgxpool"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/blockpack/blockevents"
	vcntcompactor "github.com/grafana/blockpack/valuecountscompactor"
	viccompactor "github.com/grafana/blockpack/valueindexcompactor"
	vicconsumer "github.com/grafana/blockpack/valueindexconsumer"
	vblockpack "github.com/grafana/tempo/tempodb/encoding/vblockpack"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newMinioFromS3Cfg(cfg *s3cfg.Config) (*minio.Client, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", cfg.Region)
	}
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: !cfg.Insecure,
		Region: cfg.Region,
	})
}

// manifestStore is the structural shape colhashmanifest.Store requires, and which
// vicconsumer.Config.ManifestStore / vcntcompactor.Config.ManifestStore each locally
// redeclare (issue #216/#507): a ctx-scoped whole-object Get/Put. Declared here, rather
// than importing colhashmanifest directly (an internal blockpack package tempo cannot
// import), so toVICConsumerCfg/toVCNTCompactorCfg can accept and assign it structurally --
// any value satisfying this method set (blockpack.NewPgColumnManifestStore's return value,
// or the plain *tempoVCCStore object-store adapter already defined below) is directly
// assignable to either Config's ManifestStore field with no further wrapping.
type manifestStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
}

// toVICConsumerCfg converts the Tempo config struct to the blockpack consumer config.
// manifestStore is nil when no manifest recording is configured (see initValueIndexConsumer).
func toVICConsumerCfg(cfg common.ValueIndexConsumerConfig, ms manifestStore) vicconsumer.Config {
	return vicconsumer.Config{
		Enabled:            cfg.Enabled,
		ManifestStore:      ms,
		RedisAddr:          cfg.RedisAddr,
		StreamName:         cfg.StreamName,
		ConsumerGroup:      cfg.ConsumerGroup,
		ConsumerName:       cfg.ConsumerName,
		IndexPrefix:        cfg.IndexPrefix,
		Columns:            cfg.Columns,
		FlushInterval:      cfg.FlushInterval,
		PollTimeout:        cfg.PollTimeout,
		ClaimIdleThreshold: cfg.ClaimIdleThreshold,
		BatchSize:          cfg.BatchSize,
	}
}

// toVICCompactorCfg converts the Tempo config struct to the blockpack compactor config.
func toVICCompactorCfg(cfg common.ValueIndexCompactorConfig) viccompactor.Config {
	out := viccompactor.Config{
		Enabled:               cfg.Enabled,
		IndexPrefix:           cfg.IndexPrefix,
		Tenants:               cfg.Tenants,
		CompactInterval:       cfg.CompactInterval,
		CompactThresholdFiles: cfg.CompactThresholdFiles,
		MaxOutputBytes:        cfg.MaxOutputBytes,
		ShardCount:            cfg.ShardCount,
		ShardIndex:            cfg.ShardIndex,
		CompactBatchBytes:     cfg.CompactBatchBytes,
		CompactConcurrency:    cfg.CompactConcurrency,
		CompactMaxInputFiles:  cfg.CompactMaxInputFiles,
	}
	// Allow SHARD_COUNT env var (set on the Deployment) to enable sharding.
	if v, err := strconv.Atoi(os.Getenv("SHARD_COUNT")); err == nil && v > 0 {
		out.ShardCount = v
	}
	// SHARD_INDEX can be set explicitly, or derived from POD_NAME (injected via
	// the Kubernetes Downward API) so every pod in a Deployment gets a stable,
	// unique shard assignment without needing a StatefulSet.
	if v, err := strconv.Atoi(os.Getenv("SHARD_INDEX")); err == nil && v >= 0 {
		out.ShardIndex = v
	} else if out.ShardCount > 1 {
		// StatefulSet pods are named <name>-<ordinal> (e.g. value-index-compactor-3).
		// Parse the ordinal suffix as the shard index — guaranteed unique 0..N-1.
		if name := os.Getenv("POD_NAME"); name != "" {
			if idx := strings.LastIndex(name, "-"); idx >= 0 {
				if v, err := strconv.Atoi(name[idx+1:]); err == nil && v >= 0 {
					out.ShardIndex = v % out.ShardCount
				}
			}
		}
	}
	return out
}

// toVCNTCompactorCfg converts the Tempo config struct to the blockpack VCNT compactor config.
// manifestStore is nil when no manifest recording is configured (see initValueIndexCompactor).
func toVCNTCompactorCfg(cfg common.ValueCountCompactorConfig, ms manifestStore) vcntcompactor.Config {
	out := vcntcompactor.Config{
		Enabled:               cfg.Enabled,
		ManifestStore:         ms,
		Tenants:               cfg.Tenants,
		CompactInterval:       cfg.CompactInterval,
		CompactThresholdFiles: cfg.CompactThresholdFiles,
		CompactBatchBytes:     cfg.CompactBatchBytes,
		MaxRecordsPerMerge:    cfg.MaxRecordsPerMerge,
		ShardCount:            cfg.ShardCount,
		ShardIndex:            cfg.ShardIndex,
	}
	// Same SHARD_COUNT/SHARD_INDEX env var convention as toVICCompactorCfg, so a VCNT
	// column and a VI column with the same name land on the same shard index when both
	// compactors share ShardCount/ShardIndex (byte-identical ColHash construction).
	if v, err := strconv.Atoi(os.Getenv("SHARD_COUNT")); err == nil && v > 0 {
		out.ShardCount = v
	}
	if v, err := strconv.Atoi(os.Getenv("SHARD_INDEX")); err == nil && v >= 0 {
		out.ShardIndex = v
	} else if out.ShardCount > 1 {
		if name := os.Getenv("POD_NAME"); name != "" {
			if idx := strings.LastIndex(name, "-"); idx >= 0 {
				if v, err := strconv.Atoi(name[idx+1:]); err == nil && v >= 0 {
					out.ShardIndex = v % out.ShardCount
				}
			}
		}
	}
	return out
}

// ── value-index consumer ──────────────────────────────────────────────────────

func (t *App) initValueIndexConsumer() (services.Service, error) {
	bp := t.cfg.StorageConfig.Trace.Block.Blockpack
	if !bp.ValueIndexConsumer.Enabled {
		return services.NewIdleService(nil, nil), nil
	}

	s3Client, err := newMinioFromS3Cfg(t.cfg.StorageConfig.Trace.S3)
	if err != nil {
		return nil, fmt.Errorf("value-index-consumer: create S3 client: %w", err)
	}
	bucket := t.cfg.StorageConfig.Trace.S3.Bucket

	// Column-manifest recording (colhashmanifest, task #216) was shipped in blockpack but
	// never wired to a production ManifestStore before this (issue #507) -- flushColumn
	// correctly no-ops on a nil ManifestStore, so recording was silently disabled for every
	// tenant. Prefer the Postgres-backed store (blockpack#506) when cfg.Postgres is
	// configured, mirroring the same opt-in pattern initValueIndexCompactor already uses for
	// cube's registry below. When Postgres isn't configured, fall back to the plain S3-backed
	// *tempoVCCStore adapter (defined below): its Get/Put methods already match
	// colhashmanifest.Store's exact shape (ctx-scoped whole-object read/write), so no new
	// blockpack API and no new tempo adapter type is needed for the blob path -- blockpack's
	// colhashmanifest package has no exported blob-backed constructor of its own yet (it never
	// had a production caller before this wiring).
	var pgPool *pgxpool.Pool
	var manStore manifestStore
	if pgCfg := t.cfg.StorageConfig.Trace.Postgres; pgCfg != nil {
		pgPool, err = postgres.NewPool(context.Background(), pgCfg)
		if err != nil {
			return nil, fmt.Errorf("value-index-consumer: create postgres pool for column manifest: %w", err)
		}
		if err := blockpack.ApplyColumnManifestSchema(context.Background(), pgPool); err != nil {
			pgPool.Close()
			return nil, fmt.Errorf("value-index-consumer: apply column manifest schema: %w", err)
		}
		manStore = blockpack.NewPgColumnManifestStore(pgPool)
	} else {
		manStore = &tempoVCCStore{client: s3Client, bucket: bucket}
	}

	vicCfg := toVICConsumerCfg(bp.ValueIndexConsumer, manStore)
	// Expose consumer pipeline metrics on the default registry.
	vicCfg.Registerer = prometheus.DefaultRegisterer
	// Inject a structured logger so the consumer logs job/flush boundaries
	// (blockpack NOTE-VI-028, issue #410). The pipeline was otherwise a black
	// box after startup.
	vicCfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var consumer vicconsumer.Consumer
	var consumerCloser func()
	rc, cerr := vicconsumer.NewRedisConsumer(vicCfg)
	if cerr != nil {
		if pgPool != nil {
			pgPool.Close()
		}
		return nil, fmt.Errorf("value-index-consumer: create redis consumer: %w", cerr)
	}
	consumer = rc
	consumerCloser = func() { _ = rc.Close() }

	extractor := &tempoVICExtractor{client: s3Client, bucket: bucket, viUsage: t.cfg.StorageConfig.Trace.Block.Blockpack.ViUsage}
	store := &tempoVICPutter{client: s3Client, bucket: bucket}

	svc, err := vicconsumer.NewService(vicCfg, consumer, extractor, store)
	if err != nil {
		consumerCloser()
		if pgPool != nil {
			pgPool.Close()
		}
		return nil, fmt.Errorf("value-index-consumer: create service: %w", err)
	}

	return services.NewIdleService(
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(_ error) error {
			consumerCloser()
			if pgPool != nil {
				pgPool.Close()
			}
			return nil
		},
	), nil
}

// ── value-index compactor ─────────────────────────────────────────────────────

func (t *App) initValueIndexCompactor() (services.Service, error) {
	bp := t.cfg.StorageConfig.Trace.Block.Blockpack
	vccCfg := toVICCompactorCfg(bp.ValueIndexCompactor)

	if !vccCfg.Enabled && !bp.CubeCompactorEnabled && !bp.ValueCountCompactor.Enabled {
		return services.NewIdleService(nil, nil), nil
	}

	s3Client, err := newMinioFromS3Cfg(t.cfg.StorageConfig.Trace.S3)
	if err != nil {
		return nil, fmt.Errorf("value-index-compactor: create S3 client: %w", err)
	}

	bucket := t.cfg.StorageConfig.Trace.S3.Bucket
	store := &tempoVCCStore{client: s3Client, bucket: bucket}
	exister := &tempoVCCExister{client: s3Client, bucket: bucket}
	vcntStore := &tempoVCNTStore{store: store}

	// pgPool backs the cube scheduler's registry (issue #504: Postgres is now the only
	// supported cube registry backend, no blob/index.json fallback) and, when configured, the
	// Postgres-backed column-manifest store (issue #507, below). Constructed whenever
	// cfg.Postgres is configured, not just when the cube scheduler will run -- tempodb/
	// config.go's validateConfig hard-fails at startup if CubeTenants is non-empty with
	// cfg.Postgres == nil, so cfg.Postgres is guaranteed non-nil whenever the cube branch below
	// is reached; widening the condition to cfg.Postgres != nil also lets the VCNT compactor's
	// manifest recording use the Postgres-backed store even when cube itself is disabled. This
	// is a SEPARATE pgxpool.Pool from the one tempodb.New's readerWriter owns internally (that
	// field is unexported, and this module has no reference to the concrete *readerWriter, only
	// the Reader/Writer/Compactor interfaces) -- mirrors modules/backendworker's own identical
	// cfg.Postgres != nil -> postgres.NewPool pattern for the same reason.
	var pgPool *pgxpool.Pool
	if pgCfg := t.cfg.StorageConfig.Trace.Postgres; pgCfg != nil {
		pgPool, err = postgres.NewPool(context.Background(), pgCfg)
		if err != nil {
			return nil, fmt.Errorf("value-index-compactor: create postgres pool: %w", err)
		}
		if err := blockpack.ApplyColumnManifestSchema(context.Background(), pgPool); err != nil {
			pgPool.Close()
			return nil, fmt.Errorf("value-index-compactor: apply column manifest schema: %w", err)
		}
	}

	// Column-manifest recording (colhashmanifest, task #216) -- see initValueIndexConsumer's
	// identical comment for the Postgres-vs-blob decision (issue #507). The VI compactor
	// (viccompactor.Config) has no ManifestStore field; only the VCNT compactor
	// (vcntcompactor.Config) records manifest entries.
	var manStore manifestStore
	if pgPool != nil {
		manStore = blockpack.NewPgColumnManifestStore(pgPool)
	} else {
		manStore = store
	}
	vcntCfg := toVCNTCompactorCfg(bp.ValueCountCompactor, manStore)

	return services.NewIdleService(
		func(ctx context.Context) error {
			// VI index compactor loop.
			if vccCfg.Enabled {
				vccCfg.Registerer = prometheus.DefaultRegisterer
				viSvc, viErr := viccompactor.NewService(vccCfg, store, exister)
				if viErr != nil {
					return fmt.Errorf("value-index-compactor: %w", viErr)
				}
				go func() {
					if err := viSvc.Run(ctx); err != nil && err != context.Canceled {
						level.Error(log.Logger).Log("msg", "value-index-compactor exited", "err", err)
					}
				}()
			}
			// Cube scheduler loop — runs inside the same service. Implements the full
			// L0-merge / boundary-gated L0->L1 hourly / L1->L2 daily rollup ladder plus L0
			// eviction as a single driver (#491, E-12b — replaces the former
			// CubeCompactorService, whose L1-rollup step had no boundary-completeness gate).
			if bp.CubeCompactorEnabled && len(bp.CubeTenants) > 0 {
				cubeScheduler := vblockpack.ConfigureCubeScheduler(
					s3Client, bucket, bp.CubeTenants,
					vblockpack.CubeSchedulerConfig{TickInterval: bp.CubeCompactorInterval},
					pgPool,
				)
				go cubeScheduler.Run(ctx)
			}
			// VCNT (value-counts) compactor loop — bundled the same way as cube above,
			// no dedicated StatefulSet. See blockpack valuecountscompactor NOTE-VC-005/009
			// for why this has no SourceExister (retention is Compact's own net-sum rule)
			// and why delete failures are retried with a dedicated metric rather than
			// treated as fully safe (Compact sums by key, unlike VI's identity-deduped merge).
			if vcntCfg.Enabled {
				vcntCfg.Registerer = prometheus.DefaultRegisterer
				vcntSvc, vcntErr := vcntcompactor.NewService(vcntCfg, vcntStore)
				if vcntErr != nil {
					return fmt.Errorf("value-count-compactor: %w", vcntErr)
				}
				go func() {
					if err := vcntSvc.Run(ctx); err != nil && err != context.Canceled {
						level.Error(log.Logger).Log("msg", "value-count-compactor exited", "err", err)
					}
				}()
			}
			<-ctx.Done()
			return nil
		},
		func(_ error) error {
			if pgPool != nil {
				pgPool.Close()
			}
			return nil
		},
	), nil
}

// ── S3 extractor for consumer ─────────────────────────────────────────────────

type tempoVICExtractor struct {
	client  *minio.Client
	bucket  string
	viUsage common.ViUsageConfig
}

// tenantFromBlockKey extracts the leading "<tenant>/" segment from a block
// object key ("<tenant>/<blockID>/data.blockpack"), mirroring
// vblockpack.parseBlockObjectKey's convention (unexported there, so this is
// a minimal, purpose-built duplicate rather than a cross-package reach into
// vblockpack's internals for one string split).
func tenantFromBlockKey(key string) string {
	if idx := strings.Index(key, "/"); idx > 0 {
		return key[:idx]
	}
	return ""
}

func (e *tempoVICExtractor) Extract(ctx context.Context, event blockevents.Message, yield func(vicconsumer.ColumnEntry) error) error {
	key := bareKey(event.Path)

	// Download the full file to a temp file so all block reads are local I/O
	// rather than hundreds of S3 ranged GETs (one per inner block per column).
	// GetObject on S3 starts streaming immediately; stat first to detect 404.
	_, err := e.client.StatObject(ctx, e.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" || resp.StatusCode == 404 {
			return nil
		}
		return fmt.Errorf("stat %s: %w", key, err)
	}

	obj, err := e.client.GetObject(ctx, e.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	defer func() { _ = obj.Close() }()

	tmp, err := os.CreateTemp("", "vic-*.blockpack")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err := io.Copy(tmp, obj); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("download %s: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	prov, err := newLocalFileProvider(tmpPath)
	if err != nil {
		return fmt.Errorf("open temp %s: %w", tmpPath, err)
	}
	defer func() { _ = prov.Close() }()

	reader, err := blockpack.NewReaderFromProvider(prov)
	if err != nil {
		return fmt.Errorf("open reader %s: %w", key, err)
	}

	// Extraction (the two-phase intrinsic + attribute column walk and the per-span
	// TimeSec from span:start) is delegated to blockpack.ExtractValueIndexEntries
	// (NOTE-VI-018, blockpack issue #401) so this module and the standalone
	// value-index-consumer binary share one implementation.
	//
	// #496 Fix B: resolves the real ColumnPolicy via
	// vblockpack.BuildViColumnPolicyForTenant, mirroring create.go/compactor.go's
	// synchronous write path. Note this standalone consumer target does not call
	// vblockpack.ConfigureViWatermarkCache anywhere in its own init path (that
	// singleton is installed by tempodb.NewV2Backend, not this binary's startup),
	// so the triggered-column half of the policy is always empty here -- the
	// dedicated-list half (DedicatedColumnsEnabled/DedicatedColumnsOverride)
	// still applies correctly, which is the config this legacy, not-currently-
	// deployed Redis-consumer path can act on today. Time-domain intrinsics
	// (span:start/end/duration) are truncated to millisecond precision during
	// extraction (blockpack issue #415).
	policy := vblockpack.BuildViColumnPolicyForTenant(ctx, e.viUsage, tenantFromBlockKey(key))
	return blockpack.ExtractValueIndexEntries(reader, policy, func(e blockpack.ValueIndexEntry) error {
		return yield(vicconsumer.ColumnEntry{
			ColName:   e.ColName,
			Value:     e.Value,
			ColType:   e.ColType,
			SourceRef: key,
			BlockRef:  e.BlockRef,
			TimeSec:   e.TimeSec,
		})
	})
}

// ── S3 ObjectPutter for consumer ──────────────────────────────────────────────

type tempoVICPutter struct {
	client *minio.Client
	bucket string
}

func (p *tempoVICPutter) Put(key string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, err := p.client.PutObject(ctx, p.bucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

// ── S3 IndexStore for compactor ───────────────────────────────────────────────

type tempoVCCStore struct {
	client *minio.Client
	bucket string
}

func (s *tempoVCCStore) Peek(ctx context.Context, key string, n int) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	_ = opts.SetRange(0, int64(n)-1)
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	buf := make([]byte, n)
	nr, err := io.ReadFull(obj, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return buf[:nr], nil
}

func (s *tempoVCCStore) List(ctx context.Context, prefix string) ([]viccompactor.IndexObject, error) {
	var objs []viccompactor.IndexObject
	for o := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if o.Err != nil {
			return nil, o.Err
		}
		objs = append(objs, viccompactor.IndexObject{Key: o.Key, Size: o.Size})
	}
	return objs, nil
}

// ListDirs returns the immediate child "directory" prefixes one level below
// prefix using a non-recursive S3 list with delimiter "/". This avoids
// loading millions of file keys when only the directory names are needed.
func (s *tempoVCCStore) ListDirs(ctx context.Context, prefix string) ([]string, error) {
	var dirs []string
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: false}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		// With Recursive: false minio returns common prefixes in obj.Key with a
		// trailing "/" and obj.Size == 0; actual objects have non-zero size.
		if strings.HasSuffix(obj.Key, "/") {
			dirs = append(dirs, obj.Key)
		}
	}
	return dirs, nil
}

func (s *tempoVCCStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

func (s *tempoVCCStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

func (s *tempoVCCStore) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// ── S3 Store adapter for VCNT compactor ──────────────────────────────────────

// tempoVCNTStore adapts tempoVCCStore to valuecountscompactor.Store. It cannot embed
// tempoVCCStore directly and rely on Go's structural typing to satisfy the interface for
// free: tempoVCCStore.List returns []viccompactor.IndexObject, but Store.List requires
// []valuecountscompactor.Object — two distinct named struct types with identical fields
// (blockpack NOTE-VC-005/SPEC-VC-1, from today's earlier value-index-compactor fix #33,
// which found the same "these look structurally identical but Go doesn't treat named
// types that way" mistake in blockpack's own doc comments). Get/Put/Delete/ListDirs have
// plain string/[]byte/error signatures with no divergent named type, so those are reused
// directly from the embedded *tempoVCCStore; only List needs a converting wrapper.
type tempoVCNTStore struct {
	store *tempoVCCStore
}

func (s *tempoVCNTStore) List(ctx context.Context, prefix string) ([]vcntcompactor.Object, error) {
	objs, err := s.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]vcntcompactor.Object, 0, len(objs))
	for _, o := range objs {
		out = append(out, vcntcompactor.Object{Key: o.Key, Size: o.Size})
	}
	return out, nil
}

func (s *tempoVCNTStore) ListDirs(ctx context.Context, prefix string) ([]string, error) {
	return s.store.ListDirs(ctx, prefix)
}

func (s *tempoVCNTStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.store.Get(ctx, key)
}

func (s *tempoVCNTStore) Put(ctx context.Context, key string, data []byte) error {
	return s.store.Put(ctx, key, data)
}

func (s *tempoVCNTStore) Delete(ctx context.Context, key string) error {
	return s.store.Delete(ctx, key)
}

// ── S3 SourceExister for compactor ───────────────────────────────────────────

type tempoVCCExister struct {
	client *minio.Client
	bucket string
}

func (e *tempoVCCExister) Exists(ctx context.Context, sourceRef string) (bool, error) {
	key := bareKey(sourceRef)
	_, err := e.client.StatObject(ctx, e.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.Code == "NoSuchKey" || resp.StatusCode == 404 {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", key, err)
	}
	return true, nil
}

// ── utilities ─────────────────────────────────────────────────────────────────

// bareKey strips the s3://bucket/ prefix from a full S3 URL, returning the bare key.
func bareKey(path string) string {
	if idx := strings.Index(path, "//"); idx >= 0 {
		rest := path[idx+2:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			return rest[slash+1:]
		}
	}
	return path
}

// minioReaderProvider wraps a minio client as a blockpack.ReaderProvider using ranged GETs.
type minioReaderProvider struct {
	client *minio.Client
	bucket string
	key    string
	size   int64
}

func newMinIOProvider(client *minio.Client, bucket, key string) blockpack.ReaderProvider {
	return &minioReaderProvider{client: client, bucket: bucket, key: key}
}

func (p *minioReaderProvider) Size() (int64, error) {
	if p.size > 0 {
		return p.size, nil
	}
	info, err := p.client.StatObject(context.Background(), p.bucket, p.key, minio.StatObjectOptions{})
	if err != nil {
		return 0, err
	}
	p.size = info.Size
	return p.size, nil
}

func (p *minioReaderProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	opts := minio.GetObjectOptions{}
	_ = opts.SetRange(off, off+int64(len(buf))-1)
	obj, err := p.client.GetObject(context.Background(), p.bucket, p.key, opts)
	if err != nil {
		return 0, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadFull(obj, buf)
}

func (p *minioReaderProvider) Close() error { return nil }

// ── local file provider ───────────────────────────────────────────────────────

type localFileProvider struct {
	f    *os.File
	size int64
}

func newLocalFileProvider(path string) (*localFileProvider, error) {
	f, err := os.Open(path) //nolint:gosec
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &localFileProvider{f: f, size: fi.Size()}, nil
}

func (p *localFileProvider) Size() (int64, error) { return p.size, nil }

func (p *localFileProvider) ReadAt(buf []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.f.ReadAt(buf, off)
}

func (p *localFileProvider) Close() error { return p.f.Close() }

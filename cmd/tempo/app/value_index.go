package app

// Value-index pipeline module targets for Tempo.
//
// Wires the blockpack value-index consumer (-target=value-index-consumer) and
// compactor (-target=value-index-compactor) as Tempo module targets.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/tempo/modules/postgres"
	s3cfg "github.com/grafana/tempo/tempodb/backend/s3"
	common "github.com/grafana/tempo/tempodb/encoding/common"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/blockpack/blockevents"
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
// blockpack.NewPgColumnManifestStore's return value is directly assignable to either
// Config's ManifestStore field with no further wrapping. Postgres is a hard requirement
// (2026-07-15) for both callers now -- the blob-backed *tempoVCCStore fallback this
// interface originally existed to accommodate (issue #507's either/or design) has been
// retired; see initValueIndexConsumer/initValueIndexCompactor.
type manifestStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, data []byte) error
}

// toVICConsumerCfg converts the Tempo config struct to the blockpack consumer config. ms is
// always non-nil in production (initValueIndexConsumer hard-fails before reaching this call if
// Postgres isn't configured); tests may still pass nil to confirm the pass-through.
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

// ── value-index consumer ──────────────────────────────────────────────────────

func (t *App) initValueIndexConsumer() (services.Service, error) {
	bp := t.cfg.StorageConfig.Trace.Block.Blockpack
	if !bp.ValueIndexConsumer.Enabled {
		return services.NewIdleService(nil, nil), nil
	}

	// Postgres is a hard requirement (2026-07-15) for the column-manifest store. Column-manifest
	// recording (colhashmanifest, task #216) was shipped in blockpack but never wired to a
	// production ManifestStore before issue #507 -- flushColumn correctly no-ops on a nil
	// ManifestStore, so recording was silently disabled for every tenant. #507 originally added
	// a blob-backed *tempoVCCStore fallback for deployments without Postgres configured; that
	// either/or was retired once Postgres became mandatory across the whole cube/value-index
	// pipeline (issue #504's registry, this consumer's manifest store) -- fail fast instead of
	// silently degrading.
	pgCfg := t.cfg.StorageConfig.Trace.Postgres
	if pgCfg == nil {
		return nil, errors.New("value-index-consumer: postgres is not configured; the column-manifest store requires it")
	}

	s3Client, err := newMinioFromS3Cfg(t.cfg.StorageConfig.Trace.S3)
	if err != nil {
		return nil, fmt.Errorf("value-index-consumer: create S3 client: %w", err)
	}
	bucket := t.cfg.StorageConfig.Trace.S3.Bucket

	pgPool, err := postgres.NewPool(context.Background(), pgCfg)
	if err != nil {
		return nil, fmt.Errorf("value-index-consumer: create postgres pool for column manifest: %w", err)
	}
	if err := blockpack.ApplyColumnManifestSchema(context.Background(), pgPool); err != nil {
		pgPool.Close()
		return nil, fmt.Errorf("value-index-consumer: apply column manifest schema: %w", err)
	}
	manStore := blockpack.NewPgColumnManifestStore(pgPool)

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
		pgPool.Close()
		return nil, fmt.Errorf("value-index-consumer: create redis consumer: %w", cerr)
	}
	consumer = rc
	consumerCloser = func() { _ = rc.Close() }

	extractor := &tempoVICExtractor{client: s3Client, bucket: bucket, viUsage: t.cfg.StorageConfig.Trace.Block.Blockpack.ViUsage}
	store := &tempoVICPutter{client: s3Client, bucket: bucket}

	svc, err := vicconsumer.NewService(vicCfg, consumer, extractor, store)
	if err != nil {
		consumerCloser()
		pgPool.Close()
		return nil, fmt.Errorf("value-index-consumer: create service: %w", err)
	}

	return services.NewIdleService(
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(_ error) error {
			consumerCloser()
			pgPool.Close()
			return nil
		},
	), nil
}

// ── value-index compactor ─────────────────────────────────────────────────────

// initValueIndexCompactor is now a permanent no-op (-target=value-index-compactor).
// It used to bundle three self-driven ticker loops: VI's own compaction
// (viccompactor.Service.Run, retired #162), VCNT's own compaction
// (vcntcompactor.Service.Run, retired #162), and the cube scheduler
// (vblockpack.ConfigureCubeScheduler's boundary-gated rollup ladder, retired
// #163). All three are superseded system-wide by blockpack's own
// compaction-planner/compaction-worker -- any one of these self-driven loops
// still running here would double-process the same blockpack_file_catalog
// rows compaction-worker's Postgres-claim-based concurrency now owns.
//
// The module target registration itself is deliberately kept (not removed
// from modules.go) purely so the currently-deployed StatefulSet's
// `-target=value-index-compactor` flag doesn't crash the process on its next
// routine image rollout, before an operator has a chance to update the
// deploy config. Actually retiring the StatefulSet (and, once that's done,
// this now-permanently-idle module registration too) is a separate,
// deliberately deferred live-infra decision (issue #522), not a code change.
func (t *App) initValueIndexCompactor() (services.Service, error) {
	return services.NewIdleService(nil, nil), nil
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

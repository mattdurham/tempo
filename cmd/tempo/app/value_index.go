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
	"os"
	"strings"
	"time"

	"github.com/grafana/dskit/services"
	s3cfg "github.com/grafana/tempo/tempodb/backend/s3"
	common "github.com/grafana/tempo/tempodb/encoding/common"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/blockpack/blockevents"
	viccompactor "github.com/grafana/blockpack/valueindexcompactor"
	vicconsumer "github.com/grafana/blockpack/valueindexconsumer"
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

// toVICConsumerCfg converts the Tempo config struct to the blockpack consumer config.
func toVICConsumerCfg(cfg common.ValueIndexConsumerConfig) vicconsumer.Config {
	return vicconsumer.Config{
		Enabled:              cfg.Enabled,
		RqliteURL:            cfg.RqliteURL,
		RedisAddr:            cfg.RedisAddr,
		StreamName:           cfg.StreamName,
		ConsumerGroup:        cfg.ConsumerGroup,
		ConsumerName:         cfg.ConsumerName,
		IndexPrefix:          cfg.IndexPrefix,
		Columns:              cfg.Columns,
		FlushInterval:        cfg.FlushInterval,
		PollTimeout:          cfg.PollTimeout,
		ClaimIdleThreshold:   cfg.ClaimIdleThreshold,
		BatchSize:            cfg.BatchSize,
	}
}

// toVICCompactorCfg converts the Tempo config struct to the blockpack compactor config.
func toVICCompactorCfg(cfg common.ValueIndexCompactorConfig) viccompactor.Config {
	return viccompactor.Config{
		Enabled:               cfg.Enabled,
		IndexPrefix:           cfg.IndexPrefix,
		Tenants:               cfg.Tenants,
		CompactInterval:       cfg.CompactInterval,
		CompactThresholdFiles: cfg.CompactThresholdFiles,
		MaxOutputBytes:        cfg.MaxOutputBytes,
	}
}

// ── value-index consumer ──────────────────────────────────────────────────────

func (t *App) initValueIndexConsumer() (services.Service, error) {
	vicCfg := toVICConsumerCfg(t.cfg.StorageConfig.Trace.Block.Blockpack.ValueIndexConsumer)
	if !vicCfg.Enabled {
		return services.NewIdleService(nil, nil), nil
	}
	// Expose consumer pipeline metrics on the default registry.
	vicCfg.Registerer = prometheus.DefaultRegisterer

	s3Client, err := newMinioFromS3Cfg(t.cfg.StorageConfig.Trace.S3)
	if err != nil {
		return nil, fmt.Errorf("value-index-consumer: create S3 client: %w", err)
	}

	var consumer vicconsumer.Consumer
	var consumerCloser func()
	if vicCfg.RqliteURL != "" {
		rc, cerr := vicconsumer.NewRqliteConsumer(vicCfg)
		if cerr != nil {
			return nil, fmt.Errorf("value-index-consumer: create rqlite consumer: %w", cerr)
		}
		consumer = rc
		consumerCloser = func() { _ = rc.Close() }
	} else {
		rc, cerr := vicconsumer.NewRedisConsumer(vicCfg)
		if cerr != nil {
			return nil, fmt.Errorf("value-index-consumer: create redis consumer: %w", cerr)
		}
		consumer = rc
		consumerCloser = func() { _ = rc.Close() }
	}
	if err != nil {
		return nil, fmt.Errorf("value-index-consumer: create redis consumer: %w", err)
	}

	bucket := t.cfg.StorageConfig.Trace.S3.Bucket
	extractor := &tempoVICExtractor{client: s3Client, bucket: bucket}
	store := &tempoVICPutter{client: s3Client, bucket: bucket}

	svc, err := vicconsumer.NewService(vicCfg, consumer, extractor, store)
	if err != nil {
		consumerCloser()
		return nil, fmt.Errorf("value-index-consumer: create service: %w", err)
	}

	return services.NewIdleService(
		func(ctx context.Context) error { return svc.Run(ctx) },
		func(_ error) error { consumerCloser(); return nil },
	), nil
}

// ── value-index compactor ─────────────────────────────────────────────────────

func (t *App) initValueIndexCompactor() (services.Service, error) {
	vccCfg := toVICCompactorCfg(t.cfg.StorageConfig.Trace.Block.Blockpack.ValueIndexCompactor)
	if !vccCfg.Enabled {
		return services.NewIdleService(nil, nil), nil
	}
	// Expose compactor pipeline metrics on the default registry.
	vccCfg.Registerer = prometheus.DefaultRegisterer

	s3Client, err := newMinioFromS3Cfg(t.cfg.StorageConfig.Trace.S3)
	if err != nil {
		return nil, fmt.Errorf("value-index-compactor: create S3 client: %w", err)
	}

	bucket := t.cfg.StorageConfig.Trace.S3.Bucket
	store := &tempoVCCStore{client: s3Client, bucket: bucket}
	exister := &tempoVCCExister{client: s3Client, bucket: bucket}

	svc, err := viccompactor.NewService(vccCfg, store, exister)
	if err != nil {
		return nil, fmt.Errorf("value-index-compactor: create service: %w", err)
	}

	return services.NewIdleService(
		func(ctx context.Context) error { return svc.Run(ctx) },
		nil,
	), nil
}

// ── S3 extractor for consumer ─────────────────────────────────────────────────

type tempoVICExtractor struct {
	client *minio.Client
	bucket string
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
	// value-index-consumer binary share one implementation. nil denylist indexes
	// every column (NOTE-VI-027, blockpack issue #414): the value index is
	// policy-free and the querier decides which columns are useful at read time.
	// Time-domain intrinsics (span:start/end/duration) are truncated to millisecond
	// precision during extraction (blockpack issue #415).
	return blockpack.ExtractValueIndexEntries(reader, nil, func(e blockpack.ValueIndexEntry) error {
		return yield(vicconsumer.ColumnEntry{
			ColName:   e.ColName,
			Value:     e.Value,
			ColType:   e.ColType,
			SourceRef: key,
			BlockID:   e.BlockID,
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

func (s *tempoVCCStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket,
		minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
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

// Command value-index-consumer runs the value-index consumer service.
//
// It polls a Redis Streams queue for blockpack "create" events, reads each
// referenced blockpack file from S3 via ranged GETs, extracts per-column
// entries for the configured set of columns, and flushes time-windowed L0
// value index files to S3.
//
// Usage:
//
//	value-index-consumer -config /etc/value-index/config.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v3"

	blockpack "github.com/grafana/blockpack"
	"github.com/grafana/blockpack/internal/modules/blockevents"
	"github.com/grafana/blockpack/internal/s3provider"
	vicconsumer "github.com/grafana/blockpack/valueindexconsumer"
)

// Config is the full config for the standalone binary.
type Config struct {
	S3 struct {
		Bucket   string `yaml:"bucket"`
		Endpoint string `yaml:"endpoint"`
		Region   string `yaml:"region"`
	} `yaml:"s3"`
	Consumer vicconsumer.Config `yaml:"value_index_consumer"`
}

func main() {
	// NOTE-LINT-407: run() returns an error so deferred cleanup (consumer.Close,
	// cancel) runs before os.Exit — gocritic exitAfterDefer.
	if err := run(); err != nil {
		slog.Error("value-index-consumer failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to config YAML file")
	flag.Parse()

	if *configPath == "" {
		return fmt.Errorf("usage: value-index-consumer -config <path>")
	}

	raw, err := os.ReadFile(*configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if uerr := yaml.Unmarshal(raw, &cfg); uerr != nil {
		return fmt.Errorf("parse config: %w", uerr)
	}

	// S3 client via minio SDK (handles AWS SigV4 automatically).
	minioClient, err := minio.New(cfg.S3.Endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: true,
		Region: cfg.S3.Region,
	})
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	// Redis consumer.
	consumer, err := vicconsumer.NewRedisConsumer(cfg.Consumer)
	if err != nil {
		return fmt.Errorf("create redis consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	// Extractor: reads blockpack files from S3 via ranged GETs.
	extractor := &s3Extractor{
		client: minioClient,
		bucket: cfg.S3.Bucket,
	}

	// ObjectPutter: writes value index files to S3.
	store := &s3ObjectPutter{client: minioClient, bucket: cfg.S3.Bucket}

	// Inject the structured logger so the consumer logs job/flush boundaries
	// (NOTE-VI-028, issue #410). nil would fall back to slog.Default() anyway,
	// but passing it explicitly documents the wiring.
	cfg.Consumer.Logger = slog.Default()

	svc, err := vicconsumer.NewService(cfg.Consumer, consumer, extractor, store)
	if err != nil {
		return fmt.Errorf("create consumer service: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info(
		"value-index-consumer starting",
		"redis", cfg.Consumer.RedisAddr,
		"bucket", cfg.S3.Bucket,
		"columns", cfg.Consumer.Columns,
	)
	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("consumer exited: %w", err)
	}
	slog.Info("value-index-consumer stopped")
	return nil
}

// s3Extractor implements valueindexconsumer.Extractor using a lean blockpack
// reader backed by ranged S3 GETs. Extraction (the two-phase intrinsic + attribute
// column walk and the denylist) is delegated to blockpack.ExtractValueIndexEntries
// (NOTE-VI-018) so this binary and tempo's in-process module share one
// implementation.
type s3Extractor struct {
	client *minio.Client
	bucket string
}

func (e *s3Extractor) Extract(
	ctx context.Context,
	event blockevents.Message,
	yield func(vicconsumer.ColumnEntry) error,
) error {
	// Parse "<tenant>/<block-id>/data.blockpack" from the full S3 path.
	// The path may start with s3://bucket/ — strip that prefix.
	path := event.Path
	if idx := strings.Index(path, "//"); idx >= 0 {
		// s3://bucket/key → key
		rest := path[idx+2:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			path = rest[slash+1:]
		}
	}

	// Confirm existence (and surface non-404 errors) before opening the reader.
	if _, err := e.client.StatObject(ctx, e.bucket, path, minio.StatObjectOptions{}); err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	// Open lean reader with ranged GET provider. MinIOProvider fetches size lazily.
	prov := s3provider.NewMinIOProvider(e.client, e.bucket, path)
	reader, err := blockpack.NewLeanReaderFromProvider(prov)
	if err != nil {
		return fmt.Errorf("open lean reader %s: %w", path, err)
	}

	// nil denylist indexes every column (NOTE-VI-027, issue #414): the value index
	// is policy-free, so the querier decides which columns are useful at read time.
	// Time-domain intrinsics (span:start/end/duration) are truncated to millisecond
	// precision during extraction (issue #415).
	return blockpack.ExtractValueIndexEntries(reader, nil, func(e blockpack.ValueIndexEntry) error {
		return yield(vicconsumer.ColumnEntry{
			ColName:   e.ColName,
			Value:     e.Value,
			ColType:   e.ColType,
			SourceRef: path,
			BlockID:   e.BlockID,
			BlockRef:  e.BlockRef,
			TimeSec:   e.TimeSec,
			SpanID:    e.SpanID,
			RowIdx:    uint16(e.RowIdx), //nolint:gosec // bounded by MaxBlockSpans ≤ 65534
		})
	})
}

// s3ObjectPutter implements valueindexconsumer.ObjectPutter backed by minio.
type s3ObjectPutter struct {
	client *minio.Client
	bucket string
}

func (p *s3ObjectPutter) Put(key string, data []byte) error {
	_, err := p.client.PutObject(
		context.Background(), p.bucket, key,
		strings.NewReader(string(data)), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

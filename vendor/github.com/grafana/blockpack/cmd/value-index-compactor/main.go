// Command value-index-compactor runs the value-index compactor service.
//
// It periodically merges L0 value index files into larger L1/L2 files per
// (tenant, column), dropping posting-list entries whose source blockpack file
// has been deleted by retention.
//
// Usage:
//
//	value-index-compactor -config /etc/value-index/config.yaml
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v3"

	viccompactor "github.com/grafana/blockpack/valueindexcompactor"
)

// Config is the full config for the standalone binary.
type Config struct {
	S3 struct {
		Bucket   string `yaml:"bucket"`
		Endpoint string `yaml:"endpoint"`
		Region   string `yaml:"region"`
	} `yaml:"s3"`
	Compactor viccompactor.Config `yaml:"value_index_compactor"`
}

func main() {
	// NOTE-LINT-407: run() returns an error so deferred cleanup (cancel) runs
	// before os.Exit — gocritic exitAfterDefer.
	if err := run(); err != nil {
		slog.Error("value-index-compactor failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to config YAML file")
	flag.Parse()

	if *configPath == "" {
		return fmt.Errorf("usage: value-index-compactor -config <path>")
	}

	raw, err := os.ReadFile(*configPath)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if uerr := yaml.Unmarshal(raw, &cfg); uerr != nil {
		return fmt.Errorf("parse config: %w", uerr)
	}

	minioClient, err := minio.New(cfg.S3.Endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: true,
		Region: cfg.S3.Region,
	})
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	store := &s3IndexStore{client: minioClient, bucket: cfg.S3.Bucket}
	exister := &s3SourceExister{client: minioClient, bucket: cfg.S3.Bucket}

	svc, err := viccompactor.NewService(cfg.Compactor, store, exister)
	if err != nil {
		return fmt.Errorf("create compactor service: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info(
		"value-index-compactor starting",
		"bucket", cfg.S3.Bucket,
		"tenants", cfg.Compactor.Tenants,
		"interval", cfg.Compactor.CompactInterval,
	)
	if err := svc.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("compactor exited: %w", err)
	}
	slog.Info("value-index-compactor stopped")
	return nil
}

// s3IndexStore implements viccompactor.IndexStore backed by minio.
type s3IndexStore struct {
	client *minio.Client
	bucket string
}

func (s *s3IndexStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (s *s3IndexStore) ListDirs(ctx context.Context, prefix string) ([]string, error) {
	var dirs []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, "/") {
			dirs = append(dirs, obj.Key)
		}
	}
	return dirs, nil
}

func (s *s3IndexStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

func (s *s3IndexStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(
		ctx, s.bucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

func (s *s3IndexStore) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// s3SourceExister checks whether a source blockpack still exists via HEAD.
type s3SourceExister struct {
	client *minio.Client
	bucket string
}

func (e *s3SourceExister) Exists(ctx context.Context, sourceRef string) (bool, error) {
	// sourceRef may be a full s3:// URL — strip to bare key.
	key := sourceRef
	if idx := strings.Index(key, "//"); idx >= 0 {
		rest := key[idx+2:]
		if slash := strings.Index(rest, "/"); slash >= 0 {
			key = rest[slash+1:]
		}
	}
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

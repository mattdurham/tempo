package vblockpack

// valueindex.go — writer-side synchronous value-index write path (blockpack
// NOTE-VI-042, issue #464). When configured, the block-builder (create.go) and
// backend-worker compactor (compactor.go) call WriteValueIndexL0 after every block
// flush / compaction output, writing per-column L0 index files to object storage
// with no Redis broker.
//
// The store is a process-level singleton, configured once at startup via
// ConfigureValueIndex (mirroring the embedder / block-events / value-index-query
// singletons). When it is nil (the default, value_index_enabled=false) the write
// path is a no-op: getValueIndexSink returns (nil, "") and the callers skip the
// index write entirely — byte-identical to before.

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/go-kit/log/level"
	util_log "github.com/grafana/tempo/pkg/util/log"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	blockpack "github.com/grafana/blockpack"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
)

// fileReaderProvider implements blockpack.ReaderProvider over an *os.File using
// pread (ReadAt), so the value-index extractor can stream a freshly-written block
// from its temp file without buffering the whole encoded block in memory.
type fileReaderProvider struct {
	f *os.File
}

func (p *fileReaderProvider) Size() (int64, error) {
	fi, err := p.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (p *fileReaderProvider) ReadAt(b []byte, off int64, _ blockpack.DataType) (int, error) {
	return p.f.ReadAt(b, off)
}

// s3ObjectPutter writes value-index files to S3 via minio. It satisfies
// blockpack.ObjectPutter (Put by full object key).
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

var (
	valueIndexSink        blockpack.ObjectPutter
	valueIndexPrefix      string
	valueIndexSinkMu      sync.RWMutex
	valueIndexConfigOnce  sync.Once
	defaultValueIndexPref = "indexes"
)

// ConfigureValueIndex installs the process-level value-index write sink. Call once
// at startup (tempodb.New) on writer targets (block-builder, backend-worker) when
// value_index_enabled is true and the backend is S3.
//
// A failure to build the S3 client leaves the sink unset and is logged — block
// creation and compaction must never fail because the index sink could not be set
// up. The index can always be rebuilt from the source block.
func ConfigureValueIndex(enabled bool, s3cfg *s3backend.Config, indexPrefix string) {
	if !enabled || s3cfg == nil {
		return
	}
	valueIndexConfigOnce.Do(func() {
		client, err := newMinioForValueIndex(s3cfg)
		if err != nil {
			level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index write path disabled — S3 client init failed", "err", err)
			return
		}
		if indexPrefix == "" {
			indexPrefix = defaultValueIndexPref
		}
		valueIndexSinkMu.Lock()
		valueIndexSink = &s3ObjectPutter{client: client, bucket: s3cfg.Bucket}
		valueIndexPrefix = indexPrefix
		valueIndexSinkMu.Unlock()
		level.Info(util_log.Logger).Log("msg", "vblockpack: value-index write path configured", "bucket", s3cfg.Bucket, "prefix", indexPrefix)
	})
}

// getValueIndexSink returns the configured object store and index prefix for the
// write path, or (nil, "") when the write path is disabled. create.go / compactor.go
// skip the index write when the store is nil.
func getValueIndexSink() (blockpack.ObjectPutter, string) {
	valueIndexSinkMu.RLock()
	defer valueIndexSinkMu.RUnlock()
	return valueIndexSink, valueIndexPrefix
}

// newMinioForValueIndex builds a minio client for the value-index write path from
// the trace S3 config. Credentials come from the AWS environment, matching the
// querier-side value-index client (tempodb.go).
func newMinioForValueIndex(cfg *s3backend.Config) (*minio.Client, error) {
	endpoint := cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3." + cfg.Region + ".amazonaws.com"
	}
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewEnvAWS(),
		Secure: !cfg.Insecure,
		Region: cfg.Region,
	})
}

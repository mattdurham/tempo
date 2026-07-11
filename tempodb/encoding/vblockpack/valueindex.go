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
	"github.com/grafana/tempo/tempodb/backend"
	s3backend "github.com/grafana/tempo/tempodb/backend/s3"
)

// minioNoSuchKeyCode is minio's error Code for a 404 (object not found),
// checked alongside the HTTP status across this package's several minio-backed
// object-store implementations (cubemanager.go, value_index_query.go,
// vi_backfill.go).
const minioNoSuchKeyCode = "NoSuchKey"

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

	// vcntSink is set alongside valueIndexSink — same S3 client, same bucket.
	// VCNT files go under <tenant>/value_counts/<colHash>/L0-<id>.vcnt
	vcntSink blockpack.ObjectPutter
)

// ConfigureValueIndex installs the process-level value-index write sink. Call once
// at startup (tempodb.New) on writer targets (block-builder, backend-worker) when
// value_index_enabled is true. s3cfg configures the S3 path (unchanged, byte-identical
// s3ObjectPutter construction); rawW configures the generic (Local/GCS/Azure) path via
// rawObjectPutter when s3cfg is nil. Exactly one of s3cfg/rawW is expected non-nil per
// tempodb.go's own backend-gated construction (plan.md §10) — s3cfg takes priority if
// both happen to be set, matching every other Configure* function's own S3-first branch
// order in this package.
//
// A failure to build the S3 client leaves the sink unset and is logged — block
// creation and compaction must never fail because the index sink could not be set
// up. The index can always be rebuilt from the source block.
func ConfigureValueIndex(enabled bool, s3cfg *s3backend.Config, rawW backend.RawWriter, indexPrefix string) {
	if !enabled || (s3cfg == nil && rawW == nil) {
		return
	}
	valueIndexConfigOnce.Do(func() {
		if indexPrefix == "" {
			indexPrefix = defaultValueIndexPref
		}
		if s3cfg != nil {
			client, err := newMinioForValueIndex(s3cfg)
			if err != nil {
				level.Warn(util_log.Logger).Log("msg", "vblockpack: value-index write path disabled — S3 client init failed", "err", err)
				return
			}
			valueIndexSinkMu.Lock()
			valueIndexSink = &s3ObjectPutter{client: client, bucket: s3cfg.Bucket}
			valueIndexPrefix = indexPrefix
			vcntSink = &s3ObjectPutter{client: client, bucket: s3cfg.Bucket}
			valueIndexSinkMu.Unlock()
			level.Info(util_log.Logger).Log("msg", "vblockpack: value-index + vcnt write path configured", "bucket", s3cfg.Bucket, "prefix", indexPrefix)
			return
		}
		putter := newRawObjectPutter(rawW)
		valueIndexSinkMu.Lock()
		valueIndexSink = putter
		valueIndexPrefix = indexPrefix
		vcntSink = putter
		valueIndexSinkMu.Unlock()
		level.Info(util_log.Logger).Log("msg", "vblockpack: value-index + vcnt write path configured (generic backend)", "prefix", indexPrefix)
	})
}

// getVCNTSink returns the configured object store for VCNT .vcnt files.
// Nil when value_index_enabled is false.
func getVCNTSink() blockpack.ObjectPutter {
	valueIndexSinkMu.RLock()
	defer valueIndexSinkMu.RUnlock()
	return vcntSink
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

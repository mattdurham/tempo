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
	"github.com/grafana/tempo/tempodb/backend/instrumentation"
)

// minioNoSuchKeyCode is minio's error Code for a 404 (object not found),
// checked alongside the HTTP status across this package's several minio-backed
// object-store implementations (cubemanager.go, value_index_query.go,
// vi_backfill.go).
const minioNoSuchKeyCode = "NoSuchKey"

// isMinioNoSuchKey reports whether err is minio's NoSuchKey/404 response, however it
// surfaced. minio-go's GetObject is lazy: for a nonexistent key it returns a reader with
// a nil error, and the real 404 only appears on the first Read from that reader (e.g.
// inside io.ReadAll) -- an error from EITHER GetObject itself OR the first subsequent
// read must be classified through this same check, since minio.ToErrorResponse works
// identically regardless of which call actually triggered the underlying HTTP request.
// A caller that only checks GetObject's own error (the natural but incomplete first
// instinct) will misclassify every "genuinely missing object" case as a hard error
// instead of blockpack.ErrNotFound/CubeErrNotFound, live-confirmed to silently prevent a
// tenant's usage/cube registry from ever being created in the first place.
func isMinioNoSuchKey(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.Code == minioNoSuchKeyCode || resp.StatusCode == 404
}

// newMinioClientFromS3Config builds a *minio.Client for cfg, resolving credentials from
// cfg.AccessKey/SecretKey/SessionToken first and falling back to the AWS environment.
// Shared by every S3-backed minio.Client construction in this package (cube backfill, VI
// backfill, this file's own value-index write path, cube manager, cube query path) --
// consistency fix (2026-07-14): each of these previously called credentials.NewEnvAWS()
// directly and independently, silently ignoring any explicit config-file credentials even
// when set. minio-go's Chain.Retrieve skips a provider whose AccessKeyID/SecretAccessKey are
// both empty, so this remains backward-compatible with existing env-var-only deployments.
func newMinioClientFromS3Config(s3cfg *s3backend.Config) (*minio.Client, error) {
	endpoint := s3cfg.Endpoint
	if endpoint == "" {
		endpoint = "s3." + s3cfg.Region + ".amazonaws.com"
	}
	creds := credentials.NewChainCredentials([]credentials.Provider{
		&credentials.Static{Value: credentials.Value{
			AccessKeyID:     s3cfg.AccessKey,
			SecretAccessKey: s3cfg.SecretKey.String(),
			SessionToken:    s3cfg.SessionToken.String(),
		}},
		&credentials.EnvAWS{},
	})
	// Every caller of this constructor (cube backfill, cube manager, cube query path, VI usage
	// hook, VI backfill) previously got minio-go's bare DefaultTransport -- no pool tuning, no
	// instrumentation, unlike the value-index query path's own client (tempodb.go's
	// newMinioForValueIndex). cubequerypath.go's cube-backed metrics queries can fan out many
	// concurrent GetObject calls for one query's L0 files, so this needs the same treatment.
	transport, err := minio.DefaultTransport(!s3cfg.Insecure)
	if err != nil {
		return nil, err
	}
	transport.MaxIdleConnsPerHost = 512
	transport.MaxIdleConns = 512
	return minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    !s3cfg.Insecure,
		Region:    s3cfg.Region,
		Transport: instrumentation.NewTransport(transport),
	})
}

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
			client, err := newMinioClientFromS3Config(s3cfg)
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

// chunk_client.go — Loki chunk store with injected per-read IO latency.
//
// Provides latencyChunkStore, a drop-in replacement for bench.ChunkStore that
// wraps the underlying filesystem ObjectClient with latencyObjectClient so that
// every chunk read sleeps for a configurable duration before returning data.
// This accurately models S3/GCS round-trip latency (typically 10–50 ms per request)
// without touching write paths, keeping data-generation setup fast.
//
// The latencyObjectClient only delays read operations (GetObject, GetObjectRange,
// ObjectExists, GetAttributes, List) — writes (PutObject, DeleteObject) pass
// through immediately.
//
// Usage:
//
//	// Write phase — no latency (fast setup)
//	writeStore, _ := bench.NewChunkStore(dir, tenant)
//	bench.NewBuilder(dir, opt, writeStore, ...).Generate(ctx, size)
//
//	// Read phase — with injected latency
//	readStore, _ := newLatencyChunkStore(dir, tenant, 20*time.Millisecond)
//	querier, _ := readStore.Querier()
package lokibench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/compression"
	ingclient "github.com/grafana/loki/v3/pkg/ingester/client"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/grafana/loki/v3/pkg/storage"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	objectclient "github.com/grafana/loki/v3/pkg/storage/chunk/client"
	"github.com/grafana/loki/v3/pkg/storage/chunk/client/local"
	storageconfig "github.com/grafana/loki/v3/pkg/storage/config"
	"github.com/grafana/loki/v3/pkg/storage/stores/shipper/indexshipper"
	lokiutil "github.com/grafana/loki/v3/pkg/util"
	util_log "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/pkg/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

const (
	lokiChunkTargetSize = 1572864    // 1.5 MB — matches bench.ChunkStore
	lokiBlockSize       = 256 * 1024 // 256 KB
)

// latencyObjectClient wraps an ObjectClient and sleeps before each read
// operation to simulate object storage round-trip latency (e.g. S3/GCS).
// Write operations (PutObject, DeleteObject) pass through without delay.
// It also tracks total bytes read for benchmark reporting.
type latencyObjectClient struct {
	objectclient.ObjectClient
	latency   time.Duration
	bytesRead atomic.Int64
	ioCount   atomic.Int64
}

func (c *latencyObjectClient) sleep() {
	if c.latency > 0 {
		time.Sleep(c.latency)
	}
}

// BytesRead returns total bytes read since creation (or last Reset).
func (c *latencyObjectClient) BytesRead() int64 { return c.bytesRead.Load() }

// IOCount returns total read operations since creation (or last Reset).
func (c *latencyObjectClient) IOCount() int64 { return c.ioCount.Load() }

// Reset zeroes the bytes-read and IO counters.
func (c *latencyObjectClient) Reset() {
	c.bytesRead.Store(0)
	c.ioCount.Store(0)
}

func (c *latencyObjectClient) GetObject(ctx context.Context, objectKey string) (io.ReadCloser, int64, error) {
	c.sleep()
	c.ioCount.Add(1)
	rc, size, err := c.ObjectClient.GetObject(ctx, objectKey)
	if err == nil {
		rc = &countingReadCloser{inner: rc, counter: &c.bytesRead}
	}
	return rc, size, err
}

func (c *latencyObjectClient) GetObjectRange(
	ctx context.Context,
	objectKey string,
	off, length int64,
) (io.ReadCloser, error) {
	c.sleep()
	c.ioCount.Add(1)
	rc, err := c.ObjectClient.GetObjectRange(ctx, objectKey, off, length)
	if err == nil {
		rc = &countingReadCloser{inner: rc, counter: &c.bytesRead}
	}
	return rc, err
}

// countingReadCloser wraps an io.ReadCloser and adds each Read's byte count
// to an external atomic counter. This tracks actual bytes consumed, not the
// advertised object size.
type countingReadCloser struct {
	inner   io.ReadCloser
	counter *atomic.Int64
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.inner.Read(p)
	if n > 0 {
		r.counter.Add(int64(n))
	}
	return n, err
}

func (r *countingReadCloser) Close() error { return r.inner.Close() }

func (c *latencyObjectClient) ObjectExists(ctx context.Context, objectKey string) (bool, error) {
	c.sleep()
	c.ioCount.Add(1)
	return c.ObjectClient.ObjectExists(ctx, objectKey)
}

func (c *latencyObjectClient) GetAttributes(
	ctx context.Context,
	objectKey string,
) (objectclient.ObjectAttributes, error) {
	c.sleep()
	c.ioCount.Add(1)
	return c.ObjectClient.GetAttributes(ctx, objectKey)
}

func (c *latencyObjectClient) List(
	ctx context.Context,
	prefix, delimiter string,
) ([]objectclient.StorageObject, []objectclient.StorageCommonPrefix, error) {
	c.sleep()
	c.ioCount.Add(1)
	return c.ObjectClient.List(ctx, prefix, delimiter)
}

// latencyChunkStore is a Loki chunk store with IO latency injected at the
// ObjectClient layer. It mirrors bench.NewChunkStore's setup but accepts a
// latency parameter and installs latencyObjectClient via ObjectClientDecorator.
type latencyChunkStore struct {
	store    *storage.LokiStore
	client   *latencyObjectClient // exposed for byte/IO tracking
	chunks   map[string]*chunkenc.MemChunk
	tenantID string
	cacheDir string
}

// newLatencyChunkStore creates a Loki chunk store backed by the filesystem at
// dir, with every ObjectClient read sleeping for latency before returning data.
// Directory layout matches bench.NewChunkStore so both stores share the same
// on-disk data when given the same dir.
func newLatencyChunkStore(dir, tenantID string, latency time.Duration) (*latencyChunkStore, error) {
	storDir := filepath.Join(dir, "storage")
	workDir := filepath.Join(dir, "workingdir")

	tsdbDir := filepath.Join(workDir, "tsdb-shipper-active")
	if err := os.MkdirAll(tsdbDir, 0o755); err != nil {
		return nil, fmt.Errorf("newLatencyChunkStore: mkdir tsdb: %w", err)
	}
	cacheDir := filepath.Join(workDir, "cache")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("newLatencyChunkStore: mkdir cache: %w", err)
	}

	storeCfg := storage.Config{
		MaxChunkBatchSize: 50,
		TSDBShipperConfig: indexshipper.Config{
			ActiveIndexDirectory: tsdbDir,
			Mode:                 indexshipper.ModeReadWrite,
			IngesterName:         "test",
			CacheLocation:        cacheDir,
			ResyncInterval:       5 * time.Minute,
			CacheTTL:             24 * time.Hour,
		},
		FSConfig: local.FSConfig{Directory: storDir},
	}
	var trackingClient *latencyObjectClient
	storeCfg.ObjectClientDecorator = func(c objectclient.ObjectClient) objectclient.ObjectClient {
		trackingClient = &latencyObjectClient{ObjectClient: c, latency: latency}
		return trackingClient
	}

	period := storageconfig.PeriodConfig{
		From:       storageconfig.DayTime{Time: model.Earliest},
		IndexType:  "tsdb",
		ObjectType: "filesystem",
		Schema:     "v13",
		IndexTables: storageconfig.IndexPeriodicTableConfig{
			PathPrefix: "index/",
			PeriodicTableConfig: storageconfig.PeriodicTableConfig{
				Prefix: "index_",
				Period: 24 * time.Hour,
			},
		},
		RowShards: 16,
	}
	schemaCfg := storageconfig.SchemaConfig{Configs: []storageconfig.PeriodConfig{period}}

	cm := storage.NewClientMetrics()
	lim := validation.Limits{}
	flagext.DefaultValues(&lim)
	overrides, _ := validation.NewOverrides(lim, nil)

	store, err := storage.NewStore(
		storeCfg, storageconfig.ChunkStoreConfig{}, schemaCfg, overrides,
		cm, prometheus.DefaultRegisterer, util_log.Logger, "cortex",
	)
	if err != nil {
		return nil, fmt.Errorf("newLatencyChunkStore: NewStore: %w", err)
	}
	return &latencyChunkStore{
		store:    store,
		client:   trackingClient,
		tenantID: tenantID,
		chunks:   make(map[string]*chunkenc.MemChunk),
		cacheDir: cacheDir,
	}, nil
}

// IOCount returns the total number of object-client read calls since the last ResetIO.
func (s *latencyChunkStore) IOCount() int64 {
	if s.client == nil {
		return 0
	}
	return s.client.IOCount()
}

// BytesRead returns the total bytes read from the object client since the last ResetIO.
func (s *latencyChunkStore) BytesRead() int64 {
	if s.client == nil {
		return 0
	}
	return s.client.BytesRead()
}

// ResetIO zeroes the IO and bytes-read counters on the underlying object client.
func (s *latencyChunkStore) ResetIO() {
	if s.client != nil {
		s.client.Reset()
	}
}

// ClearCache removes all cached TSDB index files so the next query re-downloads
// them from the object store. Call this before each query sub-benchmark to ensure
// the chunk store always starts with a cold index cache — matching blockpack's
// behaviour (blockpack has no separate cache layer).
func (s *latencyChunkStore) ClearCache() error {
	if err := os.RemoveAll(s.cacheDir); err != nil {
		return err
	}
	return os.MkdirAll(s.cacheDir, 0o755)
}

// Write appends entries to in-memory chunks, flushing to the LokiStore when full.
func (s *latencyChunkStore) Write(ctx context.Context, streams []logproto.Stream) error {
	for _, stream := range streams {
		enc, ok := s.chunks[stream.Labels]
		if !ok {
			enc = newLokiMemChunk()
			s.chunks[stream.Labels] = enc
		}
		for _, entry := range stream.Entries {
			if !enc.SpaceFor(&entry) {
				if err := s.flushChunk(ctx, enc, stream.Labels); err != nil {
					return err
				}
				enc = newLokiMemChunk()
				s.chunks[stream.Labels] = enc
			}
			if _, err := enc.Append(&entry); err != nil {
				return err
			}
		}
	}
	return nil
}

func newLokiMemChunk() *chunkenc.MemChunk {
	return chunkenc.NewMemChunk(
		chunkenc.ChunkFormatV4, compression.Snappy,
		chunkenc.UnorderedWithStructuredMetadataHeadBlockFmt,
		lokiBlockSize, lokiChunkTargetSize,
	)
}

func (s *latencyChunkStore) flushChunk(ctx context.Context, memChunk *chunkenc.MemChunk, labelsStr string) error {
	if err := memChunk.Close(); err != nil {
		return err
	}
	lbs, err := syntax.ParseLabels(labelsStr)
	if err != nil {
		return err
	}
	lb := labels.NewBuilder(lbs)
	lb.Set(model.MetricNameLabel, "logs")
	metric := lb.Labels()
	fp := ingclient.Fingerprint(lbs)

	firstTime, lastTime := lokiutil.RoundToMilliseconds(memChunk.Bounds())
	c := chunk.NewChunk(s.tenantID, fp, metric, chunkenc.NewFacade(memChunk, 0, 0), firstTime, lastTime)
	if err := c.Encode(); err != nil {
		return err
	}
	return s.store.Put(ctx, []chunk.Chunk{c})
}

// Name implements bench.Store.
func (s *latencyChunkStore) Name() string { return "chunk" }

// Querier returns the underlying LokiStore as a logql.Querier.
func (s *latencyChunkStore) Querier() (logql.Querier, error) { return s.store, nil }

// Close flushes remaining in-memory chunks and shuts down the store.
func (s *latencyChunkStore) Close() error {
	for labelsStr, c := range s.chunks {
		if err := s.flushChunk(context.Background(), c, labelsStr); err != nil {
			return err
		}
	}
	clear(s.chunks)
	s.store.Stop()
	return nil
}

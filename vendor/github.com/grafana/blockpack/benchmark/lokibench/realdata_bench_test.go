package lokibench

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/chunkenc"
	lokiengine "github.com/grafana/loki/v3/pkg/engine"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	logqlog "github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

const (
	// Set via -chunks flag or env var LOKIBENCH_CHUNKS_DIR.
	defaultChunksDir = "/tmp/loki-bench/chunks"
	defaultTenant    = "29"
	realDataMaxMB    = 200
)

func chunksDir() string {
	if d := os.Getenv("LOKIBENCH_CHUNKS_DIR"); d != "" {
		return d
	}
	return defaultChunksDir
}

// decodeRealChunks reads Loki chunk objects from disk and returns logproto.Stream
// entries grouped by their original stream labels. maxMB limits raw bytes read.
func decodeRealChunks(tb testing.TB, dir, tenant string, maxMB int) []logproto.Stream {
	tb.Helper()

	type streamData struct {
		labels  string
		entries []logproto.Entry
	}
	streamMap := make(map[string]*streamData)
	var totalBytes int64
	maxBytes := int64(maxMB) * 1024 * 1024
	var decoded int

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || totalBytes >= maxBytes {
			if totalBytes >= maxBytes {
				return filepath.SkipAll
			}
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return nil
		}
		totalBytes += int64(len(data))

		parts := strings.Split(path, "/")
		if len(parts) < 2 {
			return nil
		}
		fp := parts[len(parts)-2]
		fname := parts[len(parts)-1]
		key := fmt.Sprintf("%s/%s/%s", tenant, fp, fname)

		c, decErr := chunk.ParseExternalKey(tenant, key)
		if decErr != nil {
			return nil
		}
		if decErr = c.Decode(chunk.NewDecodeContext(), data); decErr != nil {
			return nil
		}

		lblsStr := c.Metric.String()
		lokiChk := c.Data.(*chunkenc.Facade)
		it, itErr := lokiChk.LokiChunk().Iterator(
			context.Background(),
			time.Unix(0, 0),
			time.Unix(1<<50, 0),
			logproto.FORWARD,
			logqlog.NewNoopPipeline().ForStream(labels.EmptyLabels()),
		)
		if itErr != nil {
			return nil
		}

		sd, ok := streamMap[lblsStr]
		if !ok {
			sd = &streamData{labels: lblsStr}
			streamMap[lblsStr] = sd
		}
		for it.Next() {
			sd.entries = append(sd.entries, it.At())
		}
		it.Close()
		decoded++

		return nil
	})
	require.NoError(tb, err)

	streams := make([]logproto.Stream, 0, len(streamMap))
	var totalEntries int
	for _, sd := range streamMap {
		streams = append(streams, logproto.Stream{
			Labels:  sd.labels,
			Entries: sd.entries,
		})
		totalEntries += len(sd.entries)
	}

	tb.Logf("Decoded %d chunks → %d streams, %d entries (%.0f MB raw)",
		decoded, len(streams), totalEntries, float64(totalBytes)/1024/1024)
	return streams
}

// BenchmarkRealData_LogOnly benchmarks LogQL queries against real Loki production
// data from tenant 29. Compares chunk store vs blockpack.
//
// Prerequisites: download chunks to /tmp/loki-bench/chunks/ first:
//
//	gsutil -m cp -r "gs://dev-us-central-0-loki-dev-005-data/29/1000*" /tmp/loki-bench/chunks/
//	gsutil -m cp -r "gs://dev-us-central-0-loki-dev-005-data/29/1001*" /tmp/loki-bench/chunks/
//	... etc
//
// Run:
//
//	cd benchmark/lokibench && go test -bench BenchmarkRealData_LogOnly -benchtime=1x -timeout 600s ./...
func BenchmarkRealData_LogOnly(b *testing.B) {
	dir := chunksDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		b.Skipf("chunks dir %s not found — download real data first", dir)
	}

	b.Log("Decoding real Loki chunks...")
	streams := decodeRealChunks(b, dir, defaultTenant, realDataMaxMB)
	if len(streams) == 0 {
		b.Skip("no streams decoded")
	}

	// Find the actual time range in the data
	var minTs, maxTs time.Time
	for _, s := range streams {
		for _, e := range s.Entries {
			if minTs.IsZero() || e.Timestamp.Before(minTs) {
				minTs = e.Timestamp
			}
			if e.Timestamp.After(maxTs) {
				maxTs = e.Timestamp
			}
		}
	}
	b.Logf("Data time range: %s → %s", minTs.Format(time.RFC3339), maxTs.Format(time.RFC3339))

	// Write to both stores
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	outDir := b.TempDir()

	b.Log("Writing to chunk store...")
	chunkStore, err := bench.NewChunkStore(outDir, defaultTenant)
	require.NoError(b, err)

	// Write streams in batches
	const batchSize = 100
	for i := 0; i < len(streams); i += batchSize {
		end := min(i+batchSize, len(streams))
		require.NoError(b, chunkStore.Write(context.Background(), streams[i:end]))
	}
	require.NoError(b, chunkStore.Close())

	b.Log("Writing to blockpack...")
	bpStore, err := NewBlockpackStore(outDir)
	require.NoError(b, err)
	for i := 0; i < len(streams); i += batchSize {
		end := min(i+batchSize, len(streams))
		require.NoError(b, bpStore.Write(context.Background(), streams[i:end]))
	}
	require.NoError(b, bpStore.Close())

	bpStat, _ := os.Stat(bpStore.Path())
	b.Logf("Blockpack file: %.1f MB", float64(bpStat.Size())/1024/1024)

	// Open stores for reading
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := bench.NewChunkStore(outDir, defaultTenant)
	require.NoError(b, err)

	bpReader, err := openBlockpackReader(bpStore.Path())
	require.NoError(b, err)
	b.Logf("Blockpack blocks: %d", bpReader.BlockCount())

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, NewLokiConverter(bpReader), logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), defaultTenant)

	// Use the last hour of data (densest period) as the query window.
	// The data spans years but is sparse — centering on the midpoint hits empty space.
	queryEnd := maxTs
	queryStart := queryEnd.Add(-time.Hour)
	b.Logf("Query window: %s → %s (last hour of data)", queryStart.Format(time.RFC3339), queryEnd.Format(time.RFC3339))

	// Define real-world queries based on the actual label names in the data
	queries := []struct {
		name  string
		query string
	}{
		{"all-logs", `{cluster=~".+"}`},
		{"cluster-filter", `{cluster="dev-us-central-0"}`},
		{"namespace-filter", `{cluster="dev-us-central-0", namespace=~"loki.*"}`},
		{"container-filter", `{cluster="dev-us-central-0", container="ingester"}`},
		{"line-filter", `{cluster="dev-us-central-0"} |= "error"`},
		{"regex-filter", `{cluster="dev-us-central-0"} |~ "(?i)timeout|deadline"`},
		{"level-filter", `{cluster="dev-us-central-0"} | detected_level="error"`},
		{"json-parse", `{cluster="dev-us-central-0"} | json`},
		{"negation", `{cluster="dev-us-central-0"} != "debug"`},
	}

	runQueries(b, ctx, queryStart, queryEnd, queries, chunkEngine, bpEngine)
}

// BenchmarkRealData_ObjectStorage is the same as BenchmarkRealData_LogOnly but with
// 50ms simulated I/O latency on BOTH chunk store and blockpack.
//
// The chunk store uses Loki's -storage-latency flag (injects latency on every
// ObjectClient Get/GetRange call). Blockpack uses WithIOLatency (injects latency
// on every GetBlockWithBytes call). Both model S3/GCS round-trip latency.
func BenchmarkRealData_ObjectStorage(b *testing.B) {
	const ioLatency = 50 * time.Millisecond

	dir := chunksDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		b.Skipf("chunks dir %s not found", dir)
	}

	streams := decodeRealChunks(b, dir, defaultTenant, realDataMaxMB)
	if len(streams) == 0 {
		b.Skip("no streams decoded")
	}

	var minTs, maxTs time.Time
	for _, s := range streams {
		for _, e := range s.Entries {
			if minTs.IsZero() || e.Timestamp.Before(minTs) {
				minTs = e.Timestamp
			}
			if e.Timestamp.After(maxTs) {
				maxTs = e.Timestamp
			}
		}
	}

	// Set storage latency flag BEFORE creating the chunk store.
	// This injects 50ms delay on every ObjectClient Get/GetRange call.
	require.NoError(b, flag.Set("storage-latency", ioLatency.String()))
	defer func() { _ = flag.Set("storage-latency", "0s") }()

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	outDir := b.TempDir()

	b.Log("Writing to chunk store (with latency flag)...")
	chunkStore, err := bench.NewChunkStore(outDir, defaultTenant)
	require.NoError(b, err)
	const batchSize = 100
	for i := 0; i < len(streams); i += batchSize {
		end := min(i+batchSize, len(streams))
		require.NoError(b, chunkStore.Write(context.Background(), streams[i:end]))
	}
	require.NoError(b, chunkStore.Close())

	b.Log("Writing to blockpack...")
	bpStore, err := NewBlockpackStore(outDir)
	require.NoError(b, err)
	for i := 0; i < len(streams); i += batchSize {
		end := min(i+batchSize, len(streams))
		require.NoError(b, bpStore.Write(context.Background(), streams[i:end]))
	}
	require.NoError(b, bpStore.Close())

	// Open for reading — both stores use latency injection.
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := newLatencyChunkStore(outDir, defaultTenant, ioLatency)
	require.NoError(b, err)

	bpReader, _, err := openBlockpackReaderWithLatency(bpStore.Path(), ioLatency)
	require.NoError(b, err)
	b.Logf("Blockpack blocks: %d, IO latency: %v, parallelism: 16", bpReader.BlockCount(), ioLatency)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpEngine := logql.NewEngine(logql.EngineOpts{},
		NewLokiConverter(bpReader).WithParallelism(16),
		logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), defaultTenant)

	queryEnd := maxTs
	queryStart := queryEnd.Add(-time.Hour)
	b.Logf("Query window: %s → %s (last hour of data)", queryStart.Format(time.RFC3339), queryEnd.Format(time.RFC3339))

	queries := []struct {
		name  string
		query string
	}{
		{"all-logs", `{cluster=~".+"}`},
		{"cluster-filter", `{cluster="dev-us-central-0"}`},
		{"namespace-filter", `{cluster="dev-us-central-0", namespace=~"loki.*"}`},
		{"container-filter", `{cluster="dev-us-central-0", container="ingester"}`},
		{"line-filter", `{cluster="dev-us-central-0"} |= "error"`},
		{"regex-filter", `{cluster="dev-us-central-0"} |~ "(?i)timeout|deadline"`},
		{"level-filter", `{cluster="dev-us-central-0"} | detected_level="error"`},
		{"json-parse", `{cluster="dev-us-central-0"} | json`},
		{"negation", `{cluster="dev-us-central-0"} != "debug"`},
	}

	runQueries(b, ctx, queryStart, queryEnd, queries, chunkEngine, bpEngine)
}

func runQueries(
	b *testing.B,
	ctx context.Context,
	queryStart, queryEnd time.Time,
	queries []struct{ name, query string },
	chunkEngine, bpEngine logql.Engine,
) {
	type storeSetup struct {
		name   string
		engine logql.Engine
	}
	stores := []storeSetup{
		{"chunk", chunkEngine},
		{"blockpack", bpEngine},
	}

	for _, q := range queries {
		for _, store := range stores {
			b.Run(fmt.Sprintf("query=%s/store=%s", q.name, store.name), func(b *testing.B) {
				params, err := logql.NewLiteralParams(
					q.query, queryStart, queryEnd, 0, 0, logproto.FORWARD, 1000, nil, nil,
				)
				require.NoError(b, err)

				eng := store.engine.Query(params)
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					bctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
					r, err := eng.Exec(bctx)
					cancel()
					if errors.Is(err, lokiengine.ErrNotSupported) {
						b.Skipf("not supported: %v", err)
					}
					require.NoError(b, err)
					b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "linesProcessed")
					b.ReportMetric(float64(r.Statistics.Summary.TotalBytesProcessed)/1024, "kbProcessed")
				}
			})
		}
	}
}

package lokibench

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// BenchmarkRealData_IOCost measures the actual I/O cost of each query by
// counting object storage operations (chunks downloaded / blocks read) and
// computing wall-clock time + simulated 50ms-per-IO cost.
//
// This is fair because:
//   - Chunks: we read Statistics.Store.TotalChunksDownloaded (actual chunk fetches)
//   - Blockpack: we count candidate blocks from selectBlocks
//   - Both are multiplied by the same 50ms latency
//   - No caching tricks — we count the real I/O operations each store performs
func BenchmarkRealData_IOCost(b *testing.B) {
	const ioLatencyMs = 50

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

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	outDir := b.TempDir()

	chunkStore, err := bench.NewChunkStore(outDir, defaultTenant)
	require.NoError(b, err)
	bpStore, err := NewBlockpackStore(outDir)
	require.NoError(b, err)

	const batchSize = 100
	for i := 0; i < len(streams); i += batchSize {
		end := min(i+batchSize, len(streams))
		require.NoError(b, chunkStore.Write(context.Background(), streams[i:end]))
		require.NoError(b, bpStore.Write(context.Background(), streams[i:end]))
	}
	require.NoError(b, chunkStore.Close())
	require.NoError(b, bpStore.Close())

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := bench.NewChunkStore(outDir, defaultTenant)
	require.NoError(b, err)
	bpReader, err := openBlockpackReader(bpStore.Path())
	require.NoError(b, err)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpQuerier := NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), defaultTenant)
	queryEnd := maxTs
	queryStart := queryEnd.Add(-time.Hour)

	b.Logf(
		"Blocks: %d, Query: %s → %s",
		bpReader.BlockCount(),
		queryStart.Format(time.RFC3339),
		queryEnd.Format(time.RFC3339),
	)

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

	for _, q := range queries {
		// --- Chunk store ---
		b.Run(fmt.Sprintf("query=%s/store=chunk", q.name), func(b *testing.B) {
			params, err := logql.NewLiteralParams(
				q.query, queryStart, queryEnd, 0, 0, logproto.FORWARD, 1000, nil, nil,
			)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cpuBefore := readCPUSecs()
				r, err := chunkEngine.Query(params).Exec(ctx)
				cpuAfter := readCPUSecs()
				require.NoError(b, err)

				chunksDownloaded := r.Statistics.Querier.Store.TotalChunksDownloaded
				cpuTime := cpuAfter.user - cpuBefore.user
				simulatedIO := float64(chunksDownloaded) * float64(ioLatencyMs)

				b.ReportMetric(float64(chunksDownloaded), "ios")
				b.ReportMetric(cpuTime*1000, "cpuMs")
				b.ReportMetric((cpuAfter.gc-cpuBefore.gc)*1000, "gcMs")
				b.ReportMetric(simulatedIO, "ioMs@50ms")
				b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "lines")
			}
		})

		// --- Blockpack ---
		b.Run(fmt.Sprintf("query=%s/store=blockpack", q.name), func(b *testing.B) {
			params, err := logql.NewLiteralParams(
				q.query, queryStart, queryEnd, 0, 0, logproto.FORWARD, 1000, nil, nil,
			)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cpuBefore := readCPUSecs()
				r, err := bpEngine.Query(params).Exec(ctx)
				cpuAfter := readCPUSecs()
				require.NoError(b, err)

				actualIOs := 0
				if s := bpQuerier.LastStats(); s != nil {
					actualIOs = s.SelectedBlocks
					if s.Explain != "" {
						b.Logf("explain: %s", s.Explain)
					}
				}
				cpuTime := cpuAfter.user - cpuBefore.user
				simulatedIO := float64(actualIOs) * float64(ioLatencyMs)

				b.ReportMetric(float64(actualIOs), "ios")
				b.ReportMetric(cpuTime*1000, "cpuMs")
				b.ReportMetric((cpuAfter.gc-cpuBefore.gc)*1000, "gcMs")
				b.ReportMetric(simulatedIO, "ioMs@50ms")
				b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "lines")
			}
		})
	}
}

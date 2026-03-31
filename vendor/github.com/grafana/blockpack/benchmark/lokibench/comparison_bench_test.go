// comparison_bench_test.go — Comprehensive chunk vs blockpack benchmark with on-disk report.
//
// Usage:
//
//	cd benchmark/lokibench
//	LOKIBENCH_CHUNKS_DIR=/tmp/loki-bench/chunks go test -run '^$' \
//	    -bench BenchmarkComparison -benchtime=1x -timeout 600s -count=1 ./...
//
// Writes a markdown report to /tmp/lokibench-report.md.
package lokibench

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
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

const reportPath = "/tmp/lokibench-report.md"

type queryDef struct {
	name      string
	query     string
	kind      string // "log", "metric", "sm" (structured metadata)
	step      time.Duration
	direction logproto.Direction
}

func allQueries() []queryDef {
	return []queryDef{
		// === Selectors: broad → narrow ===
		{"sel/all", `{cluster=~".+"}`, "log", 0, logproto.FORWARD},
		{"sel/cluster-eq", `{cluster="dev-us-central-0"}`, "log", 0, logproto.FORWARD},
		{"sel/cluster-regex", `{cluster=~"dev-.*"}`, "log", 0, logproto.FORWARD},
		{"sel/ns-eq", `{cluster="dev-us-central-0", namespace="mimir-dev-14"}`, "log", 0, logproto.FORWARD},
		{"sel/ns-regex", `{cluster="dev-us-central-0", namespace=~"mimir.*"}`, "log", 0, logproto.FORWARD},
		{"sel/container", `{cluster="dev-us-central-0", container="ingester"}`, "log", 0, logproto.FORWARD},
		{"sel/job", `{job="mimir-dev-14/cortex-gw-internal-zone-a"}`, "log", 0, logproto.FORWARD},
		{
			"sel/3-label",
			`{cluster="dev-us-central-0", namespace=~"mimir.*", container="ingester"}`,
			"log",
			0,
			logproto.FORWARD,
		},
		{
			"sel/stream-eq",
			`{cluster="dev-us-central-0", container="ingester", stream="stderr"}`,
			"log",
			0,
			logproto.FORWARD,
		},

		// === Line filters ===
		{"filter/contains", `{cluster="dev-us-central-0"} |= "error"`, "log", 0, logproto.FORWARD},
		{"filter/not-contains", `{cluster="dev-us-central-0"} != "debug"`, "log", 0, logproto.FORWARD},
		{"filter/regex", `{cluster="dev-us-central-0"} |~ "(?i)timeout|deadline|canceled"`, "log", 0, logproto.FORWARD},
		{"filter/not-regex", `{cluster="dev-us-central-0"} !~ "(?i)debug|trace"`, "log", 0, logproto.FORWARD},
		{"filter/multi", `{cluster="dev-us-central-0"} |= "level" != "debug" |~ "(?i)err"`, "log", 0, logproto.FORWARD},

		// === Structured metadata filters ===
		{"sm/level-error", `{cluster="dev-us-central-0"} | detected_level="error"`, "sm", 0, logproto.FORWARD},
		{"sm/level-warn", `{cluster="dev-us-central-0"} | detected_level="warn"`, "sm", 0, logproto.FORWARD},
		{"sm/level-regex", `{cluster="dev-us-central-0"} | detected_level=~"error|warn"`, "sm", 0, logproto.FORWARD},
		{"sm/level-not-debug", `{cluster="dev-us-central-0"} | detected_level!="debug"`, "sm", 0, logproto.FORWARD},
		{
			"sm/adaptive-sampled",
			`{cluster="dev-us-central-0"} | __adaptive_logs_sampled__=~".+"`,
			"sm",
			0,
			logproto.FORWARD,
		},

		// === Parsers ===
		{"parse/json", `{cluster="dev-us-central-0"} | json`, "log", 0, logproto.FORWARD},
		{"parse/logfmt", `{cluster="dev-us-central-0"} | logfmt`, "log", 0, logproto.FORWARD},
		{"parse/json+filter", `{cluster="dev-us-central-0"} | json | level="error"`, "log", 0, logproto.FORWARD},

		// === Direction ===
		{"dir/backward", `{cluster="dev-us-central-0"}`, "log", 0, logproto.BACKWARD},

		// === Metric queries: count/rate ===
		{"metric/count", `count_over_time({cluster="dev-us-central-0"}[1h])`, "metric", time.Minute, logproto.FORWARD},
		{"metric/rate", `rate({cluster="dev-us-central-0"}[1h])`, "metric", time.Minute, logproto.FORWARD},
		{
			"metric/rate-errors",
			`rate({cluster="dev-us-central-0"} |= "error"[1h])`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},

		// === Metric queries: aggregations ===
		{
			"metric/sum-by-ns",
			`sum by (namespace) (rate({cluster="dev-us-central-0"}[1h]))`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},
		{
			"metric/sum-by-container",
			`sum by (container) (rate({cluster="dev-us-central-0"}[1h]))`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},
		{
			"metric/count-by-level",
			`sum by (detected_level) (count_over_time({cluster="dev-us-central-0"} | detected_level=~".+"[1h]))`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},
		{
			"metric/topk-ns",
			`topk(5, sum by (namespace) (rate({cluster="dev-us-central-0"}[1h])))`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},

		// === Metric queries: narrow selectors ===
		{
			"metric/rate-narrow",
			`rate({cluster="dev-us-central-0", namespace=~"mimir.*"}[1h])`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},
		{
			"metric/count-narrow",
			`count_over_time({cluster="dev-us-central-0", container="ingester"}[1h])`,
			"metric",
			time.Minute,
			logproto.FORWARD,
		},
	}
}

type queryResult struct {
	name  string
	kind  string
	query string

	// Chunk store
	chunkWallNs int64
	chunkCPUMs  float64
	chunkGCMs   float64
	chunkIOs    int64
	chunkLines  int64
	chunkBytes  int64
	chunkAllocs int64
	chunkAllocB int64

	// Blockpack
	bpWallNs int64
	bpCPUMs  float64
	bpGCMs   float64
	bpIOs    int64
	bpLines  int64
	bpBytes  int64
	bpAllocs int64
	bpAllocB int64
}

func BenchmarkComparison(b *testing.B) {
	dir := chunksDir()
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		b.Skipf("chunks dir %s not found — set LOKIBENCH_CHUNKS_DIR", dir)
	}

	streams := decodeRealChunks(b, dir, defaultTenant, realDataMaxMB)
	if len(streams) == 0 {
		b.Skip("no streams decoded")
	}

	var minTs, maxTs time.Time
	var totalEntries int
	for _, s := range streams {
		totalEntries += len(s.Entries)
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
	chunkStoreRead, err := newLatencyChunkStore(outDir, defaultTenant, 0)
	require.NoError(b, err)
	bpReader, bpProvider, err := openBlockpackReaderWithLatency(bpStore.Path(), 0)
	require.NoError(b, err)
	bpStat, _ := os.Stat(bpStore.Path())

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpQuerier := NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), defaultTenant)
	queryEnd := maxTs
	queryStart := queryEnd.Add(-time.Hour)

	b.Logf("Dataset: %d streams, %d entries, %.1f MB blockpack, %d blocks",
		len(streams), totalEntries, float64(bpStat.Size())/1024/1024, bpReader.BlockCount())
	b.Logf("Query window: %s → %s", queryStart.Format(time.RFC3339), queryEnd.Format(time.RFC3339))

	queries := allQueries()
	var results []queryResult

	for _, q := range queries {
		var res queryResult
		res.name = q.name
		res.kind = q.kind
		res.query = q.query

		// --- Chunk ---
		b.Run(fmt.Sprintf("q=%s/s=chunk", q.name), func(b *testing.B) {
			params, err := logql.NewLiteralParams(
				q.query, queryStart, queryEnd, q.step, 0, q.direction, 1000, nil, nil,
			)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var m1 runtime.MemStats
				chunkStoreRead.ResetIO()
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				r, err := chunkEngine.Query(params).Exec(ctx)
				elapsed := time.Since(start)
				cpuAfter := readCPUSecs()

				var m2 runtime.MemStats
				runtime.ReadMemStats(&m2)

				require.NoError(b, err)
				res.chunkWallNs = elapsed.Nanoseconds()
				res.chunkCPUMs = ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
				res.chunkGCMs = (cpuAfter.gc - cpuBefore.gc) * 1000
				res.chunkIOs = chunkStoreRead.IOCount()
				res.chunkLines = r.Statistics.Summary.TotalPostFilterLines
				res.chunkBytes = int64(r.Statistics.Summary.TotalBytesProcessed)
				res.chunkAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.chunkAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

				b.ReportMetric(float64(res.chunkIOs), "ios")
				b.ReportMetric(res.chunkCPUMs, "cpuMs")
				b.ReportMetric(res.chunkGCMs, "gcMs")
				b.ReportMetric(float64(res.chunkLines), "lines")
			}
		})

		// --- Blockpack ---
		b.Run(fmt.Sprintf("q=%s/s=bp", q.name), func(b *testing.B) {
			params, err := logql.NewLiteralParams(
				q.query, queryStart, queryEnd, q.step, 0, q.direction, 1000, nil, nil,
			)
			require.NoError(b, err)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var m1 runtime.MemStats
				bpProvider.Reset()
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				r, err := bpEngine.Query(params).Exec(ctx)
				elapsed := time.Since(start)
				cpuAfter := readCPUSecs()

				var m2 runtime.MemStats
				runtime.ReadMemStats(&m2)

				require.NoError(b, err)
				if s := bpQuerier.LastStats(); s != nil {
					if s.Explain != "" {
						b.Logf("explain: %s", s.Explain)
					}
				}
				res.bpIOs = bpProvider.IOCount()
				res.bpWallNs = elapsed.Nanoseconds()
				res.bpCPUMs = ((cpuAfter.user + cpuAfter.gc) - (cpuBefore.user + cpuBefore.gc)) * 1000
				res.bpGCMs = (cpuAfter.gc - cpuBefore.gc) * 1000
				res.bpLines = r.Statistics.Summary.TotalPostFilterLines
				res.bpBytes = int64(r.Statistics.Summary.TotalBytesProcessed)
				res.bpAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.bpAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

				b.ReportMetric(float64(res.bpIOs), "ios")
				b.ReportMetric(res.bpCPUMs, "cpuMs")
				b.ReportMetric(res.bpGCMs, "gcMs")
				b.ReportMetric(float64(res.bpLines), "lines")
			}
		})

		results = append(results, res)
	}

	writeReport(reportPath, results, len(streams), totalEntries,
		float64(bpStat.Size())/1024/1024, bpReader.BlockCount(),
		queryStart, queryEnd)
	b.Logf("Report written to %s", reportPath)
}

func writeReport(path string, results []queryResult, nStreams, nEntries int,
	bpSizeMB float64, nBlocks int, queryStart, queryEnd time.Time,
) {
	var sb strings.Builder
	w := func(s string) { sb.WriteString(s) }

	w("# Blockpack vs Loki Chunks — LogQL Benchmark Report\n\n")
	w(fmt.Sprintf("**Generated:** %s  \n", time.Now().Format(time.RFC3339)))
	w(fmt.Sprintf("**Go:** %s %s/%s  \n\n", runtime.Version(), runtime.GOOS, runtime.GOARCH))

	// --- Dataset ---
	w("## Dataset\n\n")
	w("| Metric | Value |\n|---|---|\n")
	w(fmt.Sprintf("| Streams | %d |\n", nStreams))
	w(fmt.Sprintf("| Total entries | %d |\n", nEntries))
	w(fmt.Sprintf("| Blockpack file size | %.1f MB |\n", bpSizeMB))
	w(fmt.Sprintf("| Blockpack blocks | %d |\n", nBlocks))
	w(fmt.Sprintf("| Query window | `%s` → `%s` |\n", queryStart.Format(time.RFC3339), queryEnd.Format(time.RFC3339)))
	w(fmt.Sprintf("| IO latency model | 50 ms per storage operation |\n"))
	w(fmt.Sprintf("| Source | GCS tenant 29, dev-us-central-0-loki-dev-005-data |\n\n"))

	// Group results by kind
	kinds := []struct {
		title string
		kind  string
	}{
		{"Log Selector Queries", "log"},
		{"Structured Metadata Queries", "sm"},
		{"Metric Queries", "metric"},
	}

	for _, k := range kinds {
		var kResults []queryResult
		for _, r := range results {
			if r.kind == k.kind {
				kResults = append(kResults, r)
			}
		}
		if len(kResults) == 0 {
			continue
		}
		w(fmt.Sprintf("## %s\n\n", k.title))
		w(
			"| Name | Chunk IOs | Chunk CPU (ms) | Chunk Wall (ms) | Chunk Allocs | BP IOs | BP CPU (ms) | BP Wall (ms) | BP Allocs | Chunk @50ms | BP @50ms | Winner |\n",
		)
		w("|---|---|---|---|---|---|---|---|---|---|---|---|\n")

		for _, r := range kResults {
			chunkTotal := r.chunkCPUMs + float64(r.chunkIOs)*50
			bpTotal := r.bpCPUMs + float64(r.bpIOs)*50
			winner := "chunk"
			if bpTotal < chunkTotal*0.9 {
				winner = "**blockpack**"
			} else if bpTotal < chunkTotal*1.1 {
				winner = "~parity"
			}

			w(fmt.Sprintf("| %s | %d | %.2f | %.2f | %s | %d | %.2f | %.2f | %s | %.0f | %.0f | %s |\n",
				r.name,
				r.chunkIOs, r.chunkCPUMs, float64(r.chunkWallNs)/1e6, fmtAllocs(r.chunkAllocs, r.chunkAllocB),
				r.bpIOs, r.bpCPUMs, float64(r.bpWallNs)/1e6, fmtAllocs(r.bpAllocs, r.bpAllocB),
				chunkTotal, bpTotal, winner))
		}
		w("\n")
	}

	// --- Summary ---
	w("## Summary\n\n")
	var bpWins, chunkWins, parity int
	for _, r := range results {
		chunkTotal := r.chunkCPUMs + float64(r.chunkIOs)*50
		bpTotal := r.bpCPUMs + float64(r.bpIOs)*50
		if bpTotal < chunkTotal*0.9 {
			bpWins++
		} else if bpTotal < chunkTotal*1.1 {
			parity++
		} else {
			chunkWins++
		}
	}
	w(fmt.Sprintf("| Result | Count |\n|---|---|\n"))
	w(fmt.Sprintf("| Blockpack wins | %d |\n", bpWins))
	w(fmt.Sprintf("| Chunk wins | %d |\n", chunkWins))
	w(fmt.Sprintf("| Parity (±10%%) | %d |\n", parity))
	w(fmt.Sprintf("| Total queries | %d |\n\n", len(results)))

	// --- IO breakdown ---
	w("## I/O Breakdown (sorted by blockpack IOs descending)\n\n")
	w("| Name | Kind | Chunk IOs | BP IOs | Chunk Lines | BP Lines | Chunk Bytes | BP Bytes |\n")
	w("|---|---|---|---|---|---|---|---|\n")
	sorted := make([]queryResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].bpIOs != sorted[j].bpIOs {
			return sorted[i].bpIOs > sorted[j].bpIOs
		}
		return sorted[i].name < sorted[j].name
	})
	for _, r := range sorted {
		w(fmt.Sprintf("| %s | %s | %d | %d | %d | %d | %s | %s |\n",
			r.name, r.kind, r.chunkIOs, r.bpIOs, r.chunkLines, r.bpLines,
			fmtBytes(r.chunkBytes), fmtBytes(r.bpBytes)))
	}

	w("\n---\n*Report generated by `BenchmarkComparison` in `benchmark/lokibench/comparison_bench_test.go`*\n")

	os.WriteFile(path, []byte(sb.String()), 0o644) //nolint:gosec,errcheck
}

func fmtAllocs(n, b int64) string {
	if b > 1024*1024 {
		return fmt.Sprintf("%dk / %.1f MB", n/1000, float64(b)/1024/1024)
	}
	return fmt.Sprintf("%dk / %.0f KB", n/1000, float64(b)/1024)
}

func fmtBytes(b int64) string {
	if b == 0 {
		return "0"
	}
	if b > 1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(b)/1024/1024)
	}
	return fmt.Sprintf("%.1f KB", float64(b)/1024)
}

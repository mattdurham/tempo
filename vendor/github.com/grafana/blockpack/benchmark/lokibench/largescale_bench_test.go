// largescale_bench_test.go — Log benchmarks at varying dataset scales.
//
// BenchmarkLargeScale: Single large dataset (default 1 GB, 200 streams, 24h).
// BenchmarkLogsFull:   Combined report — runs 50 MB synthetic + 1 GB large-scale
//
//	datasets and writes a single HTML comparison report.
//
// Data is persisted to testdata/largescale/ (configurable via LARGESCALE_DIR). On
// subsequent runs the existing data is reused — delete the directory to regenerate.
//
// # Usage
//
//	cd benchmark/lokibench
//	go test -run '^$' -bench BenchmarkLargeScale -benchtime=1x -timeout 3600s -count=1 -v ./...
//	go test -run '^$' -bench BenchmarkLogsFull   -benchtime=1x -timeout 3600s -count=1 -v ./...
//
// # Regenerate data
//
//	rm -rf testdata/largescale && go test -run '^$' -bench BenchmarkLargeScale ...
//
// # Tuning via environment variables
//
//	LARGESCALE_DIR=testdata/largescale  # data directory (default: testdata/largescale)
//	LARGESCALE_DATA_MB=512              # dataset size in MB (default: 1024)
//	LARGESCALE_STREAMS=200              # number of log streams (default: 200)
//	LARGESCALE_WINDOW=24h               # time window (default: 24h)
//	LARGESCALE_IO_LATENCY=20ms          # per-IO latency simulation (default: 20ms)
//	LARGESCALE_REPORT=path.html         # HTML report output path
package lokibench

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/blockpack/internal/logqlparser"
	modules_blockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	modules_rw "github.com/grafana/blockpack/internal/modules/rw"
)

const (
	largescaleReportEnv     = "LARGESCALE_REPORT"
	largescaleReportDefault = "../lokibench-largescale.html"

	largescaleDefaultDir     = "testdata/largescale"
	largescaleDefaultDataMB  = 1024
	largescaleDefaultStreams = 200
	largescaleDefaultWindow  = 24 * time.Hour
	largescaleDefaultLatency = 20 * time.Millisecond
)

// largescaleConfig holds benchmark parameters for a single dataset.
type largescaleConfig struct {
	name      string // display name for the report (e.g., "50 MB Synthetic")
	dir       string
	dataMB    int
	streams   int
	window    time.Duration
	ioLatency time.Duration
}

func loadLargescaleConfig() largescaleConfig {
	cfg := largescaleConfig{
		name:      "Large-Scale",
		dir:       largescaleDefaultDir,
		dataMB:    largescaleDefaultDataMB,
		streams:   largescaleDefaultStreams,
		window:    largescaleDefaultWindow,
		ioLatency: largescaleDefaultLatency,
	}

	if v := os.Getenv("LARGESCALE_DIR"); v != "" {
		cfg.dir = v
	}
	if v := os.Getenv("LARGESCALE_DATA_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.dataMB = n
		}
	}
	if v := os.Getenv("LARGESCALE_STREAMS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.streams = n
		}
	}
	if v := os.Getenv("LARGESCALE_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.window = d
		}
	}
	if v := os.Getenv("LARGESCALE_IO_LATENCY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.ioLatency = d
		}
	}
	return cfg
}

// datasetResult holds the results from benchmarking a single dataset.
type datasetResult struct {
	cfg           largescaleConfig
	results       []synthResult
	bpSizeMB      float64
	chunkSizeMB   float64
	nBlocks       int
	bpTotalLogs   int64
	lokiTotalLogs int64
	genDuration   time.Duration
}

// BenchmarkLargeScale generates a large synthetic dataset and benchmarks query
// performance across both Loki chunks and blockpack at production-like scale.
//
// Default: 1 GB of logs, 200 streams, 24-hour window, 20ms IO latency.
// Tunable via LARGESCALE_* env vars.
func BenchmarkLargeScale(b *testing.B) {
	cfg := loadLargescaleConfig()
	ds := runDatasetBenchmark(b, cfg)

	reportPath := os.Getenv(largescaleReportEnv)
	if reportPath == "" {
		reportPath = largescaleReportDefault
	}
	writeCombinedHTMLReport(reportPath, []datasetResult{ds})
	b.Logf("HTML report written to %s", reportPath)
}

// BenchmarkLogsFull is the unified log benchmark — see logfilter_bench_test.go.

// modulesProvider implements modules_rw.ReaderProvider over an in-memory byte slice
// for use with the native blockpack engine (modules_blockio.NewReaderFromProvider).
type modulesProvider struct {
	data    []byte
	latency time.Duration
}

func (p *modulesProvider) Size() (int64, error) { return int64(len(p.data)), nil }

func (p *modulesProvider) ReadAt(buf []byte, off int64, _ modules_rw.DataType) (int, error) {
	if p.latency > 0 {
		time.Sleep(p.latency)
	}
	if off < 0 || off >= int64(len(p.data)) {
		return 0, io.EOF
	}
	n := copy(buf, p.data[off:])
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

// runDatasetBenchmark generates data (or reuses existing), opens stores with latency,
// runs all query test cases, and returns the collected results.
func runDatasetBenchmark(b *testing.B, cfg largescaleConfig) datasetResult {
	b.Helper()

	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	dir := cfg.dir
	require.NoError(b, os.MkdirAll(dir, 0o755))

	bpPath := filepath.Join(dir, "blockpack.logs")

	var genDuration time.Duration

	if _, err := os.Stat(bpPath); err == nil {
		b.Logf("[%s] Reusing existing data at %s (delete to regenerate)", cfg.name, dir)
	} else {
		chunkStore, err := bench.NewChunkStore(dir, testTenant)
		require.NoError(b, err)

		bpStore, err := NewBlockpackStore(dir)
		require.NoError(b, err)

		targetBytes := int64(cfg.dataMB) * 1024 * 1024

		opt := bench.DefaultOpt().
			WithNumStreams(cfg.streams).
			WithTimeSpread(cfg.window)

		b.Logf("[%s] Generating %d MB synthetic log data (%d streams, %s window)...",
			cfg.name, cfg.dataMB, cfg.streams, cfg.window)

		genStart := time.Now()
		builder := bench.NewBuilder(dir, opt, chunkStore, bpStore)
		require.NoError(b, builder.Generate(context.Background(), targetBytes))
		genDuration = time.Since(genStart)
		b.Logf("[%s] Data generation took %s", cfg.name, genDuration.Round(time.Second))
	}

	// --- Read phase ---
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := newLatencyChunkStore(dir, testTenant, cfg.ioLatency)
	require.NoError(b, err)

	bpReader, bpProvider, err := openBlockpackReaderWithLatency(bpPath, cfg.ioLatency)
	require.NoError(b, err)

	bpStat, err := os.Stat(bpPath)
	require.NoError(b, err)

	var totalDirBytes int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, walkErr error) error {
		if walkErr == nil && !info.IsDir() {
			totalDirBytes += info.Size()
		}
		return walkErr
	})
	chunkSizeMB := float64(totalDirBytes-bpStat.Size()) / 1024 / 1024

	config, err := bench.LoadConfig(dir)
	require.NoError(b, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()
	require.NotEmpty(b, cases, "test case generator produced no cases")

	b.Logf("[%s] Dataset: %d streams, blockpack %.1f MB (%d blocks), chunks %.1f MB, %d query cases, IO latency %s",
		cfg.name, cfg.streams, float64(bpStat.Size())/1024/1024, bpReader.BlockCount(),
		chunkSizeMB, len(cases), cfg.ioLatency)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)

	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpQuerier := NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	// nativeData is loaded once; a fresh Reader is created per
	// scenario so each native-engine sub-benchmark starts with a cold cache.
	nativeData, err := os.ReadFile(bpPath)
	require.NoError(b, err)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	var results []synthResult

	for _, tc := range cases {
		res := synthResult{
			name:  tc.Name(),
			kind:  tc.Kind(),
			query: tc.Query,
		}

		params, pErr := logql.NewLiteralParams(
			tc.Query, tc.Start, tc.End, tc.Step, 0, tc.Direction, 1000, nil, nil,
		)
		require.NoError(b, pErr)

		// --- Chunk store ---
		require.NoError(b, chunkStoreRead.ClearCache())
		chunkStoreRead.ResetIO()
		b.Run(fmt.Sprintf("q=%s/kind=%s/store=chunk", tc.Name(), tc.Kind()), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var m1, m2 runtime.MemStats
				chunkStoreRead.ResetIO()
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				r, runErr := chunkEngine.Query(params).Exec(ctx)
				elapsed := time.Since(start)
				cpuAfter := readCPUSecs()
				runtime.ReadMemStats(&m2)

				require.NoError(b, runErr)
				if res.chunkData == nil {
					res.chunkData = r.Data
				}

				res.chunkWallMs = float64(elapsed.Nanoseconds()) / 1e6
				res.chunkCPUMs = (cpuAfter.user - cpuBefore.user + cpuAfter.gc - cpuBefore.gc) * 1000
				res.chunkIOs = chunkStoreRead.IOCount()
				res.chunkLines = r.Statistics.Summary.TotalPostFilterLines
				res.chunkBytes = int64(r.Statistics.Summary.TotalBytesProcessed)
				res.chunkAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.chunkAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

				b.ReportMetric(float64(res.chunkIOs), "ios")
				b.ReportMetric(res.chunkCPUMs, "cpuMs")
				b.ReportMetric(float64(res.chunkLines), "linesProcessed")
			}
		})

		// --- Blockpack ---
		b.Run(fmt.Sprintf("q=%s/kind=%s/store=blockpack", tc.Name(), tc.Kind()), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				var m1, m2 runtime.MemStats
				bpProvider.Reset()
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				bpQuerier.ResetLines()
				r, runErr := bpEngine.Query(params).Exec(ctx)
				elapsed := time.Since(start)
				cpuAfter := readCPUSecs()
				runtime.ReadMemStats(&m2)

				require.NoError(b, runErr)
				if res.bpData == nil {
					res.bpData = r.Data
				}

				if s := bpQuerier.LastStats(); s != nil {
					res.bpExplain = s.Explain
					if s.Explain != "" {
						b.Logf("explain: %s", s.Explain)
					}
				}

				res.bpWallMs = float64(elapsed.Nanoseconds()) / 1e6
				res.bpCPUMs = (cpuAfter.user - cpuBefore.user + cpuAfter.gc - cpuBefore.gc) * 1000
				res.bpIOs = bpProvider.IOCount()
				res.bpLines = bpQuerier.LinesProcessed()
				res.bpBytes = int64(r.Statistics.Summary.TotalBytesProcessed)
				res.bpAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.bpAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

				b.ReportMetric(float64(res.bpIOs), "ios")
				b.ReportMetric(res.bpCPUMs, "cpuMs")
				b.ReportMetric(float64(res.bpLines), "linesProcessed")
			}
		})

		// --- Verification: compare captured results from both stores ---
		verifyFromCaptured(&res, b)

		// --- Native blockpack engine (log queries only) ---
		if tc.Kind() == "log" {
			sel, parseErr := logqlparser.Parse(tc.Query)
			if parseErr == nil {
				program, pipeline, compileErr := logqlparser.CompileAll(sel)
				if compileErr == nil {
					nativeReader, readerErr := modules_blockio.NewReaderFromProvider(
						&modulesProvider{data: nativeData, latency: cfg.ioLatency},
					)
					require.NoError(b, readerErr)

					b.Run(fmt.Sprintf("q=%s/kind=%s/store=native", tc.Name(), tc.Kind()), func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()

						for b.Loop() {
							var m1, m2 runtime.MemStats
							runtime.ReadMemStats(&m1)
							cpuBefore := readCPUSecs()
							start := time.Now()

							var blockIOs int
							nativeEntries, scanErr := modules_executor.CollectLogs(
								nativeReader,
								program,
								pipeline,
								modules_executor.CollectOptions{
									TimestampColumn: "log:timestamp",
								},
							)
							nativeLines := int64(len(nativeEntries))
							_ = blockIOs
							elapsed := time.Since(start)
							cpuAfter := readCPUSecs()
							runtime.ReadMemStats(&m2)

							if scanErr != nil {
								b.Logf("native CollectLogs error: %v", scanErr)
							}

							res.nativeWallMs = float64(elapsed.Nanoseconds()) / 1e6
							res.nativeCPUMs = (cpuAfter.user - cpuBefore.user + cpuAfter.gc - cpuBefore.gc) * 1000
							res.nativeIOs = blockIOs
							res.nativeLines = nativeLines
							res.nativeBytes = 0 // not tracked by native engine
							res.nativeAllocs = int64(m2.Mallocs - m1.Mallocs)
							res.nativeAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

							b.ReportMetric(float64(blockIOs), "ios")
							b.ReportMetric(res.nativeCPUMs, "cpuMs")
							b.ReportMetric(float64(nativeLines), "linesProcessed")
						}
					})
				}
			}
		}

		results = append(results, res)
	}

	var bpTotalLogs int64
	for i := range bpReader.BlockCount() {
		bpTotalLogs += int64(bpReader.BlockMeta(i).SpanCount)
	}
	var lokiTotalLogs int64
	if raw, readErr := os.ReadFile(filepath.Join(dir, "loki_count.txt")); readErr == nil {
		lokiTotalLogs, _ = strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	}

	return datasetResult{
		cfg:           cfg,
		results:       results,
		bpSizeMB:      float64(bpStat.Size()) / 1024 / 1024,
		chunkSizeMB:   chunkSizeMB,
		nBlocks:       bpReader.BlockCount(),
		bpTotalLogs:   bpTotalLogs,
		lokiTotalLogs: lokiTotalLogs,
		genDuration:   genDuration,
	}
}

// ---------------------------------------------------------------------------
// Combined HTML report
// ---------------------------------------------------------------------------

func writeCombinedHTMLReport(path string, datasets []datasetResult) {
	var sb strings.Builder
	w := func(s string) { _, _ = sb.WriteString(s) }

	// Determine IO latency from first dataset.
	ioLatency := 20 * time.Millisecond
	if len(datasets) > 0 {
		ioLatency = datasets[0].cfg.ioLatency
	}

	w(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Log Benchmark — Blockpack vs Loki Chunks</title>
<style>
	body {
		font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
		max-width: 1800px;
		margin: 40px auto;
		padding: 0 20px;
		background: #f5f5f5;
	}
	h1 { color: #333; text-align: center; margin-bottom: 20px; }
	.metadata { text-align: center; color: #666; margin-bottom: 30px; font-size: 14px; }
	.summary {
		background: white;
		padding: 20px;
		margin-bottom: 30px;
		border-radius: 4px;
		box-shadow: 0 2px 4px rgba(0,0,0,0.1);
	}
	.toc {
		background: white;
		padding: 20px;
		margin-bottom: 30px;
		border-radius: 4px;
		box-shadow: 0 2px 4px rgba(0,0,0,0.1);
	}
	.toc h2 { margin-top: 0; color: #333; font-size: 18px; }
	.toc ul { list-style: none; padding: 0; margin: 0; }
	.toc li { margin: 10px 0; padding-left: 20px; }
	.toc a { color: #2c3e50; text-decoration: none; font-weight: 500; }
	.toc a:hover { color: #3498db; text-decoration: underline; }
	.summary h2 { margin-top: 0; color: #333; font-size: 18px; }
	.summary-stat { margin: 12px 0; font-size: 15px; }
	.summary-stat strong { color: #2c3e50; }
	.dataset-header {
		background: #1a252f;
		color: white;
		padding: 25px;
		margin: 60px -20px 20px -20px;
		border-radius: 4px;
		text-align: center;
		font-size: 1.8rem;
		font-weight: 700;
		width: calc(100% + 40px);
		border-left: 6px solid #3498db;
	}
	.section-header {
		background: #2c3e50;
		color: white;
		padding: 20px;
		margin: 50px -20px 20px -20px;
		border-radius: 4px;
		text-align: center;
		font-size: 1.5rem;
		font-weight: 600;
		width: calc(100% + 40px);
	}
	button.section-header { border: none; font-family: inherit; display: block; }
	.section-header.collapsible { cursor: pointer; user-select: none; }
	.section-header.collapsible:hover { background: #34495e; }
	.chevron { font-size: 0.8em; display: inline-block; transition: transform 0.2s; }
	.collapsible-content { display: block; }
	table {
		width: 100%;
		border-collapse: collapse;
		background: white;
		box-shadow: 0 2px 4px rgba(0,0,0,0.1);
		margin-bottom: 30px;
	}
	th {
		background: #2c3e50;
		color: white;
		padding: 12px;
		text-align: left;
		font-weight: 600;
		font-size: 14px;
	}
	th.numeric { text-align: right; }
	th.blockpack-col { background: #27ae60; }
	th.chunk-col { background: #e67e22; }
	td { padding: 12px; border-bottom: 1px solid #eee; }
	td.numeric {
		text-align: right;
		font-family: 'Monaco', 'Courier New', monospace;
		font-size: 13px;
	}
	tr:hover { background: #f9f9f9; }
	.query-name { font-weight: 600; font-size: 15px; }
	.metric { font-family: 'Monaco', 'Courier New', monospace; font-size: 13px; }
	.winner-blockpack { border-bottom: 2px solid #27ae60; font-weight: 600; }
	.winner-chunk { border-bottom: 2px solid #e67e22; font-weight: 600; }
	.winner-par { color: #7f8c8d; }
	.speedup-blockpack { color: #27ae60; font-weight: 600; }
	.speedup-chunk { color: #e67e22; font-weight: 600; }
	footer { margin-top: 40px; color: #999; font-size: 0.82rem; text-align: center; }
</style>
<script>
	function toggleSection(header) {
		const content = header.nextElementSibling;
		const chevron = header.querySelector('.chevron');

		const isHidden = content.style.display === 'none';
		if (isHidden) {
			content.style.display = 'block';
			content.setAttribute('aria-hidden', 'false');
			chevron.textContent = '\u25BC';
			header.setAttribute('aria-expanded', 'true');
		} else {
			content.style.display = 'none';
			content.setAttribute('aria-hidden', 'true');
			chevron.textContent = '\u25B6';
			header.setAttribute('aria-expanded', 'false');
		}
	}

	function initCollapsibleSections() {
		// Find elements that invoke toggleSection via an inline onclick handler
		const headers = document.querySelectorAll('[onclick^="toggleSection"]');
		headers.forEach((header) => {
			// Make header behave like a button for assistive technologies
			header.setAttribute('role', 'button');
			if (!header.hasAttribute('tabindex')) {
				header.setAttribute('tabindex', '0');
			}

			const content = header.nextElementSibling;
			const isHidden = content && content.style.display === 'none';
			header.setAttribute('aria-expanded', isHidden ? 'false' : 'true');
			if (content) {
				content.setAttribute('aria-hidden', isHidden ? 'true' : 'false');
			}

			header.addEventListener('keydown', function (event) {
				const key = event.key || event.code;
				if (key === 'Enter' || key === ' ' || key === 'Spacebar') {
					event.preventDefault();
					toggleSection(header);
				}
			});
		});
	}

	if (document.readyState === 'loading') {
		document.addEventListener('DOMContentLoaded', initCollapsibleSections);
	} else {
		initCollapsibleSections();
	}
</script>
</head>
<body>
`)

	title := "Log Benchmark — Blockpack vs Loki Chunks"
	if len(datasets) == 1 {
		title = fmt.Sprintf("%s Benchmark — Blockpack vs Loki Chunks", datasets[0].cfg.name)
	}
	w(fmt.Sprintf(`<h1>%s</h1>
<div class="metadata">Generated %s &middot; Go %s &middot; %s/%s</div>
`,
		title,
		time.Now().Format("2006-01-02 15:04:05 MST"),
		runtime.Version(), runtime.GOOS, runtime.GOARCH))

	w(
		fmt.Sprintf(
			`<div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin-bottom: 20px; border-radius: 4px;">
	<h3 style="margin-top: 0; color: #856404;">IO Latency Simulation</h3>
	<p style="margin: 0; color: #856404;"><strong>All benchmarks include %s injected latency per I/O operation</strong>
	to simulate S3/GCS object storage round-trip latency.
	Blockpack: one sleep per ReadAt. Loki chunks: one sleep per ObjectClient read.
	Total wall time = processing time + (I/O count x %s).</p>
</div>
`,
			ioLatency,
			ioLatency,
		),
	)
	w(htmlWallCPUDisclaimer)

	// Table of contents
	w(`<div class="toc"><h2>📑 Table of Contents</h2><ul>`)
	for i, ds := range datasets {
		slug := fmt.Sprintf("ds%d", i)
		var hasFilter, hasLog, hasMetric bool
		for _, r := range ds.results {
			switch r.kind {
			case "filter":
				hasFilter = true
			case "log":
				hasLog = true
			case "metric":
				hasMetric = true
			}
		}
		w(fmt.Sprintf(`<li><strong><a href="#%s">%s</a></strong> — %d MB, %d streams, %s window<ul>`,
			slug, ds.cfg.name, ds.cfg.dataMB, ds.cfg.streams, ds.cfg.window))
		if hasFilter {
			w(fmt.Sprintf(`<li><a href="#%s-filter">Filter Queries</a></li>`, slug))
		}
		if hasLog {
			w(fmt.Sprintf(`<li><a href="#%s-log">Log Queries</a></li>`, slug))
		}
		if hasMetric {
			w(fmt.Sprintf(`<li><a href="#%s-metric">Metric Queries</a></li>`, slug))
		}
		w(`</ul></li>`)
	}
	w(`<li><a href="#queries-reference">📝 Query Reference</a></li>`)
	w(`</ul></div>`)

	// Per-dataset sections
	for i, ds := range datasets {
		slug := fmt.Sprintf("ds%d", i)
		writeDatasetSection(w, slug, ds)
	}

	// Queries reference section (collapsible, at the bottom)
	writeQueriesSection(w, datasets)

	// Footer
	w(`<footer>
  Report generated by <code>benchmark/lokibench/largescale_bench_test.go</code>.
</footer>
</body>
</html>
`)

	_ = os.WriteFile(path, []byte(sb.String()), 0o644) //nolint:gosec
}

const queryTableHeader = `<table class="comparison-table">
<thead>
	<tr>
		<th rowspan="2">Query</th>
		<th rowspan="2" style="text-align: center; width: 50px;">Verify</th>
		<th colspan="7" class="blockpack-col" style="text-align: center;">Blockpack (bridge)</th>
		<th colspan="5" style="background:#1a7a3e;text-align:center;color:white;">Native Engine</th>
		<th colspan="7" class="chunk-col" style="text-align: center;">Loki Chunks</th>
	</tr>
	<tr>
		<th class="numeric blockpack-col">IOs</th>
		<th class="numeric blockpack-col">CPU (ms)</th>
		<th class="numeric blockpack-col">Wall (ms)</th>
		<th class="numeric blockpack-col" title="Wall+CPU = wall-clock time + CPU time; lower is better; used for wins">Wall+CPU (ms)</th>
		<th class="numeric blockpack-col">Lines Returned</th>
		<th class="numeric blockpack-col">Allocs</th>
		<th class="numeric blockpack-col">Memory</th>
		<th class="numeric" style="background:#1a7a3e;color:white;">Wall (ms)</th>
		<th class="numeric" style="background:#1a7a3e;color:white;">Lines</th>
		<th class="numeric" style="background:#1a7a3e;color:white;">Allocs</th>
		<th class="numeric" style="background:#1a7a3e;color:white;">Memory</th>
		<th class="numeric" style="background:#1a7a3e;color:white;">Note</th>
		<th class="numeric chunk-col">IOs</th>
		<th class="numeric chunk-col">CPU (ms)</th>
		<th class="numeric chunk-col">Wall (ms)</th>
		<th class="numeric chunk-col" title="Wall+CPU = wall-clock time + CPU time; lower is better; used for wins">Wall+CPU (ms)</th>
		<th class="numeric chunk-col">Lines Returned</th>
		<th class="numeric chunk-col">Allocs</th>
		<th class="numeric chunk-col">Memory</th>
	</tr>
</thead>
<tbody>
`

func writeDatasetSection(w func(string), slug string, ds datasetResult) {
	cfg := ds.cfg
	results := ds.results

	var bpWins, chunkWins, parity int
	for _, r := range results {
		bpScore := r.bpWallMs + r.bpCPUMs
		chScore := r.chunkWallMs + r.chunkCPUMs
		switch {
		case bpScore < chScore*0.9:
			bpWins++
		case bpScore < chScore*1.1:
			parity++
		default:
			chunkWins++
		}
	}

	w(fmt.Sprintf(`<div class="dataset-header" id="%s">%s</div>`, slug, cfg.name))

	w(`<div class="summary"><h2>Dataset &amp; Results</h2>`)
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Raw data target:</strong> %d MB</div>`, cfg.dataMB))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Streams:</strong> %d</div>`, cfg.streams))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Time window:</strong> %s</div>`, cfg.window))
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Blockpack on-disk:</strong> %.1f MB (%d blocks, avg %.1f KB/block)</div>`,
		ds.bpSizeMB, ds.nBlocks, ds.bpSizeMB*1024/float64(max(ds.nBlocks, 1))))
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Chunk store on-disk:</strong> %.1f MB</div>`, ds.chunkSizeMB))
	if ds.bpTotalLogs > 0 || ds.lokiTotalLogs > 0 {
		countNote := ""
		if ds.lokiTotalLogs > 0 && ds.lokiTotalLogs != ds.bpTotalLogs {
			countNote = ` <span style="color:#e74c3c; font-weight:bold;">⚠ COUNT MISMATCH</span>`
		}
		lokiStr := "—"
		if ds.lokiTotalLogs > 0 {
			lokiStr = fmt.Sprintf("%d", ds.lokiTotalLogs)
		}
		w(fmt.Sprintf(
			`<div class="summary-stat"><strong>Total log entries:</strong> blockpack %d, chunks %s%s</div>`,
			ds.bpTotalLogs, lokiStr, countNote))
	}
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Compression:</strong> blockpack %.1fx, chunks %.1fx</div>`,
		float64(cfg.dataMB)/max(ds.bpSizeMB, 0.01), float64(cfg.dataMB)/max(ds.chunkSizeMB, 0.01)))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Queries run:</strong> %d</div>`, len(results)))
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Blockpack wins:</strong> <span class="speedup-blockpack">%d</span></div>`,
		bpWins))
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Chunk wins:</strong> <span class="speedup-chunk">%d</span></div>`,
		chunkWins))
	w(fmt.Sprintf(
		`<div class="summary-stat"><strong>Parity (&plusmn;10%%):</strong> <span class="winner-par">%d</span></div>`,
		parity))
	if ds.genDuration > 0 {
		w(fmt.Sprintf(
			`<div class="summary-stat"><strong>Data generation:</strong> %s</div>`,
			ds.genDuration.Round(time.Second)))
	}
	writeVerifySummaryHTML(w, results)
	w("</div>\n")

	writeFilterSectionHTML(w, slug+"-filter", cfg.name+" — Filter Queries", results)
	writeVerifyMismatchSection(w, slug+"-filter-verify", filterResults(results))
	writeQuerySectionHTML(w, slug+"-log", cfg.name+" — Log Queries", "log", results)
	writeQuerySectionHTML(w, slug+"-metric", cfg.name+" — Metric Queries", "metric", results)
	writeVerifyMismatchSection(w, slug+"-verify", results)
}

func writeQuerySectionHTML(w func(string), id, title, kind string, results []synthResult) {
	var rows []synthResult
	for _, r := range results {
		if r.kind == kind {
			rows = append(rows, r)
		}
	}
	if len(rows) == 0 {
		return
	}
	w(fmt.Sprintf(
		"<button type=\"button\" class=\"section-header collapsible\" id=\"%s\""+
			" onclick=\"toggleSection(this)\" aria-expanded=\"true\" aria-controls=\"%s-content\">\n"+
			"\t<span class=\"chevron\">\u25BC</span> %s\n"+
			"</button>\n"+
			"<div class=\"collapsible-content\" id=\"%s-content\" aria-hidden=\"false\">\n",
		id, id, title, id))
	w(queryTableHeader)

	cellWin := func(bpVal, chunkVal float64) (string, string) {
		const threshold = 0.95
		if bpVal < chunkVal*threshold {
			return "winner-blockpack", ""
		}
		if chunkVal < bpVal*threshold {
			return "", "winner-chunk"
		}
		return "", ""
	}

	for _, r := range rows {
		bpIOClass, chIOClass := cellWin(float64(r.bpIOs), float64(r.chunkIOs))
		bpCPUClass, chCPUClass := cellWin(r.bpCPUMs, r.chunkCPUMs)
		bpWallClass, chWallClass := cellWin(r.bpWallMs, r.chunkWallMs)
		bpAllocsClass, chAllocsClass := cellWin(float64(r.bpAllocs), float64(r.chunkAllocs))
		bpMemClass, chMemClass := cellWin(float64(r.bpAllocB), float64(r.chunkAllocB))

		fmtSpeedup := func(val string, bpV, chV float64, bpWin bool) string {
			if bpWin {
				return synthFmtSpeedup(val, synthSafeSpeedup(chV, bpV))
			}
			return synthFmtSpeedup(val, synthSafeSpeedup(bpV, chV))
		}

		bpIOsStr := fmt.Sprintf("%d", r.bpIOs)
		chIOsStr := fmt.Sprintf("%d", r.chunkIOs)
		if bpIOClass != "" {
			bpIOsStr = fmtSpeedup(bpIOsStr, float64(r.bpIOs), float64(r.chunkIOs), true)
		} else if chIOClass != "" {
			chIOsStr = fmtSpeedup(chIOsStr, float64(r.bpIOs), float64(r.chunkIOs), false)
		}

		bpCPUStr := fmt.Sprintf("%.0f", r.bpCPUMs)
		chCPUStr := fmt.Sprintf("%.0f", r.chunkCPUMs)
		if bpCPUClass != "" {
			bpCPUStr = fmtSpeedup(bpCPUStr, r.bpCPUMs, r.chunkCPUMs, true)
		} else if chCPUClass != "" {
			chCPUStr = fmtSpeedup(chCPUStr, r.bpCPUMs, r.chunkCPUMs, false)
		}

		bpWallStr := fmt.Sprintf("%.0f", r.bpWallMs)
		chWallStr := fmt.Sprintf("%.0f", r.chunkWallMs)
		if bpWallClass != "" {
			bpWallStr = fmtSpeedup(bpWallStr, r.bpWallMs, r.chunkWallMs, true)
		} else if chWallClass != "" {
			chWallStr = fmtSpeedup(chWallStr, r.bpWallMs, r.chunkWallMs, false)
		}

		bpScore := r.bpWallMs + r.bpCPUMs
		chScore := r.chunkWallMs + r.chunkCPUMs
		bpScoreClass, chScoreClass := cellWin(bpScore, chScore)
		bpScoreStr := fmt.Sprintf("%.0f", bpScore)
		chScoreStr := fmt.Sprintf("%.0f", chScore)
		if bpScoreClass != "" {
			bpScoreStr = fmtSpeedup(bpScoreStr, bpScore, chScore, true)
		} else if chScoreClass != "" {
			chScoreStr = fmtSpeedup(chScoreStr, bpScore, chScore, false)
		}

		bpLinesStr := fmt.Sprintf("%d", r.bpLines)
		chLinesStr := fmt.Sprintf("%d", r.chunkLines)

		bpAllocStr := fmt.Sprintf("%dk", r.bpAllocs/1000)
		chAllocStr := fmt.Sprintf("%dk", r.chunkAllocs/1000)
		if bpAllocsClass != "" {
			bpAllocStr = fmtSpeedup(bpAllocStr, float64(r.bpAllocs), float64(r.chunkAllocs), true)
		} else if chAllocsClass != "" {
			chAllocStr = fmtSpeedup(chAllocStr, float64(r.bpAllocs), float64(r.chunkAllocs), false)
		}

		bpMemStr := synthFmtBytes(r.bpAllocB)
		chMemStr := synthFmtBytes(r.chunkAllocB)
		if bpMemClass != "" {
			bpMemStr = fmtSpeedup(bpMemStr, float64(r.bpAllocB), float64(r.chunkAllocB), true)
		} else if chMemClass != "" {
			chMemStr = fmtSpeedup(chMemStr, float64(r.bpAllocB), float64(r.chunkAllocB), false)
		}

		// Native engine columns (log queries only; metric queries show "n/a").
		nativeStyle := "background:#d4edda;"
		var nativeWallStr, nativeLinesStr, nativeAllocStr, nativeMemStr, nativeNote string
		if r.nativeWallMs > 0 {
			nativeWallStr = fmt.Sprintf("%.0f", r.nativeWallMs)
			nativeLinesStr = fmt.Sprintf("%d", r.nativeLines)
			nativeAllocStr = fmt.Sprintf("%dk", r.nativeAllocs/1000)
			nativeMemStr = synthFmtBytes(r.nativeAllocB)
			nativeNote = ""
		} else {
			nativeWallStr = "n/a"
			nativeLinesStr = "n/a"
			nativeAllocStr = "n/a"
			nativeMemStr = "n/a"
			nativeNote = "metric"
		}

		explainAttr := ""
		if r.bpExplain != "" {
			explainAttr = fmt.Sprintf(" title=\"%s\"", htmlEscapeAttr(r.bpExplain))
		}

		w(fmt.Sprintf(
			"<tr><td class=\"query-name\"%s>%s</td>"+
				"%s"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric\" style=\"%s\">%s</td>"+
				"<td class=\"metric numeric\" style=\"%s\">%s</td>"+
				"<td class=\"metric numeric\" style=\"%s\">%s</td>"+
				"<td class=\"metric numeric\" style=\"%s\">%s</td>"+
				"<td class=\"metric numeric\" style=\"%s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"</tr>\n",
			explainAttr, r.name,
			verifyCell(r),
			bpIOClass, bpIOsStr, bpCPUClass, bpCPUStr,
			bpWallClass, bpWallStr, bpScoreClass, bpScoreStr, bpLinesStr,
			bpAllocsClass, bpAllocStr, bpMemClass, bpMemStr,
			nativeStyle, nativeWallStr,
			nativeStyle, nativeLinesStr,
			nativeStyle, nativeAllocStr,
			nativeStyle, nativeMemStr,
			nativeStyle, nativeNote,
			chIOClass, chIOsStr, chCPUClass, chCPUStr,
			chWallClass, chWallStr, chScoreClass, chScoreStr, chLinesStr,
			chAllocsClass, chAllocStr, chMemClass, chMemStr,
		))
	}
	w("</tbody></table>\n</div>\n")
}

func writeIOBreakdownHTML(w func(string), id, title string, results []synthResult) {
	w(fmt.Sprintf(
		"<button type=\"button\" class=\"section-header collapsible\" id=\"%s\""+
			" onclick=\"toggleSection(this)\" aria-expanded=\"true\" aria-controls=\"%s-content\">\n"+
			"\t<span class=\"chevron\">\u25BC</span> %s\n"+
			"</button>\n"+
			"<div class=\"collapsible-content\" id=\"%s-content\" aria-hidden=\"false\">\n",
		id, id, title, id))
	w(`<table>
<thead><tr>
	<th>Query</th><th>Kind</th>
	<th class="numeric chunk-col">Chunk IOs</th>
	<th class="numeric blockpack-col">BP IOs</th>
	<th class="numeric chunk-col">Chunk Lines</th>
	<th class="numeric blockpack-col">BP Lines</th>
	<th class="numeric chunk-col">Chunk Bytes</th>
	<th class="numeric blockpack-col">BP Bytes</th>
</tr></thead>
<tbody>
`)
	sorted := make([]synthResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].bpIOs != sorted[j].bpIOs {
			return sorted[i].bpIOs > sorted[j].bpIOs
		}
		return sorted[i].name < sorted[j].name
	})
	for _, r := range sorted {
		ioExplainAttr := ""
		if r.bpExplain != "" {
			ioExplainAttr = fmt.Sprintf(" title=\"%s\"", htmlEscapeAttr(r.bpExplain))
		}
		w(fmt.Sprintf(
			"<tr><td class=\"query-name\"%s>%s</td><td class=\"metric\">%s</td>"+
				"<td class=\"metric numeric\">%d</td><td class=\"metric numeric\">%d</td>"+
				"<td class=\"metric numeric\">%d</td><td class=\"metric numeric\">%d</td>"+
				"<td class=\"metric numeric\">%s</td><td class=\"metric numeric\">%s</td>"+
				"</tr>\n",
			ioExplainAttr, r.name, r.kind, r.chunkIOs, r.bpIOs, r.chunkLines, r.bpLines,
			synthFmtBytes(r.chunkBytes), synthFmtBytes(r.bpBytes)))
	}
	w("</tbody></table>\n</div>\n")
}

const filterQueryTableHeader = `<table class="comparison-table">
<thead>
	<tr>
		<th rowspan="2">Query</th>
		<th rowspan="2" style="text-align: center; width: 50px;">Verify</th>
		<th colspan="7" class="blockpack-col" style="text-align: center;">Blockpack</th>
		<th colspan="7" class="chunk-col" style="text-align: center;">Loki Chunks</th>
	</tr>
	<tr>
		<th class="numeric blockpack-col">IOs</th>
		<th class="numeric blockpack-col">CPU (ms)</th>
		<th class="numeric blockpack-col">Wall (ms)</th>
		<th class="numeric blockpack-col" title="Wall+CPU = wall-clock time + CPU time; lower is better; used for wins">Wall+CPU (ms)</th>
		<th class="numeric blockpack-col">Lines Returned</th>
		<th class="numeric blockpack-col">Allocs</th>
		<th class="numeric blockpack-col">Memory</th>
		<th class="numeric chunk-col">IOs</th>
		<th class="numeric chunk-col">CPU (ms)</th>
		<th class="numeric chunk-col">Wall (ms)</th>
		<th class="numeric chunk-col" title="Wall+CPU = wall-clock time + CPU time; lower is better; used for wins">Wall+CPU (ms)</th>
		<th class="numeric chunk-col">Lines Returned</th>
		<th class="numeric chunk-col">Allocs</th>
		<th class="numeric chunk-col">Memory</th>
	</tr>
</thead>
<tbody>
`

func writeFilterSectionHTML(w func(string), id, title string, results []synthResult) {
	var rows []synthResult
	for _, r := range results {
		if r.kind == "filter" {
			rows = append(rows, r)
		}
	}
	if len(rows) == 0 {
		return
	}
	w(fmt.Sprintf(
		"<button type=\"button\" class=\"section-header collapsible\" id=\"%s\""+
			" onclick=\"toggleSection(this)\" aria-expanded=\"true\" aria-controls=\"%s-content\">\n"+
			"\t<span class=\"chevron\">\u25BC</span> %s\n"+
			"</button>\n"+
			"<div class=\"collapsible-content\" id=\"%s-content\" aria-hidden=\"false\">\n",
		id, id, title, id))
	w(filterQueryTableHeader)

	cellWin := func(bpVal, chunkVal float64) (string, string) {
		const threshold = 0.95
		if bpVal < chunkVal*threshold {
			return "winner-blockpack", ""
		}
		if chunkVal < bpVal*threshold {
			return "", "winner-chunk"
		}
		return "", ""
	}

	for _, r := range rows {
		bpIOClass, chIOClass := cellWin(float64(r.bpIOs), float64(r.chunkIOs))
		bpCPUClass, chCPUClass := cellWin(r.bpCPUMs, r.chunkCPUMs)
		bpWallClass, chWallClass := cellWin(r.bpWallMs, r.chunkWallMs)
		bpAllocsClass, chAllocsClass := cellWin(float64(r.bpAllocs), float64(r.chunkAllocs))
		bpMemClass, chMemClass := cellWin(float64(r.bpAllocB), float64(r.chunkAllocB))

		fmtSpeedup := func(val string, bpV, chV float64, bpWin bool) string {
			if bpWin {
				return synthFmtSpeedup(val, synthSafeSpeedup(chV, bpV))
			}
			return synthFmtSpeedup(val, synthSafeSpeedup(bpV, chV))
		}

		bpIOsStr := fmt.Sprintf("%d", r.bpIOs)
		chIOsStr := fmt.Sprintf("%d", r.chunkIOs)
		if bpIOClass != "" {
			bpIOsStr = fmtSpeedup(bpIOsStr, float64(r.bpIOs), float64(r.chunkIOs), true)
		} else if chIOClass != "" {
			chIOsStr = fmtSpeedup(chIOsStr, float64(r.bpIOs), float64(r.chunkIOs), false)
		}

		bpCPUStr := fmt.Sprintf("%.0f", r.bpCPUMs)
		chCPUStr := fmt.Sprintf("%.0f", r.chunkCPUMs)
		if bpCPUClass != "" {
			bpCPUStr = fmtSpeedup(bpCPUStr, r.bpCPUMs, r.chunkCPUMs, true)
		} else if chCPUClass != "" {
			chCPUStr = fmtSpeedup(chCPUStr, r.bpCPUMs, r.chunkCPUMs, false)
		}

		bpWallStr := fmt.Sprintf("%.0f", r.bpWallMs)
		chWallStr := fmt.Sprintf("%.0f", r.chunkWallMs)
		if bpWallClass != "" {
			bpWallStr = fmtSpeedup(bpWallStr, r.bpWallMs, r.chunkWallMs, true)
		} else if chWallClass != "" {
			chWallStr = fmtSpeedup(chWallStr, r.bpWallMs, r.chunkWallMs, false)
		}

		bpScore := r.bpWallMs + r.bpCPUMs
		chScore := r.chunkWallMs + r.chunkCPUMs
		bpScoreClass, chScoreClass := cellWin(bpScore, chScore)
		bpScoreStr := fmt.Sprintf("%.0f", bpScore)
		chScoreStr := fmt.Sprintf("%.0f", chScore)
		if bpScoreClass != "" {
			bpScoreStr = fmtSpeedup(bpScoreStr, bpScore, chScore, true)
		} else if chScoreClass != "" {
			chScoreStr = fmtSpeedup(chScoreStr, bpScore, chScore, false)
		}

		bpAllocStr := fmt.Sprintf("%dk", r.bpAllocs/1000)
		chAllocStr := fmt.Sprintf("%dk", r.chunkAllocs/1000)
		if bpAllocsClass != "" {
			bpAllocStr = fmtSpeedup(bpAllocStr, float64(r.bpAllocs), float64(r.chunkAllocs), true)
		} else if chAllocsClass != "" {
			chAllocStr = fmtSpeedup(chAllocStr, float64(r.bpAllocs), float64(r.chunkAllocs), false)
		}

		bpMemStr := synthFmtBytes(r.bpAllocB)
		chMemStr := synthFmtBytes(r.chunkAllocB)
		if bpMemClass != "" {
			bpMemStr = fmtSpeedup(bpMemStr, float64(r.bpAllocB), float64(r.chunkAllocB), true)
		} else if chMemClass != "" {
			chMemStr = fmtSpeedup(chMemStr, float64(r.bpAllocB), float64(r.chunkAllocB), false)
		}

		filterExplainAttr := ""
		if r.bpExplain != "" {
			filterExplainAttr = fmt.Sprintf(" title=\"%s\"", htmlEscapeAttr(r.bpExplain))
		}
		nameCell := fmt.Sprintf(`<a href="#query-%s">%s</a>`, querySlug(r.name), r.name)
		w(fmt.Sprintf(
			"<tr><td class=\"query-name\"%s>%s</td>%s"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric\">%d</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric\">%d</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"<td class=\"metric numeric %s\">%s</td>"+
				"</tr>\n",
			filterExplainAttr, nameCell, verifyCell(r),
			bpIOClass, bpIOsStr, bpCPUClass, bpCPUStr, bpWallClass, bpWallStr,
			bpScoreClass, bpScoreStr, r.bpLines,
			bpAllocsClass, bpAllocStr, bpMemClass, bpMemStr,
			chIOClass, chIOsStr, chCPUClass, chCPUStr, chWallClass, chWallStr,
			chScoreClass, chScoreStr, r.chunkLines,
			chAllocsClass, chAllocStr, chMemClass, chMemStr,
		))
	}
	w("</tbody></table>\n</div>\n")
}

// filterResults returns only the "filter" kind results from a mixed result slice.
func filterResults(results []synthResult) []synthResult {
	var out []synthResult
	for _, r := range results {
		if r.kind == "filter" {
			out = append(out, r)
		}
	}
	return out
}

// htmlEscapeAttr escapes a string for use inside an HTML attribute value.
func htmlEscapeAttr(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// querySlug converts a query name like "T1/Q01/env=prod" to a URL-safe anchor slug.
func querySlug(name string) string {
	var b strings.Builder
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
			b.WriteRune(c)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// writeQueriesSection writes a collapsible "Query Reference" section listing all
// unique filter queries with their LogQL expressions.
func writeQueriesSection(w func(string), datasets []datasetResult) {
	// Collect unique queries in order (by name, deduplicated across datasets).
	seen := make(map[string]bool)
	type namedQuery struct{ name, query, bpExplain string }
	var queries []namedQuery
	for _, ds := range datasets {
		for _, r := range ds.results {
			if r.kind == "filter" && r.query != "" && !seen[r.name] {
				seen[r.name] = true
				queries = append(queries, namedQuery{r.name, r.query, r.bpExplain})
			}
		}
	}
	if len(queries) == 0 {
		return
	}

	const id = "queries-reference"
	w(fmt.Sprintf(
		"<button type=\"button\" class=\"section-header collapsible\" id=\"%s\""+
			" onclick=\"toggleSection(this)\" aria-expanded=\"false\" aria-controls=\"%s-content\">\n"+
			"\t<span class=\"chevron\" style=\"transform:rotate(-90deg)\">\u25BC</span> 📝 Query Reference\n"+
			"</button>\n"+
			"<div class=\"collapsible-content\" id=\"%s-content\" style=\"display:none\" aria-hidden=\"true\">\n",
		id, id, id))

	w(
		"<table class=\"comparison-table\"><thead><tr><th>Name</th><th>LogQL Expression</th><th>Query Plan</th></tr></thead><tbody>\n",
	)
	for _, q := range queries {
		explainHTML := ""
		if q.bpExplain != "" {
			explainHTML = fmt.Sprintf("<code>%s</code>", htmlEscapeAttr(q.bpExplain))
		}
		w(fmt.Sprintf(
			"<tr id=\"query-%s\"><td class=\"query-name\">%s</td><td><code>%s</code></td><td>%s</td></tr>\n",
			querySlug(q.name), q.name, q.query, explainHTML,
		))
	}
	w("</tbody></table>\n</div>\n")
}

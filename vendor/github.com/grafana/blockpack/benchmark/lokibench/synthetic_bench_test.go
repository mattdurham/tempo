// synthetic_bench_test.go — Self-contained synthetic log benchmark: Loki chunks vs blockpack.
//
// Generates synthetic log data in-process (no external data required) and benchmarks
// the same query set against both a Loki chunk store and a blockpack log file.
// Writes a self-contained HTML comparison report.
//
// # Usage
//
//	cd benchmark/lokibench
//	go test -run '^$' -bench BenchmarkSynthetic -benchtime=1x -timeout 600s -count=1 -v ./...
//
// # HTML report
//
// Run via the Makefile for a full report:
//
//	make bench-logs-html
//
// The report is written to /tmp/lokibench-synthetic-report.html by default,
// or to the path set by LOKIBENCH_REPORT env var.
package lokibench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

const (
	// syntheticReportEnv is the env var to override the report output path.
	syntheticReportEnv = "LOKIBENCH_REPORT"
	// syntheticReportDefault writes to the benchmark/ folder alongside the trace reports.
	// Go tests run with CWD set to the package directory (benchmark/lokibench/), so
	// "../" resolves to benchmark/.
	syntheticReportDefault = "../lokibench-bench.html"

	// syntheticIOLatency is the per-IO sleep injected into both stores to simulate
	// S3/GCS object storage round-trip latency. Applied to blockpack via WithIOLatency
	// and to Loki chunks as a post-execution sleep proportional to TotalChunksDownloaded.
	syntheticIOLatency = 20 * time.Millisecond

	// syntheticStreams is the number of log streams to generate.
	syntheticStreams = 50
	// syntheticDataMB is the approximate dataset size in bytes.
	syntheticDataMB = 50 * 1024 * 1024
	// syntheticTimeWindow is the time window covered by synthetic logs.
	syntheticTimeWindow = time.Hour
)

// htmlWallCPUDisclaimer is shared by both the synthetic and large-scale HTML reports.
const htmlWallCPUDisclaimer = `<div style="background: #e8f4f8; border-left: 4px solid #2980b9; padding: 15px; margin-bottom: 20px; border-radius: 4px;">
	<h3 style="margin-top: 0; color: #1a5276;">&#128202; Wall+CPU Score</h3>
	<p style="margin: 0; color: #1a5276;"><strong>Wall+CPU (ms) = wall-clock time + CPU time.</strong>
	This combined score captures both elapsed time and computational effort.
	Lower is better. Used to determine wins/losses in the summary.</p>
</div>
`

// synthResult holds per-query timing and I/O measurements for both stores.
type synthResult struct {
	name  string
	kind  string
	query string

	// Loki chunk store
	chunkWallMs float64
	chunkCPUMs  float64
	chunkIOs    int64
	chunkLines  int64
	chunkBytes  int64
	chunkAllocs int64
	chunkAllocB int64

	// Blockpack (via LokiConverter bridge)
	bpWallMs float64
	bpCPUMs  float64
	bpIOs    int64
	bpLines  int64
	bpBytes  int64
	bpAllocs int64
	bpAllocB int64

	// Query plan explain (from blockpack planner)
	bpExplain string

	// Native blockpack engine
	nativeWallMs float64
	nativeCPUMs  float64
	nativeIOs    int
	nativeLines  int64
	nativeBytes  int64
	nativeAllocs int64
	nativeAllocB int64

	// Verification: correctness comparison between chunk store and blockpack.
	// chunkData/bpData capture the parser.Value from the first benchmark iteration
	// so we can compare without extra query executions.
	chunkData    parser.Value
	bpData       parser.Value
	verified     bool   // true if verification was attempted
	verifyMatch  bool   // true if results matched
	verifyDetail string // mismatch description (empty if matched)
}

// BenchmarkSynthetic generates synthetic log data and benchmarks Loki chunks vs blockpack.
//
// No external data required — everything is generated in-process using bench.NewBuilder.
// The query set is auto-derived from the generated data via bench.NewTestCaseGenerator.
//
// Outputs standard Go benchmark metrics (ns/op, ios, cpuMs, linesProcessed) and
// writes a self-contained HTML comparison report on completion.
func BenchmarkSynthetic(b *testing.B) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	dir := b.TempDir()

	// --- Write phase: generate synthetic data into both stores ---
	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(b, err)

	bpStore, err := NewBlockpackStore(dir)
	require.NoError(b, err)

	opt := bench.DefaultOpt().
		WithNumStreams(syntheticStreams).
		WithTimeSpread(syntheticTimeWindow)

	b.Logf("Generating %d MB synthetic log data (%d streams, %s window)...",
		syntheticDataMB/1024/1024, syntheticStreams, syntheticTimeWindow)

	builder := bench.NewBuilder(dir, opt, chunkStore, bpStore)
	require.NoError(b, builder.Generate(context.Background(), syntheticDataMB))

	// --- Read phase: open both stores for querying ---
	// newLatencyChunkStore opens the same on-disk data written above, but wraps
	// the filesystem ObjectClient with latencyObjectClient so every chunk read
	// sleeps syntheticIOLatency — matching blockpack's WithIOLatency behaviour.
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := newLatencyChunkStore(dir, testTenant, syntheticIOLatency)
	require.NoError(b, err)

	bpReader, bpProvider, err := openBlockpackReaderWithLatency(bpStore.Path(), syntheticIOLatency)
	require.NoError(b, err)

	bpStat, err := os.Stat(bpStore.Path())
	require.NoError(b, err)

	// Compute chunk store disk usage: total dir size minus blockpack file.
	var totalDirBytes int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalDirBytes += info.Size()
		}
		return err
	})
	chunkSizeMB := float64(totalDirBytes-bpStat.Size()) / 1024 / 1024

	config, err := bench.LoadConfig(dir)
	require.NoError(b, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()
	require.NotEmpty(b, cases, "test case generator produced no cases")

	b.Logf("Dataset: %d streams, blockpack %.1f MB, chunks %.1f MB, %d blocks, %d query cases",
		syntheticStreams, float64(bpStat.Size())/1024/1024, chunkSizeMB, bpReader.BlockCount(), len(cases))

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)

	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpQuerier := NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	var results []synthResult

	for _, tc := range cases {
		res := synthResult{
			name:  tc.Name(),
			kind:  tc.Kind(),
			query: tc.Query,
		}

		params, err := logql.NewLiteralParams(
			tc.Query, tc.Start, tc.End, tc.Step, 0, tc.Direction, 1000, nil, nil,
		)
		require.NoError(b, err)

		// --- Chunk store ---
		require.NoError(b, chunkStoreRead.ClearCache())
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
				res.chunkBytes = chunkStoreRead.BytesRead()
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
				runtime.ReadMemStats(&m1)
				cpuBefore := readCPUSecs()
				start := time.Now()

				bpQuerier.ResetLines()
				bpProvider.Reset()
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
				res.bpBytes = bpProvider.BytesRead()
				res.bpAllocs = int64(m2.Mallocs - m1.Mallocs)
				res.bpAllocB = int64(m2.TotalAlloc - m1.TotalAlloc)

				b.ReportMetric(float64(res.bpIOs), "ios")
				b.ReportMetric(res.bpCPUMs, "cpuMs")
				b.ReportMetric(float64(res.bpLines), "linesProcessed")
			}
		})

		// --- Verification: compare captured results from both stores ---
		verifyFromCaptured(&res, b)

		results = append(results, res)
	}

	// Write HTML report.
	reportPath := os.Getenv(syntheticReportEnv)
	if reportPath == "" {
		reportPath = syntheticReportDefault
	}

	writeSyntheticHTMLReport(reportPath, results,
		syntheticStreams, float64(bpStat.Size())/1024/1024, chunkSizeMB, bpReader.BlockCount())
	b.Logf("HTML report written to %s", reportPath)
}

// writeSyntheticHTMLReport generates a self-contained HTML comparison report
// matching the visual style of the unified benchmark report.
func writeSyntheticHTMLReport(
	path string,
	results []synthResult,
	nStreams int,
	bpSizeMB float64,
	chunkSizeMB float64,
	nBlocks int,
) {
	var sb strings.Builder
	w := func(s string) { _, _ = sb.WriteString(s) }

	// Tally wins/losses using Wall+CPU (lower is better).
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

	w(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Blockpack vs Loki Chunks — Log Benchmark</title>
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
	.category-title {
		font-size: 1.3rem;
		margin-top: 40px;
		margin-bottom: 16px;
		color: #333;
		padding: 10px;
		background: white;
		border-left: 4px solid #3498db;
		box-shadow: 0 2px 4px rgba(0,0,0,0.1);
	}
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
			chevron.textContent = '▼';
			header.setAttribute('aria-expanded', 'true');
		} else {
			content.style.display = 'none';
			content.setAttribute('aria-hidden', 'true');
			chevron.textContent = '▶';
			header.setAttribute('aria-expanded', 'false');
		}
	}
	function initCollapsibleSections() {
		const headers = document.querySelectorAll('[onclick^="toggleSection"]');
		headers.forEach((header) => {
			header.setAttribute('role', 'button');
			if (!header.hasAttribute('tabindex')) { header.setAttribute('tabindex', '0'); }
			const content = header.nextElementSibling;
			const isHidden = content && content.style.display === 'none';
			header.setAttribute('aria-expanded', isHidden ? 'false' : 'true');
			if (content) { content.setAttribute('aria-hidden', isHidden ? 'true' : 'false'); }
			header.addEventListener('keydown', function(event) {
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

	w(fmt.Sprintf(`<h1>Blockpack vs Loki Chunks — Log Benchmark</h1>
<p class="metadata">Generated %s &nbsp;·&nbsp; Go %s &nbsp;·&nbsp; %s/%s</p>
`,
		time.Now().Format("2006-01-02 15:04:05 MST"),
		runtime.Version(), runtime.GOOS, runtime.GOARCH))

	w(
		`<div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 15px; margin-bottom: 20px; border-radius: 4px;">
	<h3 style="margin-top: 0; color: #856404;">⚠️ IO Latency Simulation</h3>
	<p style="margin: 0; color: #856404;"><strong>All benchmarks include 20ms injected latency per I/O operation</strong>
	to simulate S3/GCS object storage round-trip latency.
	For blockpack: one sleep per block read. For Loki chunks: one sleep per chunk downloaded.
	Total wall time = processing time + (I/O count × 20ms).</p>
</div>
` + htmlWallCPUDisclaimer,
	)

	w(`<div class="summary"><h2>📊 Dataset &amp; Results</h2>`)
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Streams:</strong> %d</div>`, nStreams))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Blockpack size:</strong> %.1f MB</div>`, bpSizeMB))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Chunk store size:</strong> %.1f MB</div>`, chunkSizeMB))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Blockpack blocks:</strong> %d</div>`, nBlocks))
	w(fmt.Sprintf(`<div class="summary-stat"><strong>Queries run:</strong> %d</div>`, len(results)))
	w(
		fmt.Sprintf(
			`<div class="summary-stat"><strong>Blockpack wins:</strong> <span class="speedup-blockpack">%d</span></div>`,
			bpWins,
		),
	)
	w(
		fmt.Sprintf(
			`<div class="summary-stat"><strong>Chunk wins:</strong> <span class="speedup-chunk">%d</span></div>`,
			chunkWins,
		),
	)
	w(
		fmt.Sprintf(
			`<div class="summary-stat"><strong>Parity (±10%%):</strong> <span class="winner-par">%d</span></div>`,
			parity,
		),
	)
	writeVerifySummaryHTML(w, results)
	w(`</div>
`)

	w(`<div class="toc">
	<h2>📑 Table of Contents</h2>
	<ul>
		<li><a href="#log-queries">📋 Log Queries (Blockpack vs Loki Chunks)</a></li>
		<li><a href="#metric-queries">📊 Metric Queries (Blockpack vs Loki Chunks)</a></li>
		<li><a href="#io-breakdown">📦 I/O Breakdown</a></li>
	</ul>
</div>
`)

	const queryTableHeader = `<table class="comparison-table">
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
		<th class="numeric blockpack-col">Lines</th>
		<th class="numeric blockpack-col">Allocs</th>
		<th class="numeric blockpack-col">Memory</th>
		<th class="numeric chunk-col">IOs</th>
		<th class="numeric chunk-col">CPU (ms)</th>
		<th class="numeric chunk-col">Wall (ms)</th>
		<th class="numeric chunk-col" title="Wall+CPU = wall-clock time + CPU time; lower is better; used for wins">Wall+CPU (ms)</th>
		<th class="numeric chunk-col">Lines</th>
		<th class="numeric chunk-col">Allocs</th>
		<th class="numeric chunk-col">Memory</th>
	</tr>
</thead>
<tbody>
`

	writeQuerySection := func(id, title, kind string) {
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
				"\t<span class=\"chevron\">▼</span> %s\n"+
				"</button>\n"+
				"<div class=\"collapsible-content\" id=\"%s-content\" aria-hidden=\"false\">\n",
			id, id, title, id))
		w(queryTableHeader)

		// cellWin computes which of bp/chunk wins (lower = better).
		// Returns (bpClass, chunkClass) CSS winner classes.
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
			// Per-metric winner classes.
			bpIOClass, chIOClass := cellWin(float64(r.bpIOs), float64(r.chunkIOs))
			bpCPUClass, chCPUClass := cellWin(r.bpCPUMs, r.chunkCPUMs)
			bpWallClass, chWallClass := cellWin(r.bpWallMs, r.chunkWallMs)
			bpAllocsClass, chAllocsClass := cellWin(float64(r.bpAllocs), float64(r.chunkAllocs))
			bpMemClass, chMemClass := cellWin(float64(r.bpAllocB), float64(r.chunkAllocB))

			// Format each metric, adding inline speedup to the winning cell.
			bpIOsStr := fmt.Sprintf("%d", r.bpIOs)
			chIOsStr := fmt.Sprintf("%d", r.chunkIOs)
			if bpIOClass != "" {
				bpIOsStr = synthFmtSpeedup(bpIOsStr, synthSafeSpeedup(float64(r.chunkIOs), float64(r.bpIOs)))
			} else if chIOClass != "" {
				chIOsStr = synthFmtSpeedup(chIOsStr, synthSafeSpeedup(float64(r.bpIOs), float64(r.chunkIOs)))
			}

			bpCPUStr := fmt.Sprintf("%.2f", r.bpCPUMs)
			chCPUStr := fmt.Sprintf("%.2f", r.chunkCPUMs)
			if bpCPUClass != "" {
				bpCPUStr = synthFmtSpeedup(bpCPUStr, synthSafeSpeedup(r.chunkCPUMs, r.bpCPUMs))
			} else if chCPUClass != "" {
				chCPUStr = synthFmtSpeedup(chCPUStr, synthSafeSpeedup(r.bpCPUMs, r.chunkCPUMs))
			}

			bpWallStr := fmt.Sprintf("%.0f", r.bpWallMs)
			chWallStr := fmt.Sprintf("%.0f", r.chunkWallMs)
			if bpWallClass != "" {
				bpWallStr = synthFmtSpeedup(bpWallStr, synthSafeSpeedup(r.chunkWallMs, r.bpWallMs))
			} else if chWallClass != "" {
				chWallStr = synthFmtSpeedup(chWallStr, synthSafeSpeedup(r.bpWallMs, r.chunkWallMs))
			}

			bpScore := r.bpWallMs + r.bpCPUMs
			chScore := r.chunkWallMs + r.chunkCPUMs
			bpScoreClass, chScoreClass := cellWin(bpScore, chScore)
			bpScoreStr := fmt.Sprintf("%.0f", bpScore)
			chScoreStr := fmt.Sprintf("%.0f", chScore)
			if bpScoreClass != "" {
				bpScoreStr = synthFmtSpeedup(bpScoreStr, synthSafeSpeedup(chScore, bpScore))
			} else if chScoreClass != "" {
				chScoreStr = synthFmtSpeedup(chScoreStr, synthSafeSpeedup(bpScore, chScore))
			}

			// Lines: informational only — blockpack reports 0 (no stats hook), no winner shown.
			bpLinesStr := fmt.Sprintf("%d", r.bpLines)
			chLinesStr := fmt.Sprintf("%d", r.chunkLines)

			bpAllocStr := fmt.Sprintf("%dk", r.bpAllocs/1000)
			chAllocStr := fmt.Sprintf("%dk", r.chunkAllocs/1000)
			if bpAllocsClass != "" {
				bpAllocStr = synthFmtSpeedup(bpAllocStr, synthSafeSpeedup(float64(r.chunkAllocs), float64(r.bpAllocs)))
			} else if chAllocsClass != "" {
				chAllocStr = synthFmtSpeedup(chAllocStr, synthSafeSpeedup(float64(r.bpAllocs), float64(r.chunkAllocs)))
			}

			bpMemStr := synthFmtBytes(r.bpAllocB)
			chMemStr := synthFmtBytes(r.chunkAllocB)
			if bpMemClass != "" {
				bpMemStr = synthFmtSpeedup(bpMemStr, synthSafeSpeedup(float64(r.chunkAllocB), float64(r.bpAllocB)))
			} else if chMemClass != "" {
				chMemStr = synthFmtSpeedup(chMemStr, synthSafeSpeedup(float64(r.bpAllocB), float64(r.chunkAllocB)))
			}

			w(fmt.Sprintf(
				"<tr><td class=\"query-name\">%s</td>"+
					"%s"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"<td class=\"metric numeric %s\">%s</td>"+
					"</tr>\n",
				r.name,
				verifyCell(r),
				bpIOClass, bpIOsStr,
				bpCPUClass, bpCPUStr,
				bpWallClass, bpWallStr,
				bpScoreClass, bpScoreStr,
				bpLinesStr,
				bpAllocsClass, bpAllocStr,
				bpMemClass, bpMemStr,
				chIOClass, chIOsStr,
				chCPUClass, chCPUStr,
				chWallClass, chWallStr,
				chScoreClass, chScoreStr,
				chLinesStr,
				chAllocsClass, chAllocStr,
				chMemClass, chMemStr,
			))
		}
		w("</tbody></table>\n</div>\n")
	}

	writeQuerySection("log-queries", "📋 Log Queries (Blockpack vs Loki Chunks)", "log")
	writeQuerySection("metric-queries", "📊 Metric Queries (Blockpack vs Loki Chunks)", "metric")

	// I/O Breakdown sorted by blockpack IOs descending.
	w(
		`<button type="button" class="section-header collapsible" id="io-breakdown" onclick="toggleSection(this)" aria-expanded="true" aria-controls="io-breakdown-content">
	<span class="chevron">▼</span> 📦 I/O Breakdown (sorted by blockpack IOs ↓)
</button>
<div class="collapsible-content" id="io-breakdown-content" aria-hidden="false">
<table>
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
`,
	)
	sorted := make([]synthResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].bpIOs != sorted[j].bpIOs {
			return sorted[i].bpIOs > sorted[j].bpIOs
		}
		return sorted[i].name < sorted[j].name
	})
	for _, r := range sorted {
		w(fmt.Sprintf(
			"<tr><td class=\"query-name\">%s</td><td class=\"metric\">%s</td>"+
				"<td class=\"metric numeric\">%d</td><td class=\"metric numeric\">%d</td>"+
				"<td class=\"metric numeric\">%d</td><td class=\"metric numeric\">%d</td>"+
				"<td class=\"metric numeric\">%s</td><td class=\"metric numeric\">%s</td>"+
				"</tr>\n",
			r.name, r.kind, r.chunkIOs, r.bpIOs, r.chunkLines, r.bpLines,
			synthFmtBytes(r.chunkBytes), synthFmtBytes(r.bpBytes)))
	}
	w("</tbody></table>\n</div>\n")

	writeVerifyMismatchSection(w, "verify-mismatches", results)

	w(`<footer>
  Report generated by <code>BenchmarkSynthetic</code> in
  <code>benchmark/lokibench/synthetic_bench_test.go</code>.
  IO simulation: 20ms sleep per chunk download (Loki) and per block read (blockpack). Wall time includes injected IO latency.
</footer>
</body>
</html>
`)

	_ = os.WriteFile(path, []byte(sb.String()), 0o644) //nolint:gosec
}

// synthFmtAllocs formats allocation counts and bytes for the HTML table.
func synthFmtAllocs(n, b int64) string {
	if b > 1024*1024 {
		return fmt.Sprintf("%dk/%.1fMB", n/1000, float64(b)/1024/1024)
	}
	return fmt.Sprintf("%dk/%.0fKB", n/1000, float64(b)/1024)
}

// synthFmtBytes formats a byte count for display.
func synthFmtBytes(b int64) string {
	if b == 0 {
		return "0"
	}
	if b > 1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(b)/1024/1024)
	}
	return fmt.Sprintf("%.1f KB", float64(b)/1024)
}

// synthSafeSpeedup returns loser/winner, or 1.0 on divide-by-zero.
func synthSafeSpeedup(loser, winner float64) float64 {
	if winner == 0 {
		return 1.0
	}
	return loser / winner
}

// synthFmtSpeedup appends a grey "(X.Xx)" speedup indicator when speedup > 1.0,
// matching the inline style used by the unified benchmark report.
func synthFmtSpeedup(value string, speedup float64) string {
	if speedup <= 1.0 {
		return value
	}
	return fmt.Sprintf(`%s <span style="color: #999; font-size: 0.85em;">(%.1fx)</span>`, value, speedup)
}

// BenchmarkSyntheticLogOnly is a faster variant that runs only log (not metric) queries.
// Useful for quick iteration during development.
//
//	cd benchmark/lokibench
//	go test -run '^$' -bench BenchmarkSyntheticLogOnly -benchtime=1x -timeout 300s -count=1 ./...
func BenchmarkSyntheticLogOnly(b *testing.B) {
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	dir := b.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(b, err)

	bpStore, err := NewBlockpackStore(dir)
	require.NoError(b, err)

	opt := bench.DefaultOpt().
		WithNumStreams(20).
		WithTimeSpread(30 * time.Minute)

	builder := bench.NewBuilder(dir, opt, chunkStore, bpStore)
	require.NoError(b, builder.Generate(context.Background(), 20*1024*1024))

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := newLatencyChunkStore(dir, testTenant, syntheticIOLatency)
	require.NoError(b, err)

	bpReader, bpProvider, err := openBlockpackReaderWithLatency(bpStore.Path(), syntheticIOLatency)
	require.NoError(b, err)

	config, err := bench.LoadConfig(dir)
	require.NoError(b, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)

	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpQuerier := NewLokiConverter(bpReader)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	for _, tc := range cases {
		if tc.Kind() != "log" {
			continue
		}

		params, err := logql.NewLiteralParams(
			tc.Query, tc.Start, tc.End, tc.Step, 0, tc.Direction, 1000, nil, nil,
		)
		require.NoError(b, err)

		require.NoError(b, chunkStoreRead.ClearCache())
		b.Run(fmt.Sprintf("q=%s/store=chunk", tc.Name()), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				chunkStoreRead.ResetIO()
				r, runErr := chunkEngine.Query(params).Exec(ctx)
				require.NoError(b, runErr)
				b.ReportMetric(float64(chunkStoreRead.IOCount()), "ios")
				b.ReportMetric(r.Statistics.Summary.ExecTime*1000, "cpuMs")
				b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "linesProcessed")
			}
		})

		b.Run(fmt.Sprintf("q=%s/store=blockpack", tc.Name()), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				bpProvider.Reset()
				r, runErr := bpEngine.Query(params).Exec(ctx)
				require.NoError(b, runErr)
				if s := bpQuerier.LastStats(); s != nil {
					if s.Explain != "" {
						b.Logf("explain: %s", s.Explain)
					}
				}
				b.ReportMetric(float64(bpProvider.IOCount()), "ios")
				b.ReportMetric(r.Statistics.Summary.ExecTime*1000, "cpuMs")
				b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "linesProcessed")
			}
		})
	}
}

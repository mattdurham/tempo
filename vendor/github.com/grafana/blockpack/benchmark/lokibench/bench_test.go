package lokibench

import (
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	lokiengine "github.com/grafana/loki/v3/pkg/engine"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql"
	"github.com/grafana/loki/v3/pkg/logql/bench"
	"github.com/grafana/loki/v3/pkg/logqlmodel"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTenant = "lokibench-test"

// resetPrometheusRegistry installs a fresh Prometheus DefaultRegisterer for the duration
// of the test. bench.NewChunkStore calls prometheus.MustRegister which panics on
// duplicate registration (process-global registry). Each test that creates a ChunkStore
// must call this helper first.
func resetPrometheusRegistry(t *testing.T) {
	t.Helper()
	orig := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() { prometheus.DefaultRegisterer = orig })
}

// TestBlockpackSmoke generates a small dataset in-test, writes to both chunk store
// and blockpack store, and verifies log query results match. No pre-generated data
// directory required. Runs in under 30 seconds.
func TestBlockpackSmoke(t *testing.T) {
	resetPrometheusRegistry(t)
	dir := t.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	blockpackStore, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	// Small dataset: 10 streams, 30-minute window, 512KB total.
	opt := bench.DefaultOpt().
		WithNumStreams(10).
		WithTimeSpread(30 * time.Minute)

	builder := bench.NewBuilder(dir, opt, chunkStore, blockpackStore)
	require.NoError(t, builder.Generate(context.Background(), 512*1024))
	// builder.Generate() closed both stores (flushing data to disk).
	// Install a fresh registry before creating the second ChunkStore to avoid
	// "duplicate metrics registration" panics from prometheus.MustRegister.
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpReader, err := openBlockpackReader(blockpackStore.Path())
	require.NoError(t, err)
	require.Greater(t, bpReader.BlockCount(), 0, "blockpack file must have at least one block")

	config, err := bench.LoadConfig(dir)
	require.NoError(t, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()
	require.NotEmpty(t, cases)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(t, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, NewLokiConverter(bpReader), logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	// Run up to 5 log queries to keep the smoke test fast.
	var ran int
	for _, tc := range cases {
		if tc.Kind() != "log" {
			continue
		}
		t.Run(fmt.Sprintf("query=%s", tc.Name()), func(t *testing.T) {
			params, err := logql.NewLiteralParams(
				tc.Query, tc.Start, tc.End, tc.Step, 0, tc.Direction, 1000, nil, nil,
			)
			require.NoError(t, err)

			expected, err := chunkEngine.Query(params).Exec(ctx)
			require.NoError(t, err)

			actual, err := bpEngine.Query(params).Exec(ctx)
			if errors.Is(err, lokiengine.ErrNotSupported) {
				t.Skip("query type not supported")
			}
			require.NoError(t, err)

			assertDataEqualWithTolerance(t, expected.Data, actual.Data, 0)
		})
		ran++
		if ran >= 5 {
			break
		}
	}
	if ran == 0 {
		t.Skip("no log query cases generated")
	}
}

// TestStorageEquality is a comprehensive test comparing blockpack against the chunk
// store for the full set of generated test cases (log and metric queries).
// Uses a 50MB dataset with 50 streams over 1 hour.
func TestStorageEquality(t *testing.T) {
	resetPrometheusRegistry(t)
	dir := t.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	blockpackStore, err := NewBlockpackStore(dir)
	require.NoError(t, err)

	opt := bench.DefaultOpt().
		WithNumStreams(50).
		WithTimeSpread(time.Hour)

	builder := bench.NewBuilder(dir, opt, chunkStore, blockpackStore)
	require.NoError(t, builder.Generate(context.Background(), 50*1024*1024))
	// Install a fresh registry before creating the second ChunkStore.
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(t, err)

	bpReader, err := openBlockpackReader(blockpackStore.Path())
	require.NoError(t, err)

	config, err := bench.LoadConfig(dir)
	require.NoError(t, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(t, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, NewLokiConverter(bpReader), logql.NoLimits, logger)

	ctx := user.InjectOrgID(context.Background(), testTenant)

	for _, tc := range cases {
		t.Run(fmt.Sprintf("query=%s/kind=%s", tc.Name(), tc.Kind()), func(t *testing.T) {
			params, err := logql.NewLiteralParams(
				tc.Query, tc.Start, tc.End, tc.Step, 0, tc.Direction, 1000, nil, nil,
			)
			require.NoError(t, err)

			expected, err := chunkEngine.Query(params).Exec(ctx)
			require.NoError(t, err)

			actual, err := bpEngine.Query(params).Exec(ctx)
			if errors.Is(err, lokiengine.ErrNotSupported) {
				t.Skipf("not supported: %v", err)
			}
			require.NoError(t, err)

			// Tolerance 1e-5 for floating-point metric comparisons.
			// Log stream results use content-based comparison (assertDataEqualWithTolerance
			// compares the multiset of (timestamp, line) pairs for logqlmodel.Streams).
			assertDataEqualWithTolerance(t, expected.Data, actual.Data, 1e-5)
		})
	}
}

// benchSetup holds the pre-generated data and engines for BenchmarkLogQL.
// Generated once per benchmark run via setupBenchmarkEngines.
type benchSetup struct {
	cases  []bench.TestCase
	engine logql.Engine
	name   string
}

// setupBenchmarkEngines generates a dataset and creates engines for chunk and blockpack stores.
// DataObj v2 engine is excluded because its engine field is unexported from the bench package.
func setupBenchmarkEngines(b *testing.B) []*benchSetup {
	b.Helper()
	prometheus.DefaultRegisterer = prometheus.NewRegistry()

	dir := b.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(b, err)

	blockpackStore, err := NewBlockpackStore(dir)
	require.NoError(b, err)

	// Generate 100MB of log data across 50 streams, 1 hour.
	opt := bench.DefaultOpt().
		WithNumStreams(50).
		WithTimeSpread(time.Hour)

	builder := bench.NewBuilder(dir, opt, chunkStore, blockpackStore)
	require.NoError(b, builder.Generate(context.Background(), 100*1024*1024))

	config, err := bench.LoadConfig(dir)
	require.NoError(b, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())

	// Chunk engine
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(b, err)
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)

	// Blockpack engine
	bpReader, err := openBlockpackReader(blockpackStore.Path())
	require.NoError(b, err)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, NewLokiConverter(bpReader), logql.NoLimits, logger)

	return []*benchSetup{
		{cases: cases, engine: chunkEngine, name: "chunk"},
		{cases: cases, engine: bpEngine, name: "blockpack"},
	}
}

// BenchmarkLogQL runs LogQL queries against all three storage backends
// (chunk store, dataobj v2 engine, blockpack) and reports performance metrics.
func BenchmarkLogQL(b *testing.B) {
	setups := setupBenchmarkEngines(b)

	for _, setup := range setups {
		for _, c := range setup.cases {
			b.Run(fmt.Sprintf("query=%s/kind=%s/store=%s", c.Name(), c.Kind(), setup.name), func(b *testing.B) {
				ctx := user.InjectOrgID(b.Context(), testTenant)

				labels := pprof.Labels("query", c.Name(), "kind", c.Kind(), "store", setup.name)

				pprof.Do(ctx, labels, func(ctx context.Context) {
					params, err := logql.NewLiteralParams(
						c.Query,
						c.Start,
						c.End,
						c.Step,
						0,
						c.Direction,
						1000,
						nil,
						nil,
					)
					require.NoError(b, err)

					q := setup.engine.Query(params)

					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						ctx, cancel := context.WithTimeout(ctx, time.Minute)

						r, err := q.Exec(ctx)
						cancel()
						if errors.Is(err, lokiengine.ErrNotSupported) {
							b.Skipf("not supported: %v", err)
						}
						require.NoError(b, err)

						b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "linesProcessed")
						b.ReportMetric(float64(r.Statistics.Summary.TotalBytesProcessed)/1024, "kilobytesProcessed")
					}
				})
			})
		}
	}
}

// BenchmarkLogQL_LogOnly benchmarks only log queries (not metric) for faster iteration.
func BenchmarkLogQL_LogOnly(b *testing.B) {
	setups := setupBenchmarkEngines(b)

	for _, setup := range setups {
		for _, c := range setup.cases {
			if c.Kind() != "log" {
				continue
			}
			// Only FORWARD direction to halve the cases
			if c.Direction != logproto.FORWARD {
				continue
			}
			b.Run(fmt.Sprintf("query=%s/store=%s", c.Name(), setup.name), func(b *testing.B) {
				ctx := user.InjectOrgID(b.Context(), testTenant)

				params, err := logql.NewLiteralParams(
					c.Query, c.Start, c.End, c.Step, 0, c.Direction, 1000, nil, nil,
				)
				require.NoError(b, err)

				q := setup.engine.Query(params)
				b.ReportAllocs()
				b.ResetTimer()

				for b.Loop() {
					ctx, cancel := context.WithTimeout(ctx, time.Minute)
					r, err := q.Exec(ctx)
					cancel()
					if errors.Is(err, lokiengine.ErrNotSupported) {
						b.Skipf("not supported: %v", err)
					}
					require.NoError(b, err)
					b.ReportMetric(float64(r.Statistics.Summary.TotalPostFilterLines), "linesProcessed")
					b.ReportMetric(float64(r.Statistics.Summary.TotalBytesProcessed)/1024, "kilobytesProcessed")
				}
			})
		}
	}
}

// BenchmarkLogQL_ObjectStorage simulates 50ms per-block object storage latency.
// This shows the impact of block pruning when I/O dominates — fewer blocks read = faster.
func BenchmarkLogQL_ObjectStorage(b *testing.B) {
	const ioLatency = 50 * time.Millisecond

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	dir := b.TempDir()

	chunkStore, err := bench.NewChunkStore(dir, testTenant)
	require.NoError(b, err)

	blockpackStore, err := NewBlockpackStore(dir)
	require.NoError(b, err)

	opt := bench.DefaultOpt().
		WithNumStreams(50).
		WithTimeSpread(time.Hour)

	builder := bench.NewBuilder(dir, opt, chunkStore, blockpackStore)
	require.NoError(b, builder.Generate(context.Background(), 100*1024*1024))

	config, err := bench.LoadConfig(dir)
	require.NoError(b, err)

	cases := bench.NewTestCaseGenerator(bench.DefaultTestCaseGeneratorConfig, config).Generate()

	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	chunkStoreRead, err := newLatencyChunkStore(dir, testTenant, ioLatency)
	require.NoError(b, err)

	bpReader, _, err := openBlockpackReaderWithLatency(blockpackStore.Path(), ioLatency)
	require.NoError(b, err)

	logger := level.NewFilter(log.NewNopLogger(), level.AllowWarn())
	chunkQuerier, err := chunkStoreRead.Querier()
	require.NoError(b, err)
	chunkEngine := logql.NewEngine(logql.EngineOpts{}, chunkQuerier, logql.NoLimits, logger)

	// Blockpack with parallel reads (models S3 with connection pool)
	bpQuerier := NewLokiConverter(bpReader).WithParallelism(16)
	bpEngine := logql.NewEngine(logql.EngineOpts{}, bpQuerier, logql.NoLimits, logger)

	totalBlocks := bpReader.BlockCount()

	for _, c := range cases {
		if c.Kind() != "log" || c.Direction != logproto.FORWARD {
			continue
		}
		b.Run(fmt.Sprintf("query=%s/store=chunk-50ms", c.Name()), func(b *testing.B) {
			ctx := user.InjectOrgID(b.Context(), testTenant)
			params, pErr := logql.NewLiteralParams(
				c.Query, c.Start, c.End, c.Step, 0, c.Direction, 1000, nil, nil,
			)
			require.NoError(b, pErr)

			q := chunkEngine.Query(params)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				_, runErr := q.Exec(ctx)
				cancel()
				if errors.Is(runErr, lokiengine.ErrNotSupported) {
					b.Skipf("not supported: %v", runErr)
				}
				require.NoError(b, runErr)
			}
		})
		b.Run(fmt.Sprintf("query=%s/store=blockpack-50ms", c.Name()), func(b *testing.B) {
			ctx := user.InjectOrgID(b.Context(), testTenant)
			params, err := logql.NewLiteralParams(
				c.Query, c.Start, c.End, c.Step, 0, c.Direction, 1000, nil, nil,
			)
			require.NoError(b, err)

			q := bpEngine.Query(params)
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
				_, err := q.Exec(ctx)
				cancel()
				if errors.Is(err, lokiengine.ErrNotSupported) {
					b.Skipf("not supported: %v", err)
				}
				require.NoError(b, err)
				b.ReportMetric(float64(totalBlocks), "totalBlocks")
			}
		})
	}
}

// assertDataEqualWithTolerance compares two parser.Value instances with tolerance.
// For log stream results (logqlmodel.Streams), compares by (timestamp, line) content
// to handle stream label set differences between Loki schema v13 and blockpack.
// For metric results (Vector, Matrix, Scalar), uses InDelta with tolerance.
//
// Copied from Loki's bench_test.go (those functions are unexported in package bench
// so they cannot be called from an external module).
func assertDataEqualWithTolerance(t *testing.T, expected, actual parser.Value, tolerance float64) {
	t.Helper()
	switch exp := expected.(type) {
	case logqlmodel.Streams:
		act, ok := actual.(logqlmodel.Streams)
		require.True(t, ok, "expected Streams but got %T", actual)
		assertStreamsEqualByContent(t, exp, act)
	case promql.Vector:
		act, ok := actual.(promql.Vector)
		require.True(t, ok, "expected Vector but got %T", actual)
		assertVectorEqualWithTolerance(t, exp, act, tolerance)
	case promql.Matrix:
		act, ok := actual.(promql.Matrix)
		require.True(t, ok, "expected Matrix but got %T", actual)
		assertMatrixEqualWithTolerance(t, exp, act, tolerance)
	case promql.Scalar:
		act, ok := actual.(promql.Scalar)
		require.True(t, ok, "expected Scalar but got %T", actual)
		assertScalarEqualWithTolerance(t, exp, act, tolerance)
	default:
		assert.Equal(t, expected, actual)
	}
}

// assertStreamsEqualByContent compares two Streams by the multiset of (timestamp, line)
// pairs across all streams, ignoring stream label attribution.
//
// Loki's chunk store with schema v13 promotes StructuredMetadata fields to stream labels,
// creating separate sub-streams per metadata value combination. Blockpack preserves the
// original stream labels. Both stores hold the same log lines at the same timestamps, so
// content-based comparison is the correct equality check for this benchmark.
func assertStreamsEqualByContent(t *testing.T, expected, actual logqlmodel.Streams) {
	t.Helper()
	type entry struct {
		ts   int64
		line string
	}
	collect := func(streams logqlmodel.Streams) []entry {
		var entries []entry
		for _, s := range streams {
			for _, e := range s.Entries {
				entries = append(entries, entry{ts: e.Timestamp.UnixNano(), line: e.Line})
			}
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].ts != entries[j].ts {
				return entries[i].ts < entries[j].ts
			}
			return entries[i].line < entries[j].line
		})
		return entries
	}
	expEntries := collect(expected)
	actEntries := collect(actual)
	require.Len(t, actEntries, len(expEntries), "total log entry count differs")
	for i := range expEntries {
		require.Equal(t, expEntries[i].ts, actEntries[i].ts, "timestamp differs at entry %d", i)
		require.Equal(t, expEntries[i].line, actEntries[i].line, "line differs at entry %d", i)
	}
}

func assertVectorEqualWithTolerance(t *testing.T, expected, actual promql.Vector, tolerance float64) {
	t.Helper()
	require.Len(t, actual, len(expected))
	for i := range expected {
		e, a := expected[i], actual[i]
		require.Equal(t, e.Metric, a.Metric, "metric labels differ at index %d", i)
		require.Equal(t, e.T, a.T, "timestamp differs at index %d", i)
		if tolerance > 0 {
			require.InDelta(t, e.F, a.F, tolerance, "value differs at index %d", i)
		} else {
			require.Equal(t, e.F, a.F, "value differs at index %d", i)
		}
	}
}

func assertMatrixEqualWithTolerance(t *testing.T, expected, actual promql.Matrix, tolerance float64) {
	t.Helper()
	require.Equal(t, len(expected), len(actual), "series count differs")
	for i := range expected {
		e, a := expected[i], actual[i]
		require.Equal(t, e.Metric, a.Metric, "metric labels differ at series %d", i)
		require.Len(t, a.Floats, len(e.Floats), "float point count differs at series %d", i)
		for j := range e.Floats {
			ep, ap := e.Floats[j], a.Floats[j]
			require.Equal(t, ep.T, ap.T, "timestamp differs at series %d point %d", i, j)
			if tolerance > 0 {
				require.InDelta(t, ep.F, ap.F, tolerance, "value differs at series %d point %d", i, j)
			} else {
				require.Equal(t, ep.F, ap.F, "value differs at series %d point %d", i, j)
			}
		}
	}
}

func assertScalarEqualWithTolerance(t *testing.T, expected, actual promql.Scalar, tolerance float64) {
	t.Helper()
	require.Equal(t, expected.T, actual.T, "scalar timestamp differs")
	if tolerance > 0 {
		require.InDelta(t, expected.V, actual.V, tolerance, "scalar value differs")
	} else {
		require.Equal(t, expected.V, actual.V, "scalar value differs")
	}
}

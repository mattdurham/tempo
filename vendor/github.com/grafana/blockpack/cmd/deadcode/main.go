// Package main exercises every public blockpack API function so that the
// deadcode tool does not report the public API as unreachable.
package main

import (
	"context"
	"io"

	minio "github.com/minio/minio-go/v7"

	"github.com/grafana/blockpack"
	"github.com/grafana/blockpack/benchmark"
	modulesblockio "github.com/grafana/blockpack/internal/modules/blockio"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
)

// noopProvider is a minimal ReaderProvider stub.
type noopProvider struct{}

func (n *noopProvider) Size() (int64, error)                                        { return 0, io.EOF }
func (n *noopProvider) ReadAt(_ []byte, _ int64, _ blockpack.DataType) (int, error) { return 0, io.EOF }

func main() {
	_, _ = blockpack.NewWriter(io.Discard, 0)
	r, _ := blockpack.NewReaderFromProvider(&noopProvider{})
	if r != nil {
		if matches, _ := blockpack.QueryTraceQL(r, `{}`, blockpack.QueryOptions{}); len(matches) > 0 {
			_ = matches[0].Clone()
		}
		_, _ = blockpack.QueryLogQL(r, `{}`, blockpack.LogQueryOptions{})
		_, _ = blockpack.GetTraceByID(r, "00000000000000000000000000000000")
		_ = blockpack.ColumnNames(r)
	}

	storage := blockpack.NewFileStorage("")
	_, _ = blockpack.GetBlockMeta("", storage)

	// Reference benchmark functions without calling them (avoid nil *testing.B panics)
	_ = benchmark.GenerateComprehensiveTestData
	_ = benchmark.ExecuteBlockpackQuery
	_ = benchmark.ExecuteParquetQuery
	_ = benchmark.BenchmarkQueryPair
	_ = benchmark.ReportCosts
	_ = benchmark.DeepCompareSearchResponses
	_ = benchmark.AnchorPrivateFunctions

	// Internal API hooks — keep internal functions reachable by deadcode.
	_ = vm.MergeAggregationResults()

	// modules/blockio package - anchor all public API entry points and methods.
	mbw, _ := modulesblockio.NewWriterWithConfig(modulesblockio.WriterConfig{OutputStream: io.Discard})
	if mbw != nil {
		_ = mbw.AddSpan(nil, nil, nil, "", nil, "")
		_ = mbw.AddTracesData(nil)
		_ = mbw.AddLogsData(nil)
		// Skip Flush() — it will panic with nil spans, but we only need to anchor the method reference
		_ = mbw.CurrentSize()
	}
	mbProvider := modulesblockio.NewDefaultProvider(&noopProvider{})
	if mbProvider != nil {
		_, _ = mbProvider.Size()
		_, _ = mbProvider.ReadAt(nil, 0, blockpack.DataTypeBlock)
		_ = mbProvider.IOOps()
		_ = mbProvider.BytesRead()
	}
	var mbr *modulesblockio.Reader
	if mbProvider != nil {
		mbr, _ = modulesblockio.NewReaderFromProvider(mbProvider)
	}
	if mbr != nil {
		_ = mbr.BlockCount()
		_ = mbr.BlockMeta(0)
		_, _ = mbr.GetBlockWithBytes(0, nil, nil)
		_ = mbr.AddColumnsToBlock(nil, nil)
	}
	_ = modulesblockio.CoalesceBlocks(nil, nil, modulesblockio.CoalesceConfig{})
	_, _ = modulesblockio.ReadCoalescedBlocks(nil, nil)
	_, _ = modulesblockio.ReadBlocks(nil, nil)
	_ = (*modulesblockio.Writer)(nil)
	_ = (*modulesblockio.Reader)(nil)

	// queryplanner package - anchor all public API entry points.
	if mbr != nil {
		qp := queryplanner.NewPlanner(mbr)
		plan := qp.Plan([]queryplanner.Predicate{
			{Columns: []string{"resource.service.name"}},
		}, queryplanner.TimeRange{})
		_, _ = qp.FetchBlocks(plan)
	}

	// modules/executor package - anchor all public API entry points.
	if mbr != nil {
		_, _ = modules_executor.Collect(mbr, nil, modules_executor.CollectOptions{})
		_ = modules_executor.BuildPredicates
		_ = modules_executor.SpanMatchFromRow
	}

	// AnalyzeFileLayout - anchor file layout API
	if r != nil {
		_, _ = blockpack.AnalyzeFileLayout(r)
	}

	// ExecuteMetricsLogQL - anchor new LogQL metrics API
	if r != nil {
		_, _ = blockpack.ExecuteMetricsLogQL(
			r,
			`count_over_time({service.name="svc"} [1m])`,
			blockpack.LogMetricOptions{
				StartNano: 0,
				EndNano:   int64(60 * 1e9),
				StepNano:  int64(60 * 1e9),
			},
		)
	}

	// ExecuteMetricsTraceQL - anchor new TraceQL metrics API
	if r != nil {
		_, _ = blockpack.ExecuteMetricsTraceQL(r, `{ } | count_over_time()`, blockpack.TraceMetricOptions{
			StartNano: 0,
			EndNano:   int64(60 * 1e9),
			StepNano:  int64(60 * 1e9),
		})
	}

	// CompactBlocks - anchor compaction API
	_, _ = blockpack.CompactBlocks(
		context.Background(),
		[]blockpack.ReaderProvider{&noopProvider{}},
		blockpack.CompactionConfig{},
		storage,
	)

	// ConvertProtoToBlockpack / ConvertLogsProtoToBlockpack - anchor conversion APIs
	_ = blockpack.ConvertProtoToBlockpack
	_ = blockpack.ConvertLogsProtoToBlockpack("", io.Discard, 0)

	// modules/blockio newly exported function
	_ = modulesblockio.NewSpanFieldsAdapter

	// NewMinIOProvider - anchor S3/MinIO storage provider API
	_ = blockpack.NewMinIOProvider((*minio.Client)(nil), "", "")

	// SharedLRUCache / SharedLRUProvider - anchor shared cross-reader LRU cache API
	sharedCache := blockpack.NewSharedLRUCache(1024)
	_ = blockpack.NewSharedLRUProvider(&noopProvider{}, "anchor", sharedCache)

	// FileCache + cache-aware reader constructors
	fc, _ := blockpack.OpenFileCache(blockpack.FileCacheConfig{})
	_, _ = blockpack.NewReaderWithCache(&noopProvider{}, "anchor", fc)
	_, _ = blockpack.NewLeanReaderWithCache(&noopProvider{}, "anchor", fc)
}

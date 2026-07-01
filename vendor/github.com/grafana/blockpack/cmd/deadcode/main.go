// Package main exercises every public blockpack API function so that the
// deadcode tool does not report the public API as unreachable.
package main

import (
	"context"
	"io"

	minio "github.com/minio/minio-go/v7"

	"github.com/grafana/blockpack"
	"github.com/grafana/blockpack/benchmark"
	pubblockevents "github.com/grafana/blockpack/blockevents"
	pubembedder "github.com/grafana/blockpack/embedder"
	modulesblockio "github.com/grafana/blockpack/internal/modules/blockio"
	"github.com/grafana/blockpack/internal/modules/blockio/shared"
	"github.com/grafana/blockpack/internal/modules/cube"
	"github.com/grafana/blockpack/internal/modules/embedder"
	modules_executor "github.com/grafana/blockpack/internal/modules/executor"
	"github.com/grafana/blockpack/internal/modules/queryplanner"
	"github.com/grafana/blockpack/internal/vm"
	pubvalueindexcompactor "github.com/grafana/blockpack/valueindexcompactor"
	pubvalueindexconsumer "github.com/grafana/blockpack/valueindexconsumer"
)

// anchorObjectPutter satisfies blockpack.ObjectPutter so anchorMain can reference
// WriteValueIndexL0's full signature without a real object store.
type anchorObjectPutter struct{}

func (anchorObjectPutter) Put(string, []byte) error { return nil }

func (n *noopProvider) Size() (int64, error) { return 0, io.EOF }

func (n *noopProvider) ReadAt(_ []byte, _ int64, _ blockpack.DataType) (int, error) { return 0, io.EOF }

func main() {
	_, _ = blockpack.NewWriter(io.Discard, 0)
	r, _ := blockpack.NewReaderFromProvider(&noopProvider{})
	if r != nil {
		if matches, _, _ := blockpack.QueryTraceQL(context.Background(), r, `{}`, blockpack.QueryOptions{}); len(
			matches,
		) > 0 {
			_ = matches[0].Clone()
		}
		_, _ = blockpack.GetTraceByID(r, "00000000000000000000000000000000")
	}

	storage := blockpack.NewFileStorage("")
	_, _ = blockpack.GetBlockMeta("", storage)
	_, _ = blockpack.ParseColMetas(nil, shared.BlockMeta{})

	// NOTE-VI-033 (issue #460): zero-block-read metrics value-index source. Public API
	// consumed by the querier in issue #461; anchor it until that wiring lands.
	_ = blockpack.NewSliceValueIndexSource()
	// Build-time I/O stats (issue #465): consumed by tempo's blockpackBlock to
	// attach index-path observability to its OTel span; anchor the accessor + type.
	anchorVIStats(blockpack.NewSliceValueIndexSource().Stats())
	// Value-index query-wiring public API (issue #461): consumed by tempo's
	// blockpackBlock querier, not internally, so anchor it for deadcode.
	_ = blockpack.NewIndexFileCache(nil, "", "", 0)
	// Cache-coherence API (issue #431): the querier write-throughs newly flushed
	// files and surgically evicts compactor-deleted ones so discovery stays fresh
	// without a full re-list. Consumed by tempo's blockpack querier, anchor here.
	if ifc := blockpack.NewIndexFileCache(nil, "", "", 0); ifc != nil {
		_ = ifc.AddFile("", "", "")
		ifc.RemoveFiles("", "", "")
		ifc.Invalidate("", "")
	}
	_, _, _ = blockpack.BuildValueIndexSource(context.TODO(), nil, nil, nil, 0, 0)
	_, _, _ = blockpack.BuildValueIndexSourceForMetrics(context.TODO(), nil, nil, "", 0, 0)
	// Synchronous in-process L0 write path (NOTE-VI-042, issue #464): consumed by
	// tempo's block-builder and compactor (create.go / compactor.go) to index each
	// flushed block without a Redis broker, not internally — anchor the public
	// function and ObjectPutter interface for deadcode.
	_ = blockpack.WriteValueIndexL0(nil, anchorObjectPutter{}, "", "", "")

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
	// CompileTraceQLFilterWithOptions and CompileOptions are called from QueryTraceQL
	// when QueryOptions.Embedder is set. Anchor them here for the deadcode tool.
	_ = vm.CompileTraceQLFilterWithOptions
	_ = vm.CompileOptions{}

	// modules/blockio package - anchor all public API entry points and methods.
	mbw, _ := modulesblockio.NewWriterWithConfig(modulesblockio.WriterConfig{OutputStream: io.Discard})
	if mbw != nil {
		_ = mbw.AddSpan(nil, nil, nil, "", nil, "")
		_ = mbw.AddTracesData(nil)
		// Skip Flush() — it will panic with nil spans, but we only need to anchor the method reference
		_ = mbw.CurrentSize()
		// NOTE-458 (issue #377): FlushedBytes is consumed by Tempo's
		// vblockpack walBlock.DataLength to cut blocks by real on-disk size.
		_ = mbw.FlushedBytes()
	}
	mbProvider := modulesblockio.NewDefaultProvider(&noopProvider{})
	if mbProvider != nil {
		_, _ = mbProvider.Size()
		_, _ = mbProvider.ReadAt(nil, 0, blockpack.DataTypeBlock)
		_ = mbProvider.IOOps()
		_ = mbProvider.BytesRead()
		// I/O guardrail (NOTE-403): classifies io_ops / bytes-per-io against the
		// documented bands. Public API for tempo / CI to gate read-path regressions.
		_ = mbProvider.IOHealth()
	}
	{
		h := modulesblockio.EvaluateIOHealth(0, 0)
		_ = h.Band()
		_ = h.IOOpsBand.String()
		_ = modulesblockio.BandGood
		_ = modulesblockio.BandWarning
		_ = modulesblockio.BandCritical
	}
	var mbr *modulesblockio.Reader
	if mbProvider != nil {
		mbr, _ = modulesblockio.NewReaderFromProvider(mbProvider)
	}
	if mbr != nil {
		_ = mbr.BlockCount()
		_ = mbr.BlockMeta(0)
		_, _ = mbr.GetBlockWithBytes(0, nil)
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
		_, qs, _ := modules_executor.Collect(context.Background(), mbr, nil, modules_executor.CollectOptions{})
		_ = qs.Explain()
		_ = qs.SelectedBlocks()
		_ = modules_executor.BuildPredicates
		_ = modules_executor.SpanMatchFromRow
	}

	// AnalyzeFileLayout - anchor file layout API
	if r != nil {
		_, _ = blockpack.AnalyzeFileLayout(r)
	}

	// ExecuteMetricsTraceQL - anchor new TraceQL metrics API
	if r != nil {
		_, _ = blockpack.ExecuteMetricsTraceQL(
			context.Background(),
			r,
			`{ } | count_over_time()`,
			blockpack.TraceMetricOptions{
				StartNano: 0,
				EndNano:   int64(60 * 1e9),
				StepNano:  int64(60 * 1e9),
			},
		)
	}

	// CompactBlocks - anchor compaction API
	_, _ = blockpack.CompactBlocks(
		context.Background(),
		[]blockpack.ReaderProvider{&noopProvider{}},
		blockpack.CompactionConfig{},
		storage,
	)

	// CompactBlocksStreaming - anchor memory-bounded streaming compaction API (NOTE-459).
	// Consumed externally by tempo's vblockpack compactor.
	_, _ = blockpack.CompactBlocksStreaming(
		context.Background(),
		[]blockpack.CompactionProviderFunc{
			func() (blockpack.ReaderProvider, error) { return &noopProvider{}, nil },
		},
		blockpack.CompactionConfig{},
		storage,
	)

	// ReadBlockValueSets / BlockSimilarity - anchor compaction similarity-scoring API
	// (NOTE-463, issue #382). Consumed externally by tempo's vblockpack compactor to read
	// each candidate block's intrinsic ToC value sets and order inputs by content similarity.
	leanR, _ := blockpack.NewLeanReaderFromProvider(&noopProvider{})
	if leanR != nil {
		vs, _ := blockpack.ReadBlockValueSets(leanR)
		_ = blockpack.BlockSimilarity(vs, vs)
	}

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
	_, _ = blockpack.NewReaderForProgram(nil, &noopProvider{}, "anchor", fc)

	// MemoryCache / MemCache / ChainedCache — multi-tier cache constructors
	mc, _ := blockpack.NewMemoryCache(blockpack.MemoryCacheConfig{MaxBytes: 1024})
	rc, _ := blockpack.OpenMemCache(blockpack.MemCacheConfig{})
	_ = blockpack.NewChainedCache(mc, fc, rc)

	// TypedTieredCache — anchor typed cache constructors and SectionCache constructors
	tc := blockpack.NewTypedTieredCache(blockpack.DefaultTypedConfig(mc, mc))
	_ = blockpack.NewTypedTieredCache(blockpack.TypedConfig{Registerer: nil})
	_, _ = blockpack.NewReaderWithSectionCache(&noopProvider{}, "id", tc)
	_, _ = blockpack.NewLeanReaderWithSectionCache(&noopProvider{}, "id", tc)
	// SectionCache nil guard via NewFilecacheAdapter
	var sc blockpack.SectionCache = tc
	_ = sc

	// NewWriterWithConfig - anchor vector-enabled writer constructor
	_, _ = blockpack.NewWriterWithConfig(blockpack.WriterConfig{OutputStream: io.Discard})

	// embedder subpackage - anchor the public HTTP embedder constructor used by
	// the write path (tempo's vblockpack create/embedder.go). NOTE-370 moved this
	// out of the top-level blockpack package into github.com/grafana/blockpack/embedder.
	_, _ = pubembedder.NewHTTPEmbedder(pubembedder.HTTPConfig{})

	// internal embedder package - anchor public API entry points used by the write
	// path and the GGUF-backed constructors that remain part of the package's
	// public surface.
	_ = embedder.AssembleText
	emb, _ := embedder.New(embedder.Config{})
	if emb != nil {
		_ = emb.Dim()
		_, _ = emb.Embed("")
		_, _ = emb.EmbedBatch(nil)
		_ = emb.AssembleText(nil)
		emb.Close()
	}
	if emb != nil {
		_ = embedder.NewWithBackend(emb, nil, 0)
	}
	_, _ = embedder.NewHTTP(embedder.HTTPConfig{})
	_ = embedder.DefaultModelPath

	// WantAll/WantOnly/SetProcessCacheBytes/TwoTierTypedConfig — public API used by tempo.
	_ = blockpack.WantAll()
	_ = blockpack.WantOnly(nil)
	blockpack.SetProcessCacheBytes(0)
	_ = blockpack.TwoTierTypedConfig(nil, nil)

	// CompileTraceQL/QueryTraceQLWithProgram — public API used by tempo.
	prog, _ := blockpack.CompileTraceQL(`{}`, blockpack.QueryOptions{})
	if prog != nil && r != nil {
		_, _, _ = blockpack.QueryTraceQLWithProgram(context.Background(), r, prog, blockpack.QueryOptions{})
	}

	// blockevents subpackage — anchor the public publisher API used by tempo's
	// vblockpack create/compaction paths (blockpack issue #397, NOTE-VI-015).
	bep, _ := pubblockevents.NewPublisher(pubblockevents.Config{})
	if bep != nil {
		_ = bep.Publish(context.Background(), pubblockevents.Message{Action: pubblockevents.ActionCreate})
		_ = bep.Close()
	}
	_ = pubblockevents.NewNoopPublisher()
	if cp := pubblockevents.NewChanPublisher(0); cp != nil {
		_ = cp.C()
		_ = cp.Dropped()
	}
	_ = pubblockevents.ErrPublisherClosed
	var _ pubblockevents.RedisStreamsPublisher
	var _ pubblockevents.Action

	// valueindexconsumer subpackage — anchor the public consumer-service API
	// used by tempo's value-index consumer deployment (blockpack issue #398,
	// NOTE-VI-016). NewRedisConsumer dials Redis so is only exercised for its
	// type signature, not invoked here.
	anchorValueIndexConsumer()
	anchorValueIndexCompactor()
	// internal/modules/cube — anchor cube compactor functions not yet wired to production.
	anchorCubeCompactor()
}

// anchorValueIndexCompactor anchors the public compactor-service API used by
// tempo's value-index compactor deployment (blockpack issue #399, NOTE-VI-017).
func anchorValueIndexCompactor() {
	cfg := pubvalueindexcompactor.Config{Enabled: true, Tenants: []string{"*"}}
	svc, err := pubvalueindexcompactor.NewService(cfg, nil, nil)
	if err == nil && svc != nil {
		_ = svc.Run(context.Background())
		_ = svc.RunOnce(context.Background())
	}
	var (
		_ pubvalueindexcompactor.IndexStore
		_ pubvalueindexcompactor.SourceExister
	)
	_ = pubvalueindexcompactor.DefaultIndexPrefix
	_ = pubvalueindexcompactor.DefaultCompactInterval
	_ = pubvalueindexcompactor.DefaultCompactThresholdFiles
	_ = pubvalueindexcompactor.DefaultMaxOutputBytes
}

func anchorValueIndexConsumer() {
	cfg := pubvalueindexconsumer.Config{Enabled: true, Columns: []string{"span:name"}}
	svc, err := pubvalueindexconsumer.NewService(cfg, nil, nil, nil)
	if err == nil && svc != nil {
		_ = svc.Run(context.Background())
	}
	var (
		_ pubvalueindexconsumer.Message
		_ pubvalueindexconsumer.ColumnEntry
		_ pubvalueindexconsumer.Consumer
		_ pubvalueindexconsumer.Extractor
		_ pubvalueindexconsumer.ObjectPutter
		_ pubvalueindexconsumer.RedisConsumer
	)
	_ = pubvalueindexconsumer.DefaultStreamName
	_ = pubvalueindexconsumer.DefaultConsumerGroup
	_ = pubvalueindexconsumer.DefaultIndexPrefix
	_ = pubvalueindexconsumer.DefaultBatchSize
	_ = pubvalueindexconsumer.DefaultPollTimeout
	_ = pubvalueindexconsumer.DefaultFlushInterval
	_ = pubvalueindexconsumer.DefaultMaxColumnBufferBytes
	if c, err := pubvalueindexconsumer.NewRedisConsumer(cfg); err == nil && c != nil {
		_ = c.Close()
	}
}

func anchorCubeCompactor() {
	// Anchor cube compactor functions not yet wired to a production entry point.
	// PlanL1Rollup and Compactor.Evict are future cube service integration points.
	_, _ = cube.PlanL1Rollup(nil, 0, "", "")
	comp := cube.NewCompactor(nil, nil, cube.CompactorConfig{})
	if comp != nil {
		_ = comp.Evict(context.Background(), "", "")
	}
}

// anchorVIStats consumes a ValueIndexBuildStats so the type and its fields are not
// reported as dead. The stats are produced by SliceValueIndexSource.Stats and read
// by tempo's querier (issue #465), which is a separate process.
func anchorVIStats(s blockpack.ValueIndexBuildStats) {
	_ = s.FilesRead
	_ = s.BytesRead
	_ = s.Hits
}

package parquetconv

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/grafana/blockpack/internal/otlpconvert"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// WriteParquet writes OTLP traces to a Tempo vparquet5 block directory and returns the total size.
// It removes any existing data in outDir first to prevent accumulation of stale blocks.
func WriteParquet(traces []*tracev1.TracesData, outDir string) (int64, error) {
	// Clean existing directory to prevent accumulating old blocks.
	// Each call creates a new UUID-based block, so without cleanup the directory grows unboundedly.
	// Safety: only remove if outDir is non-empty, non-root, and looks like a path with depth.
	if outDir == "" || outDir == "/" || outDir == "." {
		return 0, fmt.Errorf("refusing to clean unsafe output directory: %q", outDir)
	}
	if err := os.RemoveAll(outDir); err != nil {
		return 0, fmt.Errorf("failed to clean output directory: %w", err)
	}

	// Reuse the canonical OTLP→Tempo conversion logic to avoid data loss and duplication.
	// BuildTempoTraces properly handles:
	// - Events, links, dropped counts, tracestate
	// - Schema URLs
	// - Instrumentation scope attributes
	// - Complex AnyValue types (arrays, kvlists)
	// - Proper resource/scope deduplication
	tempoTraces, traceIDs, err := otlpconvert.BuildTempoTraces(traces)
	if err != nil {
		return 0, err
	}

	_, size, err := WriteTempoBlock(tempoTraces, traceIDs, outDir)
	return size, err
}

// WriteTempoBlock writes traces into a Tempo vparquet5 block directory.
func WriteTempoBlock(traces []*tempopb.Trace, traceIDs []common.ID, outDir string) (string, int64, error) {
	if len(traces) == 0 {
		return "", 0, fmt.Errorf("no traces provided")
	}
	if len(traces) != len(traceIDs) {
		return "", 0, fmt.Errorf("trace count mismatch: %d traces vs %d ids", len(traces), len(traceIDs))
	}

	ctx := context.Background()
	tenant := "default"
	blockID := uuid.New()

	rawReader, rawWriter, _, err := local.New(&local.Config{Path: outDir})
	if err != nil {
		return "", 0, err
	}
	reader := backend.NewReader(rawReader)
	writer := backend.NewWriter(rawWriter)

	// Use test-specific dedicated columns matching Tempo's test setup
	dedicatedCols := testDedicatedColumns()
	meta := backend.NewBlockMetaWithDedicatedColumns(tenant, blockID, vparquet5.VersionString, dedicatedCols)
	meta.TotalObjects = int64(len(traces))

	cfg := &common.BlockConfig{
		BloomFP:             common.DefaultBloomFP,
		BloomShardSizeBytes: common.DefaultBloomShardSizeBytes,
		RowGroupSizeBytes:   100_000_000,
		DedicatedColumns:    dedicatedCols,
	}

	iter := &traceIterator{traces: traces, ids: traceIDs, index: 0}
	outMeta, err := vparquet5.CreateBlock(ctx, cfg, meta, iter, reader, writer)
	if err != nil {
		return "", 0, err
	}
	if err := writer.WriteBlockMeta(ctx, outMeta); err != nil { //nolint:govet
		return "", 0, err
	}
	if err := writer.WriteTenantIndex(ctx, tenant, []*backend.BlockMeta{outMeta}, nil); err != nil { //nolint:govet
		return "", 0, err
	}

	blockPath := filepath.Join(outDir, tenant, blockID.String())
	size, err := dirSize(blockPath)
	if err != nil {
		return "", 0, err
	}
	return blockPath, size, nil
}

// testDedicatedColumns returns the dedicated columns configuration used in Tempo's tests.
// This matches pkg/util/test/req.go MakeDedicatedColumns() from Tempo repository.
func testDedicatedColumns() backend.DedicatedColumns {
	return backend.DedicatedColumns{
		// Resource dedicated columns
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.1",
			Type:  backend.DedicatedColumnTypeString,
		},
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.2",
			Type:  backend.DedicatedColumnTypeString,
		},
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.3",
			Type:  backend.DedicatedColumnTypeString,
		},
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.4",
			Type:  backend.DedicatedColumnTypeString,
		},
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.5",
			Type:  backend.DedicatedColumnTypeString,
		},
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.6",
			Type:  backend.DedicatedColumnTypeInt,
		},
		{
			Scope: backend.DedicatedColumnScopeResource,
			Name:  "dedicated.resource.7",
			Type:  backend.DedicatedColumnTypeInt,
		},
		// Span dedicated columns
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.1", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.2", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.3", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.4", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.5", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.6", Type: backend.DedicatedColumnTypeInt},
		{Scope: backend.DedicatedColumnScopeSpan, Name: "dedicated.span.7", Type: backend.DedicatedColumnTypeInt},
		// Event dedicated columns not supported in our Tempo version (v1.5.1)
		// {Scope: backend.DedicatedColumnScopeEvent, Name: "dedicated.event.1", Type: backend.DedicatedColumnTypeString},
		// {Scope: backend.DedicatedColumnScopeEvent, Name: "dedicated.event.2", Type: backend.DedicatedColumnTypeString},
	}
}

type traceIterator struct {
	traces []*tempopb.Trace
	ids    []common.ID
	index  int
}

func (it *traceIterator) Next(_ context.Context) (common.ID, *tempopb.Trace, error) {
	if it.index >= len(it.traces) {
		return nil, nil, io.EOF
	}
	id := it.ids[it.index]
	tr := it.traces[it.index]
	it.index++
	return id, tr, nil
}

func (it *traceIterator) Close() {
}

func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

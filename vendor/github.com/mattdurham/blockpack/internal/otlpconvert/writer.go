package otlpconvert

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/encoding/vparquet5"
	blockpackio "github.com/mattdurham/blockpack/blockpack/io"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// WriteBlockpack encodes OTLP traces into the blockpack format.
func WriteBlockpack(traces []*tracev1.TracesData, maxSpansPerBlock int) ([]byte, error) {
	normalized, err := NormalizeTracesForBlockpack(traces)
	if err != nil {
		return nil, err
	}
	writer := blockpackio.NewWriter(maxSpansPerBlock)
	if len(normalized) != len(traces) {
		return nil, fmt.Errorf("normalized trace count mismatch: %d != %d", len(normalized), len(traces))
	}
	for i, trace := range normalized {
		if err := writer.AddTracesDataWithRaw(trace, traces[i]); err != nil {
			return nil, err
		}
	}
	return writer.Flush()
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

	meta := backend.NewBlockMeta(tenant, blockID, vparquet5.VersionString, backend.EncNone, "")
	meta.TotalObjects = int64(len(traces))

	// Use test-specific dedicated columns matching Tempo's test setup
	dedicatedCols := testDedicatedColumns()
	meta.DedicatedColumns = dedicatedCols

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
	if err := writer.WriteBlockMeta(ctx, outMeta); err != nil {
		return "", 0, err
	}
	if err := writer.WriteTenantIndex(ctx, tenant, []*backend.BlockMeta{outMeta}, nil); err != nil {
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
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.1", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.2", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.3", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.4", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.5", Type: backend.DedicatedColumnTypeString},
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.6", Type: backend.DedicatedColumnTypeInt},
		{Scope: backend.DedicatedColumnScopeResource, Name: "dedicated.resource.7", Type: backend.DedicatedColumnTypeInt},
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

func (it *traceIterator) Next(ctx context.Context) (common.ID, *tempopb.Trace, error) {
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
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
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
